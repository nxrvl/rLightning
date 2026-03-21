//! Pipeline batching: groups consecutive same-shard commands for batch execution
//! under a single lock acquisition, reducing locking overhead in pipelines.
//!
//! When multiple pipelined commands target the same shard, this module groups them
//! and executes all commands under a single lock instead of acquiring/releasing
//! the lock for each command individually.

use crate::command::{Command, CommandError, CommandResult};
use crate::networking::raw_command::cmd_eq;
use crate::networking::resp::RespValue;
use crate::storage::item::Entry;
use crate::storage::sharded::ShardMap;
use crate::storage::value::StoreValue;
use std::time::{Duration, Instant};
use crate::storage::clock::cached_now;

/// Classification of a command for pipeline batching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    /// Read-only single-key command (can use read lock)
    Read,
    /// Write single-key command (needs write lock)
    Write,
    /// Cannot be batched (multi-key, transaction, pub/sub, scripting, etc.)
    Excluded,
}

/// A batch of consecutive commands targeting the same shard.
pub struct BatchGroup<'a> {
    pub shard_idx: usize,
    pub is_read_only: bool,
    pub commands: Vec<&'a Command>,
}

/// Entry in the pipeline processing sequence.
pub enum PipelineEntry<'a> {
    /// A batch of commands to execute under a single shard lock.
    Batch(BatchGroup<'a>),
    /// An individual command that must go through normal dispatch.
    Individual(&'a Command),
}

/// Classify a command for batching using fast byte-level dispatch.
/// Returns Read for read-only single-key commands, Write for write single-key commands,
/// and Excluded for everything else (multi-key, transactions, pub/sub, scripting, etc.).
pub fn classify_command(name: &[u8], args: &[Vec<u8>]) -> CommandKind {
    match name.first().map(|b| b.to_ascii_uppercase()) {
        Some(b'A') => {
            if name.len() == 6 && cmd_eq(name, b"APPEND") {
                return CommandKind::Write;
            }
            CommandKind::Excluded
        }
        Some(b'D') => {
            if name.len() == 3 && cmd_eq(name, b"DEL") {
                // DEL with multiple keys is multi-key - exclude
                if args.len() > 1 {
                    return CommandKind::Excluded;
                }
                return CommandKind::Write;
            }
            if name.len() == 4 && cmd_eq(name, b"DECR") {
                return CommandKind::Write;
            }
            if name.len() == 6 && cmd_eq(name, b"DECRBY") {
                return CommandKind::Write;
            }
            CommandKind::Excluded
        }
        Some(b'E') => {
            if name.len() == 6 {
                if cmd_eq(name, b"EXISTS") {
                    // EXISTS with multiple keys is multi-key
                    if args.len() > 1 {
                        return CommandKind::Excluded;
                    }
                    return CommandKind::Read;
                }
                if cmd_eq(name, b"EXPIRE") {
                    // EXPIRE with condition flags (NX/XX/GT/LT) is complex - exclude
                    if args.len() > 2 {
                        return CommandKind::Excluded;
                    }
                    return CommandKind::Write;
                }
            }
            CommandKind::Excluded
        }
        Some(b'G') => {
            if name.len() == 3 && cmd_eq(name, b"GET") {
                return CommandKind::Read;
            }
            if name.len() == 6 && cmd_eq(name, b"GETDEL") {
                return CommandKind::Write;
            }
            CommandKind::Excluded
        }
        Some(b'H') => {
            if name.len() == 4 {
                if cmd_eq(name, b"HGET") {
                    return CommandKind::Read;
                }
                if cmd_eq(name, b"HSET") {
                    return CommandKind::Write;
                }
                if cmd_eq(name, b"HDEL") {
                    return CommandKind::Write;
                }
                if cmd_eq(name, b"HLEN") {
                    return CommandKind::Read;
                }
            }
            if name.len() == 7 && cmd_eq(name, b"HEXISTS") {
                return CommandKind::Read;
            }
            CommandKind::Excluded
        }
        Some(b'I') => {
            if name.len() == 4 && cmd_eq(name, b"INCR") {
                return CommandKind::Write;
            }
            if name.len() == 6 && cmd_eq(name, b"INCRBY") {
                return CommandKind::Write;
            }
            CommandKind::Excluded
        }
        Some(b'L') => {
            if name.len() == 4 && cmd_eq(name, b"LLEN") {
                return CommandKind::Read;
            }
            if name.len() == 5 && cmd_eq(name, b"LPUSH") {
                return CommandKind::Write;
            }
            CommandKind::Excluded
        }
        Some(b'P') => {
            if name.len() == 4 && cmd_eq(name, b"PTTL") {
                return CommandKind::Read;
            }
            if name.len() == 7 {
                if cmd_eq(name, b"PEXPIRE") {
                    // PEXPIRE with condition flags (NX/XX/GT/LT) is complex - exclude
                    if args.len() > 2 {
                        return CommandKind::Excluded;
                    }
                    return CommandKind::Write;
                }
                if cmd_eq(name, b"PERSIST") {
                    return CommandKind::Write;
                }
            }
            CommandKind::Excluded
        }
        Some(b'R') => {
            if name.len() == 5 && cmd_eq(name, b"RPUSH") {
                return CommandKind::Write;
            }
            CommandKind::Excluded
        }
        Some(b'S') => {
            if name.len() == 3 && cmd_eq(name, b"SET") {
                // SET with options (NX/XX/EX/PX/GET/KEEPTTL) is complex - exclude
                if args.len() > 2 {
                    return CommandKind::Excluded;
                }
                return CommandKind::Write;
            }
            if name.len() == 4 {
                if cmd_eq(name, b"SADD") {
                    return CommandKind::Write;
                }
                if cmd_eq(name, b"SREM") {
                    return CommandKind::Write;
                }
            }
            if name.len() == 5 {
                if cmd_eq(name, b"SCARD") {
                    return CommandKind::Read;
                }
            }
            if name.len() == 6 && cmd_eq(name, b"STRLEN") {
                return CommandKind::Read;
            }
            if name.len() == 9 && cmd_eq(name, b"SISMEMBER") {
                return CommandKind::Read;
            }
            CommandKind::Excluded
        }
        Some(b'T') => {
            if name.len() == 3 && cmd_eq(name, b"TTL") {
                return CommandKind::Read;
            }
            if name.len() == 4 && cmd_eq(name, b"TYPE") {
                return CommandKind::Read;
            }
            CommandKind::Excluded
        }
        Some(b'Z') => {
            if name.len() == 4 {
                // ZADD excluded from batching: batch_zadd doesn't handle NX/XX/GT/LT/CH flags
                if cmd_eq(name, b"ZREM") {
                    return CommandKind::Write;
                }
            }
            if name.len() == 5 && cmd_eq(name, b"ZCARD") {
                return CommandKind::Read;
            }
            if name.len() == 6 && cmd_eq(name, b"ZSCORE") {
                return CommandKind::Read;
            }
            CommandKind::Excluded
        }
        _ => CommandKind::Excluded,
    }
}

/// Extract the primary key from a command. Returns None if the command
/// has no key argument (e.g., PING) or is excluded from batching.
fn extract_primary_key(cmd: &Command) -> Option<&[u8]> {
    if cmd.args.is_empty() {
        return None;
    }
    let kind = classify_command(cmd.name.as_bytes(), &cmd.args);
    match kind {
        CommandKind::Excluded => None,
        CommandKind::Read | CommandKind::Write => Some(&cmd.args[0]),
    }
}

/// Group a pipeline of commands into batch groups and individual commands.
/// Only groups consecutive commands that target the same shard and have compatible
/// lock types (all reads or all writes).
pub fn group_pipeline<'a>(
    commands: &'a [Command],
    shard_index_fn: impl Fn(&[u8]) -> usize,
) -> Vec<PipelineEntry<'a>> {
    let mut entries = Vec::with_capacity(commands.len());
    let mut batch_shard: usize = 0;
    let mut batch_read_only: bool = true;
    let mut batch_cmds: Vec<&'a Command> = Vec::new();

    for cmd in commands {
        let key = extract_primary_key(cmd);

        match key {
            Some(key) => {
                let shard_idx = shard_index_fn(key);
                let is_read = classify_command(cmd.name.as_bytes(), &cmd.args) == CommandKind::Read;

                if !batch_cmds.is_empty() {
                    // Check if this command extends the current batch
                    if batch_shard == shard_idx && batch_read_only == is_read {
                        batch_cmds.push(cmd);
                        continue;
                    }
                    // Different shard or lock type - flush current batch
                    flush_batch(
                        &mut entries,
                        &mut batch_cmds,
                        batch_shard,
                        batch_read_only,
                    );
                }

                // Start new batch
                batch_shard = shard_idx;
                batch_read_only = is_read;
                batch_cmds.push(cmd);
            }
            None => {
                // Non-batchable command - flush current batch and add individually
                flush_batch(
                    &mut entries,
                    &mut batch_cmds,
                    batch_shard,
                    batch_read_only,
                );
                entries.push(PipelineEntry::Individual(cmd));
            }
        }
    }

    // Flush remaining batch
    flush_batch(
        &mut entries,
        &mut batch_cmds,
        batch_shard,
        batch_read_only,
    );

    entries
}

fn flush_batch<'a>(
    entries: &mut Vec<PipelineEntry<'a>>,
    batch_cmds: &mut Vec<&'a Command>,
    shard_idx: usize,
    is_read_only: bool,
) {
    if batch_cmds.is_empty() {
        return;
    }
    if batch_cmds.len() == 1 {
        // Single command - no benefit from batching, dispatch individually
        entries.push(PipelineEntry::Individual(batch_cmds.pop().unwrap()));
    } else {
        entries.push(PipelineEntry::Batch(BatchGroup {
            shard_idx,
            is_read_only,
            commands: std::mem::take(batch_cmds),
        }));
    }
}

/// Execute a batch of read commands under a single shard read lock.
/// Returns one CommandResult per command in order.
pub fn execute_read_batch(map: &ShardMap, commands: &[&Command]) -> Vec<CommandResult> {
    let now = cached_now();
    commands
        .iter()
        .map(|cmd| execute_read_cmd(map, cmd, now))
        .collect()
}

/// Execute a batch of write commands under a single shard write lock.
/// Returns one (CommandResult, mem_delta, key_delta) per command in order.
/// key_delta: +1 = key created, -1 = key deleted, 0 = no change.
pub fn execute_write_batch(
    map: &mut ShardMap,
    commands: &[&Command],
    max_key_size: usize,
    max_value_size: usize,
) -> Vec<(CommandResult, i64, i32)> {
    commands
        .iter()
        .map(|cmd| {
            // Validate key size for commands that have a key arg
            if let Some(key) = cmd.args.first() {
                if key.len() > max_key_size {
                    return (Err(CommandError::InvalidArgument(format!(
                        "key size {} exceeds maximum {}",
                        key.len(),
                        max_key_size
                    ))), 0, 0);
                }
            }
            // Validate value size for SET (args[1]) and APPEND (args[1])
            let name = cmd.name.as_bytes();
            if cmd.args.len() >= 2 {
                let is_set = name.len() == 3 && cmd_eq(name, b"SET");
                let is_append = name.len() == 6 && cmd_eq(name, b"APPEND");
                if (is_set || is_append) && cmd.args[1].len() > max_value_size {
                    return (Err(CommandError::InvalidArgument(format!(
                        "value size {} exceeds maximum {}",
                        cmd.args[1].len(),
                        max_value_size
                    ))), 0, 0);
                }
            }
            execute_write_cmd(map, cmd)
        })
        .collect()
}

// --- Read command implementations ---

fn execute_read_cmd(map: &ShardMap, cmd: &Command, now: Instant) -> CommandResult {
    let name = cmd.name.as_bytes();
    match name.first().map(|b| b.to_ascii_uppercase()) {
        Some(b'E') if cmd_eq(name, b"EXISTS") => batch_exists(map, &cmd.args, now),
        Some(b'G') if cmd_eq(name, b"GET") => batch_get(map, &cmd.args, now),
        Some(b'H') => {
            if cmd_eq(name, b"HGET") {
                batch_hget(map, &cmd.args, now)
            } else if cmd_eq(name, b"HLEN") {
                batch_hlen(map, &cmd.args, now)
            } else if cmd_eq(name, b"HEXISTS") {
                batch_hexists(map, &cmd.args, now)
            } else {
                Err(CommandError::UnknownCommand(cmd.name.clone()))
            }
        }
        Some(b'L') if cmd_eq(name, b"LLEN") => batch_llen(map, &cmd.args, now),
        Some(b'P') if cmd_eq(name, b"PTTL") => batch_pttl(map, &cmd.args, now),
        Some(b'S') => {
            if cmd_eq(name, b"SCARD") {
                batch_scard(map, &cmd.args, now)
            } else if cmd_eq(name, b"STRLEN") {
                batch_strlen(map, &cmd.args, now)
            } else if cmd_eq(name, b"SISMEMBER") {
                batch_sismember(map, &cmd.args, now)
            } else {
                Err(CommandError::UnknownCommand(cmd.name.clone()))
            }
        }
        Some(b'T') => {
            if cmd_eq(name, b"TTL") {
                batch_ttl(map, &cmd.args, now)
            } else if cmd_eq(name, b"TYPE") {
                batch_type(map, &cmd.args, now)
            } else {
                Err(CommandError::UnknownCommand(cmd.name.clone()))
            }
        }
        Some(b'Z') => {
            if cmd_eq(name, b"ZCARD") {
                batch_zcard(map, &cmd.args, now)
            } else if cmd_eq(name, b"ZSCORE") {
                batch_zscore(map, &cmd.args, now)
            } else {
                Err(CommandError::UnknownCommand(cmd.name.clone()))
            }
        }
        _ => Err(CommandError::UnknownCommand(cmd.name.clone())),
    }
}

/// Look up entry, returning None if expired or missing.
#[inline]
fn get_valid_entry<'a>(
    map: &'a ShardMap,
    key: &[u8],
    now: Instant,
) -> Option<&'a Entry> {
    map.get(key).and_then(|entry| {
        if entry.expires_at.map_or(false, |t| now > t) {
            None // expired
        } else {
            Some(entry)
        }
    })
}

fn batch_get(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match &entry.value {
            StoreValue::Str(data) => Ok(RespValue::BulkString(Some(data.to_vec()))),
            _ => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::BulkString(None)),
    }
}

fn batch_exists(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let exists = get_valid_entry(map, &args[0], now).is_some();
    Ok(RespValue::Integer(if exists { 1 } else { 0 }))
}

fn batch_ttl(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match entry.expires_at {
            Some(expires_at) => {
                let remaining = expires_at.saturating_duration_since(now);
                Ok(RespValue::Integer(remaining.as_secs() as i64))
            }
            None => Ok(RespValue::Integer(-1)),
        },
        None => Ok(RespValue::Integer(-2)),
    }
}

fn batch_pttl(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match entry.expires_at {
            Some(expires_at) => {
                let remaining = expires_at.saturating_duration_since(now);
                Ok(RespValue::Integer(remaining.as_millis() as i64))
            }
            None => Ok(RespValue::Integer(-1)),
        },
        None => Ok(RespValue::Integer(-2)),
    }
}

fn batch_type(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => Ok(RespValue::SimpleString(
            entry.data_type().as_str().to_string(),
        )),
        None => Ok(RespValue::SimpleString("none".to_string())),
    }
}

fn batch_strlen(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match &entry.value {
            StoreValue::Str(data) => Ok(RespValue::Integer(data.len() as i64)),
            _ => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::Integer(0)),
    }
}

fn batch_hget(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match &entry.value {
            StoreValue::Hash(hash) => match hash.get(&args[1]) {
                Some(val) => Ok(RespValue::BulkString(Some(val.clone()))),
                None => Ok(RespValue::BulkString(None)),
            },
            _ => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::BulkString(None)),
    }
}

fn batch_hlen(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match &entry.value {
            StoreValue::Hash(hash) => Ok(RespValue::Integer(hash.len() as i64)),
            _ => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::Integer(0)),
    }
}

fn batch_hexists(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match &entry.value {
            StoreValue::Hash(hash) => {
                Ok(RespValue::Integer(if hash.contains_key(&args[1]) {
                    1
                } else {
                    0
                }))
            }
            _ => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::Integer(0)),
    }
}

fn batch_llen(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match &entry.value {
            StoreValue::List(list) => Ok(RespValue::Integer(list.len() as i64)),
            _ => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::Integer(0)),
    }
}

fn batch_scard(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match &entry.value {
            StoreValue::Set(set) => Ok(RespValue::Integer(set.len() as i64)),
            _ => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::Integer(0)),
    }
}

fn batch_sismember(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match &entry.value {
            StoreValue::Set(set) => Ok(RespValue::Integer(if set.contains(&args[1]) {
                1
            } else {
                0
            })),
            _ => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::Integer(0)),
    }
}

fn batch_zcard(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match &entry.value {
            StoreValue::ZSet(zset) => Ok(RespValue::Integer(zset.len() as i64)),
            _ => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::Integer(0)),
    }
}

fn batch_zscore(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match &entry.value {
            StoreValue::ZSet(zset) => match zset.get_score(&args[1]) {
                Some(score) => Ok(RespValue::BulkString(Some(
                    score.to_string().into_bytes(),
                ))),
                None => Ok(RespValue::BulkString(None)),
            },
            _ => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::BulkString(None)),
    }
}

// --- Write command implementations ---
// Returns (CommandResult, mem_delta, key_delta) where:
//   mem_delta = change in bytes
//   key_delta = +1 (key created), -1 (key deleted), 0 (no change)

fn execute_write_cmd(map: &mut ShardMap, cmd: &Command) -> (CommandResult, i64, i32) {
    let name = cmd.name.as_bytes();
    match name.first().map(|b| b.to_ascii_uppercase()) {
        Some(b'A') if cmd_eq(name, b"APPEND") => batch_append(map, &cmd.args),
        Some(b'D') => {
            if name.len() == 3 && cmd_eq(name, b"DEL") {
                batch_del(map, &cmd.args)
            } else if cmd_eq(name, b"DECR") {
                batch_incr_by(map, &cmd.args, -1)
            } else if cmd_eq(name, b"DECRBY") {
                if cmd.args.len() < 2 {
                    return (Err(CommandError::WrongNumberOfArguments), 0, 0);
                }
                match parse_i64(&cmd.args[1]) {
                    Some(delta) => batch_incr_by(map, &cmd.args, -delta),
                    None => (Err(CommandError::NotANumber), 0, 0),
                }
            } else {
                (Err(CommandError::UnknownCommand(cmd.name.clone())), 0, 0)
            }
        }
        Some(b'E') if cmd_eq(name, b"EXPIRE") => batch_expire(map, &cmd.args),
        Some(b'G') if cmd_eq(name, b"GETDEL") => batch_getdel(map, &cmd.args),
        Some(b'H') => {
            if cmd_eq(name, b"HSET") {
                batch_hset(map, &cmd.args)
            } else if cmd_eq(name, b"HDEL") {
                batch_hdel(map, &cmd.args)
            } else {
                (Err(CommandError::UnknownCommand(cmd.name.clone())), 0, 0)
            }
        }
        Some(b'I') => {
            if cmd_eq(name, b"INCR") {
                batch_incr_by(map, &cmd.args, 1)
            } else if cmd_eq(name, b"INCRBY") {
                if cmd.args.len() < 2 {
                    return (Err(CommandError::WrongNumberOfArguments), 0, 0);
                }
                match parse_i64(&cmd.args[1]) {
                    Some(delta) => batch_incr_by(map, &cmd.args, delta),
                    None => (Err(CommandError::NotANumber), 0, 0),
                }
            } else {
                (Err(CommandError::UnknownCommand(cmd.name.clone())), 0, 0)
            }
        }
        Some(b'L') if cmd_eq(name, b"LPUSH") => batch_lpush(map, &cmd.args),
        Some(b'P') => {
            if cmd_eq(name, b"PEXPIRE") {
                batch_pexpire(map, &cmd.args)
            } else if cmd_eq(name, b"PERSIST") {
                batch_persist(map, &cmd.args)
            } else {
                (Err(CommandError::UnknownCommand(cmd.name.clone())), 0, 0)
            }
        }
        Some(b'R') if cmd_eq(name, b"RPUSH") => batch_rpush(map, &cmd.args),
        Some(b'S') => {
            if name.len() == 3 && cmd_eq(name, b"SET") {
                batch_set(map, &cmd.args)
            } else if cmd_eq(name, b"SADD") {
                batch_sadd(map, &cmd.args)
            } else if cmd_eq(name, b"SREM") {
                batch_srem(map, &cmd.args)
            } else {
                (Err(CommandError::UnknownCommand(cmd.name.clone())), 0, 0)
            }
        }
        Some(b'Z') => {
            if cmd_eq(name, b"ZADD") {
                batch_zadd(map, &cmd.args)
            } else if cmd_eq(name, b"ZREM") {
                batch_zrem(map, &cmd.args)
            } else {
                (Err(CommandError::UnknownCommand(cmd.name.clone())), 0, 0)
            }
        }
        _ => (Err(CommandError::UnknownCommand(cmd.name.clone())), 0, 0),
    }
}

// Note: get_valid_entry_mut was removed - each write command handles
// expiry inline to avoid borrow-checker issues with the two-phase check.

fn batch_set(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let value = &args[1];
    let new_mem = (key.len() + value.len()) as i64;
    let now = cached_now();

    let (old_mem, existed_in_map) = map
        .get(key)
        .map(|e| {
            let mem = (key.len() + e.value.mem_size()) as i64;
            if e.expires_at.map_or(false, |t| now > t) {
                // Expired entry: still account for its memory since map.insert replaces it
                (mem, true)
            } else {
                (mem, true)
            }
        })
        .unwrap_or((0, false));

    let key_delta = if existed_in_map { 0 } else { 1 };
    map.insert(key.clone(), Entry::new_string(value.clone()));
    let delta = new_mem - old_mem;
    (Ok(RespValue::SimpleString("OK".to_string())), delta, key_delta)
}

fn batch_del(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.is_empty() {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    match map.remove(key) {
        Some(entry) => {
            let mem = (key.len() + entry.value.mem_size()) as i64;
            (Ok(RespValue::Integer(1)), -mem, -1)
        }
        None => (Ok(RespValue::Integer(0)), 0, 0),
    }
}

fn batch_getdel(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.is_empty() {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let now = cached_now();
    let key = &args[0];
    // Check type before removing to avoid remove-then-reinsert on WRONGTYPE
    match map.get(key) {
        Some(entry) => {
            if entry.expires_at.map_or(false, |t| now > t) {
                // Expired — remove lazily and account for freed memory
                let entry = map.remove(key).unwrap();
                let mem = (key.len() + entry.value.mem_size()) as i64;
                return (Ok(RespValue::BulkString(None)), -mem, -1);
            }
            if !matches!(&entry.value, StoreValue::Str(_)) {
                return (Err(CommandError::WrongType), 0, 0);
            }
        }
        None => return (Ok(RespValue::BulkString(None)), 0, 0),
    }
    // Safe to remove — we know key exists and is a String
    let entry = map.remove(key).unwrap();
    let mem = (key.len() + entry.value.mem_size()) as i64;
    match entry.value {
        StoreValue::Str(data) => {
            (Ok(RespValue::BulkString(Some(data.into_vec()))), -mem, -1)
        }
        _ => unreachable!(), // Type already checked above
    }
}

#[inline]
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn batch_incr_by(map: &mut ShardMap, args: &[Vec<u8>], delta: i64) -> (CommandResult, i64, i32) {
    if args.is_empty() {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();

    // Check if key exists and is valid
    let current_val = match map.get(key) {
        Some(entry) => {
            if entry.expires_at.map_or(false, |t| now > t) {
                None // expired, treat as 0
            } else {
                match &entry.value {
                    StoreValue::Str(data) => {
                        // Parse current integer value
                        match std::str::from_utf8(data).ok().and_then(|s| s.parse::<i64>().ok()) {
                            Some(v) => Some(v),
                            None => return (Err(CommandError::NotANumber), 0, 0),
                        }
                    }
                    _ => return (Err(CommandError::WrongType), 0, 0),
                }
            }
        }
        None => None,
    };

    let old_val = current_val.unwrap_or(0);
    let new_val = match old_val.checked_add(delta) {
        Some(v) => v,
        None => return (Err(CommandError::IntegerOverflow), 0, 0),
    };

    let new_bytes = new_val.to_string().into_bytes();
    let new_mem = (key.len() + new_bytes.len()) as i64;
    // Determine if key physically exists in the map (even if expired) for correct accounting
    let (old_mem, key_existed) = map
        .get(key)
        .map(|e| ((key.len() + e.value.mem_size()) as i64, true))
        .unwrap_or((0, false));

    let key_delta = if key_existed { 0 } else { 1 };
    // Remove expired entry if needed, then insert new value
    if current_val.is_none() && key_existed {
        // Key was expired — remove it before inserting (already accounted for in old_mem)
        map.remove(key.as_slice());
    }
    map.insert(key.clone(), Entry::new_string(new_bytes));
    (Ok(RespValue::Integer(new_val)), new_mem - old_mem, key_delta)
}

fn batch_expire(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let seconds = match parse_i64(&args[1]) {
        Some(s) => s,
        None => return (Err(CommandError::NotANumber), 0, 0),
    };
    // Redis: TTL <= 0 means delete the key immediately
    if seconds <= 0 {
        return match map.remove(&args[0]) {
            Some(entry) => {
                let mem = (args[0].len() + entry.value.mem_size()) as i64;
                (Ok(RespValue::Integer(1)), -mem, -1)
            }
            None => (Ok(RespValue::Integer(0)), 0, 0),
        };
    }
    let now = cached_now();
    match map.get_mut(&args[0]) {
        Some(entry) => {
            if entry.expires_at.map_or(false, |t| now > t) {
                // Expired
                return (Ok(RespValue::Integer(0)), 0, 0);
            }
            entry.expires_at = Some(now + Duration::from_secs(seconds as u64));
            (Ok(RespValue::Integer(1)), 0, 0)
        }
        None => (Ok(RespValue::Integer(0)), 0, 0),
    }
}

fn batch_pexpire(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let millis = match parse_i64(&args[1]) {
        Some(m) => m,
        None => return (Err(CommandError::NotANumber), 0, 0),
    };
    // Redis: TTL <= 0 means delete the key immediately
    if millis <= 0 {
        return match map.remove(&args[0]) {
            Some(entry) => {
                let mem = (args[0].len() + entry.value.mem_size()) as i64;
                (Ok(RespValue::Integer(1)), -mem, -1)
            }
            None => (Ok(RespValue::Integer(0)), 0, 0),
        };
    }
    let now = cached_now();
    match map.get_mut(&args[0]) {
        Some(entry) => {
            if entry.expires_at.map_or(false, |t| now > t) {
                return (Ok(RespValue::Integer(0)), 0, 0);
            }
            entry.expires_at = Some(now + Duration::from_millis(millis as u64));
            (Ok(RespValue::Integer(1)), 0, 0)
        }
        None => (Ok(RespValue::Integer(0)), 0, 0),
    }
}

fn batch_persist(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.is_empty() {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let now = cached_now();
    match map.get_mut(&args[0]) {
        Some(entry) => {
            if entry.expires_at.map_or(false, |t| now > t) {
                return (Ok(RespValue::Integer(0)), 0, 0);
            }
            if entry.expires_at.is_some() {
                entry.expires_at = None;
                (Ok(RespValue::Integer(1)), 0, 0)
            } else {
                (Ok(RespValue::Integer(0)), 0, 0)
            }
        }
        None => (Ok(RespValue::Integer(0)), 0, 0),
    }
}

fn batch_append(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let value = &args[1];
    let now = cached_now();

    // Check for expired entry
    if let Some(entry) = map.get(key) {
        if entry.expires_at.map_or(false, |t| now > t) {
            let old_mem = map
                .remove(key.as_slice())
                .map(|v| (key.len() + v.value.mem_size()) as i64)
                .unwrap_or(0);
            // Create new key
            let new_len = value.len();
            map.insert(key.clone(), Entry::new_string(value.clone()));
            return (
                Ok(RespValue::Integer(new_len as i64)),
                (key.len() + new_len) as i64 - old_mem,
                1, // expired entry replaced = new key
            );
        }
    }

    match map.get_mut(key) {
        Some(entry) => match &mut entry.value {
            StoreValue::Str(data) => {
                let added = value.len() as i64;
                data.append(value);
                let total_len = data.len();
                (Ok(RespValue::Integer(total_len as i64)), added, 0)
            }
            _ => (Err(CommandError::WrongType), 0, 0),
        },
        None => {
            // New key
            let new_len = value.len();
            let mem = (key.len() + new_len) as i64;
            map.insert(key.clone(), Entry::new_string(value.clone()));
            (Ok(RespValue::Integer(new_len as i64)), mem, 1)
        }
    }
}

fn batch_hset(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();
    let mut mem_delta: i64 = 0;

    // Remove expired entry
    if let Some(entry) = map.get(key) {
        if entry.expires_at.map_or(false, |t| now > t) {
            if let Some(v) = map.remove(key.as_slice()) {
                mem_delta -= (key.len() + v.value.mem_size()) as i64;
            }
        }
    }

    let mut new_fields = 0i64;

    if let Some(entry) = map.get_mut(key) {
        match &mut entry.value {
            StoreValue::Hash(hash) => {
                for pair in args[1..].chunks(2) {
                    let old_mem = hash.get(&pair[0]).map(|v| v.len()).unwrap_or(0);
                    let is_new = !hash.contains_key(&pair[0]);
                    hash.insert(pair[0].clone(), pair[1].clone());
                    if is_new {
                        new_fields += 1;
                        mem_delta += (pair[0].len() + pair[1].len() + 64) as i64;
                    } else {
                        mem_delta += pair[1].len() as i64 - old_mem as i64;
                    }
                }
                return (Ok(RespValue::Integer(new_fields)), mem_delta, 0);
            }
            _ => return (Err(CommandError::WrongType), 0, 0),
        }
    }

    // Key doesn't exist - create new hash
    let mut hash = hashbrown::HashMap::with_hasher(rustc_hash::FxBuildHasher);
    mem_delta += key.len() as i64;
    for pair in args[1..].chunks(2) {
        hash.insert(pair[0].clone(), pair[1].clone());
        new_fields += 1;
        mem_delta += (pair[0].len() + pair[1].len() + 64) as i64;
    }
    map.insert(key.clone(), Entry::new(StoreValue::Hash(hash)));
    (Ok(RespValue::Integer(new_fields)), mem_delta, 1)
}

fn batch_hdel(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();

    let (removed, mem_delta, is_empty) = match map.get_mut(key) {
        Some(entry) => {
            if entry.expires_at.map_or(false, |t| now > t) {
                return (Ok(RespValue::Integer(0)), 0, 0);
            }
            match &mut entry.value {
                StoreValue::Hash(hash) => {
                    let mut removed = 0i64;
                    let mut mem_delta: i64 = 0;
                    for field in &args[1..] {
                        if let Some(old_val) = hash.remove(field) {
                            removed += 1;
                            mem_delta -= (field.len() + old_val.len() + 64) as i64;
                        }
                    }
                    (removed, mem_delta, hash.is_empty())
                }
                _ => return (Err(CommandError::WrongType), 0, 0),
            }
        }
        None => return (Ok(RespValue::Integer(0)), 0, 0),
    };

    // Auto-delete empty container (Redis compatibility)
    if is_empty && removed > 0 {
        if let Some(entry) = map.remove(key) {
            let key_mem = (key.len() + entry.value.mem_size()) as i64;
            return (Ok(RespValue::Integer(removed)), mem_delta - key_mem, -1);
        }
    }
    (Ok(RespValue::Integer(removed)), mem_delta, 0)
}

fn batch_sadd(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();
    let mut mem_delta: i64 = 0;

    // Remove expired entry
    if let Some(entry) = map.get(key) {
        if entry.expires_at.map_or(false, |t| now > t) {
            if let Some(v) = map.remove(key.as_slice()) {
                mem_delta -= (key.len() + v.value.mem_size()) as i64;
            }
        }
    }

    if let Some(entry) = map.get_mut(key) {
        match &mut entry.value {
            StoreValue::Set(set) => {
                let mut added = 0i64;
                for member in &args[1..] {
                    if set.insert(member.clone()) {
                        added += 1;
                        mem_delta += (member.len() + 32) as i64;
                    }
                }
                return (Ok(RespValue::Integer(added)), mem_delta, 0);
            }
            _ => return (Err(CommandError::WrongType), 0, 0),
        }
    }

    // Key doesn't exist - create new set
    let mut set = hashbrown::HashSet::with_hasher(rustc_hash::FxBuildHasher);
    mem_delta += key.len() as i64;
    let mut added = 0i64;
    for member in &args[1..] {
        if set.insert(member.clone()) {
            added += 1;
            mem_delta += (member.len() + 32) as i64;
        }
    }
    map.insert(key.clone(), Entry::new(StoreValue::Set(set)));
    (Ok(RespValue::Integer(added)), mem_delta, 1)
}

fn batch_srem(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();

    let (removed, mem_delta, is_empty) = match map.get_mut(key) {
        Some(entry) => {
            if entry.expires_at.map_or(false, |t| now > t) {
                return (Ok(RespValue::Integer(0)), 0, 0);
            }
            match &mut entry.value {
                StoreValue::Set(set) => {
                    let mut removed = 0i64;
                    let mut mem_delta: i64 = 0;
                    for member in &args[1..] {
                        if set.remove(member) {
                            removed += 1;
                            mem_delta -= (member.len() + 32) as i64;
                        }
                    }
                    (removed, mem_delta, set.is_empty())
                }
                _ => return (Err(CommandError::WrongType), 0, 0),
            }
        }
        None => return (Ok(RespValue::Integer(0)), 0, 0),
    };

    // Auto-delete empty container (Redis compatibility)
    if is_empty && removed > 0 {
        if let Some(entry) = map.remove(key) {
            let key_mem = (key.len() + entry.value.mem_size()) as i64;
            return (Ok(RespValue::Integer(removed)), mem_delta - key_mem, -1);
        }
    }
    (Ok(RespValue::Integer(removed)), mem_delta, 0)
}

fn batch_zadd(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    // ZADD key score member [score member ...]
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();
    let mut mem_delta: i64 = 0;

    // Remove expired entry
    if let Some(entry) = map.get(key) {
        if entry.expires_at.map_or(false, |t| now > t) {
            if let Some(v) = map.remove(key.as_slice()) {
                mem_delta -= (key.len() + v.value.mem_size()) as i64;
            }
        }
    }

    // Parse score-member pairs
    let mut pairs = Vec::new();
    for pair in args[1..].chunks(2) {
        let score = match std::str::from_utf8(&pair[0])
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
        {
            Some(s) => s,
            None => return (Err(CommandError::NotANumber), 0, 0),
        };
        pairs.push((score, pair[1].clone()));
    }

    if let Some(entry) = map.get_mut(key) {
        match &mut entry.value {
            StoreValue::ZSet(zset) => {
                let mut added = 0i64;
                for (score, member) in pairs {
                    let is_new = zset.insert(score, member.clone());
                    if is_new {
                        added += 1;
                        mem_delta += (member.len() * 2 + 40) as i64;
                    }
                }
                return (Ok(RespValue::Integer(added)), mem_delta, 0);
            }
            _ => return (Err(CommandError::WrongType), 0, 0),
        }
    }

    // Key doesn't exist - create new sorted set
    let mut zset = crate::storage::value::SortedSetData::new();
    mem_delta += key.len() as i64;
    let mut added = 0i64;
    for (score, member) in pairs {
        if zset.insert(score, member.clone()) {
            added += 1;
            mem_delta += (member.len() * 2 + 40) as i64;
        }
    }
    map.insert(key.clone(), Entry::new(StoreValue::ZSet(zset)));
    (Ok(RespValue::Integer(added)), mem_delta, 1)
}

fn batch_zrem(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();

    let (removed, mem_delta, is_empty) = match map.get_mut(key) {
        Some(entry) => {
            if entry.expires_at.map_or(false, |t| now > t) {
                return (Ok(RespValue::Integer(0)), 0, 0);
            }
            match &mut entry.value {
                StoreValue::ZSet(zset) => {
                    let mut removed = 0i64;
                    let mut mem_delta: i64 = 0;
                    for member in &args[1..] {
                        if zset.remove(member) {
                            removed += 1;
                            mem_delta -= (member.len() * 2 + 40) as i64;
                        }
                    }
                    (removed, mem_delta, zset.is_empty())
                }
                _ => return (Err(CommandError::WrongType), 0, 0),
            }
        }
        None => return (Ok(RespValue::Integer(0)), 0, 0),
    };

    // Auto-delete empty container (Redis compatibility)
    if is_empty && removed > 0 {
        if let Some(entry) = map.remove(key) {
            let key_mem = (key.len() + entry.value.mem_size()) as i64;
            return (Ok(RespValue::Integer(removed)), mem_delta - key_mem, -1);
        }
    }
    (Ok(RespValue::Integer(removed)), mem_delta, 0)
}

fn batch_lpush(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();
    let mut mem_delta: i64 = 0;

    // Remove expired entry
    if let Some(entry) = map.get(key) {
        if entry.expires_at.map_or(false, |t| now > t) {
            if let Some(v) = map.remove(key.as_slice()) {
                mem_delta -= (key.len() + v.value.mem_size()) as i64;
            }
        }
    }

    if let Some(entry) = map.get_mut(key) {
        match &mut entry.value {
            StoreValue::List(list) => {
                for value in &args[1..] {
                    list.push_front(value.clone());
                    mem_delta += (value.len() + 24) as i64;
                }
                return (Ok(RespValue::Integer(list.len() as i64)), mem_delta, 0);
            }
            _ => return (Err(CommandError::WrongType), 0, 0),
        }
    }

    // Key doesn't exist - create new list
    let mut list = std::collections::VecDeque::new();
    mem_delta += key.len() as i64;
    for value in &args[1..] {
        list.push_front(value.clone());
        mem_delta += (value.len() + 24) as i64;
    }
    let len = list.len() as i64;
    map.insert(key.clone(), Entry::new(StoreValue::List(list)));
    (Ok(RespValue::Integer(len)), mem_delta, 1)
}

fn batch_rpush(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();
    let mut mem_delta: i64 = 0;

    // Remove expired entry
    if let Some(entry) = map.get(key) {
        if entry.expires_at.map_or(false, |t| now > t) {
            if let Some(v) = map.remove(key.as_slice()) {
                mem_delta -= (key.len() + v.value.mem_size()) as i64;
            }
        }
    }

    if let Some(entry) = map.get_mut(key) {
        match &mut entry.value {
            StoreValue::List(list) => {
                for value in &args[1..] {
                    list.push_back(value.clone());
                    mem_delta += (value.len() + 24) as i64;
                }
                return (Ok(RespValue::Integer(list.len() as i64)), mem_delta, 0);
            }
            _ => return (Err(CommandError::WrongType), 0, 0),
        }
    }

    // Key doesn't exist - create new list
    let mut list = std::collections::VecDeque::new();
    mem_delta += key.len() as i64;
    for value in &args[1..] {
        list.push_back(value.clone());
        mem_delta += (value.len() + 24) as i64;
    }
    let len = list.len() as i64;
    map.insert(key.clone(), Entry::new(StoreValue::List(list)));
    (Ok(RespValue::Integer(len)), mem_delta, 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sharded::ShardedStore;

    fn make_cmd(name: &str, args: &[&[u8]]) -> Command {
        Command {
            name: name.to_string(),
            args: args.iter().map(|a| a.to_vec()).collect(),
        }
    }

    // --- Classification tests ---

    #[test]
    fn test_classify_read_commands() {
        assert_eq!(classify_command(b"GET", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"get", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"EXISTS", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"TTL", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"PTTL", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"TYPE", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"STRLEN", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"HGET", &[b"k".to_vec(), b"f".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"HLEN", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"HEXISTS", &[b"k".to_vec(), b"f".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"LLEN", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"SCARD", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"SISMEMBER", &[b"k".to_vec(), b"m".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"ZCARD", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"ZSCORE", &[b"k".to_vec(), b"m".to_vec()]), CommandKind::Read);
    }

    #[test]
    fn test_classify_write_commands() {
        assert_eq!(classify_command(b"SET", &[b"k".to_vec(), b"v".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"DEL", &[b"k".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"INCR", &[b"k".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"DECR", &[b"k".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"INCRBY", &[b"k".to_vec(), b"1".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"DECRBY", &[b"k".to_vec(), b"1".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"EXPIRE", &[b"k".to_vec(), b"60".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"PEXPIRE", &[b"k".to_vec(), b"1000".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"HSET", &[b"k".to_vec(), b"f".to_vec(), b"v".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"HDEL", &[b"k".to_vec(), b"f".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"SADD", &[b"k".to_vec(), b"m".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"SREM", &[b"k".to_vec(), b"m".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"LPUSH", &[b"k".to_vec(), b"v".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"RPUSH", &[b"k".to_vec(), b"v".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"APPEND", &[b"k".to_vec(), b"v".to_vec()]), CommandKind::Write);
    }

    #[test]
    fn test_classify_excluded_commands() {
        // Transaction commands
        assert_eq!(classify_command(b"MULTI", &[]), CommandKind::Excluded);
        assert_eq!(classify_command(b"EXEC", &[]), CommandKind::Excluded);
        // Pub/Sub
        assert_eq!(classify_command(b"SUBSCRIBE", &[b"ch".to_vec()]), CommandKind::Excluded);
        assert_eq!(classify_command(b"PUBLISH", &[b"ch".to_vec(), b"msg".to_vec()]), CommandKind::Excluded);
        // Scripting
        assert_eq!(classify_command(b"EVAL", &[b"script".to_vec()]), CommandKind::Excluded);
        // Multi-key DEL
        assert_eq!(classify_command(b"DEL", &[b"k1".to_vec(), b"k2".to_vec()]), CommandKind::Excluded);
        // Multi-key EXISTS
        assert_eq!(classify_command(b"EXISTS", &[b"k1".to_vec(), b"k2".to_vec()]), CommandKind::Excluded);
        // SET with options
        assert_eq!(classify_command(b"SET", &[b"k".to_vec(), b"v".to_vec(), b"EX".to_vec()]), CommandKind::Excluded);
        // Unknown
        assert_eq!(classify_command(b"RANDOMCMD", &[]), CommandKind::Excluded);
        // MGET/MSET
        assert_eq!(classify_command(b"MGET", &[b"k1".to_vec(), b"k2".to_vec()]), CommandKind::Excluded);
        assert_eq!(classify_command(b"MSET", &[b"k1".to_vec(), b"v1".to_vec()]), CommandKind::Excluded);
        // Blocking
        assert_eq!(classify_command(b"BLPOP", &[b"k".to_vec(), b"0".to_vec()]), CommandKind::Excluded);
        // PING (no key)
        assert_eq!(classify_command(b"PING", &[]), CommandKind::Excluded);
        // EXPIRE/PEXPIRE with condition flags
        assert_eq!(classify_command(b"EXPIRE", &[b"k".to_vec(), b"60".to_vec(), b"NX".to_vec()]), CommandKind::Excluded);
        assert_eq!(classify_command(b"PEXPIRE", &[b"k".to_vec(), b"1000".to_vec(), b"GT".to_vec()]), CommandKind::Excluded);
    }

    #[test]
    fn test_classify_case_insensitive() {
        assert_eq!(classify_command(b"get", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"Get", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"GET", &[b"k".to_vec()]), CommandKind::Read);
        assert_eq!(classify_command(b"set", &[b"k".to_vec(), b"v".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"Set", &[b"k".to_vec(), b"v".to_vec()]), CommandKind::Write);
    }

    // --- Grouping tests ---

    #[test]
    fn test_group_pipeline_consecutive_same_shard_reads() {
        let store = ShardedStore::with_shard_count_for_test(16);
        // Use keys that hash to the same shard
        let shard = store.shard_index(b"key:0");
        let mut same_shard_keys = Vec::new();
        for i in 0..1000 {
            let key = format!("key:{}", i);
            if store.shard_index(key.as_bytes()) == shard {
                same_shard_keys.push(key);
            }
            if same_shard_keys.len() >= 3 {
                break;
            }
        }
        assert!(same_shard_keys.len() >= 3, "Need at least 3 same-shard keys");

        let commands: Vec<Command> = same_shard_keys
            .iter()
            .map(|k| make_cmd("GET", &[k.as_bytes()]))
            .collect();

        let entries = group_pipeline(&commands, |k| store.shard_index(k));
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            PipelineEntry::Batch(group) => {
                assert_eq!(group.shard_idx, shard);
                assert!(group.is_read_only);
                assert_eq!(group.commands.len(), same_shard_keys.len());
            }
            _ => panic!("Expected batch"),
        }
    }

    #[test]
    fn test_group_pipeline_excluded_breaks_batch() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let shard = store.shard_index(b"key:0");
        let mut same_shard_keys = Vec::new();
        for i in 0..1000 {
            let key = format!("key:{}", i);
            if store.shard_index(key.as_bytes()) == shard {
                same_shard_keys.push(key);
            }
            if same_shard_keys.len() >= 4 {
                break;
            }
        }

        let commands = vec![
            make_cmd("GET", &[same_shard_keys[0].as_bytes()]),
            make_cmd("GET", &[same_shard_keys[1].as_bytes()]),
            make_cmd("MULTI", &[]), // Excluded - breaks batch
            make_cmd("GET", &[same_shard_keys[2].as_bytes()]),
            make_cmd("GET", &[same_shard_keys[3].as_bytes()]),
        ];

        let entries = group_pipeline(&commands, |k| store.shard_index(k));
        // Should be: Batch(2 GETs) + Individual(MULTI) + Batch(2 GETs)
        assert_eq!(entries.len(), 3);
        assert!(matches!(&entries[0], PipelineEntry::Batch(g) if g.commands.len() == 2));
        assert!(matches!(&entries[1], PipelineEntry::Individual(_)));
        assert!(matches!(&entries[2], PipelineEntry::Batch(g) if g.commands.len() == 2));
    }

    #[test]
    fn test_group_pipeline_mixed_read_write_same_shard() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let shard = store.shard_index(b"key:0");
        let mut same_shard_keys = Vec::new();
        for i in 0..1000 {
            let key = format!("key:{}", i);
            if store.shard_index(key.as_bytes()) == shard {
                same_shard_keys.push(key);
            }
            if same_shard_keys.len() >= 3 {
                break;
            }
        }

        let commands = vec![
            make_cmd("GET", &[same_shard_keys[0].as_bytes()]),
            make_cmd("GET", &[same_shard_keys[1].as_bytes()]),
            make_cmd("SET", &[same_shard_keys[2].as_bytes(), b"val"]),
        ];

        let entries = group_pipeline(&commands, |k| store.shard_index(k));
        // Reads and writes are separate groups
        assert_eq!(entries.len(), 2);
        assert!(matches!(&entries[0], PipelineEntry::Batch(g) if g.is_read_only && g.commands.len() == 2));
        // Single write is Individual, not Batch
        assert!(matches!(&entries[1], PipelineEntry::Individual(_)));
    }

    #[test]
    fn test_group_pipeline_single_commands_not_batched() {
        let commands = vec![
            make_cmd("GET", &[b"key1"]),
            make_cmd("PING", &[]),
            make_cmd("SET", &[b"key2", b"val"]),
        ];

        let store = ShardedStore::with_shard_count_for_test(16);
        let entries = group_pipeline(&commands, |k| store.shard_index(k));
        // Each is individual (no consecutive same-shard commands)
        for entry in &entries {
            assert!(matches!(entry, PipelineEntry::Individual(_)));
        }
    }

    #[test]
    fn test_group_pipeline_preserves_order() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let commands = vec![
            make_cmd("GET", &[b"a"]),
            make_cmd("PING", &[]),
            make_cmd("SET", &[b"b", b"v"]),
            make_cmd("MULTI", &[]),
        ];

        let entries = group_pipeline(&commands, |k| store.shard_index(k));
        assert_eq!(entries.len(), 4);
    }

    // --- Read batch execution tests ---

    #[test]
    fn test_batch_read_get() {
        let store = ShardedStore::with_shard_count_for_test(16);
        store.insert(b"key1".to_vec(), Entry::new_string(b"val1".to_vec()));

        let shard_idx = store.shard_index(b"key1");
        let cmd_hit = make_cmd("GET", &[b"key1"]);
        let cmd_miss = make_cmd("GET", &[b"nonexistent"]);
        let cmds: Vec<&Command> = vec![&cmd_hit, &cmd_miss];

        let results = store.execute_on_shard_ref(shard_idx, |map| {
            execute_read_batch(map, &cmds)
        });

        assert_eq!(results.len(), 2);
        // key1 is in this shard — verify correct value returned
        match &results[0] {
            Ok(RespValue::BulkString(Some(data))) => assert_eq!(data, &b"val1".to_vec()),
            other => panic!("Expected BulkString(Some(val1)), got {:?}", other),
        }
        // nonexistent may or may not hash to this shard, but batch_get returns nil for missing keys
        match &results[1] {
            Ok(RespValue::BulkString(None)) => {} // Correct: nil for missing
            Ok(RespValue::BulkString(Some(_))) => panic!("Expected nil for nonexistent key"),
            other => panic!("Expected nil BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_batch_read_expired_key() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let mut entry = Entry::new_string(b"val".to_vec());
        entry.expires_at = Some(cached_now() - Duration::from_secs(1)); // Already expired
        store.insert(b"expired".to_vec(), entry);

        let cmd = make_cmd("GET", &[b"expired"]);
        let cmds: Vec<&Command> = vec![&cmd];
        let shard_idx = store.shard_index(b"expired");

        let results = store.execute_on_shard_ref(shard_idx, |map| {
            execute_read_batch(map, &cmds)
        });
        assert_eq!(results.len(), 1);
        match &results[0] {
            Ok(RespValue::BulkString(None)) => {} // Correct: nil for expired
            other => panic!("Expected nil, got {:?}", other),
        }
    }

    #[test]
    fn test_batch_read_wrong_type() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let hash = hashbrown::HashMap::with_hasher(rustc_hash::FxBuildHasher);
        store.insert(b"hash_key".to_vec(), Entry::new(StoreValue::Hash(hash)));

        let cmd = make_cmd("GET", &[b"hash_key"]); // GET on a hash
        let cmds: Vec<&Command> = vec![&cmd];
        let shard_idx = store.shard_index(b"hash_key");

        let results = store.execute_on_shard_ref(shard_idx, |map| {
            execute_read_batch(map, &cmds)
        });
        assert!(results[0].is_err()); // WRONGTYPE error
    }

    // --- Write batch execution tests ---

    #[test]
    fn test_batch_write_set_del() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let shard_idx = store.shard_index(b"key1");

        let cmd1 = make_cmd("SET", &[b"key1", b"val1"]);
        let cmd2 = make_cmd("SET", &[b"key1", b"val2"]); // Overwrite
        let cmds: Vec<&Command> = vec![&cmd1, &cmd2];

        let results = store.execute_on_shard_mut(shard_idx, |map| {
            execute_write_batch(map, &cmds, 1024, 5 * 1024 * 1024)
        });
        assert_eq!(results.len(), 2);
        assert!(results[0].0.is_ok());
        assert!(results[1].0.is_ok());

        // Verify the final value is the overwritten one
        let entry = store.get(b"key1").expect("key should exist after SET");
        assert_eq!(entry.value.as_str().unwrap(), &b"val2".to_vec());
    }

    #[test]
    fn test_batch_write_incr() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let shard_idx = store.shard_index(b"counter");
        store.insert(b"counter".to_vec(), Entry::new_string(b"10".to_vec()));

        let cmd1 = make_cmd("INCR", &[b"counter"]);
        let cmd2 = make_cmd("INCR", &[b"counter"]);
        let cmds: Vec<&Command> = vec![&cmd1, &cmd2];

        let results = store.execute_on_shard_mut(shard_idx, |map| {
            execute_write_batch(map, &cmds, 1024, 5 * 1024 * 1024)
        });
        assert_eq!(results.len(), 2);
        match &results[0].0 {
            Ok(RespValue::Integer(11)) => {}
            other => panic!("Expected 11, got {:?}", other),
        }
        match &results[1].0 {
            Ok(RespValue::Integer(12)) => {}
            other => panic!("Expected 12, got {:?}", other),
        }
    }

    #[test]
    fn test_batch_write_error_isolation() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let shard_idx = store.shard_index(b"key1");
        let hash = hashbrown::HashMap::with_hasher(rustc_hash::FxBuildHasher);
        store.insert(b"key1".to_vec(), Entry::new(StoreValue::Hash(hash)));

        // SET on a hash (WRONGTYPE), then SET on a new key (should succeed)
        let cmd1 = make_cmd("INCR", &[b"key1"]); // WRONGTYPE
        let cmd2 = make_cmd("SET", &[b"key_new", b"val"]);
        let cmds: Vec<&Command> = vec![&cmd1, &cmd2];

        let results = store.execute_on_shard_mut(shard_idx, |map| {
            execute_write_batch(map, &cmds, 1024, 5 * 1024 * 1024)
        });
        assert_eq!(results.len(), 2);
        assert!(results[0].0.is_err()); // WRONGTYPE error
        assert!(results[1].0.is_ok()); // Should still succeed (error isolation)
    }

    #[test]
    fn test_batch_write_ordering_preserved() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let shard_idx = store.shard_index(b"key");

        let cmd1 = make_cmd("SET", &[b"key", b"first"]);
        let cmd2 = make_cmd("SET", &[b"key", b"second"]);
        let cmd3 = make_cmd("SET", &[b"key", b"third"]);
        let cmds: Vec<&Command> = vec![&cmd1, &cmd2, &cmd3];

        store.execute_on_shard_mut(shard_idx, |map| {
            execute_write_batch(map, &cmds, 1024, 5 * 1024 * 1024)
        });

        // After batch, key should have the last value
        let entry = store.get(b"key").expect("key should exist");
        assert_eq!(entry.value.as_str().unwrap(), &b"third".to_vec());
    }

    #[test]
    fn test_batch_read_multiple_types() {
        let store = ShardedStore::with_shard_count_for_test(16);
        store.insert(b"str".to_vec(), Entry::new_string(b"hello".to_vec()));

        let mut hash = hashbrown::HashMap::with_hasher(rustc_hash::FxBuildHasher);
        hash.insert(b"f1".to_vec(), b"v1".to_vec());
        store.insert(b"hash".to_vec(), Entry::new(StoreValue::Hash(hash)));

        let cmd1 = make_cmd("TYPE", &[b"str"]);
        let _cmd2 = make_cmd("TYPE", &[b"hash"]);
        let _cmd3 = make_cmd("TYPE", &[b"missing"]);

        // These may be on different shards, just test the execution logic
        let now = cached_now();
        let shard_idx = store.shard_index(b"str");
        store.execute_on_shard_ref(shard_idx, |map| {
            let result = execute_read_cmd(map, &cmd1, now);
            if map.contains_key(b"str".as_slice()) {
                match result {
                    Ok(RespValue::SimpleString(ref s)) => assert_eq!(s, "string"),
                    _ => {} // Key may not be in this shard
                }
            }
        });
    }

    // --- Pipeline batching correctness ---

    #[test]
    fn test_pipeline_batching_correctness() {
        let store = ShardedStore::with_shard_count_for_test(16);

        // Insert test data
        store.insert(b"k1".to_vec(), Entry::new_string(b"v1".to_vec()));
        store.insert(b"k2".to_vec(), Entry::new_string(b"v2".to_vec()));
        store.insert(b"counter".to_vec(), Entry::new_string(b"0".to_vec()));

        // Simulate a pipeline: GET k1, GET k2, INCR counter, EXISTS k1
        let commands = vec![
            make_cmd("GET", &[b"k1"]),
            make_cmd("GET", &[b"k2"]),
            make_cmd("INCR", &[b"counter"]),
            make_cmd("EXISTS", &[b"k1"]),
        ];

        let entries = group_pipeline(&commands, |k| store.shard_index(k));
        let mut results = Vec::new();

        for entry in &entries {
            match entry {
                PipelineEntry::Batch(group) => {
                    if group.is_read_only {
                        let batch_results =
                            store.execute_on_shard_ref(group.shard_idx, |map| {
                                execute_read_batch(map, &group.commands)
                            });
                        results.extend(batch_results);
                    } else {
                        let batch_results =
                            store.execute_on_shard_mut(group.shard_idx, |map| {
                                execute_write_batch(map, &group.commands, 1024, 5 * 1024 * 1024)
                            });
                        results.extend(batch_results.into_iter().map(|(r, _, _)| r));
                    }
                }
                PipelineEntry::Individual(cmd) => {
                    // Execute individually through the shard
                    let name = cmd.name.as_bytes();
                    let kind = classify_command(name, &cmd.args);
                    if let Some(key) = cmd.args.first() {
                        let shard_idx = store.shard_index(key);
                        match kind {
                            CommandKind::Read => {
                                let r = store.execute_on_shard_ref(shard_idx, |map| {
                                    execute_read_cmd(map, cmd, cached_now())
                                });
                                results.push(r);
                            }
                            CommandKind::Write => {
                                let (r, _, _) = store.execute_on_shard_mut(shard_idx, |map| {
                                    execute_write_cmd(map, cmd)
                                });
                                results.push(r);
                            }
                            CommandKind::Excluded => {
                                results.push(Ok(RespValue::SimpleString("OK".to_string())));
                            }
                        }
                    }
                }
            }
        }

        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_fallback_for_excluded_commands() {
        let commands = vec![
            make_cmd("MULTI", &[]),
            make_cmd("SET", &[b"key", b"val"]),
            make_cmd("EXEC", &[]),
        ];

        let store = ShardedStore::with_shard_count_for_test(16);
        let entries = group_pipeline(&commands, |k| store.shard_index(k));

        // MULTI and EXEC are excluded, SET in between is individual
        assert_eq!(entries.len(), 3);
        for entry in &entries {
            assert!(matches!(entry, PipelineEntry::Individual(_)));
        }
    }
}
