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
use crate::storage::value::{CompactValue, StoreValue};
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

/// A sorted pipeline entry with original index tracking for response reordering.
/// Used by `group_pipeline_sorted` to map batch results back to original command order.
pub struct SortedPipelineEntry<'a> {
    pub entry: PipelineEntry<'a>,
    /// Original indices of the commands in this entry, in order.
    /// For Batch entries, maps `commands[i]` to `original_indices[i]`.
    /// For Individual entries, has exactly one element.
    pub original_indices: Vec<usize>,
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
            if name.len() == 5 && cmd_eq(name, b"SCARD") {
                return CommandKind::Read;
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
/// Superseded by `group_pipeline_sorted` which sorts by shard for better batching.
#[allow(dead_code)]
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

#[allow(dead_code)]
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

/// Annotated command for sorted pipeline grouping.
struct AnnotatedCommand<'a> {
    cmd: &'a Command,
    original_index: usize,
    shard_idx: usize,
    is_read: bool,
}

/// Group a pipeline of commands using sort-then-group strategy for maximum batching.
///
/// Commands are stable-sorted by shard_index (preserving relative order within each shard),
/// then grouped into consecutive same-shard, same-kind batches. Excluded commands act as
/// ordering barriers that prevent reordering across them.
///
/// Returns entries with original index mapping for response reordering.
pub fn group_pipeline_sorted<'a>(
    commands: &'a [Command],
    shard_index_fn: impl Fn(&[u8]) -> usize,
) -> Vec<SortedPipelineEntry<'a>> {
    let mut entries: Vec<SortedPipelineEntry<'a>> = Vec::with_capacity(commands.len());
    let mut segment: Vec<AnnotatedCommand<'a>> = Vec::new();

    for (i, cmd) in commands.iter().enumerate() {
        let key = extract_primary_key(cmd);

        match key {
            Some(key) => {
                let shard_idx = shard_index_fn(key);
                let is_read = classify_command(cmd.name.as_bytes(), &cmd.args) == CommandKind::Read;
                segment.push(AnnotatedCommand {
                    cmd,
                    original_index: i,
                    shard_idx,
                    is_read,
                });
            }
            None => {
                // Excluded command - flush segment then add as individual barrier
                flush_sorted_segment(&mut entries, &mut segment);
                entries.push(SortedPipelineEntry {
                    entry: PipelineEntry::Individual(cmd),
                    original_indices: vec![i],
                });
            }
        }
    }

    // Flush remaining segment
    flush_sorted_segment(&mut entries, &mut segment);

    entries
}

/// Flush a segment of batchable commands: stable-sort by shard, group consecutive
/// same-shard/same-kind entries into batches.
fn flush_sorted_segment<'a>(
    entries: &mut Vec<SortedPipelineEntry<'a>>,
    segment: &mut Vec<AnnotatedCommand<'a>>,
) {
    if segment.is_empty() {
        return;
    }

    // Stable sort by shard_idx only - preserves relative order within each shard
    segment.sort_by_key(|a| a.shard_idx);

    // Group consecutive same-shard, same-kind entries into batches
    let mut i = 0;
    while i < segment.len() {
        let start = i;
        let shard = segment[i].shard_idx;
        let is_read = segment[i].is_read;
        i += 1;
        while i < segment.len() && segment[i].shard_idx == shard && segment[i].is_read == is_read {
            i += 1;
        }

        let batch_len = i - start;
        if batch_len == 1 {
            // Single command - dispatch individually
            entries.push(SortedPipelineEntry {
                entry: PipelineEntry::Individual(segment[start].cmd),
                original_indices: vec![segment[start].original_index],
            });
        } else {
            // Multiple commands - form batch
            let commands: Vec<&'a Command> = segment[start..i].iter().map(|a| a.cmd).collect();
            let indices: Vec<usize> = segment[start..i].iter().map(|a| a.original_index).collect();
            entries.push(SortedPipelineEntry {
                entry: PipelineEntry::Batch(BatchGroup {
                    shard_idx: shard,
                    is_read_only: is_read,
                    commands,
                }),
                original_indices: indices,
            });
        }
    }

    segment.clear();
}

/// Execute a batch of read commands under a single shard read lock.
/// Returns one CommandResult per command in order.
pub fn execute_read_batch(map: &ShardMap, commands: &[&Command]) -> (Vec<CommandResult>, Vec<Vec<u8>>) {
    let now = cached_now();
    let mut expired_keys: Vec<Vec<u8>> = Vec::new();
    let mut results = Vec::with_capacity(commands.len());
    for cmd in commands {
        results.push(execute_read_cmd(map, cmd, now));
        // Track expired keys for deferred lazy-expire cleanup.
        // The read batch holds a read lock so it can't remove entries;
        // callers clean up after releasing the lock.
        if let Some(key) = cmd.args.first()
            && map.get(key.as_slice()).is_some_and(|e| e.expires_at.is_some_and(|t| now > t))
        {
            expired_keys.push(key.clone());
        }
    }
    (results, expired_keys)
}

/// Execute a batch of write commands under a single shard write lock.
/// Returns one (CommandResult, mem_delta, key_delta) per command in order.
/// Estimate the additional memory a write batch will require.
/// Used for pre-batch eviction to proactively free space before acquiring
/// the shard lock. Only counts non-exempt (allocating) commands.
///
/// Commands with insufficient arguments are skipped because they will be
/// rejected during execution, producing no mutation. This prevents spurious
/// eviction from pipelines of malformed commands.
pub fn estimate_write_batch_memory(commands: &[&Command]) -> usize {
    let mut total: usize = 0;
    for cmd in commands {
        let name = cmd.name.as_bytes();
        if is_memory_exempt_command(name) {
            continue;
        }
        // Skip commands that will fail arg-count validation — they produce
        // no mutation, so charging memory for them would cause spurious eviction.
        if !has_valid_write_args(name, cmd.args.len()) {
            continue;
        }
        // INCR/DECR family: in-place numeric updates. Worst case is creating
        // a new key with a small integer string. Use CompactValue::mem_for_data_len
        // for the correct inline/heap size (32 bytes on 64-bit), plus per-entry
        // overhead for potential new key creation.
        // Also validate that INCRBY/DECRBY have a parseable numeric argument;
        // non-numeric args will fail at execution without mutating anything.
        if is_incr_decr(name) {
            if let Some(key) = cmd.args.first() {
                // INCRBY/DECRBY: skip if the increment arg is not a valid integer
                let is_by_variant = cmd.args.len() == 2;
                if is_by_variant && parse_i64(&cmd.args[1]).is_none() {
                    continue;
                }
                // i64::MIN is at most 20 chars, always fits in CompactValue inline
                total += key.len() + CompactValue::mem_for_data_len(20) + 64;
            }
            continue;
        }
        // Default: sum all argument bytes as a conservative estimate
        // (key + value for SET, key + field + value for HSET, etc.)
        for arg in &cmd.args {
            total += arg.len();
        }
        // Per-entry overhead (hash slot, Entry struct, expiration metadata)
        total += 64;
    }
    total
}

/// Estimate memory for a single command, used by the per-command budget check
/// inside execute_write_batch to reject commands that would individually exceed
/// the remaining memory budget (matching standalone maybe_evict(required_size) semantics).
#[inline]
fn estimate_single_cmd_memory(cmd: &Command) -> usize {
    let name = cmd.name.as_bytes();
    // Only estimate memory for SET — the only batched write command where the
    // standalone path passes actual data size to maybe_evict(). All other
    // commands either call check_write_memory(0) or skip memory checks entirely,
    // so returning 0 here matches their standalone semantics and prevents
    // batched commands from being OOM-rejected when standalone would succeed.
    if !(name.len() == 3 && cmd_eq(name, b"SET")) {
        return 0;
    }
    if cmd.args.len() != 2 {
        return 0;
    }
    // Match the standalone SET path: required_size = key.len() + value.mem_size()
    // where value.mem_size() accounts for CompactValue inline/heap storage.
    cmd.args[0].len() + CompactValue::mem_for_data_len(cmd.args[1].len())
}

/// Check if a command has the correct argument count to execute successfully.
/// Exact-arity commands use `==`, variable-arity commands use `>=`.
/// Commands that fail this check will return `WrongNumberOfArguments`.
#[inline]
fn has_valid_write_args(name: &[u8], arg_count: usize) -> bool {
    match name.first().map(|b| b.to_ascii_uppercase()) {
        Some(b'A') if cmd_eq(name, b"APPEND") => arg_count == 2,
        Some(b'D') => {
            if cmd_eq(name, b"DECR") {
                arg_count == 1
            } else if cmd_eq(name, b"DECRBY") {
                arg_count == 2
            } else {
                true
            }
        }
        Some(b'H') if cmd_eq(name, b"HSET") => arg_count >= 3 && (arg_count - 1).is_multiple_of(2),
        Some(b'I') => {
            if cmd_eq(name, b"INCR") {
                arg_count == 1
            } else if cmd_eq(name, b"INCRBY") {
                arg_count == 2
            } else {
                true
            }
        }
        Some(b'L') if cmd_eq(name, b"LPUSH") => arg_count >= 2,
        Some(b'R') if cmd_eq(name, b"RPUSH") => arg_count >= 2,
        Some(b'S') => {
            if cmd_eq(name, b"SET") {
                arg_count == 2
            } else if cmd_eq(name, b"SADD") {
                arg_count >= 2
            } else {
                true
            }
        }
        _ => true,
    }
}

/// Returns true for INCR/DECR family commands (used by estimate_write_batch_memory
/// for numeric-specific worst-case estimation).
#[inline]
fn is_incr_decr(name: &[u8]) -> bool {
    match name.first().map(|b| b.to_ascii_uppercase()) {
        Some(b'D') => cmd_eq(name, b"DECR") || cmd_eq(name, b"DECRBY"),
        Some(b'I') => cmd_eq(name, b"INCR") || cmd_eq(name, b"INCRBY"),
        _ => false,
    }
}

/// Returns true for commands whose standalone paths do not call
/// check_write_memory at all: INCR/DECR family (in-place numeric updates),
/// HSET (hash_set direct), and APPEND (atomic_append direct).
/// These must bypass the batch memory gate to match standalone behavior.
#[inline]
fn skips_standalone_memcheck(name: &[u8]) -> bool {
    match name.first().map(|b| b.to_ascii_uppercase()) {
        Some(b'A') => cmd_eq(name, b"APPEND"),
        Some(b'D') => cmd_eq(name, b"DECR") || cmd_eq(name, b"DECRBY"),
        Some(b'H') => cmd_eq(name, b"HSET") || cmd_eq(name, b"HSETNX"),
        Some(b'I') => cmd_eq(name, b"INCR") || cmd_eq(name, b"INCRBY"),
        _ => false,
    }
}

/// key_delta: +1 = key created, -1 = key deleted, 0 = no change.
///
/// `memory_budget`: optional remaining bytes before maxmemory is hit.
/// When set, the batch tracks accumulated memory growth and returns OOM
/// errors for commands that would exceed the budget.
pub fn execute_write_batch(
    map: &mut ShardMap,
    commands: &[&Command],
    max_key_size: usize,
    max_value_size: usize,
    memory_budget: Option<usize>,
) -> Vec<(CommandResult, i64, i32)> {
    let mut results = Vec::with_capacity(commands.len());
    let mut accumulated_mem: i64 = 0;

    for cmd in commands {
        // Per-command memory budget check: if accumulated allocations have reached
        // or exceeded the remaining budget, reject allocating commands with OOM.
        // Memory-exempt commands (DEL, HDEL, SREM, ZREM, GETDEL, EXPIRE, PEXPIRE,
        // PERSIST) are always allowed, matching Redis semantics where memory-freeing
        // or metadata-only commands run even when over maxmemory.
        if let Some(budget) = memory_budget {
            let cmd_name = cmd.name.as_bytes();
            // Memory-exempt commands (DEL, HDEL, SREM, etc.) always run.
            // Commands that skip standalone memcheck (INCR, DECR, HSET,
            // APPEND, etc.) always run — their standalone paths don't call
            // check_write_memory at all.
            if !is_memory_exempt_command(cmd_name) && !skips_standalone_memcheck(cmd_name) {
                // Estimate this command's memory need and check whether
                // accumulated growth plus the estimate would exceed the
                // remaining budget, matching the standalone maybe_evict()
                // which uses strict > (current + size > max_memory).
                // This means a command that exactly fits the budget is allowed,
                // consistent with standalone behavior.
                let cmd_estimate = estimate_single_cmd_memory(cmd) as i64;
                if accumulated_mem + cmd_estimate > budget as i64 {
                    results.push((Err(CommandError::StorageError(
                        "OOM command not allowed when used memory > 'maxmemory'.".to_string(),
                    )), 0, 0));
                    continue;
                }
            }
        }

        // Validate key size for commands that have a key arg
        if let Some(key) = cmd.args.first()
            && key.len() > max_key_size
        {
            results.push((Err(CommandError::InvalidArgument(format!(
                "key size {} exceeds maximum {}",
                key.len(),
                max_key_size
            ))), 0, 0));
            continue;
        }
        // Validate value size for all data-bearing arguments
        let name = cmd.name.as_bytes();
        if cmd.args.len() >= 2 {
            let is_set = name.len() == 3 && cmd_eq(name, b"SET");
            let is_append = name.len() == 6 && cmd_eq(name, b"APPEND");
            if (is_set || is_append) && cmd.args[1].len() > max_value_size {
                results.push((Err(CommandError::InvalidArgument(format!(
                    "value size {} exceeds maximum {}",
                    cmd.args[1].len(),
                    max_value_size
                ))), 0, 0));
                continue;
            }
            // HSET: validate field values (args[2], args[4], ...)
            let is_hset = name.len() == 4 && cmd_eq(name, b"HSET");
            if is_hset {
                let mut oversized = false;
                for pair in cmd.args[1..].chunks(2) {
                    if pair.iter().any(|v| v.len() > max_value_size) {
                        oversized = true;
                        break;
                    }
                }
                if oversized {
                    results.push((Err(CommandError::InvalidArgument(
                        "hash field or value exceeds maximum size".to_string(),
                    )), 0, 0));
                    continue;
                }
            }
            // SADD: validate members (args[1..])
            let is_sadd = name.len() == 4 && cmd_eq(name, b"SADD");
            // LPUSH/RPUSH: validate values (args[1..])
            let is_lpush = name.len() == 5 && cmd_eq(name, b"LPUSH");
            let is_rpush = name.len() == 5 && cmd_eq(name, b"RPUSH");
            if (is_sadd || is_lpush || is_rpush)
                && cmd.args[1..].iter().any(|v| v.len() > max_value_size)
            {
                results.push((Err(CommandError::InvalidArgument(
                    "value size exceeds maximum".to_string(),
                )), 0, 0));
                continue;
            }
        }
        let (result, mem_delta, key_delta) = execute_write_cmd(map, cmd);
        accumulated_mem += mem_delta;
        results.push((result, mem_delta, key_delta));
    }
    results
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
        if entry.expires_at.is_some_and(|t| now > t) {
            None // expired
        } else {
            Some(entry)
        }
    })
}

fn batch_get(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.len() != 1 {
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
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let exists = get_valid_entry(map, &args[0], now).is_some();
    Ok(RespValue::Integer(if exists { 1 } else { 0 }))
}

fn batch_ttl(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match entry.expires_at {
            Some(expires_at) => {
                let remaining = expires_at.saturating_duration_since(now);
                let secs = remaining.as_secs() as i64;
                // Match standalone TTL: clamp to 1 when there's a positive
                // sub-second remainder (Redis never returns 0 for a live key).
                Ok(RespValue::Integer(if secs > 0 { secs } else { 1 }))
            }
            None => Ok(RespValue::Integer(-1)),
        },
        None => Ok(RespValue::Integer(-2)),
    }
}

fn batch_pttl(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    match get_valid_entry(map, &args[0], now) {
        Some(entry) => match entry.expires_at {
            Some(expires_at) => {
                let remaining = expires_at.saturating_duration_since(now);
                let millis = remaining.as_millis() as i64;
                // Match standalone PTTL: clamp to 1 when there's a positive
                // sub-millisecond remainder (Redis never returns 0 for a live key).
                Ok(RespValue::Integer(if millis > 0 { millis } else { 1 }))
            }
            None => Ok(RespValue::Integer(-1)),
        },
        None => Ok(RespValue::Integer(-2)),
    }
}

fn batch_type(map: &ShardMap, args: &[Vec<u8>], now: Instant) -> CommandResult {
    if args.len() != 1 {
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
    if args.len() != 1 {
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
    if args.len() != 2 {
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
    if args.len() != 1 {
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
    if args.len() != 2 {
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
    if args.len() != 1 {
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
    if args.len() != 1 {
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
    if args.len() != 2 {
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
    if args.len() != 1 {
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
    if args.len() != 2 {
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

/// Returns true for commands that are memory-neutral or only free memory.
/// These are allowed even when over maxmemory, matching Redis semantics:
/// - DEL, GETDEL, HDEL, SREM, ZREM: freeing commands
/// - EXPIRE, PEXPIRE, PERSIST: metadata-only (modify expiration, no allocation)
#[inline]
fn is_memory_exempt_command(name: &[u8]) -> bool {
    match name.first().map(|b| b.to_ascii_uppercase()) {
        Some(b'D') => cmd_eq(name, b"DEL"),
        Some(b'E') => cmd_eq(name, b"EXPIRE"),
        Some(b'G') => cmd_eq(name, b"GETDEL"),
        Some(b'H') => cmd_eq(name, b"HDEL"),
        Some(b'P') => cmd_eq(name, b"PEXPIRE") || cmd_eq(name, b"PERSIST"),
        Some(b'S') => cmd_eq(name, b"SREM"),
        Some(b'Z') => cmd_eq(name, b"ZREM"),
        _ => false,
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
                if cmd.args.len() != 1 {
                    return (Err(CommandError::WrongNumberOfArguments), 0, 0);
                }
                batch_incr_by(map, &cmd.args, -1)
            } else if cmd_eq(name, b"DECRBY") {
                if cmd.args.len() != 2 {
                    return (Err(CommandError::WrongNumberOfArguments), 0, 0);
                }
                match parse_i64(&cmd.args[1]) {
                    Some(delta) => match delta.checked_neg() {
                        Some(neg) => batch_incr_by(map, &cmd.args, neg),
                        None => (Err(CommandError::InvalidArgument(
                            "value is not an integer or out of range".to_string(),
                        )), 0, 0),
                    },
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
                if cmd.args.len() != 1 {
                    return (Err(CommandError::WrongNumberOfArguments), 0, 0);
                }
                batch_incr_by(map, &cmd.args, 1)
            } else if cmd_eq(name, b"INCRBY") {
                if cmd.args.len() != 2 {
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
            if cmd_eq(name, b"ZREM") {
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
    if args.len() != 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let value = &args[1];
    let new_mem = (key.len() + CompactValue::mem_for_data_len(value.len())) as i64;

    let (old_mem, physically_exists) = map
        .get(key)
        .map(|e| {
            let mem = (key.len() + e.value.mem_size()) as i64;
            (mem, true)
        })
        .unwrap_or((0, false));

    // Use physical existence for key_delta to match non-batch set_value path
    // (ShardEntry::Occupied always yields is_new_key=false regardless of expiry)
    let key_delta = if physically_exists { 0 } else { 1 };
    map.insert(key.clone(), Entry::new_string(value.clone()));
    let delta = new_mem - old_mem;
    (Ok(RespValue::SimpleString("OK".to_string())), delta, key_delta)
}

fn batch_del(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() != 1 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    // Remove the key regardless of expiry status, matching the non-batch
    // del() path in engine.rs which does active_db().remove(key) without
    // checking expiry. This ensures consistent client-facing results
    // whether a DEL is batched or not.
    match map.remove(key) {
        Some(entry) => {
            let mem = (key.len() + entry.value.mem_size()) as i64;
            (Ok(RespValue::Integer(1)), -mem, -1)
        }
        None => (Ok(RespValue::Integer(0)), 0, 0),
    }
}

fn batch_getdel(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() != 1 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let now = cached_now();
    let key = &args[0];
    // Check type before removing to avoid remove-then-reinsert on WRONGTYPE
    match map.get(key) {
        Some(entry) => {
            if entry.expires_at.is_some_and(|t| now > t) {
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

    // Check if key exists, capture value + memory in a single lookup
    let (current_val, old_mem, physically_exists) = match map.get(key) {
        Some(entry) => {
            let mem = (key.len() + entry.value.mem_size()) as i64;
            if entry.expires_at.is_some_and(|t| now > t) {
                (None, mem, true) // expired, treat as 0
            } else {
                match &entry.value {
                    StoreValue::Str(data) => {
                        match std::str::from_utf8(data).ok().and_then(|s| s.parse::<i64>().ok()) {
                            Some(v) => (Some(v), mem, true),
                            None => return (Err(CommandError::NotANumber), 0, 0),
                        }
                    }
                    _ => return (Err(CommandError::WrongType), 0, 0),
                }
            }
        }
        None => (None, 0, false),
    };

    let old_val = current_val.unwrap_or(0);
    let new_val = match old_val.checked_add(delta) {
        Some(v) => v,
        None => return (Err(CommandError::IntegerOverflow), 0, 0),
    };

    let new_bytes = new_val.to_string().into_bytes();
    let new_mem = (key.len() + CompactValue::mem_for_data_len(new_bytes.len())) as i64;

    let key_delta = if physically_exists { 0 } else { 1 };

    if current_val.is_some() && physically_exists {
        // Key exists and is not expired — modify in place to preserve TTL
        let entry = map.get_mut(key).unwrap();
        let new_cv: CompactValue = new_bytes.into();
        entry.cached_mem_size = new_cv.mem_size() as u64;
        entry.value = StoreValue::Str(new_cv);
        entry.touch();
    } else {
        // Expired or new key — remove stale entry if needed, then insert fresh
        if current_val.is_none() && physically_exists {
            map.remove(key.as_slice());
        }
        map.insert(key.clone(), Entry::new_string(new_bytes));
    }
    (Ok(RespValue::Integer(new_val)), new_mem - old_mem, key_delta)
}

fn batch_expire(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() != 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let seconds = match parse_i64(&args[1]) {
        Some(s) => s,
        None => return (Err(CommandError::NotANumber), 0, 0),
    };
    let now = cached_now();
    // Redis: TTL <= 0 means delete the key immediately
    if seconds <= 0 {
        return match map.get(&args[0]) {
            Some(entry) if entry.expires_at.is_some_and(|t| now > t) => {
                // Already expired — lazily remove the stale entry
                let entry = map.remove(&args[0]).unwrap();
                let mem = (args[0].len() + entry.value.mem_size()) as i64;
                (Ok(RespValue::Integer(0)), -mem, -1)
            }
            Some(_) => {
                let entry = map.remove(&args[0]).unwrap();
                let mem = (args[0].len() + entry.value.mem_size()) as i64;
                (Ok(RespValue::Integer(1)), -mem, -1)
            }
            None => (Ok(RespValue::Integer(0)), 0, 0),
        };
    }
    match map.get_mut(&args[0]) {
        Some(entry) => {
            if entry.expires_at.is_some_and(|t| now > t) {
                // Expired — lazily remove the entry
                let entry = map.remove(&args[0]).unwrap();
                let mem = (args[0].len() + entry.value.mem_size()) as i64;
                return (Ok(RespValue::Integer(0)), -mem, -1);
            }
            entry.expires_at = Some(now + Duration::from_secs(seconds as u64));
            (Ok(RespValue::Integer(1)), 0, 0)
        }
        None => (Ok(RespValue::Integer(0)), 0, 0),
    }
}

fn batch_pexpire(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() != 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let millis = match parse_i64(&args[1]) {
        Some(m) => m,
        None => return (Err(CommandError::NotANumber), 0, 0),
    };
    let now = cached_now();
    // Redis: TTL <= 0 means delete the key immediately
    if millis <= 0 {
        return match map.get(&args[0]) {
            Some(entry) if entry.expires_at.is_some_and(|t| now > t) => {
                // Already expired — lazily remove the stale entry
                let entry = map.remove(&args[0]).unwrap();
                let mem = (args[0].len() + entry.value.mem_size()) as i64;
                (Ok(RespValue::Integer(0)), -mem, -1)
            }
            Some(_) => {
                let entry = map.remove(&args[0]).unwrap();
                let mem = (args[0].len() + entry.value.mem_size()) as i64;
                (Ok(RespValue::Integer(1)), -mem, -1)
            }
            None => (Ok(RespValue::Integer(0)), 0, 0),
        };
    }
    match map.get_mut(&args[0]) {
        Some(entry) => {
            if entry.expires_at.is_some_and(|t| now > t) {
                // Expired — lazily remove the entry
                let entry = map.remove(&args[0]).unwrap();
                let mem = (args[0].len() + entry.value.mem_size()) as i64;
                return (Ok(RespValue::Integer(0)), -mem, -1);
            }
            entry.expires_at = Some(now + Duration::from_millis(millis as u64));
            (Ok(RespValue::Integer(1)), 0, 0)
        }
        None => (Ok(RespValue::Integer(0)), 0, 0),
    }
}

fn batch_persist(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() != 1 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let now = cached_now();
    match map.get_mut(&args[0]) {
        Some(entry) => {
            if entry.expires_at.is_some_and(|t| now > t) {
                // Expired — lazily remove the entry
                let entry = map.remove(&args[0]).unwrap();
                let mem = (args[0].len() + entry.value.mem_size()) as i64;
                return (Ok(RespValue::Integer(0)), -mem, -1);
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
    if args.len() != 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let value = &args[1];
    let now = cached_now();

    // Check for expired entry
    if let Some(entry) = map.get(key)
        && entry.expires_at.is_some_and(|t| now > t)
    {
        let old_mem = map
            .remove(key.as_slice())
            .map(|v| (key.len() + v.value.mem_size()) as i64)
            .unwrap_or(0);
        // Create new key (replacing expired = net key_delta 0)
        let new_len = value.len();
        map.insert(key.clone(), Entry::new_string(value.clone()));
        return (
            Ok(RespValue::Integer(new_len as i64)),
            (key.len() + CompactValue::mem_for_data_len(new_len)) as i64 - old_mem,
            0, // replacing expired entry: remove + create cancel out
        );
    }

    match map.get_mut(key) {
        Some(entry) => match &mut entry.value {
            StoreValue::Str(data) => {
                let old_mem = data.mem_size() as i64;
                data.append(value);
                let new_mem = data.mem_size() as i64;
                let total_len = data.len();
                let mem_delta = new_mem - old_mem;
                entry.cached_mem_size = (entry.cached_mem_size as i64 + mem_delta).max(0) as u64;
                entry.touch();
                (Ok(RespValue::Integer(total_len as i64)), mem_delta, 0)
            }
            _ => (Err(CommandError::WrongType), 0, 0),
        },
        None => {
            // New key
            let new_len = value.len();
            let mem = (key.len() + CompactValue::mem_for_data_len(new_len)) as i64;
            map.insert(key.clone(), Entry::new_string(value.clone()));
            (Ok(RespValue::Integer(new_len as i64)), mem, 1)
        }
    }
}

fn batch_hset(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();
    let mut mem_delta: i64 = 0;
    let mut replaced_expired = false;

    // Remove expired entry
    if map.get(key).is_some_and(|entry| entry.expires_at.is_some_and(|t| now > t))
        && let Some(v) = map.remove(key.as_slice())
    {
        mem_delta -= (key.len() + v.value.mem_size()) as i64;
        replaced_expired = true;
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
                entry.cached_mem_size = (entry.cached_mem_size as i64 + mem_delta).max(0) as u64;
                entry.touch();
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
    // Net key_delta is 0 when replacing an expired entry (remove + create cancel out)
    let key_delta = if replaced_expired { 0 } else { 1 };
    (Ok(RespValue::Integer(new_fields)), mem_delta, key_delta)
}

fn batch_hdel(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();

    let (removed, mem_delta, is_empty) = match map.get_mut(key) {
        Some(entry) => {
            if entry.expires_at.is_some_and(|t| now > t) {
                // Remove expired entry (lazy cleanup) and signal key removal
                if let Some(v) = map.remove(key.as_slice()) {
                    let mem = (key.len() + v.value.mem_size()) as i64;
                    return (Ok(RespValue::Integer(0)), -mem, -1);
                }
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
                    if mem_delta != 0 {
                        entry.cached_mem_size = (entry.cached_mem_size as i64 + mem_delta).max(0) as u64;
                    }
                    (removed, mem_delta, hash.is_empty())
                }
                _ => return (Err(CommandError::WrongType), 0, 0),
            }
        }
        None => return (Ok(RespValue::Integer(0)), 0, 0),
    };

    // Auto-delete empty container (Redis compatibility)
    if is_empty && removed > 0
        && let Some(entry) = map.remove(key)
    {
        let key_mem = (key.len() + entry.value.mem_size()) as i64;
        return (Ok(RespValue::Integer(removed)), mem_delta - key_mem, -1);
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
    let mut replaced_expired = false;

    // Remove expired entry
    if map.get(key).is_some_and(|entry| entry.expires_at.is_some_and(|t| now > t))
        && let Some(v) = map.remove(key.as_slice())
    {
        mem_delta -= (key.len() + v.value.mem_size()) as i64;
        replaced_expired = true;
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
                if mem_delta != 0 {
                    entry.cached_mem_size = (entry.cached_mem_size as i64 + mem_delta).max(0) as u64;
                }
                entry.touch();
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
    // Net key_delta is 0 when replacing an expired entry (remove + create cancel out)
    let key_delta = if replaced_expired { 0 } else { 1 };
    (Ok(RespValue::Integer(added)), mem_delta, key_delta)
}

fn batch_srem(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();

    let (removed, mem_delta, is_empty) = match map.get_mut(key) {
        Some(entry) => {
            if entry.expires_at.is_some_and(|t| now > t) {
                // Remove expired entry (lazy cleanup) and signal key removal
                if let Some(v) = map.remove(key.as_slice()) {
                    let mem = (key.len() + v.value.mem_size()) as i64;
                    return (Ok(RespValue::Integer(0)), -mem, -1);
                }
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
                    if mem_delta != 0 {
                        entry.cached_mem_size = (entry.cached_mem_size as i64 + mem_delta).max(0) as u64;
                    }
                    (removed, mem_delta, set.is_empty())
                }
                _ => return (Err(CommandError::WrongType), 0, 0),
            }
        }
        None => return (Ok(RespValue::Integer(0)), 0, 0),
    };

    // Auto-delete empty container (Redis compatibility)
    if is_empty && removed > 0
        && let Some(entry) = map.remove(key)
    {
        let key_mem = (key.len() + entry.value.mem_size()) as i64;
        return (Ok(RespValue::Integer(removed)), mem_delta - key_mem, -1);
    }
    (Ok(RespValue::Integer(removed)), mem_delta, 0)
}

fn batch_zrem(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();

    let (removed, mem_delta, is_empty) = match map.get_mut(key) {
        Some(entry) => {
            if entry.expires_at.is_some_and(|t| now > t) {
                // Remove expired entry (lazy cleanup) and signal key removal
                if let Some(v) = map.remove(key.as_slice()) {
                    let mem = (key.len() + v.value.mem_size()) as i64;
                    return (Ok(RespValue::Integer(0)), -mem, -1);
                }
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
                    if mem_delta != 0 {
                        entry.cached_mem_size = (entry.cached_mem_size as i64 + mem_delta).max(0) as u64;
                    }
                    (removed, mem_delta, zset.is_empty())
                }
                _ => return (Err(CommandError::WrongType), 0, 0),
            }
        }
        None => return (Ok(RespValue::Integer(0)), 0, 0),
    };

    // Auto-delete empty container (Redis compatibility)
    if is_empty && removed > 0
        && let Some(entry) = map.remove(key)
    {
        let key_mem = (key.len() + entry.value.mem_size()) as i64;
        return (Ok(RespValue::Integer(removed)), mem_delta - key_mem, -1);
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
    let mut replaced_expired = false;

    // Remove expired entry
    if map.get(key).is_some_and(|entry| entry.expires_at.is_some_and(|t| now > t))
        && let Some(v) = map.remove(key.as_slice())
    {
        mem_delta -= (key.len() + v.value.mem_size()) as i64;
        replaced_expired = true;
    }

    if let Some(entry) = map.get_mut(key) {
        match &mut entry.value {
            StoreValue::List(list) => {
                for value in &args[1..] {
                    list.push_front(value.clone());
                    mem_delta += (value.len() + 24) as i64;
                }
                let len = list.len() as i64;
                entry.cached_mem_size = (entry.cached_mem_size as i64 + mem_delta).max(0) as u64;
                entry.touch();
                return (Ok(RespValue::Integer(len)), mem_delta, 0);
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
    // Net key_delta is 0 when replacing an expired entry (remove + create cancel out)
    let key_delta = if replaced_expired { 0 } else { 1 };
    (Ok(RespValue::Integer(len)), mem_delta, key_delta)
}

fn batch_rpush(map: &mut ShardMap, args: &[Vec<u8>]) -> (CommandResult, i64, i32) {
    if args.len() < 2 {
        return (Err(CommandError::WrongNumberOfArguments), 0, 0);
    }
    let key = &args[0];
    let now = cached_now();
    let mut mem_delta: i64 = 0;
    let mut replaced_expired = false;

    // Remove expired entry
    if map.get(key).is_some_and(|entry| entry.expires_at.is_some_and(|t| now > t))
        && let Some(v) = map.remove(key.as_slice())
    {
        mem_delta -= (key.len() + v.value.mem_size()) as i64;
        replaced_expired = true;
    }

    if let Some(entry) = map.get_mut(key) {
        match &mut entry.value {
            StoreValue::List(list) => {
                for value in &args[1..] {
                    list.push_back(value.clone());
                    mem_delta += (value.len() + 24) as i64;
                }
                let len = list.len() as i64;
                entry.cached_mem_size = (entry.cached_mem_size as i64 + mem_delta).max(0) as u64;
                entry.touch();
                return (Ok(RespValue::Integer(len)), mem_delta, 0);
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
    // Net key_delta is 0 when replacing an expired entry (remove + create cancel out)
    let key_delta = if replaced_expired { 0 } else { 1 };
    (Ok(RespValue::Integer(len)), mem_delta, key_delta)
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
        assert_eq!(classify_command(b"GETDEL", &[b"k".to_vec()]), CommandKind::Write);
        assert_eq!(classify_command(b"PERSIST", &[b"k".to_vec()]), CommandKind::Write);
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

    // --- Sorted grouping tests ---

    #[test]
    fn test_sorted_group_same_shard_reads() {
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
        assert!(same_shard_keys.len() >= 3, "Need at least 3 same-shard keys");

        let commands: Vec<Command> = same_shard_keys
            .iter()
            .map(|k| make_cmd("GET", &[k.as_bytes()]))
            .collect();

        let entries = group_pipeline_sorted(&commands, |k| store.shard_index(k));
        assert_eq!(entries.len(), 1);
        match &entries[0].entry {
            PipelineEntry::Batch(group) => {
                assert_eq!(group.shard_idx, shard);
                assert!(group.is_read_only);
                assert_eq!(group.commands.len(), same_shard_keys.len());
            }
            _ => panic!("Expected batch"),
        }
        // Original indices should be 0, 1, 2
        assert_eq!(entries[0].original_indices, vec![0, 1, 2]);
    }

    #[test]
    fn test_sorted_group_excluded_breaks_batch() {
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
            make_cmd("MULTI", &[]), // Excluded - barrier
            make_cmd("GET", &[same_shard_keys[2].as_bytes()]),
            make_cmd("GET", &[same_shard_keys[3].as_bytes()]),
        ];

        let entries = group_pipeline_sorted(&commands, |k| store.shard_index(k));
        // Should be: Batch(2 GETs) + Individual(MULTI) + Batch(2 GETs)
        assert_eq!(entries.len(), 3);
        assert!(matches!(&entries[0].entry, PipelineEntry::Batch(g) if g.commands.len() == 2));
        assert_eq!(entries[0].original_indices, vec![0, 1]);
        assert!(matches!(&entries[1].entry, PipelineEntry::Individual(_)));
        assert_eq!(entries[1].original_indices, vec![2]);
        assert!(matches!(&entries[2].entry, PipelineEntry::Batch(g) if g.commands.len() == 2));
        assert_eq!(entries[2].original_indices, vec![3, 4]);
    }

    #[test]
    fn test_sorted_group_mixed_read_write_same_shard() {
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

        let entries = group_pipeline_sorted(&commands, |k| store.shard_index(k));
        // Reads and writes are separate groups within the same shard
        assert_eq!(entries.len(), 2);
        assert!(matches!(&entries[0].entry, PipelineEntry::Batch(g) if g.is_read_only && g.commands.len() == 2));
        // Single write is Individual, not Batch
        assert!(matches!(&entries[1].entry, PipelineEntry::Individual(_)));
    }

    #[test]
    fn test_sorted_group_single_commands_not_batched() {
        let commands = vec![
            make_cmd("GET", &[b"key1"]),
            make_cmd("PING", &[]),
            make_cmd("SET", &[b"key2", b"val"]),
        ];

        let store = ShardedStore::with_shard_count_for_test(16);
        let entries = group_pipeline_sorted(&commands, |k| store.shard_index(k));
        // Each is individual (PING is excluded, others may be different shards)
        for entry in &entries {
            assert!(matches!(&entry.entry, PipelineEntry::Individual(_)));
        }
    }

    #[test]
    fn test_sorted_group_original_indices_with_barriers() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let commands = vec![
            make_cmd("GET", &[b"a"]),    // 0
            make_cmd("PING", &[]),       // 1 (excluded barrier)
            make_cmd("SET", &[b"b", b"v"]), // 2
            make_cmd("MULTI", &[]),      // 3 (excluded barrier)
        ];

        let entries = group_pipeline_sorted(&commands, |k| store.shard_index(k));
        assert_eq!(entries.len(), 4);
        // Verify each entry has the correct original index
        assert_eq!(entries[0].original_indices, vec![0]);
        assert_eq!(entries[1].original_indices, vec![1]);
        assert_eq!(entries[2].original_indices, vec![2]);
        assert_eq!(entries[3].original_indices, vec![3]);
    }

    #[test]
    fn test_sorted_group_non_consecutive_same_shard_batched() {
        // This is the key improvement: non-consecutive same-shard commands
        // get sorted together and batched.
        let store = ShardedStore::with_shard_count_for_test(16);

        // Find keys on two different shards
        let shard_a = store.shard_index(b"key:0");
        let mut keys_a = Vec::new();
        let mut keys_b = Vec::new();
        for i in 0..10000 {
            let key = format!("key:{}", i);
            let shard = store.shard_index(key.as_bytes());
            if shard == shard_a && keys_a.len() < 2 {
                keys_a.push(key);
            } else if shard != shard_a && keys_b.len() < 1 {
                keys_b.push(key);
            }
            if keys_a.len() >= 2 && keys_b.len() >= 1 {
                break;
            }
        }
        assert!(keys_a.len() >= 2 && keys_b.len() >= 1);

        // Interleave: GET shard_a, GET shard_b, GET shard_a
        let commands = vec![
            make_cmd("GET", &[keys_a[0].as_bytes()]),  // shard_a, idx 0
            make_cmd("GET", &[keys_b[0].as_bytes()]),  // shard_b, idx 1
            make_cmd("GET", &[keys_a[1].as_bytes()]),  // shard_a, idx 2
        ];

        let entries = group_pipeline_sorted(&commands, |k| store.shard_index(k));

        // With sorting: the two shard_a GETs should be batched together
        // even though they were non-consecutive in the original pipeline
        let mut found_batch = false;
        for entry in &entries {
            if let PipelineEntry::Batch(g) = &entry.entry {
                if g.commands.len() == 2 {
                    found_batch = true;
                    assert_eq!(g.shard_idx, shard_a);
                    assert!(g.is_read_only);
                    // Original indices should include both shard_a commands
                    assert!(entry.original_indices.contains(&0));
                    assert!(entry.original_indices.contains(&2));
                }
            }
        }
        assert!(found_batch, "Expected shard_a commands to be batched together");

        // The shard_b command should be individual
        let shard_b_entry = entries.iter().find(|e| {
            matches!(&e.entry, PipelineEntry::Individual(cmd) if cmd.args[0] == keys_b[0].as_bytes())
        });
        assert!(shard_b_entry.is_some());
        assert_eq!(shard_b_entry.unwrap().original_indices, vec![1]);
    }

    #[test]
    fn test_sorted_group_preserves_order_within_shard() {
        // Verify that stable sort preserves relative order within the same shard
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
        assert!(same_shard_keys.len() >= 3);

        // SET, GET, SET on same shard - within shard order is preserved
        let commands = vec![
            make_cmd("SET", &[same_shard_keys[0].as_bytes(), b"v1"]),
            make_cmd("GET", &[same_shard_keys[1].as_bytes()]),
            make_cmd("SET", &[same_shard_keys[2].as_bytes(), b"v2"]),
        ];

        let entries = group_pipeline_sorted(&commands, |k| store.shard_index(k));
        // Within the same shard: SET(0), GET(1), SET(2) - relative order preserved
        // Groups: Individual(SET at 0), Individual(GET at 1), Individual(SET at 2)
        // (reads and writes alternate, each forms a group of 1)
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].original_indices[0], 0);
        assert_eq!(entries[1].original_indices[0], 1);
        assert_eq!(entries[2].original_indices[0], 2);
    }

    #[test]
    fn test_sorted_group_response_reordering_correctness() {
        // Simulate the response reordering that server.rs does with response_slots
        let store = ShardedStore::with_shard_count_for_test(16);

        // Find keys on different shards
        let shard_a = store.shard_index(b"key:0");
        let mut keys_a = vec![];
        let mut keys_b = vec![];
        for i in 0..10000 {
            let key = format!("key:{}", i);
            let shard = store.shard_index(key.as_bytes());
            if shard == shard_a && keys_a.len() < 2 {
                keys_a.push(key);
            } else if shard != shard_a && keys_b.len() < 2 {
                keys_b.push(key);
            }
            if keys_a.len() >= 2 && keys_b.len() >= 2 {
                break;
            }
        }
        assert!(keys_a.len() >= 2 && keys_b.len() >= 2);

        // Pipeline: A0, B0, A1, B1
        let commands = vec![
            make_cmd("GET", &[keys_a[0].as_bytes()]),  // 0
            make_cmd("GET", &[keys_b[0].as_bytes()]),  // 1
            make_cmd("GET", &[keys_a[1].as_bytes()]),  // 2
            make_cmd("GET", &[keys_b[1].as_bytes()]),  // 3
        ];

        let entries = group_pipeline_sorted(&commands, |k| store.shard_index(k));

        // Simulate response collection with response_slots
        let mut response_slots: Vec<String> = vec![String::new(); 4];
        for entry in &entries {
            match &entry.entry {
                PipelineEntry::Batch(g) => {
                    for (i, _cmd) in g.commands.iter().enumerate() {
                        response_slots[entry.original_indices[i]] = format!("resp_{}", entry.original_indices[i]);
                    }
                }
                PipelineEntry::Individual(_cmd) => {
                    let idx = entry.original_indices[0];
                    response_slots[idx] = format!("resp_{}", idx);
                }
            }
        }

        // After reordering, responses should be in original order
        assert_eq!(response_slots[0], "resp_0");
        assert_eq!(response_slots[1], "resp_1");
        assert_eq!(response_slots[2], "resp_2");
        assert_eq!(response_slots[3], "resp_3");
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

        let (results, _expired) = store.execute_on_shard_ref(shard_idx, |map| {
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

        let (results, expired_keys) = store.execute_on_shard_ref(shard_idx, |map| {
            execute_read_batch(map, &cmds)
        });
        assert_eq!(results.len(), 1);
        match &results[0] {
            Ok(RespValue::BulkString(None)) => {} // Correct: nil for expired
            other => panic!("Expected nil, got {:?}", other),
        }
        // Verify the expired key was tracked for deferred cleanup
        assert!(expired_keys.contains(&b"expired".to_vec()));
    }

    #[test]
    fn test_batch_read_wrong_type() {
        let store = ShardedStore::with_shard_count_for_test(16);
        let hash = hashbrown::HashMap::with_hasher(rustc_hash::FxBuildHasher);
        store.insert(b"hash_key".to_vec(), Entry::new(StoreValue::Hash(hash)));

        let cmd = make_cmd("GET", &[b"hash_key"]); // GET on a hash
        let cmds: Vec<&Command> = vec![&cmd];
        let shard_idx = store.shard_index(b"hash_key");

        let (results, _expired) = store.execute_on_shard_ref(shard_idx, |map| {
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
            execute_write_batch(map, &cmds, 1024, 5 * 1024 * 1024, None)
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
            execute_write_batch(map, &cmds, 1024, 5 * 1024 * 1024, None)
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
            execute_write_batch(map, &cmds, 1024, 5 * 1024 * 1024, None)
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
            execute_write_batch(map, &cmds, 1024, 5 * 1024 * 1024, None)
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

        // Test TYPE on string key - execute on the correct shard
        let now = cached_now();
        let str_shard = store.shard_index(b"str");
        let cmd_str = make_cmd("TYPE", &[b"str"]);
        store.execute_on_shard_ref(str_shard, |map| {
            let result = execute_read_cmd(map, &cmd_str, now);
            assert!(
                matches!(result, Ok(RespValue::SimpleString(ref s)) if s == "string"),
                "TYPE on string key should return 'string', got: {:?}",
                result
            );
        });

        // Test TYPE on hash key - execute on the correct shard
        let hash_shard = store.shard_index(b"hash");
        let cmd_hash = make_cmd("TYPE", &[b"hash"]);
        store.execute_on_shard_ref(hash_shard, |map| {
            let result = execute_read_cmd(map, &cmd_hash, now);
            assert!(
                matches!(result, Ok(RespValue::SimpleString(ref s)) if s == "hash"),
                "TYPE on hash key should return 'hash', got: {:?}",
                result
            );
        });

        // Test TYPE on missing key - execute on any shard
        let missing_shard = store.shard_index(b"missing");
        let cmd_missing = make_cmd("TYPE", &[b"missing"]);
        store.execute_on_shard_ref(missing_shard, |map| {
            let result = execute_read_cmd(map, &cmd_missing, now);
            assert!(
                matches!(result, Ok(RespValue::SimpleString(ref s)) if s == "none"),
                "TYPE on missing key should return 'none', got: {:?}",
                result
            );
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
                        let (batch_results, _expired) =
                            store.execute_on_shard_ref(group.shard_idx, |map| {
                                execute_read_batch(map, &group.commands)
                            });
                        results.extend(batch_results);
                    } else {
                        let batch_results =
                            store.execute_on_shard_mut(group.shard_idx, |map| {
                                execute_write_batch(map, &group.commands, 1024, 5 * 1024 * 1024, None)
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

        // Verify actual result values (not just count)
        // GET k1 -> BulkString("v1")
        assert!(
            matches!(&results[0], Ok(RespValue::BulkString(Some(v))) if v == b"v1"),
            "GET k1 should return 'v1', got: {:?}",
            results[0]
        );
        // GET k2 -> BulkString("v2")
        assert!(
            matches!(&results[1], Ok(RespValue::BulkString(Some(v))) if v == b"v2"),
            "GET k2 should return 'v2', got: {:?}",
            results[1]
        );
        // INCR counter -> Integer(1)
        assert!(
            matches!(&results[2], Ok(RespValue::Integer(1))),
            "INCR counter should return 1, got: {:?}",
            results[2]
        );
        // EXISTS k1 -> Integer(1)
        assert!(
            matches!(&results[3], Ok(RespValue::Integer(1))),
            "EXISTS k1 should return 1, got: {:?}",
            results[3]
        );
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

    // --- RESP3 compatibility tests ---

    #[test]
    fn test_batch_results_use_resp3_compatible_types() {
        // Verify that batch read/write results use only basic RESP types
        // (BulkString, Integer, SimpleString, Error) which are compatible
        // with both RESP2 and RESP3 protocols. This validates that removing
        // the RESP2-only restriction is safe.
        let store = ShardedStore::with_shard_count_for_test(16);
        store.insert(b"str_key".to_vec(), Entry::new_string(b"hello".to_vec()));
        let shard_idx = store.shard_index(b"str_key");

        // Read commands: GET returns BulkString, EXISTS returns Integer, TTL returns Integer
        let cmd_get = make_cmd("GET", &[b"str_key"]);
        let cmd_exists = make_cmd("EXISTS", &[b"str_key"]);
        let cmd_ttl = make_cmd("TTL", &[b"str_key"]);
        let cmds: Vec<&Command> = vec![&cmd_get, &cmd_exists, &cmd_ttl];
        let (results, _) = store.execute_on_shard_ref(shard_idx, |map| {
            execute_read_batch(map, &cmds)
        });
        assert!(matches!(&results[0], Ok(RespValue::BulkString(Some(_)))));
        assert!(matches!(&results[1], Ok(RespValue::Integer(_))));
        assert!(matches!(&results[2], Ok(RespValue::Integer(_))));

        // Write commands: SET returns SimpleString, INCR returns Integer
        let cmd_set = make_cmd("SET", &[b"str_key", b"world"]);
        let cmd_incr = make_cmd("INCR", &[b"counter"]);
        let wcmds: Vec<&Command> = vec![&cmd_set, &cmd_incr];
        let results = store.execute_on_shard_mut(shard_idx, |map| {
            execute_write_batch(map, &wcmds, 1024, 5 * 1024 * 1024, None)
        });
        assert!(matches!(&results[0].0, Ok(RespValue::SimpleString(_))));
        assert!(matches!(&results[1].0, Ok(RespValue::Integer(_))));

        // All these types serialize identically in RESP2 and RESP3
        for (result, _, _) in &results {
            if let Ok(resp) = result {
                assert!(resp.serialize().is_ok(), "Response must serialize for both protocols");
            }
        }
    }

    // --- Security/ACL batch filtering tests ---

    #[test]
    fn test_sorted_group_acl_filtering_simulation() {
        // Simulate the ACL pre-filtering that server.rs does for security-enabled
        // connections. Commands that fail ACL checks are excluded from batch execution
        // and get error responses at their original indices.
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
            make_cmd("SET", &[same_shard_keys[0].as_bytes(), b"v1"]),  // allowed
            make_cmd("SET", &[same_shard_keys[1].as_bytes(), b"v2"]),  // denied by ACL
            make_cmd("SET", &[same_shard_keys[2].as_bytes(), b"v3"]),  // allowed
        ];

        let entries = group_pipeline_sorted(&commands, |k| store.shard_index(k));

        // Simulate ACL filtering: command at index 1 is denied
        let mut response_slots: Vec<String> = vec![String::new(); 3];
        let denied_index = 1;

        for entry in &entries {
            match &entry.entry {
                PipelineEntry::Batch(g) => {
                    let mut allowed_cmds = Vec::new();
                    let mut allowed_indices = Vec::new();
                    for (i, &cmd) in g.commands.iter().enumerate() {
                        let orig_idx = entry.original_indices[i];
                        if orig_idx == denied_index {
                            // Simulate ACL denial
                            response_slots[orig_idx] = "NOPERM".to_string();
                        } else {
                            allowed_cmds.push(cmd);
                            allowed_indices.push(orig_idx);
                        }
                    }
                    // Execute only allowed commands
                    for (i, _cmd) in allowed_cmds.iter().enumerate() {
                        response_slots[allowed_indices[i]] = format!("OK_{}", allowed_indices[i]);
                    }
                }
                PipelineEntry::Individual(cmd) => {
                    let orig_idx = entry.original_indices[0];
                    if orig_idx == denied_index {
                        response_slots[orig_idx] = "NOPERM".to_string();
                    } else {
                        let _ = cmd; // used in real dispatch
                        response_slots[orig_idx] = format!("OK_{}", orig_idx);
                    }
                }
            }
        }

        // Verify: command 0 and 2 succeed, command 1 gets NOPERM
        assert_eq!(response_slots[0], "OK_0");
        assert_eq!(response_slots[1], "NOPERM");
        assert_eq!(response_slots[2], "OK_2");
    }
}
