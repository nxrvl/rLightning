use std::collections::HashMap;

use crate::command::handler::CommandHandler;
use crate::command::{Command, CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::{IN_TRANSACTION, StorageEngine};

/// Extract all keys that a command will access from its arguments.
/// Used by EXEC to collect keys for transaction-level locking.
fn extract_keys_from_command(cmd: &Command) -> Vec<Vec<u8>> {
    let cmd_lower = cmd.name.to_lowercase();
    match cmd_lower.as_str() {
        // Commands with no data keys (server, connection, admin)
        "ping" | "echo" | "info" | "auth" | "config" | "keys" | "scan" | "dbsize" | "randomkey"
        | "flushall" | "flushdb" | "monitor" | "select" | "quit" | "reset" | "client"
        | "command" | "save" | "bgsave" | "bgrewriteaof" | "lastsave" | "shutdown" | "slowlog"
        | "latency" | "memory" | "debug" | "swapdb" | "time" | "lolwut" | "cluster" | "asking"
        | "readonly" | "readwrite" | "module" | "acl" | "wait" | "waitaof" | "role"
        | "replicaof" | "slaveof" | "replconf" | "psync" | "failover" | "sentinel" | "script"
        | "function" => vec![],

        // All args are keys
        "del" | "unlink" | "exists" | "touch" => cmd.args.clone(),

        // MGET: all args are keys
        "mget" => cmd.args.clone(),

        // MSET/MSETNX: keys at even positions (key, value, key, value, ...)
        "mset" | "msetnx" => cmd.args.iter().step_by(2).cloned().collect(),

        // Two-key commands: args[0] and args[1]
        "rename" | "renamenx" | "lmove" | "rpoplpush" | "smove" | "copy" | "lcs" => {
            let mut keys = Vec::new();
            if !cmd.args.is_empty() {
                keys.push(cmd.args[0].clone());
            }
            if cmd.args.len() > 1 {
                keys.push(cmd.args[1].clone());
            }
            keys
        }

        // BLMOVE: args[0] source, args[1] dest
        "blmove" => {
            let mut keys = Vec::new();
            if !cmd.args.is_empty() {
                keys.push(cmd.args[0].clone());
            }
            if cmd.args.len() > 1 {
                keys.push(cmd.args[1].clone());
            }
            keys
        }

        // BLPOP/BRPOP: args[0..n-1] are keys, last is timeout
        "blpop" | "brpop" | "bzpopmin" | "bzpopmax" => {
            if cmd.args.len() > 1 {
                cmd.args[..cmd.args.len() - 1].to_vec()
            } else {
                vec![]
            }
        }

        // LMPOP/ZMPOP: args[0] is numkeys, args[1..1+numkeys] are keys
        "lmpop" | "zmpop" => {
            if let Some(numkeys) = cmd
                .args
                .first()
                .and_then(|a| String::from_utf8_lossy(a).parse::<usize>().ok())
            {
                cmd.args.iter().skip(1).take(numkeys).cloned().collect()
            } else {
                vec![]
            }
        }

        // BLMPOP/BZMPOP: args[0] is timeout, args[1] is numkeys, args[2..2+numkeys] are keys
        "blmpop" | "bzmpop" => {
            if let Some(numkeys) = cmd
                .args
                .get(1)
                .and_then(|a| String::from_utf8_lossy(a).parse::<usize>().ok())
            {
                cmd.args.iter().skip(2).take(numkeys).cloned().collect()
            } else {
                vec![]
            }
        }

        // SINTER/SUNION/SDIFF: all args are keys
        "sinter" | "sunion" | "sdiff" | "pfcount" => cmd.args.clone(),

        // SINTERCARD: args[0] is numkeys, args[1..1+numkeys] are keys
        "sintercard" => {
            if let Some(numkeys) = cmd
                .args
                .first()
                .and_then(|a| String::from_utf8_lossy(a).parse::<usize>().ok())
            {
                cmd.args.iter().skip(1).take(numkeys).cloned().collect()
            } else {
                vec![]
            }
        }

        // SINTERSTORE/SUNIONSTORE/SDIFFSTORE: args[0] is dest, args[1..] are source keys
        "sinterstore" | "sunionstore" | "sdiffstore" | "pfmerge" => cmd.args.clone(),

        // ZINTERSTORE/ZUNIONSTORE/ZDIFFSTORE: args[0] dest, args[1] numkeys, args[2..2+numkeys] source keys
        "zinterstore" | "zunionstore" | "zdiffstore" => {
            let mut keys = Vec::new();
            if !cmd.args.is_empty() {
                keys.push(cmd.args[0].clone());
            }
            if let Some(numkeys) = cmd
                .args
                .get(1)
                .and_then(|a| String::from_utf8_lossy(a).parse::<usize>().ok())
            {
                for k in cmd.args.iter().skip(2).take(numkeys) {
                    keys.push(k.clone());
                }
            }
            keys
        }

        // ZINTER/ZUNION/ZDIFF: args[0] numkeys, args[1..1+numkeys] source keys
        "zinter" | "zunion" | "zdiff" => {
            if let Some(numkeys) = cmd
                .args
                .first()
                .and_then(|a| String::from_utf8_lossy(a).parse::<usize>().ok())
            {
                cmd.args.iter().skip(1).take(numkeys).cloned().collect()
            } else {
                vec![]
            }
        }

        // ZRANGESTORE/GEOSEARCHSTORE: args[0] dest, args[1] source
        "zrangestore" | "geosearchstore" => {
            let mut keys = Vec::new();
            if !cmd.args.is_empty() {
                keys.push(cmd.args[0].clone());
            }
            if cmd.args.len() > 1 {
                keys.push(cmd.args[1].clone());
            }
            keys
        }

        // GEORADIUS/GEORADIUSBYMEMBER: args[0] is source key, may have STORE/STOREDIST dest key
        "georadius" | "georadius_ro" | "georadiusbymember" | "georadiusbymember_ro" => {
            let mut keys = Vec::new();
            if !cmd.args.is_empty() {
                keys.push(cmd.args[0].clone());
            }
            // Scan options for STORE/STOREDIST destination key
            for i in 1..cmd.args.len() {
                if (cmd.args[i].eq_ignore_ascii_case(b"STORE")
                    || cmd.args[i].eq_ignore_ascii_case(b"STOREDIST"))
                    && let Some(dest) = cmd.args.get(i + 1)
                {
                    keys.push(dest.clone());
                }
            }
            keys
        }

        // BITOP: args[0] is operation, args[1] is dest, args[2..] are source keys
        "bitop" => {
            if cmd.args.len() > 1 {
                cmd.args[1..].to_vec()
            } else {
                vec![]
            }
        }

        // XREAD/XREADGROUP: keys appear after "STREAMS" keyword
        "xread" | "xreadgroup" => {
            let streams_pos = cmd
                .args
                .iter()
                .position(|a| a.eq_ignore_ascii_case(b"STREAMS"));
            if let Some(pos) = streams_pos {
                let remaining = &cmd.args[pos + 1..];
                // Keys are the first half, IDs are the second half
                let num_keys = remaining.len() / 2;
                remaining.iter().take(num_keys).cloned().collect()
            } else {
                vec![]
            }
        }

        // EVAL/EVALSHA/FCALL: args[0] is script/sha/function, args[1] is numkeys, args[2..2+numkeys] are keys
        "eval" | "evalsha" | "eval_ro" | "evalsha_ro" | "fcall" | "fcall_ro" => {
            if let Some(numkeys) = cmd
                .args
                .get(1)
                .and_then(|a| String::from_utf8_lossy(a).parse::<usize>().ok())
            {
                cmd.args.iter().skip(2).take(numkeys).cloned().collect()
            } else {
                vec![]
            }
        }

        // SORT/SORT_RO: args[0] is key, may have STORE dest
        "sort" | "sort_ro" => {
            let mut keys = Vec::new();
            if !cmd.args.is_empty() {
                keys.push(cmd.args[0].clone());
            }
            // Check for STORE option
            for i in 1..cmd.args.len() {
                if cmd.args[i].eq_ignore_ascii_case(b"STORE") {
                    if let Some(dest) = cmd.args.get(i + 1) {
                        keys.push(dest.clone());
                    }
                    break;
                }
            }
            keys
        }

        // MIGRATE: args[0..2] are host/port/key-or-empty, special multi-key via KEYS option
        "migrate" => {
            let mut keys = Vec::new();
            // args[2] is the key (or empty string for multi-key)
            if cmd.args.len() > 2 && !cmd.args[2].is_empty() {
                keys.push(cmd.args[2].clone());
            }
            // Check for KEYS option
            for i in 0..cmd.args.len() {
                if cmd.args[i].eq_ignore_ascii_case(b"KEYS") {
                    for k in &cmd.args[i + 1..] {
                        keys.push(k.clone());
                    }
                    break;
                }
            }
            keys
        }

        // Default: first argument is the key (covers the vast majority of single-key commands)
        _ => {
            if cmd.args.is_empty() {
                vec![]
            } else {
                vec![cmd.args[0].clone()]
            }
        }
    }
}

/// Per-connection transaction state for MULTI/EXEC/WATCH support
#[derive(Debug)]
pub struct TransactionState {
    /// Whether we're inside a MULTI block (queuing commands)
    pub in_multi: bool,
    /// Queued commands waiting for EXEC
    pub queue: Vec<Command>,
    /// Watched keys: maps (db_index, key) -> version_at_watch.
    /// DB-scoped so that watching the same key name in different databases works correctly.
    pub watched_keys: HashMap<(usize, Vec<u8>), u64>,
    /// Whether any watched key was modified (dirty flag)
    pub dirty: bool,
    /// Whether there was a command error during queuing (EXECABORT)
    pub has_command_errors: bool,
    /// Errors from queued commands (stored for EXEC response)
    pub queued_errors: Vec<(usize, CommandError)>,
    /// Per-database flush epochs recorded at WATCH time.
    /// Only databases that have watched keys are tracked.
    pub flush_epochs_at_watch: HashMap<usize, u64>,
}

impl Default for TransactionState {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionState {
    pub fn new() -> Self {
        Self {
            in_multi: false,
            queue: Vec::new(),
            watched_keys: HashMap::new(),
            dirty: false,
            has_command_errors: false,
            queued_errors: Vec::new(),
            flush_epochs_at_watch: HashMap::new(),
        }
    }

    /// Reset all transaction state (after EXEC or DISCARD)
    pub fn reset(&mut self) {
        self.in_multi = false;
        self.queue.clear();
        self.watched_keys.clear();
        self.dirty = false;
        self.has_command_errors = false;
        self.queued_errors.clear();
        self.flush_epochs_at_watch.clear();
    }

    /// Check if any watched keys have been modified since WATCH was called
    pub fn check_watched_keys(&self, engine: &StorageEngine) -> bool {
        // Check if a FLUSH operation occurred in any watched database since WATCH
        for (&db_index, &epoch) in &self.flush_epochs_at_watch {
            if engine.get_flush_epoch_for_db(db_index) != epoch {
                return true; // A flush happened in a watched DB, invalidate
            }
        }

        // Check each watched key using its DB-scoped version
        for ((db_index, key), &watched_version) in &self.watched_keys {
            let current_version = engine.get_key_version_for_db(key, *db_index);
            if current_version != watched_version {
                return true; // Key was modified
            }
        }
        false
    }
}

/// Handle the MULTI command - start a transaction
pub fn handle_multi(state: &mut TransactionState) -> CommandResult {
    if state.in_multi {
        return Err(CommandError::InternalError(
            "MULTI calls can not be nested".to_string(),
        ));
    }

    state.in_multi = true;
    state.queue.clear();
    state.has_command_errors = false;
    state.queued_errors.clear();
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Handle the DISCARD command - abort a transaction
pub fn handle_discard(state: &mut TransactionState) -> CommandResult {
    if !state.in_multi {
        return Err(CommandError::InternalError(
            "DISCARD without MULTI".to_string(),
        ));
    }

    state.reset();
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Handle the WATCH command - watch keys for modifications
pub fn handle_watch(
    state: &mut TransactionState,
    engine: &StorageEngine,
    args: &[Vec<u8>],
    db_index: usize,
) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    if state.in_multi {
        return Err(CommandError::InternalError(
            "WATCH inside MULTI is not allowed".to_string(),
        ));
    }

    // Record flush epoch for this database (only if not already tracked)
    state
        .flush_epochs_at_watch
        .entry(db_index)
        .or_insert_with(|| engine.get_flush_epoch_for_db(db_index));

    // Record the current version of each watched key, keyed by (db_index, key)
    for key in args {
        let version = engine.get_key_version_for_db(key, db_index);
        state.watched_keys.insert((db_index, key.clone()), version);
    }

    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Handle the UNWATCH command - clear all watched keys and flush epoch tracking
pub fn handle_unwatch(state: &mut TransactionState) -> CommandResult {
    state.watched_keys.clear();
    state.flush_epochs_at_watch.clear();
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Queue a command during a MULTI block
/// Returns QUEUED response or error if the command itself is invalid
pub fn queue_command(state: &mut TransactionState, command: Command) -> CommandResult {
    // Commands that are not allowed inside MULTI
    let cmd_lower = command.name.to_lowercase();
    match cmd_lower.as_str() {
        "multi" => {
            return Err(CommandError::InternalError(
                "MULTI calls can not be nested".to_string(),
            ));
        }
        "watch" => {
            return Err(CommandError::InternalError(
                "WATCH inside MULTI is not allowed".to_string(),
            ));
        }
        // AUTH must not be queued inside MULTI because the auth handler always
        // returns OK and relies on a post-hoc check in the server networking
        // layer. That post-hoc check does not run for commands executed within
        // EXEC, so queuing AUTH would silently bypass authentication.
        "auth" => {
            return Err(CommandError::InternalError(
                "AUTH inside MULTI is not allowed".to_string(),
            ));
        }
        // Pub/sub commands change connection state and are not allowed inside MULTI
        "subscribe" | "unsubscribe" | "psubscribe" | "punsubscribe" | "ssubscribe"
        | "sunsubscribe" => {
            return Err(CommandError::InternalError(format!(
                "Command not allowed inside MULTI: {}",
                cmd_lower
            )));
        }
        // Blocking commands would block the entire EXEC and are not allowed
        "blpop" | "brpop" | "blmove" | "blmpop" | "bzpopmin" | "bzpopmax" | "bzmpop" | "wait"
        | "waitaof" => {
            return Err(CommandError::InternalError(format!(
                "Command not allowed inside MULTI: {}",
                cmd_lower
            )));
        }
        _ => {}
    }

    state.queue.push(command);
    Ok(RespValue::SimpleString("QUEUED".to_string()))
}

/// Handle the EXEC command - execute all queued commands atomically
pub async fn handle_exec(
    state: &mut TransactionState,
    handler: &CommandHandler,
    engine: &StorageEngine,
    db_index: usize,
) -> CommandResult {
    if !state.in_multi {
        return Err(CommandError::InternalError(
            "EXEC without MULTI".to_string(),
        ));
    }

    // Check if there were command errors during queuing
    if state.has_command_errors {
        state.reset();
        return Err(CommandError::InternalError(
            "Transaction discarded because of previous errors.".to_string(),
        ));
    }

    // Check if any watched keys were modified (optimistic locking)
    if state.check_watched_keys(engine) {
        state.reset();
        return Ok(RespValue::BulkString(None)); // nil = transaction aborted due to WATCH
    }

    // Collect all keys from queued commands for locking
    let commands: Vec<Command> = state.queue.drain(..).collect();
    let all_keys: Vec<Vec<u8>> = commands
        .iter()
        .flat_map(extract_keys_from_command)
        .collect();

    // Lock all keys in sorted order to prevent deadlocks and ensure isolation
    let _lock_guard = engine.lock_keys(&all_keys).await;

    // Execute all queued commands while holding locks.
    // Set IN_TRANSACTION so nested lock_keys calls (e.g. from MSETNX) are no-ops,
    // avoiding deadlocks since EXEC already holds all necessary locks.
    let results = IN_TRANSACTION
        .scope(true, async {
            let mut results = Vec::with_capacity(commands.len());
            for cmd in commands {
                match handler.process(cmd, db_index).await {
                    Ok(response) => results.push(response),
                    Err(e) => results.push(RespValue::Error(e.to_string())),
                }
            }
            results
        })
        .await;

    // Clear transaction state (locks released when _lock_guard is dropped)
    state.reset();

    Ok(RespValue::Array(Some(results)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;

    fn create_engine() -> Arc<StorageEngine> {
        StorageEngine::new(StorageConfig::default())
    }

    #[test]
    fn test_multi_starts_transaction() {
        let mut state = TransactionState::new();
        assert!(!state.in_multi);

        let result = handle_multi(&mut state).unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        assert!(state.in_multi);
    }

    #[test]
    fn test_multi_nested_error() {
        let mut state = TransactionState::new();
        handle_multi(&mut state).unwrap();

        let result = handle_multi(&mut state);
        assert!(result.is_err());
    }

    #[test]
    fn test_discard_clears_state() {
        let mut state = TransactionState::new();
        handle_multi(&mut state).unwrap();

        let cmd = Command {
            name: "SET".to_string(),
            args: vec![b"key".to_vec(), b"val".to_vec()],
        };
        queue_command(&mut state, cmd).unwrap();
        assert_eq!(state.queue.len(), 1);

        let result = handle_discard(&mut state).unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        assert!(!state.in_multi);
        assert!(state.queue.is_empty());
    }

    #[test]
    fn test_discard_without_multi_error() {
        let mut state = TransactionState::new();
        let result = handle_discard(&mut state);
        assert!(result.is_err());
    }

    #[test]
    fn test_queue_command() {
        let mut state = TransactionState::new();
        handle_multi(&mut state).unwrap();

        let cmd = Command {
            name: "SET".to_string(),
            args: vec![b"key".to_vec(), b"val".to_vec()],
        };
        let result = queue_command(&mut state, cmd).unwrap();
        assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));
        assert_eq!(state.queue.len(), 1);
    }

    #[test]
    fn test_queue_multi_nested_error() {
        let mut state = TransactionState::new();
        handle_multi(&mut state).unwrap();

        let cmd = Command {
            name: "MULTI".to_string(),
            args: vec![],
        };
        let result = queue_command(&mut state, cmd);
        assert!(result.is_err());
    }

    #[test]
    fn test_queue_watch_inside_multi_error() {
        let mut state = TransactionState::new();
        handle_multi(&mut state).unwrap();

        let cmd = Command {
            name: "WATCH".to_string(),
            args: vec![b"key".to_vec()],
        };
        let result = queue_command(&mut state, cmd);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_watch_records_versions() {
        let engine = create_engine();
        let mut state = TransactionState::new();

        let result = handle_watch(
            &mut state,
            &engine,
            &[b"key1".to_vec(), b"key2".to_vec()],
            0,
        )
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        assert_eq!(state.watched_keys.len(), 2);
        assert!(state.watched_keys.contains_key(&(0, b"key1".to_vec())));
        assert!(state.watched_keys.contains_key(&(0, b"key2".to_vec())));
    }

    #[tokio::test]
    async fn test_watch_no_args_error() {
        let engine = create_engine();
        let mut state = TransactionState::new();

        let result = handle_watch(&mut state, &engine, &[], 0);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_watch_inside_multi_error() {
        let engine = create_engine();
        let mut state = TransactionState::new();
        handle_multi(&mut state).unwrap();

        let result = handle_watch(&mut state, &engine, &[b"key1".to_vec()], 0);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unwatch_clears_watched_keys() {
        let engine = create_engine();
        let mut state = TransactionState::new();

        handle_watch(&mut state, &engine, &[b"key1".to_vec()], 0).unwrap();
        assert_eq!(state.watched_keys.len(), 1);

        let result = handle_unwatch(&mut state).unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        assert!(state.watched_keys.is_empty());
    }

    #[tokio::test]
    async fn test_exec_without_multi_error() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        let result = handle_exec(&mut state, &handler, &engine, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_exec_empty_transaction() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        handle_multi(&mut state).unwrap();
        let result = handle_exec(&mut state, &handler, &engine, 0).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
        assert!(!state.in_multi);
    }

    #[tokio::test]
    async fn test_exec_basic_transaction() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        handle_multi(&mut state).unwrap();

        // Queue SET and GET commands
        queue_command(
            &mut state,
            Command {
                name: "SET".to_string(),
                args: vec![b"mykey".to_vec(), b"myvalue".to_vec()],
            },
        )
        .unwrap();
        queue_command(
            &mut state,
            Command {
                name: "GET".to_string(),
                args: vec![b"mykey".to_vec()],
            },
        )
        .unwrap();

        let result = handle_exec(&mut state, &handler, &engine, 0).await.unwrap();
        match result {
            RespValue::Array(Some(ref results)) => {
                assert_eq!(results.len(), 2);
                assert_eq!(results[0], RespValue::SimpleString("OK".to_string()));
                assert_eq!(results[1], RespValue::BulkString(Some(b"myvalue".to_vec())));
            }
            _ => panic!("Expected Array response from EXEC"),
        }
    }

    #[tokio::test]
    async fn test_exec_with_error_in_queue() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        handle_multi(&mut state).unwrap();

        // Queue a valid SET
        queue_command(
            &mut state,
            Command {
                name: "SET".to_string(),
                args: vec![b"key".to_vec(), b"val".to_vec()],
            },
        )
        .unwrap();

        // Queue a command that will fail at execution (wrong type)
        // First set a string key
        engine
            .set(b"strkey".to_vec(), b"hello".to_vec(), None)
            .await
            .unwrap();
        queue_command(
            &mut state,
            Command {
                name: "LPUSH".to_string(),
                args: vec![b"strkey".to_vec(), b"item".to_vec()],
            },
        )
        .unwrap();

        // Queue another valid GET
        queue_command(
            &mut state,
            Command {
                name: "GET".to_string(),
                args: vec![b"key".to_vec()],
            },
        )
        .unwrap();

        let result = handle_exec(&mut state, &handler, &engine, 0).await.unwrap();
        match result {
            RespValue::Array(Some(ref results)) => {
                assert_eq!(results.len(), 3);
                // SET succeeds
                assert_eq!(results[0], RespValue::SimpleString("OK".to_string()));
                // LPUSH on string key returns error
                match &results[1] {
                    RespValue::Error(_) => {} // Expected
                    other => panic!("Expected Error, got {:?}", other),
                }
                // GET succeeds
                assert_eq!(results[2], RespValue::BulkString(Some(b"val".to_vec())));
            }
            _ => panic!("Expected Array response from EXEC"),
        }
    }

    #[tokio::test]
    async fn test_watch_detects_modification() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        // Set initial value
        engine
            .set(b"watched_key".to_vec(), b"initial".to_vec(), None)
            .await
            .unwrap();

        // WATCH the key
        handle_watch(&mut state, &engine, &[b"watched_key".to_vec()], 0).unwrap();

        // Modify the key (simulating another client)
        engine
            .set(b"watched_key".to_vec(), b"modified".to_vec(), None)
            .await
            .unwrap();

        // Start transaction
        handle_multi(&mut state).unwrap();
        queue_command(
            &mut state,
            Command {
                name: "SET".to_string(),
                args: vec![b"watched_key".to_vec(), b"new_value".to_vec()],
            },
        )
        .unwrap();

        // EXEC should return nil (transaction aborted)
        let result = handle_exec(&mut state, &handler, &engine, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // The key should still have the modified value, not the transaction value
        let val = engine.get(b"watched_key").await.unwrap().unwrap();
        assert_eq!(val, b"modified");
    }

    #[tokio::test]
    async fn test_watch_no_modification_succeeds() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        // Set initial value
        engine
            .set(b"watched_key".to_vec(), b"initial".to_vec(), None)
            .await
            .unwrap();

        // WATCH the key
        handle_watch(&mut state, &engine, &[b"watched_key".to_vec()], 0).unwrap();

        // Don't modify the key - start transaction directly
        handle_multi(&mut state).unwrap();
        queue_command(
            &mut state,
            Command {
                name: "SET".to_string(),
                args: vec![b"watched_key".to_vec(), b"new_value".to_vec()],
            },
        )
        .unwrap();

        // EXEC should succeed
        let result = handle_exec(&mut state, &handler, &engine, 0).await.unwrap();
        match result {
            RespValue::Array(Some(ref results)) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0], RespValue::SimpleString("OK".to_string()));
            }
            _ => panic!("Expected Array response, got {:?}", result),
        }

        // The key should now have the new value
        let val = engine.get(b"watched_key").await.unwrap().unwrap();
        assert_eq!(val, b"new_value");
    }

    #[tokio::test]
    async fn test_watch_multiple_keys() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        engine
            .set(b"key1".to_vec(), b"val1".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"key2".to_vec(), b"val2".to_vec(), None)
            .await
            .unwrap();

        // WATCH both keys
        handle_watch(
            &mut state,
            &engine,
            &[b"key1".to_vec(), b"key2".to_vec()],
            0,
        )
        .unwrap();

        // Modify only key2
        engine
            .set(b"key2".to_vec(), b"modified".to_vec(), None)
            .await
            .unwrap();

        // Transaction should fail because key2 was modified
        handle_multi(&mut state).unwrap();
        queue_command(
            &mut state,
            Command {
                name: "SET".to_string(),
                args: vec![b"key1".to_vec(), b"new1".to_vec()],
            },
        )
        .unwrap();

        let result = handle_exec(&mut state, &handler, &engine, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_exec_clears_state() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        handle_multi(&mut state).unwrap();
        queue_command(
            &mut state,
            Command {
                name: "PING".to_string(),
                args: vec![],
            },
        )
        .unwrap();

        handle_exec(&mut state, &handler, &engine, 0).await.unwrap();

        // State should be fully reset
        assert!(!state.in_multi);
        assert!(state.queue.is_empty());
        assert!(state.watched_keys.is_empty());
    }

    #[tokio::test]
    async fn test_discard_clears_watched_keys() {
        let engine = create_engine();
        let mut state = TransactionState::new();

        handle_watch(&mut state, &engine, &[b"key1".to_vec()], 0).unwrap();
        assert!(!state.watched_keys.is_empty());

        handle_multi(&mut state).unwrap();
        handle_discard(&mut state).unwrap();

        // DISCARD should clear watched keys too
        assert!(state.watched_keys.is_empty());
    }

    #[tokio::test]
    async fn test_transaction_with_incr() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        engine
            .set(b"counter".to_vec(), b"10".to_vec(), None)
            .await
            .unwrap();

        handle_multi(&mut state).unwrap();
        queue_command(
            &mut state,
            Command {
                name: "INCR".to_string(),
                args: vec![b"counter".to_vec()],
            },
        )
        .unwrap();
        queue_command(
            &mut state,
            Command {
                name: "INCR".to_string(),
                args: vec![b"counter".to_vec()],
            },
        )
        .unwrap();
        queue_command(
            &mut state,
            Command {
                name: "GET".to_string(),
                args: vec![b"counter".to_vec()],
            },
        )
        .unwrap();

        let result = handle_exec(&mut state, &handler, &engine, 0).await.unwrap();
        match result {
            RespValue::Array(Some(ref results)) => {
                assert_eq!(results.len(), 3);
                assert_eq!(results[0], RespValue::Integer(11));
                assert_eq!(results[1], RespValue::Integer(12));
                assert_eq!(results[2], RespValue::BulkString(Some(b"12".to_vec())));
            }
            _ => panic!("Expected Array response from EXEC"),
        }
    }

    #[tokio::test]
    async fn test_watch_on_nonexistent_key() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        // WATCH a key that doesn't exist
        handle_watch(&mut state, &engine, &[b"nonexistent".to_vec()], 0).unwrap();

        // Create the key (modification)
        engine
            .set(b"nonexistent".to_vec(), b"now_exists".to_vec(), None)
            .await
            .unwrap();

        // Transaction should fail
        handle_multi(&mut state).unwrap();
        queue_command(
            &mut state,
            Command {
                name: "GET".to_string(),
                args: vec![b"nonexistent".to_vec()],
            },
        )
        .unwrap();

        let result = handle_exec(&mut state, &handler, &engine, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_watch_del_triggers_abort() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        engine
            .set(b"mykey".to_vec(), b"myval".to_vec(), None)
            .await
            .unwrap();

        // WATCH the key
        handle_watch(&mut state, &engine, &[b"mykey".to_vec()], 0).unwrap();

        // Delete the key
        engine.del(b"mykey").await.unwrap();

        // Transaction should fail
        handle_multi(&mut state).unwrap();
        queue_command(
            &mut state,
            Command {
                name: "SET".to_string(),
                args: vec![b"mykey".to_vec(), b"newval".to_vec()],
            },
        )
        .unwrap();

        let result = handle_exec(&mut state, &handler, &engine, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_concurrent_transactions_overlapping_keys() {
        // Two transactions racing on the same counter key should produce correct final value
        let engine = create_engine();

        // Initialize counter
        engine
            .set(b"counter".to_vec(), b"0".to_vec(), None)
            .await
            .unwrap();

        let engine1 = Arc::clone(&engine);
        let engine2 = Arc::clone(&engine);

        // Run two transactions concurrently, each incrementing the counter 50 times
        let t1 = tokio::spawn(async move {
            let handler = CommandHandler::new(Arc::clone(&engine1));
            for _ in 0..50 {
                let mut state = TransactionState::new();
                handle_multi(&mut state).unwrap();
                queue_command(
                    &mut state,
                    Command {
                        name: "INCR".to_string(),
                        args: vec![b"counter".to_vec()],
                    },
                )
                .unwrap();
                let _ = handle_exec(&mut state, &handler, &engine1, 0).await;
            }
        });

        let t2 = tokio::spawn(async move {
            let handler = CommandHandler::new(Arc::clone(&engine2));
            for _ in 0..50 {
                let mut state = TransactionState::new();
                handle_multi(&mut state).unwrap();
                queue_command(
                    &mut state,
                    Command {
                        name: "INCR".to_string(),
                        args: vec![b"counter".to_vec()],
                    },
                )
                .unwrap();
                let _ = handle_exec(&mut state, &handler, &engine2, 0).await;
            }
        });

        t1.await.unwrap();
        t2.await.unwrap();

        // Final counter should be exactly 100 (no lost updates)
        let val = engine.get(b"counter").await.unwrap().unwrap();
        let counter: i64 = String::from_utf8_lossy(&val).parse().unwrap();
        assert_eq!(counter, 100);
    }

    #[tokio::test]
    async fn test_concurrent_multi_key_transactions() {
        // Two transactions operating on overlapping keys should not interleave
        let engine = create_engine();

        engine
            .set(b"a".to_vec(), b"0".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"b".to_vec(), b"0".to_vec(), None)
            .await
            .unwrap();

        let engine1 = Arc::clone(&engine);
        let engine2 = Arc::clone(&engine);

        // Transaction 1: INCR a, INCR b (20 times)
        let t1 = tokio::spawn(async move {
            let handler = CommandHandler::new(Arc::clone(&engine1));
            for _ in 0..20 {
                let mut state = TransactionState::new();
                handle_multi(&mut state).unwrap();
                queue_command(
                    &mut state,
                    Command {
                        name: "INCR".to_string(),
                        args: vec![b"a".to_vec()],
                    },
                )
                .unwrap();
                queue_command(
                    &mut state,
                    Command {
                        name: "INCR".to_string(),
                        args: vec![b"b".to_vec()],
                    },
                )
                .unwrap();
                let _ = handle_exec(&mut state, &handler, &engine1, 0).await;
            }
        });

        // Transaction 2: INCR a, INCR b (20 times)
        let t2 = tokio::spawn(async move {
            let handler = CommandHandler::new(Arc::clone(&engine2));
            for _ in 0..20 {
                let mut state = TransactionState::new();
                handle_multi(&mut state).unwrap();
                queue_command(
                    &mut state,
                    Command {
                        name: "INCR".to_string(),
                        args: vec![b"a".to_vec()],
                    },
                )
                .unwrap();
                queue_command(
                    &mut state,
                    Command {
                        name: "INCR".to_string(),
                        args: vec![b"b".to_vec()],
                    },
                )
                .unwrap();
                let _ = handle_exec(&mut state, &handler, &engine2, 0).await;
            }
        });

        t1.await.unwrap();
        t2.await.unwrap();

        // Both counters should be exactly 40
        let a_val = engine.get(b"a").await.unwrap().unwrap();
        let a: i64 = String::from_utf8_lossy(&a_val).parse().unwrap();
        let b_val = engine.get(b"b").await.unwrap().unwrap();
        let b: i64 = String::from_utf8_lossy(&b_val).parse().unwrap();
        assert_eq!(a, 40);
        assert_eq!(b, 40);
    }

    #[tokio::test]
    async fn test_watch_concurrent_modification_detected() {
        // WATCH should detect when another "client" modifies a watched key
        // between WATCH and EXEC
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));

        engine
            .set(b"balance".to_vec(), b"100".to_vec(), None)
            .await
            .unwrap();

        // Client 1: WATCH balance
        let mut state = TransactionState::new();
        handle_watch(&mut state, &engine, &[b"balance".to_vec()], 0).unwrap();

        // Client 2: modifies balance between WATCH and EXEC
        let engine2 = Arc::clone(&engine);
        let handler2 = CommandHandler::new(Arc::clone(&engine2));
        let mut state2 = TransactionState::new();
        handle_multi(&mut state2).unwrap();
        queue_command(
            &mut state2,
            Command {
                name: "SET".to_string(),
                args: vec![b"balance".to_vec(), b"50".to_vec()],
            },
        )
        .unwrap();
        let result2 = handle_exec(&mut state2, &handler2, &engine, 0)
            .await
            .unwrap();
        // Client 2's transaction should succeed
        match result2 {
            RespValue::Array(Some(ref results)) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0], RespValue::SimpleString("OK".to_string()));
            }
            _ => panic!("Expected Array response"),
        }

        // Client 1: MULTI + SET balance 200 + EXEC (should fail because balance was modified)
        handle_multi(&mut state).unwrap();
        queue_command(
            &mut state,
            Command {
                name: "SET".to_string(),
                args: vec![b"balance".to_vec(), b"200".to_vec()],
            },
        )
        .unwrap();
        let result1 = handle_exec(&mut state, &handler, &engine, 0).await.unwrap();
        // Should return nil (transaction aborted)
        assert_eq!(result1, RespValue::BulkString(None));

        // Balance should remain at 50 (client 2's value)
        let val = engine.get(b"balance").await.unwrap().unwrap();
        assert_eq!(val, b"50");
    }

    #[tokio::test]
    async fn test_discard_clears_all_state() {
        let engine = create_engine();
        let mut state = TransactionState::new();

        // WATCH some keys
        engine
            .set(b"k1".to_vec(), b"v1".to_vec(), None)
            .await
            .unwrap();
        handle_watch(&mut state, &engine, &[b"k1".to_vec(), b"k2".to_vec()], 0).unwrap();
        assert_eq!(state.watched_keys.len(), 2);

        // Start MULTI and queue commands
        handle_multi(&mut state).unwrap();
        assert!(state.in_multi);
        queue_command(
            &mut state,
            Command {
                name: "SET".to_string(),
                args: vec![b"k1".to_vec(), b"new".to_vec()],
            },
        )
        .unwrap();
        queue_command(
            &mut state,
            Command {
                name: "SET".to_string(),
                args: vec![b"k2".to_vec(), b"new2".to_vec()],
            },
        )
        .unwrap();
        assert_eq!(state.queue.len(), 2);

        // DISCARD should clear everything
        handle_discard(&mut state).unwrap();
        assert!(!state.in_multi);
        assert!(state.queue.is_empty());
        assert!(state.watched_keys.is_empty());
        assert!(!state.dirty);
        assert!(!state.has_command_errors);
        assert!(state.queued_errors.is_empty());
    }

    #[test]
    fn test_extract_keys_single_key_commands() {
        let cmd = Command {
            name: "SET".to_string(),
            args: vec![b"mykey".to_vec(), b"myval".to_vec()],
        };
        assert_eq!(extract_keys_from_command(&cmd), vec![b"mykey".to_vec()]);

        let cmd = Command {
            name: "GET".to_string(),
            args: vec![b"mykey".to_vec()],
        };
        assert_eq!(extract_keys_from_command(&cmd), vec![b"mykey".to_vec()]);

        let cmd = Command {
            name: "INCR".to_string(),
            args: vec![b"counter".to_vec()],
        };
        assert_eq!(extract_keys_from_command(&cmd), vec![b"counter".to_vec()]);
    }

    #[test]
    fn test_extract_keys_multi_key_commands() {
        let cmd = Command {
            name: "MSET".to_string(),
            args: vec![
                b"k1".to_vec(),
                b"v1".to_vec(),
                b"k2".to_vec(),
                b"v2".to_vec(),
            ],
        };
        assert_eq!(
            extract_keys_from_command(&cmd),
            vec![b"k1".to_vec(), b"k2".to_vec()]
        );

        let cmd = Command {
            name: "DEL".to_string(),
            args: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        };
        assert_eq!(
            extract_keys_from_command(&cmd),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn test_extract_keys_no_key_commands() {
        let cmd = Command {
            name: "PING".to_string(),
            args: vec![],
        };
        assert!(extract_keys_from_command(&cmd).is_empty());

        let cmd = Command {
            name: "INFO".to_string(),
            args: vec![b"server".to_vec()],
        };
        assert!(extract_keys_from_command(&cmd).is_empty());
    }

    #[test]
    fn test_extract_keys_two_key_commands() {
        let cmd = Command {
            name: "RENAME".to_string(),
            args: vec![b"old".to_vec(), b"new".to_vec()],
        };
        assert_eq!(
            extract_keys_from_command(&cmd),
            vec![b"old".to_vec(), b"new".to_vec()]
        );

        let cmd = Command {
            name: "LMOVE".to_string(),
            args: vec![
                b"src".to_vec(),
                b"dst".to_vec(),
                b"LEFT".to_vec(),
                b"RIGHT".to_vec(),
            ],
        };
        assert_eq!(
            extract_keys_from_command(&cmd),
            vec![b"src".to_vec(), b"dst".to_vec()]
        );
    }

    #[test]
    fn test_extract_keys_numkeys_commands() {
        let cmd = Command {
            name: "ZINTERSTORE".to_string(),
            args: vec![
                b"dest".to_vec(),
                b"2".to_vec(),
                b"zset1".to_vec(),
                b"zset2".to_vec(),
                b"WEIGHTS".to_vec(),
                b"1".to_vec(),
                b"2".to_vec(),
            ],
        };
        assert_eq!(
            extract_keys_from_command(&cmd),
            vec![b"dest".to_vec(), b"zset1".to_vec(), b"zset2".to_vec()]
        );
    }

    #[test]
    fn test_extract_keys_geosearchstore() {
        // GEOSEARCHSTORE dest source FROMLONLAT ...
        let cmd = Command {
            name: "GEOSEARCHSTORE".to_string(),
            args: vec![
                b"destkey".to_vec(),
                b"srckey".to_vec(),
                b"FROMLONLAT".to_vec(),
                b"2.35".to_vec(),
                b"48.86".to_vec(),
                b"BYRADIUS".to_vec(),
                b"100".to_vec(),
                b"km".to_vec(),
            ],
        };
        assert_eq!(
            extract_keys_from_command(&cmd),
            vec![b"destkey".to_vec(), b"srckey".to_vec()]
        );
    }

    #[test]
    fn test_extract_keys_georadius_with_store() {
        // GEORADIUS key lon lat radius unit STORE dest
        let cmd = Command {
            name: "GEORADIUS".to_string(),
            args: vec![
                b"mygeo".to_vec(),
                b"2.35".to_vec(),
                b"48.86".to_vec(),
                b"100".to_vec(),
                b"km".to_vec(),
                b"STORE".to_vec(),
                b"destkey".to_vec(),
            ],
        };
        assert_eq!(
            extract_keys_from_command(&cmd),
            vec![b"mygeo".to_vec(), b"destkey".to_vec()]
        );
    }

    #[test]
    fn test_extract_keys_georadius_with_storedist() {
        // GEORADIUS key lon lat radius unit STOREDIST dest
        let cmd = Command {
            name: "GEORADIUS".to_string(),
            args: vec![
                b"mygeo".to_vec(),
                b"2.35".to_vec(),
                b"48.86".to_vec(),
                b"100".to_vec(),
                b"km".to_vec(),
                b"ASC".to_vec(),
                b"STOREDIST".to_vec(),
                b"distkey".to_vec(),
            ],
        };
        assert_eq!(
            extract_keys_from_command(&cmd),
            vec![b"mygeo".to_vec(), b"distkey".to_vec()]
        );
    }

    #[test]
    fn test_extract_keys_georadius_no_store() {
        // GEORADIUS key lon lat radius unit (no STORE)
        let cmd = Command {
            name: "GEORADIUS".to_string(),
            args: vec![
                b"mygeo".to_vec(),
                b"2.35".to_vec(),
                b"48.86".to_vec(),
                b"100".to_vec(),
                b"km".to_vec(),
            ],
        };
        assert_eq!(extract_keys_from_command(&cmd), vec![b"mygeo".to_vec()]);
    }

    #[test]
    fn test_extract_keys_georadiusbymember_with_store() {
        // GEORADIUSBYMEMBER key member radius unit STORE dest
        let cmd = Command {
            name: "GEORADIUSBYMEMBER".to_string(),
            args: vec![
                b"mygeo".to_vec(),
                b"Paris".to_vec(),
                b"100".to_vec(),
                b"km".to_vec(),
                b"STORE".to_vec(),
                b"nearby".to_vec(),
            ],
        };
        assert_eq!(
            extract_keys_from_command(&cmd),
            vec![b"mygeo".to_vec(), b"nearby".to_vec()]
        );
    }
}
