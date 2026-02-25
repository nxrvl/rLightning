use std::collections::HashMap;

use crate::command::{Command, CommandError, CommandResult};
use crate::command::handler::CommandHandler;
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// Per-connection transaction state for MULTI/EXEC/WATCH support
#[derive(Debug)]
pub struct TransactionState {
    /// Whether we're inside a MULTI block (queuing commands)
    pub in_multi: bool,
    /// Queued commands waiting for EXEC
    pub queue: Vec<Command>,
    /// Watched keys and their versions at WATCH time
    pub watched_keys: HashMap<Vec<u8>, u64>,
    /// Whether any watched key was modified (dirty flag)
    pub dirty: bool,
    /// Whether there was a command error during queuing (EXECABORT)
    pub has_command_errors: bool,
    /// Errors from queued commands (stored for EXEC response)
    pub queued_errors: Vec<(usize, CommandError)>,
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
    }

    /// Check if any watched keys have been modified since WATCH was called
    pub fn check_watched_keys(&self, engine: &StorageEngine) -> bool {
        for (key, &watched_version) in &self.watched_keys {
            let current_version = engine.get_key_version(key);
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
) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    if state.in_multi {
        return Err(CommandError::InternalError(
            "WATCH inside MULTI is not allowed".to_string(),
        ));
    }

    // Record the current version of each watched key
    for key in args {
        let version = engine.get_key_version(key);
        state.watched_keys.insert(key.clone(), version);
    }

    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Handle the UNWATCH command - clear all watched keys
pub fn handle_unwatch(state: &mut TransactionState) -> CommandResult {
    state.watched_keys.clear();
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

    // Execute all queued commands and collect results
    let commands: Vec<Command> = state.queue.drain(..).collect();
    let mut results = Vec::with_capacity(commands.len());

    for cmd in commands {
        match handler.process(cmd).await {
            Ok(response) => results.push(response),
            Err(e) => results.push(RespValue::Error(e.to_string())),
        }
    }

    // Clear transaction state
    state.reset();

    Ok(RespValue::Array(Some(results)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::storage::engine::StorageConfig;

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

        let result =
            handle_watch(&mut state, &engine, &[b"key1".to_vec(), b"key2".to_vec()]).unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        assert_eq!(state.watched_keys.len(), 2);
        assert!(state.watched_keys.contains_key(&b"key1".to_vec()));
        assert!(state.watched_keys.contains_key(&b"key2".to_vec()));
    }

    #[tokio::test]
    async fn test_watch_no_args_error() {
        let engine = create_engine();
        let mut state = TransactionState::new();

        let result = handle_watch(&mut state, &engine, &[]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_watch_inside_multi_error() {
        let engine = create_engine();
        let mut state = TransactionState::new();
        handle_multi(&mut state).unwrap();

        let result = handle_watch(&mut state, &engine, &[b"key1".to_vec()]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unwatch_clears_watched_keys() {
        let engine = create_engine();
        let mut state = TransactionState::new();

        handle_watch(&mut state, &engine, &[b"key1".to_vec()]).unwrap();
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

        let result = handle_exec(&mut state, &handler, &engine).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_exec_empty_transaction() {
        let engine = create_engine();
        let handler = CommandHandler::new(Arc::clone(&engine));
        let mut state = TransactionState::new();

        handle_multi(&mut state).unwrap();
        let result = handle_exec(&mut state, &handler, &engine).await.unwrap();
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

        let result = handle_exec(&mut state, &handler, &engine).await.unwrap();
        match result {
            RespValue::Array(Some(ref results)) => {
                assert_eq!(results.len(), 2);
                assert_eq!(results[0], RespValue::SimpleString("OK".to_string()));
                assert_eq!(
                    results[1],
                    RespValue::BulkString(Some(b"myvalue".to_vec()))
                );
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

        let result = handle_exec(&mut state, &handler, &engine).await.unwrap();
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
        handle_watch(&mut state, &engine, &[b"watched_key".to_vec()]).unwrap();

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
        let result = handle_exec(&mut state, &handler, &engine).await.unwrap();
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
        handle_watch(&mut state, &engine, &[b"watched_key".to_vec()]).unwrap();

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
        let result = handle_exec(&mut state, &handler, &engine).await.unwrap();
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

        let result = handle_exec(&mut state, &handler, &engine).await.unwrap();
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

        handle_exec(&mut state, &handler, &engine).await.unwrap();

        // State should be fully reset
        assert!(!state.in_multi);
        assert!(state.queue.is_empty());
        assert!(state.watched_keys.is_empty());
    }

    #[tokio::test]
    async fn test_discard_clears_watched_keys() {
        let engine = create_engine();
        let mut state = TransactionState::new();

        handle_watch(&mut state, &engine, &[b"key1".to_vec()]).unwrap();
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

        let result = handle_exec(&mut state, &handler, &engine).await.unwrap();
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
        handle_watch(&mut state, &engine, &[b"nonexistent".to_vec()]).unwrap();

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

        let result = handle_exec(&mut state, &handler, &engine).await.unwrap();
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
        handle_watch(&mut state, &engine, &[b"mykey".to_vec()]).unwrap();

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

        let result = handle_exec(&mut state, &handler, &engine).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }
}
