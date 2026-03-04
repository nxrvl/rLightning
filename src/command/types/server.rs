use crate::command::utils::bytes_to_string;
use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// Redis INFO command - Get information about the server
#[allow(dead_code)]
pub async fn info(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    // Build a simple INFO response with some basic information
    let info_str = [
        "# Server",
        "redis_version:6.0.0",
        "redis_mode:standalone",
        "os:rust",
        "arch_bits:64",
        "process_id:1",
        "",
        "# Memory",
        "used_memory_human:0K",
        "used_memory_peak_human:0K",
        "",
        "# Stats",
        "total_connections_received:1",
        "total_commands_processed:1",
        "",
        "# Replication",
        "role:master",
        "",
        "# CPU",
        "used_cpu_sys:0.0",
        "used_cpu_user:0.0",
        "",
        "# Cluster",
        "cluster_enabled:0",
    ]
    .join("\r\n");

    Ok(RespValue::BulkString(Some(info_str.into_bytes())))
}

/// Redis AUTH command - Authenticate to the server
/// Supports both AUTH password and AUTH username password
pub async fn auth(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Authentication is handled by the Server's authentication middleware
    // For compatibility, we always return OK because the actual authentication
    // check happens in the networking layer
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis CONFIG command - Get or set server configuration
pub async fn config(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcommand = bytes_to_string(&args[0])?.to_uppercase();

    match subcommand.as_str() {
        "GET" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }

            // Redis supports multiple patterns: CONFIG GET pattern [pattern ...]
            let mut result = Vec::new();
            for arg in &args[1..] {
                let pattern = bytes_to_string(arg)?.to_lowercase();
                for (key, value) in engine.config_get(&pattern) {
                    result.push(RespValue::BulkString(Some(key.into_bytes())));
                    result.push(RespValue::BulkString(Some(value.into_bytes())));
                }
            }

            Ok(RespValue::Array(Some(result)))
        }
        _ => {
            // CONFIG HELP and other unhandled subcommands
            Ok(RespValue::SimpleString("OK".to_string()))
        }
    }
}

/// Redis KEYS command - Find all keys matching the given pattern
pub async fn keys(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let pattern = bytes_to_string(&args[0])?;
    let matching_keys = engine.keys(&pattern).await?;

    let result: Vec<RespValue> = matching_keys
        .into_iter()
        .map(|k| RespValue::BulkString(Some(k)))
        .collect();

    Ok(RespValue::Array(Some(result)))
}

/// Redis RENAME command - Rename a key to a new name
pub async fn rename(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = args[0].clone();
    let new_key = args[1].clone();

    // RENAME to self is a no-op in Redis
    if key == new_key {
        // Verify source key exists (Redis returns error if it doesn't)
        if engine.exists(&key).await? {
            return Ok(RespValue::SimpleString("OK".to_string()));
        } else {
            return Err(CommandError::InvalidArgument("no such key".to_string()));
        }
    }

    // Get the full item to preserve data type and TTL
    let item = match engine.get_item(&key).await? {
        Some(item) => item,
        None => {
            return Err(CommandError::InvalidArgument("Key not found".to_string()));
        }
    };

    // Compute remaining TTL from the item's expiration
    let ttl = item.expires_at.map(|exp| {
        let now = std::time::Instant::now();
        if exp > now {
            exp - now
        } else {
            std::time::Duration::from_millis(1)
        }
    });

    // Set the new key preserving data type and TTL
    engine
        .set_with_type(new_key, item.value, item.data_type, ttl)
        .await?;

    // Delete the old key
    engine.del(&key).await?;

    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis RENAMENX command - Rename a key only if the new key does not exist
pub async fn renamenx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = args[0].clone();
    let new_key = args[1].clone();

    // RENAMENX to self: Redis returns 0 (destination exists = source key itself)
    if key == new_key {
        if !engine.exists(&key).await? {
            return Err(CommandError::InvalidArgument("no such key".to_string()));
        }
        return Ok(RespValue::Integer(0));
    }

    // Check if the destination key already exists
    if engine.exists(&new_key).await? {
        return Ok(RespValue::Integer(0));
    }

    // Get the full item to preserve data type and TTL
    let item = match engine.get_item(&key).await? {
        Some(item) => item,
        None => {
            return Err(CommandError::InvalidArgument("no such key".to_string()));
        }
    };

    // Compute remaining TTL from the item's expiration
    let ttl = item.expires_at.map(|exp| {
        let now = std::time::Instant::now();
        if exp > now {
            exp - now
        } else {
            std::time::Duration::from_millis(1)
        }
    });

    // Set the new key preserving data type and TTL
    engine
        .set_with_type(new_key, item.value, item.data_type, ttl)
        .await?;

    // Delete the old key
    engine.del(&key).await?;

    Ok(RespValue::Integer(1))
}

/// Redis FLUSHALL command - Delete all keys from all databases
pub async fn flushall(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    engine.flush_all().await?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis FLUSHDB command - Delete all keys from the current database
pub async fn flushdb(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    engine.flush_db().await?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis MONITOR command - Stream back every command processed by the server
pub async fn monitor(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    // This is a simplified implementation that just returns a message
    // A real implementation would require setting up a subscription channel
    // from the client to receive all commands as they come in

    // For our simple server, we'll just return a message explaining this
    Ok(RespValue::Error("MONITOR not fully implemented in this server. This command should allow you to see all commands processed by the server in real-time.".to_string()))
}

/// Redis SCAN command - Incrementally iterate over keys
#[allow(dead_code)]
pub async fn scan(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Parse cursor
    let cursor_str = bytes_to_string(&args[0])?;
    let cursor = cursor_str.parse::<u64>().map_err(|_| {
        CommandError::InvalidArgument("Cursor value must be an integer".to_string())
    })?;

    // Check for MATCH option
    let mut pattern = String::from("*");
    let mut count = 10; // Default scan count

    let mut i = 1;
    while i < args.len() {
        match bytes_to_string(&args[i])?.to_uppercase().as_str() {
            "MATCH" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                pattern = bytes_to_string(&args[i + 1])?.to_string();
                i += 2;
            }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let count_str = bytes_to_string(&args[i + 1])?;
                count = count_str.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "COUNT value must be a positive integer".to_string(),
                    )
                })?;
                i += 2;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported SCAN option: {}",
                    bytes_to_string(&args[i])?
                )));
            }
        }
    }

    // Get all keys matching the pattern
    let all_keys = engine.keys(&pattern).await?;

    // Simplified implementation - we just return all keys after the cursor
    // A real implementation would use a more efficient scan algorithm

    let total_keys = all_keys.len() as u64;

    // If cursor is 0 or exceeds our total, start from the beginning
    let start_idx = if cursor == 0 || cursor >= total_keys {
        0
    } else {
        cursor as usize
    };

    // Calculate the end index based on count
    let end_idx = (start_idx + count).min(all_keys.len());

    // Get the keys for this batch
    let batch: Vec<RespValue> = all_keys[start_idx..end_idx]
        .iter()
        .map(|k| RespValue::BulkString(Some(k.clone())))
        .collect();

    // Calculate new cursor position
    let new_cursor = if end_idx >= all_keys.len() {
        // We've reached the end, return 0 to signal completion
        0
    } else {
        // Return the next position
        end_idx as u64
    };

    // Prepare response - SCAN always returns an array with two elements:
    // 1. The new cursor position (as a string)
    // 2. An array of keys
    let response = vec![
        // The cursor must be returned as a bulk string, not an integer
        RespValue::BulkString(Some(new_cursor.to_string().into_bytes())),
        RespValue::Array(Some(batch)),
    ];

    Ok(RespValue::Array(Some(response)))
}

/// Redis DBSIZE command - Return the number of keys in the current database
pub async fn dbsize(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Use per-database key count via active_db() which respects the task-local db index
    let key_count = engine.active_db().len();
    Ok(RespValue::Integer(key_count as i64))
}

/// Redis RANDOMKEY command - Return a random key from the database
pub async fn randomkey(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Use efficient random key selection instead of O(n) scan
    match engine.get_random_key().await? {
        Some(key) => Ok(RespValue::BulkString(Some(key))),
        None => Ok(RespValue::BulkString(None)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::types::connection::config_set;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_server_commands() {
        let engine_config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(engine_config));

        // Test INFO command
        let info_args = vec![];
        let result = info(&engine, &info_args).await.unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let data_str = String::from_utf8_lossy(&data);
            assert!(data_str.contains("redis_version"));
            assert!(data_str.contains("role:master"));
        } else {
            panic!("Expected bulk string response from INFO");
        }

        // Test AUTH command
        let auth_args = vec![b"password".to_vec()];
        let result = auth(&engine, &auth_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Test CONFIG GET command
        let config_args = vec![b"GET".to_vec(), b"maxmemory".to_vec()];
        let result = config(&engine, &config_args).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"maxmemory".to_vec())));
            // Default StorageConfig has max_memory = 128 * 1024 * 1024 = 134217728
            assert_eq!(items[1], RespValue::BulkString(Some(b"134217728".to_vec())));
        } else {
            panic!("Expected array response from CONFIG GET");
        }

        // Test FLUSHALL/FLUSHDB commands
        // Add some test data
        engine
            .set(b"key1".to_vec(), b"value1".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"key2".to_vec(), b"value2".to_vec(), None)
            .await
            .unwrap();

        // Verify keys exist
        assert!(engine.exists(b"key1").await.unwrap());
        assert!(engine.exists(b"key2").await.unwrap());

        // Test FLUSHALL
        let flush_args = vec![];
        let result = flushall(&engine, &flush_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify all keys are gone
        assert!(!engine.exists(b"key1").await.unwrap());
        assert!(!engine.exists(b"key2").await.unwrap());

        // Add more test data
        engine
            .set(b"key3".to_vec(), b"value3".to_vec(), None)
            .await
            .unwrap();

        // Test FLUSHDB
        let result = flushdb(&engine, &flush_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify all keys are gone
        assert!(!engine.exists(b"key3").await.unwrap());
    }

    // Test SCAN command to verify proper formatting
    #[tokio::test]
    async fn test_scan_command() {
        let engine_config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(engine_config));

        // Add a bunch of test keys with different patterns
        for i in 0..100 {
            // Add some keys with different prefixes
            let key1 = format!("scan:test:{}", i).into_bytes();
            let key2 = format!("other:test:{}", i).into_bytes();

            engine.set(key1, b"value".to_vec(), None).await.unwrap();
            engine.set(key2, b"value".to_vec(), None).await.unwrap();
        }

        // Test basic SCAN with default parameters
        let scan_args = vec![b"0".to_vec()];
        let result = scan(&engine, &scan_args).await.unwrap();

        // Verify the response format
        if let RespValue::Array(Some(scan_result)) = result {
            // SCAN must return exactly 2 elements: cursor position and array of keys
            assert_eq!(scan_result.len(), 2);

            // First element must be the cursor position as a bulk string
            match &scan_result[0] {
                RespValue::BulkString(Some(cursor_bytes)) => {
                    // Should be able to parse the cursor as a number
                    let cursor_str = String::from_utf8_lossy(cursor_bytes);
                    cursor_str
                        .parse::<u64>()
                        .expect("Cursor should be a valid number");
                }
                _ => panic!("Expected cursor to be a bulk string"),
            }

            // Second element must be an array of keys
            match &scan_result[1] {
                RespValue::Array(Some(keys)) => {
                    // We should have found some keys
                    assert!(!keys.is_empty());

                    // Each key should be a bulk string
                    for key in keys {
                        match key {
                            RespValue::BulkString(Some(_)) => {
                                // This is correct
                            }
                            _ => panic!("Each key should be a bulk string"),
                        }
                    }
                }
                _ => panic!("Expected an array of keys"),
            }
        } else {
            panic!("Expected SCAN to return an array with 2 elements");
        }

        // Test SCAN with MATCH option
        let scan_args = vec![b"0".to_vec(), b"MATCH".to_vec(), b"scan:test:*".to_vec()];
        let result = scan(&engine, &scan_args).await.unwrap();

        if let RespValue::Array(Some(scan_result)) = result {
            assert_eq!(scan_result.len(), 2);

            // Check the keys array
            if let RespValue::Array(Some(keys)) = &scan_result[1] {
                // We should have found only keys matching the pattern
                for key in keys {
                    if let RespValue::BulkString(Some(key_bytes)) = key {
                        let key_str = String::from_utf8_lossy(key_bytes);
                        assert!(key_str.starts_with("scan:test:"));
                    } else {
                        panic!("Expected keys to be bulk strings");
                    }
                }
            } else {
                panic!("Expected an array of keys");
            }
        }

        // Test SCAN with COUNT option
        let scan_args = vec![b"0".to_vec(), b"COUNT".to_vec(), b"5".to_vec()];
        let result = scan(&engine, &scan_args).await.unwrap();

        if let RespValue::Array(Some(scan_result)) = result {
            assert_eq!(scan_result.len(), 2);

            // Check that we got the right cursor format
            if let RespValue::BulkString(Some(cursor_bytes)) = &scan_result[0] {
                let cursor_str = String::from_utf8_lossy(cursor_bytes);
                assert!(!cursor_str.is_empty());
            } else {
                panic!("Expected cursor to be a bulk string");
            }

            // Check the keys array
            if let RespValue::Array(Some(keys)) = &scan_result[1] {
                // The COUNT is just a hint, but we should at least respect the upper bound
                assert!(keys.len() <= 5);
            } else {
                panic!("Expected an array of keys");
            }
        }

        // Test serialization of SCAN response to verify RESP protocol conformance
        let scan_args = vec![b"0".to_vec()];
        if let RespValue::Array(Some(scan_result)) = scan(&engine, &scan_args).await.unwrap() {
            // Serialize the response
            let serialized = RespValue::Array(Some(scan_result.clone()))
                .serialize()
                .unwrap();

            // Check that it starts with *2\r\n (array of 2 elements)
            assert_eq!(&serialized[0..4], b"*2\r\n");

            // Look for the cursor in the output
            let cursor_value = match &scan_result[0] {
                RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                _ => panic!("Expected cursor to be a bulk string"),
            };

            // Check that the cursor starts with $
            let cursor_prefix = format!("${}\r\n", cursor_value.len());
            let cursor_bytes = cursor_prefix.as_bytes();

            // serialized should contain cursor_bytes
            let mut found = false;
            for i in 0..serialized.len() - cursor_bytes.len() {
                if &serialized[i..i + cursor_bytes.len()] == cursor_bytes {
                    found = true;
                    break;
                }
            }
            assert!(
                found,
                "RESP serialization should contain proper cursor format"
            );
        }
    }

    fn extract_config_pairs(result: RespValue) -> Vec<(String, String)> {
        if let RespValue::Array(Some(items)) = result {
            let mut pairs = Vec::new();
            for chunk in items.chunks(2) {
                if let (RespValue::BulkString(Some(k)), RespValue::BulkString(Some(v))) =
                    (&chunk[0], &chunk[1])
                {
                    pairs.push((
                        String::from_utf8_lossy(k).to_string(),
                        String::from_utf8_lossy(v).to_string(),
                    ));
                }
            }
            pairs
        } else {
            panic!("Expected array response from CONFIG GET");
        }
    }

    #[tokio::test]
    async fn test_config_set_get_roundtrip() {
        let engine = StorageEngine::new(StorageConfig::default());

        // CONFIG SET a known runtime param then CONFIG GET it back
        let set_args = vec![b"hz".to_vec(), b"15".to_vec()];
        let result = config_set(&engine, &set_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let get_args = vec![b"GET".to_vec(), b"hz".to_vec()];
        let result = config(&engine, &get_args).await.unwrap();
        let pairs = extract_config_pairs(result);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0], ("hz".to_string(), "15".to_string()));
    }

    #[tokio::test]
    async fn test_config_set_get_multiple() {
        let engine = StorageEngine::new(StorageConfig::default());

        // CONFIG SET multiple known params at once
        let set_args = vec![
            b"hz".to_vec(),
            b"20".to_vec(),
            b"timeout".to_vec(),
            b"300".to_vec(),
        ];
        let result = config_set(&engine, &set_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Retrieve each
        let get_args = vec![b"GET".to_vec(), b"hz".to_vec()];
        let pairs = extract_config_pairs(config(&engine, &get_args).await.unwrap());
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1, "20");

        let get_args = vec![b"GET".to_vec(), b"timeout".to_vec()];
        let pairs = extract_config_pairs(config(&engine, &get_args).await.unwrap());
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1, "300");
    }

    #[tokio::test]
    async fn test_config_set_unknown_param_rejected() {
        let engine = StorageEngine::new(StorageConfig::default());

        // CONFIG SET with an unknown parameter should return an error
        let set_args = vec![b"nonexistent-param".to_vec(), b"value".to_vec()];
        let result = config_set(&engine, &set_args).await.unwrap();
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[tokio::test]
    async fn test_config_get_glob_patterns() {
        let engine = StorageEngine::new(StorageConfig::default());

        // The engine is seeded with defaults including maxmemory, maxmemory-policy, port, etc.
        // Test wildcard: CONFIG GET max*
        let get_args = vec![b"GET".to_vec(), b"max*".to_vec()];
        let pairs = extract_config_pairs(config(&engine, &get_args).await.unwrap());
        let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"maxmemory"), "max* should match maxmemory");
        assert!(
            keys.contains(&"maxmemory-policy"),
            "max* should match maxmemory-policy"
        );

        // Test CONFIG GET * returns all seeded params
        let get_args = vec![b"GET".to_vec(), b"*".to_vec()];
        let pairs = extract_config_pairs(config(&engine, &get_args).await.unwrap());
        let keys: Vec<String> = pairs.iter().map(|(k, _)| k.clone()).collect();
        assert!(keys.contains(&"maxmemory".to_string()));
        assert!(keys.contains(&"port".to_string()));
        assert!(keys.contains(&"databases".to_string()));
        assert!(keys.contains(&"appendonly".to_string()));
        assert!(keys.contains(&"appendfsync".to_string()));
        assert!(keys.contains(&"save".to_string()));
        assert!(keys.contains(&"hz".to_string()));
        assert!(keys.contains(&"tcp-backlog".to_string()));
        assert!(keys.contains(&"tcp-keepalive".to_string()));
        assert!(keys.contains(&"bind".to_string()));
        assert!(keys.contains(&"timeout".to_string()));
        assert!(keys.contains(&"requirepass".to_string()));

        // Test exact match for a non-existent param returns empty
        let get_args = vec![b"GET".to_vec(), b"nonexistent-param-xyz".to_vec()];
        let pairs = extract_config_pairs(config(&engine, &get_args).await.unwrap());
        assert_eq!(pairs.len(), 0);
    }

    #[tokio::test]
    async fn test_config_set_maxmemory_policy() {
        let engine = StorageEngine::new(StorageConfig::default());

        // Default should be allkeys-lru
        let get_args = vec![b"GET".to_vec(), b"maxmemory-policy".to_vec()];
        let pairs = extract_config_pairs(config(&engine, &get_args).await.unwrap());
        assert_eq!(pairs[0].1, "allkeys-lru");

        // Set to noeviction
        let set_args = vec![b"maxmemory-policy".to_vec(), b"noeviction".to_vec()];
        let result = config_set(&engine, &set_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let pairs = extract_config_pairs(config(&engine, &get_args).await.unwrap());
        assert_eq!(pairs[0].1, "noeviction");

        // Set to allkeys-lfu
        let set_args = vec![b"maxmemory-policy".to_vec(), b"allkeys-lfu".to_vec()];
        config_set(&engine, &set_args).await.unwrap();
        let pairs = extract_config_pairs(config(&engine, &get_args).await.unwrap());
        assert_eq!(pairs[0].1, "allkeys-lfu");

        // Invalid policy should return error
        let set_args = vec![b"maxmemory-policy".to_vec(), b"invalid-policy".to_vec()];
        let result = config_set(&engine, &set_args).await.unwrap();
        if let RespValue::Error(msg) = result {
            assert!(msg.contains("Invalid argument"));
        } else {
            panic!("Expected error for invalid maxmemory-policy");
        }
    }

    #[tokio::test]
    async fn test_config_set_overwrite() {
        let engine = StorageEngine::new(StorageConfig::default());

        // Set a value
        let set_args = vec![b"timeout".to_vec(), b"100".to_vec()];
        config_set(&engine, &set_args).await.unwrap();

        let get_args = vec![b"GET".to_vec(), b"timeout".to_vec()];
        let pairs = extract_config_pairs(config(&engine, &get_args).await.unwrap());
        assert_eq!(pairs[0].1, "100");

        // Overwrite the value
        let set_args = vec![b"timeout".to_vec(), b"200".to_vec()];
        config_set(&engine, &set_args).await.unwrap();

        let pairs = extract_config_pairs(config(&engine, &get_args).await.unwrap());
        assert_eq!(pairs[0].1, "200");
    }
}
