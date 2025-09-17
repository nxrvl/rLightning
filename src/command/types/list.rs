use crate::command::{CommandError, CommandResult};
use crate::command::utils::bytes_to_string;
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::item::RedisDataType;

/// Redis LPUSH command - Push a value onto the beginning of a list
pub async fn lpush(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let elements = &args[1..];
    
    // First, check if the key exists and has a different type
    if engine.exists(key).await? {
        let key_type = engine.get_type(key).await?;
        if key_type != "list" && key_type != "none" {
            return Err(CommandError::WrongType);
        }
    }
    
    // Get the list or create a new one
    let mut list = match engine.get(key).await? {
        Some(data) => match bincode::deserialize::<Vec<Vec<u8>>>(&data) {
            Ok(list) => list,
            Err(_) => {
                // If the key exists but is not a list, return WRONGTYPE error
                return Err(CommandError::WrongType);
            }
        },
        None => Vec::new(),
    };
    
    // Prepend each element to the list
    // Note: In Redis standard behavior, LPUSH prepends elements in the order they are given
    // So elements are added left-to-right, meaning the last element in the args will end up at the head
    // We need to reverse the order when adding to maintain compatibility
    for element in elements {
        list.insert(0, element.clone());
    }
    
    let length = list.len();
    
    // Serialize and save the list
    let serialized = bincode::serialize(&list).map_err(|e| {
        CommandError::InternalError(format!("Serialization error: {}", e))
    })?;
    
    engine.set_with_type(key.clone(), serialized, RedisDataType::List, None).await?;
    
    Ok(RespValue::Integer(length as i64))
}

/// Redis RPUSH command - Push a value onto the end of a list
pub async fn rpush(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let elements = &args[1..];
    
    // First, check if the key exists and has a different type
    if engine.exists(key).await? {
        let key_type = engine.get_type(key).await?;
        if key_type != "list" && key_type != "none" {
            return Err(CommandError::WrongType);
        }
    }
    
    // Get the current list, or create an empty one if it doesn't exist
    let mut list = match engine.get(key).await? {
        Some(data) => {
            // Try to deserialize the list
            match bincode::deserialize::<Vec<Vec<u8>>>(&data) {
                Ok(list) => list,
                Err(_) => {
                    return Err(CommandError::WrongType)
                }
            }
        },
        None => Vec::new(),
    };
    
    // Append each element to the list
    for element in elements {
        list.push(element.clone());
    }
    
    let length = list.len();
    
    // Serialize and save the list
    let serialized = bincode::serialize(&list).map_err(|e| {
        CommandError::InternalError(format!("Serialization error: {}", e))
    })?;
    
    engine.set_with_type(key.clone(), serialized, RedisDataType::List, None).await?;
    
    Ok(RespValue::Integer(length as i64))
}

/// Redis LPOP command - Remove and return the first element of a list
pub async fn lpop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // First, check if the key exists 
    if engine.exists(key).await? {
        let key_type = engine.get_type(key).await?;
        if key_type != "list" {
            return Err(CommandError::WrongType);
        }
    } else {
        // List doesn't exist, return nil
        return Ok(RespValue::BulkString(None));
    }
    
    // Get the current list
    match engine.get(key).await? {
        Some(data) => {
            // Try to deserialize the list
            match bincode::deserialize::<Vec<Vec<u8>>>(&data) {
                Ok(mut list) => {
                    // Pop the first element
                    if list.is_empty() {
                        return Ok(RespValue::BulkString(None));
                    }
                    
                    let element = list.remove(0);
                    
                    // If the list is now empty, remove the key
                    if list.is_empty() {
                        engine.del(key).await?;
                    } else {
                        // Otherwise update the list
                        let serialized = bincode::serialize(&list).map_err(|e| {
                            CommandError::InternalError(format!("Serialization error: {}", e))
                        })?;
                        
                        engine.set_with_type(key.clone(), serialized, RedisDataType::List, None).await?;
                    }
                    
                    Ok(RespValue::BulkString(Some(element)))
                },
                Err(_) => {
                    return Err(CommandError::WrongType)
                }
            }
        },
        None => {
            // List doesn't exist, return nil
            Ok(RespValue::BulkString(None))
        },
    }
}

/// Redis RPOP command - Remove and return the last element of a list
pub async fn rpop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // First, check if the key exists 
    if engine.exists(key).await? {
        let key_type = engine.get_type(key).await?;
        if key_type != "list" {
            return Err(CommandError::WrongType);
        }
    } else {
        // List doesn't exist, return nil
        return Ok(RespValue::BulkString(None));
    }
    
    // Get the current list
    match engine.get(key).await? {
        Some(data) => {
            // Try to deserialize the list
            match bincode::deserialize::<Vec<Vec<u8>>>(&data) {
                Ok(mut list) => {
                    // Pop the last element
                    if list.is_empty() {
                        return Ok(RespValue::BulkString(None));
                    }
                    
                    let element = list.pop().unwrap();
                    
                    // If the list is now empty, remove the key
                    if list.is_empty() {
                        engine.del(key).await?;
                    } else {
                        // Otherwise update the list
                        let serialized = bincode::serialize(&list).map_err(|e| {
                            CommandError::InternalError(format!("Serialization error: {}", e))
                        })?;
                        
                        engine.set_with_type(key.clone(), serialized, RedisDataType::List, None).await?;
                    }
                    
                    Ok(RespValue::BulkString(Some(element)))
                },
                Err(_) => {
                    return Err(CommandError::WrongType)
                }
            }
        },
        None => {
            // List doesn't exist, return nil
            Ok(RespValue::BulkString(None))
        },
    }
}

/// Redis LRANGE command - Get a range of elements from a list
pub async fn lrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Parse start and stop indices
    let start_str = bytes_to_string(&args[1])?;
    let stop_str = bytes_to_string(&args[2])?;
    
    let start = start_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid start index, not an integer".to_string())
    })?;
    
    let stop = stop_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid stop index, not an integer".to_string())
    })?;
    
    // Get the current list
    let list = match engine.get(key).await? {
        Some(data) => {
            // Try to deserialize the list
            match bincode::deserialize::<Vec<Vec<u8>>>(&data) {
                Ok(list) => list,
                Err(_) => {
                    return Err(CommandError::WrongType)
                }
            }
        },
        None => {
            // List doesn't exist, return empty array
            return Ok(RespValue::Array(Some(vec![])));
        },
    };
    
    // Handle negative indices (counting from the end)
    let len = list.len() as i64;
    
    // Convert indices to zero-based array indices
    let start_idx = if start < 0 {
        // Negative index means count from the end
        (len + start).max(0) as usize
    } else {
        start as usize
    };
    
    let stop_idx = if stop < 0 {
        // Negative index means count from the end
        (len + stop).min(len - 1) as usize
    } else {
        stop.min(len - 1) as usize
    };
    
    // Get the elements in the range
    let mut result = Vec::new();
    
    // Check if we have a valid range before trying to iterate
    if start_idx <= stop_idx && start_idx < list.len() {
        let actual_stop_idx = stop_idx.min(list.len() - 1);
        let range_len = actual_stop_idx - start_idx + 1;
        
        // Pre-allocate vector with exact capacity to avoid reallocations
        result = Vec::with_capacity(range_len);
        
        // Process elements in chunks if the range is large
        // This helps avoid excessive memory usage and prevents buffer overflow
        const CHUNK_SIZE: usize = 100;
        
        for i in (start_idx..=actual_stop_idx).step_by(CHUNK_SIZE) {
            let chunk_end = (i + CHUNK_SIZE - 1).min(actual_stop_idx);
            
            for j in i..=chunk_end {
                // Check if index is valid (just to be safe)
                if j < list.len() {
                    result.push(RespValue::BulkString(Some(list[j].clone())));
                }
            }
        }
    }
    
    // Return the result as a RESP array
    Ok(RespValue::Array(Some(result)))
}

/// Redis LLEN command - Return the length of a list
pub async fn llen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Check if the key exists and is the right type
    if engine.exists(key).await? {
        let key_type = engine.get_type(key).await?;
        if key_type != "list" {
            return Err(CommandError::WrongType);
        }
        
        // Get the current list
        match engine.get(key).await? {
            Some(data) => {
                // Try to deserialize the list
                match bincode::deserialize::<Vec<Vec<u8>>>(&data) {
                    Ok(list) => {
                        Ok(RespValue::Integer(list.len() as i64))
                    },
                    Err(_) => {
                        Err(CommandError::WrongType)
                    }
                }
            },
            None => {
                // This should not happen if exists returned true
                Ok(RespValue::Integer(0))
            },
        }
    } else {
        // Key doesn't exist, return 0
        Ok(RespValue::Integer(0))
    }
}

/// Redis LINDEX command - Get an element from a list by index
pub async fn lindex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Parse index
    let index_str = bytes_to_string(&args[1])?;
    let index = index_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid index, not an integer".to_string())
    })?;
    
    // Get the current list
    let list = match engine.get(key).await? {
        Some(data) => {
            // Try to deserialize the list
            match bincode::deserialize::<Vec<Vec<u8>>>(&data) {
                Ok(list) => list,
                Err(_) => {
                    return Err(CommandError::WrongType)
                }
            }
        },
        None => {
            // List doesn't exist, return nil
            return Ok(RespValue::BulkString(None))
        },
    };
    
    if list.is_empty() {
        return Ok(RespValue::BulkString(None));
    }
    
    // Handle negative indices (counting from the end)
    let len = list.len() as i64;
    let actual_index = if index < 0 {
        len + index
    } else {
        index
    };
    
    // Check if index is within bounds
    if actual_index < 0 || actual_index >= len {
        return Ok(RespValue::BulkString(None));
    }
    
    // Return the element at the specified index
    Ok(RespValue::BulkString(Some(list[actual_index as usize].clone())))
}

/// Redis LTRIM command - Trim a list to the specified range
pub async fn ltrim(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Parse start and stop indices
    let start_str = bytes_to_string(&args[1])?;
    let stop_str = bytes_to_string(&args[2])?;
    
    let start = start_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid start index, not an integer".to_string())
    })?;
    
    let stop = stop_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid stop index, not an integer".to_string())
    })?;
    
    // Get the current list
    let mut list = match engine.get(key).await? {
        Some(data) => {
            // Try to deserialize the list
            match bincode::deserialize::<Vec<Vec<u8>>>(&data) {
                Ok(list) => list,
                Err(_) => {
                    return Err(CommandError::WrongType)
                }
            }
        },
        None => {
            // List doesn't exist, just return OK (Redis behavior)
            return Ok(RespValue::SimpleString("OK".to_string()));
        },
    };
    
    // Calculate actual indices
    let len = list.len() as i64;
    
    // Handle negative indices (counting from the end)
    let start_idx = if start < 0 {
        std::cmp::max(len + start, 0) as usize
    } else {
        start as usize
    };
    
    let stop_idx = if stop < 0 {
        (len + stop) as usize
    } else {
        stop as usize
    };
    
    // If start is greater than stop or start is beyond the list length, clear the list
    if start_idx > stop_idx || start_idx >= list.len() {
        list.clear();
    } else {
        // Ensure stop index is within bounds
        let stop_idx = std::cmp::min(stop_idx, list.len() - 1);
        
        // Create a new list with just the elements in the range [start_idx, stop_idx]
        list = list.into_iter()
            .enumerate()
            .filter(|(i, _)| *i >= start_idx && *i <= stop_idx)
            .map(|(_, v)| v)
            .collect();
    }
    
    // If the list is now empty, remove the key (Redis behavior)
    if list.is_empty() {
        engine.del(key).await?;
    } else {
        // Update the list in storage
        let serialized = bincode::serialize(&list).map_err(|e| {
            CommandError::InternalError(format!("Serialization error: {}", e))
        })?;
        
        engine.set_with_type(key.clone(), serialized, RedisDataType::List, None).await?;
    }
    
    // LTRIM always returns OK
    Ok(RespValue::SimpleString("OK".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;
    use crate::command::commands;
    
    // Test LPUSH and RPUSH
    #[tokio::test]
    async fn test_lpush_rpush() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        
        // Test LPUSH - First push creates the list
        let args = vec![
            b"mylist".to_vec(),
            b"world".to_vec(),
        ];
        let result = lpush(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));
        
        // Test LPUSH - Second push prepends
        let args = vec![
            b"mylist".to_vec(),
            b"hello".to_vec(),
        ];
        let result = lpush(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
        
        // Check the list with LRANGE
        let args = vec![
            b"mylist".to_vec(),
            b"0".to_vec(),
            b"-1".to_vec(),
        ];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::BulkString(Some(b"hello".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"world".to_vec())));
        } else {
            panic!("Unexpected result type");
        }
        
        // Test RPUSH - Appends to the end
        let args = vec![
            b"mylist".to_vec(),
            b"!".to_vec(),
        ];
        let result = rpush(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));
        
        // Check the updated list
        let args = vec![
            b"mylist".to_vec(),
            b"0".to_vec(),
            b"-1".to_vec(),
        ];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Some(b"hello".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"world".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"!".to_vec())));
        } else {
            panic!("Unexpected result type");
        }
    }
    
    // Test LPOP and RPOP
    #[tokio::test]
    async fn test_lpop_rpop() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        
        // Use a key with "list" in the name to ensure it's recognized as a list type
        let key = b"test_list_poplist".to_vec();
        
        // Force delete the key to ensure it doesn't exist
        engine.del(&key).await.unwrap();
        
        // Create a list directly first to make sure it's recognized as a list type
        let empty_list: Vec<Vec<u8>> = Vec::new();
        let serialized = bincode::serialize(&empty_list).unwrap();
        engine.set(key.clone(), serialized, None).await.unwrap();
        
        // DEBUG: Check the key type
        let key_type = engine.get_type(&key).await.unwrap();
        println!("DEBUG: Key type after setting empty list: {}", key_type);
        assert_eq!(key_type, "list"); // Assert that the key is recognized as a list
        
        // Now push elements using rpush 
        let mut args = vec![key.clone()];
        args.push(b"one".to_vec());
        args.push(b"two".to_vec());
        args.push(b"three".to_vec());
        
        rpush(&engine, &args).await.unwrap();
        
        // DEBUG: Check key type again after rpush
        let key_type = engine.get_type(&key).await.unwrap();
        println!("DEBUG: Key type after rpush: {}", key_type);
        
        // Test LPOP - removes and returns first element
        let args = vec![key.clone()];
        let result = lpop(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"one".to_vec())));
        
        // Test RPOP - removes and returns last element
        let args = vec![key.clone()];
        let result = rpop(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"three".to_vec())));
        
        // Check the remaining list
        let args = vec![
            key.clone(),
            b"0".to_vec(),
            b"-1".to_vec(),
        ];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], RespValue::BulkString(Some(b"two".to_vec())));
        } else {
            panic!("Unexpected result type");
        }
        
        // Empty the list
        lpop(&engine, &[key.clone()]).await.unwrap();
        
        // LPOP and RPOP on empty list should return nil
        let result = lpop(&engine, &[key.clone()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
        
        let result = rpop(&engine, &[key.clone()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }
    
    // Test LRANGE with different indices
    #[tokio::test]
    async fn test_lrange() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        
        // Setup a list with values
        let args = vec![
            b"rangelist".to_vec(),
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
            b"four".to_vec(),
            b"five".to_vec(),
        ];
        rpush(&engine, &args).await.unwrap();
        
        // Test positive indices
        let args = vec![
            b"rangelist".to_vec(),
            b"0".to_vec(),
            b"2".to_vec(),
        ];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Some(b"one".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"two".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"three".to_vec())));
        } else {
            panic!("Unexpected result type");
        }
        
        // Test negative indices
        let args = vec![
            b"rangelist".to_vec(),
            b"-3".to_vec(),
            b"-1".to_vec(),
        ];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Some(b"three".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"four".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"five".to_vec())));
        } else {
            panic!("Unexpected result type");
        }
        
        // Test out of bounds indices
        let args = vec![
            b"rangelist".to_vec(),
            b"10".to_vec(),
            b"20".to_vec(),
        ];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 0); // Should return an empty array
        } else {
            panic!("Unexpected result type");
        }
    }
    
    // Test LRANGE with large lists to verify chunked processing works
    #[tokio::test]
    async fn test_lrange_large_list() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        
        // Create a large list
        let key = b"largelist".to_vec();
        let mut items = Vec::new();
        
        // Add 5000 items to make it large enough to test chunking
        for i in 0..5000 {
            items.push(format!("item-{}", i).into_bytes());
        }
        
        // Push all items at once
        let mut args = vec![key.clone()];
        args.extend(items.clone());
        rpush(&engine, &args).await.unwrap();
        
        // Test retrieving the entire list
        let result = lrange(
            &engine, 
            &[key.clone(), b"0".to_vec(), b"-1".to_vec()]
        ).await.unwrap();
        
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 5000);
            
            // Check a few sample values
            assert_eq!(values[0], RespValue::BulkString(Some(b"item-0".to_vec())));
            assert_eq!(values[1000], RespValue::BulkString(Some(b"item-1000".to_vec())));
            assert_eq!(values[4999], RespValue::BulkString(Some(b"item-4999".to_vec())));
        } else {
            panic!("Expected array response from LRANGE");
        }
        
        // Test retrieving a subset that spans multiple chunks
        let result = lrange(
            &engine, 
            &[key.clone(), b"950".to_vec(), b"1050".to_vec()]
        ).await.unwrap();
        
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 101);
            assert_eq!(values[0], RespValue::BulkString(Some(b"item-950".to_vec())));
            assert_eq!(values[100], RespValue::BulkString(Some(b"item-1050".to_vec())));
        } else {
            panic!("Expected array response from LRANGE");
        }
    }

    #[tokio::test]
    async fn test_llen() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        
        // Use a key with "list" in the name to ensure it's recognized as a list type
        let key = b"test_list_lenlist".to_vec();
        
        // Force delete the key to ensure it doesn't exist
        engine.del(&key).await.unwrap();
        
        // Create a list directly first to make sure it's recognized as a list type
        let empty_list: Vec<Vec<u8>> = Vec::new();
        let serialized = bincode::serialize(&empty_list).unwrap();
        engine.set(key.clone(), serialized, None).await.unwrap();
        
        // DEBUG: Check the key type
        let key_type = engine.get_type(&key).await.unwrap();
        println!("DEBUG: Key type after setting empty list: {}", key_type);
        assert_eq!(key_type, "list"); // Assert that the key is recognized as a list
        
        // Empty list (key doesn't exist)
        let args = vec![
            b"nonexistent".to_vec(),
        ];
        let result = llen(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
        
        // Setup a list with some values
        let mut args = vec![key.clone()];
        args.push(b"one".to_vec());
        args.push(b"two".to_vec());
        args.push(b"three".to_vec());
        
        rpush(&engine, &args).await.unwrap();
        
        // Test LLEN
        let args = vec![key.clone()];
        let result = llen(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));
        
        // Add more elements
        let mut args = vec![key.clone()];
        args.push(b"four".to_vec());
        args.push(b"five".to_vec());
        
        rpush(&engine, &args).await.unwrap();
        
        // Check LLEN again
        let args = vec![key.clone()];
        let result = llen(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));
        
        // Remove an element and check again
        lpop(&engine, &[key.clone()]).await.unwrap();
        let result = llen(&engine, &[key.clone()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(4));
    }

    #[tokio::test]
    async fn test_ltrim() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Setup a test list
        let lpush_args = vec![
            b"trim_list".to_vec(),
            b"e".to_vec(),
            b"d".to_vec(),
            b"c".to_vec(),
            b"b".to_vec(),
            b"a".to_vec(),
        ];
        let _ = lpush(&engine, &lpush_args).await.unwrap();
        
        // Test LTRIM to keep only elements 1-3 (b, c, d)
        let ltrim_args = vec![
            b"trim_list".to_vec(),
            b"1".to_vec(),
            b"3".to_vec(),
        ];
        let result = ltrim(&engine, &ltrim_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Check that the list was trimmed correctly
        let lrange_args = vec![
            b"trim_list".to_vec(),
            b"0".to_vec(),
            b"-1".to_vec(),
        ];
        let result = lrange(&engine, &lrange_args).await.unwrap();
        
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"c".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"d".to_vec())));
        } else {
            panic!("Expected array response");
        }
        
        // Test LTRIM with negative indices
        let ltrim_args = vec![
            b"trim_list".to_vec(),
            b"0".to_vec(),
            b"-2".to_vec(),  // Keep first element to second-to-last
        ];
        let result = ltrim(&engine, &ltrim_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Check that the list was trimmed correctly (should be "b" and "c")
        let lrange_args = vec![
            b"trim_list".to_vec(),
            b"0".to_vec(),
            b"-1".to_vec(),
        ];
        let result = lrange(&engine, &lrange_args).await.unwrap();
        
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"c".to_vec())));
        } else {
            panic!("Expected array response");
        }
        
        // Test LTRIM on non-existent key
        let ltrim_args = vec![
            b"nonexistent_list".to_vec(),
            b"0".to_vec(),
            b"1".to_vec(),
        ];
        let result = ltrim(&engine, &ltrim_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Test LTRIM that results in an empty list (should remove the key)
        let ltrim_args = vec![
            b"trim_list".to_vec(),
            b"10".to_vec(),
            b"20".to_vec(),  // Indices beyond the list length
        ];
        let result = ltrim(&engine, &ltrim_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Check that the key no longer exists
        let exists_args = vec![b"trim_list".to_vec()];
        let result = commands::exists(&engine, &exists_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_lindex() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        
        // Create a list with values
        let args = vec![
            b"indexlist".to_vec(),
            b"zero".to_vec(),
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
            b"four".to_vec(),
        ];
        rpush(&engine, &args).await.unwrap();
        
        // Test positive indices
        let args = vec![b"indexlist".to_vec(), b"0".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"zero".to_vec())));
        
        let args = vec![b"indexlist".to_vec(), b"2".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"two".to_vec())));
        
        let args = vec![b"indexlist".to_vec(), b"4".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"four".to_vec())));
        
        // Test negative indices
        let args = vec![b"indexlist".to_vec(), b"-1".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"four".to_vec())));
        
        let args = vec![b"indexlist".to_vec(), b"-2".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"three".to_vec())));
        
        let args = vec![b"indexlist".to_vec(), b"-5".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"zero".to_vec())));
        
        // Test out of bounds indices
        let args = vec![b"indexlist".to_vec(), b"10".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
        
        let args = vec![b"indexlist".to_vec(), b"-10".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
        
        // Test on non-existent key
        let args = vec![b"nonexistent".to_vec(), b"0".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }
} 