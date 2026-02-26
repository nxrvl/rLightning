use std::time::{Duration, Instant};

use futures::future::select_all;

use crate::command::{CommandError, CommandResult};
use crate::command::utils::bytes_to_string;
use crate::command::types::blocking::BlockingManager;
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
    
    // Prepend all elements at once using splice for O(N+M) instead of O(N*M).
    // Redis LPUSH pushes elements left-to-right, so the last arg ends up at the head.
    let new_elements: Vec<Vec<u8>> = elements.iter().rev().map(|e| e.clone()).collect();
    list.splice(0..0, new_elements);
    
    let length = list.len();
    
    // Serialize and save the list
    let serialized = bincode::serialize(&list).map_err(|e| {
        CommandError::InternalError(format!("Serialization error: {}", e))
    })?;
    
    engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;
    
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
    
    engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;
    
    Ok(RespValue::Integer(length as i64))
}

/// Redis LPOP command - Remove and return the first element(s) of a list
pub async fn lpop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Parse optional count parameter (Redis 6.2+)
    let count = if args.len() == 2 {
        let count_str = bytes_to_string(&args[1])?;
        let c = count_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        if c < 0 {
            return Err(CommandError::InvalidArgument("value is not an integer or out of range".to_string()));
        }
        Some(c as usize)
    } else {
        None
    };

    // First, check if the key exists
    if engine.exists(key).await? {
        let key_type = engine.get_type(key).await?;
        if key_type != "list" {
            return Err(CommandError::WrongType);
        }
    } else {
        // List doesn't exist, return nil (or empty array with count)
        return if count.is_some() {
            Ok(RespValue::BulkString(None))
        } else {
            Ok(RespValue::BulkString(None))
        };
    }

    // Get the current list
    match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<Vec<Vec<u8>>>(&data) {
                Ok(mut list) => {
                    if list.is_empty() {
                        return if count.is_some() {
                            Ok(RespValue::BulkString(None))
                        } else {
                            Ok(RespValue::BulkString(None))
                        };
                    }

                    if let Some(count) = count {
                        // Multi-pop: return array
                        let actual_count = std::cmp::min(count, list.len());
                        let elements: Vec<Vec<u8>> = list.drain(..actual_count).collect();
                        let result: Vec<RespValue> = elements
                            .into_iter()
                            .map(|e| RespValue::BulkString(Some(e)))
                            .collect();

                        if list.is_empty() {
                            engine.del(key).await?;
                        } else {
                            let serialized = bincode::serialize(&list).map_err(|e| {
                                CommandError::InternalError(format!("Serialization error: {}", e))
                            })?;
                            engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;
                        }

                        Ok(RespValue::Array(Some(result)))
                    } else {
                        // Single-pop: return bulk string
                        let element = list.remove(0);

                        if list.is_empty() {
                            engine.del(key).await?;
                        } else {
                            let serialized = bincode::serialize(&list).map_err(|e| {
                                CommandError::InternalError(format!("Serialization error: {}", e))
                            })?;
                            engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;
                        }

                        Ok(RespValue::BulkString(Some(element)))
                    }
                },
                Err(_) => Err(CommandError::WrongType),
            }
        },
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Redis RPOP command - Remove and return the last element(s) of a list
pub async fn rpop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Parse optional count parameter (Redis 6.2+)
    let count = if args.len() == 2 {
        let count_str = bytes_to_string(&args[1])?;
        let c = count_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        if c < 0 {
            return Err(CommandError::InvalidArgument("value is not an integer or out of range".to_string()));
        }
        Some(c as usize)
    } else {
        None
    };

    // First, check if the key exists
    if engine.exists(key).await? {
        let key_type = engine.get_type(key).await?;
        if key_type != "list" {
            return Err(CommandError::WrongType);
        }
    } else {
        return Ok(RespValue::BulkString(None));
    }

    // Get the current list
    match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<Vec<Vec<u8>>>(&data) {
                Ok(mut list) => {
                    if list.is_empty() {
                        return Ok(RespValue::BulkString(None));
                    }

                    if let Some(count) = count {
                        // Multi-pop: return array (pop from the end)
                        let actual_count = std::cmp::min(count, list.len());
                        let start = list.len() - actual_count;
                        let elements: Vec<Vec<u8>> = list.drain(start..).rev().collect();
                        let result: Vec<RespValue> = elements
                            .into_iter()
                            .map(|e| RespValue::BulkString(Some(e)))
                            .collect();

                        if list.is_empty() {
                            engine.del(key).await?;
                        } else {
                            let serialized = bincode::serialize(&list).map_err(|e| {
                                CommandError::InternalError(format!("Serialization error: {}", e))
                            })?;
                            engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;
                        }

                        Ok(RespValue::Array(Some(result)))
                    } else {
                        // Single-pop: return bulk string
                        let element = list.pop().unwrap();

                        if list.is_empty() {
                            engine.del(key).await?;
                        } else {
                            let serialized = bincode::serialize(&list).map_err(|e| {
                                CommandError::InternalError(format!("Serialization error: {}", e))
                            })?;
                            engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;
                        }

                        Ok(RespValue::BulkString(Some(element)))
                    }
                },
                Err(_) => Err(CommandError::WrongType),
            }
        },
        None => Ok(RespValue::BulkString(None)),
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
        let raw = len + stop;
        if raw < 0 {
            // Negative result means the range is empty, clear the list
            list.clear();
            if list.is_empty() {
                engine.del(key).await?;
            }
            return Ok(RespValue::SimpleString("OK".to_string()));
        }
        raw as usize
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
        
        engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;
    }
    
    // LTRIM always returns OK
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis LPUSHX command - Push elements to the head of a list only if the key exists
pub async fn lpushx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];

    if !engine.exists(key).await? {
        return Ok(RespValue::Integer(0));
    }
    let key_type = engine.get_type(key).await?;
    if key_type != "list" {
        return Err(CommandError::WrongType);
    }

    let mut list = match engine.get(key).await? {
        Some(data) => bincode::deserialize::<Vec<Vec<u8>>>(&data)
            .map_err(|_| CommandError::WrongType)?,
        None => return Ok(RespValue::Integer(0)),
    };

    // Prepend all elements at once using splice for O(N+M) instead of O(N*M).
    // Redis LPUSHX pushes elements left-to-right, so the last arg ends up at the head.
    let new_elements: Vec<Vec<u8>> = args[1..].iter().rev().cloned().collect();
    list.splice(0..0, new_elements);

    let length = list.len();
    let serialized = bincode::serialize(&list)
        .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
    engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;

    Ok(RespValue::Integer(length as i64))
}

/// Redis RPUSHX command - Push elements to the tail of a list only if the key exists
pub async fn rpushx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];

    if !engine.exists(key).await? {
        return Ok(RespValue::Integer(0));
    }
    let key_type = engine.get_type(key).await?;
    if key_type != "list" {
        return Err(CommandError::WrongType);
    }

    let mut list = match engine.get(key).await? {
        Some(data) => bincode::deserialize::<Vec<Vec<u8>>>(&data)
            .map_err(|_| CommandError::WrongType)?,
        None => return Ok(RespValue::Integer(0)),
    };

    for element in &args[1..] {
        list.push(element.clone());
    }

    let length = list.len();
    let serialized = bincode::serialize(&list)
        .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
    engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;

    Ok(RespValue::Integer(length as i64))
}

/// Redis LINSERT command - Insert an element before or after a pivot element
pub async fn linsert(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let position = bytes_to_string(&args[1])?.to_uppercase();
    let pivot = &args[2];
    let element = &args[3];

    if position != "BEFORE" && position != "AFTER" {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }

    if !engine.exists(key).await? {
        return Ok(RespValue::Integer(0));
    }

    let key_type = engine.get_type(key).await?;
    if key_type != "list" {
        return Err(CommandError::WrongType);
    }

    let mut list = match engine.get(key).await? {
        Some(data) => bincode::deserialize::<Vec<Vec<u8>>>(&data)
            .map_err(|_| CommandError::WrongType)?,
        None => return Ok(RespValue::Integer(0)),
    };

    if let Some(idx) = list.iter().position(|e| e == pivot) {
        let insert_idx = if position == "BEFORE" { idx } else { idx + 1 };
        list.insert(insert_idx, element.clone());

        let length = list.len();
        let serialized = bincode::serialize(&list)
            .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
        engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;

        Ok(RespValue::Integer(length as i64))
    } else {
        Ok(RespValue::Integer(-1))
    }
}

/// Redis LSET command - Set the value of an element at an index
pub async fn lset(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let index_str = bytes_to_string(&args[1])?;
    let index = index_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let element = &args[2];

    if !engine.exists(key).await? {
        return Err(CommandError::InvalidArgument("no such key".to_string()));
    }

    let key_type = engine.get_type(key).await?;
    if key_type != "list" {
        return Err(CommandError::WrongType);
    }

    let mut list = match engine.get(key).await? {
        Some(data) => bincode::deserialize::<Vec<Vec<u8>>>(&data)
            .map_err(|_| CommandError::WrongType)?,
        None => return Err(CommandError::InvalidArgument("no such key".to_string())),
    };

    let len = list.len() as i64;
    let actual_index = if index < 0 { len + index } else { index };

    if actual_index < 0 || actual_index >= len {
        return Err(CommandError::InvalidArgument("index out of range".to_string()));
    }

    list[actual_index as usize] = element.clone();

    let serialized = bincode::serialize(&list)
        .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
    engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;

    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis LREM command - Remove elements from a list
/// LREM key count element
/// count > 0: Remove count elements equal to element, moving from head to tail.
/// count < 0: Remove |count| elements equal to element, moving from tail to head.
/// count = 0: Remove all elements equal to element.
pub async fn lrem(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let count = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let element = &args[2];

    let key_type = engine.get_type(key).await?;
    if key_type != "list" && key_type != "none" {
        return Err(CommandError::WrongType);
    }

    let mut list = match engine.get(key).await? {
        Some(data) => bincode::deserialize::<Vec<Vec<u8>>>(&data)
            .map_err(|_| CommandError::WrongType)?,
        None => return Ok(RespValue::Integer(0)),
    };

    let mut removed = 0i64;
    let max_removals = if count == 0 {
        list.len() as i64
    } else {
        count.abs()
    };

    if count >= 0 {
        // Remove from head to tail
        list.retain(|e| {
            if removed >= max_removals {
                return true;
            }
            if e == element {
                removed += 1;
                false
            } else {
                true
            }
        });
    } else {
        // Remove from tail to head: reverse, remove from head, reverse back
        list.reverse();
        list.retain(|e| {
            if removed >= max_removals {
                return true;
            }
            if e == element {
                removed += 1;
                false
            } else {
                true
            }
        });
        list.reverse();
    }

    if list.is_empty() {
        engine.del(key).await?;
    } else {
        let serialized = bincode::serialize(&list)
            .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
        engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;
    }

    Ok(RespValue::Integer(removed))
}

/// Redis LPOS command - Return the index of matching elements in a list
pub async fn lpos(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let element = &args[1];

    let mut rank = 1i64;
    let mut count: Option<i64> = None;
    let mut maxlen = 0usize;

    let mut i = 2;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "RANK" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::InvalidArgument("syntax error".to_string()));
                }
                i += 1;
                rank = bytes_to_string(&args[i])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if rank == 0 {
                    return Err(CommandError::InvalidArgument("RANK can't be zero: use a positive or negative rank".to_string()));
                }
            }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::InvalidArgument("syntax error".to_string()));
                }
                i += 1;
                count = Some(bytes_to_string(&args[i])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?);
                if count.unwrap() < 0 {
                    return Err(CommandError::InvalidArgument("COUNT can't be negative".to_string()));
                }
            }
            "MAXLEN" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::InvalidArgument("syntax error".to_string()));
                }
                i += 1;
                let ml = bytes_to_string(&args[i])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if ml < 0 {
                    return Err(CommandError::InvalidArgument("MAXLEN can't be negative".to_string()));
                }
                maxlen = ml as usize;
            }
            _ => return Err(CommandError::InvalidArgument("syntax error".to_string())),
        }
        i += 1;
    }

    if !engine.exists(key).await? {
        return Ok(if count.is_some() {
            RespValue::Array(Some(vec![]))
        } else {
            RespValue::BulkString(None)
        });
    }

    let key_type = engine.get_type(key).await?;
    if key_type != "list" {
        return Err(CommandError::WrongType);
    }

    let list = match engine.get(key).await? {
        Some(data) => bincode::deserialize::<Vec<Vec<u8>>>(&data)
            .map_err(|_| CommandError::WrongType)?,
        None => return Ok(if count.is_some() {
            RespValue::Array(Some(vec![]))
        } else {
            RespValue::BulkString(None)
        }),
    };

    let mut matches = Vec::new();

    if rank > 0 {
        // Forward scan
        let search_len = if maxlen > 0 { maxlen.min(list.len()) } else { list.len() };
        let mut found = 0i64;
        for idx in 0..search_len {
            if list[idx] == *element {
                found += 1;
                if found >= rank {
                    matches.push(idx as i64);
                    if let Some(c) = count {
                        if c > 0 && matches.len() >= c as usize {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    } else {
        // Reverse scan
        let abs_rank = rank.unsigned_abs();
        let start = if maxlen > 0 { list.len().saturating_sub(maxlen) } else { 0 };
        let mut found = 0u64;
        for idx in (start..list.len()).rev() {
            if list[idx] == *element {
                found += 1;
                if found >= abs_rank {
                    matches.push(idx as i64);
                    if let Some(c) = count {
                        if c > 0 && matches.len() >= c as usize {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    if count.is_some() {
        Ok(RespValue::Array(Some(
            matches.into_iter().map(RespValue::Integer).collect()
        )))
    } else {
        match matches.first() {
            Some(&idx) => Ok(RespValue::Integer(idx)),
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

/// Redis LMOVE command - Atomically pop from one list and push to another
pub async fn lmove(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let source = &args[0];
    let destination = &args[1];
    let wherefrom = bytes_to_string(&args[2])?.to_uppercase();
    let whereto = bytes_to_string(&args[3])?.to_uppercase();

    if !matches!(wherefrom.as_str(), "LEFT" | "RIGHT") {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }
    if !matches!(whereto.as_str(), "LEFT" | "RIGHT") {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }

    if !engine.exists(source).await? {
        return Ok(RespValue::BulkString(None));
    }
    let key_type = engine.get_type(source).await?;
    if key_type != "list" {
        return Err(CommandError::WrongType);
    }

    let mut src_list = match engine.get(source).await? {
        Some(data) => bincode::deserialize::<Vec<Vec<u8>>>(&data)
            .map_err(|_| CommandError::WrongType)?,
        None => return Ok(RespValue::BulkString(None)),
    };

    if src_list.is_empty() {
        return Ok(RespValue::BulkString(None));
    }

    // Pop from source
    let element = if wherefrom == "LEFT" {
        src_list.remove(0)
    } else {
        src_list.pop().unwrap()
    };

    if source == destination {
        // Same-key: pop and push in memory, save once to avoid intermediate state
        if whereto == "LEFT" {
            src_list.insert(0, element.clone());
        } else {
            src_list.push(element.clone());
        }

        let serialized = bincode::serialize(&src_list)
            .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
        engine.set_with_type_preserve_ttl(source.clone(), serialized, RedisDataType::List).await?;

        return Ok(RespValue::BulkString(Some(element)));
    }

    // Different keys: save source, then handle destination
    if src_list.is_empty() {
        engine.del(source).await?;
    } else {
        let serialized = bincode::serialize(&src_list)
            .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
        engine.set_with_type_preserve_ttl(source.clone(), serialized, RedisDataType::List).await?;
    }

    // Get or create destination list
    let mut dst_list = {
        if engine.exists(destination).await? {
            let dest_type = engine.get_type(destination).await?;
            if dest_type != "list" {
                return Err(CommandError::WrongType);
            }
        }
        match engine.get(destination).await? {
            Some(data) => bincode::deserialize::<Vec<Vec<u8>>>(&data)
                .map_err(|_| CommandError::WrongType)?,
            None => Vec::new(),
        }
    };

    // Push to destination
    if whereto == "LEFT" {
        dst_list.insert(0, element.clone());
    } else {
        dst_list.push(element.clone());
    }

    let serialized = bincode::serialize(&dst_list)
        .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
    engine.set_with_type_preserve_ttl(destination.clone(), serialized, RedisDataType::List).await?;

    Ok(RespValue::BulkString(Some(element)))
}

/// Redis RPOPLPUSH command (deprecated) - Pop from tail of source, push to head of destination
/// This is an alias for LMOVE source destination RIGHT LEFT
pub async fn rpoplpush(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    // Translate to LMOVE args: source, destination, RIGHT, LEFT
    let lmove_args = vec![
        args[0].clone(),
        args[1].clone(),
        b"RIGHT".to_vec(),
        b"LEFT".to_vec(),
    ];
    lmove(engine, &lmove_args).await
}

/// Redis LMPOP command - Pop elements from the first non-empty list
pub async fn lmpop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let numkeys_str = bytes_to_string(&args[0])?;
    let numkeys = numkeys_str.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    if numkeys == 0 {
        return Err(CommandError::InvalidArgument("numkeys can't be non-positive".to_string()));
    }

    if args.len() < 1 + numkeys + 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let keys = &args[1..1 + numkeys];
    let direction = bytes_to_string(&args[1 + numkeys])?.to_uppercase();

    if direction != "LEFT" && direction != "RIGHT" {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }

    let mut count = 1usize;
    let mut opt_idx = 2 + numkeys;
    if opt_idx < args.len() {
        let count_opt = bytes_to_string(&args[opt_idx])?.to_uppercase();
        if count_opt == "COUNT" {
            opt_idx += 1;
            if opt_idx >= args.len() {
                return Err(CommandError::WrongNumberOfArguments);
            }
            count = bytes_to_string(&args[opt_idx])?.parse::<usize>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?;
            if count == 0 {
                return Err(CommandError::InvalidArgument("COUNT value of 0 is not allowed".to_string()));
            }
        }
    }

    for key in keys {
        if !engine.exists(key).await? {
            continue;
        }
        let key_type = engine.get_type(key).await?;
        if key_type != "list" {
            return Err(CommandError::WrongType);
        }

        let mut list = match engine.get(key).await? {
            Some(data) => bincode::deserialize::<Vec<Vec<u8>>>(&data)
                .map_err(|_| CommandError::WrongType)?,
            None => continue,
        };

        if list.is_empty() {
            continue;
        }

        let pop_count = count.min(list.len());
        let mut popped = Vec::with_capacity(pop_count);

        if direction == "LEFT" {
            popped.extend(list.drain(..pop_count));
        } else {
            let start = list.len() - pop_count;
            popped.extend(list.drain(start..));
            popped.reverse();
        }

        if list.is_empty() {
            engine.del(key).await?;
        } else {
            let serialized = bincode::serialize(&list)
                .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
            engine.set_with_type_preserve_ttl(key.clone(), serialized, RedisDataType::List).await?;
        }

        let elements: Vec<RespValue> = popped.into_iter()
            .map(|e| RespValue::BulkString(Some(e)))
            .collect();

        return Ok(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(key.clone())),
            RespValue::Array(Some(elements)),
        ])));
    }

    Ok(RespValue::Array(None))
}

// --- Blocking Commands ---

/// Helper: try to pop an element from a list
async fn try_pop(engine: &StorageEngine, key: &[u8], left: bool) -> Result<Option<Vec<u8>>, CommandError> {
    if !engine.exists(key).await? {
        return Ok(None);
    }
    let key_type = engine.get_type(key).await?;
    if key_type != "list" {
        return Err(CommandError::WrongType);
    }

    let mut list = match engine.get(key).await? {
        Some(data) => bincode::deserialize::<Vec<Vec<u8>>>(&data)
            .map_err(|_| CommandError::WrongType)?,
        None => return Ok(None),
    };

    if list.is_empty() {
        return Ok(None);
    }

    let element = if left {
        list.remove(0)
    } else {
        list.pop().unwrap()
    };

    if list.is_empty() {
        engine.del(key).await?;
    } else {
        let serialized = bincode::serialize(&list)
            .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
        engine.set_with_type_preserve_ttl(key.to_vec(), serialized, RedisDataType::List).await?;
    }

    Ok(Some(element))
}

/// Core blocking pop implementation shared by BLPOP and BRPOP
async fn blocking_pop(
    engine: &StorageEngine,
    keys: &[Vec<u8>],
    timeout_secs: f64,
    left: bool,
    blocking_mgr: &BlockingManager,
) -> CommandResult {
    // Try immediate pop first
    for key in keys {
        match try_pop(engine, key, left).await? {
            Some(element) => {
                return Ok(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(key.clone())),
                    RespValue::BulkString(Some(element)),
                ])));
            }
            None => continue,
        }
    }

    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None // 0 means wait indefinitely
    };

    loop {
        // Subscribe to all keys before checking (avoids race condition)
        let receivers: Vec<_> = keys.iter()
            .map(|k| blocking_mgr.subscribe(k))
            .collect();

        // Try to pop from each key
        for key in keys {
            match try_pop(engine, key, left).await? {
                Some(element) => {
                    return Ok(RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.clone())),
                        RespValue::BulkString(Some(element)),
                    ])));
                }
                None => continue,
            }
        }

        // Wait for any key to get data or timeout
        let futs: Vec<_> = receivers.into_iter()
            .map(|mut rx| Box::pin(async move { let _ = rx.changed().await; }))
            .collect();

        if futs.is_empty() {
            return Ok(RespValue::Array(None));
        }

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(RespValue::Array(None));
            }
            if tokio::time::timeout(remaining, select_all(futs)).await.is_err() {
                return Ok(RespValue::Array(None));
            }
        } else {
            let _ = select_all(futs).await;
        }
    }
}

/// Redis BLPOP command - Blocking left pop with timeout
pub async fn blpop(engine: &StorageEngine, args: &[Vec<u8>], blocking_mgr: &BlockingManager) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let timeout_str = bytes_to_string(args.last().unwrap())?;
    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument("timeout is negative".to_string()));
    }

    let keys = &args[..args.len() - 1];
    blocking_pop(engine, keys, timeout_secs, true, blocking_mgr).await
}

/// Redis BRPOP command - Blocking right pop with timeout
pub async fn brpop(engine: &StorageEngine, args: &[Vec<u8>], blocking_mgr: &BlockingManager) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let timeout_str = bytes_to_string(args.last().unwrap())?;
    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument("timeout is negative".to_string()));
    }

    let keys = &args[..args.len() - 1];
    blocking_pop(engine, keys, timeout_secs, false, blocking_mgr).await
}

/// Redis BLMOVE command - Blocking LMOVE with timeout
pub async fn blmove(engine: &StorageEngine, args: &[Vec<u8>], blocking_mgr: &BlockingManager) -> CommandResult {
    if args.len() != 5 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let source = &args[0];
    let destination = &args[1];
    let wherefrom = bytes_to_string(&args[2])?.to_uppercase();
    let whereto = bytes_to_string(&args[3])?.to_uppercase();
    let timeout_str = bytes_to_string(&args[4])?;

    if !matches!(wherefrom.as_str(), "LEFT" | "RIGHT") {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }
    if !matches!(whereto.as_str(), "LEFT" | "RIGHT") {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }

    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument("timeout is negative".to_string()));
    }

    // Try immediately first
    let lmove_args = vec![source.clone(), destination.clone(), args[2].clone(), args[3].clone()];
    let result = lmove(engine, &lmove_args).await?;
    if result != RespValue::BulkString(None) {
        return Ok(result);
    }

    // Block until data is available
    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None
    };

    loop {
        let receivers: Vec<_> = vec![blocking_mgr.subscribe(source)];

        // Try again
        let result = lmove(engine, &lmove_args).await?;
        if result != RespValue::BulkString(None) {
            return Ok(result);
        }

        let futs: Vec<_> = receivers.into_iter()
            .map(|mut rx| Box::pin(async move { let _ = rx.changed().await; }))
            .collect();

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(RespValue::BulkString(None));
            }
            if tokio::time::timeout(remaining, select_all(futs)).await.is_err() {
                return Ok(RespValue::BulkString(None));
            }
        } else {
            let _ = select_all(futs).await;
        }
    }
}

/// Redis BLMPOP command - Blocking LMPOP with timeout
pub async fn blmpop(engine: &StorageEngine, args: &[Vec<u8>], blocking_mgr: &BlockingManager) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let timeout_str = bytes_to_string(&args[0])?;
    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument("timeout is negative".to_string()));
    }

    // Parse remaining args as LMPOP args (numkeys key... direction [COUNT count])
    let lmpop_args = &args[1..];

    // Try immediately
    let result = lmpop(engine, lmpop_args).await?;
    if result != RespValue::Array(None) {
        return Ok(result);
    }

    // Parse keys for subscription
    let numkeys_str = bytes_to_string(&lmpop_args[0])?;
    let numkeys = numkeys_str.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    if numkeys == 0 || lmpop_args.len() < 1 + numkeys + 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let keys = &lmpop_args[1..1 + numkeys];

    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None
    };

    loop {
        let receivers: Vec<_> = keys.iter()
            .map(|k| blocking_mgr.subscribe(k))
            .collect();

        let result = lmpop(engine, lmpop_args).await?;
        if result != RespValue::Array(None) {
            return Ok(result);
        }

        let futs: Vec<_> = receivers.into_iter()
            .map(|mut rx| Box::pin(async move { let _ = rx.changed().await; }))
            .collect();

        if futs.is_empty() {
            return Ok(RespValue::Array(None));
        }

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(RespValue::Array(None));
            }
            if tokio::time::timeout(remaining, select_all(futs)).await.is_err() {
                return Ok(RespValue::Array(None));
            }
        } else {
            let _ = select_all(futs).await;
        }
    }
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
        
        // Create a list directly first using set_with_type to properly track type
        let empty_list: Vec<Vec<u8>> = Vec::new();
        let serialized = bincode::serialize(&empty_list).unwrap();
        engine.set_with_type(key.clone(), serialized, crate::storage::item::RedisDataType::List, None).await.unwrap();
        
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
        
        // Create a list directly first using set_with_type to properly track type
        let empty_list: Vec<Vec<u8>> = Vec::new();
        let serialized = bincode::serialize(&empty_list).unwrap();
        engine.set_with_type(key.clone(), serialized, crate::storage::item::RedisDataType::List, None).await.unwrap();
        
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

    #[tokio::test]
    async fn test_lpushx_rpushx() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // LPUSHX on non-existent key returns 0
        let args = vec![b"mylist".to_vec(), b"a".to_vec()];
        let result = lpushx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Create the list first
        rpush(&engine, &[b"mylist".to_vec(), b"x".to_vec()]).await.unwrap();

        // LPUSHX on existing list works
        let result = lpushx(&engine, &[b"mylist".to_vec(), b"a".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        // RPUSHX on non-existent key returns 0
        let result = rpushx(&engine, &[b"nolist".to_vec(), b"a".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // RPUSHX on existing list works
        let result = rpushx(&engine, &[b"mylist".to_vec(), b"z".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Verify order: [a, x, z]
        let result = lrange(&engine, &[b"mylist".to_vec(), b"0".to_vec(), b"-1".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"x".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"z".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_linsert() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create list [a, b, d]
        rpush(&engine, &[b"mylist".to_vec(), b"a".to_vec(), b"b".to_vec(), b"d".to_vec()]).await.unwrap();

        // Insert "c" BEFORE "d"
        let result = linsert(&engine, &[b"mylist".to_vec(), b"BEFORE".to_vec(), b"d".to_vec(), b"c".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(4));

        // Insert "e" AFTER "d"
        let result = linsert(&engine, &[b"mylist".to_vec(), b"AFTER".to_vec(), b"d".to_vec(), b"e".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));

        // Verify: [a, b, c, d, e]
        let result = lrange(&engine, &[b"mylist".to_vec(), b"0".to_vec(), b"-1".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 5);
            assert_eq!(values[2], RespValue::BulkString(Some(b"c".to_vec())));
            assert_eq!(values[4], RespValue::BulkString(Some(b"e".to_vec())));
        } else {
            panic!("Expected array");
        }

        // Pivot not found returns -1
        let result = linsert(&engine, &[b"mylist".to_vec(), b"BEFORE".to_vec(), b"z".to_vec(), b"x".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(-1));

        // Non-existent key returns 0
        let result = linsert(&engine, &[b"nolist".to_vec(), b"BEFORE".to_vec(), b"a".to_vec(), b"b".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_lset() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create list [a, b, c]
        rpush(&engine, &[b"mylist".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]).await.unwrap();

        // Set index 1 to "B"
        let result = lset(&engine, &[b"mylist".to_vec(), b"1".to_vec(), b"B".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify
        let result = lindex(&engine, &[b"mylist".to_vec(), b"1".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"B".to_vec())));

        // Negative index
        let result = lset(&engine, &[b"mylist".to_vec(), b"-1".to_vec(), b"C".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = lindex(&engine, &[b"mylist".to_vec(), b"-1".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"C".to_vec())));

        // Out of range index
        let result = lset(&engine, &[b"mylist".to_vec(), b"10".to_vec(), b"x".to_vec()]).await;
        assert!(result.is_err());

        // Non-existent key
        let result = lset(&engine, &[b"nolist".to_vec(), b"0".to_vec(), b"x".to_vec()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_lpos() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create list [a, b, c, b, a]
        rpush(&engine, &[b"mylist".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]).await.unwrap();

        // Find first "b"
        let result = lpos(&engine, &[b"mylist".to_vec(), b"b".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Find with COUNT 0 (all occurrences)
        let result = lpos(&engine, &[b"mylist".to_vec(), b"b".to_vec(), b"COUNT".to_vec(), b"0".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values, vec![RespValue::Integer(1), RespValue::Integer(3)]);
        } else {
            panic!("Expected array");
        }

        // Find with RANK 2 (second occurrence)
        let result = lpos(&engine, &[b"mylist".to_vec(), b"b".to_vec(), b"RANK".to_vec(), b"2".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Find with negative RANK (from end)
        let result = lpos(&engine, &[b"mylist".to_vec(), b"a".to_vec(), b"RANK".to_vec(), b"-1".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(4));

        // Not found returns nil
        let result = lpos(&engine, &[b"mylist".to_vec(), b"z".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Non-existent key returns nil
        let result = lpos(&engine, &[b"nolist".to_vec(), b"a".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // With MAXLEN
        let result = lpos(&engine, &[b"mylist".to_vec(), b"a".to_vec(), b"COUNT".to_vec(), b"0".to_vec(), b"MAXLEN".to_vec(), b"3".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values, vec![RespValue::Integer(0)]);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_lmove() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create source [a, b, c]
        rpush(&engine, &[b"src".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]).await.unwrap();

        // LMOVE src dst LEFT RIGHT -> moves "a" to end of dst
        let result = lmove(&engine, &[b"src".to_vec(), b"dst".to_vec(), b"LEFT".to_vec(), b"RIGHT".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"a".to_vec())));

        // src should be [b, c]
        let result = lrange(&engine, &[b"src".to_vec(), b"0".to_vec(), b"-1".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array");
        }

        // dst should be [a]
        let result = lrange(&engine, &[b"dst".to_vec(), b"0".to_vec(), b"-1".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
        } else {
            panic!("Expected array");
        }

        // Non-existent source returns nil
        let result = lmove(&engine, &[b"nolist".to_vec(), b"dst".to_vec(), b"LEFT".to_vec(), b"RIGHT".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Same key rotation: src [b, c] -> LMOVE src src LEFT RIGHT -> src [c, b]
        let result = lmove(&engine, &[b"src".to_vec(), b"src".to_vec(), b"LEFT".to_vec(), b"RIGHT".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"b".to_vec())));

        let result = lrange(&engine, &[b"src".to_vec(), b"0".to_vec(), b"-1".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::BulkString(Some(b"c".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_lmpop() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create lists
        rpush(&engine, &[b"list1".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]).await.unwrap();
        rpush(&engine, &[b"list2".to_vec(), b"x".to_vec(), b"y".to_vec()]).await.unwrap();

        // Pop 1 from left of first non-empty key
        let result = lmpop(&engine, &[b"2".to_vec(), b"list1".to_vec(), b"list2".to_vec(), b"LEFT".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"list1".to_vec())));
            if let RespValue::Array(Some(elements)) = &values[1] {
                assert_eq!(elements.len(), 1);
                assert_eq!(elements[0], RespValue::BulkString(Some(b"a".to_vec())));
            } else {
                panic!("Expected elements array");
            }
        } else {
            panic!("Expected array");
        }

        // Pop 2 from right
        let result = lmpop(&engine, &[b"2".to_vec(), b"list1".to_vec(), b"list2".to_vec(), b"RIGHT".to_vec(), b"COUNT".to_vec(), b"2".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"list1".to_vec())));
            if let RespValue::Array(Some(elements)) = &values[1] {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], RespValue::BulkString(Some(b"c".to_vec())));
                assert_eq!(elements[1], RespValue::BulkString(Some(b"b".to_vec())));
            } else {
                panic!("Expected elements array");
            }
        } else {
            panic!("Expected array");
        }

        // list1 is now empty, so next pop should come from list2
        let result = lmpop(&engine, &[b"2".to_vec(), b"list1".to_vec(), b"list2".to_vec(), b"LEFT".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"list2".to_vec())));
        } else {
            panic!("Expected array");
        }

        // All empty returns nil
        let result = lmpop(&engine, &[b"1".to_vec(), b"emptylist".to_vec(), b"LEFT".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Array(None));
    }

    #[tokio::test]
    async fn test_blpop_immediate() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        // Create list with data
        rpush(&engine, &[b"mylist".to_vec(), b"hello".to_vec()]).await.unwrap();

        // BLPOP should return immediately
        let result = blpop(&engine, &[b"mylist".to_vec(), b"1".to_vec()], &blocking_mgr).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"mylist".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"hello".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_blpop_timeout() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        // BLPOP on empty list should timeout
        let start = Instant::now();
        let result = blpop(&engine, &[b"emptylist".to_vec(), b"0.1".to_vec()], &blocking_mgr).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(result, RespValue::Array(None));
        assert!(elapsed >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn test_blpop_wakeup() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        let blocking_mgr = Arc::new(BlockingManager::new());

        let engine_clone = engine.clone();
        let mgr_clone = blocking_mgr.clone();

        // Spawn BLPOP in background
        let handle = tokio::spawn(async move {
            blpop(&engine_clone, &[b"wakekey".to_vec(), b"5".to_vec()], &mgr_clone).await
        });

        // Give it time to start waiting
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Push data to wake it up
        rpush(&engine, &[b"wakekey".to_vec(), b"wakedata".to_vec()]).await.unwrap();
        blocking_mgr.notify_key(b"wakekey");

        let result = handle.await.unwrap().unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"wakekey".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"wakedata".to_vec())));
        } else {
            panic!("Expected array, got {:?}", result);
        }
    }

    #[tokio::test]
    async fn test_brpop_immediate() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        rpush(&engine, &[b"mylist".to_vec(), b"a".to_vec(), b"b".to_vec()]).await.unwrap();

        let result = brpop(&engine, &[b"mylist".to_vec(), b"1".to_vec()], &blocking_mgr).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"mylist".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_blmove_immediate() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        rpush(&engine, &[b"src".to_vec(), b"a".to_vec(), b"b".to_vec()]).await.unwrap();

        let result = blmove(&engine, &[b"src".to_vec(), b"dst".to_vec(), b"LEFT".to_vec(), b"RIGHT".to_vec(), b"1".to_vec()], &blocking_mgr).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"a".to_vec())));

        // Verify dst has the element
        let result = lrange(&engine, &[b"dst".to_vec(), b"0".to_vec(), b"-1".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_blmove_timeout() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        let start = Instant::now();
        let result = blmove(&engine, &[b"empty".to_vec(), b"dst".to_vec(), b"LEFT".to_vec(), b"RIGHT".to_vec(), b"0.1".to_vec()], &blocking_mgr).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(result, RespValue::BulkString(None));
        assert!(elapsed >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn test_blmpop_immediate() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        rpush(&engine, &[b"mylist".to_vec(), b"a".to_vec(), b"b".to_vec()]).await.unwrap();

        // BLMPOP timeout numkeys key LEFT
        let result = blmpop(&engine, &[b"1".to_vec(), b"1".to_vec(), b"mylist".to_vec(), b"LEFT".to_vec()], &blocking_mgr).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"mylist".to_vec())));
            if let RespValue::Array(Some(elements)) = &values[1] {
                assert_eq!(elements[0], RespValue::BulkString(Some(b"a".to_vec())));
            } else {
                panic!("Expected elements array");
            }
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_blmpop_timeout() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        let start = Instant::now();
        let result = blmpop(&engine, &[b"0.1".to_vec(), b"1".to_vec(), b"emptylist".to_vec(), b"LEFT".to_vec()], &blocking_mgr).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(result, RespValue::Array(None));
        assert!(elapsed >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn test_blpop_multi_key() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        // Only second key has data
        rpush(&engine, &[b"list2".to_vec(), b"hello".to_vec()]).await.unwrap();

        let result = blpop(&engine, &[b"list1".to_vec(), b"list2".to_vec(), b"1".to_vec()], &blocking_mgr).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"list2".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"hello".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_concurrent_lpush_same_list() {
        // Two clients racing LPUSH on the same list - no elements should be lost
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let mut handles = Vec::new();
        for i in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..10 {
                    let args = vec![
                        b"race_list".to_vec(),
                        format!("val_{}_{}", i, j).into_bytes(),
                    ];
                    lpush(&eng, &args).await.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // All 100 elements should be present
        let result = llen(&engine, &[b"race_list".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(100));
    }
}