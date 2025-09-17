use crate::command::{CommandError, CommandResult};
use crate::command::utils::bytes_to_string;
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::item::RedisDataType;

// We'll use a vector to store score-member pairs, sorted by score
type SortedSet = Vec<(f64, Vec<u8>)>;

/// Redis ZADD command - Add members to a sorted set with scores
pub async fn zadd(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Parse options - check for NX, XX, etc.
    let mut only_if_new = false; // NX option
    let mut only_if_exists = false; // XX option
    let mut i = 1;
    
    // Process options
    while i < args.len() {
        let option = bytes_to_string(&args[i])?;
        match option.to_uppercase().as_str() {
            "NX" => {
                only_if_new = true;
                i += 1;
            },
            "XX" => {
                only_if_exists = true;
                i += 1;
            },
            _ => {
                // Not an option, continue to score-member pairs
                break;
            }
        }
    }
    
    // Check for conflicting options
    if only_if_new && only_if_exists {
        return Err(CommandError::InvalidArgument("NX and XX options cannot be used together".to_string()));
    }
    
    // Now check that we have score-member pairs
    if (args.len() - i) < 2 || (args.len() - i) % 2 != 0 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    // Get the current sorted set or create a new one
    let mut sorted_set = match engine.get(key).await? {
        Some(data) => {
            bincode::deserialize::<SortedSet>(&data).unwrap_or_default()
        },
        None => Vec::new(),
    };
    
    // Track how many new members were added
    let mut added = 0;
    
    // Process score-member pairs
    while i < args.len() {
        if i + 1 >= args.len() {
            return Err(CommandError::WrongNumberOfArguments);
        }
        
        let score_bytes = &args[i];
        let member = args[i + 1].clone();
        i += 2;
        
        // Parse the score
        let score_str = bytes_to_string(score_bytes)?;
        let score = score_str.parse::<f64>().map_err(|_| {
            CommandError::InvalidArgument("Score is not a valid float".to_string())
        })?;
        
        // Check if member already exists 
        let existing_index = sorted_set.iter().position(|(_, m)| m == &member);
        
        if let Some(index) = existing_index {
            // Member exists already
            if !only_if_new {
                // Update the existing member if NX is not set
                sorted_set[index].0 = score;
            }
            // With NX, we skip updating existing members
        } else {
            // Member doesn't exist
            if !only_if_exists {
                // Add new member if XX is not set
                sorted_set.push((score, member));
                added += 1;
            }
            // With XX, we skip adding new members
        }
    }
    
    // Sort the set by score
    sorted_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
    // Serialize and save the sorted set
    let serialized = bincode::serialize(&sorted_set).map_err(|e| {
        CommandError::InternalError(format!("Serialization error: {}", e))
    })?;
    
    engine.set_with_type(key.clone(), serialized, RedisDataType::ZSet, None).await?;
    
    Ok(RespValue::Integer(added))
}

/// Redis ZRANGE command - Return a range of members in a sorted set by index
pub async fn zrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len() > 4 {
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
    
    // Check for WITHSCORES option
    let with_scores = args.len() == 4 && 
        bytes_to_string(&args[3]).map(|s| s.to_uppercase() == "WITHSCORES").unwrap_or(false);
    
    // Get the sorted set
    let sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(mut sorted_set) => {
                    // Sort by score for consistency with Redis
                    sorted_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                    sorted_set
                },
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist, return empty array (Redis behavior)
            return Ok(RespValue::Array(Some(vec![])));
        },
    };
    
    // Get the total number of elements
    let len = sorted_set.len() as i64;
    
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
    if start_idx <= stop_idx && start_idx < sorted_set.len() {
        // Calculate the actual range length
        let actual_stop_idx = stop_idx.min(sorted_set.len() - 1);
        
        // Pre-allocate vector with appropriate capacity
        // If with_scores is true, we'll have twice as many items
        let range_len = actual_stop_idx - start_idx + 1;
        let capacity = if with_scores { range_len * 2 } else { range_len };
        result = Vec::with_capacity(capacity);
        
        for (score, member) in sorted_set.iter().take(actual_stop_idx + 1).skip(start_idx) {
            // Always add the member
            result.push(RespValue::BulkString(Some(member.clone())));
            
            // If WITHSCORES option is present, add the score after each member as a separate array item
            if with_scores {
                // Format score as string with proper format
                let score_str = format!("{}", score);
                result.push(RespValue::BulkString(Some(score_str.into_bytes())));
            }
        }
    }
    
    // ZRANGE always returns an array (Redis behavior)
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZREM command - Remove members from a sorted set
pub async fn zrem(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let members = &args[1..];
    
    // Get the current sorted set
    let mut sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist, nothing to remove
            return Ok(RespValue::Integer(0));
        },
    };
    
    // Track how many members were removed
    let mut removed = 0;
    
    // Remove each member
    for member in members {
        let old_len = sorted_set.len();
        sorted_set.retain(|(_, m)| m != member);
        if sorted_set.len() < old_len {
            removed += 1;
        }
    }
    
    if removed > 0 {
        // If the sorted set is now empty, remove the key
        if sorted_set.is_empty() {
            engine.del(key).await?;
        } else {
            // Otherwise, update the sorted set
            let serialized = bincode::serialize(&sorted_set).map_err(|e| {
                CommandError::InternalError(format!("Serialization error: {}", e))
            })?;
            
            engine.set_with_type(key.clone(), serialized, RedisDataType::ZSet, None).await?;
        }
    }
    
    Ok(RespValue::Integer(removed))
}

/// Redis ZSCORE command - Get the score of a member in a sorted set
pub async fn zscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let member = &args[1];
    
    // Get the sorted set
    let sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist
            return Ok(RespValue::BulkString(None));
        },
    };
    
    // Find the score for this member
    for (score, m) in sorted_set.iter() {
        if m == member {
            // Convert score to string with proper formatting
            let score_str = format!("{}", score);
            return Ok(RespValue::BulkString(Some(score_str.into_bytes())));
        }
    }
    
    // Member not found
    Ok(RespValue::BulkString(None))
}

/// Redis ZCARD command - Get the cardinality (number of members) of a sorted set
pub async fn zcard(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Get the sorted set
    let sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist, return 0
            return Ok(RespValue::Integer(0));
        },
    };
    
    Ok(RespValue::Integer(sorted_set.len() as i64))
}

/// Redis ZCOUNT command - Count members in a sorted set within a score range
pub async fn zcount(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let min_str = bytes_to_string(&args[1])?;
    let max_str = bytes_to_string(&args[2])?;
    
    // Parse min and max scores (supporting -inf and +inf)
    let min_score = if min_str == "-inf" {
        f64::NEG_INFINITY
    } else if min_str == "+inf" {
        f64::INFINITY
    } else {
        min_str.parse::<f64>().map_err(|_| {
            CommandError::InvalidArgument("Min score is not a valid float".to_string())
        })?
    };
    
    let max_score = if max_str == "-inf" {
        f64::NEG_INFINITY
    } else if max_str == "+inf" {
        f64::INFINITY
    } else {
        max_str.parse::<f64>().map_err(|_| {
            CommandError::InvalidArgument("Max score is not a valid float".to_string())
        })?
    };
    
    // Get the sorted set
    let sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist, return 0
            return Ok(RespValue::Integer(0));
        },
    };
    
    // Count members within the score range
    let count = sorted_set.iter()
        .filter(|(score, _)| *score >= min_score && *score <= max_score)
        .count();
    
    Ok(RespValue::Integer(count as i64))
}

/// Redis ZRANK command - Get the rank (index) of a member in a sorted set
pub async fn zrank(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let member = &args[1];
    
    // Get the sorted set
    let mut sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist
            return Ok(RespValue::BulkString(None));
        },
    };
    
    // Sort by score (ascending order)
    sorted_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
    // Find the rank (0-based index) of the member
    for (index, (_, m)) in sorted_set.iter().enumerate() {
        if m == member {
            return Ok(RespValue::Integer(index as i64));
        }
    }
    
    // Member not found
    Ok(RespValue::BulkString(None))
}

/// Redis ZREVRANGE command - Return a range of members in a sorted set by index (in reverse order)
pub async fn zrevrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len() > 4 {
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
    
    // Check for WITHSCORES option
    let with_scores = args.len() == 4 && 
        bytes_to_string(&args[3]).map(|s| s.to_uppercase() == "WITHSCORES").unwrap_or(false);
    
    // Get the sorted set
    let mut sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(mut sorted_set) => {
                    // Sort by score for consistency with Redis
                    sorted_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                    sorted_set
                },
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist, return empty array (Redis behavior)
            return Ok(RespValue::Array(Some(vec![])));
        },
    };
    
    // Reverse the sorted set for descending order
    sorted_set.reverse();
    
    // Get the total number of elements
    let len = sorted_set.len() as i64;
    
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
    if start_idx <= stop_idx && start_idx < sorted_set.len() {
        // Calculate the actual range length
        let actual_stop_idx = stop_idx.min(sorted_set.len() - 1);
        
        // Pre-allocate vector with appropriate capacity
        // If with_scores is true, we'll have twice as many items
        let range_len = actual_stop_idx - start_idx + 1;
        let capacity = if with_scores { range_len * 2 } else { range_len };
        result = Vec::with_capacity(capacity);
        
        for (score, member) in sorted_set.iter().take(actual_stop_idx + 1).skip(start_idx) {
            // Always add the member
            result.push(RespValue::BulkString(Some(member.clone())));
            
            // If WITHSCORES option is present, add the score after each member as a separate array item
            if with_scores {
                // Format score as string with proper format
                let score_str = format!("{}", score);
                result.push(RespValue::BulkString(Some(score_str.into_bytes())));
            }
        }
    }
    
    // ZREVRANGE always returns an array (Redis behavior)
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZINCRBY command - Increment the score of a member in a sorted set
pub async fn zincrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let increment_str = bytes_to_string(&args[1])?;
    let member = &args[2];
    
    // Parse the increment value
    let increment = increment_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("Increment is not a valid float".to_string())
    })?;
    
    // Get the current sorted set or create a new one
    let mut sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => Vec::new(),
    };
    
    // Find if member already exists and update its score
    let mut found = false;
    let mut new_score = increment;
    
    for (score, m) in sorted_set.iter_mut() {
        if m == member {
            *score += increment;
            new_score = *score;
            found = true;
            break;
        }
    }
    
    // If member doesn't exist, add it with the increment as its score
    if !found {
        sorted_set.push((increment, member.clone()));
        new_score = increment;
    }
    
    // Sort the set by score
    sorted_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
    // Serialize and save the sorted set
    let serialized = bincode::serialize(&sorted_set).map_err(|e| {
        CommandError::InternalError(format!("Serialization error: {}", e))
    })?;
    
    engine.set_with_type(key.clone(), serialized, RedisDataType::ZSet, None).await?;
    
    // Return the new score
    let score_str = format!("{}", new_score);
    Ok(RespValue::BulkString(Some(score_str.into_bytes())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;
    
    #[tokio::test]
    async fn test_sorted_set_commands() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test ZADD
        let zadd_args = vec![
            b"zset1".to_vec(),
            b"1.5".to_vec(), b"member1".to_vec(),
            b"2.5".to_vec(), b"member2".to_vec(),
            b"3.5".to_vec(), b"member3".to_vec(),
        ];
        let result = zadd(&engine, &zadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3)); // Added 3 members
        
        // Test ZSCORE
        let zscore_args = vec![b"zset1".to_vec(), b"member2".to_vec()];
        let result = zscore(&engine, &zscore_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"2.5".to_vec())));
        
        // Test ZRANGE
        let zrange_args = vec![b"zset1".to_vec(), b"0".to_vec(), b"1".to_vec()];
        let result = zrange(&engine, &zrange_args).await.unwrap();
        if let RespValue::Array(Some(elements)) = result {
            assert_eq!(elements.len(), 2);
            assert_eq!(elements[0], RespValue::BulkString(Some(b"member1".to_vec())));
            assert_eq!(elements[1], RespValue::BulkString(Some(b"member2".to_vec())));
        } else {
            panic!("Expected array response from ZRANGE");
        }
        
        // Test ZRANGE with WITHSCORES
        let zrange_args = vec![
            b"zset1".to_vec(), 
            b"0".to_vec(), 
            b"1".to_vec(),
            b"WITHSCORES".to_vec(),
        ];
        let result = zrange(&engine, &zrange_args).await.unwrap();
        if let RespValue::Array(Some(elements)) = result {
            assert_eq!(elements.len(), 4);
            assert_eq!(elements[0], RespValue::BulkString(Some(b"member1".to_vec())));
            assert_eq!(elements[1], RespValue::BulkString(Some(b"1.5".to_vec())));
            assert_eq!(elements[2], RespValue::BulkString(Some(b"member2".to_vec())));
            assert_eq!(elements[3], RespValue::BulkString(Some(b"2.5".to_vec())));
        } else {
            panic!("Expected array response from ZRANGE");
        }
        
        // Test ZREM
        let zrem_args = vec![
            b"zset1".to_vec(),
            b"member1".to_vec(),
            b"nonexistent".to_vec(),
        ];
        let result = zrem(&engine, &zrem_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Removed 1 member
        
        // Verify that the member was removed
        let zscore_args = vec![b"zset1".to_vec(), b"member1".to_vec()];
        let result = zscore(&engine, &zscore_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None)); // No longer exists
    }
    
    // Test ZRANGE with WITHSCORES option to ensure proper formatting
    #[tokio::test]
    async fn test_zrange_withscores_format() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Set up a sorted set
        let zadd_args = vec![
            b"zset_withscores".to_vec(),
            b"1.5".to_vec(), b"one".to_vec(),
            b"2.5".to_vec(), b"two".to_vec(),
            b"3.5".to_vec(), b"three".to_vec(),
        ];
        let _ = zadd(&engine, &zadd_args).await.unwrap();
        
        // Test ZRANGE with WITHSCORES
        let zrange_args = vec![
            b"zset_withscores".to_vec(),
            b"0".to_vec(),
            b"-1".to_vec(),
            b"WITHSCORES".to_vec(),
        ];
        let result = zrange(&engine, &zrange_args).await.unwrap();
        
        if let RespValue::Array(Some(items)) = result {
            // Each member-score pair should be two separate items
            assert_eq!(items.len(), 6);
            
            // Check first member-score pair
            assert_eq!(items[0], RespValue::BulkString(Some(b"one".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"1.5".to_vec())));
            
            // Check second member-score pair
            assert_eq!(items[2], RespValue::BulkString(Some(b"two".to_vec())));
            assert_eq!(items[3], RespValue::BulkString(Some(b"2.5".to_vec())));
            
            // Check third member-score pair
            assert_eq!(items[4], RespValue::BulkString(Some(b"three".to_vec())));
            assert_eq!(items[5], RespValue::BulkString(Some(b"3.5".to_vec())));
        } else {
            panic!("Expected array response");
        }
    }
    
    // Test ZRANGE with large sorted sets
    #[tokio::test]
    async fn test_zrange_large_sorted_set() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Create a large sorted set
        let key = b"large_zset".to_vec();
        
        // Add 2000 members
        for i in 0..2000 {
            let score = i as f64;
            let member = format!("member-{}", i).into_bytes();
            
            let args = vec![
                key.clone(),
                score.to_string().into_bytes(),
                member,
            ];
            
            zadd(&engine, &args).await.unwrap();
        }
        
        // Retrieve a range that crosses chunk boundaries
        let zrange_args = vec![
            key.clone(),
            b"990".to_vec(),
            b"1010".to_vec(),
            b"WITHSCORES".to_vec(),
        ];
        
        let result = zrange(&engine, &zrange_args).await.unwrap();
        if let RespValue::Array(Some(elements)) = result {
            // Should be 42 elements (21 members with scores)
            assert_eq!(elements.len(), 42);
            
            // Check first and last elements
            assert_eq!(elements[0], RespValue::BulkString(Some(b"member-990".to_vec())));
            assert_eq!(elements[1], RespValue::BulkString(Some(b"990".to_vec())));
            
            assert_eq!(elements[40], RespValue::BulkString(Some(b"member-1010".to_vec())));
            assert_eq!(elements[41], RespValue::BulkString(Some(b"1010".to_vec())));
        } else {
            panic!("Expected array response from ZRANGE");
        }
    }
    
    // Test ZRANGE with WITHSCORES in different positions
    #[tokio::test]
    async fn test_zrange_withscores_positions() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Create a test sorted set
        let key = b"zs_withscores_position".to_vec();
        let zadd_args = vec![
            key.clone(),
            b"1".to_vec(), b"one".to_vec(),
            b"2".to_vec(), b"two".to_vec(),
            b"3".to_vec(), b"three".to_vec(),
        ];
        
        let _ = zadd(&engine, &zadd_args).await.unwrap();
        
        // Test basic WITHSCORES as 4th argument
        let zrange_args1 = vec![
            key.clone(),
            b"0".to_vec(),
            b"2".to_vec(),
            b"WITHSCORES".to_vec(),
        ];
        
        let result1 = zrange(&engine, &zrange_args1).await.unwrap();
        if let RespValue::Array(Some(elements)) = result1 {
            assert_eq!(elements.len(), 6); // 3 members with scores = 6 elements
        } else {
            panic!("Expected array response from ZRANGE");
        }
        
        // Test case-insensitive WITHSCORES
        let zrange_args2 = vec![
            key.clone(),
            b"0".to_vec(),
            b"2".to_vec(),
            b"wItHsCoReS".to_vec(), // Mixed case
        ];
        
        let result2 = zrange(&engine, &zrange_args2).await.unwrap();
        if let RespValue::Array(Some(elements)) = result2 {
            assert_eq!(elements.len(), 6); // Still 3 members with scores = 6 elements
        } else {
            panic!("Expected array response from ZRANGE");
        }
    }
    
    // Test ZADD with many score-member pairs
    #[tokio::test]
    async fn test_zadd_multiple_pairs() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test ZADD with multiple score-member pairs in a single command (10 pairs)
        let zadd_args = vec![
            b"multi_zset".to_vec(),
            b"1.0".to_vec(), b"member1".to_vec(),
            b"2.0".to_vec(), b"member2".to_vec(),
            b"3.0".to_vec(), b"member3".to_vec(),
            b"4.0".to_vec(), b"member4".to_vec(),
            b"5.0".to_vec(), b"member5".to_vec(),
            b"6.0".to_vec(), b"member6".to_vec(),
            b"7.0".to_vec(), b"member7".to_vec(),
            b"8.0".to_vec(), b"member8".to_vec(), 
            b"9.0".to_vec(), b"member9".to_vec(),
            b"10.0".to_vec(), b"member10".to_vec(),
        ];
        
        let result = zadd(&engine, &zadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(10)); // Added 10 new members
        
        // Verify a few scores
        let zscore_args = vec![b"multi_zset".to_vec(), b"member1".to_vec()];
        let result = zscore(&engine, &zscore_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"1".to_vec())));
        
        let zscore_args = vec![b"multi_zset".to_vec(), b"member5".to_vec()];
        let result = zscore(&engine, &zscore_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"5".to_vec())));
        
        let zscore_args = vec![b"multi_zset".to_vec(), b"member10".to_vec()];
        let result = zscore(&engine, &zscore_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"10".to_vec())));
        
        // Now update some existing members and add new ones
        let zadd_args = vec![
            b"multi_zset".to_vec(),
            b"11.0".to_vec(), b"member1".to_vec(),    // Update existing
            b"12.0".to_vec(), b"member12".to_vec(),   // Add new
            b"13.0".to_vec(), b"member13".to_vec(),   // Add new
            b"14.0".to_vec(), b"member5".to_vec(),    // Update existing
        ];
        
        let result = zadd(&engine, &zadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(2)); // Added 2 new members (not counting updates)
        
        // Verify updates
        let zscore_args = vec![b"multi_zset".to_vec(), b"member1".to_vec()];
        let result = zscore(&engine, &zscore_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"11".to_vec())));
        
        let zscore_args = vec![b"multi_zset".to_vec(), b"member5".to_vec()];
        let result = zscore(&engine, &zscore_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"14".to_vec())));
        
        // Verify new members
        let zscore_args = vec![b"multi_zset".to_vec(), b"member12".to_vec()];
        let result = zscore(&engine, &zscore_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"12".to_vec())));
        
        let zscore_args = vec![b"multi_zset".to_vec(), b"member13".to_vec()];
        let result = zscore(&engine, &zscore_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"13".to_vec())));
    }
} 