use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::item::RedisDataType;
use std::collections::HashSet;

/// Redis SADD command - Add members to a set
pub async fn sadd(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let members = &args[1..];

    // First deduplicate the input members to avoid counting duplicates within the command
    let mut unique_members = HashSet::new();
    for member in members {
        unique_members.insert(member.clone());
    }

    // First check if the key already exists
    if engine.exists(key).await? {
        // Get the key type
        let key_type = engine.get_type(key).await?;
        // In Redis, SADD on a non-set key returns a WRONGTYPE error
        if key_type != "set" {
            return Err(CommandError::WrongType);
        }
        
        // Key exists and is a set, so update it
        let existing_set = if let Some(data) = engine.get(key).await? {
            // Try to deserialize as a HashSet
            bincode::deserialize::<HashSet<Vec<u8>>>(&data).unwrap_or_default()
        } else {
            // This shouldn't happen (key exists but get returns None)
            HashSet::new()
        };
        
        // Count how many new members we're adding
        let mut new_members_count = 0;
        let mut set = existing_set;
        
        // Add the new members and count only those that weren't already in the set
        for member in unique_members {
            if set.insert(member) {
                new_members_count += 1;
            }
        }
        
        // Serialize the set
        let serialized = match bincode::serialize(&set) {
            Ok(data) => data,
            Err(e) => {
                return Err(CommandError::InternalError(format!("Serialization error: {}", e)));
            }
        };
        
        // Store the set in the storage engine
        engine.set_with_type(key.clone(), serialized, RedisDataType::Set, None).await?;
        
        // Return the number of new members added
        Ok(RespValue::Integer(new_members_count))
    } else {
        // Key doesn't exist, create a new set
        let set = unique_members;
        
        // Serialize the set
        let serialized = match bincode::serialize(&set) {
            Ok(data) => data,
            Err(e) => {
                return Err(CommandError::InternalError(format!("Serialization error: {}", e)));
            }
        };
        
        // Store the set in the storage engine
        engine.set_with_type(key.clone(), serialized, RedisDataType::Set, None).await?;
        
        // Return the number of new members added (all of them)
        Ok(RespValue::Integer(set.len() as i64))
    }
}

/// Redis SREM command - Remove members from a set
pub async fn srem(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let members = &args[1..];
    
    // Get the current set
    let mut set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<HashSet<Vec<u8>>>(&data) {
                Ok(set) => set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Set doesn't exist, so nothing to remove
            return Ok(RespValue::Integer(0));
        },
    };
    
    // Track how many members were removed
    let mut removed = 0;
    
    // Remove each member (with error handling to prevent crashes)
    for member in members {
        // Using a more defensive approach to avoid potential panics
        match set.remove(member) {
            true => removed += 1,
            false => continue, // Member not found, continue to next
        }
    }
    
    // Only update storage if we actually removed something
    if removed > 0 {
        if set.is_empty() {
            // If the set is now empty, remove the key
            // Use a try-catch to prevent connection reset if this fails
            match engine.del(key).await {
                Ok(_) => {},
                Err(e) => {
                    return Err(CommandError::InternalError(format!("Error deleting key: {}", e)));
                }
            }
        } else {
            // Otherwise, update the set with proper error handling
            let serialized = match bincode::serialize(&set) {
                Ok(data) => data,
                Err(e) => {
                    return Err(CommandError::InternalError(format!("Serialization error: {}", e)));
                }
            };
            
            match engine.set(key.clone(), serialized, None).await {
                Ok(_) => {},
                Err(e) => {
                    return Err(CommandError::InternalError(format!("Error updating set: {}", e)));
                }
            }
        }
    }
    
    Ok(RespValue::Integer(removed))
}

/// Redis SMEMBERS command - Get all members of a set
pub async fn smembers(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    println!("DEBUG - SMEMBERS: Getting members for key '{}'", String::from_utf8_lossy(key));
    
    // Get the set
    let set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<HashSet<Vec<u8>>>(&data) {
                Ok(set) => {
                    println!("DEBUG - SMEMBERS: Deserialized {} members", set.len());
                    for member in &set {
                        println!("DEBUG - SMEMBERS: Member: '{}'", String::from_utf8_lossy(member));
                    }
                    set
                },
                Err(e) => {
                    println!("DEBUG - SMEMBERS: Error deserializing set: {}", e);
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            println!("DEBUG - SMEMBERS: Set doesn't exist, returning empty array");
            // Set doesn't exist, return empty array
            return Ok(RespValue::Array(Some(vec![])));
        },
    };
    
    // Convert to an array of bulk strings
    let result: Vec<RespValue> = set.into_iter()
        .map(|member| RespValue::BulkString(Some(member)))
        .collect();
    
    Ok(RespValue::Array(Some(result)))
}

/// Redis SISMEMBER command - Check if a value is a member of a set
pub async fn sismember(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let member = &args[1];
    
    // Get the set
    let set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<HashSet<Vec<u8>>>(&data) {
                Ok(set) => set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Set doesn't exist, so member isn't in it
            return Ok(RespValue::Integer(0));
        },
    };
    
    // Check if the member exists
    Ok(RespValue::Integer(if set.contains(member) { 1 } else { 0 }))
}

/// Redis SCARD command - Get the cardinality (number of members) of a set
pub async fn scard(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Get the set
    let set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<HashSet<Vec<u8>>>(&data) {
                Ok(set) => set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Set doesn't exist, return 0
            return Ok(RespValue::Integer(0));
        },
    };
    
    Ok(RespValue::Integer(set.len() as i64))
}

/// Redis SPOP command - Remove and return a random member from a set
pub async fn spop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let count = if args.len() == 2 {
        match String::from_utf8_lossy(&args[1]).parse::<usize>() {
            Ok(n) => Some(n),
            Err(_) => return Err(CommandError::NotANumber),
        }
    } else {
        None
    };
    
    // Get the set
    let mut set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<HashSet<Vec<u8>>>(&data) {
                Ok(set) => set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Set doesn't exist
            return if count.is_some() {
                Ok(RespValue::Array(Some(vec![])))
            } else {
                Ok(RespValue::BulkString(None))
            };
        },
    };
    
    if set.is_empty() {
        return if count.is_some() {
            Ok(RespValue::Array(Some(vec![])))
        } else {
            Ok(RespValue::BulkString(None))
        };
    }
    
    if let Some(count) = count {
        // Return multiple elements
        let mut result = Vec::new();
        let actual_count = std::cmp::min(count, set.len());
        
        for _ in 0..actual_count {
            if let Some(member) = set.iter().next().cloned() {
                set.remove(&member);
                result.push(RespValue::BulkString(Some(member)));
            }
        }
        
        // Update or delete the key
        if set.is_empty() {
            engine.del(key).await?;
        } else {
            let serialized = bincode::serialize(&set)
                .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
            engine.set(key.clone(), serialized, None).await?;
        }
        
        Ok(RespValue::Array(Some(result)))
    } else {
        // Return single element
        if let Some(member) = set.iter().next().cloned() {
            set.remove(&member);
            
            // Update or delete the key
            if set.is_empty() {
                engine.del(key).await?;
            } else {
                let serialized = bincode::serialize(&set)
                    .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
                engine.set(key.clone(), serialized, None).await?;
            }
            
            Ok(RespValue::BulkString(Some(member)))
        } else {
            Ok(RespValue::BulkString(None))
        }
    }
}

/// Helper function to get a set from storage
async fn get_set(engine: &StorageEngine, key: &[u8]) -> Result<HashSet<Vec<u8>>, CommandError> {
    match engine.get(key).await? {
        Some(data) => {
            bincode::deserialize::<HashSet<Vec<u8>>>(&data)
                .map_err(|_| CommandError::WrongType)
        },
        None => Ok(HashSet::new()),
    }
}

/// Redis SINTER command - Return the intersection of multiple sets
pub async fn sinter(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Get the first set
    let mut result = get_set(engine, &args[0]).await?;

    // Intersect with remaining sets
    for key in &args[1..] {
        let other_set = get_set(engine, key).await?;
        result = result.intersection(&other_set).cloned().collect();
    }

    // Convert to array of bulk strings
    let members: Vec<RespValue> = result
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();

    Ok(RespValue::Array(Some(members)))
}

/// Redis SINTERSTORE command - Store intersection of sets in destination key
pub async fn sinterstore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let destination = &args[0];
    let keys = &args[1..];

    // Get the first set
    let mut result = get_set(engine, &keys[0]).await?;

    // Intersect with remaining sets
    for key in &keys[1..] {
        let other_set = get_set(engine, key).await?;
        result = result.intersection(&other_set).cloned().collect();
    }

    let count = result.len() as i64;

    // Store the result (or delete if empty)
    if result.is_empty() {
        let _ = engine.del(destination).await;
    } else {
        let serialized = bincode::serialize(&result)
            .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
        engine.set_with_type(destination.to_vec(), serialized, RedisDataType::Set, None).await?;
    }

    Ok(RespValue::Integer(count))
}

/// Redis SUNION command - Return the union of multiple sets
pub async fn sunion(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let mut result = HashSet::new();

    // Union all sets
    for key in args {
        let set = get_set(engine, key).await?;
        result = result.union(&set).cloned().collect();
    }

    // Convert to array of bulk strings
    let members: Vec<RespValue> = result
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();

    Ok(RespValue::Array(Some(members)))
}

/// Redis SUNIONSTORE command - Store union of sets in destination key
pub async fn sunionstore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let destination = &args[0];
    let keys = &args[1..];

    let mut result = HashSet::new();

    // Union all sets
    for key in keys {
        let set = get_set(engine, key).await?;
        result = result.union(&set).cloned().collect();
    }

    let count = result.len() as i64;

    // Store the result (or delete if empty)
    if result.is_empty() {
        let _ = engine.del(destination).await;
    } else {
        let serialized = bincode::serialize(&result)
            .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
        engine.set_with_type(destination.to_vec(), serialized, RedisDataType::Set, None).await?;
    }

    Ok(RespValue::Integer(count))
}

/// Redis SDIFF command - Return the difference between the first set and all subsequent sets
pub async fn sdiff(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Get the first set
    let mut result = get_set(engine, &args[0]).await?;

    // Subtract remaining sets
    for key in &args[1..] {
        let other_set = get_set(engine, key).await?;
        result = result.difference(&other_set).cloned().collect();
    }

    // Convert to array of bulk strings
    let members: Vec<RespValue> = result
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();

    Ok(RespValue::Array(Some(members)))
}

/// Redis SDIFFSTORE command - Store difference of sets in destination key
pub async fn sdiffstore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let destination = &args[0];
    let keys = &args[1..];

    // Get the first set
    let mut result = get_set(engine, &keys[0]).await?;

    // Subtract remaining sets
    for key in &keys[1..] {
        let other_set = get_set(engine, key).await?;
        result = result.difference(&other_set).cloned().collect();
    }

    let count = result.len() as i64;

    // Store the result (or delete if empty)
    if result.is_empty() {
        let _ = engine.del(destination).await;
    } else {
        let serialized = bincode::serialize(&result)
            .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
        engine.set_with_type(destination.to_vec(), serialized, RedisDataType::Set, None).await?;
    }

    Ok(RespValue::Integer(count))
}

/// Redis SRANDMEMBER command - Get random member(s) from a set without removing them
pub async fn srandmember(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let count = if args.len() == 2 {
        match String::from_utf8_lossy(&args[1]).parse::<i64>() {
            Ok(n) => Some(n),
            Err(_) => return Err(CommandError::NotANumber),
        }
    } else {
        None
    };
    
    // Get the set
    let set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<HashSet<Vec<u8>>>(&data) {
                Ok(set) => set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Set doesn't exist
            return if count.is_some() {
                Ok(RespValue::Array(Some(vec![])))
            } else {
                Ok(RespValue::BulkString(None))
            };
        },
    };
    
    if set.is_empty() {
        return if count.is_some() {
            Ok(RespValue::Array(Some(vec![])))
        } else {
            Ok(RespValue::BulkString(None))
        };
    }
    
    if let Some(count) = count {
        // Return multiple elements (with possible repetition if count > set size)
        let mut result = Vec::new();
        let set_vec: Vec<_> = set.into_iter().collect();
        
        if count >= 0 {
            // Positive count: return unique elements
            let actual_count = std::cmp::min(count as usize, set_vec.len());
            for item in set_vec.iter().take(actual_count) {
                result.push(RespValue::BulkString(Some(item.clone())));
            }
        } else {
            // Negative count: return with possible repetition
            let actual_count = (-count) as usize;
            for _ in 0..actual_count {
                let index = fastrand::usize(0..set_vec.len());
                result.push(RespValue::BulkString(Some(set_vec[index].clone())));
            }
        }
        
        Ok(RespValue::Array(Some(result)))
    } else {
        // Return single element
        let set_vec: Vec<_> = set.into_iter().collect();
        let index = fastrand::usize(0..set_vec.len());
        Ok(RespValue::BulkString(Some(set_vec[index].clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;
    
    #[tokio::test]
    async fn test_set_commands() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test SADD
        let sadd_args = vec![
            b"set1".to_vec(),
            b"member1".to_vec(),
            b"member2".to_vec(),
        ];
        let result = sadd(&engine, &sadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(2)); // Added 2 members
        
        // Add a duplicate and a new member
        let sadd_args = vec![
            b"set1".to_vec(),
            b"member2".to_vec(),
            b"member3".to_vec(),
        ];
        let result = sadd(&engine, &sadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Added 1 new member
        
        // Test SISMEMBER
        let sismember_args = vec![b"set1".to_vec(), b"member1".to_vec()];
        let result = sismember(&engine, &sismember_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Is a member
        
        let sismember_args = vec![b"set1".to_vec(), b"nonexistent".to_vec()];
        let result = sismember(&engine, &sismember_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Not a member
        
        // Test SMEMBERS
        let smembers_args = vec![b"set1".to_vec()];
        let result = smembers(&engine, &smembers_args).await.unwrap();
        if let RespValue::Array(Some(members)) = result {
            assert_eq!(members.len(), 3);
            
            // Verify that all expected members are present
            let mut found_member1 = false;
            let mut found_member2 = false;
            let mut found_member3 = false;
            
            for member in members {
                if let RespValue::BulkString(Some(data)) = member {
                    if data == b"member1" {
                        found_member1 = true;
                    } else if data == b"member2" {
                        found_member2 = true;
                    } else if data == b"member3" {
                        found_member3 = true;
                    }
                }
            }
            
            assert!(found_member1, "member1 not found in SMEMBERS results");
            assert!(found_member2, "member2 not found in SMEMBERS results");
            assert!(found_member3, "member3 not found in SMEMBERS results");
            
        } else {
            panic!("Expected array response from SMEMBERS");
        }
        
        // Test SREM
        let srem_args = vec![
            b"set1".to_vec(),
            b"member1".to_vec(),
            b"nonexistent".to_vec(),
        ];
        let result = srem(&engine, &srem_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Removed 1 member
        
        // Verify that the member was removed
        let sismember_args = vec![b"set1".to_vec(), b"member1".to_vec()];
        let result = sismember(&engine, &sismember_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // No longer a member
    }
} 