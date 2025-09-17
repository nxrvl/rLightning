use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// Redis HSET command - Set the value of a hash field
pub async fn hset(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let fields_values = &args[1..];
    
    let mut new_fields_count = 0;
    
    // Process field-value pairs
    for i in (0..fields_values.len()).step_by(2) {
        if i + 1 >= fields_values.len() {
            break; // Not enough arguments for this pair
        }
        
        let field = &fields_values[i];
        let value = &fields_values[i + 1];
        
        // Construct the hash key for this field
        let mut hash_key = Vec::with_capacity(key.len() + 1 + field.len());
        hash_key.extend_from_slice(key);
        hash_key.push(b':'); // Use ':' as separator
        hash_key.extend_from_slice(field);
        
        // Check if this field already exists in the hash
        let field_exists = engine.exists(&hash_key).await?;
        
        // Set the field-value pair
        engine.set(hash_key, value.clone(), None).await?;
        
        // Increment the counter for new fields
        if !field_exists {
            new_fields_count += 1;
        }
    }
    
    Ok(RespValue::Integer(new_fields_count))
}

/// Redis HGET command - Get the value of a hash field
pub async fn hget(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let field = &args[1];
    
    // First check if the key exists and is a hash
    if engine.exists(key).await? {
        // Get the type of the key
        let key_type = engine.get_type(key).await?;
        
        // If the key exists but is not a hash type, return an error
        if key_type != "hash" && !key_type.is_empty() {
            return Err(CommandError::WrongType);
        }
    }
    
    // Construct the hash key for this field
    let mut hash_key = Vec::with_capacity(key.len() + 1 + field.len());
    hash_key.extend_from_slice(key);
    hash_key.push(b':'); // Use ':' as separator
    hash_key.extend_from_slice(field);
    
    // Get the field's value
    match engine.get(&hash_key).await? {
        Some(value) => Ok(RespValue::BulkString(Some(value))),
        None => Ok(RespValue::BulkString(None)), // Field not found
    }
}

/// Redis HGETALL command - Get all fields and values from a hash
pub async fn hgetall(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let key_prefix = [key.as_slice(), b":"].concat();
    
    // Get all keys in the storage engine
    let all_keys = engine.all_keys().await?;
    
    // Filter for keys that have our prefix
    let mut result = Vec::new();
    
    for full_key in all_keys {
        if full_key.starts_with(&key_prefix) {
            // Extract the field name (remove the prefix)
            let field = full_key[key_prefix.len()..].to_vec();
            
            // Get the value for this field
            if let Some(value) = engine.get(&full_key).await? {
                result.push(RespValue::BulkString(Some(field)));
                result.push(RespValue::BulkString(Some(value)));
            }
        }
    }
    
    Ok(RespValue::Array(Some(result)))
}

/// Redis HDEL command - Delete one or more hash fields
pub async fn hdel(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let fields = &args[1..];
    
    let mut deleted = 0;
    
    // Construct the hash key format for each field and delete if exists
    for field in fields {
        // Construct the hash key for this field
        let mut hash_key = Vec::with_capacity(key.len() + 1 + field.len());
        hash_key.extend_from_slice(key);
        hash_key.push(b':');
        hash_key.extend_from_slice(field);
        
        // Check if the field exists before deleting
        if engine.exists(&hash_key).await? {
            // Delete the key and increment counter only if successful
            if engine.del(&hash_key).await? {
                deleted += 1;
            }
        }
    }
    
    // Redis returns the number of fields actually deleted
    Ok(RespValue::Integer(deleted))
}

/// Redis HEXISTS command - Check if a hash field exists
pub async fn hexists(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let field = &args[1];
    
    // Construct the hash key for this field
    let mut hash_key = Vec::with_capacity(key.len() + 1 + field.len());
    hash_key.extend_from_slice(key);
    hash_key.push(b':'); // Use ':' as separator
    hash_key.extend_from_slice(field);
    
    // Check if the hash field exists
    let exists = engine.exists(&hash_key).await?;
    
    Ok(RespValue::Integer(if exists { 1 } else { 0 }))
}

/// Redis HMSET command - Set multiple hash fields to multiple values
pub async fn hmset(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len() % 2 != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // First, check if the key exists and is the right type
    if engine.exists(key).await? {
        let key_type = engine.get_type(key).await?;
        if key_type != "hash" && !key_type.is_empty() {
            return Err(CommandError::WrongType);
        }
    }
    
    let fields_values = &args[1..];
    
    // Process field-value pairs
    for i in (0..fields_values.len()).step_by(2) {
        let field = &fields_values[i];
        let value = &fields_values[i + 1];
        
        // We'll use the key itself as the hash name, and store each field with a special separator format
        let mut hash_key = Vec::with_capacity(key.len() + 1 + field.len());
        hash_key.extend_from_slice(key);
        hash_key.push(b':'); // Use ':' as separator
        hash_key.extend_from_slice(field);
        
        // Set the value
        engine.set(hash_key, value.clone(), None).await?;
    }
    
    // HMSET always returns OK
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis HKEYS command - Get all field names in a hash
pub async fn hkeys(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let key_prefix = [key.as_slice(), b":"].concat();
    
    // Get all keys in the storage engine
    let all_keys = engine.all_keys().await?;
    
    // Filter for keys that have our prefix and extract field names
    let mut result = Vec::new();
    
    for full_key in all_keys {
        if full_key.starts_with(&key_prefix) {
            // Extract the field name (remove the prefix)
            let field = full_key[key_prefix.len()..].to_vec();
            result.push(RespValue::BulkString(Some(field)));
        }
    }
    
    Ok(RespValue::Array(Some(result)))
}

/// Redis HVALS command - Get all values in a hash
pub async fn hvals(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let key_prefix = [key.as_slice(), b":"].concat();
    
    // Get all keys in the storage engine
    let all_keys = engine.all_keys().await?;
    
    // Filter for keys that have our prefix and get their values
    let mut result = Vec::new();
    
    for full_key in all_keys {
        if full_key.starts_with(&key_prefix) {
            // Get the value for this field
            if let Some(value) = engine.get(&full_key).await? {
                result.push(RespValue::BulkString(Some(value)));
            }
        }
    }
    
    Ok(RespValue::Array(Some(result)))
}

/// Redis HLEN command - Get the number of fields in a hash
pub async fn hlen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let key_prefix = [key.as_slice(), b":"].concat();
    
    // Get all keys in the storage engine
    let all_keys = engine.all_keys().await?;
    
    // Count keys that have our prefix
    let count = all_keys.iter().filter(|k| k.starts_with(&key_prefix)).count();
    
    Ok(RespValue::Integer(count as i64))
}

/// Redis HMGET command - Get values of multiple hash fields
pub async fn hmget(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let fields = &args[1..];
    
    let mut result = Vec::new();
    
    for field in fields {
        // Construct the hash key for this field
        let mut hash_key = Vec::with_capacity(key.len() + 1 + field.len());
        hash_key.extend_from_slice(key);
        hash_key.push(b':');
        hash_key.extend_from_slice(field);
        
        // Get the field's value
        match engine.get(&hash_key).await? {
            Some(value) => result.push(RespValue::BulkString(Some(value))),
            None => result.push(RespValue::BulkString(None)),
        }
    }
    
    Ok(RespValue::Array(Some(result)))
}

/// Redis HINCRBY command - Increment the integer value of a hash field by the given number
pub async fn hincrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let field = &args[1];
    
    // Parse the increment value
    let increment = String::from_utf8_lossy(&args[2]).parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Increment amount is not a valid integer".to_string())
    })?;
    
    // Construct the hash key for this field
    let mut hash_key = Vec::with_capacity(key.len() + 1 + field.len());
    hash_key.extend_from_slice(key);
    hash_key.push(b':');
    hash_key.extend_from_slice(field);
    
    // Get the current value or default to 0
    let current_value = match engine.get(&hash_key).await? {
        Some(data) => {
            let str_value = String::from_utf8_lossy(&data);
            str_value.parse::<i64>().map_err(|_| CommandError::WrongType)?
        },
        None => 0,
    };
    
    // Increment the value
    let new_value = current_value.checked_add(increment).ok_or_else(|| {
        CommandError::InvalidArgument("Increment operation would overflow".to_string())
    })?;
    
    // Store the new value
    engine.set(hash_key, new_value.to_string().into_bytes(), None).await?;
    
    Ok(RespValue::Integer(new_value))
}

/// Redis HINCRBYFLOAT command - Increment the float value of a hash field by the given amount
pub async fn hincrbyfloat(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let field = &args[1];
    
    // Parse the increment value
    let increment = String::from_utf8_lossy(&args[2]).parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("Increment amount is not a valid float".to_string())
    })?;
    
    // Construct the hash key for this field
    let mut hash_key = Vec::with_capacity(key.len() + 1 + field.len());
    hash_key.extend_from_slice(key);
    hash_key.push(b':');
    hash_key.extend_from_slice(field);
    
    // Get the current value or default to 0.0
    let current_value = match engine.get(&hash_key).await? {
        Some(data) => {
            let str_value = String::from_utf8_lossy(&data);
            str_value.parse::<f64>().map_err(|_| CommandError::WrongType)?
        },
        None => 0.0,
    };
    
    // Increment the value
    let new_value = current_value + increment;
    
    // Check for NaN or infinity
    if !new_value.is_finite() {
        return Err(CommandError::InvalidArgument("Result is not a valid number".to_string()));
    }
    
    // Store the new value with Redis-compatible formatting
    let new_value_str = if new_value.fract() == 0.0 && new_value.abs() < 1e15 {
        format!("{:.0}", new_value)
    } else {
        format!("{}", new_value)
    };
    
    engine.set(hash_key, new_value_str.as_bytes().to_vec(), None).await?;
    
    Ok(RespValue::BulkString(Some(new_value_str.into_bytes())))
}

/// Redis HSETNX command - Set the value of a hash field, only if the field does not exist
pub async fn hsetnx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let field = &args[1];
    let value = &args[2];
    
    // Construct the hash key for this field
    let mut hash_key = Vec::with_capacity(key.len() + 1 + field.len());
    hash_key.extend_from_slice(key);
    hash_key.push(b':');
    hash_key.extend_from_slice(field);
    
    // Check if the field already exists
    if engine.exists(&hash_key).await? {
        return Ok(RespValue::Integer(0)); // Field exists, don't set
    }
    
    // Set the field value
    engine.set(hash_key, value.clone(), None).await?;
    
    Ok(RespValue::Integer(1)) // Field was set
}

/// Redis HSTRLEN command - Get the length of the value of a hash field
pub async fn hstrlen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let field = &args[1];
    
    // Construct the hash key for this field
    let mut hash_key = Vec::with_capacity(key.len() + 1 + field.len());
    hash_key.extend_from_slice(key);
    hash_key.push(b':');
    hash_key.extend_from_slice(field);
    
    // Get the field's value and return its length
    match engine.get(&hash_key).await? {
        Some(value) => Ok(RespValue::Integer(value.len() as i64)),
        None => Ok(RespValue::Integer(0)), // Field doesn't exist
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;
    
    #[tokio::test]
    async fn test_hash_commands() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test HSET
        let hset_args = vec![
            b"hash1".to_vec(),
            b"field1".to_vec(),
            b"value1".to_vec()
        ];
        let result = hset(&engine, &hset_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // New field
        
        // Set another field
        let hset_args = vec![
            b"hash1".to_vec(),
            b"field2".to_vec(), 
            b"value2".to_vec()
        ];
        let result = hset(&engine, &hset_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // New field
        
        // Update existing field
        let hset_args = vec![
            b"hash1".to_vec(),
            b"field1".to_vec(),
            b"new_value".to_vec()
        ];
        let result = hset(&engine, &hset_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Existing field
        
        // Test HGET
        let hget_args = vec![b"hash1".to_vec(), b"field1".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"new_value".to_vec())));
        
        // Test HGETALL
        let hgetall_args = vec![b"hash1".to_vec()];
        let result = hgetall(&engine, &hgetall_args).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4); // 2 fields * 2 (key + value)
            
            // We need to handle the fact that hash maps don't guarantee order
            let mut found_field1 = false;
            let mut found_field2 = false;
            
            for i in (0..items.len()).step_by(2) {
                if let RespValue::BulkString(Some(field)) = &items[i] {
                    if field == b"field1" {
                        found_field1 = true;
                        if let RespValue::BulkString(Some(value)) = &items[i+1] {
                            assert_eq!(value, b"new_value");
                        } else {
                            panic!("Expected bulk string for field1 value");
                        }
                    } else if field == b"field2" {
                        found_field2 = true;
                        if let RespValue::BulkString(Some(value)) = &items[i+1] {
                            assert_eq!(value, b"value2");
                        } else {
                            panic!("Expected bulk string for field2 value");
                        }
                    }
                }
            }
            
            assert!(found_field1, "field1 not found in HGETALL results");
            assert!(found_field2, "field2 not found in HGETALL results");
            
        } else {
            panic!("Expected array response from HGETALL");
        }
        
        // Test HEXISTS
        let hexists_args = vec![b"hash1".to_vec(), b"field1".to_vec()];
        let result = hexists(&engine, &hexists_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Exists
        
        let hexists_args = vec![b"hash1".to_vec(), b"nonexistent".to_vec()];
        let result = hexists(&engine, &hexists_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Does not exist
        
        // Test HDEL
        let hdel_args = vec![b"hash1".to_vec(), b"field1".to_vec()];
        let result = hdel(&engine, &hdel_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Deleted 1 field
        
        // Verify field is gone
        let hexists_args = vec![b"hash1".to_vec(), b"field1".to_vec()];
        let result = hexists(&engine, &hexists_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // No longer exists
    }
    
    #[tokio::test]
    async fn test_hmset() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test HMSET with multiple fields
        let hmset_args = vec![
            b"hmset_hash".to_vec(),
            b"field1".to_vec(), b"value1".to_vec(),
            b"field2".to_vec(), b"value2".to_vec(),
            b"field3".to_vec(), b"value3".to_vec(),
        ];
        let result = hmset(&engine, &hmset_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Verify fields were set
        let hget_args = vec![b"hmset_hash".to_vec(), b"field1".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value1".to_vec())));
        
        let hget_args = vec![b"hmset_hash".to_vec(), b"field2".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value2".to_vec())));
        
        let hget_args = vec![b"hmset_hash".to_vec(), b"field3".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value3".to_vec())));
        
        // Test HMSET with existing fields (should overwrite)
        let hmset_args = vec![
            b"hmset_hash".to_vec(),
            b"field1".to_vec(), b"new_value1".to_vec(),
            b"field2".to_vec(), b"new_value2".to_vec(),
        ];
        let result = hmset(&engine, &hmset_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Verify fields were updated
        let hget_args = vec![b"hmset_hash".to_vec(), b"field1".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"new_value1".to_vec())));
        
        let hget_args = vec![b"hmset_hash".to_vec(), b"field2".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"new_value2".to_vec())));
        
        // Field3 should be unchanged
        let hget_args = vec![b"hmset_hash".to_vec(), b"field3".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value3".to_vec())));
    }
    
    #[tokio::test]
    async fn test_hset_multiple_fields() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test HSET with multiple fields in a single command
        let hset_args = vec![
            b"multi_hash".to_vec(),
            b"field1".to_vec(), b"value1".to_vec(),
            b"field2".to_vec(), b"value2".to_vec(),
            b"field3".to_vec(), b"value3".to_vec(),
        ];
        let result = hset(&engine, &hset_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3)); // 3 new fields
        
        // Verify fields were set
        let hget_args = vec![b"multi_hash".to_vec(), b"field1".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value1".to_vec())));
        
        let hget_args = vec![b"multi_hash".to_vec(), b"field2".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value2".to_vec())));
        
        let hget_args = vec![b"multi_hash".to_vec(), b"field3".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value3".to_vec())));
        
        // Update a few fields
        let hset_args = vec![
            b"multi_hash".to_vec(),
            b"field1".to_vec(), b"updated1".to_vec(),
            b"field4".to_vec(), b"value4".to_vec(),
        ];
        let result = hset(&engine, &hset_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // 1 new field, 1 updated field
        
        // Verify updates
        let hget_args = vec![b"multi_hash".to_vec(), b"field1".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"updated1".to_vec())));
        
        let hget_args = vec![b"multi_hash".to_vec(), b"field4".to_vec()];
        let result = hget(&engine, &hget_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value4".to_vec())));
    }
} 