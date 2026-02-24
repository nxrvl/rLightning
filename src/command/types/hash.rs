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

fn glob_match(pattern: &str, input: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let inp: Vec<char> = input.chars().collect();
    let (plen, ilen) = (pat.len(), inp.len());
    let (mut pi, mut ii) = (0, 0);
    let (mut star_p, mut star_i) = (usize::MAX, 0);
    while ii < ilen {
        if pi < plen && (pat[pi] == '?' || pat[pi] == inp[ii]) {
            pi += 1;
            ii += 1;
        } else if pi < plen && pat[pi] == '*' {
            star_p = pi;
            star_i = ii;
            pi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_i += 1;
            ii = star_i;
        } else {
            return false;
        }
    }
    while pi < plen && pat[pi] == '*' {
        pi += 1;
    }
    pi == plen
}

/// Redis HRANDFIELD command - Get random fields from a hash
pub async fn hrandfield(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let key_prefix = [key.as_slice(), b":"].concat();

    // Collect all fields and values
    let all_keys = engine.all_keys().await?;
    let mut fields: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for full_key in &all_keys {
        if full_key.starts_with(&key_prefix) {
            let field = full_key[key_prefix.len()..].to_vec();
            if let Some(value) = engine.get(full_key).await? {
                fields.push((field, value));
            }
        }
    }

    if fields.is_empty() {
        if args.len() >= 2 {
            return Ok(RespValue::Array(Some(vec![])));
        } else {
            return Ok(RespValue::BulkString(None));
        }
    }

    if args.len() == 1 {
        // No count: return single random field name
        let idx = fastrand::usize(0..fields.len());
        return Ok(RespValue::BulkString(Some(fields[idx].0.clone())));
    }

    let count_val = String::from_utf8_lossy(&args[1]).parse::<i64>().map_err(|_| {
        CommandError::NotANumber
    })?;

    let with_values = if args.len() == 3 {
        let opt = String::from_utf8_lossy(&args[2]).to_uppercase();
        if opt == "WITHVALUES" {
            true
        } else {
            return Err(CommandError::InvalidArgument(format!("Unsupported option: {}", opt)));
        }
    } else {
        false
    };

    let mut result: Vec<RespValue> = Vec::new();

    if count_val >= 0 {
        // Positive count: return up to count unique fields
        let actual_count = std::cmp::min(count_val as usize, fields.len());
        // Use Fisher-Yates partial shuffle for uniqueness
        let mut indices: Vec<usize> = (0..fields.len()).collect();
        for i in 0..actual_count {
            let j = fastrand::usize(i..indices.len());
            indices.swap(i, j);
        }
        for i in 0..actual_count {
            let idx = indices[i];
            result.push(RespValue::BulkString(Some(fields[idx].0.clone())));
            if with_values {
                result.push(RespValue::BulkString(Some(fields[idx].1.clone())));
            }
        }
    } else {
        // Negative count: return |count| fields, may repeat
        let actual_count = (-count_val) as usize;
        for _ in 0..actual_count {
            let idx = fastrand::usize(0..fields.len());
            result.push(RespValue::BulkString(Some(fields[idx].0.clone())));
            if with_values {
                result.push(RespValue::BulkString(Some(fields[idx].1.clone())));
            }
        }
    }

    Ok(RespValue::Array(Some(result)))
}

/// Redis HSCAN command - Incrementally iterate hash fields
pub async fn hscan(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let cursor_str = String::from_utf8_lossy(&args[1]);
    let cursor = cursor_str.parse::<u64>().map_err(|_| {
        CommandError::InvalidArgument("Cursor value must be an integer".to_string())
    })?;

    // Parse optional MATCH and COUNT
    let mut pattern = String::from("*");
    let mut count: usize = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                pattern = String::from_utf8_lossy(&args[i + 1]).to_string();
                i += 2;
            }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                count = String::from_utf8_lossy(&args[i + 1]).parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("COUNT must be a positive integer".to_string())
                })?;
                i += 2;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported HSCAN option: {}",
                    opt
                )));
            }
        }
    }

    let key_prefix = [key.as_slice(), b":"].concat();
    let all_keys = engine.all_keys().await?;

    // Collect field-value pairs
    let mut field_values: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for full_key in &all_keys {
        if full_key.starts_with(&key_prefix) {
            let field = full_key[key_prefix.len()..].to_vec();
            let field_str = String::from_utf8_lossy(&field);
            if pattern == "*" || glob_match(&pattern, &field_str) {
                if let Some(value) = engine.get(full_key).await? {
                    field_values.push((field, value));
                }
            }
        }
    }

    // Sort for deterministic cursor behavior
    field_values.sort_by(|a, b| a.0.cmp(&b.0));

    let total = field_values.len();
    let start = if cursor == 0 || cursor as usize >= total {
        0
    } else {
        cursor as usize
    };

    let end = (start + count).min(total);
    let next_cursor = if end >= total { 0 } else { end as u64 };

    let mut result_items: Vec<RespValue> = Vec::new();
    for (field, value) in &field_values[start..end] {
        result_items.push(RespValue::BulkString(Some(field.clone())));
        result_items.push(RespValue::BulkString(Some(value.clone())));
    }

    Ok(RespValue::Array(Some(vec![
        RespValue::BulkString(Some(next_cursor.to_string().into_bytes())),
        RespValue::Array(Some(result_items)),
    ])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::collections::HashSet;
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

    #[tokio::test]
    async fn test_hrandfield_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Set up a hash with 3 fields
        hset(&engine, &[
            b"myhash".to_vec(),
            b"f1".to_vec(), b"v1".to_vec(),
            b"f2".to_vec(), b"v2".to_vec(),
            b"f3".to_vec(), b"v3".to_vec(),
        ]).await.unwrap();

        // HRANDFIELD with no count - returns single field name
        let result = hrandfield(&engine, &[b"myhash".to_vec()]).await.unwrap();
        if let RespValue::BulkString(Some(field)) = result {
            let s = String::from_utf8_lossy(&field);
            assert!(s == "f1" || s == "f2" || s == "f3", "Got unexpected field: {}", s);
        } else {
            panic!("Expected BulkString");
        }
    }

    #[tokio::test]
    async fn test_hrandfield_positive_count() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        hset(&engine, &[
            b"myhash2".to_vec(),
            b"f1".to_vec(), b"v1".to_vec(),
            b"f2".to_vec(), b"v2".to_vec(),
            b"f3".to_vec(), b"v3".to_vec(),
        ]).await.unwrap();

        // HRANDFIELD with positive count 2 - returns 2 unique fields
        let result = hrandfield(&engine, &[b"myhash2".to_vec(), b"2".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            // Verify all items are valid field names
            let mut seen = HashSet::new();
            for item in &items {
                if let RespValue::BulkString(Some(field)) = item {
                    let s = String::from_utf8_lossy(field).to_string();
                    assert!(s == "f1" || s == "f2" || s == "f3");
                    seen.insert(s);
                }
            }
            assert_eq!(seen.len(), 2, "Should return 2 unique fields");
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_hrandfield_with_values() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        hset(&engine, &[
            b"myhash3".to_vec(),
            b"f1".to_vec(), b"v1".to_vec(),
            b"f2".to_vec(), b"v2".to_vec(),
        ]).await.unwrap();

        // HRANDFIELD count 2 WITHVALUES - returns field-value pairs
        let result = hrandfield(&engine, &[
            b"myhash3".to_vec(), b"2".to_vec(), b"WITHVALUES".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4); // 2 fields * 2 (field + value)
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_hrandfield_negative_count() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        hset(&engine, &[
            b"myhash4".to_vec(),
            b"f1".to_vec(), b"v1".to_vec(),
        ]).await.unwrap();

        // Negative count: can return repeats
        let result = hrandfield(&engine, &[b"myhash4".to_vec(), b"-5".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 5); // Returns 5 items (all f1 since only 1 field)
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_hrandfield_empty_hash() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // No count on nonexistent key
        let result = hrandfield(&engine, &[b"nokey".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // With count on nonexistent key
        let result = hrandfield(&engine, &[b"nokey".to_vec(), b"3".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_hscan_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        hset(&engine, &[
            b"scanhash".to_vec(),
            b"name".to_vec(), b"Alice".to_vec(),
            b"age".to_vec(), b"30".to_vec(),
            b"city".to_vec(), b"NYC".to_vec(),
        ]).await.unwrap();

        // HSCAN cursor 0
        let result = hscan(&engine, &[b"scanhash".to_vec(), b"0".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2); // cursor + array
            if let RespValue::BulkString(Some(cursor)) = &items[0] {
                assert_eq!(cursor, b"0"); // All 3 fields fit in default count
            }
            if let RespValue::Array(Some(fv_pairs)) = &items[1] {
                assert_eq!(fv_pairs.len(), 6); // 3 fields * 2
            }
        }
    }

    #[tokio::test]
    async fn test_hscan_with_match() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        hset(&engine, &[
            b"scanhash2".to_vec(),
            b"name".to_vec(), b"Bob".to_vec(),
            b"nickname".to_vec(), b"Bobby".to_vec(),
            b"age".to_vec(), b"25".to_vec(),
        ]).await.unwrap();

        // HSCAN with MATCH n*
        let result = hscan(&engine, &[
            b"scanhash2".to_vec(), b"0".to_vec(),
            b"MATCH".to_vec(), b"n*".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            if let RespValue::Array(Some(fv_pairs)) = &items[1] {
                // Should match "name" and "nickname"
                assert_eq!(fv_pairs.len(), 4); // 2 matched fields * 2
            }
        }
    }

    #[tokio::test]
    async fn test_hscan_nonexistent_key() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let result = hscan(&engine, &[b"nokey".to_vec(), b"0".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            if let RespValue::BulkString(Some(cursor)) = &items[0] {
                assert_eq!(cursor, b"0");
            }
            if let RespValue::Array(Some(fv_pairs)) = &items[1] {
                assert!(fv_pairs.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn test_hscan_with_count() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Add many fields
        for i in 0..15 {
            hset(&engine, &[
                b"bighash".to_vec(),
                format!("field{:02}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            ]).await.unwrap();
        }

        // HSCAN with COUNT 5
        let result = hscan(&engine, &[
            b"bighash".to_vec(), b"0".to_vec(),
            b"COUNT".to_vec(), b"5".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            if let RespValue::BulkString(Some(cursor)) = &items[0] {
                let cursor_val: u64 = String::from_utf8_lossy(cursor).parse().unwrap();
                assert!(cursor_val > 0, "Cursor should be non-zero for partial scan");
            }
            if let RespValue::Array(Some(fv_pairs)) = &items[1] {
                assert_eq!(fv_pairs.len(), 10); // 5 fields * 2 (field + value)
            }
        }
    }
}