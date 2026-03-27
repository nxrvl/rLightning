use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// Redis HSET command - Set the value of one or more hash fields
pub async fn hset(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let fields_values = &args[1..];

    let pairs: Vec<(&[u8], &[u8])> = fields_values
        .chunks_exact(2)
        .map(|chunk| (chunk[0].as_slice(), chunk[1].as_slice()))
        .collect();

    let new_count = engine.hash_set(key, &pairs)?;
    Ok(RespValue::Integer(new_count))
}

/// Redis HGET command - Get the value of a hash field
pub async fn hget(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let field = &args[1];

    match engine.hash_get(key, field)? {
        Some(value) => Ok(RespValue::BulkString(Some(value))),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Redis HGETALL command - Get all fields and values from a hash
pub async fn hgetall(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let pairs = engine.hash_getall(key)?;

    let mut result = Vec::with_capacity(pairs.len() * 2);
    for (field, value) in pairs {
        result.push(RespValue::BulkString(Some(field)));
        result.push(RespValue::BulkString(Some(value)));
    }

    Ok(RespValue::Array(Some(result)))
}

/// Redis HDEL command - Delete one or more hash fields
pub async fn hdel(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let fields: Vec<&[u8]> = args[1..].iter().map(|f| f.as_slice()).collect();
    let deleted = engine.hash_del(key, &fields)?;
    Ok(RespValue::Integer(deleted))
}

/// Redis HEXISTS command - Check if a hash field exists
pub async fn hexists(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let field = &args[1];
    let exists = engine.hash_exists(key, field)?;
    Ok(RespValue::Integer(if exists { 1 } else { 0 }))
}

/// Redis HMSET command - Set multiple hash fields to multiple values
pub async fn hmset(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len() % 2 != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let fields_values = &args[1..];

    let pairs: Vec<(&[u8], &[u8])> = fields_values
        .chunks_exact(2)
        .map(|chunk| (chunk[0].as_slice(), chunk[1].as_slice()))
        .collect();

    engine.hash_set(key, &pairs)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis HKEYS command - Get all field names in a hash
pub async fn hkeys(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let keys = engine.hash_keys(key)?;
    let result: Vec<RespValue> = keys
        .into_iter()
        .map(|k| RespValue::BulkString(Some(k)))
        .collect();
    Ok(RespValue::Array(Some(result)))
}

/// Redis HVALS command - Get all values in a hash
pub async fn hvals(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let vals = engine.hash_vals(key)?;
    let result: Vec<RespValue> = vals
        .into_iter()
        .map(|v| RespValue::BulkString(Some(v)))
        .collect();
    Ok(RespValue::Array(Some(result)))
}

/// Redis HLEN command - Get the number of fields in a hash
pub async fn hlen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let len = engine.hash_len(key)?;
    Ok(RespValue::Integer(len))
}

/// Redis HMGET command - Get values of multiple hash fields
pub async fn hmget(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let fields: Vec<&[u8]> = args[1..].iter().map(|f| f.as_slice()).collect();
    let values = engine.hash_mget(key, &fields)?;

    let result: Vec<RespValue> = values.into_iter().map(RespValue::BulkString).collect();
    Ok(RespValue::Array(Some(result)))
}

/// Redis HINCRBY command - Increment the integer value of a hash field by the given number
pub async fn hincrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let field = &args[1];
    let increment = String::from_utf8_lossy(&args[2])
        .parse::<i64>()
        .map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;

    let new_val = engine.hash_incr_by(key, field, increment)?;
    Ok(RespValue::Integer(new_val))
}

/// Redis HINCRBYFLOAT command - Increment the float value of a hash field by the given amount
pub async fn hincrbyfloat(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let field = &args[1];
    let increment = String::from_utf8_lossy(&args[2])
        .parse::<f64>()
        .map_err(|_| CommandError::InvalidArgument("value is not a valid float".to_string()))?;

    let new_val_str = engine.hash_incr_by_float(key, field, increment)?;
    Ok(RespValue::BulkString(Some(new_val_str.into_bytes())))
}

/// Redis HSETNX command - Set the value of a hash field, only if the field does not exist
pub async fn hsetnx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let field = &args[1];
    let value = &args[2];

    let was_set = engine.hash_set_nx(key, field, value)?;
    Ok(RespValue::Integer(if was_set { 1 } else { 0 }))
}

/// Redis HSTRLEN command - Get the length of the value of a hash field
pub async fn hstrlen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let field = &args[1];
    let len = engine.hash_strlen(key, field)?;
    Ok(RespValue::Integer(len))
}

use crate::utils::glob::glob_match;

/// Redis HRANDFIELD command - Get random fields from a hash
pub async fn hrandfield(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Get all field-value pairs from the hash
    let fields = engine.hash_randfield(key)?;

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

    let count_val = String::from_utf8_lossy(&args[1])
        .parse::<i64>()
        .map_err(|_| CommandError::NotANumber)?;

    let with_values = if args.len() == 3 {
        let opt = String::from_utf8_lossy(&args[2]).to_uppercase();
        if opt == "WITHVALUES" {
            true
        } else {
            return Err(CommandError::InvalidArgument(format!(
                "Unsupported option: {}",
                opt
            )));
        }
    } else {
        false
    };

    let mut result: Vec<RespValue> = Vec::new();

    if count_val >= 0 {
        // Positive count: return up to count unique fields
        let actual_count = std::cmp::min(count_val as usize, fields.len());
        let mut indices: Vec<usize> = (0..fields.len()).collect();
        for i in 0..actual_count {
            let j = fastrand::usize(i..indices.len());
            indices.swap(i, j);
        }
        for &idx in &indices[..actual_count] {
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
                count = String::from_utf8_lossy(&args[i + 1])
                    .parse::<usize>()
                    .map_err(|_| {
                        CommandError::InvalidArgument(
                            "COUNT must be a positive integer".to_string(),
                        )
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

    // Get all field-value pairs from the hash
    let all_pairs = engine.hash_getall(key)?;

    // Filter by pattern and sort for deterministic cursor behavior
    let mut field_values: Vec<(Vec<u8>, Vec<u8>)> = all_pairs
        .into_iter()
        .filter(|(field, _)| {
            let field_str = String::from_utf8_lossy(field);
            pattern == "*" || glob_match(&pattern, &field_str)
        })
        .collect();

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

    #[tokio::test]
    async fn test_hash_commands() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Test HSET
        let result = hset(
            &engine,
            &[b"hash1".to_vec(), b"field1".to_vec(), b"value1".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1)); // New field

        // Set another field
        let result = hset(
            &engine,
            &[b"hash1".to_vec(), b"field2".to_vec(), b"value2".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1)); // New field

        // Update existing field
        let result = hset(
            &engine,
            &[b"hash1".to_vec(), b"field1".to_vec(), b"new_value".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Existing field

        // Test HGET
        let result = hget(&engine, &[b"hash1".to_vec(), b"field1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"new_value".to_vec())));

        // Test HGETALL
        let result = hgetall(&engine, &[b"hash1".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4); // 2 fields * 2 (key + value)

            let mut found_field1 = false;
            let mut found_field2 = false;

            for i in (0..items.len()).step_by(2) {
                if let RespValue::BulkString(Some(field)) = &items[i] {
                    if field == b"field1" {
                        found_field1 = true;
                        if let RespValue::BulkString(Some(value)) = &items[i + 1] {
                            assert_eq!(value, b"new_value");
                        } else {
                            panic!("Expected bulk string for field1 value");
                        }
                    } else if field == b"field2" {
                        found_field2 = true;
                        if let RespValue::BulkString(Some(value)) = &items[i + 1] {
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
        let result = hexists(&engine, &[b"hash1".to_vec(), b"field1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Exists

        let result = hexists(&engine, &[b"hash1".to_vec(), b"nonexistent".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Does not exist

        // Test HDEL
        let result = hdel(&engine, &[b"hash1".to_vec(), b"field1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Deleted 1 field

        // Verify field is gone
        let result = hexists(&engine, &[b"hash1".to_vec(), b"field1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0)); // No longer exists
    }

    #[tokio::test]
    async fn test_hmset() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Test HMSET with multiple fields
        let result = hmset(
            &engine,
            &[
                b"hmset_hash".to_vec(),
                b"field1".to_vec(),
                b"value1".to_vec(),
                b"field2".to_vec(),
                b"value2".to_vec(),
                b"field3".to_vec(),
                b"value3".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify fields were set
        let result = hget(&engine, &[b"hmset_hash".to_vec(), b"field1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value1".to_vec())));

        let result = hget(&engine, &[b"hmset_hash".to_vec(), b"field2".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value2".to_vec())));

        let result = hget(&engine, &[b"hmset_hash".to_vec(), b"field3".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value3".to_vec())));

        // Test HMSET with existing fields (should overwrite)
        let result = hmset(
            &engine,
            &[
                b"hmset_hash".to_vec(),
                b"field1".to_vec(),
                b"new_value1".to_vec(),
                b"field2".to_vec(),
                b"new_value2".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify fields were updated
        let result = hget(&engine, &[b"hmset_hash".to_vec(), b"field1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"new_value1".to_vec())));

        let result = hget(&engine, &[b"hmset_hash".to_vec(), b"field2".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"new_value2".to_vec())));

        // Field3 should be unchanged
        let result = hget(&engine, &[b"hmset_hash".to_vec(), b"field3".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value3".to_vec())));
    }

    #[tokio::test]
    async fn test_hset_multiple_fields() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Test HSET with multiple fields in a single command
        let result = hset(
            &engine,
            &[
                b"multi_hash".to_vec(),
                b"field1".to_vec(),
                b"value1".to_vec(),
                b"field2".to_vec(),
                b"value2".to_vec(),
                b"field3".to_vec(),
                b"value3".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(3)); // 3 new fields

        // Verify fields were set
        let result = hget(&engine, &[b"multi_hash".to_vec(), b"field1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value1".to_vec())));

        let result = hget(&engine, &[b"multi_hash".to_vec(), b"field2".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value2".to_vec())));

        let result = hget(&engine, &[b"multi_hash".to_vec(), b"field3".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value3".to_vec())));

        // Update a few fields
        let result = hset(
            &engine,
            &[
                b"multi_hash".to_vec(),
                b"field1".to_vec(),
                b"updated1".to_vec(),
                b"field4".to_vec(),
                b"value4".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1)); // 1 new field, 1 updated field

        // Verify updates
        let result = hget(&engine, &[b"multi_hash".to_vec(), b"field1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"updated1".to_vec())));

        let result = hget(&engine, &[b"multi_hash".to_vec(), b"field4".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value4".to_vec())));
    }

    #[tokio::test]
    async fn test_hrandfield_basic() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        hset(
            &engine,
            &[
                b"myhash".to_vec(),
                b"f1".to_vec(),
                b"v1".to_vec(),
                b"f2".to_vec(),
                b"v2".to_vec(),
                b"f3".to_vec(),
                b"v3".to_vec(),
            ],
        )
        .await
        .unwrap();

        // HRANDFIELD with no count - returns single field name
        let result = hrandfield(&engine, &[b"myhash".to_vec()]).await.unwrap();
        if let RespValue::BulkString(Some(field)) = result {
            let s = String::from_utf8_lossy(&field);
            assert!(
                s == "f1" || s == "f2" || s == "f3",
                "Got unexpected field: {}",
                s
            );
        } else {
            panic!("Expected BulkString");
        }
    }

    #[tokio::test]
    async fn test_hrandfield_positive_count() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        hset(
            &engine,
            &[
                b"myhash2".to_vec(),
                b"f1".to_vec(),
                b"v1".to_vec(),
                b"f2".to_vec(),
                b"v2".to_vec(),
                b"f3".to_vec(),
                b"v3".to_vec(),
            ],
        )
        .await
        .unwrap();

        // HRANDFIELD with positive count 2 - returns 2 unique fields
        let result = hrandfield(&engine, &[b"myhash2".to_vec(), b"2".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
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
        let engine = StorageEngine::new(config);

        hset(
            &engine,
            &[
                b"myhash3".to_vec(),
                b"f1".to_vec(),
                b"v1".to_vec(),
                b"f2".to_vec(),
                b"v2".to_vec(),
            ],
        )
        .await
        .unwrap();

        // HRANDFIELD count 2 WITHVALUES
        let result = hrandfield(
            &engine,
            &[b"myhash3".to_vec(), b"2".to_vec(), b"WITHVALUES".to_vec()],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4); // 2 fields * 2 (field + value)
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_hrandfield_negative_count() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        hset(
            &engine,
            &[b"myhash4".to_vec(), b"f1".to_vec(), b"v1".to_vec()],
        )
        .await
        .unwrap();

        // Negative count: can return repeats
        let result = hrandfield(&engine, &[b"myhash4".to_vec(), b"-5".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 5);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_hrandfield_empty_hash() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // No count on nonexistent key
        let result = hrandfield(&engine, &[b"nokey".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // With count on nonexistent key
        let result = hrandfield(&engine, &[b"nokey".to_vec(), b"3".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_hscan_basic() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        hset(
            &engine,
            &[
                b"scanhash".to_vec(),
                b"name".to_vec(),
                b"Alice".to_vec(),
                b"age".to_vec(),
                b"30".to_vec(),
                b"city".to_vec(),
                b"NYC".to_vec(),
            ],
        )
        .await
        .unwrap();

        // HSCAN cursor 0
        let result = hscan(&engine, &[b"scanhash".to_vec(), b"0".to_vec()])
            .await
            .unwrap();
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
        let engine = StorageEngine::new(config);

        hset(
            &engine,
            &[
                b"scanhash2".to_vec(),
                b"name".to_vec(),
                b"Bob".to_vec(),
                b"nickname".to_vec(),
                b"Bobby".to_vec(),
                b"age".to_vec(),
                b"25".to_vec(),
            ],
        )
        .await
        .unwrap();

        // HSCAN with MATCH n*
        let result = hscan(
            &engine,
            &[
                b"scanhash2".to_vec(),
                b"0".to_vec(),
                b"MATCH".to_vec(),
                b"n*".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result
            && let RespValue::Array(Some(fv_pairs)) = &items[1]
        {
            // Should match "name" and "nickname"
            assert_eq!(fv_pairs.len(), 4); // 2 matched fields * 2
        }
    }

    #[tokio::test]
    async fn test_hscan_nonexistent_key() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        let result = hscan(&engine, &[b"nokey".to_vec(), b"0".to_vec()])
            .await
            .unwrap();
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
        let engine = StorageEngine::new(config);

        // Add many fields
        for i in 0..15 {
            hset(
                &engine,
                &[
                    b"bighash".to_vec(),
                    format!("field{:02}", i).into_bytes(),
                    format!("value{}", i).into_bytes(),
                ],
            )
            .await
            .unwrap();
        }

        // HSCAN with COUNT 5
        let result = hscan(
            &engine,
            &[
                b"bighash".to_vec(),
                b"0".to_vec(),
                b"COUNT".to_vec(),
                b"5".to_vec(),
            ],
        )
        .await
        .unwrap();
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

    #[tokio::test]
    async fn test_namespace_isolation() {
        // Test that SET key:field and HSET key field don't collide
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Set a string key with composite name that looks like old hash format
        engine
            .set(b"mykey:field1".to_vec(), b"string_value".to_vec(), None)
            .await
            .unwrap();

        // Set a hash key with the same base name
        hset(
            &engine,
            &[
                b"mykey".to_vec(),
                b"field1".to_vec(),
                b"hash_value".to_vec(),
            ],
        )
        .await
        .unwrap();

        // The string key should still be accessible and unchanged
        let string_val = engine.get(b"mykey:field1").await.unwrap();
        assert_eq!(string_val, Some(b"string_value".to_vec()));

        // The hash field should be independent
        let result = hget(&engine, &[b"mykey".to_vec(), b"field1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"hash_value".to_vec())));

        // TYPE should correctly identify them
        let str_type = engine.get_type(b"mykey:field1").await.unwrap();
        assert_eq!(str_type, "string");

        let hash_type = engine.get_type(b"mykey").await.unwrap();
        assert_eq!(hash_type, "hash");
    }

    #[tokio::test]
    async fn test_hgetall_correctness() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Set multiple fields
        hset(
            &engine,
            &[
                b"test_hash".to_vec(),
                b"a".to_vec(),
                b"1".to_vec(),
                b"b".to_vec(),
                b"2".to_vec(),
                b"c".to_vec(),
                b"3".to_vec(),
            ],
        )
        .await
        .unwrap();

        // HGETALL should return all 3 field-value pairs
        let result = hgetall(&engine, &[b"test_hash".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 6);
            let mut map = std::collections::HashMap::new();
            for i in (0..items.len()).step_by(2) {
                if let (RespValue::BulkString(Some(k)), RespValue::BulkString(Some(v))) =
                    (&items[i], &items[i + 1])
                {
                    map.insert(k.clone(), v.clone());
                }
            }
            assert_eq!(map.get(b"a".as_slice()), Some(&b"1".to_vec()));
            assert_eq!(map.get(b"b".as_slice()), Some(&b"2".to_vec()));
            assert_eq!(map.get(b"c".as_slice()), Some(&b"3".to_vec()));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_del_removes_entire_hash() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create a hash with multiple fields
        hset(
            &engine,
            &[
                b"del_hash".to_vec(),
                b"f1".to_vec(),
                b"v1".to_vec(),
                b"f2".to_vec(),
                b"v2".to_vec(),
                b"f3".to_vec(),
                b"v3".to_vec(),
            ],
        )
        .await
        .unwrap();

        // DEL should remove the entire hash as a single key
        let deleted = engine.del(b"del_hash").await.unwrap();
        assert!(deleted);

        // All fields should be gone
        let result = hget(&engine, &[b"del_hash".to_vec(), b"f1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        let result = hlen(&engine, &[b"del_hash".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_concurrent_hset_hget() {
        use std::sync::Arc;
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let mut handles = vec![];
        for i in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let field = format!("field{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                hset(&eng, &[b"concurrent_hash".to_vec(), field, value])
                    .await
                    .unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // All 10 fields should be present
        let result = hlen(&engine, &[b"concurrent_hash".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(10));

        // Verify each field
        for i in 0..10 {
            let field = format!("field{}", i).into_bytes();
            let expected = format!("value{}", i).into_bytes();
            let result = hget(&engine, &[b"concurrent_hash".to_vec(), field])
                .await
                .unwrap();
            assert_eq!(result, RespValue::BulkString(Some(expected)));
        }
    }
}
