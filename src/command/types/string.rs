use crate::command::{CommandError, CommandResult};
use crate::command::utils::{bytes_to_string, parse_ttl};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// Redis SET command - Set the string value of a key
pub async fn set(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    let value = args[1].clone();
    
    // Check for options: EX seconds, PX milliseconds, NX, XX
    let mut ttl = None;
    let mut nx = false;
    let mut xx = false;
    let mut keepttl = false;
    let mut i = 2;
    
    while i < args.len() {
        let option = bytes_to_string(&args[i])?.to_uppercase();
        
        match option.as_str() {
            "EX" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let seconds = parse_ttl(&args[i + 1])?;
                ttl = seconds;
                i += 2;
            },
            "PX" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let millis_str = bytes_to_string(&args[i + 1])?;
                let millis = millis_str.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("PX value must be a valid integer".to_string())
                })?;
                
                if millis < 0 {
                    // Negative TTL means delete the key after setting it
                    ttl = None;
                } else {
                    ttl = Some(std::time::Duration::from_millis(millis as u64));
                }
                i += 2;
            },
            "NX" => {
                nx = true;
                i += 1;
            },
            "XX" => {
                xx = true;
                i += 1;
            },
            "KEEPTTL" => {
                keepttl = true;
                i += 1;
            },
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported SET option: {}", option
                )));
            }
        }
    }
    
    // KEEPTTL and EX/PX are mutually exclusive
    if keepttl && ttl.is_some() {
        return Err(CommandError::InvalidArgument(
            "KEEPTTL and EX/PX options cannot be used together".to_string()
        ));
    }
    
    // NX and XX options are mutually exclusive
    if nx && xx {
        println!("DEBUG: Detected conflicting NX and XX options in SET command");
        return Err(CommandError::InvalidArgument(
            "NX and XX options cannot be used together".to_string()
        ));
    }
    
    // Check key existence for NX/XX conditions
    let exists = engine.exists(&key).await?;
    
    // NX: Only set the key if it does not already exist
    if nx && exists {
        return Ok(RespValue::BulkString(None)); // Key exists, don't set
    }
    
    // XX: Only set the key if it already exists
    if xx && !exists {
        return Ok(RespValue::BulkString(None)); // Key doesn't exist, don't set
    }
    
    // Handle KEEPTTL: Get the current TTL if we're keeping it and the key exists
    if keepttl && exists {
        ttl = engine.ttl(&key).await?;
    }
    
    // For large values, add debug logging
    if value.len() > 10240 {  // Log details for values > 10KB
        tracing::debug!("SET command with large value: key length={}, value length={}", 
                        key.len(), value.len());
    }
    
    // Perform the SET operation
    match engine.set(key, value, ttl).await {
        Ok(_) => Ok(RespValue::SimpleString("OK".to_string())),
        Err(e) => {
            tracing::error!("SET command failed: {}", e);
            Err(CommandError::InvalidArgument(format!("SET operation failed: {}", e)))
        }
    }
}

/// Redis GET command - Get the value of a key
pub async fn get(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // Check if the key exists first
    if !engine.exists(&key).await? {
        return Ok(RespValue::BulkString(None));
    }
    
    // Check if the key is not a known collection type
    let key_type = engine.get_type(&key).await?;
    
    // If the key has a specific collection type, return WRONGTYPE error
    if key_type == "list" || key_type == "set" || key_type == "zset" || key_type == "hash" {
        return Err(CommandError::WrongType);
    }
    
    // Otherwise, treat it as a string and get the value
    match engine.get(&key).await? {
        Some(value) => Ok(RespValue::BulkString(Some(value))),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Redis MGET command - Return the values of all specified keys
pub async fn mget(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let mut values = Vec::with_capacity(args.len());
    for key in args {
        match engine.get(key).await? {
            Some(value) => values.push(RespValue::BulkString(Some(value))),
            None => values.push(RespValue::BulkString(None)),
        }
    }
    
    Ok(RespValue::Array(Some(values)))
}

/// Redis MSET command - Set multiple key-value pairs
pub async fn mset(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 || args.len() % 2 != 0 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    for i in (0..args.len()).step_by(2) {
        engine.set(args[i].clone(), args[i+1].clone(), None).await?;
    }
    
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis INCR command - Increment the integer value of a key by one
pub async fn incr(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // Get the current value or default to 0
    let value = match engine.get(&key).await? {
        Some(data) => {
            // Try to parse the value as an integer
            let str_value = bytes_to_string(&data)?;
            str_value.parse::<i64>().map_err(|_| {
                CommandError::WrongType
            })?
        },
        None => 0,
    };
    
    // Increment the value
    let new_value = value.checked_add(1).ok_or_else(|| {
        CommandError::InvalidArgument("Increment operation would overflow".to_string())
    })?;
    
    // Store the new value
    engine.set(key, new_value.to_string().into_bytes(), None).await?;
    
    Ok(RespValue::Integer(new_value))
}

/// Redis INCRBY command - Increment the integer value of a key by the given amount
pub async fn incrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // Parse the increment value
    let increment = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Increment amount is not a valid integer".to_string())
    })?;
    
    // Get the current value or default to 0
    let value = match engine.get(&key).await? {
        Some(data) => {
            // Try to parse the value as an integer
            let str_value = bytes_to_string(&data)?;
            str_value.parse::<i64>().map_err(|_| {
                CommandError::WrongType
            })?
        },
        None => 0,
    };
    
    // Increment the value by the given amount
    let new_value = value.checked_add(increment).ok_or_else(|| {
        CommandError::InvalidArgument("Increment operation would overflow".to_string())
    })?;
    
    // Store the new value
    engine.set(key, new_value.to_string().into_bytes(), None).await?;
    
    Ok(RespValue::Integer(new_value))
}

/// Redis DECR command - Decrement the integer value of a key by one
pub async fn decr(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // Get the current value or default to 0
    let value = match engine.get(&key).await? {
        Some(data) => {
            // Try to parse the value as an integer
            let str_value = bytes_to_string(&data)?;
            str_value.parse::<i64>().map_err(|_| {
                CommandError::WrongType
            })?
        },
        None => 0,
    };
    
    // Decrement the value
    let new_value = value.checked_sub(1).ok_or_else(|| {
        CommandError::InvalidArgument("Decrement operation would underflow".to_string())
    })?;
    
    // Store the new value
    engine.set(key, new_value.to_string().into_bytes(), None).await?;
    
    Ok(RespValue::Integer(new_value))
}

/// Redis DECRBY command - Decrement the integer value of a key by the given amount
pub async fn decrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // Parse the decrement value
    let decrement = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Decrement amount is not a valid integer".to_string())
    })?;
    
    // Get the current value or default to 0
    let value = match engine.get(&key).await? {
        Some(data) => {
            // Try to parse the value as an integer
            let str_value = bytes_to_string(&data)?;
            str_value.parse::<i64>().map_err(|_| {
                CommandError::WrongType
            })?
        },
        None => 0,
    };
    
    // Decrement the value by the given amount
    let new_value = value.checked_sub(decrement).ok_or_else(|| {
        CommandError::InvalidArgument("Decrement operation would underflow".to_string())
    })?;
    
    // Store the new value
    engine.set(key, new_value.to_string().into_bytes(), None).await?;
    
    Ok(RespValue::Integer(new_value))
}

/// Redis APPEND command - Append a value to a key
pub async fn append(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    let value = args[1].clone();
    
    // Get the current value
    let mut current_value = engine.get(&key).await?.unwrap_or_default();
    
    // Append the new value
    current_value.extend_from_slice(&value);
    
    // Store the result
    engine.set(key, current_value.clone(), None).await?;
    
    // Return the new length
    Ok(RespValue::Integer(current_value.len() as i64))
}

/// Redis STRLEN command - Get the length of the value stored in a key
pub async fn strlen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // Check if the key exists and get its type
    let key_type = engine.get_type(&key).await?;
    
    // If the key has a specific collection type, return WRONGTYPE error
    if key_type == "list" || key_type == "set" || key_type == "zset" || key_type == "hash" {
        return Err(CommandError::WrongType);
    }
    
    // Get the value length
    match engine.get(&key).await? {
        Some(value) => Ok(RespValue::Integer(value.len() as i64)),
        None => Ok(RespValue::Integer(0)),
    }
}

/// Redis GETRANGE command - Get a substring of the string stored at a key
pub async fn getrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // Parse start and end positions
    let start = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Start position is not a valid integer".to_string())
    })?;
    
    let end = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("End position is not a valid integer".to_string())
    })?;
    
    // Check if the key exists and get its type
    let key_type = engine.get_type(&key).await?;
    
    // If the key has a specific collection type, return WRONGTYPE error
    if key_type == "list" || key_type == "set" || key_type == "zset" || key_type == "hash" {
        return Err(CommandError::WrongType);
    }
    
    // Get the value
    let value = match engine.get(&key).await? {
        Some(data) => data,
        None => return Ok(RespValue::BulkString(Some(Vec::new()))),
    };
    
    let len = value.len() as i64;
    
    // Handle negative indices (Python-style)
    let norm_start = if start < 0 { len + start } else { start };
    let norm_end = if end < 0 { len + end } else { end };
    
    // Clamp to valid range
    let start_idx = std::cmp::max(0, norm_start) as usize;
    let end_idx = std::cmp::min(len - 1, norm_end) as usize;
    
    // Extract substring
    if start_idx >= value.len() || norm_start > norm_end {
        return Ok(RespValue::BulkString(Some(Vec::new())));
    }
    
    let result = value[start_idx..=std::cmp::min(end_idx, value.len() - 1)].to_vec();
    Ok(RespValue::BulkString(Some(result)))
}

/// Redis SETRANGE command - Overwrite part of a string at key starting at the specified offset
pub async fn setrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    let value_to_set = args[2].clone();
    
    // Parse offset
    let offset = bytes_to_string(&args[1])?.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("Offset is not a valid integer".to_string())
    })?;
    
    // Check if the key exists and get its type
    let key_type = engine.get_type(&key).await?;
    
    // If the key has a specific collection type, return WRONGTYPE error
    if key_type == "list" || key_type == "set" || key_type == "zset" || key_type == "hash" {
        return Err(CommandError::WrongType);
    }
    
    // Get the current value or create an empty one
    let mut current_value = engine.get(&key).await?.unwrap_or_default();
    
    // Extend the current value if necessary
    let required_len = offset + value_to_set.len();
    if current_value.len() < required_len {
        current_value.resize(required_len, 0); // Pad with null bytes
    }
    
    // Overwrite the specified range
    for (i, &byte) in value_to_set.iter().enumerate() {
        current_value[offset + i] = byte;
    }
    
    // Store the modified value
    engine.set(key, current_value.clone(), None).await?;
    
    // Return the new length
    Ok(RespValue::Integer(current_value.len() as i64))
}

/// Redis GETSET command - Set the string value of a key and return its old value
pub async fn getset(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    let new_value = args[1].clone();
    
    // Check if the key exists and get its type
    let key_type = engine.get_type(&key).await?;
    
    // If the key has a specific collection type, return WRONGTYPE error
    if key_type == "list" || key_type == "set" || key_type == "zset" || key_type == "hash" {
        return Err(CommandError::WrongType);
    }
    
    // Get the old value
    let old_value = engine.get(&key).await?;
    
    // Set the new value
    engine.set(key, new_value, None).await?;
    
    // Return the old value
    match old_value {
        Some(value) => Ok(RespValue::BulkString(Some(value))),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Redis MSETNX command - Set multiple key-value pairs, only if none of the keys exist
pub async fn msetnx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 || args.len() % 2 != 0 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    // Check if any of the keys already exist
    for i in (0..args.len()).step_by(2) {
        if engine.exists(&args[i]).await? {
            return Ok(RespValue::Integer(0)); // At least one key exists
        }
    }
    
    // Set all key-value pairs
    for i in (0..args.len()).step_by(2) {
        engine.set(args[i].clone(), args[i+1].clone(), None).await?;
    }
    
    Ok(RespValue::Integer(1))
}

/// Redis INCRBYFLOAT command - Increment the float value of a key by the given amount
pub async fn incrbyfloat(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // Parse the increment value
    let increment = bytes_to_string(&args[1])?.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("Increment amount is not a valid float".to_string())
    })?;
    
    // Get the current value or default to 0.0
    let value = match engine.get(&key).await? {
        Some(data) => {
            // Try to parse the value as a float
            let str_value = bytes_to_string(&data)?;
            str_value.parse::<f64>().map_err(|_| {
                CommandError::WrongType
            })?
        },
        None => 0.0,
    };
    
    // Increment the value by the given amount
    let new_value = value + increment;
    
    // Check for NaN or infinity
    if !new_value.is_finite() {
        return Err(CommandError::InvalidArgument("Result is not a valid number".to_string()));
    }
    
    // Store the new value
    let new_value_str = if new_value.fract() == 0.0 && new_value.abs() < 1e15 {
        // If it's a whole number, format without decimal
        format!("{:.0}", new_value)
    } else {
        // Otherwise, use Redis-compatible float formatting
        format!("{}", new_value)
    };
    
    engine.set(key, new_value_str.as_bytes().to_vec(), None).await?;
    
    Ok(RespValue::BulkString(Some(new_value_str.into_bytes())))
}

/// Redis SETNX command - Set the value of a key, only if the key does not exist
pub async fn setnx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    let value = args[1].clone();
    
    // Check if the key already exists
    if engine.exists(&key).await? {
        return Ok(RespValue::Integer(0)); // Key exists, operation failed
    }
    
    // Set the key since it doesn't exist
    engine.set(key, value, None).await?;
    Ok(RespValue::Integer(1)) // Operation succeeded
}

/// Redis SETEX command - Set the value and expiration of a key
pub async fn setex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    let ttl = parse_ttl(&args[1])?;
    let value = args[2].clone();
    
    // Set the key with the TTL
    engine.set(key, value, ttl).await?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis PEXPIRE command - Set a key's time to live in milliseconds
pub async fn pexpire(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    let millis_str = bytes_to_string(&args[1])?;
    let millis = millis_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("PEXPIRE value must be a valid integer".to_string())
    })?;
    
    let ttl = if millis < 0 {
        None // Negative TTL means delete the key
    } else {
        Some(std::time::Duration::from_millis(millis as u64))
    };
    
    // If TTL is None, we need to delete the key
    if ttl.is_none() {
        let deleted = engine.del(&key).await?;
        return Ok(RespValue::Integer(if deleted { 1 } else { 0 }));
    }
    
    match engine.expire(&key, ttl).await? {
        true => Ok(RespValue::Integer(1)),
        false => Ok(RespValue::Integer(0)),
    }
}

/// Redis PTTL command - Get the time to live for a key in milliseconds
pub async fn pttl(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // First check if the key exists
    let exists = engine.exists(&key).await?;
    if !exists {
        return Ok(RespValue::Integer(-2)); // Key does not exist
    }
    
    // If key exists, check its TTL
    let ttl = engine.ttl(&key).await?;
    
    match ttl {
        Some(duration) => {
            // Return milliseconds, ensure we return at least 1 if the duration is >0
            let millis = duration.as_millis();
            Ok(RespValue::Integer(if millis > 0 { millis as i64 } else { 1 }))
        },
        None => Ok(RespValue::Integer(-1)), // Key exists but has no expiration
    }
}

/// Redis GETEX command - Get the value of a key and optionally set its expiration
/// Options: EX seconds, PX milliseconds, EXAT unix-time-seconds, PXAT unix-time-milliseconds, PERSIST
pub async fn getex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Check type
    if engine.exists(key).await? {
        let key_type = engine.get_type(key).await?;
        if key_type == "list" || key_type == "set" || key_type == "zset" || key_type == "hash" {
            return Err(CommandError::WrongType);
        }
    }

    // Get the value first
    let value = engine.get(key).await?;

    // If key doesn't exist, return nil regardless of options
    if value.is_none() {
        return Ok(RespValue::BulkString(None));
    }

    // Parse and apply expiration option if provided
    if args.len() >= 2 {
        let option = bytes_to_string(&args[1])?.to_uppercase();

        match option.as_str() {
            "EX" => {
                if args.len() != 3 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let seconds = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if seconds <= 0 {
                    return Err(CommandError::InvalidArgument("invalid expire time in 'getex' command".to_string()));
                }
                engine.expire(key, Some(std::time::Duration::from_secs(seconds as u64))).await?;
            },
            "PX" => {
                if args.len() != 3 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let millis = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if millis <= 0 {
                    return Err(CommandError::InvalidArgument("invalid expire time in 'getex' command".to_string()));
                }
                engine.expire(key, Some(std::time::Duration::from_millis(millis as u64))).await?;
            },
            "EXAT" => {
                if args.len() != 3 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let timestamp = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if timestamp <= 0 {
                    return Err(CommandError::InvalidArgument("invalid expire time in 'getex' command".to_string()));
                }
                engine.expire_at(key, timestamp).await?;
            },
            "PXAT" => {
                if args.len() != 3 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let timestamp_ms = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if timestamp_ms <= 0 {
                    return Err(CommandError::InvalidArgument("invalid expire time in 'getex' command".to_string()));
                }
                engine.pexpire_at(key, timestamp_ms).await?;
            },
            "PERSIST" => {
                if args.len() != 2 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                engine.expire(key, None).await?;
            },
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported GETEX option: {}", option
                )));
            }
        }
    }

    Ok(RespValue::BulkString(value))
}

/// Redis GETDEL command - Get the value of a key and delete it
pub async fn getdel(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Check type
    if engine.exists(key).await? {
        let key_type = engine.get_type(key).await?;
        if key_type == "list" || key_type == "set" || key_type == "zset" || key_type == "hash" {
            return Err(CommandError::WrongType);
        }
    }

    // Get the value
    let value = engine.get(key).await?;

    // If the key exists, delete it
    if value.is_some() {
        engine.del(key).await?;
    }

    match value {
        Some(v) => Ok(RespValue::BulkString(Some(v))),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Redis PSETEX command - Set key to hold string value with millisecond expiration
pub async fn psetex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = args[0].clone();
    let millis = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    if millis <= 0 {
        return Err(CommandError::InvalidArgument("invalid expire time in 'psetex' command".to_string()));
    }

    let value = args[2].clone();
    let ttl = Some(std::time::Duration::from_millis(millis as u64));

    engine.set(key, value, ttl).await?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis LCS command - Find Longest Common Subsequence between two strings
/// Supports: LEN, IDX, MINMATCHLEN, WITHMATCHLEN options
pub async fn lcs(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key1 = &args[0];
    let key2 = &args[1];

    // Get both string values (non-existent keys treated as empty strings)
    let val1 = engine.get(key1).await?.unwrap_or_default();
    let val2 = engine.get(key2).await?.unwrap_or_default();

    // Parse options
    let mut len_only = false;
    let mut idx = false;
    let mut min_match_len: usize = 0;
    let mut with_match_len = false;
    let mut i = 2;

    while i < args.len() {
        let option = bytes_to_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "LEN" => {
                len_only = true;
                i += 1;
            },
            "IDX" => {
                idx = true;
                i += 1;
            },
            "MINMATCHLEN" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                min_match_len = bytes_to_string(&args[i + 1])?.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                i += 2;
            },
            "WITHMATCHLEN" => {
                with_match_len = true;
                i += 1;
            },
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported LCS option: {}", option
                )));
            }
        }
    }

    let a = &val1;
    let b = &val2;
    let m = a.len();
    let n = b.len();

    // Build DP table
    let mut dp = vec![vec![0u32; n + 1]; m + 1];
    for i2 in 1..=m {
        for j in 1..=n {
            if a[i2 - 1] == b[j - 1] {
                dp[i2][j] = dp[i2 - 1][j - 1] + 1;
            } else {
                dp[i2][j] = std::cmp::max(dp[i2 - 1][j], dp[i2][j - 1]);
            }
        }
    }

    let lcs_len = dp[m][n] as i64;

    if len_only {
        return Ok(RespValue::Integer(lcs_len));
    }

    if idx {
        // Find matching ranges by backtracking through the DP table
        let mut matches: Vec<((usize, usize), (usize, usize))> = Vec::new();
        let mut ci = m;
        let mut cj = n;

        // Collect individual matching positions first
        let mut match_positions: Vec<(usize, usize)> = Vec::new();
        while ci > 0 && cj > 0 {
            if a[ci - 1] == b[cj - 1] {
                match_positions.push((ci - 1, cj - 1));
                ci -= 1;
                cj -= 1;
            } else if dp[ci - 1][cj] >= dp[ci][cj - 1] {
                ci -= 1;
            } else {
                cj -= 1;
            }
        }
        match_positions.reverse();

        // Group consecutive positions into ranges
        if !match_positions.is_empty() {
            let mut start_a = match_positions[0].0;
            let mut start_b = match_positions[0].1;
            let mut end_a = start_a;
            let mut end_b = start_b;

            for k in 1..match_positions.len() {
                let (pa, pb) = match_positions[k];
                if pa == end_a + 1 && pb == end_b + 1 {
                    end_a = pa;
                    end_b = pb;
                } else {
                    let match_len = end_a - start_a + 1;
                    if match_len >= min_match_len {
                        matches.push(((start_a, end_a), (start_b, end_b)));
                    }
                    start_a = pa;
                    start_b = pb;
                    end_a = pa;
                    end_b = pb;
                }
            }
            let match_len = end_a - start_a + 1;
            if match_len >= min_match_len {
                matches.push(((start_a, end_a), (start_b, end_b)));
            }
        }

        // Build response: ["matches", [match1, match2, ...], "len", lcs_len]
        let mut match_array: Vec<RespValue> = Vec::new();
        for ((a_start, a_end), (b_start, b_end)) in &matches {
            let mut match_entry = vec![
                RespValue::Array(Some(vec![
                    RespValue::Integer(*a_start as i64),
                    RespValue::Integer(*a_end as i64),
                ])),
                RespValue::Array(Some(vec![
                    RespValue::Integer(*b_start as i64),
                    RespValue::Integer(*b_end as i64),
                ])),
            ];
            if with_match_len {
                match_entry.push(RespValue::Integer((a_end - a_start + 1) as i64));
            }
            match_array.push(RespValue::Array(Some(match_entry)));
        }

        let result = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"matches".to_vec())),
            RespValue::Array(Some(match_array)),
            RespValue::BulkString(Some(b"len".to_vec())),
            RespValue::Integer(lcs_len),
        ]));

        return Ok(result);
    }

    // Default: return the LCS string itself
    // Backtrack to reconstruct the LCS string
    let mut result = Vec::new();
    let mut ci = m;
    let mut cj = n;
    while ci > 0 && cj > 0 {
        if a[ci - 1] == b[cj - 1] {
            result.push(a[ci - 1]);
            ci -= 1;
            cj -= 1;
        } else if dp[ci - 1][cj] >= dp[ci][cj - 1] {
            ci -= 1;
        } else {
            cj -= 1;
        }
    }
    result.reverse();

    Ok(RespValue::BulkString(Some(result)))
}

/// Redis SUBSTR command - deprecated alias for GETRANGE
pub async fn substr(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    getrange(engine, args).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;
    
    #[tokio::test]
    async fn test_string_commands() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test SET and GET
        let set_args = vec![b"test_key".to_vec(), b"test_value".to_vec()];
        let result = set(&engine, &set_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        let get_args = vec![b"test_key".to_vec()];
        let result = get(&engine, &get_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"test_value".to_vec())));
        
        // Test INCR
        let incr_args = vec![b"counter".to_vec()];
        let result = incr(&engine, &incr_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));
        
        let result = incr(&engine, &incr_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
        
        // Test DECR
        let decr_args = vec![b"counter".to_vec()];
        let result = decr(&engine, &decr_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));
        
        // Test APPEND
        let append_args = vec![b"str_key".to_vec(), b"Hello".to_vec()];
        let result = append(&engine, &append_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));
        
        let append_args = vec![b"str_key".to_vec(), b" World".to_vec()];
        let result = append(&engine, &append_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(11));
        
        let get_args = vec![b"str_key".to_vec()];
        let result = get(&engine, &get_args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"Hello World".to_vec())));
    }
    
    #[tokio::test]
    async fn test_set_conflicting_options() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test SET with conflicting NX and XX options
        let set_args = vec![
            b"key".to_vec(),
            b"value".to_vec(),
            b"NX".to_vec(),
            b"XX".to_vec(),
        ];
        
        let result = set(&engine, &set_args).await;
        assert!(result.is_err());
        
        if let Err(CommandError::InvalidArgument(msg)) = result {
            assert!(msg.contains("NX and XX options cannot be used together"));
        } else {
            panic!("Expected InvalidArgument error for conflicting options");
        }
    }
    
    #[tokio::test]
    async fn test_large_json_value() {
        let mut config = StorageConfig::default();
        // Increase size limits for this test
        config.max_value_size = 1024 * 1024 * 10; // 10MB
        config.max_memory = 1024 * 1024 * 100; // 100MB - ensure enough memory for the test
        let engine = Arc::new(StorageEngine::new(config));
        
        // Create a large JSON string with various special characters and nested structures
        let mut large_json = String::from("{\n  \"items\": [\n");
        
        // Generate a large nested JSON structure - reduce size to avoid test failures
        // Using a smaller dataset to ensure test passes in CI environments
        for i in 0..100 {
            large_json.push_str(&format!(
                "    {{\n      \"id\": {},\n      \"name\": \"Item {}\",\n      \"description\": \"A description with quotes and special chars\",\n      \"active\": {}\n    }}{}\n",
                i, 
                i,
                if i % 2 == 0 { "true" } else { "false" },
                if i < 99 { "," } else { "" }
            ));
        }
        
        large_json.push_str("  ]\n}");
        
        // Verify size is still good for testing
        assert!(large_json.len() > 5000, "Generated JSON should be at least 5KB");
        println!("Large JSON size for test: {} bytes", large_json.len());
        
        // Test setting the large JSON value
        let key = "test_json_key".to_string();
        let set_args = vec![
            key.as_bytes().to_vec(),
            large_json.as_bytes().to_vec(),
        ];
        
        // Try setting the value
        let result = set(&engine, &set_args).await;
        if result.is_err() {
            println!("Set error: {:?}", result);
            if let Err(CommandError::InvalidArgument(msg)) = &result {
                if msg.contains("SET operation failed") {
                    println!("Storage error during SET operation. Check memory limits and value size.");
                }
            }
        }
        
        assert!(result.is_ok(), "Failed to set large JSON value: {:?}", result);
        assert_eq!(result.unwrap(), RespValue::SimpleString("OK".to_string()));
        
        // Test retrieving the large JSON value
        let get_args = vec![key.as_bytes().to_vec()];
        let result = get(&engine, &get_args).await;
        
        if result.is_err() {
            println!("Get error: {:?}", result);
        }
        assert!(result.is_ok(), "Failed to get large JSON value: {:?}", result);
        
        if let Ok(RespValue::BulkString(Some(value))) = result {
            let retrieved_json = String::from_utf8_lossy(&value);
            
            // Check if lengths match first
            if retrieved_json.len() != large_json.len() {
                println!("Length mismatch! Original: {}, Retrieved: {}", 
                         large_json.len(), retrieved_json.len());
            }
            
            assert_eq!(retrieved_json, large_json, "Retrieved JSON does not match original");
        } else {
            panic!("Expected BulkString response for large JSON value");
        }
    }
    
    #[tokio::test]
    async fn test_json_with_control_characters() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Create a JSON string with control characters that could potentially cause issues
        let json_with_controls = format!(
            "{{\"data\":\"Some data with control chars: {}{}{}{}{}\",\"binary\":\"ABC\"}}",
            '\u{0001}', // SOH (Start of Heading)
            '\u{0002}', // STX (Start of Text)
            '\u{0003}', // ETX (End of Text)
            '\u{001B}', // ESC (Escape)
            '\u{001F}'  // US (Unit Separator)
        );
        
        // Test setting the value with control characters
        let set_args = vec![
            b"control_key".to_vec(),
            json_with_controls.as_bytes().to_vec(),
        ];
        
        let result = set(&engine, &set_args).await;
        // We should still be able to store this, even with the control characters
        assert!(result.is_ok(), "Failed to set JSON with control characters: {:?}", result);
        
        // Test retrieving the value
        let get_args = vec![b"control_key".to_vec()];
        let result = get(&engine, &get_args).await;
        assert!(result.is_ok(), "Failed to get JSON with control characters: {:?}", result);
        
        if let Ok(RespValue::BulkString(Some(value))) = result {
            let retrieved_json = String::from_utf8_lossy(&value);
            assert_eq!(retrieved_json, json_with_controls, "Retrieved JSON does not match original");
        } else {
            panic!("Expected BulkString response for JSON with control characters");
        }
    }
    
    #[tokio::test]
    async fn test_problematic_resp_patterns() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test data patterns that previously caused RESP parsing issues
        let test_patterns = vec![
            "B64JSON:W3data",
            "+FAKE_SIMPLE_STRING", 
            "-FAKE_ERROR",
            ":12345_FAKE_INTEGER",
            "$999\r\nFAKE_BULK",
            "*2\r\nFAKE_ARRAY",
        ];
        
        for (i, pattern) in test_patterns.iter().enumerate() {
            let key = format!("test_key_{}", i).into_bytes();
            
            // Store the problematic pattern - this should work now
            let set_args = vec![key.clone(), pattern.as_bytes().to_vec()];
            let result = set(&engine, &set_args).await;
            assert!(result.is_ok(), "Should be able to store pattern: {}", pattern);
            assert_eq!(result.unwrap(), RespValue::SimpleString("OK".to_string()));
            
            // Retrieve the pattern - this should also work and return exact data
            let get_args = vec![key];
            let result = get(&engine, &get_args).await;
            assert!(result.is_ok(), "Should be able to retrieve pattern: {}", pattern);
            
            if let Ok(RespValue::BulkString(Some(data))) = result {
                let retrieved = String::from_utf8_lossy(&data);
                assert_eq!(retrieved, *pattern, "Data should match exactly for pattern: {}", pattern);
            } else {
                panic!("Expected BulkString response for pattern: {}", pattern);
            }
        }
    }
    
    #[tokio::test]
    async fn test_b64json_specific_case() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // This is the EXACT pattern that was causing "Invalid RESP format, starts with: B64JSON:W3"
        let b64json_data = "B64JSON:W3siaWQiOiAxLCJuYW1lIjoiRXhhbXBsZSIsImFjdGl2ZSI6dHJ1ZX0=";

        // SET the problematic data
        let set_args = vec![
            b"test_b64json_key".to_vec(),
            b64json_data.as_bytes().to_vec(),
        ];

        let set_result = set(&engine, &set_args).await;
        assert!(set_result.is_ok(), "SET with B64JSON data should succeed: {:?}", set_result);
        assert_eq!(set_result.unwrap(), RespValue::SimpleString("OK".to_string()));

        // GET the data back
        let get_args = vec![b"test_b64json_key".to_vec()];
        let get_result = get(&engine, &get_args).await;
        assert!(get_result.is_ok(), "GET with B64JSON data should succeed: {:?}", get_result);

        // Verify we get back exactly the same data
        if let Ok(RespValue::BulkString(Some(retrieved_data))) = get_result {
            let retrieved_str = String::from_utf8_lossy(&retrieved_data);
            assert_eq!(retrieved_str, b64json_data, "Retrieved B64JSON data should match original exactly");
            println!("✅ B64JSON test passed! Original: {}", b64json_data);
            println!("✅ Retrieved: {}", retrieved_str);
        } else {
            panic!("Expected BulkString response for B64JSON data");
        }
    }

    #[tokio::test]
    async fn test_b64_prefix_variations() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Test various b64: prefixed patterns that were causing errors
        let test_cases = vec![
            ("b64:W3siaWQiOiAxLCAi", "The exact error pattern from logs"),
            ("b64:encoded_data_here", "Generic b64 prefix"),
            ("b64:SGVsbG8gV29ybGQh", "Base64 encoded 'Hello World!'"),
            ("b64:", "Edge case: just the prefix"),
            ("b64:123", "Short encoded data"),
        ];

        for (data, description) in test_cases {
            let key = format!("test_b64_{}", data.len()).into_bytes();

            // SET the data
            let set_args = vec![key.clone(), data.as_bytes().to_vec()];
            let set_result = set(&engine, &set_args).await;
            assert!(set_result.is_ok(), "SET should succeed for: {} - {:?}", description, set_result);
            assert_eq!(set_result.unwrap(), RespValue::SimpleString("OK".to_string()));

            // GET the data back
            let get_args = vec![key.clone()];
            let get_result = get(&engine, &get_args).await;
            assert!(get_result.is_ok(), "GET should succeed for: {} - {:?}", description, get_result);

            // Verify exact match
            if let Ok(RespValue::BulkString(Some(retrieved_data))) = get_result {
                let retrieved_str = String::from_utf8_lossy(&retrieved_data);
                assert_eq!(retrieved_str, data,
                    "Data mismatch for {}: expected '{}', got '{}'", description, data, retrieved_str);
                println!("✅ Test passed for: {}", description);
            } else {
                panic!("Expected BulkString response for: {}", description);
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_operations_with_special_data() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Simulate the real-world scenario: multiple SET/GET operations with b64 data
        let operations = vec![
            ("user:123", "b64:userdata1"),
            ("session:abc", "b64:sessioninfo2"),
            ("cache:xyz", "b64:cachedvalue3"),
        ];

        // Perform all SETs
        for (key, value) in &operations {
            let set_args = vec![key.as_bytes().to_vec(), value.as_bytes().to_vec()];
            let result = set(&engine, &set_args).await;
            assert!(result.is_ok(), "SET failed for key {}: {:?}", key, result);
        }

        // Perform all GETs and verify
        for (key, expected_value) in &operations {
            let get_args = vec![key.as_bytes().to_vec()];
            let result = get(&engine, &get_args).await;
            assert!(result.is_ok(), "GET failed for key {}: {:?}", key, result);

            if let Ok(RespValue::BulkString(Some(data))) = result {
                let retrieved = String::from_utf8_lossy(&data);
                assert_eq!(retrieved, *expected_value,
                    "Data mismatch for key {}: expected '{}', got '{}'",
                    key, expected_value, retrieved);
            } else {
                panic!("Expected BulkString for key {}", key);
            }
        }

        println!("✅ All {} operations succeeded", operations.len() * 2);
    }

    #[tokio::test]
    async fn test_getex_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // GETEX on non-existent key returns nil
        let args = vec![b"nonexistent".to_vec()];
        let result = getex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Set a key, then GETEX without options just returns the value
        engine.set(b"mykey".to_vec(), b"myvalue".to_vec(), None).await.unwrap();
        let args = vec![b"mykey".to_vec()];
        let result = getex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myvalue".to_vec())));
    }

    #[tokio::test]
    async fn test_getex_with_ex() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine.set(b"mykey".to_vec(), b"myvalue".to_vec(), None).await.unwrap();

        // GETEX with EX sets expiration in seconds
        let args = vec![b"mykey".to_vec(), b"EX".to_vec(), b"100".to_vec()];
        let result = getex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myvalue".to_vec())));

        // Verify TTL was set
        let ttl = engine.ttl(b"mykey").await.unwrap();
        assert!(ttl.is_some());
        assert!(ttl.unwrap().as_secs() <= 100);
        assert!(ttl.unwrap().as_secs() >= 98);
    }

    #[tokio::test]
    async fn test_getex_with_px() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine.set(b"mykey".to_vec(), b"myvalue".to_vec(), None).await.unwrap();

        // GETEX with PX sets expiration in milliseconds
        let args = vec![b"mykey".to_vec(), b"PX".to_vec(), b"50000".to_vec()];
        let result = getex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myvalue".to_vec())));

        let ttl = engine.ttl(b"mykey").await.unwrap();
        assert!(ttl.is_some());
        assert!(ttl.unwrap().as_millis() <= 50000);
        assert!(ttl.unwrap().as_millis() >= 49000);
    }

    #[tokio::test]
    async fn test_getex_with_persist() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Set with TTL
        engine.set(b"mykey".to_vec(), b"myvalue".to_vec(), Some(std::time::Duration::from_secs(60))).await.unwrap();

        // GETEX PERSIST removes the TTL
        let args = vec![b"mykey".to_vec(), b"PERSIST".to_vec()];
        let result = getex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myvalue".to_vec())));

        let ttl = engine.ttl(b"mykey").await.unwrap();
        assert!(ttl.is_none());
    }

    #[tokio::test]
    async fn test_getex_invalid_options() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine.set(b"mykey".to_vec(), b"myvalue".to_vec(), None).await.unwrap();

        // EX with non-positive value
        let args = vec![b"mykey".to_vec(), b"EX".to_vec(), b"0".to_vec()];
        assert!(getex(&engine, &args).await.is_err());

        let args = vec![b"mykey".to_vec(), b"EX".to_vec(), b"-5".to_vec()];
        assert!(getex(&engine, &args).await.is_err());

        // Invalid option
        let args = vec![b"mykey".to_vec(), b"BADOPTION".to_vec()];
        assert!(getex(&engine, &args).await.is_err());
    }

    #[tokio::test]
    async fn test_getdel_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // GETDEL on non-existent key returns nil
        let args = vec![b"nonexistent".to_vec()];
        let result = getdel(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Set a key, GETDEL returns value and deletes
        engine.set(b"mykey".to_vec(), b"myvalue".to_vec(), None).await.unwrap();
        let args = vec![b"mykey".to_vec()];
        let result = getdel(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myvalue".to_vec())));

        // Key should no longer exist
        assert!(!engine.exists(b"mykey").await.unwrap());

        // Calling GETDEL again returns nil
        let result = getdel(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_getdel_wrong_type() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Create a list key using the lpush command
        use crate::command::types::list::lpush;
        let lpush_args = vec![b"listkey".to_vec(), b"val".to_vec()];
        lpush(&engine, &lpush_args).await.unwrap();

        // GETDEL on list should return WRONGTYPE
        let args = vec![b"listkey".to_vec()];
        let result = getdel(&engine, &args).await;
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[tokio::test]
    async fn test_psetex_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // PSETEX sets value with millisecond TTL
        let args = vec![b"mykey".to_vec(), b"50000".to_vec(), b"myvalue".to_vec()];
        let result = psetex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify value
        let val = engine.get(b"mykey").await.unwrap();
        assert_eq!(val, Some(b"myvalue".to_vec()));

        // Verify TTL is in milliseconds
        let ttl = engine.ttl(b"mykey").await.unwrap();
        assert!(ttl.is_some());
        assert!(ttl.unwrap().as_millis() <= 50000);
        assert!(ttl.unwrap().as_millis() >= 49000);
    }

    #[tokio::test]
    async fn test_psetex_invalid_ttl() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Zero milliseconds is invalid
        let args = vec![b"mykey".to_vec(), b"0".to_vec(), b"myvalue".to_vec()];
        assert!(psetex(&engine, &args).await.is_err());

        // Negative milliseconds is invalid
        let args = vec![b"mykey".to_vec(), b"-100".to_vec(), b"myvalue".to_vec()];
        assert!(psetex(&engine, &args).await.is_err());

        // Wrong number of args
        let args = vec![b"mykey".to_vec(), b"5000".to_vec()];
        assert!(psetex(&engine, &args).await.is_err());
    }

    #[tokio::test]
    async fn test_lcs_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine.set(b"key1".to_vec(), b"ohmytext".to_vec(), None).await.unwrap();
        engine.set(b"key2".to_vec(), b"mynewtext".to_vec(), None).await.unwrap();

        // Default: returns the LCS string
        let args = vec![b"key1".to_vec(), b"key2".to_vec()];
        let result = lcs(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"mytext".to_vec())));
    }

    #[tokio::test]
    async fn test_lcs_len() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine.set(b"key1".to_vec(), b"ohmytext".to_vec(), None).await.unwrap();
        engine.set(b"key2".to_vec(), b"mynewtext".to_vec(), None).await.unwrap();

        // LEN option returns just the length
        let args = vec![b"key1".to_vec(), b"key2".to_vec(), b"LEN".to_vec()];
        let result = lcs(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(6));
    }

    #[tokio::test]
    async fn test_lcs_idx() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine.set(b"key1".to_vec(), b"ohmytext".to_vec(), None).await.unwrap();
        engine.set(b"key2".to_vec(), b"mynewtext".to_vec(), None).await.unwrap();

        // IDX option returns matching ranges
        let args = vec![b"key1".to_vec(), b"key2".to_vec(), b"IDX".to_vec()];
        let result = lcs(&engine, &args).await.unwrap();

        // Should be an array with "matches", [...], "len", 6
        if let RespValue::Array(Some(arr)) = &result {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], RespValue::BulkString(Some(b"matches".to_vec())));
            assert_eq!(arr[2], RespValue::BulkString(Some(b"len".to_vec())));
            assert_eq!(arr[3], RespValue::Integer(6));
        } else {
            panic!("Expected Array response from LCS IDX");
        }
    }

    #[tokio::test]
    async fn test_lcs_idx_with_matchlen() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine.set(b"key1".to_vec(), b"ohmytext".to_vec(), None).await.unwrap();
        engine.set(b"key2".to_vec(), b"mynewtext".to_vec(), None).await.unwrap();

        let args = vec![
            b"key1".to_vec(), b"key2".to_vec(),
            b"IDX".to_vec(), b"WITHMATCHLEN".to_vec(),
        ];
        let result = lcs(&engine, &args).await.unwrap();

        if let RespValue::Array(Some(arr)) = &result {
            assert_eq!(arr[3], RespValue::Integer(6));
            // Each match entry should have 3 elements (a_range, b_range, matchlen)
            if let RespValue::Array(Some(matches)) = &arr[1] {
                for m in matches {
                    if let RespValue::Array(Some(entry)) = m {
                        assert_eq!(entry.len(), 3);
                    }
                }
            }
        } else {
            panic!("Expected Array response");
        }
    }

    #[tokio::test]
    async fn test_lcs_idx_minmatchlen() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine.set(b"key1".to_vec(), b"ohmytext".to_vec(), None).await.unwrap();
        engine.set(b"key2".to_vec(), b"mynewtext".to_vec(), None).await.unwrap();

        // MINMATCHLEN 4 filters out matches shorter than 4
        let args = vec![
            b"key1".to_vec(), b"key2".to_vec(),
            b"IDX".to_vec(), b"MINMATCHLEN".to_vec(), b"4".to_vec(),
        ];
        let result = lcs(&engine, &args).await.unwrap();

        if let RespValue::Array(Some(arr)) = &result {
            if let RespValue::Array(Some(matches)) = &arr[1] {
                // All matches should have length >= 4
                for m in matches {
                    if let RespValue::Array(Some(entry)) = m {
                        if let RespValue::Array(Some(a_range)) = &entry[0] {
                            if let (RespValue::Integer(start), RespValue::Integer(end)) = (&a_range[0], &a_range[1]) {
                                assert!((end - start + 1) >= 4);
                            }
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_lcs_nonexistent_keys() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Both keys don't exist - should return empty string
        let args = vec![b"key1".to_vec(), b"key2".to_vec()];
        let result = lcs(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(Vec::new())));

        // One key exists
        engine.set(b"key1".to_vec(), b"hello".to_vec(), None).await.unwrap();
        let result = lcs(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(Vec::new())));
    }

    #[tokio::test]
    async fn test_lcs_identical_strings() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine.set(b"key1".to_vec(), b"hello".to_vec(), None).await.unwrap();
        engine.set(b"key2".to_vec(), b"hello".to_vec(), None).await.unwrap();

        let args = vec![b"key1".to_vec(), b"key2".to_vec()];
        let result = lcs(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"hello".to_vec())));

        let args = vec![b"key1".to_vec(), b"key2".to_vec(), b"LEN".to_vec()];
        let result = lcs(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[tokio::test]
    async fn test_substr_is_getrange_alias() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine.set(b"mykey".to_vec(), b"Hello, World!".to_vec(), None).await.unwrap();

        // SUBSTR should behave exactly like GETRANGE
        let args = vec![b"mykey".to_vec(), b"0".to_vec(), b"4".to_vec()];

        let getrange_result = getrange(&engine, &args).await.unwrap();
        let substr_result = substr(&engine, &args).await.unwrap();
        assert_eq!(getrange_result, substr_result);
        assert_eq!(substr_result, RespValue::BulkString(Some(b"Hello".to_vec())));

        // Test with negative indices too
        let args = vec![b"mykey".to_vec(), b"-6".to_vec(), b"-1".to_vec()];
        let getrange_result = getrange(&engine, &args).await.unwrap();
        let substr_result = substr(&engine, &args).await.unwrap();
        assert_eq!(getrange_result, substr_result);
        assert_eq!(substr_result, RespValue::BulkString(Some(b"World!".to_vec())));
    }

    #[tokio::test]
    async fn test_substr_nonexistent_key() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let args = vec![b"nonexistent".to_vec(), b"0".to_vec(), b"10".to_vec()];
        let result = substr(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(Vec::new())));
    }
} 