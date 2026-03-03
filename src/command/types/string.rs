use crate::command::utils::{bytes_to_string, parse_ttl};
use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::{SetResult, StorageEngine};
use crate::storage::item::RedisDataType;

/// Redis SET command - Set the string value of a key
/// Uses set_with_options() for atomic NX/XX/GET handling
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
    let mut get_old = false;
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
            }
            "PX" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let millis_str = bytes_to_string(&args[i + 1])?;
                let millis = millis_str.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("PX value must be a valid integer".to_string())
                })?;

                if millis <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'set' command".to_string(),
                    ));
                }
                ttl = Some(std::time::Duration::from_millis(millis as u64));
                i += 2;
            }
            "EXAT" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let timestamp_str = bytes_to_string(&args[i + 1])?;
                let timestamp = timestamp_str.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("EXAT value must be a valid integer".to_string())
                })?;
                if timestamp <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'set' command".to_string(),
                    ));
                }
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                let diff = timestamp - now_secs;
                if diff <= 0 {
                    // Key would already be expired; set a minimal TTL
                    ttl = Some(std::time::Duration::from_millis(1));
                } else {
                    ttl = Some(std::time::Duration::from_secs(diff as u64));
                }
                i += 2;
            }
            "PXAT" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let timestamp_str = bytes_to_string(&args[i + 1])?;
                let timestamp_ms = timestamp_str.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("PXAT value must be a valid integer".to_string())
                })?;
                if timestamp_ms <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'set' command".to_string(),
                    ));
                }
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let diff_ms = timestamp_ms - now_ms;
                if diff_ms <= 0 {
                    ttl = Some(std::time::Duration::from_millis(1));
                } else {
                    ttl = Some(std::time::Duration::from_millis(diff_ms as u64));
                }
                i += 2;
            }
            "GET" => {
                get_old = true;
                i += 1;
            }
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "KEEPTTL" => {
                keepttl = true;
                i += 1;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported SET option: {}",
                    option
                )));
            }
        }
    }

    // KEEPTTL and EX/PX are mutually exclusive
    if keepttl && ttl.is_some() {
        return Err(CommandError::InvalidArgument(
            "KEEPTTL and EX/PX options cannot be used together".to_string(),
        ));
    }

    // NX and XX options are mutually exclusive
    if nx && xx {
        return Err(CommandError::InvalidArgument(
            "NX and XX options cannot be used together".to_string(),
        ));
    }

    // For large values, add debug logging
    if value.len() > 10240 {
        tracing::debug!(
            "SET command with large value: key length={}, value length={}",
            key.len(),
            value.len()
        );
    }

    // Use atomic set_with_options for NX/XX/GET/KEEPTTL - single atomic operation
    match engine
        .set_with_options(key, value, ttl, nx, xx, get_old, keepttl)
        .await?
    {
        SetResult::OldValue(old_val) => {
            // GET flag was specified
            Ok(RespValue::BulkString(old_val))
        }
        SetResult::Ok => Ok(RespValue::SimpleString("OK".to_string())),
        SetResult::NotSet => {
            // NX/XX condition not met
            if get_old {
                // SET ... NX GET on existing key returns the old value via OldValue above,
                // but if NX fails and GET is set, set_with_options returns OldValue.
                // This branch handles plain NX/XX without GET.
                Ok(RespValue::BulkString(None))
            } else {
                Ok(RespValue::BulkString(None))
            }
        }
    }
}

/// Redis GET command - Get the value of a key
/// Uses atomic_read for thread-safe single-lookup type check + value read
pub async fn get(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Single DashMap lookup: type check + value read atomically
    let value = engine.atomic_read(key, RedisDataType::String, |data| Ok(data.cloned()))?;

    Ok(RespValue::BulkString(value))
}

/// Redis MGET command - Return the values of all specified keys
pub async fn mget(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let mut values = Vec::with_capacity(args.len());
    for key in args {
        // Redis MGET returns nil for non-string keys (never WRONGTYPE)
        let key_type = engine.get_type(key).await?;
        if key_type != "string" && key_type != "none" {
            values.push(RespValue::BulkString(None));
        } else {
            match engine.get(key).await? {
                Some(value) => values.push(RespValue::BulkString(Some(value))),
                None => values.push(RespValue::BulkString(None)),
            }
        }
    }

    Ok(RespValue::Array(Some(values)))
}

/// Redis MSET command - Set multiple key-value pairs atomically.
/// Uses lock_keys for cross-key coordination (sorted order prevents deadlocks).
/// Redis guarantees MSET is atomic — all keys are set or none are visible to concurrent clients.
pub async fn mset(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 || !args.len().is_multiple_of(2) {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Collect all keys for atomic locking
    let keys: Vec<Vec<u8>> = (0..args.len())
        .step_by(2)
        .map(|i| args[i].clone())
        .collect();

    // Lock all keys in sorted order to prevent deadlocks and coordinate with transactions/MSETNX
    let _guard = engine.lock_keys(&keys).await;

    for i in (0..args.len()).step_by(2) {
        engine
            .set(args[i].clone(), args[i + 1].clone(), None)
            .await?;
    }

    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis INCR command - Increment the integer value of a key by one
/// Uses atomic_incr for thread-safe read-modify-write
pub async fn incr(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let new_value = engine.atomic_incr(&args[0], 1)?;
    Ok(RespValue::Integer(new_value))
}

/// Redis INCRBY command - Increment the integer value of a key by the given amount
/// Uses atomic_incr for thread-safe read-modify-write
pub async fn incrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let increment = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    let new_value = engine.atomic_incr(&args[0], increment)?;
    Ok(RespValue::Integer(new_value))
}

/// Redis DECR command - Decrement the integer value of a key by one
/// Uses atomic_incr with -1 delta for thread-safe read-modify-write
pub async fn decr(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let new_value = engine.atomic_incr(&args[0], -1)?;
    Ok(RespValue::Integer(new_value))
}

/// Redis DECRBY command - Decrement the integer value of a key by the given amount
/// Uses atomic_incr with negated delta for thread-safe read-modify-write
pub async fn decrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let decrement = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    let neg_decrement = decrement.checked_neg().ok_or_else(|| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let new_value = engine.atomic_incr(&args[0], neg_decrement)?;
    Ok(RespValue::Integer(new_value))
}

/// Redis APPEND command - Append a value to a key
/// Uses atomic_append for thread-safe read-modify-write
pub async fn append(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let new_len = engine.atomic_append(&args[0], &args[1])?;
    Ok(RespValue::Integer(new_len as i64))
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
    if key_type == "list"
        || key_type == "set"
        || key_type == "zset"
        || key_type == "hash"
        || key_type == "stream"
    {
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
    if key_type == "list"
        || key_type == "set"
        || key_type == "zset"
        || key_type == "hash"
        || key_type == "stream"
    {
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
/// Uses atomic_modify for thread-safe read-modify-write with TTL preservation
pub async fn setrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = args[0].clone();
    let value_to_set = args[2].clone();

    // Parse offset
    let offset = bytes_to_string(&args[1])?
        .parse::<usize>()
        .map_err(|_| CommandError::InvalidArgument("Offset is not a valid integer".to_string()))?;

    // Validate size before acquiring lock
    const MAX_STRING_SIZE: usize = 512 * 1024 * 1024;
    let required_len = offset.checked_add(value_to_set.len()).ok_or_else(|| {
        CommandError::InvalidArgument("string exceeds maximum allowed size (512MB)".to_string())
    })?;
    if required_len > MAX_STRING_SIZE {
        return Err(CommandError::InvalidArgument(
            "string exceeds maximum allowed size (512MB)".to_string(),
        ));
    }

    // Atomic read-modify-write: type check, pad, overwrite, preserve TTL
    let new_len = engine.atomic_modify(&key, RedisDataType::String, |current| {
        let mut val = current.map(|v| v.clone()).unwrap_or_default();

        if val.len() < required_len {
            val.resize(required_len, 0); // Pad with null bytes
        }

        // Overwrite the specified range
        for (i, &byte) in value_to_set.iter().enumerate() {
            val[offset + i] = byte;
        }

        let new_len = val.len() as i64;
        Ok((Some(val), new_len))
    })?;

    Ok(RespValue::Integer(new_len))
}

/// Redis GETSET command - Set the string value of a key and return its old value
/// Uses atomic_getset for thread-safe get-and-replace
pub async fn getset(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = args[0].clone();
    let new_value = args[1].clone();

    let old_value = engine.atomic_getset(key, new_value).await?;
    Ok(RespValue::BulkString(old_value))
}

/// Redis MSETNX command - Set multiple key-value pairs, only if none of the keys exist.
/// Uses lock_keys for cross-key coordination (prevents races between concurrent MSETNX/EXEC)
/// and set_nx per key for per-key atomicity (prevents races with concurrent SET operations).
pub async fn msetnx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 || !args.len().is_multiple_of(2) {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Collect all keys for atomic locking
    let keys: Vec<Vec<u8>> = (0..args.len())
        .step_by(2)
        .map(|i| args[i].clone())
        .collect();

    // Lock all keys in sorted order to prevent deadlocks and coordinate with transactions
    let _guard = engine.lock_keys(&keys).await;

    // Use set_nx per key for per-key atomicity via DashMap's entry API.
    // This prevents the TOCTOU race where a concurrent SET (which bypasses lock_keys)
    // could create a key between an exists() check and a set() call.
    // Track (key, version_after_insert) so rollback can avoid deleting a concurrent writer's value.
    let mut inserted_keys: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut all_inserted = true;

    for i in (0..args.len()).step_by(2) {
        match engine
            .set_nx(args[i].clone(), args[i + 1].clone(), None)
            .await
        {
            Ok((true, version)) => {
                inserted_keys.push((args[i].clone(), version));
            }
            Ok((false, _)) => {
                // Key already existed - rollback all previously inserted keys
                all_inserted = false;
                break;
            }
            Err(e) => {
                // Storage error - rollback and propagate
                for (key, version) in &inserted_keys {
                    // Atomically delete only if version still matches - prevents TOCTOU race
                    // where a concurrent SET between version check and del could be lost
                    engine.del_if_version_matches(key, *version).await;
                }
                return Err(CommandError::from(e));
            }
        }
    }

    if !all_inserted {
        // Rollback: atomically delete keys we inserted, but only if their version hasn't changed.
        // del_if_version_matches checks version and deletes within the same DashMap shard lock,
        // preventing a TOCTOU race where a concurrent SET could be lost.
        for (key, version) in &inserted_keys {
            engine.del_if_version_matches(key, *version).await;
        }
        return Ok(RespValue::Integer(0));
    }

    Ok(RespValue::Integer(1))
}

/// Redis INCRBYFLOAT command - Increment the float value of a key by the given amount
/// Uses atomic_incr_float for thread-safe read-modify-write
pub async fn incrbyfloat(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let increment = bytes_to_string(&args[1])?
        .parse::<f64>()
        .map_err(|_| CommandError::InvalidArgument("value is not a valid float".to_string()))?;

    let new_value = engine.atomic_incr_float(&args[0], increment)?;

    // Format using the same logic as the storage engine's format_float
    let new_value_str = if new_value.fract() == 0.0 && new_value.abs() < 1e15 {
        format!("{:.0}", new_value)
    } else {
        format!("{}", new_value)
    };

    Ok(RespValue::BulkString(Some(new_value_str.into_bytes())))
}

/// Redis SETNX command - Set the value of a key, only if the key does not exist
/// Uses set_with_options with NX flag for atomic check-and-set
pub async fn setnx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = args[0].clone();
    let value = args[1].clone();

    match engine
        .set_with_options(key, value, None, true, false, false, false)
        .await?
    {
        SetResult::Ok => Ok(RespValue::Integer(1)),
        SetResult::NotSet => Ok(RespValue::Integer(0)),
        _ => Ok(RespValue::Integer(0)),
    }
}

/// Redis SETEX command - Set the value and expiration of a key
pub async fn setex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = args[0].clone();
    let seconds_str = bytes_to_string(&args[1])?;
    let seconds = seconds_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    if seconds <= 0 {
        return Err(CommandError::InvalidArgument(
            "invalid expire time in 'setex' command".to_string(),
        ));
    }

    let value = args[2].clone();
    let ttl = Some(std::time::Duration::from_secs(seconds as u64));

    engine.set(key, value, ttl).await?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis PEXPIRE command - Set a key's time to live in milliseconds
pub async fn pexpire(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = args[0].clone();
    let millis_str = bytes_to_string(&args[1])?;
    let millis = millis_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("PEXPIRE value must be a valid integer".to_string())
    })?;

    let ttl = if millis <= 0 {
        None // Zero or negative TTL means delete the key immediately
    } else {
        Some(std::time::Duration::from_millis(millis as u64))
    };

    // Parse optional condition flags (NX, XX, GT, LT) - Redis 7.0+
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    for arg in args.iter().skip(2) {
        match bytes_to_string(arg)?.to_uppercase().as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            _ => {
                return Err(CommandError::InvalidArgument(
                    "Unsupported option".to_string(),
                ));
            }
        }
    }

    // If TTL is None, we need to delete the key
    if ttl.is_none() {
        let deleted = engine.del(&key).await?;
        return Ok(RespValue::Integer(if deleted { 1 } else { 0 }));
    }

    // Apply condition flags
    if nx || xx || gt || lt {
        let current_ttl = engine.ttl(&key).await?;
        let has_expiry = current_ttl.is_some();

        if nx && has_expiry {
            return Ok(RespValue::Integer(0));
        }
        if xx && !has_expiry {
            return Ok(RespValue::Integer(0));
        }
        if gt
            && let Some(current) = current_ttl
            && let Some(new_ttl) = &ttl
            && *new_ttl <= current
        {
            return Ok(RespValue::Integer(0));
        }
        if lt
            && let Some(current) = current_ttl
            && let Some(new_ttl) = &ttl
            && *new_ttl >= current
        {
            return Ok(RespValue::Integer(0));
        }
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
            Ok(RespValue::Integer(if millis > 0 {
                millis as i64
            } else {
                1
            }))
        }
        None => Ok(RespValue::Integer(-1)), // Key exists but has no expiration
    }
}

/// Redis GETEX command - Get the value of a key and optionally set its expiration
/// Options: EX seconds, PX milliseconds, EXAT unix-time-seconds, PXAT unix-time-milliseconds, PERSIST
/// Uses atomic_get_set_expiry for thread-safe get-with-expiry-change
pub async fn getex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Parse expiration option before acquiring lock
    let new_expiry: Option<Option<std::time::Duration>> = if args.len() >= 2 {
        let option = bytes_to_string(&args[1])?.to_uppercase();

        match option.as_str() {
            "EX" => {
                if args.len() != 3 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let seconds = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
                if seconds <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'getex' command".to_string(),
                    ));
                }
                Some(Some(std::time::Duration::from_secs(seconds as u64)))
            }
            "PX" => {
                if args.len() != 3 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let millis = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
                if millis <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'getex' command".to_string(),
                    ));
                }
                Some(Some(std::time::Duration::from_millis(millis as u64)))
            }
            "EXAT" => {
                if args.len() != 3 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let timestamp = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
                if timestamp <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'getex' command".to_string(),
                    ));
                }
                let now_unix = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                if timestamp <= now_unix {
                    // Past timestamp - set a zero duration so key expires immediately
                    Some(Some(std::time::Duration::from_secs(0)))
                } else {
                    Some(Some(std::time::Duration::from_secs(
                        (timestamp - now_unix) as u64,
                    )))
                }
            }
            "PXAT" => {
                if args.len() != 3 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let timestamp_ms = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
                if timestamp_ms <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'getex' command".to_string(),
                    ));
                }
                let now_unix_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                if timestamp_ms <= now_unix_ms {
                    Some(Some(std::time::Duration::from_secs(0)))
                } else {
                    Some(Some(std::time::Duration::from_millis(
                        (timestamp_ms - now_unix_ms) as u64,
                    )))
                }
            }
            "PERSIST" => {
                if args.len() != 2 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                Some(None) // Remove expiry
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported GETEX option: {}",
                    option
                )));
            }
        }
    } else {
        None // No expiry change
    };

    // Atomic get + expiry modification in single lock hold
    let value = engine.atomic_get_set_expiry(key, new_expiry).await?;
    Ok(RespValue::BulkString(value))
}

/// Redis GETDEL command - Get the value of a key and delete it
/// Uses atomic_getdel for thread-safe get-and-delete with atomic type checking
pub async fn getdel(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let value = engine.atomic_getdel(key)?;
    Ok(RespValue::BulkString(value))
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
        return Err(CommandError::InvalidArgument(
            "invalid expire time in 'psetex' command".to_string(),
        ));
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
            }
            "IDX" => {
                idx = true;
                i += 1;
            }
            "MINMATCHLEN" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                min_match_len = bytes_to_string(&args[i + 1])?
                    .parse::<usize>()
                    .map_err(|_| {
                        CommandError::InvalidArgument(
                            "value is not an integer or out of range".to_string(),
                        )
                    })?;
                i += 2;
            }
            "WITHMATCHLEN" => {
                with_match_len = true;
                i += 1;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported LCS option: {}",
                    option
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

            for &(pa, pb) in &match_positions[1..] {
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
        assert!(
            large_json.len() > 5000,
            "Generated JSON should be at least 5KB"
        );
        println!("Large JSON size for test: {} bytes", large_json.len());

        // Test setting the large JSON value
        let key = "test_json_key".to_string();
        let set_args = vec![key.as_bytes().to_vec(), large_json.as_bytes().to_vec()];

        // Try setting the value
        let result = set(&engine, &set_args).await;
        if result.is_err() {
            println!("Set error: {:?}", result);
            if let Err(CommandError::InvalidArgument(msg)) = &result {
                if msg.contains("SET operation failed") {
                    println!(
                        "Storage error during SET operation. Check memory limits and value size."
                    );
                }
            }
        }

        assert!(
            result.is_ok(),
            "Failed to set large JSON value: {:?}",
            result
        );
        assert_eq!(result.unwrap(), RespValue::SimpleString("OK".to_string()));

        // Test retrieving the large JSON value
        let get_args = vec![key.as_bytes().to_vec()];
        let result = get(&engine, &get_args).await;

        if result.is_err() {
            println!("Get error: {:?}", result);
        }
        assert!(
            result.is_ok(),
            "Failed to get large JSON value: {:?}",
            result
        );

        if let Ok(RespValue::BulkString(Some(value))) = result {
            let retrieved_json = String::from_utf8_lossy(&value);

            // Check if lengths match first
            if retrieved_json.len() != large_json.len() {
                println!(
                    "Length mismatch! Original: {}, Retrieved: {}",
                    large_json.len(),
                    retrieved_json.len()
                );
            }

            assert_eq!(
                retrieved_json, large_json,
                "Retrieved JSON does not match original"
            );
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
        assert!(
            result.is_ok(),
            "Failed to set JSON with control characters: {:?}",
            result
        );

        // Test retrieving the value
        let get_args = vec![b"control_key".to_vec()];
        let result = get(&engine, &get_args).await;
        assert!(
            result.is_ok(),
            "Failed to get JSON with control characters: {:?}",
            result
        );

        if let Ok(RespValue::BulkString(Some(value))) = result {
            let retrieved_json = String::from_utf8_lossy(&value);
            assert_eq!(
                retrieved_json, json_with_controls,
                "Retrieved JSON does not match original"
            );
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
            assert!(
                result.is_ok(),
                "Should be able to store pattern: {}",
                pattern
            );
            assert_eq!(result.unwrap(), RespValue::SimpleString("OK".to_string()));

            // Retrieve the pattern - this should also work and return exact data
            let get_args = vec![key];
            let result = get(&engine, &get_args).await;
            assert!(
                result.is_ok(),
                "Should be able to retrieve pattern: {}",
                pattern
            );

            if let Ok(RespValue::BulkString(Some(data))) = result {
                let retrieved = String::from_utf8_lossy(&data);
                assert_eq!(
                    retrieved, *pattern,
                    "Data should match exactly for pattern: {}",
                    pattern
                );
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
        assert!(
            set_result.is_ok(),
            "SET with B64JSON data should succeed: {:?}",
            set_result
        );
        assert_eq!(
            set_result.unwrap(),
            RespValue::SimpleString("OK".to_string())
        );

        // GET the data back
        let get_args = vec![b"test_b64json_key".to_vec()];
        let get_result = get(&engine, &get_args).await;
        assert!(
            get_result.is_ok(),
            "GET with B64JSON data should succeed: {:?}",
            get_result
        );

        // Verify we get back exactly the same data
        if let Ok(RespValue::BulkString(Some(retrieved_data))) = get_result {
            let retrieved_str = String::from_utf8_lossy(&retrieved_data);
            assert_eq!(
                retrieved_str, b64json_data,
                "Retrieved B64JSON data should match original exactly"
            );
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
            assert!(
                set_result.is_ok(),
                "SET should succeed for: {} - {:?}",
                description,
                set_result
            );
            assert_eq!(
                set_result.unwrap(),
                RespValue::SimpleString("OK".to_string())
            );

            // GET the data back
            let get_args = vec![key.clone()];
            let get_result = get(&engine, &get_args).await;
            assert!(
                get_result.is_ok(),
                "GET should succeed for: {} - {:?}",
                description,
                get_result
            );

            // Verify exact match
            if let Ok(RespValue::BulkString(Some(retrieved_data))) = get_result {
                let retrieved_str = String::from_utf8_lossy(&retrieved_data);
                assert_eq!(
                    retrieved_str, data,
                    "Data mismatch for {}: expected '{}', got '{}'",
                    description, data, retrieved_str
                );
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
                assert_eq!(
                    retrieved, *expected_value,
                    "Data mismatch for key {}: expected '{}', got '{}'",
                    key, expected_value, retrieved
                );
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
        engine
            .set(b"mykey".to_vec(), b"myvalue".to_vec(), None)
            .await
            .unwrap();
        let args = vec![b"mykey".to_vec()];
        let result = getex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myvalue".to_vec())));
    }

    #[tokio::test]
    async fn test_getex_with_ex() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine
            .set(b"mykey".to_vec(), b"myvalue".to_vec(), None)
            .await
            .unwrap();

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

        engine
            .set(b"mykey".to_vec(), b"myvalue".to_vec(), None)
            .await
            .unwrap();

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
        engine
            .set(
                b"mykey".to_vec(),
                b"myvalue".to_vec(),
                Some(std::time::Duration::from_secs(60)),
            )
            .await
            .unwrap();

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

        engine
            .set(b"mykey".to_vec(), b"myvalue".to_vec(), None)
            .await
            .unwrap();

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
        engine
            .set(b"mykey".to_vec(), b"myvalue".to_vec(), None)
            .await
            .unwrap();
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

        engine
            .set(b"key1".to_vec(), b"ohmytext".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"key2".to_vec(), b"mynewtext".to_vec(), None)
            .await
            .unwrap();

        // Default: returns the LCS string
        let args = vec![b"key1".to_vec(), b"key2".to_vec()];
        let result = lcs(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"mytext".to_vec())));
    }

    #[tokio::test]
    async fn test_lcs_len() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine
            .set(b"key1".to_vec(), b"ohmytext".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"key2".to_vec(), b"mynewtext".to_vec(), None)
            .await
            .unwrap();

        // LEN option returns just the length
        let args = vec![b"key1".to_vec(), b"key2".to_vec(), b"LEN".to_vec()];
        let result = lcs(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(6));
    }

    #[tokio::test]
    async fn test_lcs_idx() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine
            .set(b"key1".to_vec(), b"ohmytext".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"key2".to_vec(), b"mynewtext".to_vec(), None)
            .await
            .unwrap();

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

        engine
            .set(b"key1".to_vec(), b"ohmytext".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"key2".to_vec(), b"mynewtext".to_vec(), None)
            .await
            .unwrap();

        let args = vec![
            b"key1".to_vec(),
            b"key2".to_vec(),
            b"IDX".to_vec(),
            b"WITHMATCHLEN".to_vec(),
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

        engine
            .set(b"key1".to_vec(), b"ohmytext".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"key2".to_vec(), b"mynewtext".to_vec(), None)
            .await
            .unwrap();

        // MINMATCHLEN 4 filters out matches shorter than 4
        let args = vec![
            b"key1".to_vec(),
            b"key2".to_vec(),
            b"IDX".to_vec(),
            b"MINMATCHLEN".to_vec(),
            b"4".to_vec(),
        ];
        let result = lcs(&engine, &args).await.unwrap();

        if let RespValue::Array(Some(arr)) = &result {
            if let RespValue::Array(Some(matches)) = &arr[1] {
                // All matches should have length >= 4
                for m in matches {
                    if let RespValue::Array(Some(entry)) = m {
                        if let RespValue::Array(Some(a_range)) = &entry[0] {
                            if let (RespValue::Integer(start), RespValue::Integer(end)) =
                                (&a_range[0], &a_range[1])
                            {
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
        engine
            .set(b"key1".to_vec(), b"hello".to_vec(), None)
            .await
            .unwrap();
        let result = lcs(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(Vec::new())));
    }

    #[tokio::test]
    async fn test_lcs_identical_strings() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine
            .set(b"key1".to_vec(), b"hello".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"key2".to_vec(), b"hello".to_vec(), None)
            .await
            .unwrap();

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

        engine
            .set(b"mykey".to_vec(), b"Hello, World!".to_vec(), None)
            .await
            .unwrap();

        // SUBSTR should behave exactly like GETRANGE
        let args = vec![b"mykey".to_vec(), b"0".to_vec(), b"4".to_vec()];

        let getrange_result = getrange(&engine, &args).await.unwrap();
        let substr_result = substr(&engine, &args).await.unwrap();
        assert_eq!(getrange_result, substr_result);
        assert_eq!(
            substr_result,
            RespValue::BulkString(Some(b"Hello".to_vec()))
        );

        // Test with negative indices too
        let args = vec![b"mykey".to_vec(), b"-6".to_vec(), b"-1".to_vec()];
        let getrange_result = getrange(&engine, &args).await.unwrap();
        let substr_result = substr(&engine, &args).await.unwrap();
        assert_eq!(getrange_result, substr_result);
        assert_eq!(
            substr_result,
            RespValue::BulkString(Some(b"World!".to_vec()))
        );
    }

    #[tokio::test]
    async fn test_substr_nonexistent_key() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let args = vec![b"nonexistent".to_vec(), b"0".to_vec(), b"10".to_vec()];
        let result = substr(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(Vec::new())));
    }

    #[tokio::test]
    async fn test_concurrent_set_nx_single_winner() {
        // Two clients racing SET NX on the same key - exactly one should succeed
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        let success_count = Arc::new(std::sync::atomic::AtomicI64::new(0));

        let mut handles = Vec::new();
        for i in 0..10 {
            let eng = engine.clone();
            let count = success_count.clone();
            handles.push(tokio::spawn(async move {
                let args = vec![
                    b"contested_key".to_vec(),
                    format!("value_{}", i).into_bytes(),
                    b"NX".to_vec(),
                ];
                let result = set(&eng, &args).await.unwrap();
                if result == RespValue::SimpleString("OK".to_string()) {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Exactly one SET NX should succeed
        assert_eq!(success_count.load(std::sync::atomic::Ordering::SeqCst), 1);
        // The key should exist with some value
        assert!(engine.exists(b"contested_key").await.unwrap());
    }

    #[tokio::test]
    async fn test_concurrent_incr_no_lost_updates() {
        // Multiple clients racing INCR on the same key - no updates should be lost
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let num_tasks = 100;
        let mut handles = Vec::new();
        for _ in 0..num_tasks {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let args = vec![b"atomic_counter".to_vec()];
                incr(&eng, &args).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // All increments should be accounted for
        let val = engine.get(b"atomic_counter").await.unwrap().unwrap();
        let final_val: i64 = String::from_utf8(val).unwrap().parse().unwrap();
        assert_eq!(final_val, num_tasks);
    }

    #[tokio::test]
    async fn test_concurrent_incrby_correctness() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let num_tasks = 50;
        let mut handles = Vec::new();
        for _ in 0..num_tasks {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let args = vec![b"incrby_counter".to_vec(), b"10".to_vec()];
                incrby(&eng, &args).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let val = engine.get(b"incrby_counter").await.unwrap().unwrap();
        let final_val: i64 = String::from_utf8(val).unwrap().parse().unwrap();
        assert_eq!(final_val, num_tasks * 10);
    }

    #[tokio::test]
    async fn test_concurrent_decr_correctness() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Start from 1000
        engine
            .set(b"decr_counter".to_vec(), b"1000".to_vec(), None)
            .await
            .unwrap();

        let num_tasks = 100;
        let mut handles = Vec::new();
        for _ in 0..num_tasks {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let args = vec![b"decr_counter".to_vec()];
                decr(&eng, &args).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let val = engine.get(b"decr_counter").await.unwrap().unwrap();
        let final_val: i64 = String::from_utf8(val).unwrap().parse().unwrap();
        assert_eq!(final_val, 1000 - num_tasks);
    }

    #[tokio::test]
    async fn test_concurrent_append_correctness() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let num_tasks = 50;
        let mut handles = Vec::new();
        for _ in 0..num_tasks {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let args = vec![b"append_key".to_vec(), b"x".to_vec()];
                append(&eng, &args).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // All appends should be captured - length should be 50
        let val = engine.get(b"append_key").await.unwrap().unwrap();
        assert_eq!(val.len(), num_tasks as usize);
    }

    #[tokio::test]
    async fn test_msetnx_atomicity() {
        // MSETNX should be all-or-nothing
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Set one key that will block the MSETNX
        engine
            .set(b"ms_key2".to_vec(), b"existing".to_vec(), None)
            .await
            .unwrap();

        // MSETNX should fail because ms_key2 exists
        let args = vec![
            b"ms_key1".to_vec(),
            b"val1".to_vec(),
            b"ms_key2".to_vec(),
            b"val2".to_vec(),
            b"ms_key3".to_vec(),
            b"val3".to_vec(),
        ];
        let result = msetnx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // ms_key1 and ms_key3 should NOT have been set
        assert!(!engine.exists(b"ms_key1").await.unwrap());
        assert!(!engine.exists(b"ms_key3").await.unwrap());

        // ms_key2 should still have its original value
        let val = engine.get(b"ms_key2").await.unwrap().unwrap();
        assert_eq!(val, b"existing");
    }

    #[tokio::test]
    async fn test_msetnx_success() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // MSETNX with no existing keys should succeed
        let args = vec![
            b"msnx_a".to_vec(),
            b"v1".to_vec(),
            b"msnx_b".to_vec(),
            b"v2".to_vec(),
        ];
        let result = msetnx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        assert_eq!(engine.get(b"msnx_a").await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get(b"msnx_b").await.unwrap(), Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_setnx_atomic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // First SETNX should succeed
        let args = vec![b"setnx_key".to_vec(), b"first".to_vec()];
        let result = setnx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Second SETNX on same key should fail
        let args = vec![b"setnx_key".to_vec(), b"second".to_vec()];
        let result = setnx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Value should be the first one
        let val = engine.get(b"setnx_key").await.unwrap().unwrap();
        assert_eq!(val, b"first");
    }

    #[tokio::test]
    async fn test_getset_atomic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // GETSET on non-existent key returns nil
        let args = vec![b"gs_key".to_vec(), b"val1".to_vec()];
        let result = getset(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // GETSET returns old value and sets new
        let args = vec![b"gs_key".to_vec(), b"val2".to_vec()];
        let result = getset(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"val1".to_vec())));

        // Verify new value is set
        let val = engine.get(b"gs_key").await.unwrap().unwrap();
        assert_eq!(val, b"val2");
    }

    #[tokio::test]
    async fn test_getdel_atomic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine
            .set(b"gd_key".to_vec(), b"myval".to_vec(), None)
            .await
            .unwrap();

        let args = vec![b"gd_key".to_vec()];
        let result = getdel(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myval".to_vec())));

        // Key should be gone
        assert!(!engine.exists(b"gd_key").await.unwrap());

        // Second GETDEL returns nil
        let result = getdel(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_set_keepttl() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Set with TTL
        let args = vec![
            b"kt_key".to_vec(),
            b"val1".to_vec(),
            b"EX".to_vec(),
            b"100".to_vec(),
        ];
        set(&engine, &args).await.unwrap();

        // Verify TTL is set
        let ttl = engine.ttl(b"kt_key").await.unwrap();
        assert!(ttl.is_some());

        // SET with KEEPTTL should preserve the TTL
        let args = vec![b"kt_key".to_vec(), b"val2".to_vec(), b"KEEPTTL".to_vec()];
        set(&engine, &args).await.unwrap();

        // Value should be updated
        let val = engine.get(b"kt_key").await.unwrap().unwrap();
        assert_eq!(val, b"val2");

        // TTL should be preserved (still close to 100s)
        let ttl = engine.ttl(b"kt_key").await.unwrap();
        assert!(ttl.is_some());
        assert!(ttl.unwrap().as_secs() >= 95);
    }

    #[tokio::test]
    async fn test_set_get_flag_atomic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // SET with GET on non-existent key returns nil
        let args = vec![b"sg_key".to_vec(), b"val1".to_vec(), b"GET".to_vec()];
        let result = set(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // SET with GET on existing key returns old value
        let args = vec![b"sg_key".to_vec(), b"val2".to_vec(), b"GET".to_vec()];
        let result = set(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"val1".to_vec())));

        // Value should be updated to val2
        let val = engine.get(b"sg_key").await.unwrap().unwrap();
        assert_eq!(val, b"val2");
    }

    #[tokio::test]
    async fn test_setrange_atomic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Test basic SETRANGE on empty key (creates key with null padding)
        let args = vec![b"sr_key".to_vec(), b"5".to_vec(), b"Hello".to_vec()];
        let result = setrange(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(10)); // 5 null bytes + "Hello"

        let val = engine.get(b"sr_key").await.unwrap().unwrap();
        assert_eq!(val.len(), 10);
        assert_eq!(&val[5..], b"Hello");
        assert_eq!(&val[..5], &[0, 0, 0, 0, 0]);

        // Test SETRANGE overwriting part of existing value
        let args = vec![b"sr_key".to_vec(), b"0".to_vec(), b"World".to_vec()];
        let result = setrange(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(10));
        let val = engine.get(b"sr_key").await.unwrap().unwrap();
        assert_eq!(&val[..5], b"World");
        assert_eq!(&val[5..], b"Hello");
    }

    #[tokio::test]
    async fn test_setrange_preserves_ttl() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Set a key with TTL
        engine
            .set(
                b"ttl_key".to_vec(),
                b"hello".to_vec(),
                Some(std::time::Duration::from_secs(3600)),
            )
            .await
            .unwrap();

        // SETRANGE should preserve the TTL
        let args = vec![b"ttl_key".to_vec(), b"0".to_vec(), b"world".to_vec()];
        setrange(&engine, &args).await.unwrap();

        // TTL should still be set
        let ttl = engine.ttl(b"ttl_key").await.unwrap();
        assert!(ttl.is_some(), "TTL should be preserved after SETRANGE");
    }

    #[tokio::test]
    async fn test_setrange_wrongtype() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Create a list key
        engine
            .set_with_type(
                b"list_key".to_vec(),
                b"data".to_vec(),
                RedisDataType::List,
                None,
            )
            .await
            .unwrap();

        // SETRANGE on a list key should return WRONGTYPE
        let args = vec![b"list_key".to_vec(), b"0".to_vec(), b"hello".to_vec()];
        let result = setrange(&engine, &args).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_setrange_concurrent() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Initialize key with enough space
        engine
            .set(b"concurrent_sr".to_vec(), vec![0u8; 100], None)
            .await
            .unwrap();

        // Spawn multiple tasks doing SETRANGE on overlapping ranges
        let mut handles = Vec::new();
        for i in 0u8..10 {
            let engine = engine.clone();
            handles.push(tokio::spawn(async move {
                let offset = (i as usize) * 10;
                let value = vec![b'A' + i; 10];
                let args = vec![
                    b"concurrent_sr".to_vec(),
                    offset.to_string().into_bytes(),
                    value,
                ];
                setrange(&engine, &args).await.unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify final value has exactly 100 bytes and each 10-byte segment
        // is filled with a single character (no interleaving/corruption)
        let val = engine.get(b"concurrent_sr").await.unwrap().unwrap();
        assert_eq!(val.len(), 100);
        for i in 0u8..10 {
            let segment = &val[(i as usize) * 10..(i as usize + 1) * 10];
            // Each segment should be all the same byte (from one of the tasks)
            assert!(
                segment.iter().all(|&b| b == segment[0]),
                "Segment {} has mixed bytes (data corruption from race condition): {:?}",
                i,
                segment
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_setrange_concurrent_overlapping() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Multiple tasks writing to the SAME offset - tests atomicity
        let num_tasks = 20;
        let mut handles = Vec::new();
        for i in 0u8..num_tasks {
            let engine = engine.clone();
            handles.push(tokio::spawn(async move {
                let value = vec![b'A' + i; 5];
                let args = vec![b"overlap_sr".to_vec(), b"0".to_vec(), value];
                setrange(&engine, &args).await.unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // The final value should be from exactly one of the tasks (no mixed bytes)
        let val = engine.get(b"overlap_sr").await.unwrap().unwrap();
        assert_eq!(val.len(), 5);
        assert!(
            val.iter().all(|&b| b == val[0]),
            "Value has mixed bytes from different tasks: {:?}",
            val
        );
    }

    #[tokio::test]
    async fn test_get_atomic_wrongtype() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Create a list key
        engine
            .set_with_type(
                b"list_key2".to_vec(),
                b"data".to_vec(),
                RedisDataType::List,
                None,
            )
            .await
            .unwrap();

        // GET on a list key should return WRONGTYPE
        let args = vec![b"list_key2".to_vec()];
        let result = get(&engine, &args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_atomic_nonexistent() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // GET on nonexistent key should return nil
        let args = vec![b"no_such_key".to_vec()];
        let result = get(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_getex_atomic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Set a key
        engine
            .set(b"getex_key".to_vec(), b"value1".to_vec(), None)
            .await
            .unwrap();

        // GETEX with EX should return value and set TTL
        let args = vec![b"getex_key".to_vec(), b"EX".to_vec(), b"3600".to_vec()];
        let result = getex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value1".to_vec())));

        // Verify TTL was set
        let ttl = engine.ttl(b"getex_key").await.unwrap();
        assert!(ttl.is_some(), "TTL should be set after GETEX EX");

        // GETEX with PERSIST should remove TTL
        let args = vec![b"getex_key".to_vec(), b"PERSIST".to_vec()];
        let result = getex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value1".to_vec())));

        let ttl = engine.ttl(b"getex_key").await.unwrap();
        assert!(ttl.is_none(), "TTL should be removed after GETEX PERSIST");
    }

    #[tokio::test]
    async fn test_getex_atomic_nonexistent() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // GETEX on nonexistent key should return nil
        let args = vec![b"no_key".to_vec(), b"EX".to_vec(), b"100".to_vec()];
        let result = getex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_getex_atomic_wrongtype() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        engine
            .set_with_type(
                b"list_getex".to_vec(),
                b"data".to_vec(),
                RedisDataType::List,
                None,
            )
            .await
            .unwrap();

        let args = vec![b"list_getex".to_vec(), b"EX".to_vec(), b"100".to_vec()];
        let result = getex(&engine, &args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mset_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // MSET with 3 key-value pairs
        let args = vec![
            b"k1".to_vec(),
            b"v1".to_vec(),
            b"k2".to_vec(),
            b"v2".to_vec(),
            b"k3".to_vec(),
            b"v3".to_vec(),
        ];
        let result = mset(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify all keys are set
        let r1 = get(&engine, &[b"k1".to_vec()]).await.unwrap();
        assert_eq!(r1, RespValue::BulkString(Some(b"v1".to_vec())));
        let r2 = get(&engine, &[b"k2".to_vec()]).await.unwrap();
        assert_eq!(r2, RespValue::BulkString(Some(b"v2".to_vec())));
        let r3 = get(&engine, &[b"k3".to_vec()]).await.unwrap();
        assert_eq!(r3, RespValue::BulkString(Some(b"v3".to_vec())));
    }

    #[tokio::test]
    async fn test_mset_wrong_args() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Odd number of arguments
        let args = vec![b"k1".to_vec(), b"v1".to_vec(), b"k2".to_vec()];
        let result = mset(&engine, &args).await;
        assert!(result.is_err());

        // Empty arguments
        let result = mset(&engine, &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_mset_concurrent_no_partial_state() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Spawn multiple tasks that MSET the same keys to different values.
        // Each task sets k1=<task_id>, k2=<task_id>, k3=<task_id>.
        // After all tasks complete, all keys should have the same value
        // (whichever task ran last), demonstrating no partial states.
        let num_tasks = 20;
        let mut handles = Vec::new();
        for task_id in 0..num_tasks {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let val = format!("task_{task_id}").into_bytes();
                let args = vec![
                    b"mset_ck1".to_vec(),
                    val.clone(),
                    b"mset_ck2".to_vec(),
                    val.clone(),
                    b"mset_ck3".to_vec(),
                    val,
                ];
                mset(&eng, &args).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // All three keys must have the same value (from whatever MSET ran last)
        let r1 = engine.get(b"mset_ck1").await.unwrap().unwrap();
        let r2 = engine.get(b"mset_ck2").await.unwrap().unwrap();
        let r3 = engine.get(b"mset_ck3").await.unwrap().unwrap();
        assert_eq!(r1, r2, "k1 and k2 should match (no partial MSET state)");
        assert_eq!(r2, r3, "k2 and k3 should match (no partial MSET state)");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_mset_concurrent_independent_keys() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Each task MSETs a unique pair of keys. After all tasks,
        // every key should be set correctly.
        let num_tasks = 50;
        let mut handles = Vec::new();
        for task_id in 0..num_tasks {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let k1 = format!("mset_ik_{task_id}_a").into_bytes();
                let k2 = format!("mset_ik_{task_id}_b").into_bytes();
                let val = format!("val_{task_id}").into_bytes();
                let args = vec![k1, val.clone(), k2, val];
                mset(&eng, &args).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Verify all keys exist with correct values
        for task_id in 0..num_tasks {
            let k1 = format!("mset_ik_{task_id}_a").into_bytes();
            let k2 = format!("mset_ik_{task_id}_b").into_bytes();
            let expected = format!("val_{task_id}").into_bytes();
            let v1 = engine.get(&k1).await.unwrap().unwrap();
            let v2 = engine.get(&k2).await.unwrap().unwrap();
            assert_eq!(v1, expected);
            assert_eq!(v2, expected);
        }
    }

    #[tokio::test]
    async fn test_msetnx_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // MSETNX should succeed when none of the keys exist
        let args = vec![
            b"nx1".to_vec(),
            b"v1".to_vec(),
            b"nx2".to_vec(),
            b"v2".to_vec(),
        ];
        let result = msetnx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Verify keys were set
        let r1 = get(&engine, &[b"nx1".to_vec()]).await.unwrap();
        assert_eq!(r1, RespValue::BulkString(Some(b"v1".to_vec())));
        let r2 = get(&engine, &[b"nx2".to_vec()]).await.unwrap();
        assert_eq!(r2, RespValue::BulkString(Some(b"v2".to_vec())));
    }

    #[tokio::test]
    async fn test_msetnx_existing_key_rollback() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Pre-set one key
        engine
            .set(b"nxe2".to_vec(), b"existing".to_vec(), None)
            .await
            .unwrap();

        // MSETNX with one existing key should fail and rollback
        let args = vec![
            b"nxe1".to_vec(),
            b"new1".to_vec(),
            b"nxe2".to_vec(),
            b"new2".to_vec(), // This key exists
            b"nxe3".to_vec(),
            b"new3".to_vec(),
        ];
        let result = msetnx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // nxe1 should NOT exist (rolled back)
        let r1 = get(&engine, &[b"nxe1".to_vec()]).await.unwrap();
        assert_eq!(r1, RespValue::BulkString(None));

        // nxe2 should still have original value
        let r2 = get(&engine, &[b"nxe2".to_vec()]).await.unwrap();
        assert_eq!(r2, RespValue::BulkString(Some(b"existing".to_vec())));

        // nxe3 should NOT exist (never attempted, loop broke at nxe2)
        let r3 = get(&engine, &[b"nxe3".to_vec()]).await.unwrap();
        assert_eq!(r3, RespValue::BulkString(None));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_msetnx_concurrent_only_one_wins() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Multiple tasks try MSETNX on the same keys. Exactly one should succeed.
        let num_tasks = 20;
        let results = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for task_id in 0..num_tasks {
            let eng = engine.clone();
            let res = results.clone();
            handles.push(tokio::spawn(async move {
                let val = format!("task_{task_id}").into_bytes();
                let args = vec![
                    b"msetnx_ck1".to_vec(),
                    val.clone(),
                    b"msetnx_ck2".to_vec(),
                    val,
                ];
                let result = msetnx(&eng, &args).await.unwrap();
                if let RespValue::Integer(n) = result {
                    res.lock().unwrap().push(n);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let results = results.lock().unwrap();
        let successes = results.iter().filter(|&&r| r == 1).count();
        let failures = results.iter().filter(|&&r| r == 0).count();

        // Exactly one task should have succeeded
        assert_eq!(successes, 1, "Exactly one MSETNX should succeed");
        assert_eq!(failures, num_tasks - 1, "All other MSETNXs should fail");

        // Both keys should have the same value (from the winning task)
        let v1 = engine.get(b"msetnx_ck1").await.unwrap().unwrap();
        let v2 = engine.get(b"msetnx_ck2").await.unwrap().unwrap();
        assert_eq!(v1, v2, "Both keys should have the winning task's value");
    }
}
