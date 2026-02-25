use std::time::Duration;

use crate::command::{CommandError, CommandResult};
use crate::command::utils::bytes_to_string;
use crate::networking::resp::RespValue;
use crate::storage::engine::{StorageEngine, NUM_DATABASES};
use crate::storage::item::RedisDataType;

/// Redis COPY command - Copy the value stored at the source key to the destination key
pub async fn copy(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let src = &args[0];
    let dst = args[1].clone();

    let mut replace = false;
    let mut _destination_db: Option<usize> = None;

    let mut i = 2;
    while i < args.len() {
        let option = bytes_to_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "REPLACE" => {
                replace = true;
                i += 1;
            }
            "DB" | "DESTINATION" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let db_str = bytes_to_string(&args[i + 1])?;
                let db = db_str.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if db >= NUM_DATABASES {
                    return Err(CommandError::InvalidArgument(
                        "invalid DB index".to_string(),
                    ));
                }
                if db != 0 {
                    return Err(CommandError::InvalidArgument(
                        "ERR cross-database COPY is not yet supported".to_string(),
                    ));
                }
                _destination_db = Some(db);
                i += 2;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported COPY option: {}",
                    option
                )));
            }
        }
    }

    // For now, copy within the same database (db 0)
    match engine.copy_key(src, dst, replace).await? {
        true => Ok(RespValue::Integer(1)),
        false => Ok(RespValue::Integer(0)),
    }
}

/// Redis MOVE command - Move a key to another database
pub async fn move_cmd(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let db_str = bytes_to_string(&args[1])?;
    let db = db_str.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    if db >= NUM_DATABASES {
        return Err(CommandError::InvalidArgument(
            "invalid DB index".to_string(),
        ));
    }

    if db == 0 {
        // Cannot move to the same database
        return Ok(RespValue::Integer(0));
    }

    match engine.move_key(key, db).await? {
        true => Ok(RespValue::Integer(1)),
        false => Ok(RespValue::Integer(0)),
    }
}

/// Redis UNLINK command - Delete keys asynchronously (functionally same as DEL)
pub async fn unlink(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let mut deleted = 0;
    for key in args {
        if engine.del(key).await? {
            deleted += 1;
        }
    }

    Ok(RespValue::Integer(deleted))
}

/// Redis TOUCH command - Alters the last access time of a key(s)
pub async fn touch(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let mut touched = 0i64;
    for key in args {
        if engine.touch(key).await? {
            touched += 1;
        }
    }

    Ok(RespValue::Integer(touched))
}

/// Redis EXPIREAT command - Set the expiration for a key as a UNIX timestamp (seconds)
pub async fn expireat(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let timestamp_str = bytes_to_string(&args[1])?;
    let timestamp = timestamp_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    // Parse optional condition flags (NX, XX, GT, LT)
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

    // Apply condition flags
    if nx || xx || gt || lt {
        let current_ttl = engine.ttl(key).await?;
        let has_expiry = current_ttl.is_some();

        // NX: Set expiry only when the key has no expiry
        if nx && has_expiry {
            return Ok(RespValue::Integer(0));
        }
        // XX: Set expiry only when the key has an existing expiry
        if xx && !has_expiry {
            return Ok(RespValue::Integer(0));
        }
        // GT: Set expiry only when the new expiry is greater than current
        if gt {
            if let Some(current) = current_ttl {
                let now_unix = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                let new_remaining = timestamp - now_unix;
                if new_remaining <= current.as_secs() as i64 {
                    return Ok(RespValue::Integer(0));
                }
            }
        }
        // LT: Set expiry only when the new expiry is less than current
        if lt {
            if let Some(current) = current_ttl {
                let now_unix = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                let new_remaining = timestamp - now_unix;
                if new_remaining >= current.as_secs() as i64 {
                    return Ok(RespValue::Integer(0));
                }
            }
        }
    }

    match engine.expire_at(key, timestamp).await? {
        true => Ok(RespValue::Integer(1)),
        false => Ok(RespValue::Integer(0)),
    }
}

/// Redis PEXPIREAT command - Set the expiration for a key as a UNIX timestamp in milliseconds
pub async fn pexpireat(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let timestamp_str = bytes_to_string(&args[1])?;
    let timestamp_ms = timestamp_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

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

    // Apply condition flags
    if nx || xx || gt || lt {
        let current_ttl = engine.ttl(key).await?;
        let has_expiry = current_ttl.is_some();

        if nx && has_expiry {
            return Ok(RespValue::Integer(0));
        }
        if xx && !has_expiry {
            return Ok(RespValue::Integer(0));
        }
        if gt {
            if let Some(current) = current_ttl {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let new_remaining_ms = timestamp_ms - now_ms;
                if new_remaining_ms <= current.as_millis() as i64 {
                    return Ok(RespValue::Integer(0));
                }
            }
        }
        if lt {
            if let Some(current) = current_ttl {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let new_remaining_ms = timestamp_ms - now_ms;
                if new_remaining_ms >= current.as_millis() as i64 {
                    return Ok(RespValue::Integer(0));
                }
            }
        }
    }

    match engine.pexpire_at(key, timestamp_ms).await? {
        true => Ok(RespValue::Integer(1)),
        false => Ok(RespValue::Integer(0)),
    }
}

/// Redis EXPIRETIME command - Get the expiration Unix timestamp for a key (seconds)
pub async fn expiretime(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Check if key exists
    if !engine.exists(key).await? {
        return Ok(RespValue::Integer(-2)); // Key does not exist
    }

    match engine.expiretime(key).await? {
        Some(ts) => Ok(RespValue::Integer(ts)),
        None => Ok(RespValue::Integer(-1)), // Key exists but has no expiration
    }
}

/// Redis PEXPIRETIME command - Get the expiration Unix timestamp for a key (milliseconds)
pub async fn pexpiretime(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Check if key exists
    if !engine.exists(key).await? {
        return Ok(RespValue::Integer(-2)); // Key does not exist
    }

    match engine.pexpiretime(key).await? {
        Some(ts) => Ok(RespValue::Integer(ts)),
        None => Ok(RespValue::Integer(-1)), // Key exists but has no expiration
    }
}

/// Redis OBJECT command - Inspect the internals of Redis objects
pub async fn object(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcommand = bytes_to_string(&args[0])?.to_uppercase();

    match subcommand.as_str() {
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Some(b"OBJECT subcommands:".to_vec())),
                RespValue::BulkString(Some(
                    b"ENCODING <key> - Return the encoding of the object stored at <key>."
                        .to_vec(),
                )),
                RespValue::BulkString(Some(
                    b"FREQ <key> - Return the access frequency index of the key."
                        .to_vec(),
                )),
                RespValue::BulkString(Some(
                    b"HELP - Return subcommand help summary.".to_vec(),
                )),
                RespValue::BulkString(Some(
                    b"IDLETIME <key> - Return the idle time of the key in seconds."
                        .to_vec(),
                )),
                RespValue::BulkString(Some(
                    b"REFCOUNT <key> - Return the reference count of the object stored at <key>."
                        .to_vec(),
                )),
            ];
            Ok(RespValue::Array(Some(help)))
        }
        "ENCODING" => {
            if args.len() != 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let key = &args[1];
            match engine.get_encoding(key).await? {
                Some(encoding) => Ok(RespValue::BulkString(Some(encoding.into_bytes()))),
                None => Err(CommandError::InvalidArgument(
                    "no such key".to_string(),
                )),
            }
        }
        "IDLETIME" => {
            if args.len() != 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let key = &args[1];
            match engine.get_idle_time(key).await? {
                Some(idle) => Ok(RespValue::Integer(idle as i64)),
                None => Err(CommandError::InvalidArgument(
                    "no such key".to_string(),
                )),
            }
        }
        "REFCOUNT" => {
            if args.len() != 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let key = &args[1];
            if engine.exists(key).await? {
                // rLightning always reports refcount of 1
                Ok(RespValue::Integer(1))
            } else {
                Err(CommandError::InvalidArgument(
                    "no such key".to_string(),
                ))
            }
        }
        "FREQ" => {
            if args.len() != 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let key = &args[1];
            if engine.exists(key).await? {
                // Frequency tracking not implemented; return 0
                Ok(RespValue::Integer(0))
            } else {
                Err(CommandError::InvalidArgument(
                    "no such key".to_string(),
                ))
            }
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown OBJECT subcommand '{}'",
            subcommand
        ))),
    }
}

/// Redis DUMP command - Return a serialized version of the value stored at the specified key
pub async fn dump(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    match engine.get_item(key).await? {
        Some(item) => {
            // Serialize using a simple format: [type_byte][value_bytes]
            let type_byte = match item.data_type {
                RedisDataType::String => 0u8,
                RedisDataType::List => 1u8,
                RedisDataType::Set => 2u8,
                RedisDataType::Hash => 3u8,
                RedisDataType::ZSet => 4u8,
                RedisDataType::Stream => 5u8,
            };
            let mut serialized = Vec::with_capacity(1 + item.value.len());
            serialized.push(type_byte);
            serialized.extend_from_slice(&item.value);
            Ok(RespValue::BulkString(Some(serialized)))
        }
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Redis RESTORE command - Create a key using the provided serialized value
pub async fn restore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = args[0].clone();
    let ttl_str = bytes_to_string(&args[1])?;
    let ttl_ms = ttl_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    let serialized = &args[2];
    if serialized.is_empty() {
        return Err(CommandError::InvalidArgument(
            "DUMP payload version or checksum are wrong".to_string(),
        ));
    }

    // Parse options
    let mut replace = false;
    for arg in args.iter().skip(3) {
        match bytes_to_string(arg)?.to_uppercase().as_str() {
            "REPLACE" => replace = true,
            "ABSTTL" => {} // Accept but ignore
            "IDLETIME" => {} // Accept but ignore
            "FREQ" => {}     // Accept but ignore
            _ => {
                return Err(CommandError::InvalidArgument(
                    "Unsupported RESTORE option".to_string(),
                ));
            }
        }
    }

    // Deserialize: first byte is type, rest is value
    let type_byte = serialized[0];
    let value = serialized[1..].to_vec();
    let data_type = match type_byte {
        0 => RedisDataType::String,
        1 => RedisDataType::List,
        2 => RedisDataType::Set,
        3 => RedisDataType::Hash,
        4 => RedisDataType::ZSet,
        5 => RedisDataType::Stream,
        _ => {
            return Err(CommandError::InvalidArgument(
                "DUMP payload version or checksum are wrong".to_string(),
            ));
        }
    };

    let ttl = if ttl_ms > 0 {
        Some(Duration::from_millis(ttl_ms as u64))
    } else {
        None
    };

    match engine.restore_key(key, value, data_type, ttl, replace).await {
        Ok(true) => Ok(RespValue::SimpleString("OK".to_string())),
        Ok(false) => Err(CommandError::InvalidArgument(
            "Target key name already exists".to_string(),
        )),
        Err(e) => Err(e.into()),
    }
}

/// Redis SORT command - Sort the elements in a list, set or sorted set
pub async fn sort(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Parse options
    let mut alpha = false;
    let mut desc = false;
    let mut limit_offset: Option<usize> = None;
    let mut limit_count: Option<usize> = None;
    let mut store_key: Option<Vec<u8>> = None;

    let mut i = 1;
    while i < args.len() {
        let option = bytes_to_string(&args[i])?.to_uppercase();
        match option.as_str() {
            "ASC" => {
                desc = false;
                i += 1;
            }
            "DESC" => {
                desc = true;
                i += 1;
            }
            "ALPHA" => {
                alpha = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                limit_offset = Some(
                    bytes_to_string(&args[i + 1])?
                        .parse::<usize>()
                        .map_err(|_| {
                            CommandError::InvalidArgument(
                                "value is not an integer or out of range".to_string(),
                            )
                        })?,
                );
                limit_count = Some(
                    bytes_to_string(&args[i + 2])?
                        .parse::<usize>()
                        .map_err(|_| {
                            CommandError::InvalidArgument(
                                "value is not an integer or out of range".to_string(),
                            )
                        })?,
                );
                i += 3;
            }
            "STORE" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                store_key = Some(args[i + 1].clone());
                i += 2;
            }
            "BY" => {
                // BY pattern - skip for now (accept but don't use)
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                i += 2;
            }
            "GET" => {
                // GET pattern - skip for now (accept but don't use)
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                i += 2;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported SORT option: {}",
                    option
                )));
            }
        }
    }

    // Get the value and determine type
    if !engine.exists(key).await? {
        if let Some(store) = store_key {
            // STORE with empty source creates empty list
            engine.del(&store).await?;
            return Ok(RespValue::Integer(0));
        }
        return Ok(RespValue::Array(Some(vec![])));
    }

    let key_type = engine.get_type(key).await?;
    let mut elements: Vec<Vec<u8>> = match key_type.as_str() {
        "list" => {
            if let Some(value) = engine.get(key).await? {
                bincode::deserialize::<Vec<Vec<u8>>>(&value).unwrap_or_default()
            } else {
                vec![]
            }
        }
        "set" => {
            if let Some(value) = engine.get(key).await? {
                let set: std::collections::HashSet<Vec<u8>> =
                    bincode::deserialize(&value).unwrap_or_default();
                set.into_iter().collect()
            } else {
                vec![]
            }
        }
        "zset" => {
            if let Some(value) = engine.get(key).await? {
                let zset: Vec<(f64, Vec<u8>)> =
                    bincode::deserialize(&value).unwrap_or_default();
                zset.into_iter().map(|(_, member)| member).collect()
            } else {
                vec![]
            }
        }
        _ => {
            return Err(CommandError::WrongType);
        }
    };

    // Sort the elements
    if alpha {
        if desc {
            elements.sort_by(|a, b| b.cmp(a));
        } else {
            elements.sort();
        }
    } else {
        // Numeric sort
        let mut numeric_pairs: Vec<(f64, Vec<u8>)> = Vec::with_capacity(elements.len());
        for elem in &elements {
            let s = String::from_utf8_lossy(elem);
            let num = s.parse::<f64>().map_err(|_| {
                CommandError::InvalidArgument(
                    "One or more scores can't be converted into double".to_string(),
                )
            })?;
            numeric_pairs.push((num, elem.clone()));
        }
        if desc {
            numeric_pairs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            numeric_pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        }
        elements = numeric_pairs.into_iter().map(|(_, v)| v).collect();
    }

    // Apply LIMIT
    if let (Some(offset), Some(count)) = (limit_offset, limit_count) {
        let start = offset.min(elements.len());
        let end = (start + count).min(elements.len());
        elements = elements[start..end].to_vec();
    }

    // Handle STORE option
    if let Some(store) = store_key {
        let count = elements.len() as i64;
        let serialized = bincode::serialize(&elements).map_err(|e| {
            CommandError::InternalError(format!("Serialization error: {}", e))
        })?;
        engine
            .set_with_type(store, serialized, RedisDataType::List, None)
            .await?;
        return Ok(RespValue::Integer(count));
    }

    let result: Vec<RespValue> = elements
        .into_iter()
        .map(|e| RespValue::BulkString(Some(e)))
        .collect();
    Ok(RespValue::Array(Some(result)))
}

/// Redis SORT_RO command - Read-only variant of SORT (same behavior, no STORE option allowed)
pub async fn sort_ro(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    // Check if STORE option is present
    for (i, arg) in args.iter().enumerate() {
        if bytes_to_string(arg)?.to_uppercase() == "STORE" {
            let _ = i;
            return Err(CommandError::InvalidArgument(
                "SORT_RO does not support the STORE option".to_string(),
            ));
        }
    }

    sort(engine, args).await
}

/// Redis WAIT command - Wait for replicas to acknowledge writes
pub async fn wait_cmd(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let _numreplicas = bytes_to_string(&args[0])?
        .parse::<i64>()
        .map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
    let _timeout = bytes_to_string(&args[1])?
        .parse::<i64>()
        .map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;

    // Since we're a standalone server with no replicas, return 0 immediately
    Ok(RespValue::Integer(0))
}

/// Redis WAITAOF command - Wait for AOF acknowledgements
pub async fn waitaof(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let _numlocal = bytes_to_string(&args[0])?
        .parse::<i64>()
        .map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
    let _numreplicas = bytes_to_string(&args[1])?
        .parse::<i64>()
        .map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
    let _timeout = bytes_to_string(&args[2])?
        .parse::<i64>()
        .map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;

    // Return [local, replicas] - standalone server, return [0, 0]
    Ok(RespValue::Array(Some(vec![
        RespValue::Integer(0),
        RespValue::Integer(0),
    ])))
}

/// Redis SELECT command - Select the database for the current connection
pub async fn select(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let db_str = bytes_to_string(&args[0])?;
    let db = db_str.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    if db >= NUM_DATABASES {
        return Err(CommandError::InvalidArgument(format!(
            "DB index is out of range"
        )));
    }

    // The actual database switching is handled at the server level
    // by tracking the db_index per connection.
    // Here we just validate the index and return OK.
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Enhanced Redis SCAN command with TYPE filter support
pub async fn scan_with_type(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Parse cursor
    let cursor_str = bytes_to_string(&args[0])?;
    let cursor = cursor_str.parse::<u64>().map_err(|_| {
        CommandError::InvalidArgument("Cursor value must be an integer".to_string())
    })?;

    // Parse options
    let mut pattern = String::from("*");
    let mut count = 10usize;
    let mut type_filter: Option<String> = None;

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
            "TYPE" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                type_filter = Some(bytes_to_string(&args[i + 1])?.to_lowercase());
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

    // Apply TYPE filter if present
    let filtered_keys = if let Some(ref type_name) = type_filter {
        let mut filtered = Vec::new();
        for key in &all_keys {
            let kt = engine.get_type(key).await?;
            if kt == *type_name {
                filtered.push(key.clone());
            }
        }
        filtered
    } else {
        all_keys
    };

    let total_keys = filtered_keys.len() as u64;

    let start_idx = if cursor == 0 || cursor >= total_keys {
        0
    } else {
        cursor as usize
    };

    let end_idx = (start_idx + count).min(filtered_keys.len());

    let batch: Vec<RespValue> = filtered_keys[start_idx..end_idx]
        .iter()
        .map(|k| RespValue::BulkString(Some(k.clone())))
        .collect();

    let new_cursor = if end_idx >= filtered_keys.len() {
        0
    } else {
        end_idx as u64
    };

    let response = vec![
        RespValue::BulkString(Some(new_cursor.to_string().into_bytes())),
        RespValue::Array(Some(batch)),
    ];

    Ok(RespValue::Array(Some(response)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn test_copy_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Set source key
        engine
            .set(b"src".to_vec(), b"value1".to_vec(), None)
            .await
            .unwrap();

        // Copy to new key
        let result = copy(&engine, &[b"src".to_vec(), b"dst".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Verify destination has the value
        let val = engine.get(b"dst").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));

        // Source should still exist
        let src_val = engine.get(b"src").await.unwrap();
        assert_eq!(src_val, Some(b"value1".to_vec()));

        // Copy to existing key without REPLACE should fail
        engine
            .set(b"existing".to_vec(), b"old".to_vec(), None)
            .await
            .unwrap();
        let result = copy(&engine, &[b"src".to_vec(), b"existing".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Copy with REPLACE should succeed
        let result = copy(
            &engine,
            &[b"src".to_vec(), b"existing".to_vec(), b"REPLACE".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let val = engine.get(b"existing").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));

        // Copy non-existent key should return 0
        let result = copy(&engine, &[b"nonexistent".to_vec(), b"dst2".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_copy_with_ttl() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Set source with TTL
        engine
            .set(
                b"src_ttl".to_vec(),
                b"val".to_vec(),
                Some(Duration::from_secs(100)),
            )
            .await
            .unwrap();

        // Copy it
        let result = copy(&engine, &[b"src_ttl".to_vec(), b"dst_ttl".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Destination should also have a TTL
        let ttl = engine.ttl(b"dst_ttl").await.unwrap();
        assert!(ttl.is_some());
        assert!(ttl.unwrap().as_secs() > 0);
    }

    #[tokio::test]
    async fn test_move_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Set a key
        engine
            .set(b"moveme".to_vec(), b"value".to_vec(), None)
            .await
            .unwrap();

        // Move to db 1
        let result = move_cmd(&engine, &[b"moveme".to_vec(), b"1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Key should no longer exist in db 0
        assert!(!engine.exists(b"moveme").await.unwrap());

        // Key should exist in db 1
        let db1 = engine.get_db(1).unwrap();
        assert!(db1.contains_key(&b"moveme".to_vec()));

        // Move non-existent key
        let result = move_cmd(&engine, &[b"nonexistent".to_vec(), b"1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Move to same database (0)
        engine
            .set(b"stayhere".to_vec(), b"val".to_vec(), None)
            .await
            .unwrap();
        let result = move_cmd(&engine, &[b"stayhere".to_vec(), b"0".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Move to invalid db
        let result = move_cmd(&engine, &[b"stayhere".to_vec(), b"99".to_vec()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unlink_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        engine
            .set(b"k1".to_vec(), b"v1".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"k2".to_vec(), b"v2".to_vec(), None)
            .await
            .unwrap();

        let result = unlink(&engine, &[b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(2));

        assert!(!engine.exists(b"k1").await.unwrap());
        assert!(!engine.exists(b"k2").await.unwrap());
    }

    #[tokio::test]
    async fn test_touch_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        engine
            .set(b"t1".to_vec(), b"v1".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"t2".to_vec(), b"v2".to_vec(), None)
            .await
            .unwrap();

        let result = touch(
            &engine,
            &[b"t1".to_vec(), b"t2".to_vec(), b"nonexistent".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_expireat_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        engine
            .set(b"ea_key".to_vec(), b"value".to_vec(), None)
            .await
            .unwrap();

        // Set expiration 100 seconds in the future
        let future_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 100;

        let result = expireat(
            &engine,
            &[b"ea_key".to_vec(), future_ts.to_string().into_bytes()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Verify TTL is set
        let ttl = engine.ttl(b"ea_key").await.unwrap();
        assert!(ttl.is_some());
        let ttl_secs = ttl.unwrap().as_secs();
        assert!(ttl_secs >= 98 && ttl_secs <= 101);

        // Set expiration in the past should delete the key
        let past_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 100;

        engine
            .set(b"ea_key2".to_vec(), b"value".to_vec(), None)
            .await
            .unwrap();
        let result = expireat(
            &engine,
            &[b"ea_key2".to_vec(), past_ts.to_string().into_bytes()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Key was deleted
        assert!(!engine.exists(b"ea_key2").await.unwrap());

        // Non-existent key
        let result = expireat(
            &engine,
            &[
                b"nonexistent".to_vec(),
                future_ts.to_string().into_bytes(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_pexpireat_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        engine
            .set(b"pea_key".to_vec(), b"value".to_vec(), None)
            .await
            .unwrap();

        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 100_000; // 100 seconds

        let result = pexpireat(
            &engine,
            &[b"pea_key".to_vec(), future_ms.to_string().into_bytes()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let ttl = engine.ttl(b"pea_key").await.unwrap();
        assert!(ttl.is_some());
    }

    #[tokio::test]
    async fn test_expiretime_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Key without expiration
        engine
            .set(b"et_key".to_vec(), b"value".to_vec(), None)
            .await
            .unwrap();
        let result = expiretime(&engine, &[b"et_key".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(-1));

        // Key with expiration
        engine
            .expire(b"et_key", Some(Duration::from_secs(100)))
            .await
            .unwrap();
        let result = expiretime(&engine, &[b"et_key".to_vec()])
            .await
            .unwrap();
        if let RespValue::Integer(ts) = result {
            let now_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(ts >= now_ts + 98 && ts <= now_ts + 102);
        } else {
            panic!("Expected integer response");
        }

        // Non-existent key
        let result = expiretime(&engine, &[b"nonexistent".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[tokio::test]
    async fn test_pexpiretime_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        engine
            .set(b"pet_key".to_vec(), b"value".to_vec(), None)
            .await
            .unwrap();

        // Without expiration
        let result = pexpiretime(&engine, &[b"pet_key".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(-1));

        // With expiration
        engine
            .expire(b"pet_key", Some(Duration::from_secs(100)))
            .await
            .unwrap();
        let result = pexpiretime(&engine, &[b"pet_key".to_vec()])
            .await
            .unwrap();
        if let RespValue::Integer(ts_ms) = result {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            assert!(ts_ms >= now_ms + 98_000 && ts_ms <= now_ms + 102_000);
        } else {
            panic!("Expected integer response");
        }

        // Non-existent key
        let result = pexpiretime(&engine, &[b"nonexistent".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[tokio::test]
    async fn test_object_encoding() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Integer encoding
        engine
            .set(b"num".to_vec(), b"12345".to_vec(), None)
            .await
            .unwrap();
        let result = object(&engine, &[b"ENCODING".to_vec(), b"num".to_vec()])
            .await
            .unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Some(b"int".to_vec()))
        );

        // Short string encoding
        engine
            .set(b"short".to_vec(), b"hello".to_vec(), None)
            .await
            .unwrap();
        let result = object(&engine, &[b"ENCODING".to_vec(), b"short".to_vec()])
            .await
            .unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Some(b"embstr".to_vec()))
        );

        // Long string encoding
        let long_str = "a".repeat(100);
        engine
            .set(b"long".to_vec(), long_str.into_bytes(), None)
            .await
            .unwrap();
        let result = object(&engine, &[b"ENCODING".to_vec(), b"long".to_vec()])
            .await
            .unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Some(b"raw".to_vec()))
        );

        // Non-existent key
        let result = object(&engine, &[b"ENCODING".to_vec(), b"nope".to_vec()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_object_refcount() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        engine
            .set(b"obj".to_vec(), b"val".to_vec(), None)
            .await
            .unwrap();
        let result = object(&engine, &[b"REFCOUNT".to_vec(), b"obj".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_object_idletime() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        engine
            .set(b"idle".to_vec(), b"val".to_vec(), None)
            .await
            .unwrap();

        let result = object(&engine, &[b"IDLETIME".to_vec(), b"idle".to_vec()])
            .await
            .unwrap();
        if let RespValue::Integer(idle) = result {
            assert!(idle >= 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[tokio::test]
    async fn test_object_help() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        let result = object(&engine, &[b"HELP".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
        } else {
            panic!("Expected array response");
        }
    }

    #[tokio::test]
    async fn test_dump_restore() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Set a key
        engine
            .set(b"dump_key".to_vec(), b"hello".to_vec(), None)
            .await
            .unwrap();

        // Dump it
        let dump_result = dump(&engine, &[b"dump_key".to_vec()]).await.unwrap();
        let serialized = if let RespValue::BulkString(Some(data)) = dump_result {
            data
        } else {
            panic!("Expected bulk string from DUMP");
        };

        // Delete the original
        engine.del(b"dump_key").await.unwrap();

        // Restore it under a new name
        let result = restore(
            &engine,
            &[b"restored_key".to_vec(), b"0".to_vec(), serialized.clone()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify the restored value
        let val = engine.get(b"restored_key").await.unwrap();
        assert_eq!(val, Some(b"hello".to_vec()));

        // Restore with TTL
        let result = restore(
            &engine,
            &[b"restored_ttl".to_vec(), b"5000".to_vec(), serialized.clone()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        let ttl = engine.ttl(b"restored_ttl").await.unwrap();
        assert!(ttl.is_some());

        // Restore to existing key without REPLACE should fail
        let result = restore(
            &engine,
            &[b"restored_key".to_vec(), b"0".to_vec(), serialized.clone()],
        )
        .await;
        assert!(result.is_err());

        // Restore with REPLACE should succeed
        let result = restore(
            &engine,
            &[
                b"restored_key".to_vec(),
                b"0".to_vec(),
                serialized,
                b"REPLACE".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Dump non-existent key returns nil
        let result = dump(&engine, &[b"nope".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_sort_numeric() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create a list
        let list = vec![
            b"3".to_vec(),
            b"1".to_vec(),
            b"2".to_vec(),
            b"5".to_vec(),
            b"4".to_vec(),
        ];
        let serialized = bincode::serialize(&list).unwrap();
        engine
            .set_with_type(b"mylist".to_vec(), serialized, RedisDataType::List, None)
            .await
            .unwrap();

        // Sort ascending
        let result = sort(&engine, &[b"mylist".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            let values: Vec<Vec<u8>> = items
                .into_iter()
                .map(|v| {
                    if let RespValue::BulkString(Some(data)) = v {
                        data
                    } else {
                        panic!("Expected bulk string");
                    }
                })
                .collect();
            assert_eq!(values, vec![b"1", b"2", b"3", b"4", b"5"]);
        } else {
            panic!("Expected array result");
        }

        // Sort descending
        let result = sort(&engine, &[b"mylist".to_vec(), b"DESC".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            let values: Vec<Vec<u8>> = items
                .into_iter()
                .map(|v| {
                    if let RespValue::BulkString(Some(data)) = v {
                        data
                    } else {
                        panic!("Expected bulk string");
                    }
                })
                .collect();
            assert_eq!(values, vec![b"5", b"4", b"3", b"2", b"1"]);
        }
    }

    #[tokio::test]
    async fn test_sort_alpha() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        let list = vec![
            b"banana".to_vec(),
            b"apple".to_vec(),
            b"cherry".to_vec(),
        ];
        let serialized = bincode::serialize(&list).unwrap();
        engine
            .set_with_type(b"fruits".to_vec(), serialized, RedisDataType::List, None)
            .await
            .unwrap();

        let result = sort(&engine, &[b"fruits".to_vec(), b"ALPHA".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            let values: Vec<Vec<u8>> = items
                .into_iter()
                .map(|v| {
                    if let RespValue::BulkString(Some(data)) = v {
                        data
                    } else {
                        panic!("Expected bulk string");
                    }
                })
                .collect();
            assert_eq!(values, vec![b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()]);
        }
    }

    #[tokio::test]
    async fn test_sort_with_limit() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        let list: Vec<Vec<u8>> = (1..=10).map(|i: i32| i.to_string().into_bytes()).collect();
        let serialized = bincode::serialize(&list).unwrap();
        engine
            .set_with_type(b"numbers".to_vec(), serialized, RedisDataType::List, None)
            .await
            .unwrap();

        let result = sort(
            &engine,
            &[
                b"numbers".to_vec(),
                b"LIMIT".to_vec(),
                b"2".to_vec(),
                b"3".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            let values: Vec<Vec<u8>> = items
                .into_iter()
                .map(|v| {
                    if let RespValue::BulkString(Some(data)) = v {
                        data
                    } else {
                        panic!("Expected bulk string");
                    }
                })
                .collect();
            assert_eq!(values, vec![b"3", b"4", b"5"]);
        }
    }

    #[tokio::test]
    async fn test_sort_with_store() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        let list = vec![b"3".to_vec(), b"1".to_vec(), b"2".to_vec()];
        let serialized = bincode::serialize(&list).unwrap();
        engine
            .set_with_type(b"src_list".to_vec(), serialized, RedisDataType::List, None)
            .await
            .unwrap();

        let result = sort(
            &engine,
            &[
                b"src_list".to_vec(),
                b"STORE".to_vec(),
                b"dst_list".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Verify stored result
        let stored = engine.get(b"dst_list").await.unwrap().unwrap();
        let stored_list: Vec<Vec<u8>> = bincode::deserialize(&stored).unwrap();
        assert_eq!(stored_list, vec![b"1", b"2", b"3"]);
    }

    #[tokio::test]
    async fn test_sort_ro() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        let list = vec![b"3".to_vec(), b"1".to_vec(), b"2".to_vec()];
        let serialized = bincode::serialize(&list).unwrap();
        engine
            .set_with_type(b"ro_list".to_vec(), serialized, RedisDataType::List, None)
            .await
            .unwrap();

        // SORT_RO without STORE should work
        let result = sort_ro(&engine, &[b"ro_list".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
        }

        // SORT_RO with STORE should fail
        let result = sort_ro(
            &engine,
            &[
                b"ro_list".to_vec(),
                b"STORE".to_vec(),
                b"dst".to_vec(),
            ],
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sort_nonexistent() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        let result = sort(&engine, &[b"nokey".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_wait_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        let result = wait_cmd(&engine, &[b"1".to_vec(), b"100".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Wrong args
        let result = wait_cmd(&engine, &[b"1".to_vec()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_waitaof_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        let result = waitaof(&engine, &[b"0".to_vec(), b"1".to_vec(), b"100".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::Integer(0));
            assert_eq!(items[1], RespValue::Integer(0));
        } else {
            panic!("Expected array result");
        }
    }

    #[tokio::test]
    async fn test_select_command() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Valid databases
        for db in 0..16 {
            let result = select(&engine, &[db.to_string().into_bytes()])
                .await
                .unwrap();
            assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        }

        // Invalid database
        let result = select(&engine, &[b"16".to_vec()]).await;
        assert!(result.is_err());

        // Non-integer
        let result = select(&engine, &[b"abc".to_vec()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_scan_with_type_filter() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Set some string keys
        engine
            .set(b"s1".to_vec(), b"val1".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"s2".to_vec(), b"val2".to_vec(), None)
            .await
            .unwrap();

        // Set a list key
        let list = vec![b"item1".to_vec()];
        let serialized = bincode::serialize(&list).unwrap();
        engine
            .set_with_type(b"mylist".to_vec(), serialized, RedisDataType::List, None)
            .await
            .unwrap();

        // Scan with TYPE filter for string
        let result = scan_with_type(
            &engine,
            &[
                b"0".to_vec(),
                b"TYPE".to_vec(),
                b"string".to_vec(),
                b"COUNT".to_vec(),
                b"100".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(scan_result)) = result {
            if let RespValue::Array(Some(keys)) = &scan_result[1] {
                // Should only contain string keys
                for key in keys {
                    if let RespValue::BulkString(Some(k)) = key {
                        let key_type = engine.get_type(k).await.unwrap();
                        assert_eq!(key_type, "string");
                    }
                }
            }
        }

        // Scan with TYPE filter for list
        let result = scan_with_type(
            &engine,
            &[
                b"0".to_vec(),
                b"TYPE".to_vec(),
                b"list".to_vec(),
                b"COUNT".to_vec(),
                b"100".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(scan_result)) = result {
            if let RespValue::Array(Some(keys)) = &scan_result[1] {
                assert_eq!(keys.len(), 1);
            }
        }
    }

    #[tokio::test]
    async fn test_scan_with_match_and_type() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        engine
            .set(b"user:1".to_vec(), b"a".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"user:2".to_vec(), b"b".to_vec(), None)
            .await
            .unwrap();
        engine
            .set(b"other:1".to_vec(), b"c".to_vec(), None)
            .await
            .unwrap();

        // Scan with both MATCH and TYPE
        let result = scan_with_type(
            &engine,
            &[
                b"0".to_vec(),
                b"MATCH".to_vec(),
                b"user:*".to_vec(),
                b"TYPE".to_vec(),
                b"string".to_vec(),
                b"COUNT".to_vec(),
                b"100".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(scan_result)) = result {
            if let RespValue::Array(Some(keys)) = &scan_result[1] {
                assert_eq!(keys.len(), 2);
            }
        }
    }
}
