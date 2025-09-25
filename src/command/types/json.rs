use crate::command::error::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use serde::de::IgnoredAny;
use serde_json::Value;
use tokio::task;
use tracing::{debug, warn};
// use crate::command::types::json_integration::{
//     enhanced_json_get, enhanced_json_set, enhanced_json_type,
//     enhanced_json_arrappend, enhanced_json_arrtrim
// };

/// Redis JSON.GET command - Get a JSON value at a specific path
/// Also handles the GET_JSON alias used by some clients
pub async fn json_get(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        String::from_utf8_lossy(&args[1]).to_string()
    } else {
        "$".to_string() // Default to root
    };
    let tokens = parse_json_path(&path);

    debug!(
        "JSON.GET command for key: {}, path: {}",
        String::from_utf8_lossy(key),
        path
    );

    match engine.get(key).await? {
        Some(data) => match String::from_utf8(data) {
            Ok(json_str) => match serde_json::from_str::<Value>(&json_str) {
                Ok(value) => {
                    let processed = crate::utils::datetime::process_json_for_serialization(value);
                    match select_json_path(&processed, &tokens) {
                        Some(selected) => {
                            let serialized = serde_json::to_vec(selected).map_err(|e| {
                                CommandError::InternalError(format!(
                                    "Failed to serialize JSON: {}",
                                    e
                                ))
                            })?;
                            Ok(RespValue::BulkString(Some(serialized)))
                        }
                        None => Ok(RespValue::BulkString(None)),
                    }
                }
                Err(_) => {
                    if tokens.is_empty() {
                        Ok(RespValue::BulkString(Some(json_str.into_bytes())))
                    } else {
                        Ok(RespValue::BulkString(None))
                    }
                }
            },
            Err(_) => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Redis JSON.SET command - Set a JSON value at a specific path
/// Also handles the SET_JSON alias used by some clients
pub async fn json_set(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = String::from_utf8_lossy(&args[1]);
    let value_bytes = args[2].clone();

    debug!(
        "JSON.SET command for key: {}, path: {}",
        String::from_utf8_lossy(key),
        path
    );

    // Handle root path case
    if path == "." || path == "$" {
        let validated_bytes = match validate_or_sanitize_json(value_bytes).await {
            Ok(bytes) => bytes,
            Err(err) => return Err(err),
        };
        engine.set(key.clone(), validated_bytes, None).await?;
        return Ok(RespValue::SimpleString("OK".to_string()));
    }

    // Parse the new value for path-based updates
    let new_value_str = String::from_utf8(value_bytes.clone())
        .map_err(|_| CommandError::InvalidArgument("Value is not valid UTF-8".to_string()))?;
    let new_value = serde_json::from_str::<Value>(&new_value_str)
        .map_err(|e| CommandError::InvalidArgument(format!("Invalid JSON: {}", e)))?;

    // Handle path-based update
    let tokens = parse_json_path(&path);

    // Get existing data
    match engine.get(key).await? {
        Some(data) => {
            let json_str = String::from_utf8(data).map_err(|_| {
                CommandError::InvalidArgument("Stored value is not valid UTF-8".to_string())
            })?;
            let mut json_value = serde_json::from_str::<Value>(&json_str).map_err(|e| {
                CommandError::InvalidArgument(format!("Invalid stored JSON: {}", e))
            })?;

            // Update the value at the specified path
            if update_json_at_path(&mut json_value, &tokens, new_value) {
                // Serialize and store the updated JSON
                let updated_json = serde_json::to_vec(&json_value).map_err(|e| {
                    CommandError::InternalError(format!("JSON serialization error: {}", e))
                })?;
                engine.set(key.clone(), updated_json, None).await?;
                Ok(RespValue::SimpleString("OK".to_string()))
            } else {
                Err(CommandError::InvalidArgument(
                    "Path does not exist".to_string(),
                ))
            }
        }
        None => {
            // If key doesn't exist and it's a root path, create it
            if path == "." || path == "$" {
                let validated_bytes = match validate_or_sanitize_json(value_bytes).await {
                    Ok(bytes) => bytes,
                    Err(err) => return Err(err),
                };
                engine.set(key.clone(), validated_bytes, None).await?;
                Ok(RespValue::SimpleString("OK".to_string()))
            } else {
                Err(CommandError::InvalidArgument(
                    "Key does not exist".to_string(),
                ))
            }
        }
    }
}

/// Redis JSON.TYPE command - Get the type of value at path
pub async fn json_type(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        String::from_utf8_lossy(&args[1]).to_string()
    } else {
        "$".to_string()
    };
    let tokens = parse_json_path(&path);

    debug!(
        "JSON.TYPE command for key: {}, path: {}",
        String::from_utf8_lossy(key),
        path
    );

    // Get the raw data from storage
    match engine.get(key).await? {
        Some(data) => match String::from_utf8(data) {
            Ok(json_str) => match serde_json::from_str::<Value>(&json_str) {
                Ok(json_value) => match select_json_path(&json_value, &tokens) {
                    Some(selected) => {
                        let type_name = json_type_name(selected);
                        Ok(RespValue::BulkString(Some(type_name.into_bytes())))
                    }
                    None => Ok(RespValue::BulkString(None)),
                },
                Err(_) => Err(CommandError::WrongType),
            },
            Err(_) => Err(CommandError::WrongType),
        },
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Helper function to get the JSON type name
fn json_type_name(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "boolean".to_string(),
        Value::Number(_) => "number".to_string(),
        Value::String(_) => "string".to_string(),
        Value::Array(_) => "array".to_string(),
        Value::Object(_) => "object".to_string(),
    }
}

async fn validate_or_sanitize_json(bytes: Vec<u8>) -> Result<Vec<u8>, CommandError> {
    match validate_json_bytes(bytes.clone()).await {
        Ok(valid) => Ok(valid),
        Err(err) => {
            if !matches!(err, CommandError::InvalidArgument(_)) {
                return Err(err);
            }

            let sanitized = escape_control_characters(&bytes);
            if sanitized == bytes {
                return Err(err);
            }

            match validate_json_bytes(sanitized.clone()).await {
                Ok(valid) => {
                    debug!("Sanitized JSON payload containing control characters");
                    Ok(valid)
                }
                Err(_) => Err(err),
            }
        }
    }
}

async fn validate_json_bytes(bytes: Vec<u8>) -> Result<Vec<u8>, CommandError> {
    let validation_task = task::spawn_blocking(move || -> Result<Vec<u8>, serde_json::Error> {
        serde_json::from_slice::<IgnoredAny>(&bytes)?;
        Ok(bytes)
    });

    match validation_task.await {
        Ok(Ok(valid)) => Ok(valid),
        Ok(Err(err)) => Err(CommandError::InvalidArgument(format!(
            "Invalid JSON: {}",
            err
        ))),
        Err(join_err) => Err(CommandError::InternalError(format!(
            "Failed to validate JSON payload: {}",
            join_err
        ))),
    }
}

fn escape_control_characters(bytes: &[u8]) -> Vec<u8> {
    // Try to parse as JSON first to understand structure
    let json_str = String::from_utf8_lossy(bytes);

    // Use a simple state machine to track if we're inside a JSON string
    let mut result = Vec::with_capacity(bytes.len() * 2); // Reserve extra space for escaping
    let mut in_string = false;
    let mut escaped = false;

    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    for &b in bytes {
        // Track JSON string boundaries
        if !escaped && b == b'"' {
            in_string = !in_string;
            result.push(b);
        } else if !escaped && b == b'\\' {
            escaped = true;
            result.push(b);
        } else if in_string && ((b < 0x20 && b != b'\n' && b != b'\r' && b != b'\t') || b == 0x7f) {
            // Inside a JSON string and found a control character - properly escape it
            // Note: We need to add the backslash escape sequence
            result.extend_from_slice(b"\\u00");
            result.push(HEX[(b >> 4) as usize]);
            result.push(HEX[(b & 0x0F) as usize]);
            escaped = false;
        } else {
            result.push(b);
            escaped = false;
        }
    }

    result
}

#[derive(Debug, Clone)]
enum PathToken {
    Key(String),
    Index(usize),
}

fn parse_json_path(path: &str) -> Vec<PathToken> {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "." || trimmed == "$" {
        return Vec::new();
    }

    let mut working = trimmed;
    if let Some(stripped) = working.strip_prefix('$') {
        working = stripped;
        if let Some(rest) = working.strip_prefix('.') {
            working = rest;
        }
    } else if let Some(rest) = working.strip_prefix('.') {
        working = rest;
    }

    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut chars = working.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                if !current.is_empty() {
                    // Check if the current token is a number (array index)
                    if let Ok(idx) = current.parse::<usize>() {
                        tokens.push(PathToken::Index(idx));
                    } else {
                        tokens.push(PathToken::Key(current.clone()));
                    }
                    current.clear();
                }
            }
            '[' => {
                if !current.is_empty() {
                    // Check if the current token is a number (array index)
                    if let Ok(idx) = current.parse::<usize>() {
                        tokens.push(PathToken::Index(idx));
                    } else {
                        tokens.push(PathToken::Key(current.clone()));
                    }
                    current.clear();
                }
                let mut index_str = String::new();
                while let Some(&c) = chars.peek() {
                    chars.next();
                    if c == ']' {
                        break;
                    }
                    index_str.push(c);
                }
                if let Ok(idx) = index_str.parse::<usize>() {
                    tokens.push(PathToken::Index(idx));
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        // Check if the current token is a number (array index)
        if let Ok(idx) = current.parse::<usize>() {
            tokens.push(PathToken::Index(idx));
        } else {
            tokens.push(PathToken::Key(current));
        }
    }

    tokens
}

fn select_json_path<'a>(value: &'a Value, tokens: &[PathToken]) -> Option<&'a Value> {
    let mut current = value;

    for token in tokens {
        match token {
            PathToken::Key(key) => match current {
                Value::Object(map) => {
                    current = map.get(key)?;
                }
                _ => return None,
            },
            PathToken::Index(idx) => match current {
                Value::Array(arr) => current = arr.get(*idx)?,
                _ => return None,
            },
        }
    }

    Some(current)
}

/// Find a mutable reference to a JSON value at a specific path
fn find_mutable_json_path<'a>(value: &'a mut Value, tokens: &[PathToken]) -> Option<&'a mut Value> {
    if tokens.is_empty() {
        return Some(value);
    }

    let mut current = value;

    for token in tokens {
        match token {
            PathToken::Key(key) => {
                if let Value::Object(map) = current {
                    current = map.get_mut(key)?;
                } else {
                    return None;
                }
            }
            PathToken::Index(idx) => {
                if let Value::Array(arr) = current {
                    current = arr.get_mut(*idx)?;
                } else {
                    return None;
                }
            }
        }
    }

    Some(current)
}

/// Update a JSON value at a specific path
fn update_json_at_path(value: &mut Value, tokens: &[PathToken], new_value: Value) -> bool {
    if tokens.is_empty() {
        *value = new_value;
        return true;
    }

    let (first, rest) = tokens.split_first().unwrap();

    match first {
        PathToken::Key(key) => {
            if let Value::Object(map) = value {
                if rest.is_empty() {
                    // We're at the target path, update the value
                    map.insert(key.clone(), new_value);
                    true
                } else if let Some(next_value) = map.get_mut(key) {
                    // Continue traversing
                    update_json_at_path(next_value, rest, new_value)
                } else {
                    // Path doesn't exist
                    false
                }
            } else {
                false
            }
        }
        PathToken::Index(idx) => {
            if let Value::Array(arr) = value {
                if let Some(next_value) = arr.get_mut(*idx) {
                    if rest.is_empty() {
                        // We're at the target path, update the value
                        *next_value = new_value;
                        true
                    } else {
                        // Continue traversing
                        update_json_at_path(next_value, rest, new_value)
                    }
                } else {
                    // Index out of bounds
                    false
                }
            } else {
                false
            }
        }
    }
}

/// Redis JSON.ARRAPPEND command - Append values to an array
pub async fn json_arrappend(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = String::from_utf8_lossy(&args[1]).to_string();
    let values_to_append = &args[2..];

    debug!(
        "JSON.ARRAPPEND command for key: {}, path: {}",
        String::from_utf8_lossy(key),
        path
    );

    // Parse the path
    let tokens = parse_json_path(&path);

    // Get the current JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(mut json_value) => {
                            // Find the array at the specified path
                            let array_ref = if tokens.is_empty()
                                || (tokens.len() == 1
                                    && matches!(&tokens[0], PathToken::Key(k) if k == "$" || k == "."))
                            {
                                // Root path
                                &mut json_value
                            } else {
                                // Navigate to the specified path
                                find_mutable_json_path(&mut json_value, &tokens).ok_or_else(
                                    || {
                                        CommandError::InvalidArgument(
                                            "Path does not exist".to_string(),
                                        )
                                    },
                                )?
                            };

                            if let Value::Array(arr) = array_ref {
                                // Parse and append each value
                                for value_bytes in values_to_append {
                                    let value_str = String::from_utf8_lossy(value_bytes);
                                    match serde_json::from_str::<Value>(&value_str) {
                                        Ok(parsed_value) => {
                                            arr.push(parsed_value);
                                        }
                                        Err(_) => {
                                            // If it's not valid JSON, treat as string
                                            arr.push(Value::String(value_str.to_string()));
                                        }
                                    }
                                }

                                let array_len = arr.len() as i64;

                                // Store the updated JSON
                                let updated_json =
                                    serde_json::to_string(&json_value).map_err(|e| {
                                        CommandError::InternalError(format!(
                                            "JSON serialization error: {}",
                                            e
                                        ))
                                    })?;

                                engine
                                    .set(key.clone(), updated_json.into_bytes(), None)
                                    .await?;

                                Ok(RespValue::Integer(array_len))
                            } else {
                                Err(CommandError::InvalidArgument(
                                    "Value at path is not an array".to_string(),
                                ))
                            }
                        }
                        Err(e) => Err(CommandError::InvalidArgument(format!(
                            "Invalid JSON: {}",
                            e
                        ))),
                    }
                }
                Err(_) => Err(CommandError::InvalidArgument(
                    "Value is not valid UTF-8".to_string(),
                )),
            }
        }
        None => Err(CommandError::InvalidArgument(
            "Key does not exist".to_string(),
        )),
    }
}

/// Redis JSON.ARRTRIM command - Trim an array to include a specified range of elements
pub async fn json_arrtrim(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = String::from_utf8_lossy(&args[1]).to_string();
    let start = String::from_utf8_lossy(&args[2])
        .parse::<i64>()
        .map_err(|_| {
            CommandError::InvalidArgument("Start index is not a valid integer".to_string())
        })?;
    let stop = String::from_utf8_lossy(&args[3])
        .parse::<i64>()
        .map_err(|_| {
            CommandError::InvalidArgument("Stop index is not a valid integer".to_string())
        })?;

    debug!(
        "JSON.ARRTRIM command for key: {}, path: {}, start: {}, stop: {}",
        String::from_utf8_lossy(key),
        path,
        start,
        stop
    );

    // Parse the path
    let tokens = parse_json_path(&path);

    // Get the current JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(mut json_value) => {
                            // Find the array at the specified path
                            let array_ref = if tokens.is_empty()
                                || (tokens.len() == 1
                                    && matches!(&tokens[0], PathToken::Key(k) if k == "$" || k == "."))
                            {
                                // Root path
                                &mut json_value
                            } else {
                                // Navigate to the specified path
                                find_mutable_json_path(&mut json_value, &tokens).ok_or_else(
                                    || {
                                        CommandError::InvalidArgument(
                                            "Path does not exist".to_string(),
                                        )
                                    },
                                )?
                            };

                            if let Value::Array(arr) = array_ref {
                                let len = arr.len() as i64;

                                // Handle negative indices
                                let norm_start = if start < 0 { len + start } else { start };
                                let norm_stop = if stop < 0 { len + stop } else { stop };

                                // Clamp to valid range
                                let start_idx = std::cmp::max(0, norm_start) as usize;
                                let stop_idx = std::cmp::min(len - 1, norm_stop) as usize;

                                if start_idx < arr.len() && start_idx <= stop_idx {
                                    // Extract the trimmed portion
                                    let trimmed: Vec<Value> = arr
                                        .drain(start_idx..=std::cmp::min(stop_idx, arr.len() - 1))
                                        .collect();
                                    *arr = trimmed;
                                } else {
                                    // Invalid range, clear the array
                                    arr.clear();
                                }

                                let array_len = arr.len() as i64;

                                // Store the updated JSON
                                let updated_json =
                                    serde_json::to_string(&json_value).map_err(|e| {
                                        CommandError::InternalError(format!(
                                            "JSON serialization error: {}",
                                            e
                                        ))
                                    })?;

                                engine
                                    .set(key.clone(), updated_json.into_bytes(), None)
                                    .await?;

                                Ok(RespValue::Integer(array_len))
                            } else {
                                Err(CommandError::InvalidArgument(
                                    "Value at path is not an array".to_string(),
                                ))
                            }
                        }
                        Err(e) => Err(CommandError::InvalidArgument(format!(
                            "Invalid JSON: {}",
                            e
                        ))),
                    }
                }
                Err(_) => Err(CommandError::InvalidArgument(
                    "Value is not valid UTF-8".to_string(),
                )),
            }
        }
        None => Err(CommandError::InvalidArgument(
            "Key does not exist".to_string(),
        )),
    }
}

/// Redis JSON.RESP command - Get JSON value in RESP format
/// Returns the JSON value at key/path encoded as RESP types according to Redis JSON specs
pub async fn json_resp(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        String::from_utf8_lossy(&args[1]).to_string()
    } else {
        "$".to_string() // Default to root
    };

    debug!(
        "JSON.RESP command for key: {}, path: {}",
        String::from_utf8_lossy(key),
        path
    );

    // Get the raw data from storage
    match engine.get(key).await? {
        Some(data) => {
            // Try to parse as JSON
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_value) => {
                            // Apply path selection if not root
                            let selected_value = if path == "$" || path == "." {
                                json_value
                            } else {
                                // For now, just return the whole object for simplicity
                                // TODO: Implement proper JSONPath support
                                json_value
                            };

                            // Convert JSON to RESP according to Redis JSON specs
                            Ok(json_to_resp(&selected_value))
                        }
                        Err(e) => {
                            warn!(
                                "JSON parse error for key {}: {}",
                                String::from_utf8_lossy(key),
                                e
                            );
                            Err(CommandError::WrongType)
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "UTF-8 decode error for key {}: {}",
                        String::from_utf8_lossy(key),
                        e
                    );
                    Err(CommandError::WrongType)
                }
            }
        }
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Convert JSON value to RESP according to Redis JSON specifications
fn json_to_resp(value: &Value) -> RespValue {
    match value {
        Value::Null => RespValue::BulkString(None),
        Value::Bool(b) => RespValue::SimpleString(if *b {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                RespValue::Integer(n.as_i64().unwrap_or(0))
            } else {
                // Floating point numbers are returned as bulk strings
                RespValue::BulkString(Some(n.to_string().into_bytes()))
            }
        }
        Value::String(s) => RespValue::BulkString(Some(s.as_bytes().to_vec())),
        Value::Array(arr) => {
            let mut resp_array = vec![RespValue::SimpleString("[".to_string())];
            for item in arr {
                resp_array.push(json_to_resp(item));
            }
            RespValue::Array(Some(resp_array))
        }
        Value::Object(obj) => {
            let mut resp_array = vec![RespValue::SimpleString("{".to_string())];
            for (key, val) in obj {
                resp_array.push(RespValue::BulkString(Some(key.as_bytes().to_vec())));
                resp_array.push(json_to_resp(val));
            }
            RespValue::Array(Some(resp_array))
        }
    }
}

/// Redis JSON.DEL command - Delete a JSON value
pub async fn json_del(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        String::from_utf8_lossy(&args[1]).to_string()
    } else {
        "$".to_string() // Default to root
    };

    debug!(
        "JSON.DEL command for key: {}, path: {}",
        String::from_utf8_lossy(key),
        path
    );

    // If path is root, delete the entire key
    if path == "$" || path == "." {
        match engine.del(key).await? {
            true => Ok(RespValue::Integer(1)),
            false => Ok(RespValue::Integer(0)),
        }
    } else {
        // For non-root paths, we'd need proper JSONPath implementation
        // For now, return not implemented for complex paths
        Err(CommandError::InternalError(
            "JSON.DEL for non-root paths not yet implemented".to_string(),
        ))
    }
}

/// Redis JSON.OBJKEYS command - Return the keys in the object that's referenced by path
pub async fn json_objkeys(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        String::from_utf8_lossy(&args[1]).to_string()
    } else {
        "$".to_string() // Default to root
    };

    debug!(
        "JSON.OBJKEYS command for key: {}, path: {}",
        String::from_utf8_lossy(key),
        path
    );

    // Get the JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_value) => {
                            // For simplicity, assume root path for now
                            if let Value::Object(obj) = json_value {
                                let keys: Vec<RespValue> = obj
                                    .keys()
                                    .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                                    .collect();
                                Ok(RespValue::Array(Some(keys)))
                            } else {
                                Ok(RespValue::BulkString(None)) // Not an object
                            }
                        }
                        Err(e) => Err(CommandError::InvalidArgument(format!(
                            "Invalid JSON: {}",
                            e
                        ))),
                    }
                }
                Err(_) => Err(CommandError::InvalidArgument(
                    "Value is not valid UTF-8".to_string(),
                )),
            }
        }
        None => Ok(RespValue::BulkString(None)), // Key doesn't exist
    }
}

/// Redis JSON.OBJLEN command - Return the number of keys in the object that's referenced by path
pub async fn json_objlen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        String::from_utf8_lossy(&args[1]).to_string()
    } else {
        "$".to_string() // Default to root
    };

    debug!(
        "JSON.OBJLEN command for key: {}, path: {}",
        String::from_utf8_lossy(key),
        path
    );

    // Get the JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_value) => {
                            // For simplicity, assume root path for now
                            if let Value::Object(obj) = json_value {
                                Ok(RespValue::Integer(obj.len() as i64))
                            } else {
                                Ok(RespValue::BulkString(None)) // Not an object
                            }
                        }
                        Err(e) => Err(CommandError::InvalidArgument(format!(
                            "Invalid JSON: {}",
                            e
                        ))),
                    }
                }
                Err(_) => Err(CommandError::InvalidArgument(
                    "Value is not valid UTF-8".to_string(),
                )),
            }
        }
        None => Ok(RespValue::BulkString(None)), // Key doesn't exist
    }
}

/// Redis JSON.ARRLEN command - Return the length of the array at path
pub async fn json_arrlen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        String::from_utf8_lossy(&args[1]).to_string()
    } else {
        "$".to_string() // Default to root
    };

    debug!(
        "JSON.ARRLEN command for key: {}, path: {}",
        String::from_utf8_lossy(key),
        path
    );

    // Parse the path
    let tokens = parse_json_path(&path);

    // Get the JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_value) => {
                            // Navigate to the specified path
                            let target_value = if tokens.is_empty()
                                || (tokens.len() == 1
                                    && matches!(&tokens[0], PathToken::Key(k) if k == "$" || k == "."))
                            {
                                &json_value
                            } else {
                                match select_json_path(&json_value, &tokens) {
                                    Some(v) => v,
                                    None => return Ok(RespValue::BulkString(None)), // Path doesn't exist
                                }
                            };

                            // Check if the value at the path is an array
                            if let Value::Array(arr) = target_value {
                                Ok(RespValue::Integer(arr.len() as i64))
                            } else {
                                // Not an array - return 0 for compatibility
                                Ok(RespValue::Integer(0))
                            }
                        }
                        Err(e) => Err(CommandError::InvalidArgument(format!(
                            "Invalid JSON: {}",
                            e
                        ))),
                    }
                }
                Err(_) => Err(CommandError::InvalidArgument(
                    "Value is not valid UTF-8".to_string(),
                )),
            }
        }
        None => Ok(RespValue::BulkString(None)), // Key doesn't exist
    }
}

/// Redis JSON.NUMINCRBY command - Increment the number value at path by the provided number
pub async fn json_numincrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = String::from_utf8_lossy(&args[1]).to_string();
    let increment = String::from_utf8_lossy(&args[2])
        .parse::<f64>()
        .map_err(|_| {
            CommandError::InvalidArgument("Increment value is not a valid number".to_string())
        })?;

    debug!(
        "JSON.NUMINCRBY command for key: {}, path: {}, increment: {}",
        String::from_utf8_lossy(key),
        path,
        increment
    );

    // Parse the path
    let tokens = parse_json_path(&path);

    // Get the current JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(mut json_value) => {
                            // Helper function to increment number at path
                            let increment_at_path =
                                |value: &mut Value,
                                 tokens: &[PathToken],
                                 inc: f64|
                                 -> Result<f64, CommandError> {
                                    if tokens.is_empty() {
                                        // Root path
                                        if let Value::Number(num) = value {
                                            let current_value = num.as_f64().unwrap_or(0.0);
                                            let new_value = current_value + inc;

                                            if !new_value.is_finite() {
                                                return Err(CommandError::InvalidArgument(
                                                    "Result is not a valid number".to_string(),
                                                ));
                                            }

                                            *value = if new_value.fract() == 0.0
                                                && new_value.abs() < 1e15
                                            {
                                                Value::Number(serde_json::Number::from(
                                                    new_value as i64,
                                                ))
                                            } else {
                                                Value::Number(
                                                    serde_json::Number::from_f64(new_value)
                                                        .ok_or_else(|| {
                                                            CommandError::InvalidArgument(
                                                                "Invalid number result".to_string(),
                                                            )
                                                        })?,
                                                )
                                            };
                                            Ok(new_value)
                                        } else {
                                            Err(CommandError::InvalidArgument(
                                                "Value at path is not a number".to_string(),
                                            ))
                                        }
                                    } else {
                                        // Navigate to path
                                        let target = find_mutable_json_path(value, tokens)
                                            .ok_or_else(|| {
                                                CommandError::InvalidArgument(
                                                    "Path does not exist".to_string(),
                                                )
                                            })?;

                                        if let Value::Number(num) = target {
                                            let current_value = num.as_f64().unwrap_or(0.0);
                                            let new_value = current_value + inc;

                                            if !new_value.is_finite() {
                                                return Err(CommandError::InvalidArgument(
                                                    "Result is not a valid number".to_string(),
                                                ));
                                            }

                                            *target = if new_value.fract() == 0.0
                                                && new_value.abs() < 1e15
                                            {
                                                Value::Number(serde_json::Number::from(
                                                    new_value as i64,
                                                ))
                                            } else {
                                                Value::Number(
                                                    serde_json::Number::from_f64(new_value)
                                                        .ok_or_else(|| {
                                                            CommandError::InvalidArgument(
                                                                "Invalid number result".to_string(),
                                                            )
                                                        })?,
                                                )
                                            };
                                            Ok(new_value)
                                        } else {
                                            Err(CommandError::InvalidArgument(
                                                "Value at path is not a number".to_string(),
                                            ))
                                        }
                                    }
                                };

                            // Perform the increment
                            let new_value = increment_at_path(&mut json_value, &tokens, increment)?;

                            // Store the updated JSON
                            let updated_json = serde_json::to_string(&json_value).map_err(|e| {
                                CommandError::InternalError(format!(
                                    "JSON serialization error: {}",
                                    e
                                ))
                            })?;

                            engine
                                .set(key.clone(), updated_json.into_bytes(), None)
                                .await?;

                            Ok(RespValue::BulkString(Some(
                                new_value.to_string().into_bytes(),
                            )))
                        }
                        Err(e) => Err(CommandError::InvalidArgument(format!(
                            "Invalid JSON: {}",
                            e
                        ))),
                    }
                }
                Err(_) => Err(CommandError::InvalidArgument(
                    "Value is not valid UTF-8".to_string(),
                )),
            }
        }
        None => Err(CommandError::InvalidArgument(
            "Key does not exist".to_string(),
        )),
    }
}
