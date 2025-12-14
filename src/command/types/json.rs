use serde_json::Value;
use crate::command::error::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use tracing::{debug, warn};
// use crate::command::types::json_integration::{
//     enhanced_json_get, enhanced_json_set, enhanced_json_type,
//     enhanced_json_arrappend, enhanced_json_arrtrim
// };

/// Extract a value from JSON at the specified path
/// Supports Redis-compatible path syntax:
/// - `.` or `$` - root (return whole document)
/// - `.field` - get field from object
/// - `.field.nested` - navigate nested fields
/// - `.field[0]` - array index access
///
/// Uses serde_json's pointer() for optimal performance
fn extract_json_at_path<'a>(value: &'a Value, path: &str) -> Result<&'a Value, CommandError> {
    // Handle root paths
    if path == "." || path == "$" || path.is_empty() {
        return Ok(value);
    }

    // Convert Redis path to JSON Pointer format
    let json_pointer = convert_redis_path_to_pointer(path)?;

    // Use serde_json's efficient pointer lookup
    value.pointer(&json_pointer)
        .ok_or_else(|| CommandError::InvalidArgument(format!("Path not found: {}", path)))
}

/// Convert Redis JSON path syntax to JSON Pointer (RFC 6901)
/// - `.field` -> `/field`
/// - `.field.nested` -> `/field/nested`
/// - `.field[0]` -> `/field/0`
/// - `$` or `$.field` -> `/field`
fn convert_redis_path_to_pointer(path: &str) -> Result<String, CommandError> {
    let path = path.trim();

    // Handle root
    if path == "." || path == "$" {
        return Ok(String::new());
    }

    // Remove leading `.` or `$`
    let path = if path.starts_with('.') {
        &path[1..]
    } else if path.starts_with("$.") {
        &path[2..]
    } else if path.starts_with('$') {
        &path[1..]
    } else {
        path
    };

    // Convert to JSON Pointer format
    // Replace `.` with `/` and handle array indices
    let mut pointer = String::with_capacity(path.len() + 10);
    let mut chars = path.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                pointer.push('/');
            },
            '[' => {
                // Array index: field[0] -> /field/0
                pointer.push('/');
                // Collect digits
                while let Some(&next_ch) = chars.peek() {
                    if next_ch.is_ascii_digit() {
                        pointer.push(chars.next().unwrap());
                    } else if next_ch == ']' {
                        chars.next(); // consume ']'
                        break;
                    } else {
                        return Err(CommandError::InvalidArgument(
                            format!("Invalid array index in path: {}", path)
                        ));
                    }
                }
            },
            _ => {
                if pointer.is_empty() || pointer.ends_with('/') {
                    pointer.push('/');
                }
                pointer.push(ch);
            }
        }
    }

    Ok(pointer)
}

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

    debug!("JSON.GET command for key: {}, path: {}", String::from_utf8_lossy(key), path);

    // Get the raw data from storage
    match engine.get(key).await? {
        Some(data) => {
            // Try to parse as JSON
            match String::from_utf8(data) {
                Ok(json_str) => {
                    // Parse JSON value
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(mut json_value) => {
                            // Apply datetime transformation
                            json_value = crate::utils::datetime::process_json_for_serialization(json_value);

                            // Extract value at path
                            let result_value = extract_json_at_path(&json_value, &path)?;

                            // Serialize result
                            match serde_json::to_string(&result_value) {
                                Ok(result_str) => Ok(RespValue::BulkString(Some(result_str.into_bytes()))),
                                Err(_) => Err(CommandError::InvalidArgument("JSON serialization failed".to_string()))
                            }
                        },
                        Err(_) => Err(CommandError::WrongType)
                    }
                },
                Err(_) => Err(CommandError::WrongType)
            }
        },
        None => Ok(RespValue::BulkString(None))
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
    let value_data = &args[2];

    debug!("JSON.SET command for key: {}, path: {}", String::from_utf8_lossy(key), path);

    // Parse the JSON value
    let json_str = String::from_utf8_lossy(value_data);
    match serde_json::from_str::<Value>(&json_str) {
        Ok(_) => {
            // Store as raw JSON string
            engine.set(key.clone(), value_data.clone(), None).await?;
            Ok(RespValue::SimpleString("OK".to_string()))
        },
        Err(_) => Err(CommandError::InvalidArgument("Invalid JSON".to_string()))
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

    debug!("JSON.TYPE command for key: {}, path: {}", String::from_utf8_lossy(key), path);

    // Get the raw data from storage
    match engine.get(key).await? {
        Some(data) => {
            // Try to parse as JSON
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_value) => {
                            let type_name = json_type_name(&json_value);
                            Ok(RespValue::BulkString(Some(type_name.into_bytes())))
                        },
                        Err(_) => Err(CommandError::WrongType)
                    }
                },
                Err(_) => Err(CommandError::WrongType)
            }
        },
        None => Ok(RespValue::BulkString(None))
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

/// Redis JSON.ARRAPPEND command - Append values to an array
pub async fn json_arrappend(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = String::from_utf8_lossy(&args[1]).to_string();
    let values_to_append = &args[2..];

    debug!("JSON.ARRAPPEND command for key: {}, path: {}", String::from_utf8_lossy(key), path);

    // Get the current JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(mut json_value) => {
                            // For simplicity, assume root path for now
                            if let Value::Array(ref mut arr) = json_value {
                                // Parse and append each value
                                for value_bytes in values_to_append {
                                    let value_str = String::from_utf8_lossy(value_bytes);
                                    match serde_json::from_str::<Value>(&value_str) {
                                        Ok(parsed_value) => {
                                            arr.push(parsed_value);
                                        },
                                        Err(_) => {
                                            // If it's not valid JSON, treat as string
                                            arr.push(Value::String(value_str.to_string()));
                                        }
                                    }
                                }
                                
                                let array_len = arr.len() as i64;
                                
                                // Store the updated JSON
                                let updated_json = serde_json::to_string(&json_value)
                                    .map_err(|e| CommandError::InternalError(format!("JSON serialization error: {}", e)))?;
                                
                                engine.set(key.clone(), updated_json.into_bytes(), None).await?;
                                
                                Ok(RespValue::Integer(array_len))
                            } else {
                                Err(CommandError::InvalidArgument("Value at path is not an array".to_string()))
                            }
                        },
                        Err(e) => Err(CommandError::InvalidArgument(format!("Invalid JSON: {}", e)))
                    }
                },
                Err(_) => Err(CommandError::InvalidArgument("Value is not valid UTF-8".to_string()))
            }
        },
        None => Err(CommandError::InvalidArgument("Key does not exist".to_string()))
    }
}

/// Redis JSON.ARRTRIM command - Trim an array to include a specified range of elements
pub async fn json_arrtrim(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = String::from_utf8_lossy(&args[1]).to_string();
    let start = String::from_utf8_lossy(&args[2]).parse::<i64>()
        .map_err(|_| CommandError::InvalidArgument("Start index is not a valid integer".to_string()))?;
    let stop = String::from_utf8_lossy(&args[3]).parse::<i64>()
        .map_err(|_| CommandError::InvalidArgument("Stop index is not a valid integer".to_string()))?;

    debug!("JSON.ARRTRIM command for key: {}, path: {}, start: {}, stop: {}", 
           String::from_utf8_lossy(key), path, start, stop);

    // Get the current JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(mut json_value) => {
                            // For simplicity, assume root path for now
                            if let Value::Array(ref mut arr) = json_value {
                                let len = arr.len() as i64;
                                
                                // Handle negative indices
                                let norm_start = if start < 0 { len + start } else { start };
                                let norm_stop = if stop < 0 { len + stop } else { stop };
                                
                                // Clamp to valid range
                                let start_idx = std::cmp::max(0, norm_start) as usize;
                                let stop_idx = std::cmp::min(len - 1, norm_stop) as usize;
                                
                                if start_idx < arr.len() && start_idx <= stop_idx {
                                    // Extract the trimmed portion
                                    let trimmed: Vec<Value> = arr.drain(start_idx..=std::cmp::min(stop_idx, arr.len() - 1)).collect();
                                    *arr = trimmed;
                                } else {
                                    // Invalid range, clear the array
                                    arr.clear();
                                }
                                
                                let array_len = arr.len() as i64;
                                
                                // Store the updated JSON
                                let updated_json = serde_json::to_string(&json_value)
                                    .map_err(|e| CommandError::InternalError(format!("JSON serialization error: {}", e)))?;
                                
                                engine.set(key.clone(), updated_json.into_bytes(), None).await?;
                                
                                Ok(RespValue::Integer(array_len))
                            } else {
                                Err(CommandError::InvalidArgument("Value at path is not an array".to_string()))
                            }
                        },
                        Err(e) => Err(CommandError::InvalidArgument(format!("Invalid JSON: {}", e)))
                    }
                },
                Err(_) => Err(CommandError::InvalidArgument("Value is not valid UTF-8".to_string()))
            }
        },
        None => Err(CommandError::InvalidArgument("Key does not exist".to_string()))
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

    debug!("JSON.RESP command for key: {}, path: {}", String::from_utf8_lossy(key), path);

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
                        },
                        Err(e) => {
                            warn!("JSON parse error for key {}: {}", String::from_utf8_lossy(key), e);
                            Err(CommandError::WrongType)
                        }
                    }
                },
                Err(e) => {
                    warn!("UTF-8 decode error for key {}: {}", String::from_utf8_lossy(key), e);
                    Err(CommandError::WrongType)
                }
            }
        },
        None => Ok(RespValue::BulkString(None))
    }
}

/// Convert JSON value to RESP according to Redis JSON specifications
fn json_to_resp(value: &Value) -> RespValue {
    match value {
        Value::Null => RespValue::BulkString(None),
        Value::Bool(b) => RespValue::SimpleString(if *b { "true".to_string() } else { "false".to_string() }),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                RespValue::Integer(n.as_i64().unwrap_or(0))
            } else {
                // Floating point numbers are returned as bulk strings
                RespValue::BulkString(Some(n.to_string().into_bytes()))
            }
        },
        Value::String(s) => RespValue::BulkString(Some(s.as_bytes().to_vec())),
        Value::Array(arr) => {
            let mut resp_array = vec![RespValue::SimpleString("[".to_string())];
            for item in arr {
                resp_array.push(json_to_resp(item));
            }
            RespValue::Array(Some(resp_array))
        },
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

    debug!("JSON.DEL command for key: {}, path: {}", String::from_utf8_lossy(key), path);

    // If path is root, delete the entire key
    if path == "$" || path == "." {
        match engine.del(key).await? {
            true => Ok(RespValue::Integer(1)),
            false => Ok(RespValue::Integer(0)),
        }
    } else {
        // For non-root paths, we'd need proper JSONPath implementation
        // For now, return not implemented for complex paths
        Err(CommandError::InternalError("JSON.DEL for non-root paths not yet implemented".to_string()))
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

    debug!("JSON.OBJKEYS command for key: {}, path: {}", String::from_utf8_lossy(key), path);

    // Get the JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_value) => {
                            // For simplicity, assume root path for now
                            if let Value::Object(obj) = json_value {
                                let keys: Vec<RespValue> = obj.keys()
                                    .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                                    .collect();
                                Ok(RespValue::Array(Some(keys)))
                            } else {
                                Ok(RespValue::BulkString(None)) // Not an object
                            }
                        },
                        Err(e) => Err(CommandError::InvalidArgument(format!("Invalid JSON: {}", e)))
                    }
                },
                Err(_) => Err(CommandError::InvalidArgument("Value is not valid UTF-8".to_string()))
            }
        },
        None => Ok(RespValue::BulkString(None)) // Key doesn't exist
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

    debug!("JSON.OBJLEN command for key: {}, path: {}", String::from_utf8_lossy(key), path);

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
                        },
                        Err(e) => Err(CommandError::InvalidArgument(format!("Invalid JSON: {}", e)))
                    }
                },
                Err(_) => Err(CommandError::InvalidArgument("Value is not valid UTF-8".to_string()))
            }
        },
        None => Ok(RespValue::BulkString(None)) // Key doesn't exist
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

    debug!("JSON.ARRLEN command for key: {}, path: {}", String::from_utf8_lossy(key), path);

    // Get the JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_value) => {
                            // For simplicity, assume root path for now
                            if let Value::Array(arr) = json_value {
                                Ok(RespValue::Integer(arr.len() as i64))
                            } else {
                                Ok(RespValue::BulkString(None)) // Not an array
                            }
                        },
                        Err(e) => Err(CommandError::InvalidArgument(format!("Invalid JSON: {}", e)))
                    }
                },
                Err(_) => Err(CommandError::InvalidArgument("Value is not valid UTF-8".to_string()))
            }
        },
        None => Ok(RespValue::BulkString(None)) // Key doesn't exist
    }
}

/// Redis JSON.NUMINCRBY command - Increment the number value at path by the provided number
pub async fn json_numincrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let path = String::from_utf8_lossy(&args[1]).to_string();
    let increment = String::from_utf8_lossy(&args[2]).parse::<f64>()
        .map_err(|_| CommandError::InvalidArgument("Increment value is not a valid number".to_string()))?;

    debug!("JSON.NUMINCRBY command for key: {}, path: {}, increment: {}", 
           String::from_utf8_lossy(key), path, increment);

    // Get the current JSON data
    match engine.get(key).await? {
        Some(data) => {
            match String::from_utf8(data) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(mut json_value) => {
                            // For simplicity, assume root path for now
                            if let Value::Number(num) = json_value {
                                let current_value = num.as_f64().unwrap_or(0.0);
                                let new_value = current_value + increment;
                                
                                // Check for valid result
                                if !new_value.is_finite() {
                                    return Err(CommandError::InvalidArgument("Result is not a valid number".to_string()));
                                }
                                
                                // Update the JSON value
                                json_value = if new_value.fract() == 0.0 && new_value.abs() < 1e15 {
                                    Value::Number(serde_json::Number::from(new_value as i64))
                                } else {
                                    Value::Number(serde_json::Number::from_f64(new_value)
                                        .ok_or_else(|| CommandError::InvalidArgument("Invalid number result".to_string()))?)
                                };
                                
                                // Store the updated JSON
                                let updated_json = serde_json::to_string(&json_value)
                                    .map_err(|e| CommandError::InternalError(format!("JSON serialization error: {}", e)))?;
                                
                                engine.set(key.clone(), updated_json.into_bytes(), None).await?;
                                
                                Ok(RespValue::BulkString(Some(new_value.to_string().into_bytes())))
                            } else {
                                Err(CommandError::InvalidArgument("Value at path is not a number".to_string()))
                            }
                        },
                        Err(e) => Err(CommandError::InvalidArgument(format!("Invalid JSON: {}", e)))
                    }
                },
                Err(_) => Err(CommandError::InvalidArgument("Value is not valid UTF-8".to_string()))
            }
        },
        None => Err(CommandError::InvalidArgument("Key does not exist".to_string()))
    }
}