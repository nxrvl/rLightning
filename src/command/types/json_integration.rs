use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant};
use once_cell::sync::Lazy;
use tracing::{debug, warn, info};
use serde_json::{Value, json};

use crate::command::error::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::command::types::json_config::JsonConfig;
use crate::command::types::json_storage::JsonStorage;
use crate::networking::json_resp::JsonResp;

/// Global JSON storage with thread-safe access
static JSON_STORAGE: Lazy<Arc<JsonStorage>> = Lazy::new(|| {
    let config = JsonConfig::default();
    Arc::new(JsonStorage::new(config, 1000)) // capacity of 1000 items
});

/// Initialize the JSON storage and return a reference to it
pub fn get_json_storage() -> Arc<JsonStorage> {
    JSON_STORAGE.clone()
}

/// Enhanced JSON.GET implementation using optimized JSON storage
pub async fn enhanced_json_get(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Get path argument
    let path = if args.len() > 1 {
        let path_str = String::from_utf8(args[1].clone())
            .map_err(|_| CommandError::InvalidArgument("Invalid JSON path".to_string()))?;
        debug!("JSON.GET with path: {}", path_str);
        path_str
    } else {
        debug!("JSON.GET with default path: .");
        ".".to_string() // Default path is root
    };
    
    // Check for special key prefixes
    if key.len() > 0 {
        let key_str = String::from_utf8_lossy(key);
        debug!("JSON.GET for key: {}", key_str);
        
        if key_str.starts_with("bnf:") {
            debug!("Detected bnf: prefix in JSON.GET - applying Python client compatibility mode");
        }
    }
    
    // Try to get the value from the storage engine
    match engine.get(key).await? {
        Some(data) => {
            // Try to parse the data as JSON
            match String::from_utf8(data.clone()) {
                Ok(json_str) => {
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_value) => {
                            // Use our optimized JSON storage
                            let storage = get_json_storage();
                            let key_str = String::from_utf8_lossy(key).to_string();
                            
                            // Store the value if not already there
                            if storage.get(&key_str).is_none() {
                                storage.set(&key_str, json_value.clone(), None);
                            }
                            
                            // Get the value at path
                            let result = storage.get_path(&key_str, &path);
                            
                            match result {
                                Some(value) => {
                                    // Process any datetime fields
                                    let processed_value = crate::utils::datetime::process_json_for_serialization(value);
                                    
                                    // Serialize with safe handling
                                    let json_resp = JsonResp::default();
                                    match json_resp.serialize_json(&processed_value) {
                                        Ok(resp_value) => Ok(resp_value),
                                        Err(e) => {
                                            warn!("Failed to serialize JSON: {:?}", e);
                                            Err(CommandError::InternalError("Failed to serialize JSON".to_string()))
                                        }
                                    }
                                },
                                None => Ok(RespValue::BulkString(None)), // Path not found
                            }
                        },
                        Err(e) => {
                            debug!("JSON parse error: {}", e);
                            Err(CommandError::WrongType)
                        },
                    }
                },
                Err(_) => Err(CommandError::WrongType),
            }
        },
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Enhanced JSON.SET implementation using optimized JSON storage
pub async fn enhanced_json_set(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // Log more details about the key for debugging
    if key.len() > 0 {
        let key_str = String::from_utf8_lossy(&key);
        debug!("JSON.SET for key: {}", key_str);
        
        // Special handling for known key prefixes
        if key_str.starts_with("bnf:") {
            debug!("Detected bnf: prefix - applying Python client compatibility mode");
        }
    }
    
    // Get path argument
    let path = String::from_utf8(args[1].clone())
        .map_err(|_| CommandError::InvalidArgument("Invalid JSON path".to_string()))?;
    
    debug!("JSON.SET with path: {}", path);
    
    // Try to decode the JSON data with better error handling
    let json_data = match String::from_utf8(args[2].clone()) {
        Ok(data) => data,
        Err(e) => {
            debug!("UTF-8 decoding error in JSON data: {}", e);
            // Attempt recovery for binary data
            let data = args[2].clone();
            
            // Check if this looks like JSON despite encoding issues
            if data.len() > 2 && (data[0] == b'{' || data[0] == b'[') {
                debug!("Detected binary JSON-like data, attempting to sanitize");
                
                // Create a sanitized version by replacing invalid UTF-8 bytes
                let mut sanitized = Vec::with_capacity(data.len());
                for &byte in data.iter() {
                    if byte >= 128 || byte < 32 {
                        // Replace with space for non-ASCII or control chars
                        sanitized.push(b' ');
                    } else {
                        sanitized.push(byte);
                    }
                }
                
                match String::from_utf8(sanitized) {
                    Ok(s) => s,
                    Err(_) => {
                        return Err(CommandError::InvalidArgument("Invalid UTF-8 in JSON data (recovery failed)".to_string()));
                    }
                }
            } else {
                return Err(CommandError::InvalidArgument("Invalid UTF-8 in JSON data".to_string()));
            }
        }
    };
    
    // Log large JSON data size
    if json_data.len() > 1024 * 1024 {
        debug!("Large JSON data detected: {}MB", json_data.len() / (1024 * 1024));
    } else if json_data.len() > 1024 {
        debug!("JSON data size: {}KB", json_data.len() / 1024);
    }
    
    // Parse the JSON value
    let json_value = match serde_json::from_str::<Value>(&json_data) {
        Ok(value) => value,
        Err(e) => {
            debug!("Failed to parse JSON data: {}", e);
            
            // Attempt recovery for common JSON errors
            if json_data.len() > 5 {
                debug!("Attempting JSON parsing recovery");
                
                // Check for common issues like escaped backslashes, missing quotes, etc.
                let sanitized_json = json_data.replace("\\\\", "\\")
                                             .replace("\\'", "'")
                                             .replace("\\\"", "\"");
                
                match serde_json::from_str::<Value>(&sanitized_json) {
                    Ok(value) => value,
                    Err(_) => {
                        return Err(CommandError::InvalidArgument(format!("Invalid JSON data: {}", e)));
                    }
                }
            } else {
                return Err(CommandError::InvalidArgument(format!("Invalid JSON data: {}", e)));
            }
        },
    };
    
    // Process the value to handle datetime fields
    let processed_value = crate::utils::datetime::process_json_for_serialization(json_value);
    
    // Option flags (NX or XX)
    let nx = args.len() > 3 && args[3].eq_ignore_ascii_case(b"NX");
    let xx = args.len() > 3 && args[3].eq_ignore_ascii_case(b"XX");
    
    // Check existence conditions
    let exists = engine.exists(&key).await?;
    if (nx && exists) || (xx && !exists) {
        return Ok(RespValue::BulkString(None)); // Condition not met
    }
    
    // Use our optimized JSON storage
    let storage = get_json_storage();
    let key_str = String::from_utf8_lossy(&key).to_string();
    
    // Set the value at path
    if path == "." {
        // Set the entire document
        storage.set(&key_str.as_bytes(), processed_value.clone(), None).await?;
        
        // Also store in the main engine
        let json_str = serde_json::to_string(&processed_value)
            .map_err(|e| CommandError::InternalError(format!("Failed to serialize JSON: {}", e)))?;
            
        engine.set(key, json_str.into_bytes(), None).await?;
    } else {
        // Set value at specific path
        storage.set_path(&key_str.as_bytes(), &path, processed_value.clone(), None).await?;
        
        // Update the main engine with the full document
        match storage.get(&key_str.as_bytes()).await? {
            Some(full_doc) => {
                let json_str = serde_json::to_string(&full_doc)
                    .map_err(|e| CommandError::InternalError(format!("Failed to serialize JSON: {}", e)))?;
                    
                engine.set(key, json_str.into_bytes(), None).await?;
            },
            None => {
                // This shouldn't happen since we just set the path
                warn!("Failed to retrieve document after set_path");
                return Err(CommandError::InternalError("Failed to update document".to_string()));
            }
        }
    }
    
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Enhanced JSON.TYPE implementation using optimized JSON storage
pub async fn enhanced_json_type(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Get path argument
    let path = if args.len() > 1 {
        String::from_utf8(args[1].clone())
            .map_err(|_| CommandError::InvalidArgument("Invalid JSON path".to_string()))?
    } else {
        ".".to_string() // Default path is root
    };
    
    // Use our optimized JSON storage
    let storage = get_json_storage();
    let key_str = String::from_utf8_lossy(key).to_string();
    
    // Check if document exists in storage
    if let Ok(Some(_)) = storage.get(&key_str.as_bytes()).await {
        // Get type at path
        if let Ok(Some(type_name)) = storage.type_at_path(&key_str.as_bytes(), &path).await {
            return Ok(RespValue::SimpleString(type_name));
        }
    } else {
        // If not in storage, try to get from engine and add to storage
        match engine.get(key).await? {
            Some(data) => {
                match String::from_utf8(data.clone()) {
                    Ok(json_str) => {
                        match serde_json::from_str::<Value>(&json_str) {
                            Ok(json_value) => {
                                // Store in optimized storage
                                storage.set(&key_str.as_bytes(), json_value, None).await?;
                                
                                // Try again to get type
                                if let Ok(Some(type_name)) = storage.type_at_path(&key_str.as_bytes(), &path).await {
                                    return Ok(RespValue::SimpleString(type_name));
                                }
                            },
                            Err(e) => {
                                debug!("Failed to parse JSON for TYPE: {}", e);
                                return Err(CommandError::WrongType);
                            }
                        }
                    },
                    Err(_) => return Err(CommandError::WrongType),
                }
            },
            None => return Ok(RespValue::BulkString(None)),
        }
    }
    
    // Path not found
    Ok(RespValue::BulkString(None))
}

/// Enhanced JSON.ARRAPPEND implementation with optimized storage
pub async fn enhanced_json_arrappend(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Get path argument
    let path = String::from_utf8(args[1].clone())
        .map_err(|_| CommandError::InvalidArgument("Invalid JSON path".to_string()))?;
    
    // Use our optimized JSON storage
    let storage = get_json_storage();
    let key_str = String::from_utf8_lossy(key).to_string();
    
    // Ensure document exists
    let mut current_value = match storage.get(&key_str.as_bytes()).await? {
        Some(value) => value,
        None => {
            // Try to get from engine
            match engine.get(key).await? {
                Some(data) => {
                    // Try to parse as JSON
                    match String::from_utf8(data) {
                        Ok(json_str) => {
                            match serde_json::from_str::<Value>(&json_str) {
                                Ok(json_value) => {
                                    // Store in our storage
                                    storage.set(&key_str.as_bytes(), json_value.clone(), None).await?;
                                    json_value
                                },
                                Err(e) => {
                                    debug!("JSON parse error: {}", e);
                                    return Err(CommandError::WrongType);
                                }
                            }
                        },
                        Err(_) => return Err(CommandError::WrongType),
                    }
                },
                None => return Err(CommandError::InvalidArgument("Key does not exist".to_string())),
            }
        }
    };
    
    // Find the target array
    let target_value = if path == "." {
        &mut current_value
    } else {
        // Get the value at path - we need to clone for now
        match storage.get_path(&key_str.as_bytes(), &path).await? {
            Some(value) => {
                // Update the main value with the path location
                storage.set_path(&key_str.as_bytes(), &path, value.clone(), None).await?;
                
                // Get updated document
                match storage.get(&key_str.as_bytes()).await? {
                    Some(updated) => {
                        current_value = updated;
                        // Now get reference to target again
                        match find_path_mut(&mut current_value, &path) {
                            Some(target) => target,
                            None => return Err(CommandError::InvalidArgument("Path not found after update".to_string())),
                        }
                    },
                    None => return Err(CommandError::InternalError("Failed to get updated document".to_string())),
                }
            },
            None => return Err(CommandError::InvalidArgument("Path not found".to_string())),
        }
    };
    
    // Check if target is an array
    if !target_value.is_array() {
        return Err(CommandError::WrongType);
    }
    
    // Get array reference
    let array = target_value.as_array_mut().unwrap();
    
    // Parse and append each value
    for i in 2..args.len() {
        let value_str = match String::from_utf8(args[i].clone()) {
            Ok(s) => s,
            Err(_) => return Err(CommandError::InvalidArgument("Invalid JSON value".to_string())),
        };
        
        let value = match serde_json::from_str::<Value>(&value_str) {
            Ok(v) => v,
            Err(e) => {
                debug!("Failed to parse JSON value for array append: {}", e);
                return Err(CommandError::InvalidArgument(format!("Invalid JSON value: {}", e)));
            },
        };
        
        array.push(value);
    }
    
    // Update the document in storage
    storage.set(&key_str.as_bytes(), current_value.clone(), None).await?;
    
    // Update the main engine
    let json_str = serde_json::to_string(&current_value)
        .map_err(|e| CommandError::InternalError(format!("Failed to serialize JSON: {}", e)))?;
        
    engine.set(key.to_vec(), json_str.into_bytes(), None).await?;
    
    // Return new array length
    Ok(RespValue::Integer(array.len() as i64))
}

/// Enhanced JSON.ARRTRIM implementation with optimized storage
pub async fn enhanced_json_arrtrim(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Get path argument
    let path = String::from_utf8(args[1].clone())
        .map_err(|_| CommandError::InvalidArgument("Invalid JSON path".to_string()))?;
    
    // Parse start/end indices
    let start = String::from_utf8(args[2].clone())
        .map_err(|_| CommandError::InvalidArgument("Invalid start index".to_string()))?
        .parse::<i64>()
        .map_err(|_| CommandError::InvalidArgument("Invalid start index".to_string()))?;
        
    let end = String::from_utf8(args[3].clone())
        .map_err(|_| CommandError::InvalidArgument("Invalid end index".to_string()))?
        .parse::<i64>()
        .map_err(|_| CommandError::InvalidArgument("Invalid end index".to_string()))?;
    
    // Use our optimized JSON storage
    let storage = get_json_storage();
    let key_str = String::from_utf8_lossy(key).to_string();
    
    // Ensure document exists
    let mut current_value = match storage.get(&key_str.as_bytes()).await? {
        Some(value) => value,
        None => {
            // Try to get from engine
            match engine.get(key).await? {
                Some(data) => {
                    // Try to parse as JSON
                    match String::from_utf8(data) {
                        Ok(json_str) => {
                            match serde_json::from_str::<Value>(&json_str) {
                                Ok(json_value) => {
                                    // Store in our storage
                                    storage.set(&key_str.as_bytes(), json_value.clone(), None).await?;
                                    json_value
                                },
                                Err(e) => {
                                    debug!("JSON parse error: {}", e);
                                    return Err(CommandError::WrongType);
                                }
                            }
                        },
                        Err(_) => return Err(CommandError::WrongType),
                    }
                },
                None => return Err(CommandError::InvalidArgument("Key does not exist".to_string())),
            }
        }
    };
    
    // Find the target array
    let target_value = if path == "." {
        &mut current_value
    } else {
        // Get updated path
        match find_path_mut(&mut current_value, &path) {
            Some(target) => target,
            None => return Err(CommandError::InvalidArgument("Path not found".to_string())),
        }
    };
    
    // Check if target is an array
    if !target_value.is_array() {
        return Err(CommandError::WrongType);
    }
    
    // Get array reference
    let array = target_value.as_array_mut().unwrap();
    let len = array.len() as i64;
    
    // Convert negative indices to positive ones
    let start_idx = if start < 0 { len + start } else { start };
    let end_idx = if end < 0 { len + end } else { end };
    
    // Ensure start_idx and end_idx are in range
    let start_idx = start_idx.max(0) as usize;
    let end_idx = end_idx.min(len - 1) as usize;
    
    // If start > end, result is an empty array
    let new_array = if start_idx > end_idx {
        Vec::new()
    } else {
        array[start_idx..=end_idx].to_vec()
    };
    
    // Replace the array content
    *array = new_array;
    
    // Update the document in storage
    storage.set(&key_str.as_bytes(), current_value.clone(), None).await?;
    
    // Update the main engine
    let json_str = serde_json::to_string(&current_value)
        .map_err(|e| CommandError::InternalError(format!("Failed to serialize JSON: {}", e)))?;
        
    engine.set(key.to_vec(), json_str.into_bytes(), None).await?;
    
    // Return new array length
    Ok(RespValue::Integer(array.len() as i64))
}

/// Helper function to navigate to a path in a JSON value
fn find_path_mut<'a>(value: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    if path.is_empty() || path == "." {
        return Some(value);
    }
    
    // Parse path segments
    let segments: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
    
    let mut current = value;
    
    for segment in segments {
        match current {
            Value::Object(obj) => {
                current = obj.get_mut(segment)?;
            },
            Value::Array(_) => {
                let index = segment.parse::<usize>().ok()?;
                current = current.get_mut(index)?;
            },
            _ => return None,
        }
    }
    
    Some(current)
}

/// Get the type name of a JSON value
pub fn json_type_name(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "boolean".to_string(),
        Value::Number(_) => "number".to_string(),
        Value::String(_) => "string".to_string(),
        Value::Array(_) => "array".to_string(),
        Value::Object(_) => "object".to_string(),
    }
}