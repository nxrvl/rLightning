use bytes::{Buf, BytesMut};
use serde_json::{Map, Value};
use std::str;
use tracing::{debug, warn};

use crate::command::types::json_config::JsonConfig;
use crate::networking::resp::{RespError, RespValue};

/// Specialized JSON protocol handling for RESP
pub struct JsonResp {
    /// Configuration for JSON handling
    config: JsonConfig,
}

impl JsonResp {
    /// Create a new JSON RESP handler with the given configuration
    pub fn new(config: JsonConfig) -> Self {
        JsonResp { config }
    }
    
    /// Serialize a JSON value to a RESP bulk string
    pub fn serialize_json(&self, value: &Value) -> Result<RespValue, RespError> {
        // Check size limits first
        let estimated_size = self.estimate_json_size(value);
        if estimated_size > self.config.max_document_size_bytes {
            return Err(RespError::ValueTooLarge(format!(
                "JSON document size exceeds limit: estimated {} bytes, max {} bytes",
                estimated_size, self.config.max_document_size_bytes
            )));
        }
        
        // Check nesting depth
        let nesting_depth = self.get_nesting_depth(value);
        if nesting_depth > self.config.max_nesting_depth {
            return Err(RespError::InvalidFormatDetails(format!(
                "JSON nesting depth exceeds limit: {} levels, max {} levels",
                nesting_depth, self.config.max_nesting_depth
            )));
        }
        
        // For objects, check field count
        if let Value::Object(obj) = value {
            if obj.len() > self.config.max_object_fields {
                return Err(RespError::InvalidFormatDetails(format!(
                    "JSON object field count exceeds limit: {} fields, max {} fields",
                    obj.len(), self.config.max_object_fields
                )));
            }
        }
        
        // Convert JSON to string with the proper settings
        let json_string = match serde_json::to_string(value) {
            Ok(s) => s,
            Err(e) => {
                return Err(RespError::InvalidFormatDetails(format!(
                    "Failed to serialize JSON: {}", e
                )));
            }
        };
        
        // Apply sanitization if configured
        let final_data = if self.config.sanitize_control_chars {
            self.sanitize_control_characters(&json_string)
        } else {
            json_string.into_bytes()
        };
        
        // Create RESP bulk string
        Ok(RespValue::BulkString(Some(final_data)))
    }
    
    /// Parse a RESP bulk string into a JSON value
    pub fn parse_json(&self, value: &RespValue) -> Result<Value, RespError> {
        match value {
            RespValue::BulkString(Some(data)) => {
                // Validate UTF-8 if configured
                let json_str = if self.config.validate_utf8 {
                    match str::from_utf8(data) {
                        Ok(s) => s,
                        Err(e) => {
                            return Err(RespError::Utf8Error(e));
                        }
                    }
                } else {
                    // Still validate UTF-8 safely - invalid UTF-8 would cause UB
                    match str::from_utf8(data) {
                        Ok(s) => s,
                        Err(e) => {
                            return Err(RespError::Utf8Error(e));
                        }
                    }
                };
                
                // Parse JSON
                match serde_json::from_str::<Value>(json_str) {
                    Ok(value) => Ok(value),
                    Err(e) => {
                        debug!("JSON parse error: {}", e);
                        Err(RespError::InvalidFormatDetails(format!(
                            "Invalid JSON data: {}", e
                        )))
                    }
                }
            },
            RespValue::BulkString(None) => {
                // Null bulk string becomes null JSON
                Ok(Value::Null)
            },
            _ => {
                Err(RespError::InvalidFormatDetails(
                    "Expected bulk string for JSON data".to_string()
                ))
            }
        }
    }
    
    /// Parse a JSON path string from a RESP value
    pub fn parse_path(&self, value: &RespValue) -> Result<String, RespError> {
        match value {
            RespValue::BulkString(Some(data)) => {
                match str::from_utf8(data) {
                    Ok(s) => Ok(s.to_string()),
                    Err(e) => Err(RespError::Utf8Error(e)),
                }
            },
            RespValue::SimpleString(s) => Ok(s.clone()),
            _ => {
                Err(RespError::InvalidFormatDetails(
                    "Expected string for JSON path".to_string()
                ))
            }
        }
    }
    
    /// Sanitize control characters in a JSON string
    fn sanitize_control_characters(&self, json_str: &str) -> Vec<u8> {
        let mut sanitized = Vec::with_capacity(json_str.len());
        let mut control_chars_count = 0;
        
        // Process each byte
        for &byte in json_str.as_bytes() {
            if byte < 32 && byte != b'\n' && byte != b'\r' && byte != b'\t' {
                sanitized.push(b' '); // Replace with space
                control_chars_count += 1;
            } else {
                sanitized.push(byte);
            }
        }
        
        if control_chars_count > 0 {
            debug!("Sanitized {} control characters in JSON", control_chars_count);
        }
        
        sanitized
    }
    
    /// Estimate the size of a JSON value in bytes
    fn estimate_json_size(&self, value: &Value) -> usize {
        match value {
            Value::Null => 4, // "null"
            Value::Bool(b) => if *b { 4 } else { 5 }, // "true" or "false"
            Value::Number(n) => n.to_string().len(),
            Value::String(s) => s.len() + 2, // quotes
            Value::Array(arr) => {
                // "[" + items + commas + "]"
                let mut size = 2; // brackets
                size += arr.len().saturating_sub(1); // commas
                
                // Add size of each item
                for item in arr {
                    size += self.estimate_json_size(item);
                }
                
                size
            },
            Value::Object(obj) => {
                // "{" + keys + values + colons + commas + "}"
                let mut size = 2; // braces
                size += obj.len().saturating_sub(1); // commas
                size += obj.len(); // colons
                
                // Add size of each key-value pair
                for (key, value) in obj {
                    size += key.len() + 2; // key + quotes
                    size += self.estimate_json_size(value);
                }
                
                size
            }
        }
    }
    
    /// Get the maximum nesting depth of a JSON value
    fn get_nesting_depth(&self, value: &Value) -> usize {
        match value {
            Value::Array(arr) => {
                if arr.is_empty() {
                    1
                } else {
                    // Find maximum depth of any array element + 1
                    1 + arr.iter()
                        .map(|item| self.get_nesting_depth(item))
                        .max()
                        .unwrap_or(0)
                }
            },
            Value::Object(obj) => {
                if obj.is_empty() {
                    1
                } else {
                    // Find maximum depth of any object value + 1
                    1 + obj.values()
                        .map(|item| self.get_nesting_depth(item))
                        .max()
                        .unwrap_or(0)
                }
            },
            // All other types have depth 1
            _ => 1,
        }
    }
}

/// Extension traits for RESP protocol handling with JSON
pub trait RespJsonExt {
    /// Convert a RESP value to a JSON value
    fn to_json(&self, config: &JsonConfig) -> Result<Value, RespError>;
    
    /// Convert a JSON value to a RESP value
    fn from_json(value: &Value, config: &JsonConfig) -> Result<Self, RespError> where Self: Sized;
}

impl RespJsonExt for RespValue {
    fn to_json(&self, config: &JsonConfig) -> Result<Value, RespError> {
        let handler = JsonResp::new(config.clone());
        handler.parse_json(self)
    }
    
    fn from_json(value: &Value, config: &JsonConfig) -> Result<Self, RespError> {
        let handler = JsonResp::new(config.clone());
        handler.serialize_json(value)
    }
}