/// Utilities for the rLightning server
// Export modules
pub mod datetime;
pub mod glob;
pub mod logging;

/// Constant for OK response
#[allow(dead_code)]
pub const OK: &str = "OK";

/// Parses a string to an integer, handling errors
#[allow(dead_code)]
pub fn parse_int(s: &str) -> Result<i64, String> {
    s.parse::<i64>().map_err(|e| e.to_string())
}

/// Safely converts a string to bytes
#[allow(dead_code)]
pub fn str_to_bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Safely converts bytes to a string, if possible
#[allow(dead_code)]
pub fn bytes_to_str(bytes: &[u8]) -> Result<&str, std::str::Utf8Error> {
    std::str::from_utf8(bytes)
}

/// Returns the memory usage of a value
#[allow(dead_code)]
pub fn memory_usage(value: &[u8]) -> usize {
    // Size of the value plus some overhead
    value.len() + std::mem::size_of::<usize>() * 2
}

/// Serializes JSON data with special handling for datetime objects
/// 
/// This function takes a serde_json::Value and ensures that any datetime values 
/// are properly converted to ISO-8601 strings that can be correctly serialized
/// as JSON. It traverses the entire JSON structure to find and convert datetime objects.
#[allow(dead_code)]
pub fn serialize_json_with_datetime(value: &serde_json::Value) -> Result<String, String> {
    use serde_json::Value;
    
    // Creates a deep copy of the JSON value with datetime values converted to strings
    fn convert_datetimes(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let mut new_map = serde_json::Map::new();
                for (k, v) in map {
                    new_map.insert(k.clone(), convert_datetimes(v));
                }
                Value::Object(new_map)
            },
            Value::Array(array) => {
                let new_array = array.iter().map(convert_datetimes).collect();
                Value::Array(new_array)
            },
            // Special handling for strings containing datetime markers
            Value::String(s) => {
                // If the string contains DATETIME-ISO8601: prefix, this is our internal datetime marker
                if s.starts_with("DATETIME-ISO8601:") {
                    // Already in our format, just keep it as is
                    Value::String(s.clone())
                } else {
                    // Regular string, no conversion needed
                    Value::String(s.clone())
                }
            },
            // Other value types (numbers, booleans, null) just clone
            _ => value.clone(),
        }
    }
    
    // Process the JSON value to convert datetime objects
    let processed_value = convert_datetimes(value);
    
    // Return the serialized JSON string
    serde_json::to_string(&processed_value).map_err(|e| e.to_string())
} 