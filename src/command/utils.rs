use std::str;
use std::time::Duration;

use crate::command::CommandError;

/// Helper to convert Vec<u8> to String
pub fn bytes_to_string(bytes: &[u8]) -> Result<String, CommandError> {
    str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|_| CommandError::InvalidArgument("Not valid UTF-8".to_string()))
}

/// Helper to parse a TTL (time-to-live) in seconds
pub fn parse_ttl(bytes: &[u8]) -> Result<Option<Duration>, CommandError> {
    let ttl_str = bytes_to_string(bytes)?;
    
    let ttl_seconds = ttl_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument(format!("ERR value is not an integer or out of range: '{}'", ttl_str))
    })?;
    
    if ttl_seconds <= 0 {
        // Zero or negative TTL means delete the key immediately
        return Ok(None);
    }
    
    Ok(Some(Duration::from_secs(ttl_seconds as u64)))
} 