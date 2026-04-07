use bytes::{Buf, Bytes, BytesMut};
use memchr::memchr_iter;
use std::str;
use thiserror::Error;
use tracing::{debug, warn};

// ── Static interned responses ────────────────────────────────────────────────
// Pre-serialized RESP byte sequences for the most common responses.
// Using Bytes::from_static avoids allocation entirely.

/// Pre-serialized "+OK\r\n"
pub static OK_RESPONSE: Bytes = Bytes::from_static(b"+OK\r\n");
/// Pre-serialized "+PONG\r\n"
pub static PONG_RESPONSE: Bytes = Bytes::from_static(b"+PONG\r\n");
/// Pre-serialized ":0\r\n"
pub static ZERO_RESPONSE: Bytes = Bytes::from_static(b":0\r\n");
/// Pre-serialized ":1\r\n"
pub static ONE_RESPONSE: Bytes = Bytes::from_static(b":1\r\n");
/// Pre-serialized "$-1\r\n"
pub static NIL_RESPONSE: Bytes = Bytes::from_static(b"$-1\r\n");
/// Pre-serialized "*0\r\n"
pub static EMPTY_ARRAY: Bytes = Bytes::from_static(b"*0\r\n");

/// Possible errors during RESP parsing
#[derive(Debug, Error)]
pub enum RespError {
    #[error("Invalid RESP data format")]
    InvalidFormat,
    #[error("Invalid RESP data format: {0}")]
    InvalidFormatDetails(String),
    #[error("Incomplete RESP data")]
    #[allow(dead_code)]
    Incomplete,
    #[error("Integer parsing error: {0}")]
    IntegerParseError(#[from] std::num::ParseIntError),
    #[error("UTF-8 decoding error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("Value too large: {0}")]
    ValueTooLarge(String),
}

/// RESP protocol version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    #[default]
    RESP2,
    RESP3,
}

/// RESP protocol data types (supports both RESP2 and RESP3)
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    // RESP2 types
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
    // RESP3 types
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(String),
    BulkError(String),
    VerbatimString { encoding: String, data: String },
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
    Attribute(Vec<(RespValue, RespValue)>),
    Push(Vec<RespValue>),
}

/// Command representation for AOF persistence
#[derive(Debug, Clone)]
pub struct RespCommand {
    /// Command name
    pub name: Vec<u8>,
    /// Command arguments
    pub args: Vec<Vec<u8>>,
}

impl RespValue {
    /// Parses a complete RESP message from a byte buffer
    /// CRITICAL FIX: This function should ONLY be called when expecting RESP commands,
    /// never when reading stored data that might contain command-like patterns
    pub fn parse(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if buffer.is_empty() {
            return Ok(None);
        }

        // SECURITY CHECK: Ensure we're only parsing actual RESP protocol data
        // This prevents stored data from being interpreted as commands
        let first_byte = buffer[0] as char;
        match first_byte {
            // RESP2 types
            '+' => parse_simple_string(buffer),
            '-' => parse_error(buffer),
            ':' => parse_integer(buffer),
            '$' => parse_bulk_string(buffer),
            '*' => parse_array(buffer),
            // RESP3 types
            '_' => parse_null(buffer),
            '#' => parse_boolean(buffer),
            ',' => parse_double(buffer),
            '(' => parse_big_number(buffer),
            '!' => parse_bulk_error(buffer),
            '=' => parse_verbatim_string(buffer),
            '%' => parse_map(buffer),
            '~' => parse_set(buffer),
            '|' => parse_attribute(buffer),
            '>' => parse_push(buffer),
            _ => {
                // Not a RESP type marker — try to parse as an inline command
                // (plain text like "PING\r\n" or "SET foo bar\r\n")
                parse_inline_command(buffer)
            }
        }
    }

    /// Try to return a pre-serialized static response (zero allocation).
    /// Returns None if this value doesn't match a known static response.
    #[inline]
    pub fn try_static_response(&self) -> Option<&'static Bytes> {
        match self {
            RespValue::SimpleString(s) if s == "OK" => Some(&OK_RESPONSE),
            RespValue::SimpleString(s) if s == "PONG" => Some(&PONG_RESPONSE),
            RespValue::Integer(0) => Some(&ZERO_RESPONSE),
            RespValue::Integer(1) => Some(&ONE_RESPONSE),
            RespValue::BulkString(None) => Some(&NIL_RESPONSE),
            RespValue::Array(Some(arr)) if arr.is_empty() => Some(&EMPTY_ARRAY),
            _ => None,
        }
    }

    /// Serializes the RespValue to a vector of bytes
    pub fn serialize(&self) -> Result<Vec<u8>, RespError> {
        // Fast path: static interned responses (avoids format/serialize overhead)
        if let Some(static_resp) = self.try_static_response() {
            return Ok(static_resp.to_vec());
        }

        // Calculate an approximate initial capacity to avoid reallocations
        let estimated_size = match self {
            RespValue::SimpleString(s) => s.len() + 3,
            RespValue::Error(s) => s.len() + 3,
            RespValue::Integer(_) => 24, // itoa max i64 is 20 digits + prefix + crlf
            RespValue::BulkString(None) => 5,
            RespValue::BulkString(Some(data)) => {
                let len_str = data.len().to_string();
                len_str.len() + data.len() + 5
            }
            RespValue::Array(None) => 5,
            RespValue::Array(Some(items)) => {
                let header_size = items.len().to_string().len() + 3;
                const SMALL_ARRAY_THRESHOLD: usize = 10;
                const AVG_SMALL_ITEM_SIZE: usize = 64;
                const AVG_LARGE_ITEM_SIZE: usize = 128;
                if items.len() < SMALL_ARRAY_THRESHOLD {
                    header_size + items.len() * AVG_SMALL_ITEM_SIZE
                } else {
                    header_size + items.len() * AVG_LARGE_ITEM_SIZE
                }
            }
            // RESP3 types
            RespValue::Null => 3,
            RespValue::Boolean(_) => 4,
            RespValue::Double(d) => d.to_string().len() + 3,
            RespValue::BigNumber(s) => s.len() + 3,
            RespValue::BulkError(s) => s.len().to_string().len() + s.len() + 5,
            RespValue::VerbatimString { encoding, data } => {
                (data.len() + encoding.len() + 1).to_string().len()
                    + encoding.len()
                    + 1
                    + data.len()
                    + 5
            }
            RespValue::Map(pairs) => pairs.len().to_string().len() + 3 + pairs.len() * 128,
            RespValue::Set(items) => items.len().to_string().len() + 3 + items.len() * 64,
            RespValue::Attribute(pairs) => pairs.len().to_string().len() + 3 + pairs.len() * 128,
            RespValue::Push(items) => items.len().to_string().len() + 3 + items.len() * 64,
        };

        let mut result = Vec::with_capacity(estimated_size);

        match self {
            RespValue::SimpleString(s) => {
                result.push(b'+');
                result.extend_from_slice(s.as_bytes());
                result.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                // Format Redis errors with standard prefix if needed
                // Redis error types: the first word before a space is the error type
                // Known error types that must NOT get an ERR prefix
                const KNOWN_PREFIXES: &[&str] = &[
                    "ERR ",
                    "WRONGTYPE ",
                    "NOPERM ",
                    "NOAUTH ",
                    "WRONGPASS ",
                    "READONLY ",
                    "EXECABORT ",
                    "NOSCRIPT ",
                    "LOADING ",
                    "BUSY ",
                    "NOPROTO ",
                    "CLUSTERDOWN ",
                    "CROSSSLOT ",
                    "MOVED ",
                    "ASK ",
                    "NOTBUSY ",
                    "MASTERDOWN ",
                    "NOREPLICAS ",
                    "UNKILLABLE ",
                ];
                let error_str =
                    if KNOWN_PREFIXES.iter().any(|p| s.starts_with(p)) || s.contains(':') {
                        // Already has a standard prefix or is a categorized error (e.g. "OOM:message")
                        s.clone()
                    } else {
                        // Add the standard ERR prefix
                        format!("ERR {}", s)
                    };

                result.push(b'-');
                result.extend_from_slice(error_str.as_bytes());
                result.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(i) => {
                result.push(b':');
                let mut itoa_buf = itoa::Buffer::new();
                result.extend_from_slice(itoa_buf.format(*i).as_bytes());
                result.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                result.extend_from_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(data)) => {
                // Check that data size is reasonable
                if data.len() > 512 * 1024 * 1024 {
                    // Increase to 512MB max (Redis default)
                    return Err(RespError::ValueTooLarge(format!(
                        "Bulk string size exceeds 512MB limit: {} bytes",
                        data.len()
                    )));
                }

                // CRITICAL FIX: Never modify user data - RESP protocol must transport data as-is
                // The previous sanitization code was corrupting data by replacing control characters
                // RESP protocol is binary-safe and should handle any data correctly

                // Format according to RESP protocol: $<length>\r\n<data>\r\n
                result.push(b'$');
                let mut itoa_buf = itoa::Buffer::new();
                result.extend_from_slice(itoa_buf.format(data.len()).as_bytes());
                result.extend_from_slice(b"\r\n");
                result.extend_from_slice(data);
                result.extend_from_slice(b"\r\n");
            }
            RespValue::Array(None) => {
                result.extend_from_slice(b"*-1\r\n");
            }
            RespValue::Array(Some(items)) => {
                // Check array size
                if items.len() > 1_000_000 {
                    return Err(RespError::ValueTooLarge(format!(
                        "Array length exceeds 1,000,000 limit: {}",
                        items.len()
                    )));
                }

                // Write array header
                result.push(b'*');
                let mut itoa_buf = itoa::Buffer::new();
                result.extend_from_slice(itoa_buf.format(items.len()).as_bytes());
                result.extend_from_slice(b"\r\n");

                // Serialize and write each element (single pass)
                let mut total_size = 0;
                for item in items {
                    let item_bytes = item.serialize()?;
                    total_size += item_bytes.len();
                    if total_size > 1024 * 1024 * 512 {
                        // 512 MB max
                        return Err(RespError::ValueTooLarge(format!(
                            "Total serialized array size exceeds 512MB limit: {} bytes",
                            total_size
                        )));
                    }
                    result.extend_from_slice(&item_bytes);
                }
            }
            // RESP3 types
            RespValue::Null => {
                result.extend_from_slice(b"_\r\n");
            }
            RespValue::Boolean(b) => {
                result.push(b'#');
                result.push(if *b { b't' } else { b'f' });
                result.extend_from_slice(b"\r\n");
            }
            RespValue::Double(d) => {
                result.push(b',');
                if d.is_infinite() {
                    if d.is_sign_positive() {
                        result.extend_from_slice(b"inf");
                    } else {
                        result.extend_from_slice(b"-inf");
                    }
                } else if d.is_nan() {
                    result.extend_from_slice(b"nan");
                } else {
                    result.extend_from_slice(d.to_string().as_bytes());
                }
                result.extend_from_slice(b"\r\n");
            }
            RespValue::BigNumber(s) => {
                result.push(b'(');
                result.extend_from_slice(s.as_bytes());
                result.extend_from_slice(b"\r\n");
            }
            RespValue::BulkError(s) => {
                result.push(b'!');
                let mut itoa_buf = itoa::Buffer::new();
                result.extend_from_slice(itoa_buf.format(s.len()).as_bytes());
                result.extend_from_slice(b"\r\n");
                result.extend_from_slice(s.as_bytes());
                result.extend_from_slice(b"\r\n");
            }
            RespValue::VerbatimString { encoding, data } => {
                let total_len = encoding.len() + 1 + data.len(); // encoding + ':' + data
                result.push(b'=');
                let mut itoa_buf = itoa::Buffer::new();
                result.extend_from_slice(itoa_buf.format(total_len).as_bytes());
                result.extend_from_slice(b"\r\n");
                result.extend_from_slice(encoding.as_bytes());
                result.push(b':');
                result.extend_from_slice(data.as_bytes());
                result.extend_from_slice(b"\r\n");
            }
            RespValue::Map(pairs) => {
                result.push(b'%');
                let mut itoa_buf = itoa::Buffer::new();
                result.extend_from_slice(itoa_buf.format(pairs.len()).as_bytes());
                result.extend_from_slice(b"\r\n");
                for (key, value) in pairs {
                    result.extend_from_slice(&key.serialize()?);
                    result.extend_from_slice(&value.serialize()?);
                }
            }
            RespValue::Set(items) => {
                result.push(b'~');
                let mut itoa_buf = itoa::Buffer::new();
                result.extend_from_slice(itoa_buf.format(items.len()).as_bytes());
                result.extend_from_slice(b"\r\n");
                for item in items {
                    result.extend_from_slice(&item.serialize()?);
                }
            }
            RespValue::Attribute(pairs) => {
                result.push(b'|');
                let mut itoa_buf = itoa::Buffer::new();
                result.extend_from_slice(itoa_buf.format(pairs.len()).as_bytes());
                result.extend_from_slice(b"\r\n");
                for (key, value) in pairs {
                    result.extend_from_slice(&key.serialize()?);
                    result.extend_from_slice(&value.serialize()?);
                }
            }
            RespValue::Push(items) => {
                result.push(b'>');
                let mut itoa_buf = itoa::Buffer::new();
                result.extend_from_slice(itoa_buf.format(items.len()).as_bytes());
                result.extend_from_slice(b"\r\n");
                for item in items {
                    result.extend_from_slice(&item.serialize()?);
                }
            }
        }

        Ok(result)
    }

    /// Convert a RESP2-style response to RESP3 format based on the command name.
    /// This is the central RESP3 adaptation point: command handlers always produce RESP2
    /// values, and this method upgrades them for RESP3 clients.
    ///
    /// For subcommand-based commands, `command_name` should be the compound name
    /// (e.g. "xinfo stream", "command docs") built by `Server::build_resp3_command_name`.
    pub fn convert_for_resp3(self, command_name: &str) -> RespValue {
        match self {
            // Null conversions: RESP2 null bulk string/array -> RESP3 Null
            RespValue::BulkString(None) => RespValue::Null,
            RespValue::Array(None) => RespValue::Null,

            // Commands that return float values as bulk strings -> RESP3 Double
            RespValue::BulkString(Some(ref data)) if Self::is_double_command(command_name) => {
                if let Ok(s) = std::str::from_utf8(data) {
                    if let Ok(d) = s.parse::<f64>() {
                        return RespValue::Double(d);
                    }
                    // Handle special float strings
                    match s {
                        "inf" => return RespValue::Double(f64::INFINITY),
                        "-inf" => return RespValue::Double(f64::NEG_INFINITY),
                        _ => {}
                    }
                }
                self
            }

            // Commands that return key-value flat arrays -> RESP3 Map
            RespValue::Array(Some(items))
                if Self::is_map_command(command_name) && items.len() % 2 == 0 =>
            {
                Self::flat_array_to_map_vec(items)
            }

            // Commands that return member arrays -> RESP3 Set
            RespValue::Array(Some(items)) if Self::is_set_command(command_name) => {
                RespValue::Set(items)
            }

            // Commands that return array of maps (each inner array -> map):
            // XINFO GROUPS, XINFO CONSUMERS
            RespValue::Array(Some(items)) if Self::is_array_of_maps_command(command_name) => {
                let converted: Vec<RespValue> =
                    items.into_iter().map(Self::flat_array_to_map).collect();
                RespValue::Array(Some(converted))
            }

            // HSCAN/ZSCAN: [cursor, flat-kv-array] -> [cursor, map]
            RespValue::Array(Some(items))
                if Self::is_scan_map_command(command_name) && items.len() == 2 =>
            {
                let mut items = items;
                let data = items.pop().unwrap();
                let cursor = items.pop().unwrap();
                RespValue::Array(Some(vec![cursor, Self::flat_array_to_map(data)]))
            }

            // SSCAN: [cursor, array] -> [cursor, set]
            RespValue::Array(Some(items)) if command_name == "sscan" && items.len() == 2 => {
                let mut items = items;
                let data = items.pop().unwrap();
                let cursor = items.pop().unwrap();
                let set_val = if let RespValue::Array(Some(inner)) = data {
                    RespValue::Set(inner)
                } else {
                    data
                };
                RespValue::Array(Some(vec![cursor, set_val]))
            }

            // XRANGE/XREVRANGE: convert each stream entry's field-values to Map
            // [[id, [f1, v1, f2, v2]], ...] -> [[id, Map{f1:v1, f2:v2}], ...]
            RespValue::Array(Some(items)) if Self::is_stream_entry_command(command_name) => {
                let converted: Vec<RespValue> =
                    items.into_iter().map(Self::convert_stream_entry).collect();
                RespValue::Array(Some(converted))
            }

            // XREAD/XREADGROUP: outer array of [name, entries] pairs -> Map,
            // plus convert each entry's field-values to Map
            // [[name, [[id, [f,v,...]], ...]], ...] -> Map{name: [[id, Map], ...]}
            RespValue::Array(Some(items)) if Self::is_stream_read_command(command_name) => {
                let mut pairs = Vec::with_capacity(items.len());
                for item in items {
                    if let RespValue::Array(Some(mut pair)) = item {
                        if pair.len() == 2 {
                            let entries = pair.pop().unwrap();
                            let stream_name = pair.pop().unwrap();
                            let converted_entries =
                                if let RespValue::Array(Some(entry_items)) = entries {
                                    RespValue::Array(Some(
                                        entry_items
                                            .into_iter()
                                            .map(Self::convert_stream_entry)
                                            .collect(),
                                    ))
                                } else {
                                    entries
                                };
                            pairs.push((stream_name, converted_entries));
                        } else {
                            // Unexpected structure, keep as-is
                            pairs.push((RespValue::Array(Some(pair)), RespValue::Null));
                        }
                    } else {
                        pairs.push((item, RespValue::Null));
                    }
                }
                RespValue::Map(pairs)
            }

            // Note: EXEC inner sub-result conversion is handled in server.rs
            // where the queued command names are available.

            // Everything else passes through unchanged
            other => other,
        }
    }

    /// Convert a flat array of alternating key-value pairs to a Map.
    /// If the value is not an even-length array, return it unchanged.
    fn flat_array_to_map(value: RespValue) -> RespValue {
        if let RespValue::Array(Some(inner)) = value {
            if inner.len() % 2 == 0 {
                Self::flat_array_to_map_vec(inner)
            } else {
                RespValue::Array(Some(inner))
            }
        } else {
            value
        }
    }

    /// Convert a Vec of alternating key-value items into a Map.
    fn flat_array_to_map_vec(items: Vec<RespValue>) -> RespValue {
        let mut pairs = Vec::with_capacity(items.len() / 2);
        let mut iter = items.into_iter();
        while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
            pairs.push((k, v));
        }
        RespValue::Map(pairs)
    }

    /// Convert a stream entry [id, [field, value, ...]] to [id, Map{field: value, ...}]
    fn convert_stream_entry(entry: RespValue) -> RespValue {
        if let RespValue::Array(Some(mut items)) = entry {
            if items.len() == 2 {
                let fields = items.pop().unwrap();
                let id = items.pop().unwrap();
                RespValue::Array(Some(vec![id, Self::flat_array_to_map(fields)]))
            } else {
                RespValue::Array(Some(items))
            }
        } else {
            entry
        }
    }

    /// Check if a command returns key-value pairs that should be a RESP3 Map.
    /// Only commands that always return flat key-value pair responses belong here.
    fn is_map_command(cmd: &str) -> bool {
        matches!(
            cmd,
            "hgetall" | "config get" | "xinfo stream" | "command docs"
        )
    }

    /// Check if a command returns an array where each element is a flat kv array -> Map
    fn is_array_of_maps_command(cmd: &str) -> bool {
        matches!(cmd, "xinfo groups" | "xinfo consumers")
    }

    /// Check if a command returns [cursor, flat-kv-array] where the data part -> Map
    fn is_scan_map_command(cmd: &str) -> bool {
        matches!(cmd, "hscan" | "zscan")
    }

    /// Check if a command returns stream entries that need field-value -> Map conversion
    fn is_stream_entry_command(cmd: &str) -> bool {
        matches!(cmd, "xrange" | "xrevrange")
    }

    /// Check if a command returns stream read results (outer array -> Map, entries -> Map)
    fn is_stream_read_command(cmd: &str) -> bool {
        matches!(cmd, "xread" | "xreadgroup")
    }

    /// Check if a command returns a set of members that should be a RESP3 Set
    fn is_set_command(cmd: &str) -> bool {
        matches!(cmd, "smembers" | "sinter" | "sunion" | "sdiff")
    }

    /// Check if a command returns a float value that should be a RESP3 Double
    fn is_double_command(cmd: &str) -> bool {
        matches!(
            cmd,
            "zscore" | "zincrby" | "geodist" | "incrbyfloat" | "hincrbyfloat"
        )
    }

    /// Convert a pub/sub message from RESP2 Array to RESP3 Push type
    pub fn convert_to_push(self) -> RespValue {
        match self {
            RespValue::Array(Some(items)) => RespValue::Push(items),
            other => other,
        }
    }
}

/// Optimized read_line using memchr for efficient CRLF scanning
fn read_line(buffer: &mut BytesMut) -> Result<Option<String>, RespError> {
    if buffer.len() < 2 {
        return Ok(None);
    }

    // Use memchr to efficiently find CR position
    let cr_positions = memchr_iter(b'\r', buffer);

    for pos in cr_positions {
        // Make sure we have at least one byte after CR
        if pos + 1 < buffer.len() && buffer[pos + 1] == b'\n' {
            // Extract the line, skipping the type marker character at buffer[0]
            if pos < 1 {
                return Err(RespError::InvalidFormat);
            }

            let line = str::from_utf8(&buffer[1..pos])?.to_string();

            // Consume the entire line including CRLF
            buffer.advance(pos + 2);

            return Ok(Some(line));
        }
    }

    // If we didn't find a CRLF, the data is incomplete
    Ok(None)
}

/// REMOVED: This recovery function was causing data corruption by treating
/// stored data as RESP commands. The fix is to NOT parse stored data at all.
/// FIXED: Proper bulk string parsing without data corruption
fn parse_bulk_string(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // CRITICAL FIX: We need to check if we have all the data BEFORE consuming the length line
    // First, peek at the length line without consuming it
    if buffer.len() < 2 {
        return Ok(None);
    }

    // Find the CRLF for the length line
    let cr_positions = memchr_iter(b'\r', buffer);
    let mut length_line_end = None;
    for pos in cr_positions {
        if pos + 1 < buffer.len() && buffer[pos + 1] == b'\n' {
            length_line_end = Some(pos);
            break;
        }
    }

    if length_line_end.is_none() {
        return Ok(None); // Don't have complete length line yet
    }

    let length_line_end = length_line_end.unwrap();

    // Parse the length WITHOUT consuming from buffer
    if length_line_end < 1 {
        return Err(RespError::InvalidFormat);
    }

    let length_str = str::from_utf8(&buffer[1..length_line_end])?;
    let length = length_str.parse::<i64>()?;

    if length == -1 {
        // Null bulk string - consume the length line and return
        buffer.advance(length_line_end + 2);
        return Ok(Some(RespValue::BulkString(None)));
    }

    if length < 0 {
        return Err(RespError::InvalidFormatDetails(format!(
            "Negative bulk string length: {}",
            length
        )));
    }

    let length = length as usize;

    // Validate data size limit BEFORE checking buffer completeness
    // to prevent a malicious client from forcing large buffer allocations
    if length > 512 * 1024 * 1024 {
        // 512MB limit
        return Err(RespError::ValueTooLarge(format!(
            "Bulk string exceeds 512MB limit: {} bytes",
            length
        )));
    }

    // Now check if we have ALL the data we need:
    // length_line + CRLF + data + CRLF
    let total_needed = length_line_end + 2 + length + 2;
    if buffer.len() < total_needed {
        // Don't have all the data yet - return without modifying buffer
        return Ok(None);
    }

    // Log large operations for debugging
    if length > 1024 * 1024 {
        // 1MB+
        debug!("Parsing large bulk string: {} MB", length / (1024 * 1024));
    }

    // Now we know we have all the data - consume the length line
    buffer.advance(length_line_end + 2);

    // Extract data using zero-copy split
    let data = buffer.split_to(length).to_vec();

    // CRITICAL FIX: Strict CRLF validation without "recovery" that corrupts data
    // We already checked we have these bytes above
    if buffer[0] != b'\r' || buffer[1] != b'\n' {
        let actual = format!("{:?}", &buffer[0..2.min(buffer.len())]);
        return Err(RespError::InvalidFormatDetails(format!(
            "Invalid CRLF after bulk string: expected [13, 10], got {}",
            actual
        )));
    }

    // Consume the CRLF
    buffer.advance(2);

    Ok(Some(RespValue::BulkString(Some(data))))
}

/// Calculate the size of a complete RESP value without consuming the buffer
/// Returns None if data is incomplete, Some(size) if complete
fn calculate_resp_size(buffer: &[u8]) -> Option<usize> {
    if buffer.is_empty() {
        return None;
    }

    match buffer[0] {
        // Simple line types: simple string, error, integer, boolean, double, big number, null
        b'+' | b'-' | b':' | b'#' | b',' | b'(' => {
            for i in 1..buffer.len() {
                if i + 1 < buffer.len() && buffer[i] == b'\r' && buffer[i + 1] == b'\n' {
                    return Some(i + 2);
                }
            }
            None
        }
        b'_' => {
            // Null is always exactly "_\r\n"
            if buffer.len() >= 3 && buffer[1] == b'\r' && buffer[2] == b'\n' {
                Some(3)
            } else {
                None
            }
        }
        // Bulk types: bulk string, bulk error, verbatim string
        b'$' | b'!' | b'=' => {
            let mut crlf_pos = None;
            for i in 1..buffer.len() {
                if i + 1 < buffer.len() && buffer[i] == b'\r' && buffer[i + 1] == b'\n' {
                    crlf_pos = Some(i);
                    break;
                }
            }

            let crlf_pos = crlf_pos?;
            let length_str = std::str::from_utf8(&buffer[1..crlf_pos]).ok()?;
            let length = length_str.parse::<i64>().ok()?;

            if length < 0 {
                // -1 is null bulk string (valid), any other negative is invalid.
                // Return header size so the parser advances and the actual parse
                // function can return a proper error for invalid negative lengths.
                return Some(crlf_pos + 2);
            }

            // Reject absurdly large bulk strings (Redis limit is 512 MB)
            // to prevent usize overflow on 32-bit targets
            if length > 512 * 1024 * 1024 {
                return Some(crlf_pos + 2);
            }

            let total_size = crlf_pos + 2 + length as usize + 2;
            if buffer.len() >= total_size {
                Some(total_size)
            } else {
                None
            }
        }
        // Aggregate types with elements: array, set, push
        b'*' | b'~' | b'>' => {
            let mut crlf_pos = None;
            for i in 1..buffer.len() {
                if i + 1 < buffer.len() && buffer[i] == b'\r' && buffer[i + 1] == b'\n' {
                    crlf_pos = Some(i);
                    break;
                }
            }

            let crlf_pos = crlf_pos?;
            let length_str = std::str::from_utf8(&buffer[1..crlf_pos]).ok()?;
            let count = length_str.parse::<i64>().ok()?;

            if count == -1 {
                return Some(crlf_pos + 2);
            }

            if !(0..=1_000_000).contains(&count) {
                return None;
            }

            let mut offset = crlf_pos + 2;
            for _ in 0..count {
                if offset >= buffer.len() {
                    return None;
                }
                let elem_size = calculate_resp_size(&buffer[offset..])?;
                offset += elem_size;
            }

            Some(offset)
        }
        // Map and attribute types: key-value pairs (count = number of pairs, elements = count * 2)
        b'%' | b'|' => {
            let mut crlf_pos = None;
            for i in 1..buffer.len() {
                if i + 1 < buffer.len() && buffer[i] == b'\r' && buffer[i + 1] == b'\n' {
                    crlf_pos = Some(i);
                    break;
                }
            }

            let crlf_pos = crlf_pos?;
            let length_str = std::str::from_utf8(&buffer[1..crlf_pos]).ok()?;
            let count = length_str.parse::<i64>().ok()?;

            if !(0..=500_000).contains(&count) {
                return None;
            }

            let mut offset = crlf_pos + 2;
            // Each pair has a key and a value
            for _ in 0..count * 2 {
                if offset >= buffer.len() {
                    return None;
                }
                let elem_size = calculate_resp_size(&buffer[offset..])?;
                offset += elem_size;
            }

            Some(offset)
        }
        _ => None,
    }
}

/// Optimized array parsing with capacity pre-allocation
/// CRITICAL FIX: Check that ALL data is available BEFORE consuming any of the buffer
fn parse_array(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // First, check if we have all the data we need WITHOUT consuming the buffer
    let total_size = match calculate_resp_size(buffer) {
        Some(size) => size,
        None => return Ok(None), // Incomplete data, wait for more
    };

    // Now we know we have all data - safe to consume
    let length_str = read_line(buffer)?;
    if length_str.is_none() {
        // This shouldn't happen since we checked above, but handle it anyway
        return Ok(None);
    }

    let length = length_str.unwrap().parse::<i64>()?;
    if length == -1 {
        return Ok(Some(RespValue::Array(None)));
    }

    // Safety check for large arrays
    if length > 1_000_000 {
        return Err(RespError::ValueTooLarge(format!(
            "Array length exceeds 1,000,000 limit: {}",
            length
        )));
    }

    // Log large array parsing
    if length > 10000 {
        debug!(
            "Parsing large array with {} elements, total size {} bytes",
            length, total_size
        );
    }

    // Pre-allocate the vector with the known capacity
    let mut values = Vec::with_capacity(length as usize);

    // Parse each array element - we know all data is available
    for _ in 0..length {
        // Try to parse the next value
        match RespValue::parse(buffer)? {
            Some(value) => values.push(value),
            None => {
                // This shouldn't happen since we pre-calculated the size
                warn!("Unexpected incomplete data in array parsing");
                return Ok(None);
            }
        }
    }

    Ok(Some(RespValue::Array(Some(values))))
}

// Helper functions for parsing different RESP types
fn parse_simple_string(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    let line = read_line(buffer)?;
    if let Some(line) = line {
        Ok(Some(RespValue::SimpleString(line)))
    } else {
        Ok(None)
    }
}

fn parse_error(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    let line = read_line(buffer)?;
    if let Some(line) = line {
        Ok(Some(RespValue::Error(line)))
    } else {
        Ok(None)
    }
}

fn parse_integer(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    let line = read_line(buffer)?;
    if let Some(line) = line {
        let value = line.parse::<i64>()?;
        Ok(Some(RespValue::Integer(value)))
    } else {
        Ok(None)
    }
}

// --- RESP3 parsing functions ---

fn parse_null(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Null is "_\r\n" - exactly 3 bytes
    if buffer.len() < 3 {
        return Ok(None);
    }
    if buffer[1] != b'\r' || buffer[2] != b'\n' {
        return Err(RespError::InvalidFormatDetails(
            "Invalid null format".to_string(),
        ));
    }
    buffer.advance(3);
    Ok(Some(RespValue::Null))
}

fn parse_boolean(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Boolean is "#t\r\n" or "#f\r\n"
    let line = read_line(buffer)?;
    if let Some(line) = line {
        match line.as_str() {
            "t" => Ok(Some(RespValue::Boolean(true))),
            "f" => Ok(Some(RespValue::Boolean(false))),
            _ => Err(RespError::InvalidFormatDetails(format!(
                "Invalid boolean value: {}",
                line
            ))),
        }
    } else {
        Ok(None)
    }
}

fn parse_double(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Double is ",<floating-point-number>\r\n" or ",inf", ",-inf", ",nan"
    let line = read_line(buffer)?;
    if let Some(line) = line {
        let value = match line.as_str() {
            "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            "nan" => f64::NAN,
            _ => line.parse::<f64>().map_err(|e| {
                RespError::InvalidFormatDetails(format!("Invalid double value '{}': {}", line, e))
            })?,
        };
        Ok(Some(RespValue::Double(value)))
    } else {
        Ok(None)
    }
}

fn parse_big_number(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Big number is "(<big-number>\r\n"
    let line = read_line(buffer)?;
    if let Some(line) = line {
        // Validate it looks like a number (digits, optional leading -)
        if line.is_empty() {
            return Err(RespError::InvalidFormatDetails(
                "Empty big number".to_string(),
            ));
        }
        let check = line.strip_prefix('-').unwrap_or(&line);
        if check.is_empty() || !check.chars().all(|c| c.is_ascii_digit()) {
            return Err(RespError::InvalidFormatDetails(format!(
                "Invalid big number: {}",
                line
            )));
        }
        Ok(Some(RespValue::BigNumber(line)))
    } else {
        Ok(None)
    }
}

fn parse_bulk_error(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Bulk error is "!<length>\r\n<error>\r\n" (same structure as bulk string)
    if buffer.len() < 2 {
        return Ok(None);
    }

    let cr_positions = memchr_iter(b'\r', buffer);
    let mut length_line_end = None;
    for pos in cr_positions {
        if pos + 1 < buffer.len() && buffer[pos + 1] == b'\n' {
            length_line_end = Some(pos);
            break;
        }
    }

    let length_line_end = match length_line_end {
        Some(pos) => pos,
        None => return Ok(None),
    };

    let length_str = str::from_utf8(&buffer[1..length_line_end])?;
    let length = length_str.parse::<i64>()?;

    if length < 0 {
        return Err(RespError::InvalidFormatDetails(format!(
            "Negative bulk error length: {}",
            length
        )));
    }

    let length = length as usize;
    let total_needed = length_line_end + 2 + length + 2;
    if buffer.len() < total_needed {
        return Ok(None);
    }

    buffer.advance(length_line_end + 2);
    let data = buffer.split_to(length).to_vec();

    if buffer[0] != b'\r' || buffer[1] != b'\n' {
        return Err(RespError::InvalidFormatDetails(
            "Invalid CRLF after bulk error".to_string(),
        ));
    }
    buffer.advance(2);

    let error_str = String::from_utf8(data).map_err(|e| {
        RespError::InvalidFormatDetails(format!("Invalid UTF-8 in bulk error: {}", e))
    })?;

    Ok(Some(RespValue::BulkError(error_str)))
}

fn parse_verbatim_string(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Verbatim string is "=<length>\r\n<encoding>:<data>\r\n"
    // where encoding is exactly 3 characters
    if buffer.len() < 2 {
        return Ok(None);
    }

    let cr_positions = memchr_iter(b'\r', buffer);
    let mut length_line_end = None;
    for pos in cr_positions {
        if pos + 1 < buffer.len() && buffer[pos + 1] == b'\n' {
            length_line_end = Some(pos);
            break;
        }
    }

    let length_line_end = match length_line_end {
        Some(pos) => pos,
        None => return Ok(None),
    };

    let length_str = str::from_utf8(&buffer[1..length_line_end])?;
    let length = length_str.parse::<i64>()?;

    if length < 4 {
        // Minimum: 3 chars encoding + ':' = 4
        return Err(RespError::InvalidFormatDetails(format!(
            "Verbatim string too short: {}",
            length
        )));
    }

    let length = length as usize;
    let total_needed = length_line_end + 2 + length + 2;
    if buffer.len() < total_needed {
        return Ok(None);
    }

    buffer.advance(length_line_end + 2);
    let data = buffer.split_to(length).to_vec();

    if buffer[0] != b'\r' || buffer[1] != b'\n' {
        return Err(RespError::InvalidFormatDetails(
            "Invalid CRLF after verbatim string".to_string(),
        ));
    }
    buffer.advance(2);

    // The encoding is the first 3 bytes, then ':', then the actual data
    if data.len() < 4 || data[3] != b':' {
        return Err(RespError::InvalidFormatDetails(
            "Invalid verbatim string format: missing encoding:data separator".to_string(),
        ));
    }

    let encoding = String::from_utf8(data[0..3].to_vec()).map_err(|e| {
        RespError::InvalidFormatDetails(format!("Invalid UTF-8 in verbatim encoding: {}", e))
    })?;
    let content = String::from_utf8(data[4..].to_vec()).map_err(|e| {
        RespError::InvalidFormatDetails(format!("Invalid UTF-8 in verbatim data: {}", e))
    })?;

    Ok(Some(RespValue::VerbatimString {
        encoding,
        data: content,
    }))
}

fn parse_map(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Map is "%<number-of-pairs>\r\n<key><value>..."
    let total_size = match calculate_resp_size(buffer) {
        Some(size) => size,
        None => return Ok(None),
    };

    let _ = total_size;
    let length_str = read_line(buffer)?;
    if length_str.is_none() {
        return Ok(None);
    }

    let count = length_str.unwrap().parse::<i64>()?;
    if count < 0 {
        return Err(RespError::InvalidFormatDetails(format!(
            "Negative map length: {}",
            count
        )));
    }

    let mut pairs = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let key = match RespValue::parse(buffer)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let value = match RespValue::parse(buffer)? {
            Some(v) => v,
            None => return Ok(None),
        };
        pairs.push((key, value));
    }

    Ok(Some(RespValue::Map(pairs)))
}

fn parse_set(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Set is "~<number-of-elements>\r\n<element>..."
    let total_size = match calculate_resp_size(buffer) {
        Some(size) => size,
        None => return Ok(None),
    };

    let _ = total_size;
    let length_str = read_line(buffer)?;
    if length_str.is_none() {
        return Ok(None);
    }

    let count = length_str.unwrap().parse::<i64>()?;
    if count < 0 {
        return Err(RespError::InvalidFormatDetails(format!(
            "Negative set length: {}",
            count
        )));
    }

    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        match RespValue::parse(buffer)? {
            Some(v) => items.push(v),
            None => return Ok(None),
        }
    }

    Ok(Some(RespValue::Set(items)))
}

fn parse_attribute(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Attribute is "|<number-of-pairs>\r\n<key><value>..."
    let total_size = match calculate_resp_size(buffer) {
        Some(size) => size,
        None => return Ok(None),
    };

    let _ = total_size;
    let length_str = read_line(buffer)?;
    if length_str.is_none() {
        return Ok(None);
    }

    let count = length_str.unwrap().parse::<i64>()?;
    if count < 0 {
        return Err(RespError::InvalidFormatDetails(format!(
            "Negative attribute length: {}",
            count
        )));
    }

    let mut pairs = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let key = match RespValue::parse(buffer)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let value = match RespValue::parse(buffer)? {
            Some(v) => v,
            None => return Ok(None),
        };
        pairs.push((key, value));
    }

    Ok(Some(RespValue::Attribute(pairs)))
}

fn parse_push(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Push is "><number-of-elements>\r\n<element>..."
    let total_size = match calculate_resp_size(buffer) {
        Some(size) => size,
        None => return Ok(None),
    };

    let _ = total_size;
    let length_str = read_line(buffer)?;
    if length_str.is_none() {
        return Ok(None);
    }

    let count = length_str.unwrap().parse::<i64>()?;
    if count < 0 {
        return Err(RespError::InvalidFormatDetails(format!(
            "Negative push length: {}",
            count
        )));
    }

    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        match RespValue::parse(buffer)? {
            Some(v) => items.push(v),
            None => return Ok(None),
        }
    }

    Ok(Some(RespValue::Push(items)))
}

/// Parse an inline (plain-text) command.
///
/// Redis inline protocol: `COMMAND arg1 arg2 ...\r\n`
/// The first byte is NOT a RESP type marker. Tokens are separated by whitespace.
/// Returns `Ok(None)` if the line is incomplete (no `\r\n` yet).
fn parse_inline_command(buffer: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
    // Find \r\n or \n
    let newline_pos = buffer.iter().position(|&b| b == b'\n');
    let newline_pos = match newline_pos {
        Some(pos) => pos,
        None => return Ok(None), // Incomplete — wait for more data
    };

    // Determine actual line end (handle \r\n and bare \n)
    let line_end = if newline_pos > 0 && buffer[newline_pos - 1] == b'\r' {
        newline_pos - 1
    } else {
        newline_pos
    };

    let line = &buffer[..line_end];

    // Skip empty lines (Redis ignores blank inline lines)
    if line.iter().all(|b| b.is_ascii_whitespace()) {
        buffer.advance(newline_pos + 1);
        return Ok(None);
    }

    // Split on ASCII whitespace into tokens
    let tokens: Vec<Vec<u8>> = line
        .split(|b| b.is_ascii_whitespace())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_vec())
        .collect();

    // Consume the line from the buffer
    buffer.advance(newline_pos + 1);

    if tokens.is_empty() {
        return Ok(None);
    }

    // Convert to RespValue::Array of BulkStrings (same as RESP-framed commands)
    let args: Vec<RespValue> = tokens
        .into_iter()
        .map(|t| RespValue::BulkString(Some(t)))
        .collect();

    Ok(Some(RespValue::Array(Some(args))))
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_parse_simple_string() {
        let mut buffer = BytesMut::from("+OK\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::SimpleString("OK".to_string()))
        );
    }

    #[test]
    fn test_parse_error() {
        let mut buffer = BytesMut::from("-Error message\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Error("Error message".to_string()))
        );
    }

    #[test]
    fn test_parse_integer() {
        let mut buffer = BytesMut::from(":1000\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Integer(1000))
        );
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buffer = BytesMut::from("$5\r\nhello\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::BulkString(Some(b"hello".to_vec())))
        );
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let mut buffer = BytesMut::from("$-1\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::BulkString(None))
        );
    }

    #[test]
    fn test_parse_array() {
        let mut buffer = BytesMut::from("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"hello".to_vec())),
                RespValue::BulkString(Some(b"world".to_vec()))
            ])))
        );
    }

    #[test]
    fn test_serialize_large_bulk_string() {
        // Create a large bulk string (500KB)
        let size = 500 * 1024;
        let large_data = vec![b'x'; size];

        let value = RespValue::BulkString(Some(large_data.clone()));
        let serialized = value.serialize().unwrap();

        // Check the size header format
        let expected_header = format!("${}\r\n", size);
        assert_eq!(
            &serialized[0..expected_header.len()],
            expected_header.as_bytes()
        );

        // Check ending CRLF
        assert_eq!(&serialized[serialized.len() - 2..], b"\r\n");

        // Check data content (sample checks to avoid excessive memory use)
        assert_eq!(serialized[expected_header.len()], b'x');
        assert_eq!(serialized[expected_header.len() + 1000], b'x');
        assert_eq!(serialized[serialized.len() - 3], b'x');
    }

    #[test]
    fn test_strict_bulk_string_validation() {
        // Test that bulk strings require exact CRLF
        let mut buffer = BytesMut::from("$5\r\nhelloXX"); // Invalid CRLF
        let result = RespValue::parse(&mut buffer);
        assert!(result.is_err(), "Should reject invalid CRLF");

        // Test valid bulk string
        let mut buffer = BytesMut::from("$5\r\nhello\r\n");
        let result = RespValue::parse(&mut buffer);
        assert!(result.is_ok(), "Should accept valid bulk string");
        if let Ok(Some(RespValue::BulkString(Some(data)))) = result {
            assert_eq!(data, b"hello");
        }
    }

    #[test]
    fn test_non_resp_without_newline_returns_none() {
        // With inline protocol support, non-RESP data without \r\n is treated
        // as incomplete input (waiting for line ending), not an error.
        let test_patterns = [
            "B64JSON:W3data",       // Starts with 'B'
            "b64:encoded_data",     // Starts with 'b'
            "{\"json\": \"data\"}", // Starts with '{'
            "HTTP/1.1 200 OK",      // Starts with 'H'
            "<html>content</html>", // Starts with '<'
            "plain text",           // Starts with 'p'
        ];

        for pattern in &test_patterns {
            let mut buffer = BytesMut::from(pattern.as_bytes());
            let result = RespValue::parse(&mut buffer);
            assert!(
                matches!(result, Ok(None)),
                "Non-RESP data without \\r\\n should be incomplete (Ok(None)), got {:?} for '{}'",
                result,
                pattern
            );
        }
    }

    #[test]
    fn test_valid_resp_data_with_special_content() {
        // Test that data containing "b64:", "{", etc. works fine when properly formatted as RESP
        let test_data = vec![
            "b64:W3siaWQiOiAxLCAi",
            "B64JSON:encoded_data",
            "{\"key\": \"value\"}",
        ];

        for data in &test_data {
            // Create a proper RESP bulk string containing this data
            let data_bytes = data.as_bytes();
            let mut buffer = BytesMut::new();
            buffer.extend_from_slice(b"$");
            buffer.extend_from_slice(data_bytes.len().to_string().as_bytes());
            buffer.extend_from_slice(b"\r\n");
            buffer.extend_from_slice(data_bytes);
            buffer.extend_from_slice(b"\r\n");

            // This should parse successfully
            let result = RespValue::parse(&mut buffer);
            assert!(
                result.is_ok(),
                "Should parse properly formatted RESP data: {}",
                data
            );

            if let Ok(Some(RespValue::BulkString(Some(retrieved_data)))) = result {
                assert_eq!(
                    retrieved_data, data_bytes,
                    "Retrieved data should match original for: {}",
                    data
                );
            } else {
                panic!("Expected BulkString for data: {}", data);
            }
        }
    }

    #[test]
    fn test_serialize_large_array() {
        // Create a moderately sized array that will trigger chunking but is smaller for testing
        let mut items = Vec::with_capacity(500);
        for i in 0..500 {
            let data = format!("item-{}", i).into_bytes();
            items.push(RespValue::BulkString(Some(data)));
        }

        let array = RespValue::Array(Some(items));
        let serialized = array.serialize().unwrap();

        // Check the array header
        let expected_header = "*500\r\n";
        assert_eq!(
            &serialized[0..expected_header.len()],
            expected_header.as_bytes()
        );

        // With a smaller dataset, we can safely convert to a string for easier assertions
        let serialized_str = String::from_utf8_lossy(&serialized);

        // Check for a few key patterns
        assert!(
            serialized_str.contains("$6\r\nitem-0"),
            "Serialized output should contain item-0"
        );
        assert!(
            serialized_str.contains("$8\r\nitem-123"),
            "Serialized output should contain item-123"
        );
        assert!(
            serialized_str.contains("$8\r\nitem-499"),
            "Serialized output should contain item-499"
        );

        // Test chunking functionality by ensuring the content was properly serialized
        let mut expected_count = 0;
        for i in 0..500 {
            let pattern = format!("item-{}", i);
            if serialized_str.contains(&pattern) {
                expected_count += 1;
            }
        }

        // All 500 items should be present
        assert_eq!(
            expected_count, 500,
            "All 500 items should be in the serialized output"
        );
    }

    #[test]
    fn test_serialize_nested_array() {
        // Create a nested array structure similar to ZRANGE WITHSCORES response
        let mut outer_array = Vec::new();

        for i in 0..5 {
            // Create member
            let member = format!("member{}", i).into_bytes();
            outer_array.push(RespValue::BulkString(Some(member)));

            // Create score
            let score = format!("{}.5", i * 10).into_bytes();
            outer_array.push(RespValue::BulkString(Some(score)));
        }

        let array = RespValue::Array(Some(outer_array));
        let serialized = array.serialize().unwrap();

        // Check array header
        assert_eq!(&serialized[0..4], b"*10\r");

        // Convert to string for easier inspection
        let serialized_str = String::from_utf8_lossy(&serialized);

        // Check for some expected patterns
        assert!(serialized_str.contains("$7\r\nmember0"));
        assert!(serialized_str.contains("$3\r\n0.5"));
        assert!(serialized_str.contains("$7\r\nmember4"));
        assert!(serialized_str.contains("$4\r\n40.5"));
    }

    #[test]
    fn test_size_limit_check() {
        // Verify that RespValue serialization handles large bulk strings correctly.
        // We test with a reasonably sized payload to verify the serialization format
        // includes the correct length prefix, rather than allocating 600MB.
        let data = vec![0x41u8; 1024]; // 1KB of 'A'
        let value = RespValue::BulkString(Some(data.clone()));
        let serialized = value
            .serialize()
            .expect("serialization should succeed for 1KB payload");
        let expected_prefix = format!("${}\r\n", data.len());
        let serialized_str = String::from_utf8_lossy(&serialized);
        assert!(
            serialized_str.starts_with(&expected_prefix),
            "BulkString should start with length prefix, got: {}",
            &serialized_str[..expected_prefix.len().min(serialized_str.len())]
        );
        assert!(
            serialized_str.ends_with("\r\n"),
            "BulkString should end with CRLF"
        );
        // Verify the total length: $<len>\r\n<data>\r\n
        assert_eq!(serialized.len(), expected_prefix.len() + data.len() + 2);
    }

    // ===== RESP3 Type Tests =====

    #[test]
    fn test_parse_null() {
        let mut buffer = BytesMut::from("_\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Null)
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_serialize_null() {
        let value = RespValue::Null;
        let bytes = value.serialize().unwrap();
        assert_eq!(bytes, b"_\r\n");
    }

    #[test]
    fn test_parse_boolean_true() {
        let mut buffer = BytesMut::from("#t\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Boolean(true))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_parse_boolean_false() {
        let mut buffer = BytesMut::from("#f\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Boolean(false))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_parse_boolean_invalid() {
        let mut buffer = BytesMut::from("#x\r\n");
        assert!(RespValue::parse(&mut buffer).is_err());
    }

    #[test]
    fn test_serialize_boolean() {
        assert_eq!(RespValue::Boolean(true).serialize().unwrap(), b"#t\r\n");
        assert_eq!(RespValue::Boolean(false).serialize().unwrap(), b"#f\r\n");
    }

    #[test]
    fn test_parse_double() {
        let mut buffer = BytesMut::from(",3.14\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Double(3.14))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_parse_double_negative() {
        let mut buffer = BytesMut::from(",-2.5\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Double(-2.5))
        );
    }

    #[test]
    fn test_parse_double_inf() {
        let mut buffer = BytesMut::from(",inf\r\n");
        if let Some(RespValue::Double(d)) = RespValue::parse(&mut buffer).unwrap() {
            assert!(d.is_infinite() && d.is_sign_positive());
        } else {
            panic!("Expected Double(inf)");
        }
    }

    #[test]
    fn test_parse_double_neg_inf() {
        let mut buffer = BytesMut::from(",-inf\r\n");
        if let Some(RespValue::Double(d)) = RespValue::parse(&mut buffer).unwrap() {
            assert!(d.is_infinite() && d.is_sign_negative());
        } else {
            panic!("Expected Double(-inf)");
        }
    }

    #[test]
    fn test_parse_double_nan() {
        let mut buffer = BytesMut::from(",nan\r\n");
        if let Some(RespValue::Double(d)) = RespValue::parse(&mut buffer).unwrap() {
            assert!(d.is_nan());
        } else {
            panic!("Expected Double(NaN)");
        }
    }

    #[test]
    fn test_serialize_double() {
        let bytes = RespValue::Double(3.14).serialize().unwrap();
        assert_eq!(bytes, b",3.14\r\n");

        let bytes = RespValue::Double(f64::INFINITY).serialize().unwrap();
        assert_eq!(bytes, b",inf\r\n");

        let bytes = RespValue::Double(f64::NEG_INFINITY).serialize().unwrap();
        assert_eq!(bytes, b",-inf\r\n");

        let bytes = RespValue::Double(f64::NAN).serialize().unwrap();
        assert_eq!(bytes, b",nan\r\n");
    }

    #[test]
    fn test_parse_big_number() {
        let mut buffer = BytesMut::from("(3492890328409238509324850943850943809\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::BigNumber(
                "3492890328409238509324850943850943809".to_string()
            ))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_parse_big_number_negative() {
        let mut buffer = BytesMut::from("(-12345678901234567890\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::BigNumber("-12345678901234567890".to_string()))
        );
    }

    #[test]
    fn test_parse_big_number_invalid() {
        let mut buffer = BytesMut::from("(abc\r\n");
        assert!(RespValue::parse(&mut buffer).is_err());
    }

    #[test]
    fn test_serialize_big_number() {
        let bytes = RespValue::BigNumber("12345678901234567890".to_string())
            .serialize()
            .unwrap();
        assert_eq!(bytes, b"(12345678901234567890\r\n");
    }

    #[test]
    fn test_parse_bulk_error() {
        let mut buffer = BytesMut::from("!21\r\nSYNTAX invalid syntax\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::BulkError("SYNTAX invalid syntax".to_string()))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_serialize_bulk_error() {
        let bytes = RespValue::BulkError("SYNTAX invalid syntax".to_string())
            .serialize()
            .unwrap();
        assert_eq!(bytes, b"!21\r\nSYNTAX invalid syntax\r\n");
    }

    #[test]
    fn test_parse_verbatim_string() {
        let mut buffer = BytesMut::from("=15\r\ntxt:Some string\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::VerbatimString {
                encoding: "txt".to_string(),
                data: "Some string".to_string(),
            })
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_parse_verbatim_string_markdown() {
        let mut buffer = BytesMut::from("=11\r\nmkd:# Hello\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::VerbatimString {
                encoding: "mkd".to_string(),
                data: "# Hello".to_string(),
            })
        );
    }

    #[test]
    fn test_serialize_verbatim_string() {
        let bytes = RespValue::VerbatimString {
            encoding: "txt".to_string(),
            data: "Some string".to_string(),
        }
        .serialize()
        .unwrap();
        assert_eq!(bytes, b"=15\r\ntxt:Some string\r\n");
    }

    #[test]
    fn test_parse_map() {
        // %2\r\n+first\r\n:1\r\n+second\r\n:2\r\n
        let mut buffer = BytesMut::from("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
        let result = RespValue::parse(&mut buffer).unwrap();
        assert_eq!(
            result,
            Some(RespValue::Map(vec![
                (
                    RespValue::SimpleString("first".to_string()),
                    RespValue::Integer(1)
                ),
                (
                    RespValue::SimpleString("second".to_string()),
                    RespValue::Integer(2)
                ),
            ]))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_parse_empty_map() {
        let mut buffer = BytesMut::from("%0\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Map(vec![]))
        );
    }

    #[test]
    fn test_serialize_map() {
        let value = RespValue::Map(vec![(
            RespValue::SimpleString("key".to_string()),
            RespValue::Integer(42),
        )]);
        let bytes = value.serialize().unwrap();
        assert_eq!(bytes, b"%1\r\n+key\r\n:42\r\n");
    }

    #[test]
    fn test_parse_set() {
        let mut buffer = BytesMut::from("~3\r\n+a\r\n+b\r\n+c\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Set(vec![
                RespValue::SimpleString("a".to_string()),
                RespValue::SimpleString("b".to_string()),
                RespValue::SimpleString("c".to_string()),
            ]))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_serialize_set() {
        let value = RespValue::Set(vec![RespValue::Integer(1), RespValue::Integer(2)]);
        let bytes = value.serialize().unwrap();
        assert_eq!(bytes, b"~2\r\n:1\r\n:2\r\n");
    }

    #[test]
    fn test_parse_attribute() {
        let mut buffer = BytesMut::from("|1\r\n+key\r\n+value\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Attribute(vec![(
                RespValue::SimpleString("key".to_string()),
                RespValue::SimpleString("value".to_string())
            ),]))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_serialize_attribute() {
        let value = RespValue::Attribute(vec![(
            RespValue::SimpleString("key".to_string()),
            RespValue::SimpleString("value".to_string()),
        )]);
        let bytes = value.serialize().unwrap();
        assert_eq!(bytes, b"|1\r\n+key\r\n+value\r\n");
    }

    #[test]
    fn test_parse_push() {
        let mut buffer = BytesMut::from(">2\r\n+pubsub\r\n+message\r\n");
        assert_eq!(
            RespValue::parse(&mut buffer).unwrap(),
            Some(RespValue::Push(vec![
                RespValue::SimpleString("pubsub".to_string()),
                RespValue::SimpleString("message".to_string()),
            ]))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_serialize_push() {
        let value = RespValue::Push(vec![
            RespValue::SimpleString("pubsub".to_string()),
            RespValue::SimpleString("message".to_string()),
        ]);
        let bytes = value.serialize().unwrap();
        assert_eq!(bytes, b">2\r\n+pubsub\r\n+message\r\n");
    }

    #[test]
    fn test_resp3_roundtrip_all_types() {
        // Test that serializing and then parsing each RESP3 type produces the same value
        let test_values = vec![
            RespValue::Null,
            RespValue::Boolean(true),
            RespValue::Boolean(false),
            RespValue::Double(1.23),
            RespValue::BigNumber("99999999999999999999".to_string()),
            RespValue::BulkError("ERR something went wrong".to_string()),
            RespValue::VerbatimString {
                encoding: "txt".to_string(),
                data: "hello world".to_string(),
            },
            RespValue::Map(vec![
                (
                    RespValue::SimpleString("a".to_string()),
                    RespValue::Integer(1),
                ),
                (
                    RespValue::SimpleString("b".to_string()),
                    RespValue::Integer(2),
                ),
            ]),
            RespValue::Set(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(3),
            ]),
            RespValue::Push(vec![
                RespValue::SimpleString("invalidate".to_string()),
                RespValue::Array(Some(vec![RespValue::SimpleString("key1".to_string())])),
            ]),
        ];

        for original in &test_values {
            let serialized = original.serialize().unwrap();
            let mut buffer = BytesMut::from(serialized.as_slice());
            let parsed = RespValue::parse(&mut buffer).unwrap();
            // Special handling for NaN (NaN != NaN)
            assert_eq!(
                parsed.as_ref(),
                Some(original),
                "Roundtrip failed for {:?}",
                original
            );
            assert!(
                buffer.is_empty(),
                "Buffer not empty after parsing {:?}",
                original
            );
        }
    }

    #[test]
    fn test_resp3_nested_types() {
        // Test a map containing various RESP3 types
        let value = RespValue::Map(vec![
            (
                RespValue::BulkString(Some(b"null".to_vec())),
                RespValue::Null,
            ),
            (
                RespValue::BulkString(Some(b"bool".to_vec())),
                RespValue::Boolean(true),
            ),
            (
                RespValue::BulkString(Some(b"double".to_vec())),
                RespValue::Double(3.14),
            ),
        ]);
        let serialized = value.serialize().unwrap();
        let mut buffer = BytesMut::from(serialized.as_slice());
        let parsed = RespValue::parse(&mut buffer).unwrap();
        assert_eq!(parsed, Some(value));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_resp3_incomplete_data() {
        // Test that incomplete RESP3 data returns None without error
        let incomplete_cases = vec![
            "_\r",            // Null missing \n
            "#",              // Boolean incomplete
            ",3.1",           // Double incomplete
            "(123",           // Big number incomplete
            "!5\r\nhe",       // Bulk error incomplete
            "=10\r\ntxt:he",  // Verbatim string incomplete
            "%1\r\n+key\r\n", // Map missing value
            "~2\r\n+a\r\n",   // Set missing element
        ];

        for case in incomplete_cases {
            let mut buffer = BytesMut::from(case.as_bytes());
            let result = RespValue::parse(&mut buffer);
            match result {
                Ok(None) => {} // Expected: incomplete data
                Ok(Some(_)) => panic!("Expected None for incomplete data: {}", case),
                Err(_) => {} // Also acceptable: some edge cases might error
            }
        }
    }

    #[test]
    fn test_protocol_version_default() {
        assert_eq!(ProtocolVersion::default(), ProtocolVersion::RESP2);
    }

    #[test]
    fn test_double_integer_value() {
        // Double that is an integer value
        let mut buffer = BytesMut::from(",10\r\n");
        if let Some(RespValue::Double(d)) = RespValue::parse(&mut buffer).unwrap() {
            assert_eq!(d, 10.0);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_double_zero() {
        let mut buffer = BytesMut::from(",0\r\n");
        if let Some(RespValue::Double(d)) = RespValue::parse(&mut buffer).unwrap() {
            assert_eq!(d, 0.0);
        } else {
            panic!("Expected Double");
        }
    }

    // --- RESP3 conversion tests ---

    #[test]
    fn test_resp3_null_conversion_bulk_string() {
        let value = RespValue::BulkString(None);
        let converted = value.convert_for_resp3("get");
        assert_eq!(converted, RespValue::Null);
        // Verify serialization produces RESP3 null
        let bytes = converted.serialize().unwrap();
        assert_eq!(bytes, b"_\r\n");
    }

    #[test]
    fn test_resp3_null_conversion_array() {
        let value = RespValue::Array(None);
        let converted = value.convert_for_resp3("keys");
        assert_eq!(converted, RespValue::Null);
        let bytes = converted.serialize().unwrap();
        assert_eq!(bytes, b"_\r\n");
    }

    #[test]
    fn test_resp3_map_conversion_hgetall() {
        // HGETALL returns flat array [field1, val1, field2, val2] -> Map
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"field1".to_vec())),
            RespValue::BulkString(Some(b"value1".to_vec())),
            RespValue::BulkString(Some(b"field2".to_vec())),
            RespValue::BulkString(Some(b"value2".to_vec())),
        ]));
        let converted = value.convert_for_resp3("hgetall");
        match converted {
            RespValue::Map(pairs) => {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].0, RespValue::BulkString(Some(b"field1".to_vec())));
                assert_eq!(pairs[0].1, RespValue::BulkString(Some(b"value1".to_vec())));
                assert_eq!(pairs[1].0, RespValue::BulkString(Some(b"field2".to_vec())));
                assert_eq!(pairs[1].1, RespValue::BulkString(Some(b"value2".to_vec())));
            }
            other => panic!("Expected Map, got {:?}", other),
        }
        // Verify serialization produces RESP3 map format
        let value2 = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"k".to_vec())),
            RespValue::BulkString(Some(b"v".to_vec())),
        ]));
        let converted2 = value2.convert_for_resp3("hgetall");
        let bytes = converted2.serialize().unwrap();
        // %1\r\n$1\r\nk\r\n$1\r\nv\r\n
        assert_eq!(&bytes[0..4], b"%1\r\n");
    }

    #[test]
    fn test_resp3_set_conversion_smembers() {
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"member1".to_vec())),
            RespValue::BulkString(Some(b"member2".to_vec())),
        ]));
        let converted = value.convert_for_resp3("smembers");
        match converted {
            RespValue::Set(items) => {
                assert_eq!(items.len(), 2);
            }
            other => panic!("Expected Set, got {:?}", other),
        }
        // Verify serialization produces RESP3 set format
        let value2 = RespValue::Array(Some(vec![RespValue::BulkString(Some(b"a".to_vec()))]));
        let converted2 = value2.convert_for_resp3("smembers");
        let bytes = converted2.serialize().unwrap();
        assert_eq!(&bytes[0..4], b"~1\r\n");
    }

    #[test]
    fn test_resp3_double_conversion_zscore() {
        let value = RespValue::BulkString(Some(b"3.14".to_vec()));
        let converted = value.convert_for_resp3("zscore");
        match converted {
            RespValue::Double(d) => {
                assert!((d - 3.14).abs() < 1e-10);
            }
            other => panic!("Expected Double, got {:?}", other),
        }
        let bytes = converted.serialize().unwrap();
        assert!(bytes.starts_with(b","));
    }

    #[test]
    fn test_resp3_double_conversion_infinity() {
        let value = RespValue::BulkString(Some(b"inf".to_vec()));
        let converted = value.convert_for_resp3("zscore");
        match converted {
            RespValue::Double(d) => assert!(d.is_infinite() && d.is_sign_positive()),
            other => panic!("Expected Double(inf), got {:?}", other),
        }
    }

    #[test]
    fn test_resp3_push_conversion() {
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"message".to_vec())),
            RespValue::BulkString(Some(b"channel".to_vec())),
            RespValue::BulkString(Some(b"hello".to_vec())),
        ]));
        let converted = value.convert_to_push();
        match converted {
            RespValue::Push(items) => {
                assert_eq!(items.len(), 3);
            }
            other => panic!("Expected Push, got {:?}", other),
        }
        // Verify serialization produces RESP3 push format
        let value2 = RespValue::Array(Some(vec![RespValue::BulkString(Some(b"x".to_vec()))]));
        let converted2 = value2.convert_to_push();
        let bytes = converted2.serialize().unwrap();
        assert_eq!(&bytes[0..4], b">1\r\n");
    }

    #[test]
    fn test_resp2_no_conversion_for_non_map_commands() {
        // GET returns a bulk string, not a map - should stay as-is
        let value = RespValue::BulkString(Some(b"hello".to_vec()));
        let converted = value.convert_for_resp3("get");
        assert_eq!(converted, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[test]
    fn test_resp3_non_double_command_keeps_bulk_string() {
        // GET returns a string that happens to look like a float - keep as bulk string
        let value = RespValue::BulkString(Some(b"3.14".to_vec()));
        let converted = value.convert_for_resp3("get");
        assert_eq!(converted, RespValue::BulkString(Some(b"3.14".to_vec())));
    }

    #[test]
    fn test_resp3_config_get_map_conversion() {
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"maxmemory".to_vec())),
            RespValue::BulkString(Some(b"0".to_vec())),
        ]));
        let converted = value.convert_for_resp3("config get");
        match converted {
            RespValue::Map(pairs) => {
                assert_eq!(pairs.len(), 1);
                assert_eq!(
                    pairs[0].0,
                    RespValue::BulkString(Some(b"maxmemory".to_vec()))
                );
                assert_eq!(pairs[0].1, RespValue::BulkString(Some(b"0".to_vec())));
            }
            other => panic!("Expected Map, got {:?}", other),
        }
    }

    #[test]
    fn test_resp3_empty_array_stays_empty() {
        // Empty array should remain an empty array (not null, not map)
        let value = RespValue::Array(Some(vec![]));
        let converted = value.convert_for_resp3("keys");
        assert_eq!(converted, RespValue::Array(Some(vec![])));
    }

    #[test]
    fn test_resp3_sinter_set_conversion() {
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));
        let converted = value.convert_for_resp3("sinter");
        match converted {
            RespValue::Set(items) => assert_eq!(items.len(), 2),
            other => panic!("Expected Set, got {:?}", other),
        }
    }

    #[test]
    fn test_resp3_boolean_serialization() {
        // Verify Boolean type serializes correctly
        let t = RespValue::Boolean(true);
        assert_eq!(t.serialize().unwrap(), b"#t\r\n");
        let f = RespValue::Boolean(false);
        assert_eq!(f.serialize().unwrap(), b"#f\r\n");
    }

    #[test]
    fn test_resp3_null_serialization() {
        let null = RespValue::Null;
        assert_eq!(null.serialize().unwrap(), b"_\r\n");
    }

    #[test]
    fn test_resp3_double_serialization() {
        let d = RespValue::Double(3.14);
        let bytes = d.serialize().unwrap();
        assert!(bytes.starts_with(b","));
        assert!(bytes.ends_with(b"\r\n"));

        let inf = RespValue::Double(f64::INFINITY);
        assert_eq!(inf.serialize().unwrap(), b",inf\r\n");

        let neg_inf = RespValue::Double(f64::NEG_INFINITY);
        assert_eq!(neg_inf.serialize().unwrap(), b",-inf\r\n");
    }

    #[test]
    fn test_resp3_map_serialization() {
        let map = RespValue::Map(vec![(
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::Integer(42),
        )]);
        let bytes = map.serialize().unwrap();
        // %1\r\n$3\r\nkey\r\n:42\r\n
        assert!(bytes.starts_with(b"%1\r\n"));
    }

    #[test]
    fn test_resp3_set_serialization() {
        let set = RespValue::Set(vec![
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]);
        let bytes = set.serialize().unwrap();
        assert!(bytes.starts_with(b"~2\r\n"));
    }

    #[test]
    fn test_resp3_push_serialization() {
        let push = RespValue::Push(vec![
            RespValue::BulkString(Some(b"message".to_vec())),
            RespValue::BulkString(Some(b"ch".to_vec())),
            RespValue::BulkString(Some(b"data".to_vec())),
        ]);
        let bytes = push.serialize().unwrap();
        assert!(bytes.starts_with(b">3\r\n"));
    }

    #[test]
    fn test_resp2_backward_compat_no_conversion() {
        // Without calling convert_for_resp3, values serialize as RESP2
        let null = RespValue::BulkString(None);
        assert_eq!(null.serialize().unwrap(), b"$-1\r\n");

        let arr_null = RespValue::Array(None);
        assert_eq!(arr_null.serialize().unwrap(), b"*-1\r\n");

        // Normal array serialization (RESP2)
        let arr = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));
        let bytes = arr.serialize().unwrap();
        assert!(bytes.starts_with(b"*2\r\n"));
    }

    #[test]
    fn test_resp3_xinfo_stream_map_conversion() {
        // XINFO STREAM returns flat kv array -> Map in RESP3
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"length".to_vec())),
            RespValue::Integer(5),
            RespValue::BulkString(Some(b"groups".to_vec())),
            RespValue::Integer(1),
        ]));
        let converted = value.convert_for_resp3("xinfo stream");
        match converted {
            RespValue::Map(pairs) => {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].0, RespValue::BulkString(Some(b"length".to_vec())));
                assert_eq!(pairs[0].1, RespValue::Integer(5));
                assert_eq!(pairs[1].0, RespValue::BulkString(Some(b"groups".to_vec())));
                assert_eq!(pairs[1].1, RespValue::Integer(1));
            }
            other => panic!("Expected Map for 'xinfo stream', got {:?}", other),
        }
    }

    #[test]
    fn test_resp3_xinfo_groups_array_of_maps() {
        // XINFO GROUPS returns array where each element is a flat kv array -> Array of Maps
        let value = RespValue::Array(Some(vec![
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"name".to_vec())),
                RespValue::BulkString(Some(b"group1".to_vec())),
                RespValue::BulkString(Some(b"consumers".to_vec())),
                RespValue::Integer(2),
            ])),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"name".to_vec())),
                RespValue::BulkString(Some(b"group2".to_vec())),
                RespValue::BulkString(Some(b"consumers".to_vec())),
                RespValue::Integer(1),
            ])),
        ]));
        let converted = value.convert_for_resp3("xinfo groups");
        if let RespValue::Array(Some(items)) = converted {
            assert_eq!(items.len(), 2);
            // Each inner element should be a Map
            for item in &items {
                assert!(
                    matches!(item, RespValue::Map(_)),
                    "Expected Map, got {:?}",
                    item
                );
            }
            if let RespValue::Map(pairs) = &items[0] {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].1, RespValue::BulkString(Some(b"group1".to_vec())));
            }
        } else {
            panic!("Expected Array of Maps for 'xinfo groups'");
        }
    }

    #[test]
    fn test_resp3_xinfo_consumers_array_of_maps() {
        let value = RespValue::Array(Some(vec![RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"name".to_vec())),
            RespValue::BulkString(Some(b"consumer1".to_vec())),
            RespValue::BulkString(Some(b"pending".to_vec())),
            RespValue::Integer(3),
        ]))]));
        let converted = value.convert_for_resp3("xinfo consumers");
        if let RespValue::Array(Some(items)) = converted {
            assert_eq!(items.len(), 1);
            assert!(matches!(&items[0], RespValue::Map(pairs) if pairs.len() == 2));
        } else {
            panic!("Expected Array of Maps for 'xinfo consumers'");
        }
    }

    #[test]
    fn test_resp3_hscan_cursor_map() {
        // HSCAN returns [cursor, [f1, v1, f2, v2]] -> [cursor, Map{f1:v1, f2:v2}]
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"0".to_vec())),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"field1".to_vec())),
                RespValue::BulkString(Some(b"val1".to_vec())),
                RespValue::BulkString(Some(b"field2".to_vec())),
                RespValue::BulkString(Some(b"val2".to_vec())),
            ])),
        ]));
        let converted = value.convert_for_resp3("hscan");
        if let RespValue::Array(Some(items)) = converted {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"0".to_vec())));
            if let RespValue::Map(pairs) = &items[1] {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].0, RespValue::BulkString(Some(b"field1".to_vec())));
                assert_eq!(pairs[0].1, RespValue::BulkString(Some(b"val1".to_vec())));
            } else {
                panic!("Expected Map as second element of HSCAN result");
            }
        } else {
            panic!("Expected Array for HSCAN result");
        }
    }

    #[test]
    fn test_resp3_zscan_cursor_map() {
        // ZSCAN returns [cursor, [member1, score1, member2, score2]] -> [cursor, Map]
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"0".to_vec())),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"alice".to_vec())),
                RespValue::BulkString(Some(b"100".to_vec())),
                RespValue::BulkString(Some(b"bob".to_vec())),
                RespValue::BulkString(Some(b"200".to_vec())),
            ])),
        ]));
        let converted = value.convert_for_resp3("zscan");
        if let RespValue::Array(Some(items)) = converted {
            assert_eq!(items.len(), 2);
            if let RespValue::Map(pairs) = &items[1] {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].0, RespValue::BulkString(Some(b"alice".to_vec())));
                assert_eq!(pairs[0].1, RespValue::BulkString(Some(b"100".to_vec())));
            } else {
                panic!("Expected Map as second element of ZSCAN result");
            }
        } else {
            panic!("Expected Array for ZSCAN result");
        }
    }

    #[test]
    fn test_resp3_sscan_cursor_set() {
        // SSCAN returns [cursor, [m1, m2]] -> [cursor, Set[m1, m2]]
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"0".to_vec())),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"alice".to_vec())),
                RespValue::BulkString(Some(b"bob".to_vec())),
            ])),
        ]));
        let converted = value.convert_for_resp3("sscan");
        if let RespValue::Array(Some(items)) = converted {
            assert_eq!(items.len(), 2);
            if let RespValue::Set(members) = &items[1] {
                assert_eq!(members.len(), 2);
            } else {
                panic!("Expected Set as second element of SSCAN result");
            }
        } else {
            panic!("Expected Array for SSCAN result");
        }
    }

    #[test]
    fn test_resp3_xrange_stream_entry_map() {
        // XRANGE returns [[id, [f1, v1, f2, v2]], ...] -> [[id, Map{f1:v1, f2:v2}], ...]
        let value = RespValue::Array(Some(vec![
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"1-0".to_vec())),
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"name".to_vec())),
                    RespValue::BulkString(Some(b"alice".to_vec())),
                    RespValue::BulkString(Some(b"age".to_vec())),
                    RespValue::BulkString(Some(b"30".to_vec())),
                ])),
            ])),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"2-0".to_vec())),
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"name".to_vec())),
                    RespValue::BulkString(Some(b"bob".to_vec())),
                ])),
            ])),
        ]));
        let converted = value.convert_for_resp3("xrange");
        if let RespValue::Array(Some(entries)) = converted {
            assert_eq!(entries.len(), 2);
            // First entry
            if let RespValue::Array(Some(entry)) = &entries[0] {
                assert_eq!(entry.len(), 2);
                assert_eq!(entry[0], RespValue::BulkString(Some(b"1-0".to_vec())));
                if let RespValue::Map(pairs) = &entry[1] {
                    assert_eq!(pairs.len(), 2);
                    assert_eq!(pairs[0].0, RespValue::BulkString(Some(b"name".to_vec())));
                    assert_eq!(pairs[0].1, RespValue::BulkString(Some(b"alice".to_vec())));
                } else {
                    panic!("Expected Map for entry field-values");
                }
            } else {
                panic!("Expected Array for stream entry");
            }
        } else {
            panic!("Expected Array for XRANGE result");
        }
    }

    #[test]
    fn test_resp3_xrevrange_stream_entry_map() {
        let value = RespValue::Array(Some(vec![RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"5-0".to_vec())),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"k".to_vec())),
                RespValue::BulkString(Some(b"v".to_vec())),
            ])),
        ]))]));
        let converted = value.convert_for_resp3("xrevrange");
        if let RespValue::Array(Some(entries)) = converted {
            if let RespValue::Array(Some(entry)) = &entries[0] {
                assert!(matches!(&entry[1], RespValue::Map(pairs) if pairs.len() == 1));
            } else {
                panic!("Expected converted stream entry");
            }
        } else {
            panic!("Expected Array for XREVRANGE result");
        }
    }

    #[test]
    fn test_resp3_xread_map_with_entry_maps() {
        // XREAD returns [[name, [[id, [f, v]], ...]], ...] -> Map{name: [[id, Map], ...]}
        let value = RespValue::Array(Some(vec![RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"stream1".to_vec())),
            RespValue::Array(Some(vec![RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"1-0".to_vec())),
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"field".to_vec())),
                    RespValue::BulkString(Some(b"value".to_vec())),
                ])),
            ]))])),
        ]))]));
        let converted = value.convert_for_resp3("xread");
        if let RespValue::Map(pairs) = converted {
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs[0].0, RespValue::BulkString(Some(b"stream1".to_vec())));
            // Inner entries should have Map field-values
            if let RespValue::Array(Some(entries)) = &pairs[0].1 {
                if let RespValue::Array(Some(entry)) = &entries[0] {
                    assert!(matches!(&entry[1], RespValue::Map(_)));
                } else {
                    panic!("Expected entry array");
                }
            } else {
                panic!("Expected entries array");
            }
        } else {
            panic!("Expected Map for XREAD result, got {:?}", converted);
        }
    }

    #[test]
    fn test_resp3_command_docs_map_conversion() {
        // COMMAND DOCS returns alternating cmd-name + doc-array pairs -> Map
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"get".to_vec())),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"summary".to_vec())),
                RespValue::BulkString(Some(b"The GET command".to_vec())),
            ])),
        ]));
        let converted = value.convert_for_resp3("command docs");
        match converted {
            RespValue::Map(pairs) => {
                assert_eq!(pairs.len(), 1);
                assert_eq!(pairs[0].0, RespValue::BulkString(Some(b"get".to_vec())));
            }
            other => panic!("Expected Map for 'command docs', got {:?}", other),
        }
    }

    #[test]
    fn test_resp3_empty_xrange_stays_empty() {
        // Empty XRANGE result should stay as empty array
        let value = RespValue::Array(Some(vec![]));
        let converted = value.convert_for_resp3("xrange");
        assert_eq!(converted, RespValue::Array(Some(vec![])));
    }

    #[test]
    fn test_resp3_unknown_command_passthrough() {
        // Unknown command names pass through unchanged
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"a".to_vec())),
            RespValue::BulkString(Some(b"b".to_vec())),
        ]));
        let converted = value.convert_for_resp3("randomcmd");
        assert_eq!(
            converted,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"a".to_vec())),
                RespValue::BulkString(Some(b"b".to_vec())),
            ]))
        );
    }

    // ─── Inline protocol tests ───────────────────────────────────────

    #[test]
    fn test_inline_ping() {
        let mut buffer = BytesMut::from("PING\r\n");
        let result = RespValue::parse(&mut buffer).unwrap();
        assert_eq!(
            result,
            Some(RespValue::Array(Some(vec![RespValue::BulkString(Some(
                b"PING".to_vec()
            ))])))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_inline_set_command() {
        let mut buffer = BytesMut::from("SET foo bar\r\n");
        let result = RespValue::parse(&mut buffer).unwrap();
        assert_eq!(
            result,
            Some(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"foo".to_vec())),
                RespValue::BulkString(Some(b"bar".to_vec())),
            ])))
        );
    }

    #[test]
    fn test_inline_bare_newline() {
        // Redis accepts bare \n as line ending
        let mut buffer = BytesMut::from("PING\n");
        let result = RespValue::parse(&mut buffer).unwrap();
        assert_eq!(
            result,
            Some(RespValue::Array(Some(vec![RespValue::BulkString(Some(
                b"PING".to_vec()
            ))])))
        );
    }

    #[test]
    fn test_inline_incomplete() {
        // No \r\n yet — should return None (incomplete)
        let mut buffer = BytesMut::from("PING");
        let result = RespValue::parse(&mut buffer).unwrap();
        assert_eq!(result, None);
        // Buffer should be unchanged
        assert_eq!(&buffer[..], b"PING");
    }

    #[test]
    fn test_inline_empty_line() {
        let mut buffer = BytesMut::from("\r\n");
        let result = RespValue::parse(&mut buffer).unwrap();
        assert_eq!(result, None);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_inline_multiple_spaces() {
        let mut buffer = BytesMut::from("SET   foo   bar\r\n");
        let result = RespValue::parse(&mut buffer).unwrap();
        assert_eq!(
            result,
            Some(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"foo".to_vec())),
                RespValue::BulkString(Some(b"bar".to_vec())),
            ])))
        );
    }

    // ── Static response interning tests ──────────────────────────────────

    #[test]
    fn test_static_ok_response() {
        let resp = RespValue::SimpleString("OK".to_string());
        assert!(resp.try_static_response().is_some());
        let bytes = resp.serialize().unwrap();
        assert_eq!(bytes, b"+OK\r\n");
    }

    #[test]
    fn test_static_pong_response() {
        let resp = RespValue::SimpleString("PONG".to_string());
        assert!(resp.try_static_response().is_some());
        let bytes = resp.serialize().unwrap();
        assert_eq!(bytes, b"+PONG\r\n");
    }

    #[test]
    fn test_static_zero_response() {
        let resp = RespValue::Integer(0);
        assert!(resp.try_static_response().is_some());
        let bytes = resp.serialize().unwrap();
        assert_eq!(bytes, b":0\r\n");
    }

    #[test]
    fn test_static_one_response() {
        let resp = RespValue::Integer(1);
        assert!(resp.try_static_response().is_some());
        let bytes = resp.serialize().unwrap();
        assert_eq!(bytes, b":1\r\n");
    }

    #[test]
    fn test_static_nil_response() {
        let resp = RespValue::BulkString(None);
        assert!(resp.try_static_response().is_some());
        let bytes = resp.serialize().unwrap();
        assert_eq!(bytes, b"$-1\r\n");
    }

    #[test]
    fn test_static_empty_array_response() {
        let resp = RespValue::Array(Some(vec![]));
        assert!(resp.try_static_response().is_some());
        let bytes = resp.serialize().unwrap();
        assert_eq!(bytes, b"*0\r\n");
    }

    #[test]
    fn test_non_static_responses_still_serialize_correctly() {
        // Non-static SimpleString
        let resp = RespValue::SimpleString("QUEUED".to_string());
        assert!(resp.try_static_response().is_none());
        assert_eq!(resp.serialize().unwrap(), b"+QUEUED\r\n");

        // Non-static integer
        let resp = RespValue::Integer(42);
        assert!(resp.try_static_response().is_none());
        assert_eq!(resp.serialize().unwrap(), b":42\r\n");

        // Negative integer
        let resp = RespValue::Integer(-1);
        assert!(resp.try_static_response().is_none());
        assert_eq!(resp.serialize().unwrap(), b":-1\r\n");

        // Large integer (itoa path)
        let resp = RespValue::Integer(9999999999);
        assert_eq!(resp.serialize().unwrap(), b":9999999999\r\n");
    }

    #[test]
    fn test_itoa_integer_serialization() {
        // Edge cases for itoa serialization
        assert_eq!(
            RespValue::Integer(i64::MAX).serialize().unwrap(),
            format!(":{}\r\n", i64::MAX).into_bytes()
        );
        assert_eq!(
            RespValue::Integer(i64::MIN).serialize().unwrap(),
            format!(":{}\r\n", i64::MIN).into_bytes()
        );
        assert_eq!(RespValue::Integer(0).serialize().unwrap(), b":0\r\n");
    }
}
