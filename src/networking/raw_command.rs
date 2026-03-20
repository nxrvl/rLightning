use smallvec::SmallVec;

use crate::command::Command;
use crate::networking::resp::{RespError, RespValue};

/// A zero-copy parsed RESP command that borrows directly from the read buffer.
///
/// This avoids all heap allocations during parsing. The caller must convert to
/// owned types (via `to_command()` or `to_resp_value()`) before advancing the buffer.
#[derive(Debug)]
pub struct RawCommand<'buf> {
    /// Command name as a borrowed byte slice (e.g., b"SET", b"get")
    pub name: &'buf [u8],
    /// Command arguments as borrowed byte slices. SmallVec avoids heap allocation
    /// for commands with <= 4 arguments (covers GET, SET, DEL, EXPIRE, etc.)
    pub args: SmallVec<[&'buf [u8]; 4]>,
    /// Total bytes consumed from the buffer (caller must advance by this amount)
    pub bytes_consumed: usize,
}

/// Case-insensitive byte comparison for command names without allocation.
/// Compares against an UPPERCASE reference (e.g., b"GET", b"SET").
#[inline]
pub fn cmd_eq(input: &[u8], uppercase_ref: &[u8]) -> bool {
    input.len() == uppercase_ref.len()
        && input
            .iter()
            .zip(uppercase_ref)
            .all(|(&a, &b)| a.to_ascii_uppercase() == b)
}

impl<'buf> RawCommand<'buf> {
    /// Try to parse a RESP array command from the buffer without copying data.
    ///
    /// Returns `Ok(Some(cmd))` if a complete command was parsed,
    /// `Ok(None)` if the data is incomplete or not a RESP array,
    /// `Err(...)` on protocol errors.
    ///
    /// The buffer is NOT modified. The caller must call `buffer.advance(cmd.bytes_consumed)`
    /// after using the command.
    pub fn try_parse(buffer: &'buf [u8]) -> Result<Option<RawCommand<'buf>>, RespError> {
        // Need at least "*N\r\n" (4 bytes minimum)
        if buffer.len() < 4 {
            return Ok(None);
        }

        // Only handle RESP array commands (the format all Redis clients use)
        if buffer[0] != b'*' {
            return Ok(None);
        }

        // Skip fast path for very large buffers (> 1MB) - let the standard parser handle those
        if buffer.len() > 1024 * 1024 {
            return Ok(None);
        }

        // Parse the array element count from "*N\r\n"
        let mut pos = 1;
        let count_start = pos;

        // Find CRLF after element count
        let crlf_pos = find_crlf(buffer, pos)?;
        if crlf_pos.is_none() {
            return Ok(None); // Incomplete
        }
        let crlf_pos = crlf_pos.unwrap();

        // Parse element count
        let count_str = std::str::from_utf8(&buffer[count_start..crlf_pos])
            .map_err(|e| RespError::Utf8Error(e))?;
        let count: i64 = count_str
            .parse()
            .map_err(|e| RespError::IntegerParseError(e))?;

        if count <= 0 {
            return Ok(None); // Null array or empty - let standard parser handle
        }

        if count > 1_000_000 {
            return Err(RespError::ValueTooLarge(format!(
                "Array length exceeds 1,000,000 limit: {}",
                count
            )));
        }

        let count = count as usize;
        pos = crlf_pos + 2; // Skip past CRLF

        // Parse each bulk string element: "$L\r\nDATA\r\n"
        let mut name: Option<&'buf [u8]> = None;
        let mut args = SmallVec::with_capacity(count.saturating_sub(1).min(8));

        for i in 0..count {
            // Check for '$' bulk string marker
            if pos >= buffer.len() {
                return Ok(None); // Incomplete
            }
            if buffer[pos] != b'$' {
                // Not a bulk string element - fall back to standard parser
                // This handles cases like nested arrays, inline strings, etc.
                return Ok(None);
            }
            pos += 1;

            // Find CRLF after length
            let len_crlf = find_crlf(buffer, pos)?;
            if len_crlf.is_none() {
                return Ok(None); // Incomplete
            }
            let len_crlf = len_crlf.unwrap();

            // Parse bulk string length
            let len_str = std::str::from_utf8(&buffer[pos..len_crlf])
                .map_err(|e| RespError::Utf8Error(e))?;
            let element_len: i64 = len_str
                .parse()
                .map_err(|e| RespError::IntegerParseError(e))?;

            if element_len < 0 {
                // Null bulk string in command - unusual but let standard parser handle
                return Ok(None);
            }

            if element_len > 512 * 1024 * 1024 {
                return Err(RespError::ValueTooLarge(format!(
                    "Bulk string exceeds 512MB limit: {} bytes",
                    element_len
                )));
            }

            let element_len = element_len as usize;
            let data_start = len_crlf + 2; // After length CRLF
            let data_end = data_start + element_len;
            let element_end = data_end + 2; // After data CRLF

            // Check we have all the data for this element
            if buffer.len() < element_end {
                return Ok(None); // Incomplete
            }

            // Verify trailing CRLF
            if buffer[data_end] != b'\r' || buffer[data_end + 1] != b'\n' {
                return Err(RespError::InvalidFormatDetails(
                    "Missing CRLF after bulk string data".to_string(),
                ));
            }

            // Borrow the data slice directly from the buffer - zero copy!
            let data = &buffer[data_start..data_end];

            if i == 0 {
                name = Some(data);
            } else {
                args.push(data);
            }

            pos = element_end;
        }

        match name {
            Some(name) => Ok(Some(RawCommand {
                name,
                args,
                bytes_consumed: pos,
            })),
            None => Ok(None),
        }
    }

    /// Convert to an owned `Command` for dispatch to the command handler.
    /// This performs the minimal allocations needed: one String for the name
    /// (lowercased) and one Vec<u8> per argument.
    pub fn to_command(&self) -> Command {
        Command {
            name: std::str::from_utf8(self.name)
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|_| String::from_utf8_lossy(self.name).to_lowercase()),
            args: self.args.iter().map(|a| a.to_vec()).collect(),
        }
    }

    /// Convert to an owned `RespValue` for paths that need ownership:
    /// MULTI command queuing, AOF logging, RESP3 protocol features.
    pub fn to_resp_value(&self) -> RespValue {
        let mut parts = Vec::with_capacity(1 + self.args.len());
        parts.push(RespValue::BulkString(Some(self.name.to_vec())));
        for arg in &self.args {
            parts.push(RespValue::BulkString(Some(arg.to_vec())));
        }
        RespValue::Array(Some(parts))
    }
}

/// Find the next CRLF (\r\n) in the buffer starting at `from`.
/// Returns `Ok(Some(pos))` where pos is the index of '\r',
/// `Ok(None)` if not found (incomplete data).
#[inline]
fn find_crlf(buffer: &[u8], from: usize) -> Result<Option<usize>, RespError> {
    // Use memchr for efficient scanning if available, otherwise linear scan
    let search = &buffer[from..];
    for i in 0..search.len().saturating_sub(1) {
        if search[i] == b'\r' && search[i + 1] == b'\n' {
            return Ok(Some(from + i));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmd_eq_basic() {
        assert!(cmd_eq(b"GET", b"GET"));
        assert!(cmd_eq(b"get", b"GET"));
        assert!(cmd_eq(b"Get", b"GET"));
        assert!(cmd_eq(b"gEt", b"GET"));
        assert!(!cmd_eq(b"SET", b"GET"));
        assert!(!cmd_eq(b"GE", b"GET"));
        assert!(!cmd_eq(b"GETS", b"GET"));
    }

    #[test]
    fn test_cmd_eq_empty() {
        assert!(cmd_eq(b"", b""));
        assert!(!cmd_eq(b"A", b""));
        assert!(!cmd_eq(b"", b"A"));
    }

    #[test]
    fn test_parse_simple_get() {
        // *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
        let buf = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let raw = RawCommand::try_parse(buf).unwrap().unwrap();
        assert!(cmd_eq(raw.name, b"GET"));
        assert_eq!(raw.args.len(), 1);
        assert_eq!(raw.args[0], b"foo");
        assert_eq!(raw.bytes_consumed, buf.len());
    }

    #[test]
    fn test_parse_set_command() {
        // *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let raw = RawCommand::try_parse(buf).unwrap().unwrap();
        assert!(cmd_eq(raw.name, b"SET"));
        assert_eq!(raw.args.len(), 2);
        assert_eq!(raw.args[0], b"foo");
        assert_eq!(raw.args[1], b"bar");
        assert_eq!(raw.bytes_consumed, buf.len());
    }

    #[test]
    fn test_parse_ping_no_args() {
        // *1\r\n$4\r\nPING\r\n
        let buf = b"*1\r\n$4\r\nPING\r\n";
        let raw = RawCommand::try_parse(buf).unwrap().unwrap();
        assert!(cmd_eq(raw.name, b"PING"));
        assert_eq!(raw.args.len(), 0);
        assert_eq!(raw.bytes_consumed, buf.len());
    }

    #[test]
    fn test_parse_many_args_smallvec_spill() {
        // Command with >4 args to trigger SmallVec heap allocation
        // *6\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n$2\r\nk3\r\n$2\r\nv3\r\n
        // That's 6 elements: MSET + 5 args (which exceeds the SmallVec<[_; 4]> inline capacity)
        let buf = b"*7\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n$2\r\nk3\r\n$2\r\nv3\r\n";
        let raw = RawCommand::try_parse(buf).unwrap().unwrap();
        assert!(cmd_eq(raw.name, b"MSET"));
        assert_eq!(raw.args.len(), 6); // 6 args, spills SmallVec
        assert_eq!(raw.args[0], b"k1");
        assert_eq!(raw.args[1], b"v1");
        assert_eq!(raw.args[4], b"k3");
        assert_eq!(raw.args[5], b"v3");
        assert_eq!(raw.bytes_consumed, buf.len());
    }

    #[test]
    fn test_parse_empty_args() {
        // *2\r\n$3\r\nSET\r\n$0\r\n\r\n  (SET with empty string arg)
        let buf = b"*2\r\n$3\r\nSET\r\n$0\r\n\r\n";
        let raw = RawCommand::try_parse(buf).unwrap().unwrap();
        assert!(cmd_eq(raw.name, b"SET"));
        assert_eq!(raw.args.len(), 1);
        assert_eq!(raw.args[0], b"");
        assert_eq!(raw.bytes_consumed, buf.len());
    }

    #[test]
    fn test_parse_binary_data() {
        // Command with binary data containing RESP-like bytes
        // *2\r\n$3\r\nSET\r\n$5\r\n\x00\r\n\xff\x01\r\n
        let mut buf = Vec::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nSET\r\n$5\r\n");
        buf.extend_from_slice(&[0x00, b'\r', b'\n', 0xFF, 0x01]); // 5 bytes of binary data
        buf.extend_from_slice(b"\r\n");

        let raw = RawCommand::try_parse(&buf).unwrap().unwrap();
        assert!(cmd_eq(raw.name, b"SET"));
        assert_eq!(raw.args.len(), 1);
        assert_eq!(raw.args[0], &[0x00, b'\r', b'\n', 0xFF, 0x01]);
        assert_eq!(raw.bytes_consumed, buf.len());
    }

    #[test]
    fn test_parse_incomplete_array_header() {
        let buf = b"*2\r";
        assert!(RawCommand::try_parse(buf).unwrap().is_none());
    }

    #[test]
    fn test_parse_incomplete_bulk_string() {
        // Complete array header but incomplete bulk string
        let buf = b"*1\r\n$4\r\nPI";
        assert!(RawCommand::try_parse(buf).unwrap().is_none());
    }

    #[test]
    fn test_parse_incomplete_bulk_string_missing_crlf() {
        // Complete data but missing trailing CRLF
        let buf = b"*1\r\n$4\r\nPING";
        assert!(RawCommand::try_parse(buf).unwrap().is_none());
    }

    #[test]
    fn test_parse_not_array() {
        // Inline command or other RESP type - should return None
        let buf = b"+OK\r\n";
        assert!(RawCommand::try_parse(buf).unwrap().is_none());
    }

    #[test]
    fn test_parse_inline_not_handled() {
        // Inline commands are not handled by zero-copy parser
        let buf = b"PING\r\n";
        assert!(RawCommand::try_parse(buf).unwrap().is_none());
    }

    #[test]
    fn test_parse_multiple_commands_in_buffer() {
        // Two commands in the buffer - should parse only the first
        let buf = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let raw = RawCommand::try_parse(buf).unwrap().unwrap();
        assert!(cmd_eq(raw.name, b"PING"));
        assert_eq!(raw.args.len(), 0);
        // bytes_consumed should only cover the first command
        assert_eq!(raw.bytes_consumed, b"*1\r\n$4\r\nPING\r\n".len());
    }

    #[test]
    fn test_to_command_lowercase() {
        let buf = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let raw = RawCommand::try_parse(buf).unwrap().unwrap();
        let cmd = raw.to_command();
        assert_eq!(cmd.name, "get");
        assert_eq!(cmd.args, vec![b"foo".to_vec()]);
    }

    #[test]
    fn test_to_command_mixed_case() {
        let buf = b"*2\r\n$3\r\nGeT\r\n$3\r\nfoo\r\n";
        let raw = RawCommand::try_parse(buf).unwrap().unwrap();
        let cmd = raw.to_command();
        assert_eq!(cmd.name, "get");
    }

    #[test]
    fn test_to_resp_value() {
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let raw = RawCommand::try_parse(buf).unwrap().unwrap();
        let resp = raw.to_resp_value();
        match resp {
            RespValue::Array(Some(parts)) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    RespValue::BulkString(Some(b"SET".to_vec()))
                );
                assert_eq!(
                    parts[1],
                    RespValue::BulkString(Some(b"foo".to_vec()))
                );
                assert_eq!(
                    parts[2],
                    RespValue::BulkString(Some(b"bar".to_vec()))
                );
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_case_insensitive_name() {
        // Verify zero-copy preserves original case in name bytes
        let buf = b"*1\r\n$4\r\nping\r\n";
        let raw = RawCommand::try_parse(buf).unwrap().unwrap();
        assert_eq!(raw.name, b"ping"); // Preserved original case
        assert!(cmd_eq(raw.name, b"PING")); // But compares case-insensitively
    }

    #[test]
    fn test_null_array_returns_none() {
        let buf = b"*-1\r\n";
        assert!(RawCommand::try_parse(buf).unwrap().is_none());
    }

    #[test]
    fn test_empty_array_returns_none() {
        let buf = b"*0\r\n";
        assert!(RawCommand::try_parse(buf).unwrap().is_none());
    }

    #[test]
    fn test_too_small_buffer() {
        assert!(RawCommand::try_parse(b"*2").unwrap().is_none());
        assert!(RawCommand::try_parse(b"").unwrap().is_none());
        assert!(RawCommand::try_parse(b"*").unwrap().is_none());
    }

    #[test]
    fn test_value_too_large_error() {
        // Array with count exceeding the 1M limit - should return error
        let buf = b"*999999999\r\n";
        let result = RawCommand::try_parse(buf);
        assert!(result.is_err());

        // Array with count under the limit but no elements - should return None (incomplete)
        let buf = b"*100\r\n";
        assert!(RawCommand::try_parse(buf).unwrap().is_none());
    }
}
