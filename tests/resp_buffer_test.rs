use bytes::BytesMut;
use rlightning::networking::resp::{RespError, RespValue};

#[test]
fn test_buffer_not_confused_with_stored_data() {
    // Test that stored data patterns are not interpreted as commands
    let test_cases = vec![
        "B64JSON:W3siaWQiOiAx",
        "{\"id\": 1, \"data\": \"test\"}",
        "[1, 2, 3, 4]",
        "HTTP/1.1 200 OK",
        "<html><body>test</body></html>",
    ];

    for data in test_cases {
        let mut buffer = BytesMut::from(data);
        let result = RespValue::parse(&mut buffer);

        assert!(
            result.is_err(),
            "Data '{}' should not parse as RESP command",
            data
        );
        if let Err(e) = result {
            match e {
                RespError::InvalidFormatDetails(msg) => {
                    assert!(
                        msg.contains("Data corruption detected") || msg.contains("Invalid RESP"),
                        "Error should indicate data corruption or invalid RESP for '{}': {}",
                        data,
                        msg
                    );
                }
                _ => panic!("Expected InvalidFormatDetails error for '{}'", data),
            }
        }
    }
}

#[test]
fn test_valid_resp_commands_parse_correctly() {
    // Test that valid RESP commands still parse correctly
    let test_cases = vec![
        (
            b"+OK\r\n".to_vec(),
            RespValue::SimpleString("OK".to_string()),
        ),
        (
            b"-ERR error\r\n".to_vec(),
            RespValue::Error("ERR error".to_string()),
        ),
        (b":42\r\n".to_vec(), RespValue::Integer(42)),
        (
            b"$5\r\nhello\r\n".to_vec(),
            RespValue::BulkString(Some(b"hello".to_vec())),
        ),
        (b"$-1\r\n".to_vec(), RespValue::BulkString(None)),
        (
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".to_vec(),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"GET".to_vec())),
                RespValue::BulkString(Some(b"key".to_vec())),
            ])),
        ),
    ];

    for (data, expected) in test_cases {
        let mut buffer = BytesMut::from(data.as_slice());
        let result = RespValue::parse(&mut buffer);

        assert!(result.is_ok(), "Failed to parse valid RESP command");
        assert_eq!(
            result.unwrap(),
            Some(expected),
            "Parsed value doesn't match expected"
        );
        assert_eq!(buffer.len(), 0, "Buffer should be consumed after parsing");
    }
}

#[test]
fn test_partial_command_handling() {
    // Test that partial commands are handled correctly
    let full_command = b"*2\r\n$3\r\nSET\r\n$5\r\nhello\r\n".to_vec();

    // Split the command at various points
    let split_points = vec![5, 10, 15, 20];

    for split_point in split_points {
        let mut buffer = BytesMut::from(&full_command[..split_point]);
        let result = RespValue::parse(&mut buffer);

        // Should return Ok(None) for incomplete data
        assert!(result.is_ok(), "Should not error on partial data");
        assert_eq!(
            result.unwrap(),
            None,
            "Should return None for incomplete data"
        );

        // Buffer may be partially consumed depending on the parser implementation
        // The important thing is that incomplete data doesn't result in errors
        // Some parsers may consume bytes they can't fully parse yet
        let consumed = split_point - buffer.len();

        // Now add the rest and parse again
        if consumed > 0 {
            // If some bytes were consumed, we need to resend them
            buffer.clear();
            buffer.extend_from_slice(&full_command);
        } else {
            buffer.extend_from_slice(&full_command[split_point..]);
        }
        let result = RespValue::parse(&mut buffer);

        assert!(result.is_ok(), "Should parse complete command");
        assert!(result.unwrap().is_some(), "Should return parsed command");
        assert_eq!(
            buffer.len(),
            0,
            "Buffer should be consumed after successful parse"
        );
    }
}

#[test]
fn test_large_bulk_string_with_special_chars() {
    // Test that large bulk strings with special characters are handled correctly
    let data =
        "B64JSON:W3siaWQiOiAxLCAibmFtZSI6ICJ0ZXN0In0sIHsiaWQiOiAyLCAibmFtZSI6ICJ0ZXN0MiJ9XQ==";
    let bulk_string_cmd = format!("${}\r\n{}\r\n", data.len(), data);

    let mut buffer = BytesMut::from(bulk_string_cmd.as_bytes());
    let result = RespValue::parse(&mut buffer);

    assert!(
        result.is_ok(),
        "Should parse bulk string with special chars"
    );
    if let Some(RespValue::BulkString(Some(parsed_data))) = result.unwrap() {
        assert_eq!(
            parsed_data,
            data.as_bytes(),
            "Bulk string data should match"
        );
    } else {
        panic!("Expected bulk string");
    }
    assert_eq!(buffer.len(), 0, "Buffer should be consumed");
}

#[test]
fn test_buffer_not_reinterpreted_after_error() {
    // Test that buffer is properly cleared after errors
    let mut buffer = BytesMut::from("INVALID_DATA_THAT_LOOKS_LIKE_JSON{\"test\": 1}");

    // First parse should fail
    let result = RespValue::parse(&mut buffer);
    assert!(result.is_err(), "Invalid data should not parse");

    // Buffer should still contain the invalid data
    assert!(!buffer.is_empty(), "Buffer should not be consumed on error");

    // Clear the buffer manually (as server would do)
    buffer.clear();

    // Now add valid command
    buffer.extend_from_slice(b"+OK\r\n");
    let result = RespValue::parse(&mut buffer);

    assert!(result.is_ok(), "Should parse valid command after clearing");
    assert_eq!(
        result.unwrap(),
        Some(RespValue::SimpleString("OK".to_string())),
        "Should parse OK response"
    );
}

#[test]
fn test_consecutive_commands_in_buffer() {
    // Test multiple commands in buffer are parsed correctly
    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(b"+OK\r\n");
    buffer.extend_from_slice(b":100\r\n");
    buffer.extend_from_slice(b"$4\r\ntest\r\n");

    // Parse first command
    let result = RespValue::parse(&mut buffer);
    assert_eq!(
        result.unwrap(),
        Some(RespValue::SimpleString("OK".to_string())),
        "First command should parse"
    );

    // Parse second command
    let result = RespValue::parse(&mut buffer);
    assert_eq!(
        result.unwrap(),
        Some(RespValue::Integer(100)),
        "Second command should parse"
    );

    // Parse third command
    let result = RespValue::parse(&mut buffer);
    assert_eq!(
        result.unwrap(),
        Some(RespValue::BulkString(Some(b"test".to_vec()))),
        "Third command should parse"
    );

    // Buffer should be empty now
    assert_eq!(buffer.len(), 0, "Buffer should be fully consumed");
}

#[test]
fn test_binary_safe_data_in_bulk_string() {
    // Test that binary data including null bytes is handled correctly
    let binary_data = vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD, b'A', b'B', 0x00];
    let bulk_string_cmd = format!("${}\r\n", binary_data.len());

    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(bulk_string_cmd.as_bytes());
    buffer.extend_from_slice(&binary_data);
    buffer.extend_from_slice(b"\r\n");

    let result = RespValue::parse(&mut buffer);

    assert!(result.is_ok(), "Should parse binary data in bulk string");
    if let Some(RespValue::BulkString(Some(parsed_data))) = result.unwrap() {
        assert_eq!(parsed_data, binary_data, "Binary data should be preserved");
    } else {
        panic!("Expected bulk string with binary data");
    }
}
