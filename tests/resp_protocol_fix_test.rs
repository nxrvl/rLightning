use bytes::BytesMut;
use rlightning::networking::resp::{RespError, RespValue};
use rlightning::networking::resp_parser_state::StatefulRespParser;
use rlightning::storage::engine::{StorageConfig, StorageEngine};
use std::sync::Arc;

/// Test the critical RESP protocol fixes
#[tokio::test]
async fn test_resp_protocol_data_command_separation() {
    // Test 1: Ensure parser rejects data that looks like commands
    let mut buffer = BytesMut::from("B64JSON:W3...");
    let result = RespValue::parse(&mut buffer);

    // Should fail because this isn't a valid RESP command
    assert!(
        result.is_err(),
        "Parser should reject data that looks like stored content"
    );

    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(
            error_msg.contains("Data corruption detected")
                || error_msg.contains("Invalid RESP command"),
            "Error should indicate data corruption detection, got: {}",
            error_msg
        );
    }
}

#[tokio::test]
async fn test_stateful_parser_prevents_confusion() {
    let mut parser = StatefulRespParser::new();

    // Test valid RESP command
    let mut buffer = BytesMut::from("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    let result = parser.parse_with_state(&mut buffer);
    assert!(result.is_ok(), "Valid RESP command should parse correctly");

    // Reset parser
    parser.reset();

    // Test rejection of data in command mode
    let mut buffer = BytesMut::from("B64JSON:W3randomdata...");
    let result = parser.parse_with_state(&mut buffer);
    assert!(
        result.is_err(),
        "Stateful parser should reject data in command mode"
    );
}

#[tokio::test]
async fn test_large_json_storage_without_corruption() {
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);

    // Create large JSON-like data that previously caused issues
    let large_json = format!(
        r#"{{
        "data": "B64JSON:W3{}",
        "size": {},
        "nested": {{
            "more_data": "{}",
            "control_chars": "\r\n\t"
        }}
    }}"#,
        "x".repeat(10000),
        15000,
        "y".repeat(5000)
    );

    let key = b"test_large_json".to_vec();
    let value = large_json.as_bytes().to_vec();

    // Store the large JSON
    let result = storage.set(key.clone(), value.clone(), None).await;
    assert!(
        result.is_ok(),
        "Should be able to store large JSON data: {:?}",
        result
    );

    // Retrieve the data
    let retrieved = storage.get(&key).await;
    assert!(
        retrieved.is_ok(),
        "Should be able to retrieve large JSON data: {:?}",
        retrieved
    );

    let retrieved_value = retrieved.unwrap();
    assert!(retrieved_value.is_some(), "Retrieved value should exist");
    assert_eq!(
        retrieved_value.unwrap(),
        value,
        "Retrieved data should match stored data exactly"
    );
}

#[tokio::test]
async fn test_bulk_string_with_exact_length() {
    // Test that bulk strings are parsed with exact length validation
    let test_data = "Hello, World!";
    let bulk_string = format!("${}\r\n{}\r\n", test_data.len(), test_data);

    let mut buffer = BytesMut::from(bulk_string.as_bytes());
    let result = RespValue::parse(&mut buffer);

    assert!(result.is_ok(), "Valid bulk string should parse correctly");

    if let Ok(Some(RespValue::BulkString(Some(data)))) = result {
        assert_eq!(
            data,
            test_data.as_bytes(),
            "Bulk string data should match exactly"
        );
    } else {
        panic!("Expected BulkString result");
    }

    // Buffer should be empty after parsing
    assert!(buffer.is_empty(), "Buffer should be completely consumed");
}

#[tokio::test]
async fn test_protocol_with_problematic_patterns() {
    // Test various patterns that previously caused issues
    let problematic_patterns = vec![
        "B64JSON:W3data",
        "+FAKE_OK\r\nmore_data",
        "$999\r\nshort_data",
        "*2\r\nincomplete",
        "{\"json\": \"with\r\ncontrol\tchars\"}",
    ];

    for pattern in problematic_patterns {
        let mut buffer = BytesMut::from(pattern);
        let result = RespValue::parse(&mut buffer);

        // These should either parse correctly (if valid RESP) or fail cleanly
        match result {
            Ok(_) => {
                // If it parsed, it was valid RESP
                println!("Pattern '{}' parsed as valid RESP", pattern);
            }
            Err(e) => {
                // If it failed, error should be descriptive
                println!("Pattern '{}' failed as expected: {}", pattern, e);
                assert!(
                    !e.to_string().is_empty(),
                    "Error message should be descriptive"
                );
            }
        }
    }
}

#[tokio::test]
async fn test_set_get_with_special_data() {
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);

    // Test data that starts with RESP command characters
    let test_cases = vec![
        (b"key1".to_vec(), b"+not_a_simple_string".to_vec()),
        (b"key2".to_vec(), b"-not_an_error".to_vec()),
        (b"key3".to_vec(), b":12345_not_an_integer".to_vec()),
        (b"key4".to_vec(), b"$10\r\nnot_bulk_string".to_vec()),
        (b"key5".to_vec(), b"*2\r\nnot_an_array".to_vec()),
    ];

    for (key, value) in test_cases {
        // Store the data
        let set_result = storage.set(key.clone(), value.clone(), None).await;
        assert!(
            set_result.is_ok(),
            "Should be able to store data that looks like RESP commands"
        );

        // Retrieve the data
        let get_result = storage.get(&key).await;
        assert!(
            get_result.is_ok(),
            "Should be able to retrieve special data"
        );

        let retrieved = get_result.unwrap();
        assert!(retrieved.is_some(), "Retrieved data should exist");
        assert_eq!(
            retrieved.unwrap(),
            value,
            "Retrieved data should match stored data exactly"
        );
    }
}

#[tokio::test]
async fn test_concurrent_operations_with_large_data() {
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);

    // Test concurrent SET/GET operations with large data
    let mut handles = vec![];

    for i in 0..10 {
        let storage_clone = Arc::clone(&storage);
        let handle = tokio::spawn(async move {
            let key = format!("concurrent_key_{}", i).into_bytes();
            let large_data = format!("B64JSON:W3{}", "x".repeat(10000 + i * 1000)).into_bytes();

            // Set data
            storage_clone
                .set(key.clone(), large_data.clone(), None)
                .await
                .unwrap();

            // Get data
            let retrieved = storage_clone.get(&key).await.unwrap();
            assert_eq!(retrieved, Some(large_data));

            i
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        let result = handle.await;
        assert!(result.is_ok(), "Concurrent operation should succeed");
    }
}

#[test]
fn test_resp_value_serialization_safety() {
    // Test that serialization produces valid RESP protocol
    let test_cases = vec![
        RespValue::SimpleString("OK".to_string()),
        RespValue::Error("ERR test error".to_string()),
        RespValue::Integer(42),
        RespValue::BulkString(Some(b"test data".to_vec())),
        RespValue::BulkString(Some(b"B64JSON:W3data".to_vec())),
        RespValue::BulkString(None),
        RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ])),
    ];

    for value in test_cases {
        let serialized = value.serialize();
        assert!(
            serialized.is_ok(),
            "Serialization should succeed for: {:?}",
            value
        );

        let bytes = serialized.unwrap();

        // Verify that serialized data can be parsed back
        let mut buffer = BytesMut::from(&bytes[..]);
        let parsed = RespValue::parse(&mut buffer);

        match parsed {
            Ok(Some(parsed_value)) => {
                // For bulk strings, compare the actual data
                match (&value, &parsed_value) {
                    (RespValue::BulkString(original), RespValue::BulkString(parsed)) => {
                        assert_eq!(original, parsed, "Bulk string round-trip failed");
                    }
                    _ => assert_eq!(value, parsed_value, "Round-trip serialization failed"),
                }
            }
            Ok(None) => panic!("Parsing returned None for: {:?}", value),
            Err(e) => panic!("Parsing failed for serialized {:?}: {}", value, e),
        }
    }
}
