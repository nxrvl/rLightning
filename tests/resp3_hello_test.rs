mod test_utils;

use rlightning::networking::resp::RespValue;

/// Test HELLO command with no arguments (defaults to RESP2)
#[tokio::test]
async fn test_hello_default() {
    let addr = test_utils::setup_test_server(400).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // HELLO with no args should return server info as RESP2 array
    let response = client.send_command_str("HELLO", &[]).await.unwrap();

    // RESP2 response is a flat array of alternating key/value pairs
    if let RespValue::Array(Some(items)) = &response {
        // Should have key-value pairs (even number of elements)
        assert!(
            items.len() % 2 == 0,
            "Expected even number of elements, got {}",
            items.len()
        );
        assert!(
            items.len() >= 14,
            "Expected at least 14 elements (7 pairs), got {}",
            items.len()
        );

        // Find the "server" key and verify value
        let mut found_server = false;
        let mut found_proto = false;
        for i in (0..items.len()).step_by(2) {
            if let RespValue::BulkString(Some(key)) = &items[i] {
                let key_str = String::from_utf8_lossy(key);
                match key_str.as_ref() {
                    "server" => {
                        if let RespValue::BulkString(Some(val)) = &items[i + 1] {
                            assert_eq!(String::from_utf8_lossy(val), "rLightning");
                            found_server = true;
                        }
                    }
                    "proto" => {
                        if let RespValue::Integer(val) = &items[i + 1] {
                            assert_eq!(*val, 2, "Default protocol should be RESP2");
                            found_proto = true;
                        }
                    }
                    _ => {}
                }
            }
        }
        assert!(found_server, "Response should contain 'server' key");
        assert!(found_proto, "Response should contain 'proto' key");
    } else {
        panic!(
            "Expected Array response for HELLO in RESP2 mode, got {:?}",
            response
        );
    }
}

/// Test HELLO 2 explicitly requests RESP2
#[tokio::test]
async fn test_hello_resp2() {
    let addr = test_utils::setup_test_server(401).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    let response = client.send_command_str("HELLO", &["2"]).await.unwrap();

    // Should get RESP2 array response
    if let RespValue::Array(Some(items)) = &response {
        // Find proto value
        for i in (0..items.len()).step_by(2) {
            if let RespValue::BulkString(Some(key)) = &items[i] {
                if String::from_utf8_lossy(key) == "proto" {
                    if let RespValue::Integer(val) = &items[i + 1] {
                        assert_eq!(*val, 2);
                        return;
                    }
                }
            }
        }
        panic!("Did not find 'proto' key in response");
    } else {
        panic!("Expected Array response, got {:?}", response);
    }
}

/// Test HELLO 3 switches to RESP3
#[tokio::test]
async fn test_hello_resp3() {
    let addr = test_utils::setup_test_server(402).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    let response = client.send_command_str("HELLO", &["3"]).await.unwrap();

    // RESP3 response should be a Map type
    if let RespValue::Map(pairs) = &response {
        let mut found_server = false;
        let mut found_proto = false;
        for (key, value) in pairs {
            if let RespValue::BulkString(Some(k)) = key {
                let key_str = String::from_utf8_lossy(k);
                match key_str.as_ref() {
                    "server" => {
                        if let RespValue::BulkString(Some(val)) = value {
                            assert_eq!(String::from_utf8_lossy(val), "rLightning");
                            found_server = true;
                        }
                    }
                    "proto" => {
                        if let RespValue::Integer(val) = value {
                            assert_eq!(*val, 3);
                            found_proto = true;
                        }
                    }
                    _ => {}
                }
            }
        }
        assert!(found_server, "RESP3 response should contain 'server' key");
        assert!(
            found_proto,
            "RESP3 response should contain 'proto' key with value 3"
        );
    } else {
        panic!("Expected Map response for HELLO 3, got {:?}", response);
    }
}

/// Test HELLO with invalid protocol version
#[tokio::test]
async fn test_hello_invalid_version() {
    let addr = test_utils::setup_test_server(403).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    let response = client.send_command_str("HELLO", &["5"]).await.unwrap();

    // Should get an error response
    if let RespValue::Error(msg) = &response {
        assert!(
            msg.contains("NOPROTO"),
            "Error should contain NOPROTO, got: {}",
            msg
        );
    } else {
        panic!(
            "Expected Error response for invalid version, got {:?}",
            response
        );
    }
}

/// Test that HELLO 3 followed by HELLO 2 switches back to RESP2
#[tokio::test]
async fn test_hello_version_switching() {
    let addr = test_utils::setup_test_server(404).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Switch to RESP3
    let response = client.send_command_str("HELLO", &["3"]).await.unwrap();
    assert!(
        matches!(response, RespValue::Map(_)),
        "HELLO 3 should return Map"
    );

    // Switch back to RESP2
    let response = client.send_command_str("HELLO", &["2"]).await.unwrap();
    assert!(
        matches!(response, RespValue::Array(Some(_))),
        "HELLO 2 should return Array, got {:?}",
        response
    );
}

/// Test that regular commands work after HELLO
#[tokio::test]
async fn test_commands_after_hello() {
    let addr = test_utils::setup_test_server(405).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Issue HELLO first
    let _response = client.send_command_str("HELLO", &[]).await.unwrap();

    // Regular commands should still work
    let response = client.send_command_str("PING", &[]).await.unwrap();
    assert_eq!(response, RespValue::SimpleString("PONG".to_string()));

    let response = client
        .send_command_str("SET", &["hello_key", "hello_value"])
        .await
        .unwrap();
    assert_eq!(response, RespValue::SimpleString("OK".to_string()));

    let response = client
        .send_command_str("GET", &["hello_key"])
        .await
        .unwrap();
    assert_eq!(
        response,
        RespValue::BulkString(Some(b"hello_value".to_vec()))
    );
}

/// Test HELLO response contains all expected fields
#[tokio::test]
async fn test_hello_response_fields() {
    let addr = test_utils::setup_test_server(406).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    let response = client.send_command_str("HELLO", &[]).await.unwrap();

    if let RespValue::Array(Some(items)) = &response {
        let mut keys_found = std::collections::HashSet::new();
        for i in (0..items.len()).step_by(2) {
            if let RespValue::BulkString(Some(key)) = &items[i] {
                keys_found.insert(String::from_utf8_lossy(key).to_string());
            }
        }

        let expected_keys = [
            "server", "version", "proto", "id", "mode", "role", "modules",
        ];
        for key in &expected_keys {
            assert!(
                keys_found.contains(*key),
                "Missing expected key '{}' in HELLO response",
                key
            );
        }
    } else {
        panic!("Expected Array response, got {:?}", response);
    }
}
