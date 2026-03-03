mod test_utils;

use rlightning::networking::resp::RespValue;

/// Helper: send HELLO 3 to switch to RESP3, then send a command
async fn send_resp3_command(
    client: &mut rlightning::networking::client::Client,
    command: &str,
    args: &[&str],
) -> RespValue {
    // Switch to RESP3 (if not already)
    let hello_resp = client.send_command_str("HELLO", &["3"]).await.unwrap();
    assert!(
        matches!(hello_resp, RespValue::Map(_)),
        "HELLO 3 should return Map, got {:?}",
        hello_resp
    );
    client.send_command_str(command, args).await.unwrap()
}

/// Test HGETALL returns Map in RESP3
#[tokio::test]
async fn test_resp3_hgetall_returns_map() {
    let addr = test_utils::setup_test_server(410).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Set up hash data
    client
        .send_command_str("HSET", &["myhash", "field1", "value1", "field2", "value2"])
        .await
        .unwrap();

    let response = send_resp3_command(&mut client, "HGETALL", &["myhash"]).await;
    match response {
        RespValue::Map(pairs) => {
            assert_eq!(pairs.len(), 2, "HGETALL should return 2 field-value pairs");
        }
        other => panic!("Expected Map for HGETALL in RESP3, got {:?}", other),
    }
}

/// Test CONFIG GET returns Map in RESP3
#[tokio::test]
async fn test_resp3_config_get_returns_map() {
    let addr = test_utils::setup_test_server(411).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    let response = send_resp3_command(&mut client, "CONFIG", &["GET", "maxmemory"]).await;
    match response {
        RespValue::Map(pairs) => {
            assert!(!pairs.is_empty(), "CONFIG GET maxmemory should return at least 1 pair");
        }
        other => panic!("Expected Map for CONFIG GET in RESP3, got {:?}", other),
    }
}

/// Test XINFO STREAM returns Map in RESP3
#[tokio::test]
async fn test_resp3_xinfo_stream_returns_map() {
    let addr = test_utils::setup_test_server(412).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Create a stream
    client
        .send_command_str("XADD", &["mystream", "*", "name", "alice"])
        .await
        .unwrap();

    let response = send_resp3_command(&mut client, "XINFO", &["STREAM", "mystream"]).await;
    match response {
        RespValue::Map(pairs) => {
            assert!(pairs.len() >= 2, "XINFO STREAM should return multiple kv pairs");
            // Verify a known key exists
            let has_length = pairs.iter().any(|(k, _)| {
                matches!(k, RespValue::BulkString(Some(data)) if data == b"length")
            });
            assert!(has_length, "XINFO STREAM should have 'length' key");
        }
        other => panic!("Expected Map for XINFO STREAM in RESP3, got {:?}", other),
    }
}

/// Test XINFO GROUPS returns Array of Maps in RESP3
#[tokio::test]
async fn test_resp3_xinfo_groups_returns_array_of_maps() {
    let addr = test_utils::setup_test_server(413).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Create stream and group
    client
        .send_command_str("XADD", &["mystream", "*", "name", "alice"])
        .await
        .unwrap();
    client
        .send_command_str("XGROUP", &["CREATE", "mystream", "mygroup", "0"])
        .await
        .unwrap();

    let response = send_resp3_command(&mut client, "XINFO", &["GROUPS", "mystream"]).await;
    match response {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 1, "Should have 1 group");
            match &items[0] {
                RespValue::Map(pairs) => {
                    let has_name = pairs.iter().any(|(k, _)| {
                        matches!(k, RespValue::BulkString(Some(data)) if data == b"name")
                    });
                    assert!(has_name, "Group info should have 'name' key");
                }
                other => panic!("Expected Map for each group, got {:?}", other),
            }
        }
        other => panic!(
            "Expected Array of Maps for XINFO GROUPS in RESP3, got {:?}",
            other
        ),
    }
}

/// Test HSCAN returns [cursor, Map] in RESP3
#[tokio::test]
async fn test_resp3_hscan_returns_cursor_map() {
    let addr = test_utils::setup_test_server(414).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Set up hash data
    client
        .send_command_str("HSET", &["myhash", "f1", "v1", "f2", "v2"])
        .await
        .unwrap();

    let response = send_resp3_command(&mut client, "HSCAN", &["myhash", "0"]).await;
    match response {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 2, "HSCAN should return [cursor, data]");
            match &items[1] {
                RespValue::Map(pairs) => {
                    assert_eq!(pairs.len(), 2, "HSCAN data should have 2 field-value pairs");
                }
                other => panic!("Expected Map as HSCAN data, got {:?}", other),
            }
        }
        other => panic!("Expected Array for HSCAN in RESP3, got {:?}", other),
    }
}

/// Test ZSCAN returns [cursor, Map] in RESP3
#[tokio::test]
async fn test_resp3_zscan_returns_cursor_map() {
    let addr = test_utils::setup_test_server(415).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Set up sorted set
    client
        .send_command_str("ZADD", &["myzset", "1.0", "alice", "2.0", "bob"])
        .await
        .unwrap();

    let response = send_resp3_command(&mut client, "ZSCAN", &["myzset", "0"]).await;
    match response {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 2, "ZSCAN should return [cursor, data]");
            match &items[1] {
                RespValue::Map(pairs) => {
                    assert_eq!(
                        pairs.len(),
                        2,
                        "ZSCAN data should have 2 member-score pairs"
                    );
                }
                other => panic!("Expected Map as ZSCAN data, got {:?}", other),
            }
        }
        other => panic!("Expected Array for ZSCAN in RESP3, got {:?}", other),
    }
}

/// Test SSCAN returns [cursor, Set] in RESP3
#[tokio::test]
async fn test_resp3_sscan_returns_cursor_set() {
    let addr = test_utils::setup_test_server(416).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Set up set
    client
        .send_command_str("SADD", &["myset", "alice", "bob"])
        .await
        .unwrap();

    let response = send_resp3_command(&mut client, "SSCAN", &["myset", "0"]).await;
    match response {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 2, "SSCAN should return [cursor, data]");
            match &items[1] {
                RespValue::Set(members) => {
                    assert_eq!(members.len(), 2, "SSCAN data should have 2 members");
                }
                other => panic!("Expected Set as SSCAN data, got {:?}", other),
            }
        }
        other => panic!("Expected Array for SSCAN in RESP3, got {:?}", other),
    }
}

/// Test XRANGE returns entries with Map field-values in RESP3
#[tokio::test]
async fn test_resp3_xrange_returns_entries_with_map_fields() {
    let addr = test_utils::setup_test_server(417).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Create stream entries
    client
        .send_command_str("XADD", &["mystream", "*", "name", "alice", "age", "30"])
        .await
        .unwrap();
    client
        .send_command_str("XADD", &["mystream", "*", "name", "bob", "age", "25"])
        .await
        .unwrap();

    let response = send_resp3_command(&mut client, "XRANGE", &["mystream", "-", "+"]).await;
    match response {
        RespValue::Array(Some(entries)) => {
            assert_eq!(entries.len(), 2, "XRANGE should return 2 entries");
            // Each entry should be [id, Map{field: value, ...}]
            for entry in &entries {
                if let RespValue::Array(Some(parts)) = entry {
                    assert_eq!(parts.len(), 2, "Each entry should have [id, fields]");
                    match &parts[1] {
                        RespValue::Map(pairs) => {
                            assert_eq!(
                                pairs.len(),
                                2,
                                "Each entry should have 2 field-value pairs"
                            );
                        }
                        other => panic!("Expected Map for entry fields, got {:?}", other),
                    }
                } else {
                    panic!("Expected Array entry, got {:?}", entry);
                }
            }
        }
        other => panic!("Expected Array for XRANGE in RESP3, got {:?}", other),
    }
}

/// Test XREAD returns Map of streams with entry Maps in RESP3
#[tokio::test]
async fn test_resp3_xread_returns_map_with_entry_maps() {
    let addr = test_utils::setup_test_server(418).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Create stream entries
    client
        .send_command_str("XADD", &["stream1", "*", "key", "val"])
        .await
        .unwrap();

    let response =
        send_resp3_command(&mut client, "XREAD", &["COUNT", "10", "STREAMS", "stream1", "0"])
            .await;
    match response {
        RespValue::Map(pairs) => {
            assert_eq!(pairs.len(), 1, "XREAD should return 1 stream");
            assert_eq!(
                pairs[0].0,
                RespValue::BulkString(Some(b"stream1".to_vec()))
            );
            // Entries should have Map field-values
            if let RespValue::Array(Some(entries)) = &pairs[0].1 {
                assert!(!entries.is_empty());
                if let RespValue::Array(Some(entry)) = &entries[0] {
                    assert!(
                        matches!(&entry[1], RespValue::Map(_)),
                        "Entry fields should be Map"
                    );
                }
            } else {
                panic!("Expected Array of entries");
            }
        }
        other => panic!("Expected Map for XREAD in RESP3, got {:?}", other),
    }
}

/// Test COMMAND DOCS returns Map in RESP3
#[tokio::test]
async fn test_resp3_command_docs_returns_map() {
    let addr = test_utils::setup_test_server(419).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    let response = send_resp3_command(&mut client, "COMMAND", &["DOCS", "get"]).await;
    match response {
        RespValue::Map(pairs) => {
            assert!(!pairs.is_empty(), "COMMAND DOCS should return at least 1 pair");
        }
        other => panic!("Expected Map for COMMAND DOCS in RESP3, got {:?}", other),
    }
}

/// Test SMEMBERS returns Set type in RESP3
#[tokio::test]
async fn test_resp3_smembers_returns_set() {
    let addr = test_utils::setup_test_server(420).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    client
        .send_command_str("SADD", &["myset", "a", "b", "c"])
        .await
        .unwrap();

    let response = send_resp3_command(&mut client, "SMEMBERS", &["myset"]).await;
    match response {
        RespValue::Set(items) => {
            assert_eq!(items.len(), 3);
        }
        other => panic!("Expected Set for SMEMBERS in RESP3, got {:?}", other),
    }
}

/// Test that RESP2 mode does NOT convert to Map/Set
#[tokio::test]
async fn test_resp2_no_map_conversion() {
    let addr = test_utils::setup_test_server(421).await.unwrap();
    let mut client = test_utils::create_client(addr).await.unwrap();

    // Stay in RESP2 (no HELLO 3)
    client
        .send_command_str("HSET", &["myhash", "f1", "v1"])
        .await
        .unwrap();

    let response = client
        .send_command_str("HGETALL", &["myhash"])
        .await
        .unwrap();
    match response {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 2, "RESP2 HGETALL should return flat array");
        }
        other => panic!(
            "Expected Array (not Map) for HGETALL in RESP2, got {:?}",
            other
        ),
    }
}
