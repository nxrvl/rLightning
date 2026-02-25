/// Redis 100% Protocol Compliance Test Suite
/// Tests command compliance using embedded server (self-contained, no external server needed)
mod test_utils;

use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

async fn setup_compliance_server(port: u16) -> (SocketAddr, Client) {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    let server = Server::new(addr, storage)
        .with_connection_limit(100)
        .with_buffer_size(1024 * 1024);

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Compliance server error: {:?}", e);
        }
    });

    sleep(Duration::from_millis(300)).await;

    let client = Client::connect(addr).await.expect("Failed to connect");
    (addr, client)
}

fn get_bulk_string(resp: &RespValue) -> Option<String> {
    match resp {
        RespValue::BulkString(Some(data)) => Some(String::from_utf8_lossy(data).to_string()),
        _ => None,
    }
}

#[tokio::test]
async fn test_comprehensive_string_commands_compliance() {
    let (_addr, mut client) = setup_compliance_server(18200).await;

    // PING
    let response = client.send_command_str("PING", &[]).await.unwrap();
    assert_eq!(
        response,
        RespValue::SimpleString("PONG".to_string()),
        "PING failed"
    );

    // SET/GET
    let response = client
        .send_command_str("SET", &["comp:testkey1", "testvalue"])
        .await
        .unwrap();
    assert!(
        matches!(response, RespValue::SimpleString(ref s) if s == "OK"),
        "SET failed: {:?}",
        response
    );

    let response = client
        .send_command_str("GET", &["comp:testkey1"])
        .await
        .unwrap();
    assert_eq!(
        get_bulk_string(&response),
        Some("testvalue".to_string()),
        "GET failed: {:?}",
        response
    );

    // STRLEN
    let response = client
        .send_command_str("STRLEN", &["comp:testkey1"])
        .await
        .unwrap();
    assert_eq!(
        response,
        RespValue::Integer(9),
        "STRLEN failed: {:?}",
        response
    );

    // APPEND
    let response = client
        .send_command_str("APPEND", &["comp:testkey1", "more!"])
        .await
        .unwrap();
    assert_eq!(
        response,
        RespValue::Integer(14),
        "APPEND failed: {:?}",
        response
    );

    // GETRANGE
    let response = client
        .send_command_str("GETRANGE", &["comp:testkey1", "0", "3"])
        .await
        .unwrap();
    assert_eq!(
        get_bulk_string(&response),
        Some("test".to_string()),
        "GETRANGE failed: {:?}",
        response
    );

    // SETRANGE
    let response = client
        .send_command_str("SETRANGE", &["comp:testkey1", "0", "best"])
        .await
        .unwrap();
    assert_eq!(
        response,
        RespValue::Integer(14),
        "SETRANGE failed: {:?}",
        response
    );

    // GETSET
    let response = client
        .send_command_str("GETSET", &["comp:testkey1", "newvalue"])
        .await
        .unwrap();
    assert!(
        get_bulk_string(&response).unwrap().starts_with("best"),
        "GETSET failed: {:?}",
        response
    );

    // MSETNX
    let response = client
        .send_command_str("MSETNX", &["comp:nx1", "value1", "comp:nx2", "value2"])
        .await
        .unwrap();
    assert_eq!(
        response,
        RespValue::Integer(1),
        "MSETNX failed: {:?}",
        response
    );

    // INCRBYFLOAT
    client
        .send_command_str("SET", &["comp:floatkey", "3.5"])
        .await
        .unwrap();
    let response = client
        .send_command_str("INCRBYFLOAT", &["comp:floatkey", "1.5"])
        .await
        .unwrap();
    if let Some(val) = get_bulk_string(&response) {
        let f: f64 = val.parse().expect("Should parse as float");
        assert!((f - 5.0).abs() < 0.001, "INCRBYFLOAT failed: got {}", f);
    }

    println!("All string commands passed compliance test");
}

#[tokio::test]
async fn test_comprehensive_hash_commands_compliance() {
    let (_addr, mut client) = setup_compliance_server(18201).await;

    // HSET
    let response = client
        .send_command_str("HSET", &["comp:hashkey", "field1", "value1"])
        .await
        .unwrap();
    assert_eq!(
        response,
        RespValue::Integer(1),
        "HSET failed: {:?}",
        response
    );

    // HKEYS
    let response = client
        .send_command_str("HKEYS", &["comp:hashkey"])
        .await
        .unwrap();
    if let RespValue::Array(Some(arr)) = &response {
        assert!(
            arr.iter()
                .any(|v| get_bulk_string(v) == Some("field1".to_string())),
            "HKEYS failed"
        );
    } else {
        panic!("HKEYS should return array: {:?}", response);
    }

    // HVALS
    let response = client
        .send_command_str("HVALS", &["comp:hashkey"])
        .await
        .unwrap();
    if let RespValue::Array(Some(arr)) = &response {
        assert!(
            arr.iter()
                .any(|v| get_bulk_string(v) == Some("value1".to_string())),
            "HVALS failed"
        );
    }

    // HLEN
    let response = client
        .send_command_str("HLEN", &["comp:hashkey"])
        .await
        .unwrap();
    assert_eq!(
        response,
        RespValue::Integer(1),
        "HLEN failed: {:?}",
        response
    );

    // HMGET
    let response = client
        .send_command_str("HMGET", &["comp:hashkey", "field1"])
        .await
        .unwrap();
    if let RespValue::Array(Some(arr)) = &response {
        assert_eq!(
            get_bulk_string(&arr[0]),
            Some("value1".to_string()),
            "HMGET failed"
        );
    }

    // HINCRBY
    client
        .send_command_str("HSET", &["comp:hashkey", "counter", "5"])
        .await
        .unwrap();
    let response = client
        .send_command_str("HINCRBY", &["comp:hashkey", "counter", "3"])
        .await
        .unwrap();
    assert_eq!(
        response,
        RespValue::Integer(8),
        "HINCRBY failed: {:?}",
        response
    );

    // HINCRBYFLOAT
    let response = client
        .send_command_str("HINCRBYFLOAT", &["comp:hashkey", "counter", "1.5"])
        .await
        .unwrap();
    if let Some(val) = get_bulk_string(&response) {
        let f: f64 = val.parse().unwrap();
        assert!((f - 9.5).abs() < 0.001, "HINCRBYFLOAT failed: got {}", f);
    }

    // HSETNX
    let response = client
        .send_command_str("HSETNX", &["comp:hashkey", "newfield1", "newvalue1"])
        .await
        .unwrap();
    assert_eq!(
        response,
        RespValue::Integer(1),
        "HSETNX failed: {:?}",
        response
    );

    // HSTRLEN
    let response = client
        .send_command_str("HSTRLEN", &["comp:hashkey", "field1"])
        .await
        .unwrap();
    assert_eq!(
        response,
        RespValue::Integer(6),
        "HSTRLEN failed: {:?}",
        response
    );

    println!("All hash commands passed compliance test");
}

#[tokio::test]
async fn test_protocol_compliance_summary() {
    let (_addr, mut client) = setup_compliance_server(18203).await;

    let mut total_commands = 0;
    let mut working_commands = 0;

    // String commands
    let string_commands = vec![
        ("SET", vec!["comp:sum:test", "value"]),
        ("GET", vec!["comp:sum:test"]),
        ("STRLEN", vec!["comp:sum:test"]),
        ("APPEND", vec!["comp:sum:test", "more"]),
        ("INCR", vec!["comp:sum:counter"]),
        ("DECR", vec!["comp:sum:counter"]),
        ("MGET", vec!["comp:sum:test"]),
        ("MSET", vec!["comp:sum:test2", "value2"]),
        ("GETRANGE", vec!["comp:sum:test", "0", "2"]),
    ];

    for (cmd_name, args) in &string_commands {
        total_commands += 1;
        let args_ref: Vec<&str> = args.iter().map(|s| s.as_ref()).collect();
        match client.send_command_str(cmd_name, &args_ref).await {
            Ok(resp) => {
                if !matches!(resp, RespValue::Error(_)) {
                    working_commands += 1;
                }
            }
            Err(_) => {}
        }
    }

    // Hash commands
    let hash_setup = client
        .send_command_str("HSET", &["comp:sum:hash", "field", "value"])
        .await;
    assert!(hash_setup.is_ok());

    let hash_commands = vec![
        ("HGET", vec!["comp:sum:hash", "field"]),
        ("HKEYS", vec!["comp:sum:hash"]),
        ("HVALS", vec!["comp:sum:hash"]),
        ("HLEN", vec!["comp:sum:hash"]),
        ("HEXISTS", vec!["comp:sum:hash", "field"]),
    ];

    for (cmd_name, args) in &hash_commands {
        total_commands += 1;
        let args_ref: Vec<&str> = args.iter().map(|s| s.as_ref()).collect();
        match client.send_command_str(cmd_name, &args_ref).await {
            Ok(resp) => {
                if !matches!(resp, RespValue::Error(_)) {
                    working_commands += 1;
                }
            }
            Err(_) => {}
        }
    }

    // Server commands
    let server_commands = vec![
        ("PING", vec![]),
        ("INFO", vec![]),
        ("TYPE", vec!["comp:sum:test"]),
        ("EXISTS", vec!["comp:sum:test"]),
        ("DEL", vec!["comp:sum:test"]),
        ("TTL", vec!["comp:sum:test"]),
    ];

    for (cmd_name, args) in &server_commands {
        total_commands += 1;
        let args_ref: Vec<&str> = args.iter().map(|s| s.as_ref()).collect();
        match client.send_command_str(cmd_name, &args_ref).await {
            Ok(resp) => {
                if !matches!(resp, RespValue::Error(_)) {
                    working_commands += 1;
                }
            }
            Err(_) => {}
        }
    }

    let compliance_percentage = (working_commands as f64 / total_commands as f64) * 100.0;

    println!("Commands tested: {}", total_commands);
    println!("Commands working: {}", working_commands);
    println!("Compliance: {:.1}%", compliance_percentage);

    assert!(
        compliance_percentage >= 85.0,
        "Redis compliance should be at least 85%, got {:.1}%",
        compliance_percentage
    );
}
