/// Test to verify the exact scenarios from the bug report

use rlightning::storage::engine::{StorageConfig, StorageEngine};
use rlightning::networking::resp::RespValue;
use rlightning::networking::client::Client;
use rlightning::networking::server::Server;
use std::sync::Arc;

/// Test the exact Python scenario from the bug report
#[tokio::test]
async fn test_bug_report_exact_scenario() {
    let mut config = StorageConfig::default();
    config.max_value_size = 10 * 1024 * 1024;
    config.max_memory = 100 * 1024 * 1024;

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 19020));
    let storage = Arc::new(StorageEngine::new(config));
    let server = Server::new(addr, Arc::clone(&storage))
        .with_buffer_size(1024 * 1024);

    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = Client::connect(addr).await.expect("Failed to connect");

    // Test sizes from the bug report
    let test_sizes = vec![8000, 8500, 9000, 9004, 9005, 10000];
    let mut results = Vec::new();

    for size in test_sizes {
        let value = "x".repeat(size);
        let key = format!("test:{}", size);

        match client.send_command_str("SET", &[&key, &value]).await {
            Ok(RespValue::SimpleString(s)) if s == "OK" => {
                // Verify we can GET it back
                match client.send_command_str("GET", &[&key]).await {
                    Ok(RespValue::BulkString(Some(data))) if data.len() == size => {
                        results.push((size, "SUCCESS"));
                    }
                    Ok(other) => {
                        results.push((size, "FAILED - GET returned unexpected"));
                        eprintln!("{} bytes: GET unexpected: {:?}", size, other);
                    }
                    Err(e) => {
                        results.push((size, "FAILED - GET error"));
                        eprintln!("{} bytes: GET error: {}", size, e);
                    }
                }
            }
            Ok(other) => {
                results.push((size, "FAILED - SET unexpected response"));
                eprintln!("{} bytes: SET unexpected: {:?}", size, other);
            }
            Err(e) => {
                results.push((size, "FAILED - SET error"));
                eprintln!("{} bytes: SET error: {}", size, e);
            }
        }
    }

    // Print results in the format from the bug report
    println!("\n=== Test Results ===");
    for (size, status) in &results {
        println!("{} bytes: {}", size, status);
    }

    // Verify all tests passed
    let all_success = results.iter().all(|(_, status)| *status == "SUCCESS");
    assert!(all_success, "Some tests failed! Results: {:?}", results);
}

/// Test for Bug #2: Type mismatch after protocol error
#[tokio::test]
async fn test_type_mismatch_after_error() {
    let mut config = StorageConfig::default();
    config.max_value_size = 10 * 1024 * 1024;
    config.max_memory = 100 * 1024 * 1024;

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 19021));
    let storage = Arc::new(StorageEngine::new(config));
    let server = Server::new(addr, Arc::clone(&storage))
        .with_buffer_size(1024 * 1024);

    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = Client::connect(addr).await.expect("Failed to connect");

    let key = "bnf:story_detail:test123";
    let large_value = "x".repeat(50000);

    // Set a large value
    match client.send_command_str("SET", &[key, &large_value]).await {
        Ok(RespValue::SimpleString(s)) if s == "OK" => {
            println!("✓ Large SET succeeded");
        }
        Ok(other) => panic!("SET failed with unexpected response: {:?}", other),
        Err(e) => panic!("SET failed with error: {}", e),
    }

    // Now GET it back - should NOT return "OK" or have type mismatch
    match client.send_command_str("GET", &[key]).await {
        Ok(RespValue::BulkString(Some(data))) => {
            assert_eq!(data.len(), 50000);
            let value_str = String::from_utf8_lossy(&data);
            assert_eq!(value_str, large_value);
            println!("✓ GET returned correct value (no type mismatch)");
        }
        Ok(RespValue::SimpleString(s)) if s == "OK" => {
            panic!("Bug #2 detected: GET returned 'OK' instead of value!");
        }
        Ok(other) => {
            panic!("GET returned unexpected type: {:?}", other);
        }
        Err(e) => {
            panic!("GET failed: {}", e);
        }
    }

    // Check TYPE command
    match client.send_command_str("TYPE", &[key]).await {
        Ok(RespValue::SimpleString(t)) => {
            assert_eq!(t, "string", "TYPE should return 'string', got '{}'", t);
            println!("✓ TYPE command returns correct type");
        }
        Ok(other) => panic!("TYPE returned unexpected: {:?}", other),
        Err(e) => panic!("TYPE failed: {}", e),
    }
}

/// Test base64 JSON data (from the bug report error messages)
#[tokio::test]
async fn test_base64_json_data() {
    let mut config = StorageConfig::default();
    config.max_value_size = 10 * 1024 * 1024;
    config.max_memory = 100 * 1024 * 1024;

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 19022));
    let storage = Arc::new(StorageEngine::new(config));
    let server = Server::new(addr, Arc::clone(&storage))
        .with_buffer_size(1024 * 1024);

    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = Client::connect(addr).await.expect("Failed to connect");

    // Create a value that starts with the characters from the bug report
    let json_like_value = r#"B64JSON:W3siaWQiOiAyLCAibmFtZSI6ICJUZXN0In1d"#.repeat(1000); // ~46KB

    let key = "bnf:stories_list:test";

    match client.send_command_str("SET", &[key, &json_like_value]).await {
        Ok(RespValue::SimpleString(s)) if s == "OK" => {
            println!("✓ Base64-like data SET succeeded");

            // Verify GET
            match client.send_command_str("GET", &[key]).await {
                Ok(RespValue::BulkString(Some(data))) => {
                    let retrieved = String::from_utf8_lossy(&data);
                    assert_eq!(retrieved, json_like_value);
                    println!("✓ Base64-like data retrieved correctly");
                }
                Ok(other) => panic!("GET unexpected: {:?}", other),
                Err(e) => panic!("GET failed: {}", e),
            }
        }
        Ok(other) => panic!("SET failed: {:?}", other),
        Err(e) => panic!("SET failed: {}", e),
    }
}
