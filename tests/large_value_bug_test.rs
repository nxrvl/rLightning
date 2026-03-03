use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
/// Integration test to reproduce the bug where values > 9004 bytes fail
/// Bug report: https://github.com/nxrvl/rlightning/issues/XXX
use rlightning::storage::engine::{StorageConfig, StorageEngine};
use std::sync::Arc;

/// Test the exact 9004 byte boundary reported in the bug
#[tokio::test]
async fn test_9004_byte_boundary() {
    let mut config = StorageConfig::default();
    config.max_value_size = 10 * 1024 * 1024; // 10MB to allow large values
    config.max_memory = 100 * 1024 * 1024; // 100MB

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 19001));
    let storage = Arc::new(StorageEngine::new(config));
    let server = Server::new(addr, Arc::clone(&storage)).with_buffer_size(1024 * 1024); // 1MB buffer

    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Connect client
    let mut client = Client::connect(addr)
        .await
        .expect("Failed to connect to server");

    // Test 9004 bytes (should work according to bug report)
    println!("\n=== Testing 9004 bytes (reported working) ===");
    let value_9004 = "x".repeat(9004);
    let result = client
        .send_command_str("SET", &["test:9004", &value_9004])
        .await;

    match &result {
        Ok(RespValue::SimpleString(s)) if s == "OK" => {
            println!("✓ 9004 bytes: SUCCESS");
        }
        Ok(other) => {
            println!("✗ 9004 bytes: Unexpected response: {:?}", other);
            panic!("9004 bytes should work but got: {:?}", other);
        }
        Err(e) => {
            println!("✗ 9004 bytes: ERROR - {}", e);
            panic!("9004 bytes should work but failed: {}", e);
        }
    }

    // Verify we can get it back
    let get_result = client.send_command_str("GET", &["test:9004"]).await;
    match get_result {
        Ok(RespValue::BulkString(Some(data))) => {
            assert_eq!(data.len(), 9004);
            println!("✓ Retrieved 9004 bytes successfully");
        }
        Ok(other) => panic!("Expected BulkString, got: {:?}", other),
        Err(e) => panic!("Failed to GET: {}", e),
    }

    // Test 9005 bytes (should fail according to bug report)
    println!("\n=== Testing 9005 bytes (reported failing) ===");
    let value_9005 = "x".repeat(9005);
    let result = client
        .send_command_str("SET", &["test:9005", &value_9005])
        .await;

    match &result {
        Ok(RespValue::SimpleString(s)) if s == "OK" => {
            println!("✓ 9005 bytes: SUCCESS (BUG IS FIXED!)");
        }
        Ok(other) => {
            println!("✗ 9005 bytes: Unexpected response: {:?}", other);
            panic!("9005 bytes failed with unexpected response: {:?}", other);
        }
        Err(e) => {
            println!("✗ 9005 bytes: ERROR - {}", e);
            println!("BUG REPRODUCED: 9005 bytes fails with: {}", e);
            panic!("BUG CONFIRMED: 9005 bytes fails with: {}", e);
        }
    }

    // Verify we can get it back
    let get_result = client.send_command_str("GET", &["test:9005"]).await;
    match get_result {
        Ok(RespValue::BulkString(Some(data))) => {
            assert_eq!(data.len(), 9005);
            println!("✓ Retrieved 9005 bytes successfully");
        }
        Ok(RespValue::BulkString(None)) => {
            println!("Key not found - SET probably failed");
        }
        Ok(other) => println!("GET returned: {:?}", other),
        Err(e) => println!("Failed to GET: {}", e),
    }
}

/// Test multiple large value sizes
#[tokio::test]
async fn test_large_values_comprehensive() {
    let mut config = StorageConfig::default();
    config.max_value_size = 10 * 1024 * 1024; // 10MB
    config.max_memory = 100 * 1024 * 1024;

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 19002));
    let storage = Arc::new(StorageEngine::new(config));
    let server = Server::new(addr, Arc::clone(&storage)).with_buffer_size(1024 * 1024); // 1MB buffer

    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = Client::connect(addr)
        .await
        .expect("Failed to connect to server");

    // Test various sizes around the boundary
    let test_sizes = vec![
        5000, 8000, 9000, 9004, // Max working
        9005, // First failing
        9010, 9050, 9100, 10000, 15000, 20000, 50000, 100000, 500000, 1_000_000,
    ];

    let mut failures = Vec::new();

    for size in test_sizes {
        println!("\nTesting {} bytes...", size);
        let value = "x".repeat(size);
        let key = format!("test:{}", size);

        let result = client.send_command_str("SET", &[&key, &value]).await;

        match result {
            Ok(RespValue::SimpleString(s)) if s == "OK" => {
                // Verify GET
                match client.send_command_str("GET", &[&key]).await {
                    Ok(RespValue::BulkString(Some(data))) if data.len() == size => {
                        println!("✓ {} bytes: SUCCESS", size);
                    }
                    Ok(other) => {
                        println!("✗ {} bytes: GET returned unexpected: {:?}", size, other);
                        failures.push((size, format!("GET failed: {:?}", other)));
                    }
                    Err(e) => {
                        println!("✗ {} bytes: GET failed: {}", size, e);
                        failures.push((size, format!("GET error: {}", e)));
                    }
                }
            }
            Ok(other) => {
                println!("✗ {} bytes: Unexpected SET response: {:?}", size, other);
                failures.push((size, format!("SET unexpected response: {:?}", other)));
            }
            Err(e) => {
                println!("✗ {} bytes: SET failed: {}", size, e);
                failures.push((size, format!("SET error: {}", e)));
            }
        }
    }

    if !failures.is_empty() {
        println!("\n=== FAILURES ===");
        for (size, error) in &failures {
            println!("{} bytes: {}", size, error);
        }
        panic!("Some tests failed");
    }
}

/// Test with base64-encoded data (similar to the bug report)
#[tokio::test]
async fn test_base64_encoded_large_values() {
    let mut config = StorageConfig::default();
    config.max_value_size = 10 * 1024 * 1024;
    config.max_memory = 100 * 1024 * 1024;

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 19003));
    let storage = Arc::new(StorageEngine::new(config));
    let server = Server::new(addr, Arc::clone(&storage)).with_buffer_size(1024 * 1024);

    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = Client::connect(addr)
        .await
        .expect("Failed to connect to server");

    // Create base64-like data that starts with characters like the error message
    let base64_chunk = "I1LTEyLTE0VDEwOjQyOj"; // From the error message
    let base64_data = base64_chunk.repeat(500); // ~10KB

    println!("Testing base64 data of {} bytes", base64_data.len());

    let result = client
        .send_command_str("SET", &["story_detail:test", &base64_data])
        .await;

    match result {
        Ok(RespValue::SimpleString(s)) if s == "OK" => {
            println!("✓ Base64 data: SUCCESS");

            // Verify GET
            let get_result = client.send_command_str("GET", &["story_detail:test"]).await;
            match get_result {
                Ok(RespValue::BulkString(Some(data))) => {
                    let retrieved = String::from_utf8_lossy(&data);
                    assert_eq!(retrieved, base64_data);
                    println!("✓ Retrieved base64 data successfully");
                }
                Ok(other) => panic!("GET returned unexpected: {:?}", other),
                Err(e) => panic!("GET failed: {}", e),
            }
        }
        Ok(other) => {
            panic!("SET returned unexpected: {:?}", other);
        }
        Err(e) => {
            println!("✗ Base64 data failed: {}", e);
            panic!("Base64 data test failed: {}", e);
        }
    }
}
