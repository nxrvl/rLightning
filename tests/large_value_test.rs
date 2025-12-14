/// Test for large value support (bug report: values > 9004 bytes fail)
/// This test reproduces the issue where SET operations fail with protocol errors
/// when the value size exceeds 9004 bytes.

use bytes::BytesMut;
use rlightning::networking::resp::RespValue;
use rlightning::storage::engine::StorageEngine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use rlightning::networking::server::Server;
use std::net::SocketAddr;

#[tokio::test]
async fn test_large_values_various_sizes() {
    // Test various sizes around the reported 9004 byte limit
    let test_sizes = vec![
        5000,
        8000,
        9000,
        9004,  // Reported max working size
        9005,  // First failing size
        9010,
        9050,
        10000,
        50000,
        100000,
        500000,
        1_000_000,  // 1MB
    ];

    for size in test_sizes {
        println!("\n=== Testing value size: {} bytes ===", size);

        let result = test_set_get_large_value(size).await;

        match result {
            Ok(_) => println!("✓ SUCCESS: {} bytes", size),
            Err(e) => println!("✗ FAILED: {} bytes - Error: {}", size, e),
        }
    }
}

async fn test_set_get_large_value(value_size: usize) -> Result<(), Box<dyn std::error::Error>> {
    // Start a test server
    use rlightning::storage::engine::StorageConfig;

    let mut config = StorageConfig::default();
    config.max_value_size = 10 * 1024 * 1024; // 10MB max value size
    config.max_memory = 100 * 1024 * 1024; // 100MB storage

    let storage = StorageEngine::new(config);
    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = TcpListener::bind(addr).await?;
    let server_addr = listener.local_addr()?;

    // Spawn server in background
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            let server = Server::new(server_addr, storage);
            // Handle single client for this test
            let _ = socket;
        }
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect as client
    let mut stream = tokio::net::TcpStream::connect(server_addr).await?;

    // Create test data
    let key = b"test_key";
    let value = vec![b'x'; value_size];

    // Build RESP command: *3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$<size>\r\n<value>\r\n
    let mut command = BytesMut::new();
    command.extend_from_slice(b"*3\r\n");
    command.extend_from_slice(b"$3\r\nSET\r\n");
    command.extend_from_slice(b"$8\r\ntest_key\r\n");
    command.extend_from_slice(b"$");
    command.extend_from_slice(value_size.to_string().as_bytes());
    command.extend_from_slice(b"\r\n");
    command.extend_from_slice(&value);
    command.extend_from_slice(b"\r\n");

    println!("Sending command of total size: {} bytes", command.len());
    println!("Command header: {:?}", String::from_utf8_lossy(&command[0..50.min(command.len())]));

    // Send the SET command
    stream.write_all(&command).await?;
    stream.flush().await?;

    println!("Command sent, waiting for response...");

    // Read response
    let mut response = vec![0u8; 1024];
    let n = stream.read(&mut response).await?;

    if n == 0 {
        return Err("Connection closed by server".into());
    }

    let response_str = String::from_utf8_lossy(&response[0..n]);
    println!("Response: {}", response_str);

    // Check for error response
    if response_str.starts_with('-') {
        return Err(format!("Server returned error: {}", response_str).into());
    }

    // Should get +OK\r\n
    if response_str.trim() != "+OK" {
        return Err(format!("Unexpected response: {}", response_str).into());
    }

    Ok(())
}

#[tokio::test]
async fn test_exact_9004_limit() {
    println!("\n=== Testing exact 9004 byte boundary ===");

    // Test 9004 bytes (should work)
    println!("\nTesting 9004 bytes (reported working):");
    match test_set_get_large_value(9004).await {
        Ok(_) => println!("✓ 9004 bytes: SUCCESS"),
        Err(e) => println!("✗ 9004 bytes: FAILED - {}", e),
    }

    // Test 9005 bytes (should fail with current bug)
    println!("\nTesting 9005 bytes (reported failing):");
    match test_set_get_large_value(9005).await {
        Ok(_) => println!("✓ 9005 bytes: SUCCESS"),
        Err(e) => println!("✗ 9005 bytes: FAILED - {}", e),
    }
}

#[test]
fn test_resp_parsing_large_bulk_string() {
    // Test RESP parsing directly with large bulk strings
    let sizes = vec![5000, 9004, 9005, 10000, 50000, 100000];

    for size in sizes {
        let value = vec![b'x'; size];

        // Create RESP bulk string
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"$");
        buffer.extend_from_slice(size.to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        buffer.extend_from_slice(&value);
        buffer.extend_from_slice(b"\r\n");

        // Try to parse
        let result = RespValue::parse(&mut buffer);

        match result {
            Ok(Some(RespValue::BulkString(Some(data)))) => {
                assert_eq!(data.len(), size, "Parsed data size mismatch for {} bytes", size);
                println!("✓ RESP parsing successful for {} bytes", size);
            }
            Ok(None) => {
                println!("✗ RESP parsing returned None (incomplete) for {} bytes", size);
            }
            Ok(other) => {
                println!("✗ RESP parsing returned unexpected type for {} bytes: {:?}", size, other);
            }
            Err(e) => {
                println!("✗ RESP parsing failed for {} bytes: {}", size, e);
                panic!("RESP parsing should not fail for {} bytes: {}", size, e);
            }
        }
    }
}

#[test]
fn test_resp_set_command_large_value() {
    // Test parsing a complete SET command with large value
    let sizes = vec![5000, 9004, 9005, 10000];

    for size in sizes {
        println!("\nTesting SET command with {} byte value", size);

        let key = b"testkey";
        let value = vec![b'x'; size];

        // Build SET command
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"*3\r\n");
        buffer.extend_from_slice(b"$3\r\nSET\r\n");
        buffer.extend_from_slice(b"$");
        buffer.extend_from_slice(key.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        buffer.extend_from_slice(key);
        buffer.extend_from_slice(b"\r\n");
        buffer.extend_from_slice(b"$");
        buffer.extend_from_slice(size.to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        buffer.extend_from_slice(&value);
        buffer.extend_from_slice(b"\r\n");

        println!("Command size: {} bytes", buffer.len());

        // Try to parse
        let result = RespValue::parse(&mut buffer);

        match result {
            Ok(Some(RespValue::Array(Some(elements)))) => {
                assert_eq!(elements.len(), 3, "Should have 3 elements");
                println!("✓ SET command parsing successful for {} byte value", size);
            }
            Ok(None) => {
                println!("✗ Parsing returned None for {} byte value", size);
                panic!("Should not return None for complete command");
            }
            Ok(other) => {
                println!("✗ Unexpected parse result: {:?}", other);
                panic!("Expected Array, got different type");
            }
            Err(e) => {
                println!("✗ Parsing failed for {} byte value: {}", size, e);
                panic!("Parsing should not fail: {}", e);
            }
        }
    }
}
