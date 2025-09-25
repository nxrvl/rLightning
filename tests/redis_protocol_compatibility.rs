use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Tests Redis protocol compatibility by sending raw RESP messages
/// Focused on basic commands to ensure compatibility
#[tokio::test]
async fn test_resp_protocol_compatibility() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server
    let addr: SocketAddr = "127.0.0.1:16395".parse()?;
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    
    let server = Server::new_with_storage(addr, storage);
    
    // Start server in background
    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });
    
    // Wait for server to start
    sleep(Duration::from_millis(200)).await;
    
    // Connect using raw TCP stream to test protocol-level compatibility
    let mut stream = match TcpStream::connect(addr).await {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("Failed to connect to server: {}", e);
            sleep(Duration::from_millis(500)).await;
            TcpStream::connect(addr).await?
        }
    };
    
    // ======== SIMPLE STRINGS (PING) ========
    
    println!("Testing PING command...");
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("PING response: {}", response);
    assert!(response.starts_with("+PONG"), "Expected PONG response, got: {}", response);
    
    // ======== BULK STRINGS (SET/GET) ========
    
    println!("Testing SET command...");
    stream.write_all(b"*3\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$9\r\ntestvalue\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("SET response: {}", response);
    assert!(response.starts_with("+OK"), "Expected OK response, got: {}", response);
    
    println!("Testing GET command...");
    stream.write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("GET response: {}", response);
    assert!(response.contains("testvalue"), "Expected response containing 'testvalue', got: {}", response);
    
    // ======== NULL BULK STRING (GET nonexistent) ========
    
    println!("Testing GET nonexistent key...");
    stream.write_all(b"*2\r\n$3\r\nGET\r\n$9\r\nnonexists\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("GET nonexistent response: {}", response);
    assert!(response.starts_with("$-1"), "Expected null bulk string response, got: {}", response);
    
    // ======== INTEGER (DEL) ========
    
    println!("Testing DEL command...");
    stream.write_all(b"*2\r\n$3\r\nDEL\r\n$7\r\ntestkey\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("DEL response: {}", response);
    assert!(response.starts_with(":"), "Expected integer response, got: {}", response);
    
    println!("All tests completed successfully!");
    Ok(())
} 