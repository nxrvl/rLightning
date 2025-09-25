use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Simple Redis protocol compliance test using raw TCP
#[tokio::test]
async fn test_redis_protocol_compliance() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server
    let addr: SocketAddr = "127.0.0.1:16399".parse()?;
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
    sleep(Duration::from_millis(300)).await;
    
    // Connect using raw TCP stream
    let mut stream = TcpStream::connect(addr).await?;
    
    println!("=== Testing Basic Redis Protocol Compliance ===");
    
    // Test 1: PING command
    println!("Test 1: PING command");
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("PING response: {:?}", response);
    assert!(response.starts_with("+PONG"), "Expected +PONG response");
    
    // Test 2: SET command
    println!("Test 2: SET command");
    stream.write_all(b"*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$5\r\nvalue\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("SET response: {:?}", response);
    assert!(response.starts_with("+OK"), "Expected +OK response");
    
    // Test 3: GET command
    println!("Test 3: GET command");
    stream.write_all(b"*2\r\n$3\r\nGET\r\n$4\r\ntest\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("GET response: {:?}", response);
    assert!(response.contains("value"), "Expected response containing 'value'");
    
    // Test 4: GET non-existent key (null response)
    println!("Test 4: GET non-existent key");
    stream.write_all(b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("GET non-existent response: {:?}", response);
    assert!(response.starts_with("$-1"), "Expected null bulk string response");
    
    // Test 5: DEL command
    println!("Test 5: DEL command");
    stream.write_all(b"*2\r\n$3\r\nDEL\r\n$4\r\ntest\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("DEL response: {:?}", response);
    assert!(response.starts_with(":"), "Expected integer response");
    
    // Test 6: JSON.SET command
    println!("Test 6: JSON.SET command");
    let json_data = r#"{"name":"test","value":123}"#;
    let set_cmd = format!("*4\r\n$8\r\nJSON.SET\r\n$8\r\njsontest\r\n$1\r\n.\r\n${}\r\n{}\r\n", json_data.len(), json_data);
    stream.write_all(set_cmd.as_bytes()).await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("JSON.SET response: {:?}", response);
    assert!(response.starts_with("+OK"), "Expected +OK response from JSON.SET");
    
    // Test 7: JSON.GET command
    println!("Test 7: JSON.GET command");
    stream.write_all(b"*2\r\n$8\r\nJSON.GET\r\n$8\r\njsontest\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("JSON.GET response: {:?}", response);
    assert!(response.contains("test"), "Expected JSON data in response");
    
    // Test 8: JSON.TYPE command
    println!("Test 8: JSON.TYPE command");
    stream.write_all(b"*2\r\n$9\r\nJSON.TYPE\r\n$8\r\njsontest\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("JSON.TYPE response: {:?}", response);
    assert!(response.contains("object"), "Expected 'object' type response");
    
    // Test 9: JSON.RESP command (Redis JSON specs compliance)
    println!("Test 9: JSON.RESP command");
    stream.write_all(b"*2\r\n$9\r\nJSON.RESP\r\n$8\r\njsontest\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("JSON.RESP response: {:?}", response);
    // JSON.RESP should return RESP-encoded format, not JSON string
    assert!(response.starts_with("*"), "Expected array response from JSON.RESP");
    
    // Test 10: Error handling (wrong number of arguments)
    println!("Test 10: Error handling");
    stream.write_all(b"*1\r\n$3\r\nSET\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("Error response: {:?}", response);
    assert!(response.starts_with("-ERR"), "Expected error response");
    
    println!("=== All Redis Protocol Compliance Tests Passed! ===");
    Ok(())
}

/// Test Redis command aliases and case sensitivity
#[tokio::test]
async fn test_redis_command_aliases() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = "127.0.0.1:16398".parse()?;
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    
    let server = Server::new_with_storage(addr, storage);
    
    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });
    
    sleep(Duration::from_millis(300)).await;
    let mut stream = TcpStream::connect(addr).await?;
    
    println!("=== Testing Command Aliases and Case Sensitivity ===");
    
    // Test case insensitive commands
    println!("Test: Case insensitive commands");
    stream.write_all(b"*3\r\n$3\r\nset\r\n$4\r\ntest\r\n$5\r\nvalue\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    assert!(response.starts_with("+OK"), "Lowercase commands should work");
    
    // Test JSON command aliases
    println!("Test: JSON command aliases");
    
    // Test jsonget alias
    stream.write_all(b"*2\r\n$7\r\njsonget\r\n$4\r\ntest\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("jsonget alias response: {:?}", response);
    // Should work (might return null if not JSON data)
    
    // Test get_json alias
    stream.write_all(b"*2\r\n$8\r\nget_json\r\n$4\r\ntest\r\n").await?;
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("get_json alias response: {:?}", response);
    
    println!("=== Command Alias Tests Completed ===");
    Ok(())
}