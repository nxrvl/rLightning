use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Tests Redis JSON commands compatibility
#[tokio::test]
async fn test_redis_json_commands() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server with a unique port
    let addr: SocketAddr = "127.0.0.1:16390".parse()?;
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    
    let server = Server::new(addr, storage);
    
    // Start server in background
    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });
    
    // Wait for server to start
    sleep(Duration::from_millis(200)).await;
    
    // Connect client
    let mut client = Client::connect(addr).await?;
    
    // Test JSON.SET command at root path
    println!("Testing JSON.SET at root path");
    let json_obj = r#"{"name":"John","age":30,"address":{"city":"New York"},"skills":["Rust","Redis"]}"#;
    let result = client.send_command_str("JSON.SET", &["user:1", ".", json_obj]).await?;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    
    // Test JSON.GET command for the entire document
    println!("Testing JSON.GET for entire document");
    let result = client.send_command_str("JSON.GET", &["user:1"]).await?;
    if let RespValue::BulkString(Some(bytes)) = result {
        let json_str = String::from_utf8(bytes)?;
        assert!(json_str.contains("John"));
        assert!(json_str.contains("New York"));
    } else {
        panic!("Expected BulkString response from JSON.GET, got: {:?}", result);
    }
    
    // Test JSON.GET command with path
    println!("Testing JSON.GET with path");
    let result = client.send_command_str("JSON.GET", &["user:1", "name"]).await?;
    if let RespValue::BulkString(Some(bytes)) = result {
        let json_str = String::from_utf8(bytes)?;
        assert_eq!(json_str, r#""John""#);
    } else {
        panic!("Expected BulkString response from JSON.GET with path, got: {:?}", result);
    }
    
    // Test JSON.TYPE command
    println!("Testing JSON.TYPE");
    let result = client.send_command_str("JSON.TYPE", &["user:1"]).await?;
    assert_eq!(result, RespValue::SimpleString("object".to_string()));
    
    let result = client.send_command_str("JSON.TYPE", &["user:1", "name"]).await?;
    assert_eq!(result, RespValue::SimpleString("string".to_string()));
    
    let result = client.send_command_str("JSON.TYPE", &["user:1", "skills"]).await?;
    assert_eq!(result, RespValue::SimpleString("array".to_string()));
    
    // Test JSON.SET for updating a specific path
    println!("Testing JSON.SET for updating a specific path");
    let result = client.send_command_str("JSON.SET", &["user:1", "age", "31"]).await?;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    
    // Verify the update worked
    let result = client.send_command_str("JSON.GET", &["user:1", "age"]).await?;
    if let RespValue::BulkString(Some(bytes)) = result {
        let json_str = String::from_utf8(bytes)?;
        assert_eq!(json_str, "31");
    } else {
        panic!("Expected BulkString response from JSON.GET after update, got: {:?}", result);
    }
    
    // Test JSON.ARRAPPEND
    println!("Testing JSON.ARRAPPEND");
    let result = client.send_command_str("JSON.ARRAPPEND", &["user:1", "skills", "\"Python\"", "\"JavaScript\""]).await?;
    if let RespValue::Integer(len) = result {
        assert_eq!(len, 4); // ["Rust", "Redis", "Python", "JavaScript"] - length 4
    } else {
        panic!("Expected Integer response from JSON.ARRAPPEND, got: {:?}", result);
    }
    
    // Verify ARRAPPEND worked
    let result = client.send_command_str("JSON.GET", &["user:1", "skills"]).await?;
    if let RespValue::BulkString(Some(bytes)) = result {
        let json_str = String::from_utf8(bytes)?;
        assert!(json_str.contains("Python"));
        assert!(json_str.contains("JavaScript"));
    } else {
        panic!("Expected BulkString response from JSON.GET for skills, got: {:?}", result);
    }
    
    // Test JSON.ARRTRIM
    println!("Testing JSON.ARRTRIM");
    let result = client.send_command_str("JSON.ARRTRIM", &["user:1", "skills", "1", "2"]).await?;
    if let RespValue::Integer(len) = result {
        assert_eq!(len, 2); // ["Redis", "Python"] - length 2
    } else {
        panic!("Expected Integer response from JSON.ARRTRIM, got: {:?}", result);
    }
    
    // Test compatibility with standard Redis commands
    println!("Testing compatibility with standard Redis commands");
    
    // DEL should work on JSON keys
    let result = client.send_command_str("DEL", &["user:1"]).await?;
    assert_eq!(result, RespValue::Integer(1));
    
    // Key should be gone
    let result = client.send_command_str("EXISTS", &["user:1"]).await?;
    assert_eq!(result, RespValue::Integer(0));
    
    println!("All tests passed!");
    Ok(())
} 