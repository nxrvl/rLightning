use std::net::SocketAddr;
use std::time::Duration;

use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

#[tokio::test]
async fn test_basic_commands() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server
    let addr: SocketAddr = "127.0.0.1:16379".parse()?;
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    
    let server = Server::new_with_storage(addr, storage);
    
    // Start server in background
    tokio::spawn(async move {
        server.start().await.unwrap();
    });
    
    // Wait for server to start
    sleep(Duration::from_millis(100)).await;
    
    // Connect client
    let mut client = Client::connect(addr).await?;
    
    // Test PING
    let response = client.send_command_str("PING", &[]).await?;
    assert_eq!(response, RespValue::SimpleString("PONG".to_string()));
    
    // Test SET/GET
    client.send_command_str("SET", &["mykey", "myvalue"]).await?;
    let response = client.send_command_str("GET", &["mykey"]).await?;
    assert_eq!(
        response,
        RespValue::BulkString(Some(b"myvalue".to_vec()))
    );
    
    // Test TTL
    client.send_command_str("EXPIRE", &["mykey", "10"]).await?;
    let response = client.send_command_str("TTL", &["mykey"]).await?;
    if let RespValue::Integer(ttl) = response {
        assert!(ttl > 0 && ttl <= 10);
    } else {
        panic!("Expected integer response from TTL");
    }
    
    // Test DEL
    let response = client.send_command_str("DEL", &["mykey"]).await?;
    assert_eq!(response, RespValue::Integer(1));
    
    // Verify key is gone
    let response = client.send_command_str("GET", &["mykey"]).await?;
    assert_eq!(response, RespValue::BulkString(None));
    
    Ok(())
} 