use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

#[tokio::test]
async fn test_list_operations_compatibility() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server
    let addr: SocketAddr = "127.0.0.1:16380".parse()?;
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    
    let server = Server::new(addr, storage);
    
    // Start server in background
    tokio::spawn(async move {
        server.start().await.unwrap();
    });
    
    // Wait for server to start
    sleep(Duration::from_millis(100)).await;
    
    // Connect client
    let mut client = Client::connect(addr).await?;
    
    // Test LPUSH behavior
    // LPUSH mylist a b c should result in the list [c, b, a]
    let lpush_args: Vec<&[u8]> = vec![b"LPUSH", b"mylist", b"a", b"b", b"c"];
    client.send_command(lpush_args).await?;
    
    // Verify with LRANGE
    let lrange_args: Vec<&[u8]> = vec![b"LRANGE", b"mylist", b"0", b"-1"];
    let response = client.send_command(lrange_args).await?;
    if let RespValue::Array(Some(values)) = response {
        assert_eq!(values.len(), 3);
        // First element should be "c"
        assert_eq!(values[0], RespValue::BulkString(Some(b"c".to_vec())));
        // Second element should be "b"
        assert_eq!(values[1], RespValue::BulkString(Some(b"b".to_vec())));
        // Third element should be "a"
        assert_eq!(values[2], RespValue::BulkString(Some(b"a".to_vec())));
    } else {
        panic!("Expected array response from LRANGE");
    }
    
    // Test RPUSH behavior
    // RPUSH otherlist a b c should result in the list [a, b, c]
    let rpush_args: Vec<&[u8]> = vec![b"RPUSH", b"otherlist", b"a", b"b", b"c"];
    client.send_command(rpush_args).await?;
    
    // Verify with LRANGE
    let lrange_other_args: Vec<&[u8]> = vec![b"LRANGE", b"otherlist", b"0", b"-1"];
    let response = client.send_command(lrange_other_args).await?;
    if let RespValue::Array(Some(values)) = response {
        assert_eq!(values.len(), 3);
        // First element should be "a"
        assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
        // Second element should be "b"
        assert_eq!(values[1], RespValue::BulkString(Some(b"b".to_vec())));
        // Third element should be "c"
        assert_eq!(values[2], RespValue::BulkString(Some(b"c".to_vec())));
    } else {
        panic!("Expected array response from LRANGE");
    }
    
    // Test LPOP behavior
    let lpop_args: Vec<&[u8]> = vec![b"LPOP", b"mylist"];
    let response = client.send_command(lpop_args).await?;
    assert_eq!(response, RespValue::BulkString(Some(b"c".to_vec())));
    
    // Test RPOP behavior
    let rpop_args: Vec<&[u8]> = vec![b"RPOP", b"mylist"];
    let response = client.send_command(rpop_args).await?;
    assert_eq!(response, RespValue::BulkString(Some(b"a".to_vec())));
    
    // Clean up
    let del_args: Vec<&[u8]> = vec![b"DEL", b"mylist", b"otherlist"];
    client.send_command(del_args).await?;
    
    Ok(())
} 