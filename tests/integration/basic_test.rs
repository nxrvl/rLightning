use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::resp::RespValue;

use crate::test_utils::{setup_test_server, create_client};

/// Basic integration test for key-value operations
#[tokio::test]
async fn test_basic_operations() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server with a unique port offset (use high numbers to avoid conflicts)
    let addr = setup_test_server(901).await?;
    
    // Connect client
    let mut client = create_client(addr).await?;
    
    // Test PING
    let ping_args: Vec<&[u8]> = vec![b"PING"];
    let response = client.send_command(ping_args).await?;
    assert_eq!(response, RespValue::SimpleString("PONG".to_string()));
    
    // Test SET
    let set_args: Vec<&[u8]> = vec![b"SET", b"mykey", b"myvalue"];
    let response = client.send_command(set_args).await?;
    assert_eq!(response, RespValue::SimpleString("OK".to_string()));
    
    // Test GET
    let get_args: Vec<&[u8]> = vec![b"GET", b"mykey"];
    let response = client.send_command(get_args).await?;
    assert_eq!(response, RespValue::BulkString(Some(b"myvalue".to_vec())));
    
    // Test SET with expiry
    let set_ex_args: Vec<&[u8]> = vec![b"SET", b"expiry_key", b"expiry_value", b"EX", b"1"];
    let response = client.send_command(set_ex_args).await?;
    assert_eq!(response, RespValue::SimpleString("OK".to_string()));
    
    // Verify expiry key exists
    let get_expiry_args: Vec<&[u8]> = vec![b"GET", b"expiry_key"];
    let response = client.send_command(get_expiry_args).await?;
    assert_eq!(response, RespValue::BulkString(Some(b"expiry_value".to_vec())));
    
    // Wait for key to expire
    sleep(Duration::from_secs(2)).await;
    
    // Verify key has expired
    let get_expired_args: Vec<&[u8]> = vec![b"GET", b"expiry_key"];
    let response = client.send_command(get_expired_args).await?;
    assert_eq!(response, RespValue::BulkString(None));
    
    Ok(())
}

/// Test for TTL-related commands
#[tokio::test]
async fn test_ttl_commands() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server with a unique port offset (use high numbers to avoid conflicts)
    let addr = setup_test_server(902).await?;
    
    // Connect client
    let mut client = create_client(addr).await?;
    
    // Set a key with expiration
    let set_ex_args: Vec<&[u8]> = vec![b"SET", b"ttl_key", b"ttl_value", b"EX", b"60"];
    let response = client.send_command(set_ex_args).await?;
    assert_eq!(response, RespValue::SimpleString("OK".to_string()));
    
    // Check TTL - should be close to 60
    let ttl_args: Vec<&[u8]> = vec![b"TTL", b"ttl_key"];
    let response = client.send_command(ttl_args).await?;
    
    if let RespValue::Integer(ttl) = response {
        assert!(ttl > 50 && ttl <= 60, "TTL should be close to 60, got: {}", ttl);
    } else {
        panic!("Expected Integer response from TTL, got: {:?}", response);
    }
    
    // Use EXPIRE to change TTL
    let expire_args: Vec<&[u8]> = vec![b"EXPIRE", b"ttl_key", b"30"];
    let response = client.send_command(expire_args).await?;
    assert_eq!(response, RespValue::Integer(1)); // 1 indicates success
    
    // Check new TTL - should be close to 30
    let ttl_args: Vec<&[u8]> = vec![b"TTL", b"ttl_key"];
    let response = client.send_command(ttl_args).await?;
    
    if let RespValue::Integer(ttl) = response {
        assert!(ttl > 20 && ttl <= 30, "TTL should be close to 30, got: {}", ttl);
    } else {
        panic!("Expected Integer response from TTL, got: {:?}", response);
    }
    
    // Use PERSIST to remove TTL
    let persist_args: Vec<&[u8]> = vec![b"PERSIST", b"ttl_key"];
    let response = client.send_command(persist_args).await?;
    assert_eq!(response, RespValue::Integer(1)); // 1 indicates success
    
    // Check TTL after PERSIST - should be -1 (no expiry)
    let ttl_args: Vec<&[u8]> = vec![b"TTL", b"ttl_key"];
    let response = client.send_command(ttl_args).await?;
    assert_eq!(response, RespValue::Integer(-1)); // -1 indicates no expiry
    
    Ok(())
}

/// Test deletion and existence checking
#[tokio::test]
async fn test_del_exists() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server with a unique port offset (use high numbers to avoid conflicts)
    let addr = setup_test_server(920).await?;
    
    // Connect client
    let mut client = create_client(addr).await?;
    
    // Set multiple keys
    for i in 1..=5 {
        let key = format!("del_key_{}", i);
        let value = format!("value_{}", i);
        let set_args: Vec<&[u8]> = vec![b"SET", key.as_bytes(), value.as_bytes()];
        client.send_command(set_args).await?;
    }
    
    // Check EXISTS for one key
    let exists_args: Vec<&[u8]> = vec![b"EXISTS", b"del_key_1"];
    let response = client.send_command(exists_args).await?;
    assert_eq!(response, RespValue::Integer(1)); // 1 indicates key exists
    
    // Check EXISTS for multiple keys
    let exists_multi_args: Vec<&[u8]> = vec![
        b"EXISTS", b"del_key_1", b"del_key_2", b"nonexistent"
    ];
    let response = client.send_command(exists_multi_args).await?;
    assert_eq!(response, RespValue::Integer(2)); // 2 keys exist
    
    // Delete one key
    let del_args: Vec<&[u8]> = vec![b"DEL", b"del_key_1"];
    let response = client.send_command(del_args).await?;
    assert_eq!(response, RespValue::Integer(1)); // 1 key deleted
    
    // Delete multiple keys
    let del_multi_args: Vec<&[u8]> = vec![
        b"DEL", b"del_key_2", b"del_key_3", b"nonexistent"
    ];
    let response = client.send_command(del_multi_args).await?;
    assert_eq!(response, RespValue::Integer(2)); // 2 keys deleted
    
    // Verify deletions
    let exists_after_args: Vec<&[u8]> = vec![
        b"EXISTS", b"del_key_1", b"del_key_2", b"del_key_3", b"del_key_4", b"del_key_5"
    ];
    let response = client.send_command(exists_after_args).await?;
    assert_eq!(response, RespValue::Integer(2)); // Only 2 keys should remain
    
    Ok(())
} 