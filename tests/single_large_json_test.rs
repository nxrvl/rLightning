use rlightning::storage::engine::{StorageConfig, StorageEngine};
use rlightning::networking::resp::RespValue;
use rlightning::networking::client::Client;
use rlightning::networking::server::Server;
use std::sync::Arc;

/// Test setting and getting a large JSON value in a clean environment
#[tokio::test]
async fn test_large_json_set_and_get() {
    // Use a unique port
    let port = 19876;
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    
    // Configure storage with sufficient buffer
    let mut config = StorageConfig::default();
    config.max_value_size = 1 * 1024 * 1024; // 1MB is enough for our test
    
    // Create storage engine and server
    let storage = Arc::new(StorageEngine::new(config));
    let server = Server::new(addr, Arc::clone(&storage));
    
    // Start server in a background task
    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });
    
    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Connect to the server
    let mut client = Client::connect(addr)
        .await
        .expect("Failed to connect to server");
    
    // Flush all data to ensure we have a clean environment
    let flush_resp = client.send_command_str("FLUSHALL", &[]).await.expect("Failed to flush");
    assert_eq!(flush_resp, RespValue::SimpleString("OK".to_string()));
    
    // Create a reasonably sized JSON test string
    let mut test_json = String::from("[");
    for i in 0..2 {
        test_json.push_str(&format!(
            "{{\"id\":{},\"name\":\"Test {}\",\"data\":\"", i, i
        ));
        
        // Add enough content to make it substantial but not too large
        for j in 0..100 {
            test_json.push_str(&format!("item_{}_", j));
        }
        
        test_json.push_str("\"}}");
        if i < 1 {
            test_json.push_str(",");
        }
    }
    test_json.push_str("]");
    
    println!("Test JSON size: {} bytes", test_json.len());
    assert!(test_json.len() > 1000, "JSON should be at least 1KB in size");
    
    // Use a unique test key
    let key = "single_test_large_json_key";
    
    // Execute the SET command
    let result = client.send_command_str("SET", &[key, &test_json])
        .await
        .expect("SET command should succeed");
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    
    // Now try to get it back
    let result = client.send_command_str("GET", &[key])
        .await
        .expect("GET command should succeed");
    
    // Verify we can get the value back
    match result {
        RespValue::BulkString(Some(value)) => {
            let retrieved_json = String::from_utf8_lossy(&value);
            assert_eq!(retrieved_json, test_json);
            println!("Successfully retrieved large JSON value");
        },
        other => panic!("Expected BulkString, got: {:?}", other)
    }
}