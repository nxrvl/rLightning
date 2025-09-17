use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use rlightning::storage::engine::{StorageEngine, StorageConfig};
use rlightning::networking::server::Server;
use rlightning::networking::resp::RespValue;

/// Test the critical buffer separation fixes
#[tokio::test]
async fn test_set_get_problematic_data() {
    // Start the server
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    let server = Server::new("127.0.0.1:0".parse().unwrap(), storage);
    
    // Start server in background task
    let server_task = tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {}", e);
        }
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Connect to server
    let mut stream = TcpStream::connect("127.0.0.1:6379").await
        .expect("Failed to connect to server");
    
    // Test 1: SET with B64JSON prefix data
    let problematic_data = "B64JSON:W3siaWQiOiAxLCJuYW1lIjoiRXhhbXBsZSIsImFjdGl2ZSI6dHJ1ZX0=";
    let set_command = format!("*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n${}\r\n{}\r\n", 
                             problematic_data.len(), problematic_data);
    
    // Send SET command
    stream.write_all(set_command.as_bytes()).await.unwrap();
    
    // Read SET response
    let mut response = vec![0u8; 1024];
    let n = stream.read(&mut response).await.unwrap();
    let set_response = String::from_utf8_lossy(&response[0..n]);
    println!("SET response: {:?}", set_response);
    
    // Should get +OK response
    assert!(set_response.contains("+OK"), "SET should succeed with OK response");
    
    // Test 2: GET the same key
    let get_command = "*2\r\n$3\r\nGET\r\n$8\r\ntest_key\r\n";
    
    // Send GET command
    stream.write_all(get_command.as_bytes()).await.unwrap();
    
    // Read GET response
    let mut response = vec![0u8; 1024];
    let n = stream.read(&mut response).await.unwrap();
    let get_response = String::from_utf8_lossy(&response[0..n]);
    println!("GET response: {:?}", get_response);
    
    // Should get the data back as bulk string
    let expected_response = format!("${}\r\n{}\r\n", problematic_data.len(), problematic_data);
    assert!(get_response.contains(&expected_response), 
            "GET should return the exact data that was stored");
    
    // Cleanup
    server_task.abort();
}

#[tokio::test]
async fn test_multiple_problematic_patterns() {
    // Test data patterns that previously caused issues
    let test_patterns = vec![
        "B64JSON:W3data",
        "+FAKE_SIMPLE_STRING", 
        "-FAKE_ERROR",
        ":12345_FAKE_INTEGER",
        "$999\r\nFAKE_BULK",
        "*2\r\nFAKE_ARRAY",
        "{\"json\": \"data with control chars\"}",
    ];
    
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    
    for (i, pattern) in test_patterns.iter().enumerate() {
        let key = format!("test_key_{}", i);
        
        // Store the problematic pattern
        let result = storage.set(key.as_bytes().to_vec(), pattern.as_bytes().to_vec(), None).await;
        assert!(result.is_ok(), "Should be able to store pattern: {}", pattern);
        
        // Retrieve the pattern
        let retrieved = storage.get(key.as_bytes()).await;
        assert!(retrieved.is_ok(), "Should be able to retrieve pattern: {}", pattern);
        
        let data = retrieved.unwrap();
        assert!(data.is_some(), "Retrieved data should exist for pattern: {}", pattern);
        assert_eq!(data.unwrap(), pattern.as_bytes(), "Data should match exactly for pattern: {}", pattern);
    }
}

#[tokio::test]
async fn test_concurrent_set_get_operations() {
    let config = StorageConfig::default();
    let storage = Arc::new(StorageEngine::new(config));
    
    // Test concurrent operations with problematic data
    let mut handles = vec![];
    
    for i in 0..20 {
        let storage_clone = Arc::clone(&storage);
        let handle = tokio::spawn(async move {
            let key = format!("concurrent_key_{}", i);
            let problematic_data = format!("B64JSON:W3{}{}", "x".repeat(i * 100), i);
            
            // SET
            let set_result = storage_clone.set(
                key.as_bytes().to_vec(), 
                problematic_data.as_bytes().to_vec(), 
                None
            ).await;
            assert!(set_result.is_ok(), "Concurrent SET should succeed for key: {}", key);
            
            // GET immediately after
            let get_result = storage_clone.get(key.as_bytes()).await;
            assert!(get_result.is_ok(), "Concurrent GET should succeed for key: {}", key);
            
            let retrieved = get_result.unwrap();
            assert!(retrieved.is_some(), "Retrieved data should exist for key: {}", key);
            assert_eq!(retrieved.unwrap(), problematic_data.as_bytes(), 
                      "Data should match exactly for key: {}", key);
            
            key
        });
        handles.push(handle);
    }
    
    // Wait for all operations
    for handle in handles {
        let key = handle.await.unwrap();
        println!("Completed concurrent test for: {}", key);
    }
}

#[test]
fn test_resp_value_serialization_with_problematic_data() {
    let test_cases = vec![
        "B64JSON:W3data",
        "+not_a_simple_string",
        "-not_an_error",  
        ":123not_an_integer",
        "$10\r\nnot_bulk",
        "*2\r\nnot_array",
    ];
    
    for data in test_cases {
        // Create bulk string response (what GET should return)
        let bulk_string = RespValue::BulkString(Some(data.as_bytes().to_vec()));
        
        // Serialize it
        let serialized = bulk_string.serialize();
        assert!(serialized.is_ok(), "Should be able to serialize: {}", data);
        
        let bytes = serialized.unwrap();
        let serialized_str = String::from_utf8_lossy(&bytes);
        
        // Should be properly formatted as: $<len>\r\n<data>\r\n  
        let expected = format!("${}\r\n{}\r\n", data.len(), data);
        assert_eq!(serialized_str, expected, "Serialization should be correct for: {}", data);
        
        println!("✓ Correctly serialized '{}' as: {:?}", data, serialized_str);
    }
}

#[tokio::test]
async fn test_large_json_with_resp_patterns() {
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    
    // Create large JSON that contains RESP-like patterns
    let large_json = format!(r#"{{
        "command_like_data": "+OK from JSON",
        "error_like_data": "-ERROR in JSON", 
        "integer_like_data": ":12345 in JSON",
        "bulk_like_data": "$100\r\ndata in JSON",
        "array_like_data": "*2\r\narray in JSON",
        "base64_data": "{}",
        "large_field": "{}"
    }}"#, "B64JSON:W3".repeat(100), "x".repeat(5000));
    
    // Store the large JSON
    let key = b"large_json_key".to_vec();
    let result = storage.set(key.clone(), large_json.as_bytes().to_vec(), None).await;
    assert!(result.is_ok(), "Should store large JSON with RESP patterns");
    
    // Retrieve it
    let retrieved = storage.get(&key).await;
    assert!(retrieved.is_ok(), "Should retrieve large JSON");
    
    let data = retrieved.unwrap();
    assert!(data.is_some(), "Retrieved data should exist");
    assert_eq!(data.unwrap(), large_json.as_bytes(), "Large JSON should match exactly");
}