use rlightning::command::types::string::{get, set};
use rlightning::networking::resp::RespValue;
use rlightning::storage::engine::{StorageConfig, StorageEngine};
use std::sync::Arc;

/// Test the specific B64JSON case that was causing issues
#[tokio::test]
async fn test_b64json_set_get() {
    let config = StorageConfig::default();
    let engine = Arc::new(StorageEngine::new(config));

    // This is the exact pattern that was causing "Invalid RESP format, starts with: B64JSON:W3"
    let b64json_data = "B64JSON:W3siaWQiOiAxLCJuYW1lIjoiRXhhbXBsZSIsImFjdGl2ZSI6dHJ1ZX0=";

    // SET the problematic data
    let set_args = vec![
        b"test_b64json_key".to_vec(),
        b64json_data.as_bytes().to_vec(),
    ];

    let set_result = set(&engine, &set_args).await;
    assert!(
        set_result.is_ok(),
        "SET with B64JSON data should succeed: {:?}",
        set_result
    );
    assert_eq!(
        set_result.unwrap(),
        RespValue::SimpleString("OK".to_string())
    );

    // GET the data back
    let get_args = vec![b"test_b64json_key".to_vec()];
    let get_result = get(&engine, &get_args).await;
    assert!(
        get_result.is_ok(),
        "GET with B64JSON data should succeed: {:?}",
        get_result
    );

    // Verify we get back exactly the same data
    if let Ok(RespValue::BulkString(Some(retrieved_data))) = get_result {
        let retrieved_str = String::from_utf8_lossy(&retrieved_data);
        assert_eq!(
            retrieved_str, b64json_data,
            "Retrieved B64JSON data should match original exactly"
        );
        println!("✅ B64JSON test passed! Original: {}", b64json_data);
        println!("✅ Retrieved: {}", retrieved_str);
    } else {
        panic!("Expected BulkString response for B64JSON data");
    }
}

#[tokio::test]
async fn test_large_b64json_data() {
    let config = StorageConfig::default();
    let engine = Arc::new(StorageEngine::new(config));

    // Create a larger B64JSON string that would definitely trigger the bug
    let large_b64json = format!("B64JSON:W3{}", "a".repeat(15000));

    let set_args = vec![
        b"large_b64json_key".to_vec(),
        large_b64json.as_bytes().to_vec(),
    ];

    let set_result = set(&engine, &set_args).await;
    assert!(set_result.is_ok(), "SET with large B64JSON should succeed");

    let get_args = vec![b"large_b64json_key".to_vec()];
    let get_result = get(&engine, &get_args).await;
    assert!(get_result.is_ok(), "GET with large B64JSON should succeed");

    if let Ok(RespValue::BulkString(Some(retrieved_data))) = get_result {
        let retrieved_str = String::from_utf8_lossy(&retrieved_data);
        assert_eq!(
            retrieved_str, large_b64json,
            "Large B64JSON data should match exactly"
        );
        println!(
            "✅ Large B64JSON test passed! Size: {} bytes",
            large_b64json.len()
        );
    } else {
        panic!("Expected BulkString response for large B64JSON data");
    }
}

#[tokio::test]
async fn test_multiple_set_get_operations() {
    let config = StorageConfig::default();
    let engine = Arc::new(StorageEngine::new(config));

    // Test multiple operations in sequence to ensure no buffer contamination
    let test_data = vec![
        ("key1", "B64JSON:W3data1"),
        ("key2", "normal_string"),
        ("key3", "B64JSON:W3data3"),
        ("key4", "+not_a_simple_string"),
        ("key5", "B64JSON:W3data5"),
    ];

    // SET all keys
    for (key, value) in &test_data {
        let set_args = vec![key.as_bytes().to_vec(), value.as_bytes().to_vec()];
        let result = set(&engine, &set_args).await;
        assert!(result.is_ok(), "SET should succeed for key: {}", key);
    }

    // GET all keys and verify data
    for (key, expected_value) in &test_data {
        let get_args = vec![key.as_bytes().to_vec()];
        let result = get(&engine, &get_args).await;
        assert!(result.is_ok(), "GET should succeed for key: {}", key);

        if let Ok(RespValue::BulkString(Some(data))) = result {
            let retrieved = String::from_utf8_lossy(&data);
            assert_eq!(
                retrieved, *expected_value,
                "Data should match for key: {}",
                key
            );
        } else {
            panic!("Expected BulkString response for key: {}", key);
        }
    }

    println!("✅ Multiple SET/GET operations test passed!");
}
