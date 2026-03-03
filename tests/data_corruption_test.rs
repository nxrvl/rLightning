#[cfg(test)]
mod data_corruption_tests {
    use base64::{Engine as _, engine::general_purpose};
    use rlightning::command::types::string;
    use rlightning::networking::resp::RespValue;
    use rlightning::storage::engine::{StorageConfig, StorageEngine};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_base64_json_corruption_scenario() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Test the exact scenario from the logs:
        // 1. SET base64-encoded JSON data
        // 2. GET immediately - should work
        // 3. GET again - should still work (not change type)

        let key = "story_detail:story_id:1:show_inactive:False";

        // Create base64-encoded JSON like the backend does
        let json_data =
            r#"{"id": 1, "title": "Test Story", "content": "Test content", "active": true}"#;
        let encoded_data = general_purpose::STANDARD.encode(json_data.as_bytes());
        let prefixed_data = format!("B64JSON:{}", encoded_data);

        println!("Original JSON: {}", json_data);
        println!("Base64 encoded: {}", encoded_data);
        println!("Prefixed data: {}", prefixed_data);
        println!("Data length: {} bytes", prefixed_data.len());

        // Step 1: SET the data
        println!("\n--- Step 1: SET operation ---");
        let set_args = vec![key.as_bytes().to_vec(), prefixed_data.as_bytes().to_vec()];

        let set_result = string::set(&engine, &set_args).await;
        assert!(set_result.is_ok(), "SET failed: {:?}", set_result);
        println!("✓ SET operation successful");

        // Check if key exists immediately after SET
        let exists_result = engine.exists(key.as_bytes()).await;
        assert!(
            exists_result.is_ok(),
            "EXISTS check failed: {:?}",
            exists_result
        );
        assert!(exists_result.unwrap(), "Key should exist after SET");
        println!("✓ Key exists after SET");

        // Check the type immediately after SET
        let type_result = engine.get_type(key.as_bytes()).await;
        assert!(type_result.is_ok(), "TYPE check failed: {:?}", type_result);
        let key_type = type_result.unwrap();
        println!("Key type after SET: {}", key_type);
        assert_eq!(key_type, "string", "Key should be string type after SET");

        // Step 2: GET the data immediately
        println!("\n--- Step 2: First GET operation ---");
        let get_args = vec![key.as_bytes().to_vec()];
        let get_result = string::get(&engine, &get_args).await;
        assert!(get_result.is_ok(), "First GET failed: {:?}", get_result);

        match get_result.unwrap() {
            RespValue::BulkString(Some(data)) => {
                let retrieved = String::from_utf8_lossy(&data);
                println!("Retrieved data: {}", retrieved);
                assert_eq!(
                    retrieved, prefixed_data,
                    "Retrieved data should match stored data"
                );
                println!("✓ First GET successful and data matches");
            }
            other => panic!("Expected BulkString(Some(_)), got: {:?}", other),
        }

        // Check type after first GET
        let type_result = engine.get_type(key.as_bytes()).await;
        assert!(
            type_result.is_ok(),
            "TYPE check after first GET failed: {:?}",
            type_result
        );
        let key_type = type_result.unwrap();
        println!("Key type after first GET: {}", key_type);
        assert_eq!(
            key_type, "string",
            "Key should still be string type after first GET"
        );

        // Step 3: GET the data again (this is where corruption might happen)
        println!("\n--- Step 3: Second GET operation ---");
        let get_result2 = string::get(&engine, &get_args).await;
        assert!(get_result2.is_ok(), "Second GET failed: {:?}", get_result2);

        match get_result2.unwrap() {
            RespValue::BulkString(Some(data)) => {
                let retrieved = String::from_utf8_lossy(&data);
                println!("Retrieved data (2nd GET): {}", retrieved);
                assert_eq!(
                    retrieved, prefixed_data,
                    "Second GET should return same data"
                );
                println!("✓ Second GET successful and data still matches");
            }
            RespValue::BulkString(None) => panic!("Second GET returned nil - data was lost!"),
            other => panic!("Expected BulkString(Some(_)), got: {:?}", other),
        }

        // Check type after second GET
        let type_result = engine.get_type(key.as_bytes()).await;
        assert!(
            type_result.is_ok(),
            "TYPE check after second GET failed: {:?}",
            type_result
        );
        let key_type = type_result.unwrap();
        println!("Key type after second GET: {}", key_type);
        assert_eq!(
            key_type, "string",
            "Key should still be string type after second GET"
        );

        // Step 4: Multiple rapid GETs to test for race conditions
        println!("\n--- Step 4: Rapid GET operations ---");
        for i in 0..10 {
            let get_result = string::get(&engine, &get_args).await;
            assert!(
                get_result.is_ok(),
                "Rapid GET {} failed: {:?}",
                i,
                get_result
            );

            match get_result.unwrap() {
                RespValue::BulkString(Some(data)) => {
                    let retrieved = String::from_utf8_lossy(&data);
                    assert_eq!(retrieved, prefixed_data, "Rapid GET {} data mismatch", i);
                }
                other => panic!("Rapid GET {} returned unexpected: {:?}", i, other),
            }
        }
        println!("✓ All rapid GETs successful");

        println!("\n✓ All tests passed - no data corruption detected");
    }

    #[tokio::test]
    async fn test_concurrent_set_get_operations() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Test concurrent operations that might cause corruption
        let key = "concurrent_test_key";
        let json_data = r#"{"concurrent": true, "test": "data"}"#;
        let encoded_data = general_purpose::STANDARD.encode(json_data.as_bytes());
        let prefixed_data = format!("B64JSON:{}", encoded_data);

        println!("Testing concurrent SET/GET operations");

        // Spawn multiple tasks doing SET and GET operations
        let mut handles = vec![];

        for i in 0..5 {
            let engine_clone = Arc::clone(&engine);
            let key_clone = format!("{}_{}", key, i);
            let data_clone = prefixed_data.clone();

            let handle = tokio::spawn(async move {
                // SET operation
                let set_args = vec![
                    key_clone.as_bytes().to_vec(),
                    data_clone.as_bytes().to_vec(),
                ];

                let set_result = string::set(&engine_clone, &set_args).await;
                assert!(
                    set_result.is_ok(),
                    "Concurrent SET {} failed: {:?}",
                    i,
                    set_result
                );

                // Immediate GET
                let get_args = vec![key_clone.as_bytes().to_vec()];
                let get_result = string::get(&engine_clone, &get_args).await;
                assert!(
                    get_result.is_ok(),
                    "Concurrent GET {} failed: {:?}",
                    i,
                    get_result
                );

                match get_result.unwrap() {
                    RespValue::BulkString(Some(data)) => {
                        let retrieved = String::from_utf8_lossy(&data);
                        assert_eq!(
                            retrieved, data_clone,
                            "Concurrent operation {} data mismatch",
                            i
                        );
                    }
                    other => panic!(
                        "Concurrent operation {} returned unexpected: {:?}",
                        i, other
                    ),
                }

                i
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            let result = handle.await;
            assert!(result.is_ok(), "Concurrent task failed: {:?}", result);
        }

        println!("✓ All concurrent operations completed successfully");
    }

    #[tokio::test]
    async fn test_large_base64_data_corruption() {
        let mut config = StorageConfig::default();
        config.max_value_size = 10 * 1024 * 1024; // 10MB
        let engine = Arc::new(StorageEngine::new(config));

        // Test with large base64-encoded data (like 5KB+ JSON from logs)
        let mut large_json = String::from(r#"{"items": ["#);
        for i in 0..1000 {
            large_json.push_str(&format!(
                r#"{{"id": {}, "data": "item_{}_data_with_more_content"}},"#,
                i, i
            ));
        }
        large_json.push_str(r#""end": true]}"#);

        let encoded_data = general_purpose::STANDARD.encode(large_json.as_bytes());
        let prefixed_data = format!("B64JSON:{}", encoded_data);

        println!("Large JSON size: {} bytes", large_json.len());
        println!("Base64 encoded size: {} bytes", encoded_data.len());
        println!("Prefixed data size: {} bytes", prefixed_data.len());

        let key = "large_base64_test";

        // SET large data
        let set_args = vec![key.as_bytes().to_vec(), prefixed_data.as_bytes().to_vec()];

        let set_result = string::set(&engine, &set_args).await;
        assert!(
            set_result.is_ok(),
            "Large data SET failed: {:?}",
            set_result
        );
        println!("✓ Large data SET successful");

        // GET large data multiple times
        for i in 0..5 {
            let get_args = vec![key.as_bytes().to_vec()];
            let get_result = string::get(&engine, &get_args).await;
            assert!(
                get_result.is_ok(),
                "Large data GET {} failed: {:?}",
                i,
                get_result
            );

            match get_result.unwrap() {
                RespValue::BulkString(Some(data)) => {
                    let retrieved = String::from_utf8_lossy(&data);
                    assert_eq!(
                        retrieved.len(),
                        prefixed_data.len(),
                        "Large data size mismatch on GET {}",
                        i
                    );
                    assert_eq!(
                        retrieved, prefixed_data,
                        "Large data content mismatch on GET {}",
                        i
                    );
                }
                other => panic!("Large data GET {} returned unexpected: {:?}", i, other),
            }

            println!("✓ Large data GET {} successful", i);
        }

        println!("✓ Large base64 data test completed without corruption");
    }
}
