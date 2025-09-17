#[cfg(test)]
mod type_mutation_tests {
    use rlightning::command::types::string;
    use rlightning::storage::engine::{StorageConfig, StorageEngine};
    use rlightning::networking::resp::RespValue;
    use std::sync::Arc;
    use base64::{Engine as _, engine::general_purpose};

    #[tokio::test]
    async fn test_string_type_consistency() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test the exact scenario from logs: base64-encoded JSON stored as string
        let key = "story_detail:story_id:1:show_inactive:False";
        let json_data = r#"{"id": 1, "title": "Test Story", "content": "Test content", "active": true}"#;
        let encoded_data = general_purpose::STANDARD.encode(json_data.as_bytes());
        let prefixed_data = format!("B64JSON:{}", encoded_data);
        
        println!("Testing key: {}", key);
        println!("Data: {}", prefixed_data);
        
        // Store as string
        let set_args = vec![
            key.as_bytes().to_vec(),
            prefixed_data.as_bytes().to_vec(),
        ];
        
        let set_result = string::set(&engine, &set_args).await;
        assert!(set_result.is_ok(), "SET failed: {:?}", set_result);
        println!("✓ SET successful");
        
        // Check type immediately after SET
        let type_result = engine.get_type(key.as_bytes()).await;
        assert!(type_result.is_ok(), "TYPE check failed: {:?}", type_result);
        let key_type1 = type_result.unwrap();
        println!("Type after SET: {}", key_type1);
        assert_eq!(key_type1, "string", "Key should be string type after SET");
        
        // GET the data
        let get_args = vec![key.as_bytes().to_vec()];
        let get_result = string::get(&engine, &get_args).await;
        assert!(get_result.is_ok(), "GET failed: {:?}", get_result);
        
        match get_result.unwrap() {
            RespValue::BulkString(Some(data)) => {
                let retrieved = String::from_utf8_lossy(&data);
                assert_eq!(retrieved, prefixed_data, "Retrieved data should match stored data");
                println!("✓ GET successful and data matches");
            },
            other => panic!("Expected BulkString(Some(_)), got: {:?}", other),
        }
        
        // Check type after GET - this is where mutation might occur
        let type_result = engine.get_type(key.as_bytes()).await;
        assert!(type_result.is_ok(), "TYPE check after GET failed: {:?}", type_result);
        let key_type2 = type_result.unwrap();
        println!("Type after GET: {}", key_type2);
        assert_eq!(key_type2, "string", "Key should still be string type after GET");
        
        // Multiple GETs to test for consistency
        for i in 0..5 {
            let get_result = string::get(&engine, &get_args).await;
            assert!(get_result.is_ok(), "GET {} failed: {:?}", i, get_result);
            
            let type_result = engine.get_type(key.as_bytes()).await;
            assert!(type_result.is_ok(), "TYPE check {} failed: {:?}", i, type_result);
            let key_type = type_result.unwrap();
            println!("Type after GET {}: {}", i, key_type);
            
            assert_eq!(key_type, "string", "Key should remain string type after GET {}", i);
        }
        
        println!("✓ String type remains consistent across multiple operations");
    }
    
    #[tokio::test]
    async fn test_base64_data_type_detection() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test various base64-encoded data that might be misdetected as other types
        let test_cases = vec![
            ("Simple base64", general_purpose::STANDARD.encode(b"hello world")),
            ("JSON base64", general_purpose::STANDARD.encode(r#"{"test": "value"}"#.as_bytes())),
            ("Array-like base64", general_purpose::STANDARD.encode(r#"["item1", "item2"]"#.as_bytes())),
            ("Binary base64", general_purpose::STANDARD.encode(&[0u8, 1u8, 2u8, 255u8])),
        ];
        
        for (desc, base64_data) in test_cases {
            let key = format!("test_key_{}", desc.replace(" ", "_"));
            let prefixed_data = format!("B64JSON:{}", base64_data);
            
            println!("\nTesting {}: {}", desc, key);
            
            // Store as string
            let set_args = vec![
                key.as_bytes().to_vec(),
                prefixed_data.as_bytes().to_vec(),
            ];
            
            let set_result = string::set(&engine, &set_args).await;
            assert!(set_result.is_ok(), "SET failed for {}: {:?}", desc, set_result);
            
            // Check type consistency
            for i in 0..3 {
                let type_result = engine.get_type(key.as_bytes()).await;
                assert!(type_result.is_ok(), "TYPE check {} failed for {}: {:?}", i, desc, type_result);
                let key_type = type_result.unwrap();
                
                assert_eq!(key_type, "string", "{} should be string type (check {}), got: {}", desc, i, key_type);
                
                // Also do a GET to make sure it doesn't trigger type mutation
                let get_result = string::get(&engine, &[key.as_bytes().to_vec()]).await;
                assert!(get_result.is_ok(), "GET {} failed for {}: {:?}", i, desc, get_result);
            }
            
            println!("✓ {} maintains string type consistently", desc);
        }
    }
    
    #[tokio::test]
    async fn test_problematic_binary_patterns() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test binary patterns that might accidentally deserialize as other types
        let problematic_patterns = vec![
            // Patterns that might look like bincode-serialized data
            vec![8, 0, 0, 0, 0, 0, 0, 0], // Looks like bincode array length
            vec![1, 0, 0, 0, 0, 0, 0, 0, 5, 104, 101, 108, 108, 111], // Might look like serialized string
            vec![2, 0, 0, 0, 0, 0, 0, 0], // Could look like 2-element array
            b"B64JSON:".to_vec(), // Common prefix
        ];
        
        for (i, pattern) in problematic_patterns.iter().enumerate() {
            let key = format!("binary_test_{}", i);
            
            println!("\nTesting binary pattern {}: {:?}", i, pattern);
            
            // Store the binary pattern
            let set_result = engine.set(key.as_bytes().to_vec(), pattern.clone(), None).await;
            assert!(set_result.is_ok(), "SET failed for binary pattern {}: {:?}", i, set_result);
            
            // Check type consistency
            for check in 0..3 {
                let type_result = engine.get_type(key.as_bytes()).await;
                assert!(type_result.is_ok(), "TYPE check {} failed for pattern {}: {:?}", check, i, type_result);
                let key_type = type_result.unwrap();
                
                // Should always be string for data stored via engine.set()
                assert_eq!(key_type, "string", "Binary pattern {} should be string type (check {}), got: {}", i, check, key_type);
                
                // Do a GET to ensure consistency
                let get_result = engine.get(key.as_bytes()).await;
                assert!(get_result.is_ok(), "GET {} failed for pattern {}: {:?}", check, i, get_result);
                assert_eq!(get_result.unwrap().unwrap(), *pattern, "Data mismatch for pattern {}", i);
            }
            
            println!("✓ Binary pattern {} maintains string type", i);
        }
    }
}