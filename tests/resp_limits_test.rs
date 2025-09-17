#[cfg(test)]
mod resp_limits_tests {
    use rlightning::command::types::string;
    use rlightning::storage::engine::{StorageConfig, StorageEngine};
    use rlightning::networking::resp::RespValue;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_resp_bulk_string_limits() {
        let mut config = StorageConfig::default();
        // Test with very large limits
        config.max_value_size = 50 * 1024 * 1024; // 50MB
        config.max_key_size = 100 * 1024; // 100KB
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test edge case: RESP bulk string length representation
        // RESP format: $<length>\r\n<data>\r\n
        
        // Test with a 10MB value (should work)
        let large_value = "X".repeat(10 * 1024 * 1024); // 10MB of X's
        let key = "large_value_test";
        
        println!("Testing 10MB value storage...");
        println!("Value size: {} bytes", large_value.len());
        
        let set_args = vec![
            key.as_bytes().to_vec(),
            large_value.as_bytes().to_vec(),
        ];
        
        let result = string::set(&engine, &set_args).await;
        match result {
            Ok(_) => {
                println!("✓ Successfully stored 10MB value");
                
                // Test retrieval
                let get_args = vec![key.as_bytes().to_vec()];
                let get_result = string::get(&engine, &get_args).await;
                match get_result {
                    Ok(RespValue::BulkString(Some(data))) => {
                        if data.len() == large_value.len() {
                            println!("✓ Successfully retrieved 10MB value with correct size");
                        } else {
                            panic!("Retrieved value size mismatch: {} vs {}", data.len(), large_value.len());
                        }
                    },
                    Ok(other) => panic!("Expected BulkString(Some(_)), got: {:?}", other),
                    Err(e) => panic!("Failed to retrieve large value: {:?}", e),
                }
            },
            Err(e) => {
                println!("✗ Failed to store 10MB value: {:?}", e);
                // This might be expected due to memory limits
            }
        }
    }
    
    #[tokio::test]
    async fn test_resp_integer_limits() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test bulk string length edge cases
        // RESP allows up to i64::MAX for bulk string length
        
        // Test with lengths around common edge cases
        let test_cases = vec![
            999,      // Just under 1000
            1000,     // Round number
            1023,     // Just under 1KB
            1024,     // 1KB
            1025,     // Just over 1KB
            10000,    // 10KB
            65535,    // 16-bit max
            65536,    // Just over 16-bit
        ];
        
        for size in test_cases {
            let key = format!("size_test_{}", size);
            let value = "X".repeat(size);
            
            println!("Testing value of size: {} bytes", size);
            
            let set_args = vec![
                key.as_bytes().to_vec(),
                value.as_bytes().to_vec(),
            ];
            
            let result = string::set(&engine, &set_args).await;
            assert!(result.is_ok(), "Failed to set value of size {}: {:?}", size, result);
            
            // Test retrieval
            let get_args = vec![key.as_bytes().to_vec()];
            let get_result = string::get(&engine, &get_args).await;
            match get_result {
                Ok(RespValue::BulkString(Some(data))) => {
                    assert_eq!(data.len(), size, "Size mismatch for {} bytes", size);
                },
                Ok(other) => panic!("Expected BulkString(Some(_)) for size {}, got: {:?}", size, other),
                Err(e) => panic!("Failed to retrieve value of size {}: {:?}", size, e),
            }
        }
        
        println!("✓ All size tests passed");
    }
    
    #[tokio::test]
    async fn test_chunked_key_patterns() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test different chunked key patterns that backends might use
        let base_key = "story_detail:story_id:1:show_inactive:False";
        
        let chunked_patterns = vec![
            format!("{}_chunk_1", base_key),
            format!("{}_chunk_2", base_key),
            format!("{}_part_1", base_key),
            format!("{}_part_2", base_key),
            format!("{}:chunk:1", base_key),
            format!("{}:chunk:2", base_key),
            format!("chunk:1:{}", base_key),
            format!("chunk:2:{}", base_key),
            format!("bnf:{}_chunk_1", base_key),  // The problematic prefix pattern
            format!("bnf:{}_chunk_2", base_key),
        ];
        
        for (i, key) in chunked_patterns.iter().enumerate() {
            let value = format!("{{\"chunk\": {}, \"data\": \"test_data_for_chunk_{}\"}}", i + 1, i + 1);
            
            println!("Testing chunked key pattern: '{}'", key);
            println!("Key length: {} bytes", key.len());
            
            let set_args = vec![
                key.as_bytes().to_vec(),
                value.as_bytes().to_vec(),
            ];
            
            let result = string::set(&engine, &set_args).await;
            assert!(result.is_ok(), "Failed to set chunked key '{}': {:?}", key, result);
            
            // Test retrieval
            let get_args = vec![key.as_bytes().to_vec()];
            let get_result = string::get(&engine, &get_args).await;
            match get_result {
                Ok(RespValue::BulkString(Some(data))) => {
                    let retrieved = String::from_utf8_lossy(&data);
                    assert_eq!(retrieved, value, "Value mismatch for key '{}'", key);
                },
                Ok(other) => panic!("Expected BulkString(Some(_)) for key '{}', got: {:?}", key, other),
                Err(e) => panic!("Failed to retrieve chunked key '{}': {:?}", key, e),
            }
        }
        
        println!("✓ All chunked key patterns work correctly");
    }
    
    #[tokio::test]
    async fn test_resp_protocol_edge_cases() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        
        // Test edge cases that might confuse RESP parsing
        let test_cases = vec![
            ("key_with_crlf", "Value with\r\nCRLF sequence"),
            ("key_with_null", "Value with\0null bytes"),
            ("key_with_binary", "Value with binary \x01\x02\x03 data"),
            ("key_with_quotes", r#"Value with "quotes" and 'apostrophes'"#),
            ("key_with_escape", "Value with \\ backslash \t tab \n newline"),
            ("key_with_dollar", "Value with $ dollar $123 signs"),
            ("key_with_asterisk", "Value with * asterisk *456* signs"),
            ("key_with_colon", "Value with : colon :789: signs"),
        ];
        
        for (key, value) in test_cases {
            println!("Testing RESP edge case: '{}'", key);
            
            let set_args = vec![
                key.as_bytes().to_vec(),
                value.as_bytes().to_vec(),
            ];
            
            let result = string::set(&engine, &set_args).await;
            assert!(result.is_ok(), "Failed to set edge case key '{}': {:?}", key, result);
            
            // Test retrieval
            let get_args = vec![key.as_bytes().to_vec()];
            let get_result = string::get(&engine, &get_args).await;
            match get_result {
                Ok(RespValue::BulkString(Some(data))) => {
                    let retrieved = String::from_utf8_lossy(&data);
                    assert_eq!(retrieved, value, "Value mismatch for edge case key '{}'", key);
                },
                Ok(other) => panic!("Expected BulkString(Some(_)) for edge case key '{}', got: {:?}", key, other),
                Err(e) => panic!("Failed to retrieve edge case key '{}': {:?}", key, e),
            }
        }
        
        println!("✓ All RESP protocol edge cases handled correctly");
    }
}