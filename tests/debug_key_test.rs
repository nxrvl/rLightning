#[cfg(test)]
mod debug_key_tests {
    use rlightning::command::types::string;
    use rlightning::networking::resp::RespValue;
    use rlightning::storage::engine::{StorageConfig, StorageEngine};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_problematic_backend_key() {
        let config = StorageConfig {
            max_value_size: 1024 * 1024 * 10, // 10MB
            ..Default::default()
        };
        let engine = Arc::new(StorageEngine::new(config));

        // Test the exact key from the backend logs
        let problematic_key = "story_detail:story_id:1:show_inactive:False";
        let prefixed_key = "bnf:story_detail:story_id:1:show_inactive:False";

        // Create a large JSON like the backend is trying to store (5012 chars)
        let mut large_json = String::from(
            r#"{"id": 1, "title": "Test Story", "content": "This is a test story with a lot of content to make it around 5012 characters long. "#,
        );

        // Pad to approximately 5012 characters
        while large_json.len() < 5000 {
            large_json.push_str("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ");
        }
        large_json.push_str(r#""active": true, "created_at": "2025-05-22T16:52:41.753Z"}"#);

        println!("Testing key: '{}'", problematic_key);
        println!("Key length: {} bytes", problematic_key.len());
        println!("JSON size: {} bytes", large_json.len());

        // Test setting with the problematic key
        let set_args = vec![
            problematic_key.as_bytes().to_vec(),
            large_json.as_bytes().to_vec(),
        ];

        let result = string::set(&engine, &set_args).await;
        assert!(
            result.is_ok(),
            "Failed to set with problematic key: {:?}",
            result
        );
        assert_eq!(result.unwrap(), RespValue::SimpleString("OK".to_string()));

        // Test getting with the original key
        let get_args = vec![problematic_key.as_bytes().to_vec()];
        let result = string::get(&engine, &get_args).await;
        assert!(
            result.is_ok(),
            "Failed to get with original key: {:?}",
            result
        );

        // Test getting with the prefixed key (this should fail/return nil)
        let get_args_prefixed = vec![prefixed_key.as_bytes().to_vec()];
        let result_prefixed = string::get(&engine, &get_args_prefixed).await;
        assert!(
            result_prefixed.is_ok(),
            "GET should succeed even for non-existent keys"
        );

        match result_prefixed.unwrap() {
            RespValue::BulkString(None) => println!("✓ Prefixed key correctly returns nil"),
            other => panic!(
                "Expected BulkString(None) for prefixed key, got: {:?}",
                other
            ),
        }

        println!("✓ Test completed successfully");
    }

    #[tokio::test]
    async fn test_very_long_key() {
        let config = StorageConfig {
            max_key_size: 2048, // Increase key size limit for this test
            ..Default::default()
        };
        let engine = Arc::new(StorageEngine::new(config));

        // Create a key that's exactly at the limit
        let long_key = "a".repeat(1024);
        let very_long_key = "a".repeat(1025); // Just over default limit

        println!("Testing long key of {} bytes", long_key.len());

        // Test setting with exactly 1024 byte key (should work)
        let set_args = vec![long_key.as_bytes().to_vec(), b"test_value".to_vec()];

        let result = string::set(&engine, &set_args).await;
        assert!(
            result.is_ok(),
            "Failed to set with 1024-byte key: {:?}",
            result
        );

        // Test setting with 1025 byte key (should fail with default config)
        let engine_default = Arc::new(StorageEngine::new(StorageConfig::default()));
        let set_args_long = vec![very_long_key.as_bytes().to_vec(), b"test_value".to_vec()];

        let result_long = string::set(&engine_default, &set_args_long).await;
        assert!(result_long.is_err(), "Should fail with key over 1024 bytes");

        println!("✓ Key size validation working correctly");
    }

    #[tokio::test]
    async fn test_chunked_json_simulation() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Simulate what the backend might be doing with chunked JSON storage
        let base_key = "story_detail:story_id:1:show_inactive:False";
        let chunk1_key = format!("{}_chunk_1", base_key);
        let chunk2_key = format!("{}_chunk_2", base_key);

        let json_part1 = r#"{"id": 1, "title": "Test Story", "content": "First part of the story"#;
        let json_part2 = r#", "active": true, "created_at": "2025-05-22T16:52:41.753Z"}"#;

        println!("Testing chunked storage simulation");
        println!("Chunk 1 key: '{}'", chunk1_key);
        println!("Chunk 2 key: '{}'", chunk2_key);

        // Store chunks
        let result1 = string::set(
            &engine,
            &[
                chunk1_key.as_bytes().to_vec(),
                json_part1.as_bytes().to_vec(),
            ],
        )
        .await;
        let result2 = string::set(
            &engine,
            &[
                chunk2_key.as_bytes().to_vec(),
                json_part2.as_bytes().to_vec(),
            ],
        )
        .await;

        assert!(result1.is_ok(), "Failed to store chunk 1");
        assert!(result2.is_ok(), "Failed to store chunk 2");

        // Retrieve chunks
        let get1 = string::get(&engine, &[chunk1_key.as_bytes().to_vec()]).await;
        let get2 = string::get(&engine, &[chunk2_key.as_bytes().to_vec()]).await;

        assert!(get1.is_ok(), "Failed to retrieve chunk 1");
        assert!(get2.is_ok(), "Failed to retrieve chunk 2");

        println!("✓ Chunked storage simulation successful");
    }
}
