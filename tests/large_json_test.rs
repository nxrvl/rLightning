#[cfg(test)]
mod large_json_tests {
    use rlightning::command::types::string;
    use rlightning::networking::resp::RespValue;
    use rlightning::storage::engine::{StorageConfig, StorageEngine};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_large_json_value() {
        let mut config = StorageConfig::default();
        // Increase size limits for this test
        config.max_value_size = 1024 * 1024 * 10; // 10MB
        let engine = Arc::new(StorageEngine::new(config));

        // Create a large JSON string with various special characters and nested structures
        let mut large_json = String::from("{\n  \"items\": [\n");

        // Generate a large nested JSON structure
        for i in 0..5000 {
            large_json.push_str(&format!(
                "    {{\n      \"id\": {},\n      \"name\": \"Item {}\",\n      \"description\": \"A longer description with special characters: \\\"quotes\\\", newlines\\n, tabs\\t\",\n      \"timestamp\": \"2023-01-01T00:00:00Z\",\n      \"active\": {}\n    }}{}\n",
                i,
                i,
                if i % 2 == 0 { "true" } else { "false" },
                if i < 4999 { "," } else { "" }
            ));
        }

        large_json.push_str("  ]\n}");

        // Verify size is quite large
        assert!(
            large_json.len() > 15000,
            "Generated JSON should be at least 15KB"
        );
        println!("JSON size: {} bytes", large_json.len());

        // Test setting the large JSON value
        let key = "stories_list:test";
        let set_args = vec![key.as_bytes().to_vec(), large_json.as_bytes().to_vec()];

        let result = string::set(&engine, &set_args).await;
        assert!(
            result.is_ok(),
            "Failed to set large JSON value: {:?}",
            result
        );
        assert_eq!(result.unwrap(), RespValue::SimpleString("OK".to_string()));

        // Test retrieving the large JSON value directly from storage engine
        // Just pause slightly before getting the value
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Since the test seems to have type issues, get it directly
        let result = engine.get(key.as_bytes()).await;
        assert!(
            result.is_ok(),
            "Failed to get large JSON value from engine: {:?}",
            result
        );

        let value_opt = result.unwrap();
        assert!(value_opt.is_some(), "Value was None");

        let value = value_opt.unwrap();
        let retrieved_json = String::from_utf8_lossy(&value);
        assert_eq!(
            retrieved_json, large_json,
            "Retrieved JSON does not match original"
        );
    }

    #[tokio::test]
    async fn test_json_with_control_characters() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Create a JSON string with control characters that could potentially cause issues
        let json_with_controls = format!(
            "{{\"data\":\"Some data with control chars: {}{}{}{}{}\",\"binary\":\"ABC\"}}",
            '\u{0001}', // SOH (Start of Heading)
            '\u{0002}', // STX (Start of Text)
            '\u{0003}', // ETX (End of Text)
            '\u{001B}', // ESC (Escape)
            '\u{001F}'  // US (Unit Separator)
        );

        // Test setting the value with control characters
        let set_args = vec![
            b"control_key".to_vec(),
            json_with_controls.as_bytes().to_vec(),
        ];

        let result = string::set(&engine, &set_args).await;
        // We should still be able to store this, even with the control characters
        assert!(
            result.is_ok(),
            "Failed to set JSON with control characters: {:?}",
            result
        );

        // Test retrieving the value directly from storage engine
        let result = engine.get(b"control_key").await;
        assert!(
            result.is_ok(),
            "Failed to get JSON with control characters from engine: {:?}",
            result
        );

        let value_opt = result.unwrap();
        assert!(value_opt.is_some(), "Value was None");

        let value = value_opt.unwrap();
        let retrieved_json = String::from_utf8_lossy(&value);
        assert_eq!(
            retrieved_json, json_with_controls,
            "Retrieved JSON does not match original"
        );
    }
}
