use std::net::SocketAddr;
use std::time::Duration;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{EvictionPolicy, StorageConfig, StorageEngine};

#[tokio::test]
async fn test_large_json_set_get() {
    // Start a server
    let storage_config = StorageConfig {
        max_memory: 1024 * 1024 * 100, // 100MB
        eviction_policy: EvictionPolicy::LRU,
        default_ttl: Duration::from_secs(0),
        max_key_size: 1024 * 1024,        // 1MB
        max_value_size: 1024 * 1024 * 10, // 10MB
    };

    let store = StorageEngine::new(storage_config);
    let server_addr: SocketAddr = "127.0.0.1:17100".parse().unwrap();
    let server = Server::new(server_addr, store);

    tokio::spawn(async move {
        server.start().await.unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a client
    let mut client = Client::connect(server_addr).await.unwrap();

    // Create a large JSON object (5.4KB) similar to the one in the logs
    let mut json_data = String::from("{\n");

    // Add fields to make it approximately 5.4KB
    for i in 0..100 {
        json_data.push_str(&format!("  \"field_{}\": \"This is test data for field {}. Adding some text to make it larger.\",\n", i, i));
    }

    // Add nested objects with different structures
    json_data.push_str("  \"nested\": {\n");
    for i in 0..30 {
        json_data.push_str(&format!("    \"nested_field_{}\": {},\n", i, i * 10));
    }

    // Add array with various elements
    json_data.push_str("    \"array\": [");
    for i in 0..20 {
        if i < 19 {
            json_data.push_str(&format!("{}, ", i));
        } else {
            json_data.push_str(&format!("{}", i));
        }
    }
    json_data.push_str("]\n  },\n");

    // Add some text with control characters to test sanitization
    json_data.push_str(&format!(
        "  \"control_chars\": \"Text with control chars: \\u001F separator \\u001E record\",\n"
    ));

    // Close the JSON object
    json_data.push_str("  \"final_field\": \"value\"\n}");

    let key = "test_large_json";

    // Use send_command to send JSON.SET command
    let set_response = client
        .send_command(vec![
            b"JSON.SET".to_vec(),
            key.as_bytes().to_vec(),
            b".".to_vec(),
            json_data.as_bytes().to_vec(),
        ])
        .await
        .unwrap();

    match set_response {
        RespValue::SimpleString(s) if s == "OK" => {
            println!("JSON.SET successful");
        }
        _ => {
            panic!(
                "JSON.SET failed with unexpected response: {:?}",
                set_response
            );
        }
    }

    // Now get the JSON using JSON.GET command
    let get_response = client
        .send_command(vec![
            b"JSON.GET".to_vec(),
            key.as_bytes().to_vec(),
            b".".to_vec(),
        ])
        .await
        .unwrap();

    match get_response {
        RespValue::BulkString(Some(data)) => {
            let retrieved_json = String::from_utf8_lossy(&data).to_string();
            println!("Retrieved JSON size: {} bytes", retrieved_json.len());

            // Check that the JSON is valid and matches what we set
            assert!(
                retrieved_json.len() > 5000,
                "Retrieved JSON should be at least 5KB"
            );
            assert!(
                retrieved_json.starts_with("{"),
                "Retrieved JSON should start with {{"
            );
            assert!(
                retrieved_json.ends_with("}"),
                "Retrieved JSON should end with }}"
            );

            // Parse the JSON to ensure it's valid
            let parsed: serde_json::Value =
                serde_json::from_str(&retrieved_json).expect("Retrieved JSON should be valid");

            // Verify some of the content
            assert!(parsed["field_1"].is_string(), "Field_1 should be a string");
            assert!(
                parsed["nested"]["array"].is_array(),
                "Nested array should be an array"
            );

            // Check control characters were handled
            let control_chars_text = parsed["control_chars"].as_str().unwrap();
            assert!(
                control_chars_text.contains("control chars"),
                "Control chars text should be present"
            );
        }
        _ => {
            panic!(
                "JSON.GET failed with unexpected response: {:?}",
                get_response
            );
        }
    }
}

#[tokio::test]
async fn test_python_redis_client_format() {
    // Start a server
    let storage_config = StorageConfig {
        max_memory: 1024 * 1024 * 100, // 100MB
        eviction_policy: EvictionPolicy::LRU,
        default_ttl: Duration::from_secs(0),
        max_key_size: 1024 * 1024,        // 1MB
        max_value_size: 1024 * 1024 * 10, // 10MB
    };

    let store = StorageEngine::new(storage_config);
    let server_addr: SocketAddr = "127.0.0.1:17101".parse().unwrap();
    let server = Server::new(server_addr, store);

    tokio::spawn(async move {
        server.start().await.unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a client
    let mut client = Client::connect(server_addr).await.unwrap();

    // Format command similar to what the Python redis-py client would send
    // The Python redis-py client prefixes keys with a namespace (like "bnf:")
    let key = "bnf:story_detail:story_id:2:show_inactive:False";

    // Create a JSON object similar to the one in the logs (approximately 5.4KB)
    let mut json_data = String::from("{\n");

    // Add fields to make it approximately 5.4KB
    for i in 0..100 {
        json_data.push_str(&format!(
            "  \"field_{}\": \"This is test data for field {}. Adding text to make it larger.\",\n",
            i, i
        ));
    }

    // Add nested objects with arrays and various data types
    json_data.push_str("  \"story\": {\n");
    json_data.push_str("    \"id\": 2,\n");
    json_data.push_str("    \"title\": \"Test Story\",\n");
    json_data
        .push_str("    \"description\": \"This is a test story for Redis protocol testing.\",\n");
    json_data.push_str("    \"show_inactive\": false,\n");
    json_data.push_str("    \"tags\": [\"test\", \"redis\", \"protocol\"],\n");
    json_data.push_str("    \"metadata\": {\n");
    json_data.push_str("      \"created_at\": \"2024-01-01T00:00:00Z\",\n");
    json_data.push_str("      \"updated_at\": \"2024-01-02T00:00:00Z\"\n");
    json_data.push_str("    }\n");
    json_data.push_str("  },\n");

    // Close the JSON object
    json_data.push_str("  \"status\": \"active\"\n}");

    println!("Test JSON size: {} bytes", json_data.len());

    // Use send_command to send JSON.SET command (simulating Python client)
    let set_response = client
        .send_command(vec![
            b"JSON.SET".to_vec(),
            key.as_bytes().to_vec(),
            b".".to_vec(),
            json_data.as_bytes().to_vec(),
        ])
        .await
        .unwrap();

    match set_response {
        RespValue::SimpleString(s) if s == "OK" => {
            println!("JSON.SET successful for Python-style key");
        }
        _ => {
            panic!(
                "JSON.SET failed with unexpected response: {:?}",
                set_response
            );
        }
    }

    // Now get the JSON using JSON.GET command
    let get_response = client
        .send_command(vec![
            b"JSON.GET".to_vec(),
            key.as_bytes().to_vec(),
            b".".to_vec(),
        ])
        .await
        .unwrap();

    match get_response {
        RespValue::BulkString(Some(data)) => {
            let retrieved_json = String::from_utf8_lossy(&data).to_string();
            println!(
                "Retrieved JSON for Python-style key: {} bytes",
                retrieved_json.len()
            );

            // Parse and verify the JSON
            let parsed: serde_json::Value =
                serde_json::from_str(&retrieved_json).expect("Retrieved JSON should be valid");

            // Verify the story structure
            assert_eq!(parsed["story"]["id"], 2);
            assert_eq!(parsed["story"]["title"], "Test Story");
            assert_eq!(parsed["story"]["show_inactive"], false);
            assert!(parsed["story"]["tags"].is_array());
            assert_eq!(parsed["status"], "active");
        }
        _ => {
            panic!(
                "JSON.GET failed with unexpected response: {:?}",
                get_response
            );
        }
    }
}

#[tokio::test]
async fn test_json_special_characters() {
    // Start a server
    let storage_config = StorageConfig {
        max_memory: 1024 * 1024 * 100, // 100MB
        eviction_policy: EvictionPolicy::LRU,
        default_ttl: Duration::from_secs(0),
        max_key_size: 1024 * 1024,        // 1MB
        max_value_size: 1024 * 1024 * 10, // 10MB
    };

    let store = StorageEngine::new(storage_config);
    let server_addr: SocketAddr = "127.0.0.1:17102".parse().unwrap();
    let server = Server::new(server_addr, store);

    tokio::spawn(async move {
        server.start().await.unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a client
    let mut client = Client::connect(server_addr).await.unwrap();

    // Test with various special characters
    let test_cases = vec![
        ("simple", r#"{"name": "test"}"#),
        ("quotes", r#"{"text": "He said \"hello\""}"#),
        ("newlines", r#"{"text": "Line 1\nLine 2"}"#),
        ("tabs", r#"{"text": "Col1\tCol2"}"#),
        ("unicode", r#"{"emoji": "😀", "chinese": "你好"}"#),
        ("escaped", r#"{"path": "C:\\Users\\test"}"#),
    ];

    for (test_name, json_data) in test_cases {
        let key = format!("test_special_{}", test_name);

        // Set the JSON
        let set_response = client
            .send_command(vec![
                b"JSON.SET".to_vec(),
                key.as_bytes().to_vec(),
                b".".to_vec(),
                json_data.as_bytes().to_vec(),
            ])
            .await
            .unwrap();

        assert!(
            matches!(set_response, RespValue::SimpleString(ref s) if s == "OK"),
            "JSON.SET should succeed for {}",
            test_name
        );

        // Get the JSON back
        let get_response = client
            .send_command(vec![
                b"JSON.GET".to_vec(),
                key.as_bytes().to_vec(),
                b".".to_vec(),
            ])
            .await
            .unwrap();

        match get_response {
            RespValue::BulkString(Some(data)) => {
                let retrieved = String::from_utf8_lossy(&data);
                // Parse both to compare
                let original: serde_json::Value = serde_json::from_str(json_data).unwrap();
                let retrieved_parsed: serde_json::Value = serde_json::from_str(&retrieved).unwrap();
                assert_eq!(
                    original, retrieved_parsed,
                    "JSON should match for {}",
                    test_name
                );
            }
            _ => panic!("JSON.GET failed for {}", test_name),
        }
    }
}
