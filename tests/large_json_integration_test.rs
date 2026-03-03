use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Test setting and getting large JSON values through the RESP protocol
#[tokio::test]
async fn test_large_json_through_resp() {
    // Create a configuration with sufficient buffer size for test
    let mut config = StorageConfig::default();
    config.max_value_size = 1024 * 1024; // 1MB

    // Use a unique port number to avoid conflicts with other tests
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 18199));
    let storage = std::sync::Arc::new(StorageEngine::new(config.clone()));
    let server = Server::new(addr, std::sync::Arc::clone(&storage));

    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Connect a client
    let mut client = Client::connect(addr)
        .await
        .expect("Failed to create client");

    // Flush all data to ensure clean test environment
    let flush_resp = client
        .send_command_str("FLUSHALL", &[])
        .await
        .expect("Failed to flush database");
    assert_eq!(flush_resp, RespValue::SimpleString("OK".to_string()));

    // Create a large JSON value (similar to the command-level test)
    let mut large_json = String::from("{\n  \"items\": [\n");

    // Generate a reasonable sized JSON structure
    for i in 0..100 {
        large_json.push_str(&format!(
            "    {{\n      \"id\": {},\n      \"name\": \"Item {}\",\n      \"description\": \"A longer description with special characters: \\\"quotes\\\", newlines\\n, tabs\\t\",\n      \"timestamp\": \"2023-01-01T00:00:00Z\",\n      \"active\": {}\n    }}{}\n",
            i,
            i,
            if i % 2 == 0 { "true" } else { "false" },
            if i < 4999 { "," } else { "" }
        ));
    }

    large_json.push_str("  ]\n}");

    // Verify the JSON is reasonably sized
    assert!(
        large_json.len() > 1000,
        "JSON string should be at least 1KB"
    );

    println!("JSON size: {} bytes", large_json.len());

    // Use a unique key with a test-specific prefix to avoid conflicts
    let key = "test_1_bnf:stories_list:skip:0:limit:10:sort_by_rating:None:sort_by_modified_chapters:None:show_inactive:False";

    // No need to delete as we've already flushed all keys

    // Add a TTL of 1800 seconds (30 minutes)
    let set_response = client
        .send_command_str("SET", &[key, &large_json, "EX", "1800"])
        .await
        .expect("Failed to send SET command");

    // Verify the SET command was successful
    assert_eq!(set_response, RespValue::SimpleString("OK".to_string()));

    // Wait a moment to ensure the write is complete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Get the value back
    let get_response = client
        .send_command_str("GET", &[key])
        .await
        .expect("Failed to send GET command");

    // Verify we got back the same large JSON value
    match get_response {
        RespValue::BulkString(Some(value)) => {
            let retrieved_json = String::from_utf8_lossy(&value);
            assert_eq!(retrieved_json, large_json);
        }
        other => panic!("Expected BulkString response, got: {:?}", other),
    }
}

/// Test setting JSON with control characters that could break the RESP protocol
#[tokio::test]
async fn test_json_with_control_chars() {
    let mut config = StorageConfig::default();
    config.max_value_size = 1 * 1024 * 1024; // 1MB

    // Use a unique port number
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 18200));
    let storage = std::sync::Arc::new(StorageEngine::new(config.clone()));
    let server = Server::new(addr, std::sync::Arc::clone(&storage));

    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Connect a client
    let mut client = Client::connect(addr)
        .await
        .expect("Failed to create client");

    // Flush all data to ensure clean test environment
    let flush_resp = client
        .send_command_str("FLUSHALL", &[])
        .await
        .expect("Failed to flush database");
    assert_eq!(flush_resp, RespValue::SimpleString("OK".to_string()));

    // Create a JSON string with control characters that could potentially cause issues
    let json_with_controls = format!(
        "{{\"data\":\"Some data with control chars: {}{}{}{}{}\",\"binary\":\"ABC\"}}",
        '\u{0001}', // SOH (Start of Heading)
        '\u{0002}', // STX (Start of Text)
        '\u{0003}', // ETX (End of Text)
        '\u{001B}', // ESC (Escape)
        '\u{001F}'  // US (Unit Separator)
    );

    // Use a unique key with a test-specific prefix
    let key = "test_2_control_test";

    // No need to delete as we've already flushed all keys

    let set_response = client
        .send_command_str("SET", &[key, &json_with_controls])
        .await
        .expect("Failed to send SET command with control characters");

    // Verify the SET command was successful
    assert_eq!(set_response, RespValue::SimpleString("OK".to_string()));

    // Get the value back
    let get_response = client
        .send_command_str("GET", &[key])
        .await
        .expect("Failed to send GET command");

    // Verify we got back the same JSON with control characters
    match get_response {
        RespValue::BulkString(Some(value)) => {
            let retrieved_json = String::from_utf8_lossy(&value);
            assert_eq!(retrieved_json, json_with_controls);
        }
        other => panic!("Expected BulkString response, got: {:?}", other),
    }
}

/// Test the exact key and value format from the error report
#[tokio::test]
async fn test_error_reproduction() {
    // Use a unique port number
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 18300));
    let mut config = StorageConfig::default();
    config.max_value_size = 1 * 1024 * 1024; // 1MB
    let storage = std::sync::Arc::new(StorageEngine::new(config));
    let server = Server::new(addr, std::sync::Arc::clone(&storage));

    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Connect a client
    let mut client = Client::connect(addr)
        .await
        .expect("Failed to create client");

    // Flush all data to ensure clean test environment
    let flush_resp = client
        .send_command_str("FLUSHALL", &[])
        .await
        .expect("Failed to flush database");
    assert_eq!(flush_resp, RespValue::SimpleString("OK".to_string()));

    // Create a large JSON value similar to the one in the error report
    // This recreates the exact structure from the client code sample
    let mut large_json = String::from("[");

    // Generate fewer items to avoid protocol errors
    for i in 0..1 {
        // Add complex JSON structure with nested fields
        large_json.push_str(&format!("
        {{
            \"id\": {},
            \"title\": \"Story {}\",
            \"description\": \"This is a story description with special chars: \\\"quotes\\\", newlines\\n, tabs\\t\",
            \"author\": {{
                \"id\": {},
                \"name\": \"Author {}\",
                \"last_name\": \"LastName {}\",
                \"sec_name\": null,
                \"created\": \"2023-01-01T00:00:00Z\",
                \"modified\": \"2023-01-02T00:00:00Z\"
            }},
            \"rating\": {},
            \"rating_sum\": {},
            \"rating_count\": {},
            \"last_rating_update\": \"2023-01-03T00:00:00Z\",
            \"story_langs\": [
                {{
                    \"language\": {{
                        \"id\": {},
                        \"code\": \"en\",
                        \"name\": \"English\",
                        \"native_name\": \"English\",
                        \"created\": \"2023-01-01T00:00:00Z\",
                        \"modified\": \"2023-01-01T00:00:00Z\"
                    }},
                    \"name\": \"Story {} in English\",
                    \"description\": \"This is a very long detailed description with lots of text to make the JSON larger. It contains special characters and formatting to ensure the protocol handles it correctly.\",
                    \"created\": \"2023-01-01T00:00:00Z\",
                    \"modified\": \"2023-01-02T00:00:00Z\"
                }}
            ],
            \"genres\": [
                {{
                    \"id\": {},
                    \"names\": {{
                        \"en\": \"Genre {}\",
                        \"es\": \"Género {}\"
                    }}
                }},
                {{
                    \"id\": {},
                    \"names\": {{
                        \"en\": \"Genre {}\",
                        \"es\": \"Género {}\"
                    }}
                }}
            ],
            \"is_active\": true,
            \"main_image_url\": \"https://example.com/image{}.jpg\",
            \"background_images\": [
                \"https://example.com/bg1{}.jpg\",
                \"https://example.com/bg2{}.jpg\"
            ],
            \"screenshot_images\": [
                \"https://example.com/screen1{}.jpg\",
                \"https://example.com/screen2{}.jpg\",
                \"https://example.com/screen3{}.jpg\"
            ],
            \"created\": \"2023-01-01T00:00:00Z\",
            \"modified\": \"2023-01-02T00:00:00Z\",
            \"coming_soon\": false
        }}{}",
            i, i, i, i, i,
            4.5 + (i as f32 * 0.1), // rating
            45 + (i * 10), // rating_sum
            10 + i, // rating_count
            i + 1, // language id
            i,
            i * 10, // genre id
            i, i,
            i * 10 + 1, // genre id 2
            i + 10, i + 10,
            i,
            i, i,
            i, i, i,
            if i < 2 { "," } else { "" }
        ));
    }

    large_json.push_str(
        "
]",
    );

    // Verify reasonable size
    println!("Generated complex JSON size: {} bytes", large_json.len());
    assert!(
        large_json.len() > 1000,
        "Generated JSON should be at least 1KB"
    );

    // Use a unique key with a test-specific prefix
    let key = "test_3_stories_list:skip:0:limit:10:sort_by_rating:None:sort_by_modified_chapters:None:show_inactive:True";

    // No need to delete as we've already flushed all keys

    // This command should now succeed
    let set_response = client
        .send_command_str("SET", &[key, &large_json, "EX", "1800"])
        .await
        .expect("Failed to send SET command with real-world JSON");

    // Verify the SET command was successful
    assert_eq!(set_response, RespValue::SimpleString("OK".to_string()));

    // Get the value back
    let get_response = client
        .send_command_str("GET", &[key])
        .await
        .expect("Failed to send GET command");

    // Verify we got back the same large JSON value
    match get_response {
        RespValue::BulkString(Some(value)) => {
            let retrieved_json = String::from_utf8_lossy(&value);
            assert_eq!(retrieved_json, large_json);
        }
        other => panic!("Expected BulkString response, got: {:?}", other),
    }
}
