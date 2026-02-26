use rlightning::command::Command;
use rlightning::command::handler::CommandHandler;
use rlightning::networking::resp::RespValue;
use rlightning::storage::engine::StorageEngine as Storage;
use std::sync::Arc;

/// Test that data corruption issue is fixed where stored data was interpreted as commands
#[tokio::test]
async fn test_no_data_corruption_with_json_storage() {
    // Create storage and command handler
    let storage = Arc::new(Storage::new(Default::default()));
    let handler = CommandHandler::new(Arc::clone(&storage));

    // Test case 1: Store JSON-like data
    let json_data = r#"{"id": 1, "data": "test"}"#;
    let set_cmd = Command {
        name: "SET".to_string(),
        args: vec![b"json_key".to_vec(), json_data.as_bytes().to_vec()],
    };

    let response = handler.process(set_cmd, 0).await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap(), RespValue::SimpleString("OK".to_string()));

    // Retrieve the JSON data
    let get_cmd = Command {
        name: "GET".to_string(),
        args: vec![b"json_key".to_vec()],
    };

    let response = handler.process(get_cmd, 0).await;
    assert!(response.is_ok());
    assert_eq!(
        response.unwrap(),
        RespValue::BulkString(Some(json_data.as_bytes().to_vec()))
    );
}

#[tokio::test]
async fn test_no_data_corruption_with_base64_data() {
    // Create storage and command handler
    let storage = Arc::new(Storage::new(Default::default()));
    let handler = CommandHandler::new(Arc::clone(&storage));

    // Test case 2: Store Base64 data that was causing issues
    let b64_data = "B64JSON:W3siaWQiOiAxLCAibmFtZSI6ICJ0ZXN0In1d";
    let set_cmd = Command {
        name: "SET".to_string(),
        args: vec![b"b64_key".to_vec(), b64_data.as_bytes().to_vec()],
    };

    let response = handler.process(set_cmd, 0).await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap(), RespValue::SimpleString("OK".to_string()));

    // Retrieve the B64 data
    let get_cmd = Command {
        name: "GET".to_string(),
        args: vec![b"b64_key".to_vec()],
    };

    let response = handler.process(get_cmd, 0).await;
    assert!(response.is_ok());
    assert_eq!(
        response.unwrap(),
        RespValue::BulkString(Some(b64_data.as_bytes().to_vec()))
    );
}

#[tokio::test]
async fn test_large_json_payload_handling() {
    // Create storage and command handler
    let storage = Arc::new(Storage::new(Default::default()));
    let handler = CommandHandler::new(Arc::clone(&storage));

    // Create a large JSON payload similar to what the Python client sends
    let large_json = serde_json::json!({
        "id": 1,
        "author_id": 100,
        "background_images": vec!["img1.jpg", "img2.jpg"],
        "screenshot_images": vec!["screen1.jpg", "screen2.jpg"],
        "created": "2024-01-01T00:00:00Z",
        "modified": "2024-01-02T00:00:00Z",
        "main_image_url": "main.jpg",
        "is_active": true,
        "coming_soon": false,
        "rating_sum": 45,
        "rating_count": 10,
        "rating": 4.5,
        "last_rating_update": "2024-01-02T00:00:00Z",
        "data": vec!["item"; 100], // Simulate large nested data
    });

    let json_str = serde_json::to_string(&large_json).unwrap();

    // Store the large JSON
    let set_cmd = Command {
        name: "SET".to_string(),
        args: vec![b"large_json".to_vec(), json_str.as_bytes().to_vec()],
    };

    let response = handler.process(set_cmd, 0).await;
    assert!(response.is_ok());
    assert_eq!(response.unwrap(), RespValue::SimpleString("OK".to_string()));

    // Retrieve and verify
    let get_cmd = Command {
        name: "GET".to_string(),
        args: vec![b"large_json".to_vec()],
    };

    let response = handler.process(get_cmd, 0).await;
    assert!(response.is_ok());

    if let RespValue::BulkString(Some(data)) = response.unwrap() {
        assert_eq!(data, json_str.as_bytes());

        // Verify we can parse it back as JSON
        let parsed: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["rating"], 4.5);
    } else {
        panic!("Expected bulk string response");
    }
}
