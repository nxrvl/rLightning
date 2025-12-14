use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageEngine, StorageConfig};

// Utility function to send a Python-style datetime JSON and test the serialization
async fn test_datetime_serialization() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Start the server programmatically
    let addr: SocketAddr = "127.0.0.1:17399".parse()?;
    let storage = Arc::new(StorageEngine::new(StorageConfig::default()));

    let server = Server::new(addr, Arc::clone(&storage))
        .with_connection_limit(100)
        .with_buffer_size(1024 * 1024);

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    // Give server time to start
    sleep(Duration::from_millis(200)).await;

    // Connect to the server
    let mut client = Client::connect(addr).await?;
    
    println!("Connected to server successfully");

    // Test with a JSON object containing a datetime in Python string representation
    let json_with_datetime = r#"{"name":"John","created_at":"2023-04-01 12:34:56.789"}"#;

    // Set JSON with datetime
    let set_result = client.send_command_str("JSON.SET", &["user:1", ".", json_with_datetime]).await?;
    assert_eq!(set_result, RespValue::SimpleString("OK".to_string()));

    // Get the JSON back
    let get_result = client.send_command_str("JSON.GET", &["user:1"]).await?;
    let get_result_str = match get_result {
        RespValue::BulkString(Some(bytes)) => String::from_utf8(bytes)?,
        _ => panic!("Expected bulk string response"),
    };

    println!("Server returned: {}", get_result_str);

    // Verify the datetime was properly handled (converted to ISO-8601)
    assert!(get_result_str.contains("2023-04-01T12:34:56.789"));
    
    // Test with an array containing a datetime
    let json_with_datetime_array = r#"["2023-04-01 12:34:56.789", "normal string"]"#;

    // Set JSON array with datetime
    let set_result = client.send_command_str("JSON.SET", &["dates", ".", json_with_datetime_array]).await?;
    assert_eq!(set_result, RespValue::SimpleString("OK".to_string()));

    // Get the JSON array back
    let get_result = client.send_command_str("JSON.GET", &["dates"]).await?;
    let get_result_str = match get_result {
        RespValue::BulkString(Some(bytes)) => String::from_utf8(bytes)?,
        _ => panic!("Expected bulk string response"),
    };

    // Verify the datetime in the array was properly handled
    println!("Server returned array: {}", get_result_str);
    assert!(get_result_str.contains("2023-04-01T12:34:56.789"));
    assert!(get_result_str.contains("normal string"));
    
    // Test nested objects with datetimes
    let nested_json = r#"{"user":{"name":"Alice","joined":"2023-04-01 12:34:56.789"},"logs":[{"timestamp":"2023-04-02 10:20:30.456","action":"login"}]}"#;

    // Set nested JSON with datetimes
    let set_result = client.send_command_str("JSON.SET", &["complex:1", ".", nested_json]).await?;
    assert_eq!(set_result, RespValue::SimpleString("OK".to_string()));

    // Get the nested JSON back
    let get_result = client.send_command_str("JSON.GET", &["complex:1"]).await?;
    let get_result_str = match get_result {
        RespValue::BulkString(Some(bytes)) => String::from_utf8(bytes)?,
        _ => panic!("Expected bulk string response"),
    };

    // Verify the datetimes in the nested structure were properly handled
    println!("Server returned nested: {}", get_result_str);
    assert!(get_result_str.contains("2023-04-01T12:34:56.789"));
    assert!(get_result_str.contains("2023-04-02T10:20:30.456"));
    
    // Test setting a path with a datetime value
    let set_path_result = client.send_command_str("JSON.SET", &["complex:1", "user.last_login", r#""2023-04-03 15:45:12.123""#]).await?;
    assert_eq!(set_path_result, RespValue::SimpleString("OK".to_string()));

    // Get the updated nested JSON back
    let get_result = client.send_command_str("JSON.GET", &["complex:1"]).await?;
    let get_result_str = match get_result {
        RespValue::BulkString(Some(bytes)) => String::from_utf8(bytes)?,
        _ => panic!("Expected bulk string response"),
    };

    // Verify the new datetime was properly added and formatted
    println!("Server returned with new date: {}", get_result_str);
    assert!(get_result_str.contains("2023-04-03T15:45:12.123"));
    
    // Test different datetime formats
    let different_formats_json = r#"{
        "iso8601": "2023-04-04T16:30:45.678",
        "space_separated": "2023-04-04 16:30:45.678",
        "slashes": "2023/04/04 16:30:45",
        "with_at": "2023-04-04 at 16:30:45"
    }"#;

    // Set JSON with different datetime formats
    let set_result = client.send_command_str("JSON.SET", &["formats", ".", different_formats_json]).await?;
    assert_eq!(set_result, RespValue::SimpleString("OK".to_string()));

    // Get the JSON back
    let get_result = client.send_command_str("JSON.GET", &["formats"]).await?;
    let get_result_str = match get_result {
        RespValue::BulkString(Some(bytes)) => String::from_utf8(bytes)?,
        _ => panic!("Expected bulk string response"),
    };

    // Verify all datetime formats were properly converted to ISO-8601
    println!("Server returned formats: {}", get_result_str);
    assert!(get_result_str.contains("2023-04-04T16:30:45.678"));
    assert!(!get_result_str.contains("2023-04-04 16:30:45.678"));
    assert!(!get_result_str.contains("2023/04/04 16:30:45"));
    assert!(!get_result_str.contains("2023-04-04 at 16:30:45"));
    
    Ok(())
}

#[tokio::test]
async fn test_json_datetime_handling() {
    match test_datetime_serialization().await {
        Ok(_) => println!("Datetime serialization test passed!"),
        Err(e) => panic!("Test failed: {}", e),
    }
} 