use std::process::Command;
use std::time::Duration;
use tokio::time;
use redis::{Client, Connection, RedisError};

// Utility function to start our Redis-compatible server for testing
async fn start_server() -> std::process::Child {
    // Build and run the server in a separate process
    let server = Command::new("cargo")
        .args(["run", "--bin", "rlightning", "--", "--port", "7379"])
        .spawn()
        .expect("Failed to start rLightning server");
    
    // Wait a bit for the server to start
    time::sleep(Duration::from_secs(3)).await;
    
    server
}

// Utility function to connect to our Redis-compatible server with retries
async fn connect_to_server_with_retry(max_retries: usize) -> Result<Connection, RedisError> {
    let client = Client::open("redis://127.0.0.1:7379")?;
    
    let mut retries = 0;
    while retries < max_retries {
        match client.get_connection() {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                println!("Connection attempt {} failed: {}", retries + 1, e);
                if retries == max_retries - 1 {
                    return Err(e);
                }
                retries += 1;
                time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    
    // This should never be reached due to the return in the error case above
    Err(RedisError::from(std::io::Error::new(
        std::io::ErrorKind::ConnectionRefused,
        "Failed to connect after max retries",
    )))
}

// Utility function to send a Python-style datetime JSON and test the serialization
async fn test_datetime_serialization() -> Result<(), Box<dyn std::error::Error>> {
    // Start the server
    let mut server = start_server().await;
    
    // Connect to the server with retries
    let mut conn = connect_to_server_with_retry(10).await?;
    
    println!("Connected to server successfully");
    
    // Test with a JSON object containing a datetime in Python string representation
    let json_with_datetime = r#"{"name":"John","created_at":"2023-04-01 12:34:56.789"}"#;
    
    // Set JSON with datetime
    let set_result: String = redis::cmd("JSON.SET")
        .arg("user:1")
        .arg(".")
        .arg(json_with_datetime)
        .query(&mut conn)?;
    
    assert_eq!(set_result, "OK");
    
    // Get the JSON back
    let get_result: String = redis::cmd("JSON.GET")
        .arg("user:1")
        .query(&mut conn)?;
    
    println!("Server returned: {}", get_result);
    
    // Verify the datetime was properly handled (converted to ISO-8601)
    assert!(get_result.contains("2023-04-01T12:34:56.789"));
    
    // Test with an array containing a datetime
    let json_with_datetime_array = r#"["2023-04-01 12:34:56.789", "normal string"]"#;
    
    // Set JSON array with datetime
    let set_result: String = redis::cmd("JSON.SET")
        .arg("dates")
        .arg(".")
        .arg(json_with_datetime_array)
        .query(&mut conn)?;
    
    assert_eq!(set_result, "OK");
    
    // Get the JSON array back
    let get_result: String = redis::cmd("JSON.GET")
        .arg("dates")
        .query(&mut conn)?;
    
    // Verify the datetime in the array was properly handled
    println!("Server returned array: {}", get_result);
    assert!(get_result.contains("2023-04-01T12:34:56.789"));
    assert!(get_result.contains("normal string"));
    
    // Test nested objects with datetimes
    let nested_json = r#"{"user":{"name":"Alice","joined":"2023-04-01 12:34:56.789"},"logs":[{"timestamp":"2023-04-02 10:20:30.456","action":"login"}]}"#;
    
    // Set nested JSON with datetimes
    let set_result: String = redis::cmd("JSON.SET")
        .arg("complex:1")
        .arg(".")
        .arg(nested_json)
        .query(&mut conn)?;
    
    assert_eq!(set_result, "OK");
    
    // Get the nested JSON back
    let get_result: String = redis::cmd("JSON.GET")
        .arg("complex:1")
        .query(&mut conn)?;
    
    // Verify the datetimes in the nested structure were properly handled
    println!("Server returned nested: {}", get_result);
    assert!(get_result.contains("2023-04-01T12:34:56.789"));
    assert!(get_result.contains("2023-04-02T10:20:30.456"));
    
    // Test setting a path with a datetime value
    let set_path_result: String = redis::cmd("JSON.SET")
        .arg("complex:1")
        .arg("user.last_login")
        .arg(r#""2023-04-03 15:45:12.123""#)
        .query(&mut conn)?;
    
    assert_eq!(set_path_result, "OK");
    
    // Get the updated nested JSON back
    let get_result: String = redis::cmd("JSON.GET")
        .arg("complex:1")
        .query(&mut conn)?;
    
    // Verify the new datetime was properly added and formatted
    println!("Server returned with new date: {}", get_result);
    assert!(get_result.contains("2023-04-03T15:45:12.123"));
    
    // Test different datetime formats
    let different_formats_json = r#"{
        "iso8601": "2023-04-04T16:30:45.678",
        "space_separated": "2023-04-04 16:30:45.678",
        "slashes": "2023/04/04 16:30:45",
        "with_at": "2023-04-04 at 16:30:45"
    }"#;
    
    // Set JSON with different datetime formats
    let set_result: String = redis::cmd("JSON.SET")
        .arg("formats")
        .arg(".")
        .arg(different_formats_json)
        .query(&mut conn)?;
    
    assert_eq!(set_result, "OK");
    
    // Get the JSON back
    let get_result: String = redis::cmd("JSON.GET")
        .arg("formats")
        .query(&mut conn)?;
    
    // Verify all datetime formats were properly converted to ISO-8601
    println!("Server returned formats: {}", get_result);
    assert!(get_result.contains("2023-04-04T16:30:45.678"));
    assert!(!get_result.contains("2023-04-04 16:30:45.678"));
    assert!(!get_result.contains("2023/04/04 16:30:45"));
    assert!(!get_result.contains("2023-04-04 at 16:30:45"));
    
    // Terminate the server
    match server.kill() {
        Ok(_) => println!("Server terminated successfully"),
        Err(e) => println!("Error killing server: {}", e),
    }
    
    Ok(())
}

#[tokio::test]
async fn test_json_datetime_handling() {
    match test_datetime_serialization().await {
        Ok(_) => println!("Datetime serialization test passed!"),
        Err(e) => panic!("Test failed: {}", e),
    }
} 