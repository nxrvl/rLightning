use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
/// Simple test for 50KB value to isolate the issue
use rlightning::storage::engine::{StorageConfig, StorageEngine};
use std::sync::Arc;

#[tokio::test]
async fn test_50kb_value_simple() {
    let mut config = StorageConfig::default();
    config.max_value_size = 10 * 1024 * 1024; // 10MB
    config.max_memory = 100 * 1024 * 1024;

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 19010));
    let storage = Arc::new(StorageEngine::new(config));
    let server = Server::new(addr, Arc::clone(&storage)).with_buffer_size(1024 * 1024); // 1MB buffer

    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = Client::connect(addr)
        .await
        .expect("Failed to connect to server");

    // Test 50KB value
    let value = vec![b'x'; 50000];
    let value_str = String::from_utf8_lossy(&value);

    println!("Sending 50KB SET command");
    let result = client
        .send_command_str("SET", &["testkey", &value_str])
        .await;

    println!("Result: {:?}", result);

    match result {
        Ok(RespValue::SimpleString(s)) if s == "OK" => {
            println!("✓ 50KB SET succeeded");

            // Try to GET it back
            let get_result = client.send_command_str("GET", &["testkey"]).await;
            match get_result {
                Ok(RespValue::BulkString(Some(data))) => {
                    assert_eq!(data.len(), 50000);
                    println!("✓ 50KB GET succeeded");
                }
                Ok(other) => panic!("Unexpected GET response: {:?}", other),
                Err(e) => panic!("GET failed: {}", e),
            }
        }
        Ok(other) => {
            panic!("SET returned unexpected response: {:?}", other);
        }
        Err(e) => {
            panic!("SET failed: {}", e);
        }
    }
}
