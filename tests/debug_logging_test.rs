use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};
use std::env;
use std::sync::Arc;

#[tokio::test]
async fn test_debug_logging() {
    // Set RUST_LOG to debug to test the logging
    unsafe {
        env::set_var("RUST_LOG", "debug");
    }

    // Initialize the tracing subscriber manually to ensure debug logging works in the test
    use tracing_subscriber::{EnvFilter, fmt};
    fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::new("debug"))
        .init();

    // Use a specific port for this test
    let port = 17689;
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    // Configure storage
    let config = StorageConfig::default();

    // Create storage engine and server
    let storage = Arc::new(StorageEngine::new(config));
    let server = Server::new_with_storage(addr, Arc::clone(&storage));

    // Start server in a background task
    let _server_handle = tokio::spawn(async move {
        let _ = server.start().await;
    });

    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Connect to the server
    let mut client = Client::connect(addr)
        .await
        .expect("Failed to connect to server");

    // Send a PING command - this should trigger request/response body logging
    let result = client
        .send_command_str("PING", &["Hello"])
        .await
        .expect("PING command should succeed");
    assert!(matches!(result, RespValue::BulkString(Some(_))));

    // Send a SET command
    let result = client
        .send_command_str("SET", &["mykey", "myvalue"])
        .await
        .expect("SET command should succeed");
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    // Send a GET command
    let result = client
        .send_command_str("GET", &["mykey"])
        .await
        .expect("GET command should succeed");
    match result {
        RespValue::BulkString(Some(value)) => {
            let str_value = String::from_utf8_lossy(&value);
            assert_eq!(str_value, "myvalue");
        }
        other => panic!("Expected BulkString, got: {:?}", other),
    }

    // The outputs from these commands should show up in the test logs with the debug data
    // This test is mainly to verify that our logging changes work as expected
}
