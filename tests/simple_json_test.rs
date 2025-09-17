use std::net::SocketAddr;
use std::time::Duration;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageEngine, StorageConfig, EvictionPolicy};

#[tokio::test]
async fn test_simple_json() {
    // Create a storage engine
    let config = StorageConfig {
        max_memory: 1024 * 1024 * 100,
        eviction_policy: EvictionPolicy::LRU,
        default_ttl: Duration::from_secs(0),
        max_key_size: 1024 * 1024,
        max_value_size: 1024 * 1024 * 10,
    };
    
    let storage = StorageEngine::new(config);
    let addr: SocketAddr = "127.0.0.1:18000".parse().unwrap();
    let server = Server::new(addr, storage);
    
    tokio::spawn(async move {
        server.start().await.unwrap();
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let mut client = Client::connect(addr).await.unwrap();
    
    // Test JSON.SET
    let response = client.send_command(vec![
        b"JSON.SET".to_vec(),
        b"testkey".to_vec(),
        b".".to_vec(),
        br#"{"test": "value"}"#.to_vec(),
    ]).await.unwrap();
    
    match response {
        RespValue::SimpleString(s) if s == "OK" => {
            println!("JSON.SET successful");
        },
        _ => {
            panic!("JSON.SET failed: {:?}", response);
        }
    }
}