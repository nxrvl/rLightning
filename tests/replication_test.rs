use std::time::Duration;
use tokio::time;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::BytesMut;

use rlightning::networking::resp::RespValue;
use rlightning::storage::engine::{StorageEngine, StorageConfig};
use rlightning::replication::{ReplicationManager, ReplicationRole, MasterLinkStatus};
use rlightning::replication::config::ReplicationConfig;

// Helper function to send a command to a Redis server
async fn send_command(stream: &mut TcpStream, command: &str, args: &[&str]) -> Result<RespValue, Box<dyn std::error::Error>> {
    // Create a RESP array for the command
    let mut cmd_parts = Vec::with_capacity(1 + args.len());
    cmd_parts.push(RespValue::BulkString(Some(command.as_bytes().to_vec())));
    
    for arg in args {
        cmd_parts.push(RespValue::BulkString(Some(arg.as_bytes().to_vec())));
    }
    
    let cmd_array = RespValue::Array(Some(cmd_parts));
    
    // Serialize and send the command
    let bytes = cmd_array.serialize()?;
    stream.write_all(&bytes).await?;
    
    // Read the response
    let mut buffer = BytesMut::with_capacity(4096);
    
    loop {
        stream.read_buf(&mut buffer).await?;
        
        match RespValue::parse(&mut buffer) {
            Ok(Some(value)) => {
                return Ok(value);
            }
            Ok(None) => {
                // Incomplete message, continue reading
                continue;
            }
            Err(e) => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Protocol error: {}", e)
                )));
            }
        }
    }
}

#[tokio::test]
async fn test_replication_basic() {
    // This test requires running two instances of the server
    // For simplicity, we'll just test the ReplicationManager directly
    
    // Create a storage engine for the master
    let storage_config = StorageConfig::default();
    let master_engine = StorageEngine::new(storage_config.clone());
    
    // Create a replication config for the master
    let master_replication_config = ReplicationConfig {
        master_host: None,
        master_port: None,
        master_password: None,
        accept_replicas: true,
        replication_timeout: Duration::from_secs(60),
        replication_backlog_size: 1024 * 1024,
        min_replicas_to_write: 0,
        min_replicas_max_lag: Duration::from_secs(10),
    };
    
    // Create a replication manager for the master
    let master_replication = ReplicationManager::new(master_engine.clone(), master_replication_config);
    
    // Initialize the master replication
    master_replication.init().await.expect("Failed to initialize master replication");
    
    // Check that the master is in the correct state
    let master_state = master_replication.get_state().await;
    assert_eq!(master_state.role, ReplicationRole::Master);
    assert_eq!(master_state.master_host, None);
    assert_eq!(master_state.master_port, None);
    assert_eq!(master_state.master_link_status, MasterLinkStatus::Down);
    assert_eq!(master_state.replication_offset, 0);
    assert!(master_state.connected_replicas.is_empty());
    
    // Create a storage engine for the replica
    let replica_engine = StorageEngine::new(storage_config);
    
    // Create a replication config for the replica
    let replica_replication_config = ReplicationConfig {
        master_host: Some("localhost".to_string()),
        master_port: Some(6379),
        master_password: None,
        accept_replicas: false,
        replication_timeout: Duration::from_secs(60),
        replication_backlog_size: 1024 * 1024,
        min_replicas_to_write: 0,
        min_replicas_max_lag: Duration::from_secs(10),
    };
    
    // Create a replication manager for the replica
    let replica_replication = ReplicationManager::new(replica_engine.clone(), replica_replication_config);
    
    // Since this is a test and we don't need to actually connect to a master,
    // we'll just check that the state is initialized correctly.
    // In a real scenario, we would use connect_to_master and handle the connection
    
    // For testing purposes, we should check the initial state
    let replica_state = replica_replication.get_state().await;
    // Initially the ReplicationManager will be created as Master
    // In a real system, connect_to_master would change the role to Replica
    assert_eq!(replica_state.role, ReplicationRole::Master);
    // Even though config has master_host and master_port, they aren't set in state until connect_to_master is called
    assert_eq!(replica_state.master_host, None);
    assert_eq!(replica_state.master_port, None);
    // Initially the link status is down since we haven't actually connected
    assert_eq!(replica_state.master_link_status, MasterLinkStatus::Down);
    
    // Since disconnect_from_master changes the state to Master role and clears host/port info
    // First, we need to connect to establish a connection state
    replica_replication.connect_to_master("localhost".to_string(), 6379).await
        .expect("Failed to connect to master");
    
    // Now disconnect from the master
    replica_replication.disconnect_from_master().await.expect("Failed to disconnect from master");
    
    // Check that the instance has proper master state after disconnect
    let new_state = replica_replication.get_state().await;
    assert_eq!(new_state.role, ReplicationRole::Master);
    assert_eq!(new_state.master_host, None);
    assert_eq!(new_state.master_port, None);
    assert_eq!(new_state.master_link_status, MasterLinkStatus::Down);
}

// This test requires running actual server instances
// It's commented out because it would fail in a CI environment
// #[tokio::test]
async fn _test_replication_with_real_servers() -> Result<(), Box<dyn std::error::Error>> {
    // Start a master server on port 6379
    // In a real test, you would start the server programmatically
    
    // Connect to the master
    let mut master_stream = TcpStream::connect("127.0.0.1:6379").await?;
    
    // Set a key on the master
    let response = send_command(&mut master_stream, "SET", &["test_key", "test_value"]).await?;
    assert!(matches!(response, RespValue::SimpleString(s) if s == "OK"));
    
    // Start a replica server on port 6380
    // In a real test, you would start the server programmatically with --master-host=127.0.0.1 --master-port=6379
    
    // Wait for replication to happen
    time::sleep(Duration::from_secs(1)).await;
    
    // Connect to the replica
    let mut replica_stream = TcpStream::connect("127.0.0.1:6380").await?;
    
    // Get the key from the replica
    let response = send_command(&mut replica_stream, "GET", &["test_key"]).await?;
    if let RespValue::BulkString(Some(data)) = &response {
        assert_eq!(String::from_utf8_lossy(data), "test_value");
    } else {
        panic!("Expected BulkString for GET response");
    }
    
    // Set another key on the master
    let response = send_command(&mut master_stream, "SET", &["another_key", "another_value"]).await?;
    assert!(matches!(response, RespValue::SimpleString(s) if s == "OK"));
    
    // Wait for replication to happen
    time::sleep(Duration::from_secs(1)).await;
    
    // Get the new key from the replica
    let response = send_command(&mut replica_stream, "GET", &["another_key"]).await?;
    if let RespValue::BulkString(Some(data)) = &response {
        assert_eq!(String::from_utf8_lossy(data), "another_value");
    } else {
        panic!("Expected BulkString for GET response");
    }
    
    // Check the role of the master
    let response = send_command(&mut master_stream, "ROLE", &[]).await?;
    if let RespValue::Array(Some(parts)) = response {
        if !parts.is_empty() {
            if let RespValue::BulkString(Some(role)) = &parts[0] {
                assert_eq!(String::from_utf8_lossy(role), "master");
            } else {
                panic!("Expected BulkString for role");
            }
        } else {
            panic!("Expected non-empty Array for ROLE response");
        }
    } else {
        panic!("Expected Array for ROLE response");
    }
    
    // Check the role of the replica
    let response = send_command(&mut replica_stream, "ROLE", &[]).await?;
    if let RespValue::Array(Some(parts)) = response {
        if !parts.is_empty() {
            if let RespValue::BulkString(Some(role)) = &parts[0] {
                assert_eq!(String::from_utf8_lossy(role), "slave");
            } else {
                panic!("Expected BulkString for role");
            }
        } else {
            panic!("Expected non-empty Array for ROLE response");
        }
    } else {
        panic!("Expected Array for ROLE response");
    }
    
    Ok(())
}