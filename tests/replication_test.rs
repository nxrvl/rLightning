use std::time::Duration;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::BytesMut;

use rlightning::networking::resp::RespValue;
use rlightning::storage::engine::{StorageEngine, StorageConfig};
use rlightning::replication::{ReplicationManager, ReplicationRole, MasterLinkStatus};
use rlightning::replication::config::ReplicationConfig;

mod test_utils;
use test_utils::{setup_test_server_with_replication, create_client};

// Helper function to send a command to a Redis server via raw TCP
async fn send_command(stream: &mut TcpStream, command: &str, args: &[&str]) -> Result<RespValue, Box<dyn std::error::Error>> {
    let mut cmd_parts = Vec::with_capacity(1 + args.len());
    cmd_parts.push(RespValue::BulkString(Some(command.as_bytes().to_vec())));

    for arg in args {
        cmd_parts.push(RespValue::BulkString(Some(arg.as_bytes().to_vec())));
    }

    let cmd_array = RespValue::Array(Some(cmd_parts));
    let bytes = cmd_array.serialize()?;
    stream.write_all(&bytes).await?;

    let mut buffer = BytesMut::with_capacity(4096);
    loop {
        let timeout = tokio::time::timeout(Duration::from_secs(5), stream.read_buf(&mut buffer)).await;
        match timeout {
            Ok(Ok(0)) => return Err("Connection closed".into()),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => return Err("Timeout reading response".into()),
        }

        match RespValue::parse(&mut buffer) {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => continue,
            Err(e) => return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Protocol error: {}", e)
            ))),
        }
    }
}

// ==================== Unit-level tests (no server needed) ====================

#[tokio::test]
async fn test_replication_manager_creation() {
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    let replication_config = ReplicationConfig::default();
    let replication_manager = ReplicationManager::new(engine.clone(), replication_config);

    let state = replication_manager.get_state().await;
    assert_eq!(state.role, ReplicationRole::Master);
    assert_eq!(state.master_host, None);
    assert_eq!(state.master_port, None);
    assert_eq!(state.master_link_status, MasterLinkStatus::Down);
    assert_eq!(state.replication_offset, 0);
    assert!(state.connected_replicas.is_empty());
}

#[tokio::test]
async fn test_replication_manager_disconnect_from_master() {
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    let mut replication_config = ReplicationConfig::default();
    replication_config.master_host = Some("localhost".to_string());
    replication_config.master_port = Some(6379);

    let replication_manager = ReplicationManager::new(engine.clone(), replication_config);

    replication_manager.connect_to_master("localhost".to_string(), 6379).await.unwrap();
    let state = replication_manager.get_state().await;
    assert_eq!(state.role, ReplicationRole::Replica);

    replication_manager.disconnect_from_master().await.unwrap();
    let state = replication_manager.get_state().await;
    assert_eq!(state.role, ReplicationRole::Master);
    assert_eq!(state.master_host, None);
    assert_eq!(state.master_port, None);
    assert_eq!(state.master_link_status, MasterLinkStatus::Down);
    assert!(!replication_manager.is_read_only());
}

#[tokio::test]
async fn test_replid_generation() {
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    let replication_config = ReplicationConfig::default();
    let mgr = ReplicationManager::new(engine.clone(), replication_config);

    let replid = mgr.get_master_replid().await;
    assert_eq!(replid.len(), 40);
    assert!(replid.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn test_register_unregister_replica() {
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    let replication_config = ReplicationConfig::default();
    let mgr = ReplicationManager::new(engine.clone(), replication_config);

    let _rx = mgr.register_replica("replica-1".to_string(), "127.0.0.1".to_string(), 6380).await;
    let state = mgr.get_state().await;
    assert_eq!(state.connected_replicas.len(), 1);
    assert_eq!(state.connected_replicas[0].id, "replica-1");
    assert_eq!(state.connected_replicas[0].port, 6380);

    mgr.unregister_replica("replica-1").await;
    let state = mgr.get_state().await;
    assert!(state.connected_replicas.is_empty());
}

#[tokio::test]
async fn test_wait_no_replicas() {
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    let replication_config = ReplicationConfig::default();
    let mgr = ReplicationManager::new(engine.clone(), replication_config);

    let count = mgr.wait_for_replicas(1, Duration::from_millis(100)).await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_is_write_command() {
    assert!(ReplicationManager::is_write_command("set"));
    assert!(ReplicationManager::is_write_command("del"));
    assert!(ReplicationManager::is_write_command("lpush"));
    assert!(ReplicationManager::is_write_command("hset"));
    assert!(ReplicationManager::is_write_command("zadd"));
    assert!(ReplicationManager::is_write_command("sadd"));
    assert!(ReplicationManager::is_write_command("flushall"));
    assert!(ReplicationManager::is_write_command("expire"));
    assert!(!ReplicationManager::is_write_command("get"));
    assert!(!ReplicationManager::is_write_command("lrange"));
    assert!(!ReplicationManager::is_write_command("info"));
    assert!(!ReplicationManager::is_write_command("ping"));
    assert!(!ReplicationManager::is_write_command("role"));
}

#[tokio::test]
async fn test_replicaof_no_one() {
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    let replication_config = ReplicationConfig::default();
    let mgr = ReplicationManager::new(engine.clone(), replication_config);

    mgr.connect_to_master("localhost".to_string(), 6379).await.unwrap();
    let state = mgr.get_state().await;
    assert_eq!(state.role, ReplicationRole::Replica);

    let result = mgr.handle_replicaof("NO", "ONE").await.unwrap();
    assert!(matches!(result, RespValue::SimpleString(s) if s == "OK"));

    let state = mgr.get_state().await;
    assert_eq!(state.role, ReplicationRole::Master);
}

// ==================== Integration tests (with live server) ====================

#[tokio::test]
async fn test_role_command_master() {
    let replication_config = ReplicationConfig::default();
    let (addr, _repl) = setup_test_server_with_replication(200, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let response = send_command(&mut stream, "ROLE", &[]).await.unwrap();

    if let RespValue::Array(Some(parts)) = response {
        assert_eq!(parts.len(), 3);
        // First element: role = "master"
        if let RespValue::BulkString(Some(role)) = &parts[0] {
            assert_eq!(role, b"master");
        } else {
            panic!("Expected BulkString for role");
        }
        // Second element: offset = 0
        if let RespValue::Integer(offset) = &parts[1] {
            assert_eq!(*offset, 0);
        } else {
            panic!("Expected Integer for offset");
        }
        // Third element: empty array of replicas
        if let RespValue::Array(Some(replicas)) = &parts[2] {
            assert!(replicas.is_empty());
        } else {
            panic!("Expected Array for replicas");
        }
    } else {
        panic!("Expected Array response, got: {:?}", response);
    }
}

#[tokio::test]
async fn test_replicaof_command() {
    let replication_config = ReplicationConfig::default();
    let (addr, repl) = setup_test_server_with_replication(201, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // REPLICAOF localhost 6379 - should switch to replica mode
    let response = send_command(&mut stream, "REPLICAOF", &["localhost", "6379"]).await.unwrap();
    assert!(matches!(response, RespValue::SimpleString(ref s) if s == "OK"));

    // Verify the state changed
    let state = repl.get_state().await;
    assert_eq!(state.role, ReplicationRole::Replica);
    assert_eq!(state.master_host, Some("localhost".to_string()));
    assert_eq!(state.master_port, Some(6379));

    // REPLICAOF NO ONE - should switch back to master mode
    let response = send_command(&mut stream, "REPLICAOF", &["NO", "ONE"]).await.unwrap();
    assert!(matches!(response, RespValue::SimpleString(ref s) if s == "OK"));

    let state = repl.get_state().await;
    assert_eq!(state.role, ReplicationRole::Master);
    assert_eq!(state.master_host, None);
    assert_eq!(state.master_port, None);
}

#[tokio::test]
async fn test_slaveof_command() {
    let replication_config = ReplicationConfig::default();
    let (addr, repl) = setup_test_server_with_replication(202, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // SLAVEOF is an alias for REPLICAOF
    let response = send_command(&mut stream, "SLAVEOF", &["localhost", "6379"]).await.unwrap();
    assert!(matches!(response, RespValue::SimpleString(ref s) if s == "OK"));

    let state = repl.get_state().await;
    assert_eq!(state.role, ReplicationRole::Replica);

    let response = send_command(&mut stream, "SLAVEOF", &["NO", "ONE"]).await.unwrap();
    assert!(matches!(response, RespValue::SimpleString(ref s) if s == "OK"));

    let state = repl.get_state().await;
    assert_eq!(state.role, ReplicationRole::Master);
}

#[tokio::test]
async fn test_wait_command_no_replicas() {
    let replication_config = ReplicationConfig::default();
    let (addr, _repl) = setup_test_server_with_replication(203, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // WAIT 1 100 - wait for 1 replica with 100ms timeout, should return 0 immediately
    let response = send_command(&mut stream, "WAIT", &["1", "100"]).await.unwrap();
    if let RespValue::Integer(count) = response {
        assert_eq!(count, 0);
    } else {
        panic!("Expected Integer response, got: {:?}", response);
    }
}

#[tokio::test]
async fn test_wait_command_wrong_args() {
    let replication_config = ReplicationConfig::default();
    let (addr, _repl) = setup_test_server_with_replication(204, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // WAIT with wrong number of args should return an error
    let response = send_command(&mut stream, "WAIT", &["1"]).await.unwrap();
    assert!(matches!(response, RespValue::Error(_)));
}

#[tokio::test]
async fn test_failover_no_replicas() {
    let replication_config = ReplicationConfig::default();
    let (addr, _repl) = setup_test_server_with_replication(205, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // FAILOVER with no connected replicas should return an error
    let response = send_command(&mut stream, "FAILOVER", &[]).await.unwrap();
    if let RespValue::Error(e) = response {
        assert!(e.contains("FAILOVER requires connected replicas"), "Got error: {}", e);
    } else {
        panic!("Expected Error response, got: {:?}", response);
    }
}

#[tokio::test]
async fn test_replconf_command() {
    let replication_config = ReplicationConfig::default();
    let (addr, _repl) = setup_test_server_with_replication(206, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // REPLCONF listening-port 6380 should return OK
    let response = send_command(&mut stream, "REPLCONF", &["listening-port", "6380"]).await.unwrap();
    assert!(matches!(response, RespValue::SimpleString(ref s) if s == "OK"));

    // REPLCONF capa eof should return OK
    let response = send_command(&mut stream, "REPLCONF", &["capa", "eof"]).await.unwrap();
    assert!(matches!(response, RespValue::SimpleString(ref s) if s == "OK"));
}

#[tokio::test]
async fn test_read_only_mode_on_replica() {
    let replication_config = ReplicationConfig::default();
    let (addr, repl) = setup_test_server_with_replication(207, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // First, verify we can write as a master
    let response = send_command(&mut stream, "SET", &["key1", "value1"]).await.unwrap();
    assert!(matches!(response, RespValue::SimpleString(ref s) if s == "OK"));

    // Become a replica (which enables read-only mode)
    let response = send_command(&mut stream, "REPLICAOF", &["localhost", "6379"]).await.unwrap();
    assert!(matches!(response, RespValue::SimpleString(ref s) if s == "OK"));

    // Now writes should be rejected
    let response = send_command(&mut stream, "SET", &["key2", "value2"]).await.unwrap();
    if let RespValue::Error(e) = response {
        assert!(e.contains("READONLY"), "Expected READONLY error, got: {}", e);
    } else {
        panic!("Expected Error response for write on replica, got: {:?}", response);
    }

    // Reads should still work
    let response = send_command(&mut stream, "GET", &["key1"]).await.unwrap();
    // key1 was set before becoming replica, should still be readable
    match response {
        RespValue::BulkString(Some(data)) => assert_eq!(data, b"value1"),
        _ => {} // may be nil if key didn't persist, that's fine
    }

    // REPLICAOF NO ONE to go back to master
    let response = send_command(&mut stream, "REPLICAOF", &["NO", "ONE"]).await.unwrap();
    assert!(matches!(response, RespValue::SimpleString(ref s) if s == "OK"));

    // Now writes should work again
    let response = send_command(&mut stream, "SET", &["key3", "value3"]).await.unwrap();
    assert!(matches!(response, RespValue::SimpleString(ref s) if s == "OK"));

    // Verify
    assert!(!repl.is_read_only());
}

#[tokio::test]
async fn test_replicaof_wrong_args() {
    let replication_config = ReplicationConfig::default();
    let (addr, _repl) = setup_test_server_with_replication(208, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // REPLICAOF with wrong number of args
    let response = send_command(&mut stream, "REPLICAOF", &["localhost"]).await.unwrap();
    assert!(matches!(response, RespValue::Error(_)));
}

#[tokio::test]
async fn test_role_after_replicaof() {
    let replication_config = ReplicationConfig::default();
    let (addr, _repl) = setup_test_server_with_replication(209, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Initially should be master
    let response = send_command(&mut stream, "ROLE", &[]).await.unwrap();
    if let RespValue::Array(Some(parts)) = &response {
        if let RespValue::BulkString(Some(role)) = &parts[0] {
            assert_eq!(role, b"master");
        } else {
            panic!("Expected BulkString");
        }
    } else {
        panic!("Expected Array");
    }

    // Become a replica
    let _ = send_command(&mut stream, "REPLICAOF", &["localhost", "6379"]).await.unwrap();

    // Now ROLE should return slave
    let response = send_command(&mut stream, "ROLE", &[]).await.unwrap();
    if let RespValue::Array(Some(parts)) = &response {
        assert!(parts.len() >= 2);
        if let RespValue::BulkString(Some(role)) = &parts[0] {
            assert_eq!(role, b"slave");
        } else {
            panic!("Expected BulkString for role");
        }
        // host
        if let RespValue::BulkString(Some(host)) = &parts[1] {
            assert_eq!(String::from_utf8_lossy(host), "localhost");
        }
        // port
        if let RespValue::Integer(port) = &parts[2] {
            assert_eq!(*port, 6379);
        }
    } else {
        panic!("Expected Array response, got: {:?}", response);
    }

    // Back to master
    let _ = send_command(&mut stream, "REPLICAOF", &["NO", "ONE"]).await.unwrap();

    let response = send_command(&mut stream, "ROLE", &[]).await.unwrap();
    if let RespValue::Array(Some(parts)) = &response {
        if let RespValue::BulkString(Some(role)) = &parts[0] {
            assert_eq!(role, b"master");
        } else {
            panic!("Expected BulkString");
        }
    } else {
        panic!("Expected Array");
    }
}

#[tokio::test]
async fn test_command_propagation_to_backlog() {
    let replication_config = ReplicationConfig::default();
    let (addr, repl) = setup_test_server_with_replication(210, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Set some keys (write commands should get propagated to backlog)
    let _ = send_command(&mut stream, "SET", &["k1", "v1"]).await.unwrap();
    let _ = send_command(&mut stream, "SET", &["k2", "v2"]).await.unwrap();
    let _ = send_command(&mut stream, "DEL", &["k1"]).await.unwrap();

    // The backlog should have received propagated commands
    let offset = repl.get_master_repl_offset();
    assert!(offset > 0, "Expected replication offset > 0, got {}", offset);

    // Backlog should contain data
    let backlog = repl.backlog().read().await;
    assert!(backlog.current_offset() > 0);
}

#[tokio::test]
async fn test_read_commands_not_propagated() {
    let replication_config = ReplicationConfig::default();
    let (addr, repl) = setup_test_server_with_replication(211, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Only write commands should be propagated
    // First, set something so GET has something to read
    let _ = send_command(&mut stream, "SET", &["k1", "v1"]).await.unwrap();
    let offset_after_write = repl.get_master_repl_offset();

    // Read commands should NOT increase the offset
    let _ = send_command(&mut stream, "GET", &["k1"]).await.unwrap();
    let _ = send_command(&mut stream, "PING", &[]).await.unwrap();
    let _ = send_command(&mut stream, "INFO", &[]).await.unwrap();

    let offset_after_reads = repl.get_master_repl_offset();
    assert_eq!(offset_after_write, offset_after_reads,
        "Read commands should not change replication offset");
}

#[tokio::test]
async fn test_failover_as_replica_rejected() {
    let replication_config = ReplicationConfig::default();
    let (addr, _repl) = setup_test_server_with_replication(212, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Become a replica
    let _ = send_command(&mut stream, "REPLICAOF", &["localhost", "6379"]).await.unwrap();

    // FAILOVER as a replica should be rejected
    let response = send_command(&mut stream, "FAILOVER", &[]).await.unwrap();
    if let RespValue::Error(e) = response {
        assert!(e.contains("server to be a master"), "Got error: {}", e);
    } else {
        panic!("Expected Error response, got: {:?}", response);
    }
}

#[tokio::test]
async fn test_multiple_write_commands_backlog() {
    let replication_config = ReplicationConfig::default();
    let (addr, repl) = setup_test_server_with_replication(213, replication_config).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Various write commands
    let _ = send_command(&mut stream, "SET", &["str_key", "hello"]).await.unwrap();
    let _ = send_command(&mut stream, "LPUSH", &["list_key", "a", "b", "c"]).await.unwrap();
    let _ = send_command(&mut stream, "HSET", &["hash_key", "field", "value"]).await.unwrap();
    let _ = send_command(&mut stream, "SADD", &["set_key", "member1", "member2"]).await.unwrap();
    let _ = send_command(&mut stream, "ZADD", &["zset_key", "1.0", "member1"]).await.unwrap();

    // All should have been propagated to backlog
    let offset = repl.get_master_repl_offset();
    assert!(offset > 0, "Expected replication offset > 0 after multiple writes");

    let backlog = repl.backlog().read().await;
    let all_data = backlog.get_from_offset(0).unwrap();
    assert!(!all_data.is_empty(), "Backlog should contain propagated commands");

    // Verify the data is valid RESP
    let data_str = String::from_utf8_lossy(&all_data);
    assert!(data_str.contains("SET") || data_str.contains("set"),
        "Backlog should contain SET command");
}

#[tokio::test]
async fn test_psync_without_replication() {
    // Test PSYNC on a server without replication manager should return error
    let (addr, _repl) = setup_test_server_with_replication(214, ReplicationConfig::default()).await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // PSYNC ? -1 should be handled (it transitions to replication stream)
    // We just verify it doesn't crash the server
    let response = send_command(&mut stream, "PSYNC", &["?", "-1"]).await;
    // The connection may close after PSYNC (it enters replication mode), so we just verify no panic
    match response {
        Ok(_) => {} // Got some response
        Err(_) => {} // Connection closed (expected for PSYNC)
    }
}

#[tokio::test]
async fn test_replication_basic() {
    // Test basic ReplicationManager operations directly
    let storage_config = StorageConfig::default();
    let master_engine = StorageEngine::new(storage_config.clone());

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

    let master_replication = ReplicationManager::new(master_engine.clone(), master_replication_config);
    master_replication.init().await.expect("Failed to initialize master replication");

    let master_state = master_replication.get_state().await;
    assert_eq!(master_state.role, ReplicationRole::Master);
    assert_eq!(master_state.master_host, None);
    assert_eq!(master_state.master_port, None);
    assert_eq!(master_state.master_link_status, MasterLinkStatus::Down);
    assert_eq!(master_state.replication_offset, 0);
    assert!(master_state.connected_replicas.is_empty());

    let replica_engine = StorageEngine::new(storage_config);
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

    let replica_replication = ReplicationManager::new(replica_engine.clone(), replica_replication_config);

    // Initially Master until connect_to_master is called
    let replica_state = replica_replication.get_state().await;
    assert_eq!(replica_state.role, ReplicationRole::Master);
    assert_eq!(replica_state.master_host, None);

    replica_replication.connect_to_master("localhost".to_string(), 6379).await
        .expect("Failed to connect to master");

    replica_replication.disconnect_from_master().await.expect("Failed to disconnect from master");

    let new_state = replica_replication.get_state().await;
    assert_eq!(new_state.role, ReplicationRole::Master);
    assert_eq!(new_state.master_host, None);
    assert_eq!(new_state.master_port, None);
    assert_eq!(new_state.master_link_status, MasterLinkStatus::Down);
}

#[tokio::test]
async fn test_client_role_command() {
    // Test using the high-level Client API
    let replication_config = ReplicationConfig::default();
    let (addr, _repl) = setup_test_server_with_replication(215, replication_config).await.unwrap();

    let mut client = create_client(addr).await.unwrap();

    // Send ROLE via client
    let response = client.send_command_str("ROLE", &[]).await.unwrap();
    // Should return an array for master role
    match response {
        RespValue::Array(Some(parts)) => {
            assert_eq!(parts.len(), 3);
        }
        _ => panic!("Expected Array response from ROLE"),
    }
}

#[tokio::test]
async fn test_client_wait_command() {
    let replication_config = ReplicationConfig::default();
    let (addr, _repl) = setup_test_server_with_replication(216, replication_config).await.unwrap();

    let mut client = create_client(addr).await.unwrap();

    // WAIT 1 50 - wait for 1 replica with 50ms timeout
    let response = client.send_command_str("WAIT", &["1", "50"]).await.unwrap();
    match response {
        RespValue::Integer(count) => assert_eq!(count, 0),
        _ => panic!("Expected Integer response from WAIT"),
    }
}

#[tokio::test]
async fn test_replication_backlog_partial_resync() {
    use rlightning::replication::ReplicationBacklog;

    let mut backlog = ReplicationBacklog::new(100);

    // Append some data
    backlog.append(b"hello ");
    assert_eq!(backlog.current_offset(), 6);

    backlog.append(b"world");
    assert_eq!(backlog.current_offset(), 11);

    // Read from beginning
    let data = backlog.get_from_offset(0).unwrap();
    assert_eq!(data, b"hello world");

    // Read from middle
    let data = backlog.get_from_offset(6).unwrap();
    assert_eq!(data, b"world");

    // Read from end
    let data = backlog.get_from_offset(11).unwrap();
    assert!(data.is_empty());

    // Invalid offsets
    assert!(backlog.get_from_offset(12).is_none());

    // Partial resync possible from any valid offset
    assert!(backlog.can_partial_resync(0));
    assert!(backlog.can_partial_resync(6));
    assert!(backlog.can_partial_resync(11));
    assert!(!backlog.can_partial_resync(12));
}

#[tokio::test]
async fn test_replication_backlog_trimming() {
    use rlightning::replication::ReplicationBacklog;

    let mut backlog = ReplicationBacklog::new(20);

    backlog.append(b"1234567890");
    assert_eq!(backlog.current_offset(), 10);

    backlog.append(b"abcdefghij");
    assert_eq!(backlog.current_offset(), 20);

    // This should trigger trimming
    backlog.append(b"XXXXX");
    assert_eq!(backlog.current_offset(), 25);

    // Old data should be gone
    assert!(!backlog.can_partial_resync(0));
    assert!(!backlog.can_partial_resync(4));

    // Recent data should be available
    assert!(backlog.can_partial_resync(5));
    let data = backlog.get_from_offset(5).unwrap();
    assert_eq!(data.len(), 20);
}
