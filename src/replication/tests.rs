use std::time::Duration;

use crate::replication::config::ReplicationConfig;
use crate::replication::{
    MasterLinkStatus, ReplicationBacklog, ReplicationManager, ReplicationRole,
};
use crate::storage::engine::{StorageConfig, StorageEngine};

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

    // Simulate being a replica by connecting (will spawn background task that fails silently)
    replication_manager
        .connect_to_master("localhost".to_string(), 6379)
        .await
        .unwrap();

    // Verify we're in replica role
    let state = replication_manager.get_state().await;
    assert_eq!(state.role, ReplicationRole::Replica);

    // Disconnect from the master
    replication_manager.disconnect_from_master().await.unwrap();

    let state = replication_manager.get_state().await;
    assert_eq!(state.role, ReplicationRole::Master);
    assert_eq!(state.master_host, None);
    assert_eq!(state.master_port, None);
    assert_eq!(state.master_link_status, MasterLinkStatus::Down);
    assert!(!replication_manager.is_read_only());
}

#[tokio::test]
async fn test_replication_backlog() {
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
}

#[tokio::test]
async fn test_replication_backlog_trimming() {
    let mut backlog = ReplicationBacklog::new(20);

    // Fill the backlog
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

    let _rx = mgr
        .register_replica("replica-1".to_string(), "127.0.0.1".to_string(), 6380)
        .await;

    let state = mgr.get_state().await;
    assert_eq!(state.connected_replicas.len(), 1);
    assert_eq!(state.connected_replicas[0].id, "replica-1");
    assert_eq!(state.connected_replicas[0].port, 6380);

    mgr.unregister_replica("replica-1").await;

    let state = mgr.get_state().await;
    assert!(state.connected_replicas.is_empty());
}

#[tokio::test]
async fn test_role_response_master() {
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    let replication_config = ReplicationConfig::default();
    let mgr = ReplicationManager::new(engine.clone(), replication_config);

    let response = mgr.get_role_response().await;
    if let crate::networking::resp::RespValue::Array(Some(parts)) = response {
        assert_eq!(parts.len(), 3);
        if let crate::networking::resp::RespValue::BulkString(Some(role)) = &parts[0] {
            assert_eq!(role, b"master");
        } else {
            panic!("Expected BulkString for role");
        }
    } else {
        panic!("Expected Array response");
    }
}

#[tokio::test]
async fn test_is_write_command() {
    assert!(ReplicationManager::is_write_command("set"));
    assert!(ReplicationManager::is_write_command("del"));
    assert!(ReplicationManager::is_write_command("lpush"));
    assert!(ReplicationManager::is_write_command("hset"));
    assert!(ReplicationManager::is_write_command("zadd"));
    assert!(ReplicationManager::is_write_command("sadd"));
    assert!(ReplicationManager::is_write_command("xreadgroup"));
    assert!(!ReplicationManager::is_write_command("get"));
    assert!(!ReplicationManager::is_write_command("xread"));
    assert!(!ReplicationManager::is_write_command("lrange"));
    assert!(!ReplicationManager::is_write_command("info"));
    assert!(!ReplicationManager::is_write_command("ping"));
}

#[tokio::test]
async fn test_wait_no_replicas() {
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    let replication_config = ReplicationConfig::default();
    let mgr = ReplicationManager::new(engine.clone(), replication_config);

    // With no replicas, WAIT should return 0 immediately
    let count = mgr.wait_for_replicas(1, Duration::from_millis(100)).await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_replicaof_no_one() {
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    let replication_config = ReplicationConfig::default();
    let mgr = ReplicationManager::new(engine.clone(), replication_config);

    // First become a replica
    mgr.connect_to_master("localhost".to_string(), 6379)
        .await
        .unwrap();
    let state = mgr.get_state().await;
    assert_eq!(state.role, ReplicationRole::Replica);

    // Then REPLICAOF NO ONE
    let result = mgr.handle_replicaof("NO", "ONE").await.unwrap();
    assert!(matches!(result, crate::networking::resp::RespValue::SimpleString(s) if s == "OK"));

    let state = mgr.get_state().await;
    assert_eq!(state.role, ReplicationRole::Master);
}
