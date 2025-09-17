// All imports in this file are used in the test functions

use crate::storage::engine::{StorageEngine, StorageConfig};
use crate::replication::{ReplicationManager, ReplicationRole, MasterLinkStatus};
use crate::replication::config::ReplicationConfig;

#[tokio::test]
async fn test_replication_manager_creation() {
    // Create a storage engine
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    
    // Create a replication config
    let replication_config = ReplicationConfig::default();
    
    // Create a replication manager
    let replication_manager = ReplicationManager::new(engine.clone(), replication_config);
    
    // Check that the replication manager is created with the correct state
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
    // Create a storage engine
    let storage_config = StorageConfig::default();
    let engine = StorageEngine::new(storage_config);
    
    // Create a replication config with a master
    let mut replication_config = ReplicationConfig::default();
    replication_config.master_host = Some("localhost".to_string());
    replication_config.master_port = Some(6379);
    
    // Create a replication manager
    let replication_manager = ReplicationManager::new(engine.clone(), replication_config);
    
    // Manually set the state to be a replica
    {
        let mut state = replication_manager.state.write().await;
        state.role = ReplicationRole::Replica;
        state.master_host = Some("localhost".to_string());
        state.master_port = Some(6379);
        state.master_link_status = MasterLinkStatus::Up;
    }
    
    // Disconnect from the master
    replication_manager.disconnect_from_master().await.unwrap();
    
    // Check that the state is updated
    let state = replication_manager.get_state().await;
    assert_eq!(state.role, ReplicationRole::Master);
    assert_eq!(state.master_host, None);
    assert_eq!(state.master_port, None);
    assert_eq!(state.master_link_status, MasterLinkStatus::Down);
}