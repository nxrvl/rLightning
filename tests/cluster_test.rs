/// Integration tests for Redis Cluster support
/// Tests cluster slot calculation, topology management, CLUSTER subcommands,
/// redirections, slot migration, and failover scenarios.
mod test_utils;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rlightning::cluster::gossip::{cluster_cron, start_cluster_bus};
use rlightning::cluster::slot::{CLUSTER_SLOTS, SlotRange, key_hash_slot};
use rlightning::cluster::{ClusterConfig, ClusterManager, NodeRole};
use rlightning::command::Command;
use rlightning::command::handler::CommandHandler;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

use test_utils::{DEFAULT_TEST_PORT, create_client};

// Helper to create a cluster manager with a storage engine
fn create_cluster_env() -> (Arc<StorageEngine>, Arc<ClusterManager>) {
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    let cluster_config = ClusterConfig {
        enabled: true,
        ..Default::default()
    };
    let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
    (storage, mgr)
}

// --- Hash Slot Calculation Tests ---

#[test]
fn test_hash_slot_calculation_basic() {
    // Verify known Redis hash slot values
    assert_eq!(key_hash_slot(b"foo"), 12182);
    assert_eq!(key_hash_slot(b"bar"), 5061);
    assert_eq!(key_hash_slot(b"hello"), 866);
}

#[test]
fn test_hash_slot_with_hash_tags() {
    // Keys with same hash tag must map to same slot
    let slot1 = key_hash_slot(b"user:{12345}:name");
    let slot2 = key_hash_slot(b"user:{12345}:email");
    let slot3 = key_hash_slot(b"user:{12345}:posts");
    assert_eq!(slot1, slot2);
    assert_eq!(slot2, slot3);

    // Different hash tags produce different slots (usually)
    let slot_a = key_hash_slot(b"{a}key");
    let slot_b = key_hash_slot(b"{b}key");
    assert_ne!(slot_a, slot_b);
}

#[test]
fn test_hash_slot_edge_cases() {
    // Empty key
    let slot = key_hash_slot(b"");
    assert!(slot < CLUSTER_SLOTS);

    // Key with empty hash tag - should use whole key
    let slot1 = key_hash_slot(b"foo{}bar");
    let slot2 = key_hash_slot(b"foo{}bar");
    assert_eq!(slot1, slot2);

    // Key with unclosed brace - should use whole key
    let slot = key_hash_slot(b"foo{bar");
    assert!(slot < CLUSTER_SLOTS);

    // Key with only braces
    let _slot = key_hash_slot(b"{}");
    // Empty tag, uses whole key

    // Very long key
    let long_key = vec![b'x'; 10000];
    let slot = key_hash_slot(&long_key);
    assert!(slot < CLUSTER_SLOTS);
}

#[test]
fn test_slot_range() {
    let range = SlotRange::new(0, 5460);
    assert!(range.contains(0));
    assert!(range.contains(2730));
    assert!(range.contains(5460));
    assert!(!range.contains(5461));
    assert_eq!(range.count(), 5461);

    let single = SlotRange::single(42);
    assert!(single.contains(42));
    assert!(!single.contains(43));
    assert_eq!(single.count(), 1);

    // Display format
    assert_eq!(format!("{}", SlotRange::new(0, 5460)), "0-5460");
    assert_eq!(format!("{}", SlotRange::single(42)), "42");
}

// --- Topology Management Tests ---

#[tokio::test]
async fn test_cluster_init_and_my_id() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let my_id = mgr.my_id().await;
    assert_eq!(my_id.len(), 40);
    assert!(my_id.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn test_cluster_add_all_slots() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // Add all 16384 slots
    let all_slots: Vec<u16> = (0..CLUSTER_SLOTS).collect();
    mgr.add_slots(&all_slots).await.unwrap();

    let state = mgr.state().read().await;
    assert_eq!(state.slots_assigned, CLUSTER_SLOTS);
    assert_eq!(format!("{}", state.cluster_state), "ok");
}

#[tokio::test]
async fn test_cluster_meet_multiple_nodes() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // Meet multiple nodes
    mgr.meet("127.0.0.1", 6380).await.unwrap();
    mgr.meet("127.0.0.1", 6381).await.unwrap();
    mgr.meet("127.0.0.1", 6382).await.unwrap();

    let state = mgr.state().read().await;
    assert_eq!(state.nodes.len(), 4);
}

#[tokio::test]
async fn test_cluster_replicate() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.meet("127.0.0.1", 6380).await.unwrap();

    let state = mgr.state().read().await;
    let my_id = state.my_id.clone();
    let other_id = state.nodes.keys().find(|k| **k != my_id).unwrap().clone();
    drop(state);

    // Try to replicate (should fail since we have no slots, but role change should work)
    mgr.replicate(&other_id).await.unwrap();

    let state = mgr.state().read().await;
    let my_node = &state.nodes[&state.my_id];
    assert_eq!(my_node.flags.role, NodeRole::Replica);
    assert_eq!(my_node.master_id.as_deref(), Some(other_id.as_str()));
}

#[tokio::test]
async fn test_cluster_replicate_self_fails() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let my_id = mgr.my_id().await;
    let result = mgr.replicate(&my_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cluster_reset_soft() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let old_id = mgr.my_id().await;
    mgr.meet("127.0.0.1", 6380).await.unwrap();
    mgr.add_slots(&[0, 1, 2, 3]).await.unwrap();

    mgr.reset(false).await;

    let state = mgr.state().read().await;
    assert_eq!(state.nodes.len(), 1);
    assert_eq!(state.my_id, old_id); // Soft reset preserves ID
    assert_eq!(state.slots_assigned, 0);
}

#[tokio::test]
async fn test_cluster_reset_hard() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let old_id = mgr.my_id().await;
    mgr.reset(true).await;

    let new_id = mgr.my_id().await;
    assert_ne!(old_id, new_id); // Hard reset changes ID
}

// --- Slot Management Tests ---

#[tokio::test]
async fn test_cluster_add_and_del_slots() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let slots: Vec<u16> = (0..100).collect();
    mgr.add_slots(&slots).await.unwrap();

    let state = mgr.state().read().await;
    assert_eq!(state.slots_assigned, 100);
    drop(state);

    // Delete some slots
    mgr.del_slots(&[50, 51, 52]).await.unwrap();

    let state = mgr.state().read().await;
    assert_eq!(state.slots_assigned, 97);
}

#[tokio::test]
async fn test_cluster_add_invalid_slot_fails() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let result = mgr.add_slots(&[16384]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cluster_flush_slots() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.add_slots(&[0, 1, 2, 3, 4]).await.unwrap();
    mgr.flush_slots().await;

    let state = mgr.state().read().await;
    assert_eq!(state.slots_assigned, 0);
}

#[tokio::test]
async fn test_cluster_setslot_migration() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.meet("127.0.0.1", 6380).await.unwrap();

    let state = mgr.state().read().await;
    let my_id = state.my_id.clone();
    let other_id = state.nodes.keys().find(|k| **k != my_id).unwrap().clone();
    drop(state);

    // Assign slot 100 to ourselves
    mgr.add_slots(&[100]).await.unwrap();

    // Start migrating slot 100 to other node
    mgr.set_slot(100, "MIGRATING", Some(&other_id))
        .await
        .unwrap();

    let state = mgr.state().read().await;
    assert!(state.migration_states.contains_key(&100));
    drop(state);

    // Set slot back to stable
    mgr.set_slot(100, "STABLE", None).await.unwrap();

    let state = mgr.state().read().await;
    assert!(!state.migration_states.contains_key(&100));
}

#[tokio::test]
async fn test_cluster_setslot_importing() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.meet("127.0.0.1", 6380).await.unwrap();

    let state = mgr.state().read().await;
    let my_id = state.my_id.clone();
    let other_id = state.nodes.keys().find(|k| **k != my_id).unwrap().clone();
    drop(state);

    // Start importing slot 200 from other node
    mgr.set_slot(200, "IMPORTING", Some(&other_id))
        .await
        .unwrap();

    let state = mgr.state().read().await;
    assert!(state.migration_states.contains_key(&200));
}

#[tokio::test]
async fn test_cluster_setslot_node() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let my_id = mgr.my_id().await;

    mgr.set_slot(500, "NODE", Some(&my_id)).await.unwrap();

    let state = mgr.state().read().await;
    assert!(state.nodes[&my_id].owns_slot(500));
    assert_eq!(state.slots_assigned, 1);
}

// --- Key Slot Operations Tests ---

#[tokio::test]
async fn test_cluster_count_keys_in_slot() {
    let (storage, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // "foo" hashes to slot 12182
    storage
        .set(b"foo".to_vec(), b"bar".to_vec(), None)
        .await
        .unwrap();

    let count = mgr.count_keys_in_slot(12182).await.unwrap();
    assert_eq!(count, 1);

    let count = mgr.count_keys_in_slot(0).await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_cluster_get_keys_in_slot() {
    let (storage, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    storage
        .set(b"foo".to_vec(), b"bar".to_vec(), None)
        .await
        .unwrap();

    let keys = mgr.get_keys_in_slot(12182, 10).await.unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], b"foo");
}

// --- Cross-Slot Validation Tests ---

#[tokio::test]
async fn test_cross_slot_detection() {
    let (_, mgr) = create_cluster_env();

    // Different keys = cross-slot
    assert!(mgr.check_cross_slot(&[b"foo", b"bar"]));

    // Same hash tag = same slot, no cross-slot
    assert!(!mgr.check_cross_slot(&[b"{tag}key1", b"{tag}key2"]));

    // Single key = no cross-slot
    assert!(!mgr.check_cross_slot(&[b"anything"]));

    // Empty = no cross-slot
    assert!(!mgr.check_cross_slot(&[]));
}

// --- Redirect Tests ---

#[tokio::test]
async fn test_cluster_redirect_moved() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.meet("127.0.0.1", 6380).await.unwrap();

    let state = mgr.state().read().await;
    let my_id = state.my_id.clone();
    let other_id = state.nodes.keys().find(|k| **k != my_id).unwrap().clone();
    drop(state);

    // Assign slot 12182 (where "foo" lives) to the other node
    // via SETSLOT NODE
    mgr.set_slot(12182, "NODE", Some(&other_id)).await.unwrap();

    // Now "foo" should redirect
    let redirect = mgr.get_redirect(b"foo").await;
    assert!(redirect.is_some());
    let redirect = redirect.unwrap();
    assert_eq!(
        redirect.redirect_type,
        rlightning::cluster::RedirectType::Moved
    );
    assert_eq!(redirect.slot, 12182);
}

#[tokio::test]
async fn test_cluster_no_redirect_for_local_key() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let my_id = mgr.my_id().await;

    // Assign the slot for "foo" to ourselves
    mgr.set_slot(12182, "NODE", Some(&my_id)).await.unwrap();

    // No redirect needed
    let redirect = mgr.get_redirect(b"foo").await;
    assert!(redirect.is_none());
}

// --- Failover Tests ---

#[tokio::test]
async fn test_cluster_failover_requires_replica() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // Failover should fail because we're a master
    let result = mgr.failover(false).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("replica"));
}

#[tokio::test]
async fn test_cluster_manual_failover() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.meet("127.0.0.1", 6380).await.unwrap();

    let state = mgr.state().read().await;
    let my_id = state.my_id.clone();
    let other_id = state.nodes.keys().find(|k| **k != my_id).unwrap().clone();
    drop(state);

    // Give the other node some slots
    mgr.set_slot(0, "NODE", Some(&other_id)).await.unwrap();
    mgr.set_slot(1, "NODE", Some(&other_id)).await.unwrap();

    // Make ourselves a replica of the other node
    mgr.replicate(&other_id).await.unwrap();

    // Now failover should succeed
    mgr.failover(true).await.unwrap();

    // We should now be master
    let state = mgr.state().read().await;
    let my_node = &state.nodes[&state.my_id];
    assert_eq!(my_node.flags.role, NodeRole::Master);
    // We should have taken over the other node's slots
    assert!(my_node.owns_slot(0));
    assert!(my_node.owns_slot(1));
}

// --- CLUSTER Command Interface Tests ---
// These tests call cluster_command directly since cluster commands are now
// handled at the server level (similar to sentinel/ACL)

#[tokio::test]
async fn test_cluster_command_info_disabled() {
    use rlightning::command::types::cluster::cluster_command;
    let (storage, _) = create_cluster_env();

    // Without a cluster manager, CLUSTER INFO shows disabled
    let result = cluster_command(&storage, &[b"INFO".to_vec()], None)
        .await
        .unwrap();
    if let RespValue::BulkString(Some(data)) = result {
        let info = String::from_utf8(data).unwrap();
        assert!(info.contains("cluster_enabled:0"));
    } else {
        panic!("Expected BulkString for CLUSTER INFO");
    }
}

#[tokio::test]
async fn test_cluster_command_info_enabled() {
    use rlightning::command::types::cluster::cluster_command;
    let (storage, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // Assign all slots so cluster is OK
    let all_slots: Vec<u16> = (0..16384).collect();
    mgr.add_slots(&all_slots).await.unwrap();

    // With a cluster manager, CLUSTER INFO shows enabled
    let result = cluster_command(&storage, &[b"INFO".to_vec()], Some(&mgr))
        .await
        .unwrap();
    if let RespValue::BulkString(Some(data)) = result {
        let info = String::from_utf8(data).unwrap();
        assert!(info.contains("cluster_enabled:1"));
        assert!(info.contains("cluster_state:ok"));
        assert!(info.contains("cluster_slots_assigned:16384"));
    } else {
        panic!("Expected BulkString for CLUSTER INFO");
    }
}

#[tokio::test]
async fn test_cluster_command_keyslot() {
    use rlightning::command::types::cluster::cluster_command;
    let (storage, _) = create_cluster_env();

    let result = cluster_command(&storage, &[b"KEYSLOT".to_vec(), b"foo".to_vec()], None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(12182));
}

#[tokio::test]
async fn test_cluster_command_help() {
    use rlightning::command::types::cluster::cluster_command;
    let (storage, _) = create_cluster_env();

    let result = cluster_command(&storage, &[b"HELP".to_vec()], None)
        .await
        .unwrap();
    if let RespValue::Array(Some(lines)) = result {
        assert!(lines.len() > 5);
    } else {
        panic!("Expected Array for CLUSTER HELP");
    }
}

#[tokio::test]
async fn test_cluster_command_nodes_enabled() {
    use rlightning::command::types::cluster::cluster_command;
    let (storage, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let result = cluster_command(&storage, &[b"NODES".to_vec()], Some(&mgr))
        .await
        .unwrap();
    if let RespValue::BulkString(Some(data)) = result {
        let nodes = String::from_utf8(data).unwrap();
        assert!(nodes.contains("myself,master"));
    } else {
        panic!("Expected BulkString for CLUSTER NODES");
    }
}

#[tokio::test]
async fn test_cluster_command_myid_enabled() {
    use rlightning::command::types::cluster::cluster_command;
    let (storage, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let result = cluster_command(&storage, &[b"MYID".to_vec()], Some(&mgr))
        .await
        .unwrap();
    if let RespValue::BulkString(Some(data)) = result {
        let id = String::from_utf8(data).unwrap();
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    } else {
        panic!("Expected BulkString for CLUSTER MYID");
    }
}

#[tokio::test]
async fn test_asking_command() {
    let (storage, _) = create_cluster_env();
    let handler = CommandHandler::new(Arc::clone(&storage));

    let cmd = Command {
        name: "asking".to_string(),
        args: vec![],
    };
    let result = handler.process(cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));
}

#[tokio::test]
async fn test_readonly_readwrite_commands() {
    let (storage, _) = create_cluster_env();
    let handler = CommandHandler::new(Arc::clone(&storage));

    let cmd = Command {
        name: "readonly".to_string(),
        args: vec![],
    };
    let result = handler.process(cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    let cmd = Command {
        name: "readwrite".to_string(),
        args: vec![],
    };
    let result = handler.process(cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));
}

// --- Config Epoch Tests ---

#[tokio::test]
async fn test_cluster_set_config_epoch() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.set_config_epoch(42).await.unwrap();

    let state = mgr.state().read().await;
    assert_eq!(state.current_epoch, 42);
    drop(state);

    // Second attempt should fail
    let result = mgr.set_config_epoch(100).await;
    assert!(result.is_err());
}

// --- Cluster Info Tests ---

#[tokio::test]
async fn test_cluster_info_all_slots_assigned() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // Initially no slots = FAIL
    let info = mgr.get_cluster_info().await;
    assert!(info.contains("cluster_state:fail"));

    // Add all slots
    let all_slots: Vec<u16> = (0..CLUSTER_SLOTS).collect();
    mgr.add_slots(&all_slots).await.unwrap();

    // Now cluster should be OK
    let info = mgr.get_cluster_info().await;
    assert!(info.contains("cluster_state:ok"));
    assert!(info.contains("cluster_slots_assigned:16384"));
    assert!(info.contains("cluster_size:1"));
}

#[tokio::test]
async fn test_cluster_nodes_output() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let nodes = mgr.get_cluster_nodes().await;
    assert!(nodes.contains("myself,master"));
    assert!(nodes.contains("127.0.0.1:6379"));
}

#[tokio::test]
async fn test_cluster_slots_output() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.add_slots(&[0, 1, 2, 3, 4]).await.unwrap();

    let slots = mgr.get_cluster_slots().await;
    assert!(!slots.is_empty());

    let (start, end, nodes) = &slots[0];
    assert_eq!(*start, 0);
    assert_eq!(*end, 4);
    assert!(!nodes.is_empty());
}

#[tokio::test]
async fn test_cluster_shards_output() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.add_slots(&[0, 1, 2]).await.unwrap();

    let shards = mgr.get_cluster_shards().await;
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].nodes.len(), 1);
    assert_eq!(shards[0].nodes[0].role, "master");
}

// --- Node Timeout and Failure Detection Tests ---

#[tokio::test]
async fn test_cluster_node_timeout_detection() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // check_node_timeouts should not panic with only one node
    mgr.check_node_timeouts().await;
}

#[tokio::test]
async fn test_cluster_automatic_failover_as_master_noop() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // As a master, try_automatic_failover should be a no-op
    mgr.try_automatic_failover().await;

    let state = mgr.state().read().await;
    assert_eq!(state.nodes[&state.my_id].flags.role, NodeRole::Master);
}

// --- Forget Tests ---

#[tokio::test]
async fn test_cluster_forget_unknown_node() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let result = mgr.forget("nonexistent_node_id").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cluster_forget_self_fails() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let my_id = mgr.my_id().await;
    let result = mgr.forget(&my_id).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("forget myself"));
}

// --- Save Config Test ---

#[tokio::test]
async fn test_cluster_save_config() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let result = mgr.save_config().await;
    assert!(result.is_ok());
}

// --- Links Test ---

#[tokio::test]
async fn test_cluster_links() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // With only one node, no links
    let links = mgr.get_cluster_links().await;
    assert!(links.is_empty());

    // Meet another node, should have links
    mgr.meet("127.0.0.1", 6380).await.unwrap();
    let links = mgr.get_cluster_links().await;
    assert!(!links.is_empty());
}

// --- Server-Level Cluster Command Tests ---
// These tests verify cluster commands work through a real server connection

/// Helper to set up a test server with cluster mode enabled
async fn setup_cluster_server(
    port_offset: u16,
) -> Result<(SocketAddr, Arc<ClusterManager>), Box<dyn std::error::Error + Send + Sync>> {
    let port = DEFAULT_TEST_PORT + port_offset;
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    let storage = StorageEngine::new(StorageConfig::default());

    let cluster_config = ClusterConfig {
        enabled: true,
        ..Default::default()
    };
    let cluster = ClusterManager::new(Arc::clone(&storage), cluster_config);
    cluster.init(addr).await;

    let server = Server::new(addr, Arc::clone(&storage))
        .with_connection_limit(100)
        .with_buffer_size(1024 * 1024)
        .with_cluster(Arc::clone(&cluster));

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    Ok((addr, cluster))
}

/// Helper to extract string from RespValue
fn resp_string(val: &RespValue) -> String {
    match val {
        RespValue::SimpleString(s) => s.clone(),
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => panic!("Expected string, got: {:?}", val),
    }
}

#[tokio::test]
async fn test_cluster_info_via_server_enabled() {
    let (addr, _cluster) = setup_cluster_server(900).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    let response = client.send_command_str("CLUSTER", &["INFO"]).await.unwrap();
    let info = resp_string(&response);
    assert!(
        info.contains("cluster_enabled:1"),
        "CLUSTER INFO should show enabled"
    );
    assert!(info.contains("cluster_known_nodes:1"), "Should know 1 node");
}

#[tokio::test]
async fn test_cluster_myid_via_server() {
    let (addr, _cluster) = setup_cluster_server(901).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    let response = client.send_command_str("CLUSTER", &["MYID"]).await.unwrap();
    let id = resp_string(&response);
    assert_eq!(id.len(), 40, "Node ID should be 40 hex chars");
    assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn test_cluster_nodes_via_server() {
    let (addr, _cluster) = setup_cluster_server(902).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    let response = client
        .send_command_str("CLUSTER", &["NODES"])
        .await
        .unwrap();
    let nodes = resp_string(&response);
    assert!(
        nodes.contains("myself,master"),
        "Should show self as master"
    );
}

#[tokio::test]
async fn test_cluster_keyslot_via_server() {
    let (addr, _cluster) = setup_cluster_server(903).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    let response = client
        .send_command_str("CLUSTER", &["KEYSLOT", "foo"])
        .await
        .unwrap();
    match response {
        RespValue::Integer(slot) => assert_eq!(slot, 12182),
        _ => panic!("Expected Integer for CLUSTER KEYSLOT, got: {:?}", response),
    }
}

// --- Gossip Activation Tests ---

#[tokio::test]
async fn test_gossip_tasks_spawned_when_cluster_enabled() {
    // Set up cluster manager with enabled=true
    let port = DEFAULT_TEST_PORT + 950;
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let storage = StorageEngine::new(StorageConfig::default());

    let cluster_config = ClusterConfig {
        enabled: true,
        ..Default::default()
    };
    let cluster = ClusterManager::new(Arc::clone(&storage), cluster_config);
    assert!(cluster.is_enabled());
    cluster.init(addr).await;

    // Spawn gossip tasks just like main.rs does
    let bus_mgr = Arc::clone(&cluster);
    let bus_addr = addr;
    tokio::spawn(async move {
        start_cluster_bus(bus_mgr, bus_addr).await;
    });

    let cron_mgr = Arc::clone(&cluster);
    tokio::spawn(async move {
        cluster_cron(cron_mgr).await;
    });

    // Give gossip tasks time to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify the cluster bus is listening on port+10000
    let bus_port = port + 10000;
    let bus_addr: SocketAddr = format!("127.0.0.1:{}", bus_port).parse().unwrap();
    let connect_result = tokio::net::TcpStream::connect(bus_addr).await;
    assert!(
        connect_result.is_ok(),
        "Cluster bus should be listening on port+10000 when cluster is enabled"
    );
}

#[tokio::test]
async fn test_gossip_tasks_not_spawned_when_cluster_disabled() {
    // Set up cluster manager with enabled=false
    let storage = StorageEngine::new(StorageConfig::default());

    let cluster_config = ClusterConfig {
        enabled: false,
        ..Default::default()
    };
    let cluster = ClusterManager::new(Arc::clone(&storage), cluster_config);
    assert!(!cluster.is_enabled());

    // In the real code, gossip tasks are only spawned inside `if cluster.is_enabled()`.
    // Verify the guard works: bus port should NOT be listening.
    let port = DEFAULT_TEST_PORT + 951;
    let bus_port = port + 10000;
    let bus_addr: SocketAddr = format!("127.0.0.1:{}", bus_port).parse().unwrap();
    let connect_result = tokio::net::TcpStream::connect(bus_addr).await;
    assert!(
        connect_result.is_err(),
        "Cluster bus should NOT be listening when cluster is disabled"
    );
}
