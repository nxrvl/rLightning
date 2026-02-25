/// Integration tests for Redis Cluster support
/// Tests cluster slot calculation, topology management, CLUSTER subcommands,
/// redirections, slot migration, and failover scenarios.

use std::sync::Arc;
use std::net::SocketAddr;

use rlightning::cluster::{ClusterConfig, ClusterManager, NodeRole};
use rlightning::cluster::slot::{CLUSTER_SLOTS, key_hash_slot, SlotRange};
use rlightning::command::handler::CommandHandler;
use rlightning::command::Command;
use rlightning::networking::resp::RespValue;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

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
    assert_eq!(
        format!("{}", state.cluster_state),
        "ok"
    );
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
    let other_id = state
        .nodes
        .keys()
        .find(|k| **k != my_id)
        .unwrap()
        .clone();
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
    let other_id = state
        .nodes
        .keys()
        .find(|k| **k != my_id)
        .unwrap()
        .clone();
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
    let other_id = state
        .nodes
        .keys()
        .find(|k| **k != my_id)
        .unwrap()
        .clone();
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
    let other_id = state
        .nodes
        .keys()
        .find(|k| **k != my_id)
        .unwrap()
        .clone();
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
    let other_id = state
        .nodes
        .keys()
        .find(|k| **k != my_id)
        .unwrap()
        .clone();
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

#[tokio::test]
async fn test_cluster_command_info() {
    let (storage, _) = create_cluster_env();
    let handler = CommandHandler::new(Arc::clone(&storage));

    let cmd = Command {
        name: "cluster".to_string(),
        args: vec![b"INFO".to_vec()],
    };
    let result = handler.process(cmd).await.unwrap();
    if let RespValue::BulkString(Some(data)) = result {
        let info = String::from_utf8(data).unwrap();
        assert!(info.contains("cluster_enabled:0"));
    } else {
        panic!("Expected BulkString for CLUSTER INFO");
    }
}

#[tokio::test]
async fn test_cluster_command_keyslot() {
    let (storage, _) = create_cluster_env();
    let handler = CommandHandler::new(Arc::clone(&storage));

    let cmd = Command {
        name: "cluster".to_string(),
        args: vec![b"KEYSLOT".to_vec(), b"foo".to_vec()],
    };
    let result = handler.process(cmd).await.unwrap();
    assert_eq!(result, RespValue::Integer(12182));
}

#[tokio::test]
async fn test_cluster_command_help() {
    let (storage, _) = create_cluster_env();
    let handler = CommandHandler::new(Arc::clone(&storage));

    let cmd = Command {
        name: "cluster".to_string(),
        args: vec![b"HELP".to_vec()],
    };
    let result = handler.process(cmd).await.unwrap();
    if let RespValue::Array(Some(lines)) = result {
        assert!(lines.len() > 5);
    } else {
        panic!("Expected Array for CLUSTER HELP");
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
    let result = handler.process(cmd).await.unwrap();
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
    let result = handler.process(cmd).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    let cmd = Command {
        name: "readwrite".to_string(),
        args: vec![],
    };
    let result = handler.process(cmd).await.unwrap();
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
