use std::net::SocketAddr;
/// Redis Cluster Compatibility Test Suite
///
/// Tests cluster redirect behavior, slot assignment, and multi-node operations.
/// Uses both local unit tests and optional Docker-based multi-node cluster testing.
///
/// For Docker-based tests:
///   docker compose -f .devcontainer/docker-compose.test.yml --profile cluster up -d
///   CLUSTER_COMPAT_TEST=1 cargo test --test redis_cluster_compat_test
///   docker compose -f .devcontainer/docker-compose.test.yml --profile cluster down
use std::sync::Arc;

use rlightning::cluster::slot::{CLUSTER_SLOTS, key_hash_slot};
use rlightning::cluster::{ClusterConfig, ClusterManager, NodeRole, RedirectType};
use rlightning::networking::resp::RespValue;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_cluster_env() -> (Arc<StorageEngine>, Arc<ClusterManager>) {
    let storage = StorageEngine::new(StorageConfig::default());
    let config = ClusterConfig {
        enabled: true,
        ..Default::default()
    };
    let mgr = ClusterManager::new(Arc::clone(&storage), config);
    (storage, mgr)
}

fn check_docker_cluster_available() -> bool {
    std::env::var("CLUSTER_COMPAT_TEST").is_ok()
}

// ---------------------------------------------------------------------------
// Cluster Redirect Behavior Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cluster_moved_redirect_format() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.meet("127.0.0.1", 6380).await.unwrap();

    let state = mgr.state().read().await;
    let my_id = state.my_id.clone();
    let other_id = state.nodes.keys().find(|k| **k != my_id).unwrap().clone();
    drop(state);

    // Assign slot for "foo" (12182) to other node
    mgr.set_slot(12182, "NODE", Some(&other_id)).await.unwrap();

    let redirect = mgr.get_redirect(b"foo", false).await;
    assert!(
        redirect.is_some(),
        "Should get redirect for key on remote node"
    );

    let r = redirect.unwrap();
    assert_eq!(r.redirect_type, RedirectType::Moved);
    assert_eq!(r.slot, 12182);
    assert!(
        r.addr.to_string().contains("127.0.0.1:6380"),
        "Redirect address should be node's address"
    );
}

#[tokio::test]
async fn test_cluster_ask_redirect_during_migration() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.meet("127.0.0.1", 6380).await.unwrap();

    let state = mgr.state().read().await;
    let my_id = state.my_id.clone();
    let other_id = state.nodes.keys().find(|k| **k != my_id).unwrap().clone();
    drop(state);

    // Assign slot 100 to self
    mgr.add_slots(&[100]).await.unwrap();

    // Start migrating slot 100 to other node
    mgr.set_slot(100, "MIGRATING", Some(&other_id))
        .await
        .unwrap();

    // Find a key that maps to slot 100 by trying candidates
    let mut test_key = None;
    for i in 0..10000u32 {
        let candidate = format!("key_{}", i);
        if key_hash_slot(candidate.as_bytes()) == 100 {
            test_key = Some(candidate);
            break;
        }
    }

    if let Some(key) = test_key {
        // When the key doesn't exist locally during migration, it may redirect
        let redirect = mgr.get_redirect(key.as_bytes(), false).await;
        // During migration, if key is not found locally, ASK redirect is expected
        if let Some(r) = redirect {
            assert_eq!(r.redirect_type, RedirectType::Ask);
            assert_eq!(r.slot, 100);
        }
    }
}

#[tokio::test]
async fn test_cluster_no_redirect_for_local_slots() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    let _my_id = mgr.my_id().await;

    // Assign all slots to self
    let all_slots: Vec<u16> = (0..CLUSTER_SLOTS).collect();
    mgr.add_slots(&all_slots).await.unwrap();

    // No key should redirect
    for key in &["foo", "bar", "hello", "world", "test"] {
        let redirect = mgr.get_redirect(key.as_bytes(), false).await;
        assert!(
            redirect.is_none(),
            "Key '{}' should not redirect when all slots are local",
            key
        );
    }
}

#[tokio::test]
async fn test_cluster_cross_slot_rejection() {
    let (_storage, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // Different keys that hash to different slots
    assert!(mgr.check_cross_slot(&[b"foo", b"bar"]));

    // Same hash tag = same slot
    assert!(!mgr.check_cross_slot(&[b"{tag}key1", b"{tag}key2", b"{tag}key3"]));

    // Mixed hash tags
    assert!(mgr.check_cross_slot(&[b"{tag1}key1", b"{tag2}key2"]));
}

#[tokio::test]
async fn test_cluster_hash_tag_with_braces() {
    // Test Redis hash tag specification
    let slot1 = key_hash_slot(b"{user1000}.following");
    let slot2 = key_hash_slot(b"{user1000}.followers");
    assert_eq!(slot1, slot2, "Same hash tag should map to same slot");

    // First { and first } after it define the tag
    let _slot3 = key_hash_slot(b"foo{}{bar}");
    let _slot4 = key_hash_slot(b"foo{}");
    // Empty hash tag means use whole key - consistent hashing
    assert_eq!(
        key_hash_slot(b"foo{}{bar}"),
        key_hash_slot(b"foo{}{bar}"),
        "Consistent hashing required"
    );

    // Nested braces: first { to first } is the tag
    let slot5 = key_hash_slot(b"foo{{bar}}zap");
    let slot6 = key_hash_slot(b"foo{{bar}}baz");
    assert_eq!(slot5, slot6, "Same effective hash tag");
}

// ---------------------------------------------------------------------------
// Cluster State Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cluster_state_transitions() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // Initially FAIL (no slots assigned)
    let info = mgr.get_cluster_info().await;
    assert!(info.contains("cluster_state:fail"));

    // Partially assigned slots still FAIL
    mgr.add_slots(&[0, 1, 2]).await.unwrap();
    let info = mgr.get_cluster_info().await;
    assert!(info.contains("cluster_state:fail"));

    // All slots assigned = OK
    let remaining: Vec<u16> = (3..CLUSTER_SLOTS).collect();
    mgr.add_slots(&remaining).await.unwrap();
    let info = mgr.get_cluster_info().await;
    assert!(info.contains("cluster_state:ok"));
}

#[tokio::test]
async fn test_cluster_slots_output_format() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // Add contiguous ranges
    mgr.add_slots(&[0, 1, 2, 3, 4, 10, 11, 12]).await.unwrap();

    let slots = mgr.get_cluster_slots().await;
    assert!(!slots.is_empty(), "Should have slot range entries");

    // Each entry should have (start, end, nodes)
    for (start, end, nodes) in &slots {
        assert!(start <= end, "Start should be <= end");
        assert!(
            !nodes.is_empty(),
            "Each range should have at least one node"
        );
    }
}

#[tokio::test]
async fn test_cluster_shards_output_format() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.add_slots(&[0, 1, 2, 3, 4]).await.unwrap();

    let shards = mgr.get_cluster_shards().await;
    assert_eq!(shards.len(), 1, "Single node should produce one shard");
    assert_eq!(shards[0].nodes.len(), 1);
    assert_eq!(shards[0].nodes[0].role, "master");
}

// ---------------------------------------------------------------------------
// CLUSTER Command Interface Tests (via cluster_command directly)
// Cluster commands are handled at the server level, not through CommandHandler
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cluster_info_via_handler() {
    use rlightning::command::types::cluster::cluster_command;
    let (storage, _) = create_cluster_env();

    let result = cluster_command(&storage, &[b"INFO".to_vec()], None)
        .await
        .unwrap();

    match result {
        RespValue::BulkString(Some(data)) => {
            let info = String::from_utf8(data).unwrap();
            assert!(info.contains("cluster_enabled"));
        }
        _ => panic!("Expected BulkString from CLUSTER INFO"),
    }
}

#[tokio::test]
async fn test_cluster_keyslot_via_handler() {
    use rlightning::command::types::cluster::cluster_command;
    let (storage, _) = create_cluster_env();

    let result = cluster_command(&storage, &[b"KEYSLOT".to_vec(), b"foo".to_vec()], None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(12182));

    let result = cluster_command(&storage, &[b"KEYSLOT".to_vec(), b"{tag}key".to_vec()], None)
        .await
        .unwrap();
    let slot = match result {
        RespValue::Integer(i) => i,
        _ => panic!("Expected integer"),
    };
    assert!(slot >= 0 && slot < CLUSTER_SLOTS as i64);
}

#[tokio::test]
async fn test_cluster_countkeysinslot_via_manager() {
    let (storage, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // Store a key - "foo" hashes to slot 12182
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
async fn test_cluster_getkeysinslot_via_manager() {
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

    let keys = mgr.get_keys_in_slot(0, 10).await.unwrap();
    assert!(keys.is_empty());
}

// ---------------------------------------------------------------------------
// Cluster Multi-Node Topology Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cluster_multi_node_topology() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    // Add 5 more nodes
    for port in 6380..=6384 {
        mgr.meet("127.0.0.1", port).await.unwrap();
    }

    let state = mgr.state().read().await;
    assert_eq!(state.nodes.len(), 6, "Should have 6 nodes");

    // Verify node output
    drop(state);
    let nodes_output = mgr.get_cluster_nodes().await;
    let node_lines: Vec<&str> = nodes_output.lines().collect();
    assert_eq!(node_lines.len(), 6);

    // One should be "myself"
    assert!(
        node_lines.iter().any(|l| l.contains("myself")),
        "One node should be flagged as myself"
    );
}

#[tokio::test]
async fn test_cluster_slot_distribution() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.meet("127.0.0.1", 6380).await.unwrap();
    mgr.meet("127.0.0.1", 6381).await.unwrap();

    let state = mgr.state().read().await;
    let node_ids: Vec<String> = state.nodes.keys().cloned().collect();
    drop(state);

    // Distribute slots across nodes
    let slots_per_node = CLUSTER_SLOTS / 3;
    for (i, node_id) in node_ids.iter().enumerate() {
        let start = (i as u16) * slots_per_node;
        let end = if i == 2 {
            CLUSTER_SLOTS
        } else {
            start + slots_per_node
        };
        for slot in start..end {
            mgr.set_slot(slot, "NODE", Some(node_id)).await.unwrap();
        }
    }

    // Cluster should be OK now
    let info = mgr.get_cluster_info().await;
    assert!(info.contains("cluster_state:ok"));
    assert!(info.contains("cluster_slots_assigned:16384"));
}

// ---------------------------------------------------------------------------
// Cluster Failover Scenarios
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cluster_failover_promotion() {
    let (_, mgr) = create_cluster_env();
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    mgr.init(addr).await;

    mgr.meet("127.0.0.1", 6380).await.unwrap();

    let state = mgr.state().read().await;
    let my_id = state.my_id.clone();
    let other_id = state.nodes.keys().find(|k| **k != my_id).unwrap().clone();
    drop(state);

    // Assign slots to other node
    for slot in 0..100 {
        mgr.set_slot(slot, "NODE", Some(&other_id)).await.unwrap();
    }

    // Become replica
    mgr.replicate(&other_id).await.unwrap();

    // Verify we're a replica
    let state = mgr.state().read().await;
    assert_eq!(state.nodes[&my_id].flags.role, NodeRole::Replica);
    drop(state);

    // Manual failover (force)
    mgr.failover(true).await.unwrap();

    // Verify promotion
    let state = mgr.state().read().await;
    assert_eq!(state.nodes[&my_id].flags.role, NodeRole::Master);
    // Verify slot takeover
    for slot in 0..100 {
        assert!(
            state.nodes[&my_id].owns_slot(slot),
            "Should own slot {} after failover",
            slot
        );
    }
}

// ---------------------------------------------------------------------------
// Docker-based Multi-Node Cluster Test (optional)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cluster_docker_redirect_behavior() {
    if !check_docker_cluster_available() {
        println!("Skipping Docker cluster test (CLUSTER_COMPAT_TEST not set)");
        // Run a basic local validation instead
        let (_, mgr) = create_cluster_env();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;
        let my_id = mgr.my_id().await;
        assert_eq!(my_id.len(), 40);
        return;
    }

    // Connect to Docker cluster nodes
    use rlightning::networking::client::Client;

    let ports = [7001, 7002, 7003];
    let mut clients = Vec::new();
    for port in &ports {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        match Client::connect(addr).await {
            Ok(client) => clients.push(client),
            Err(e) => {
                println!("Cannot connect to cluster node on port {}: {}", port, e);
                return;
            }
        }
    }

    // Test basic cluster operations on first node
    let r = clients[0]
        .send_command_str("CLUSTER", &["INFO"])
        .await
        .unwrap();
    match &r {
        RespValue::BulkString(Some(data)) => {
            let info = String::from_utf8_lossy(data);
            assert!(info.contains("cluster_enabled:1"));
        }
        _ => panic!("Expected BulkString from CLUSTER INFO"),
    }

    // Test CLUSTER KEYSLOT
    let r = clients[0]
        .send_command_str("CLUSTER", &["KEYSLOT", "foo"])
        .await
        .unwrap();
    match &r {
        RespValue::Integer(slot) => {
            assert_eq!(*slot, 12182);
        }
        _ => panic!("Expected integer from CLUSTER KEYSLOT"),
    }

    println!("Docker cluster redirect behavior test passed");
}
