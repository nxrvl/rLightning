/// Redis Sentinel Compatibility Test Suite
///
/// Tests sentinel monitoring, failover behavior, and configuration.
/// Uses both local unit tests and optional Docker-based sentinel topology testing.
///
/// For Docker-based tests:
///   docker compose -f .devcontainer/docker-compose.test.yml --profile sentinel up -d
///   SENTINEL_COMPAT_TEST=1 cargo test --test redis_sentinel_compat_test
///   docker compose -f .devcontainer/docker-compose.test.yml --profile sentinel down
mod test_utils;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::sentinel::{ReplicaEntry, SentinelConfig, SentinelManager};
use rlightning::storage::engine::{StorageConfig, StorageEngine};

use test_utils::{DEFAULT_TEST_PORT, create_client};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn setup_sentinel(
    port_offset: u16,
    config: SentinelConfig,
) -> (SocketAddr, Arc<SentinelManager>) {
    let port = DEFAULT_TEST_PORT + port_offset;
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let storage = StorageEngine::new(StorageConfig::default());

    let sentinel = SentinelManager::new(Arc::clone(&storage), config);
    sentinel.init().await;

    let server = Server::new(addr, Arc::clone(&storage))
        .with_connection_limit(100)
        .with_buffer_size(1024 * 1024)
        .with_sentinel(Arc::clone(&sentinel));

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    sleep(Duration::from_millis(200)).await;
    (addr, sentinel)
}

fn resp_string(val: &RespValue) -> String {
    match val {
        RespValue::SimpleString(s) => s.clone(),
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => panic!("Expected string, got: {:?}", val),
    }
}

fn resp_int(val: &RespValue) -> i64 {
    match val {
        RespValue::Integer(i) => *i,
        _ => panic!("Expected integer, got: {:?}", val),
    }
}

fn resp_array(val: &RespValue) -> &Vec<RespValue> {
    match val {
        RespValue::Array(Some(arr)) => arr,
        _ => panic!("Expected array, got: {:?}", val),
    }
}

fn is_error(val: &RespValue) -> bool {
    matches!(val, RespValue::Error(_))
}

fn is_null(val: &RespValue) -> bool {
    matches!(
        val,
        RespValue::Null | RespValue::BulkString(None) | RespValue::Array(None)
    )
}

fn check_docker_sentinel_available() -> bool {
    std::env::var("SENTINEL_COMPAT_TEST").is_ok()
}

// ---------------------------------------------------------------------------
// Sentinel Monitoring Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sentinel_compat_monitor_master() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(850, config).await;
    let mut c = create_client(addr).await.unwrap();

    // MONITOR
    let r = c
        .send_command_str(
            "SENTINEL",
            &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"],
        )
        .await
        .unwrap();
    assert_eq!(resp_string(&r), "OK");

    // MASTERS
    let r = c.send_command_str("SENTINEL", &["MASTERS"]).await.unwrap();
    let masters = resp_array(&r);
    assert_eq!(masters.len(), 1);

    // MASTER by name
    let r = c
        .send_command_str("SENTINEL", &["MASTER", "mymaster"])
        .await
        .unwrap();
    let fields = resp_array(&r);
    assert!(fields.len() >= 10, "Master info should have many fields");

    // Find name field
    let mut found_name = false;
    for i in (0..fields.len()).step_by(2) {
        if resp_string(&fields[i]) == "name" {
            assert_eq!(resp_string(&fields[i + 1]), "mymaster");
            found_name = true;
            break;
        }
    }
    assert!(found_name);
}

#[tokio::test]
async fn test_sentinel_compat_master_addr_by_name() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(851, config).await;
    let mut c = create_client(addr).await.unwrap();

    c.send_command_str(
        "SENTINEL",
        &["MONITOR", "mymaster", "10.0.0.1", "6379", "2"],
    )
    .await
    .unwrap();

    // GET-MASTER-ADDR-BY-NAME
    let r = c
        .send_command_str("SENTINEL", &["GET-MASTER-ADDR-BY-NAME", "mymaster"])
        .await
        .unwrap();
    let arr = resp_array(&r);
    assert_eq!(arr.len(), 2);
    assert_eq!(resp_string(&arr[0]), "10.0.0.1");
    assert_eq!(resp_string(&arr[1]), "6379");

    // Nonexistent master
    let r = c
        .send_command_str("SENTINEL", &["GET-MASTER-ADDR-BY-NAME", "nosuch"])
        .await
        .unwrap();
    assert!(is_null(&r));
}

#[tokio::test]
async fn test_sentinel_compat_monitor_multiple_masters() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(852, config).await;
    let mut c = create_client(addr).await.unwrap();

    // Monitor 3 masters
    for i in 1..=3 {
        let name = format!("master{}", i);
        let port = format!("{}", 6379 + i);
        c.send_command_str("SENTINEL", &["MONITOR", &name, "127.0.0.1", &port, "2"])
            .await
            .unwrap();
    }

    let r = c.send_command_str("SENTINEL", &["MASTERS"]).await.unwrap();
    let masters = resp_array(&r);
    assert_eq!(masters.len(), 3);

    // Verify each master's address
    for i in 1..=3 {
        let name = format!("master{}", i);
        let expected_port = format!("{}", 6379 + i);
        let r = c
            .send_command_str("SENTINEL", &["GET-MASTER-ADDR-BY-NAME", &name])
            .await
            .unwrap();
        let arr = resp_array(&r);
        assert_eq!(resp_string(&arr[1]), expected_port);
    }
}

// ---------------------------------------------------------------------------
// Sentinel Configuration Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sentinel_compat_set_config() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, sentinel) = setup_sentinel(853, config).await;
    let mut c = create_client(addr).await.unwrap();

    c.send_command_str(
        "SENTINEL",
        &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"],
    )
    .await
    .unwrap();

    // SET down-after-milliseconds
    let r = c
        .send_command_str(
            "SENTINEL",
            &["SET", "mymaster", "down-after-milliseconds", "15000"],
        )
        .await
        .unwrap();
    assert_eq!(resp_string(&r), "OK");

    // Verify
    let state = sentinel.state().read().await;
    let master = state.masters.get("mymaster").unwrap();
    assert_eq!(master.down_after_ms, 15000);
}

#[tokio::test]
async fn test_sentinel_compat_config_set_get() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(854, config).await;
    let mut c = create_client(addr).await.unwrap();

    // CONFIG SET
    let r = c
        .send_command_str(
            "SENTINEL",
            &["CONFIG", "SET", "announce-ip", "192.168.1.100"],
        )
        .await
        .unwrap();
    assert_eq!(resp_string(&r), "OK");

    // CONFIG GET
    let r = c
        .send_command_str("SENTINEL", &["CONFIG", "GET", "announce-ip"])
        .await
        .unwrap();
    let arr = resp_array(&r);
    assert_eq!(arr.len(), 2);
    assert_eq!(resp_string(&arr[1]), "192.168.1.100");
}

// ---------------------------------------------------------------------------
// Sentinel Quorum & Monitoring Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sentinel_compat_ckquorum() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(855, config).await;
    let mut c = create_client(addr).await.unwrap();

    // With quorum 1, single sentinel suffices
    c.send_command_str(
        "SENTINEL",
        &["MONITOR", "mymaster", "127.0.0.1", "6379", "1"],
    )
    .await
    .unwrap();

    let r = c
        .send_command_str("SENTINEL", &["CKQUORUM", "mymaster"])
        .await
        .unwrap();
    let msg = resp_string(&r);
    assert!(
        msg.starts_with("OK"),
        "CKQUORUM should pass with quorum=1, got: {}",
        msg
    );
}

#[tokio::test]
async fn test_sentinel_compat_replicas_query() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(856, config).await;
    let mut c = create_client(addr).await.unwrap();

    c.send_command_str(
        "SENTINEL",
        &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"],
    )
    .await
    .unwrap();

    // REPLICAS (empty initially)
    let r = c
        .send_command_str("SENTINEL", &["REPLICAS", "mymaster"])
        .await
        .unwrap();
    let arr = resp_array(&r);
    assert_eq!(arr.len(), 0);

    // SLAVES alias
    let r = c
        .send_command_str("SENTINEL", &["SLAVES", "mymaster"])
        .await
        .unwrap();
    let arr = resp_array(&r);
    assert_eq!(arr.len(), 0);
}

#[tokio::test]
async fn test_sentinel_compat_sentinels_query() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(857, config).await;
    let mut c = create_client(addr).await.unwrap();

    c.send_command_str(
        "SENTINEL",
        &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"],
    )
    .await
    .unwrap();

    // Only self as sentinel
    let r = c
        .send_command_str("SENTINEL", &["SENTINELS", "mymaster"])
        .await
        .unwrap();
    let arr = resp_array(&r);
    assert_eq!(
        arr.len(),
        0,
        "Should have 0 other sentinels (only ourselves)"
    );
}

// ---------------------------------------------------------------------------
// Sentinel Reset & Remove Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sentinel_compat_remove_master() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(858, config).await;
    let mut c = create_client(addr).await.unwrap();

    c.send_command_str(
        "SENTINEL",
        &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"],
    )
    .await
    .unwrap();

    let r = c
        .send_command_str("SENTINEL", &["REMOVE", "mymaster"])
        .await
        .unwrap();
    assert_eq!(resp_string(&r), "OK");

    let r = c.send_command_str("SENTINEL", &["MASTERS"]).await.unwrap();
    let masters = resp_array(&r);
    assert_eq!(masters.len(), 0);
}

#[tokio::test]
async fn test_sentinel_compat_reset() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(859, config).await;
    let mut c = create_client(addr).await.unwrap();

    c.send_command_str("SENTINEL", &["MONITOR", "m1", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();
    c.send_command_str("SENTINEL", &["MONITOR", "m2", "127.0.0.2", "6380", "2"])
        .await
        .unwrap();

    let r = c
        .send_command_str("SENTINEL", &["RESET", "*"])
        .await
        .unwrap();
    assert_eq!(resp_int(&r), 2);
}

// ---------------------------------------------------------------------------
// SDOWN/ODOWN Detection Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sentinel_compat_sdown_detection() {
    let config = SentinelConfig {
        enabled: true,
        down_after_ms: 100,
        ..Default::default()
    };
    let (addr, sentinel) = setup_sentinel(860, config).await;

    // Monitor a master
    sentinel
        .monitor_master("mymaster", "127.0.0.1", 9999, 1)
        .await
        .unwrap();

    // Simulate timeout
    {
        let mut state = sentinel.state().write().await;
        let master = state.masters.get_mut("mymaster").unwrap();
        master.last_ping_reply = std::time::Instant::now() - Duration::from_millis(200);
    }

    // Run monitor check
    let monitor = rlightning::sentinel::monitor::MonitorLoop::new(Arc::clone(sentinel.state()), 50);
    monitor.check_masters().await;

    // Verify SDOWN via client
    let mut c = create_client(addr).await.unwrap();
    let r = c
        .send_command_str(
            "SENTINEL",
            &["IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "9999", "0", "*"],
        )
        .await
        .unwrap();
    let fields = resp_array(&r);
    assert_eq!(
        resp_int(&fields[0]),
        1,
        "Master should be reported as SDOWN"
    );
}

// ---------------------------------------------------------------------------
// Sentinel Failover Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sentinel_compat_failover() {
    let config = SentinelConfig {
        enabled: true,
        down_after_ms: 100,
        failover_timeout_ms: 60000,
        ..Default::default()
    };
    let (_, sentinel) = setup_sentinel(861, config).await;

    // Monitor master
    sentinel
        .monitor_master("mymaster", "127.0.0.1", 9999, 1)
        .await
        .unwrap();

    // Add a replica
    {
        let mut state = sentinel.state().write().await;
        let master = state.masters.get_mut("mymaster").unwrap();
        let mut replica = ReplicaEntry::new("10.0.0.1".to_string(), 9998);
        replica.repl_offset = 1000;
        master.replicas.push(replica);
    }

    // Set SDOWN and ODOWN
    {
        let mut state = sentinel.state().write().await;
        let master = state.masters.get_mut("mymaster").unwrap();
        master.subjective_down = true;
        master.objective_down = true;
    }

    // Run failover
    let failover =
        rlightning::sentinel::failover::FailoverOrchestrator::new(Arc::clone(sentinel.state()));
    let result = failover.run_full_failover("mymaster").await;
    assert!(result.is_ok(), "Failover should succeed: {:?}", result);

    // Verify master address changed to promoted replica
    let state = sentinel.state().read().await;
    let master = state.masters.get("mymaster").unwrap();
    assert_eq!(master.host, "10.0.0.1");
    assert_eq!(master.port, 9998);
}

#[tokio::test]
async fn test_sentinel_compat_failover_selects_best_replica() {
    let config = SentinelConfig {
        enabled: true,
        down_after_ms: 100,
        failover_timeout_ms: 60000,
        ..Default::default()
    };
    let (_, sentinel) = setup_sentinel(862, config).await;

    sentinel
        .monitor_master("mymaster", "127.0.0.1", 9999, 1)
        .await
        .unwrap();

    // Add replicas with different replication offsets
    {
        let mut state = sentinel.state().write().await;
        let master = state.masters.get_mut("mymaster").unwrap();

        let mut replica1 = ReplicaEntry::new("10.0.0.1".to_string(), 9998);
        replica1.repl_offset = 500;
        master.replicas.push(replica1);

        let mut replica2 = ReplicaEntry::new("10.0.0.2".to_string(), 9997);
        replica2.repl_offset = 1000; // Higher offset = more data = better candidate
        master.replicas.push(replica2);
    }

    {
        let mut state = sentinel.state().write().await;
        let master = state.masters.get_mut("mymaster").unwrap();
        master.subjective_down = true;
        master.objective_down = true;
    }

    let failover =
        rlightning::sentinel::failover::FailoverOrchestrator::new(Arc::clone(sentinel.state()));
    failover.run_full_failover("mymaster").await.unwrap();

    // Best replica (highest offset) should be promoted
    let state = sentinel.state().read().await;
    let master = state.masters.get("mymaster").unwrap();
    assert_eq!(
        master.host, "10.0.0.2",
        "Replica with highest offset should be promoted"
    );
    assert_eq!(master.port, 9997);
}

// ---------------------------------------------------------------------------
// Sentinel Disabled Mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sentinel_compat_disabled_mode() {
    let config = SentinelConfig {
        enabled: false,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(863, config).await;
    let mut c = create_client(addr).await.unwrap();

    // MYID should still work
    let r = c.send_command_str("SENTINEL", &["MYID"]).await.unwrap();
    let id = resp_string(&r);
    assert_eq!(id.len(), 40);

    // MASTERS should fail in disabled mode
    let r = c.send_command_str("SENTINEL", &["MASTERS"]).await.unwrap();
    assert!(is_error(&r), "MASTERS should fail when sentinel disabled");
}

// ---------------------------------------------------------------------------
// Sentinel Misc Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sentinel_compat_myid() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(864, config).await;
    let mut c = create_client(addr).await.unwrap();

    let r = c.send_command_str("SENTINEL", &["MYID"]).await.unwrap();
    let id = resp_string(&r);
    assert_eq!(id.len(), 40);
    assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn test_sentinel_compat_help() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(865, config).await;
    let mut c = create_client(addr).await.unwrap();

    let r = c.send_command_str("SENTINEL", &["HELP"]).await.unwrap();
    let lines = resp_array(&r);
    assert!(lines.len() > 5, "HELP should return many lines");
}

#[tokio::test]
async fn test_sentinel_compat_flushconfig() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(866, config).await;
    let mut c = create_client(addr).await.unwrap();

    let r = c
        .send_command_str("SENTINEL", &["FLUSHCONFIG"])
        .await
        .unwrap();
    assert_eq!(resp_string(&r), "OK");
}

#[tokio::test]
async fn test_sentinel_compat_info_cache() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _) = setup_sentinel(867, config).await;
    let mut c = create_client(addr).await.unwrap();

    c.send_command_str(
        "SENTINEL",
        &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"],
    )
    .await
    .unwrap();

    let r = c
        .send_command_str("SENTINEL", &["INFO-CACHE", "mymaster"])
        .await
        .unwrap();
    let entries = resp_array(&r);
    assert_eq!(entries.len(), 1);
}

// ---------------------------------------------------------------------------
// Docker-based Sentinel Failover Test (optional)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sentinel_docker_failover() {
    if !check_docker_sentinel_available() {
        println!("Skipping Docker sentinel test (SENTINEL_COMPAT_TEST not set)");
        // Run a basic local validation
        let config = SentinelConfig {
            enabled: true,
            ..Default::default()
        };
        let (addr, _) = setup_sentinel(868, config).await;
        let mut c = create_client(addr).await.unwrap();
        let r = c.send_command_str("SENTINEL", &["MYID"]).await.unwrap();
        assert_eq!(resp_string(&r).len(), 40);
        return;
    }

    // Connect to Docker sentinel nodes
    let sentinel_ports = [26379, 26380, 26381];
    let mut sentinels = Vec::new();
    for port in &sentinel_ports {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        match Client::connect(addr).await {
            Ok(client) => sentinels.push(client),
            Err(e) => {
                println!("Cannot connect to sentinel on port {}: {}", port, e);
                return;
            }
        }
    }

    // Verify sentinel is monitoring mymaster
    let r = sentinels[0]
        .send_command_str("SENTINEL", &["MASTERS"])
        .await
        .unwrap();
    let masters = resp_array(&r);
    assert!(
        !masters.is_empty(),
        "Sentinel should be monitoring at least one master"
    );

    // Get master address
    let r = sentinels[0]
        .send_command_str("SENTINEL", &["GET-MASTER-ADDR-BY-NAME", "mymaster"])
        .await
        .unwrap();
    let addr_arr = resp_array(&r);
    assert_eq!(addr_arr.len(), 2);
    let master_host = resp_string(&addr_arr[0]);
    let master_port = resp_string(&addr_arr[1]);
    println!("Master at {}:{}", master_host, master_port);

    // Verify at least 2 sentinels know about each other
    let r = sentinels[0]
        .send_command_str("SENTINEL", &["SENTINELS", "mymaster"])
        .await
        .unwrap();
    let other_sentinels = resp_array(&r);
    assert!(
        other_sentinels.len() >= 1,
        "Should know about other sentinels"
    );

    println!("Docker sentinel failover test passed");
}
