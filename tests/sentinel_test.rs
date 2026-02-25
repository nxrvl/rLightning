/// Integration tests for Redis Sentinel support
///
/// Tests the SENTINEL command through a running server instance,
/// verifying monitoring, failover, SDOWN/ODOWN detection,
/// and configuration management.

mod test_utils;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::sentinel::{SentinelConfig, SentinelManager};
use rlightning::storage::engine::{StorageConfig, StorageEngine};

use test_utils::{DEFAULT_TEST_PORT, create_client};

/// Set up a test server with sentinel mode enabled
async fn setup_sentinel_server(
    port_offset: u16,
    sentinel_config: SentinelConfig,
) -> Result<(SocketAddr, Arc<SentinelManager>), Box<dyn std::error::Error + Send + Sync>> {
    let port = DEFAULT_TEST_PORT + port_offset;
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    let storage = StorageEngine::new(StorageConfig::default());

    let sentinel = SentinelManager::new(Arc::clone(&storage), sentinel_config);
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

    Ok((addr, sentinel))
}

/// Helper to extract string from RespValue
fn resp_string(val: &RespValue) -> String {
    match val {
        RespValue::SimpleString(s) => s.clone(),
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => panic!("Expected string, got: {:?}", val),
    }
}

/// Helper to extract integer from RespValue
fn resp_int(val: &RespValue) -> i64 {
    match val {
        RespValue::Integer(i) => *i,
        _ => panic!("Expected integer, got: {:?}", val),
    }
}

/// Helper to extract array from RespValue
fn resp_array(val: &RespValue) -> &Vec<RespValue> {
    match val {
        RespValue::Array(Some(arr)) => arr,
        _ => panic!("Expected array, got: {:?}", val),
    }
}

/// Helper to check if response is an error
fn is_error(val: &RespValue) -> bool {
    matches!(val, RespValue::Error(_))
}

/// Helper to check if response is null
fn is_null(val: &RespValue) -> bool {
    matches!(val, RespValue::Null | RespValue::BulkString(None) | RespValue::Array(None))
}

#[tokio::test]
async fn test_sentinel_myid_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(800, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    let response = client.send_command_str("SENTINEL", &["MYID"]).await.unwrap();
    let id = resp_string(&response);
    assert_eq!(id.len(), 40, "Sentinel ID should be 40 chars, got: {}", id);
}

#[tokio::test]
async fn test_sentinel_monitor_and_masters_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(801, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor a master
    let response = client
        .send_command_str("SENTINEL", &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();
    assert_eq!(resp_string(&response), "OK");

    // List masters
    let response = client
        .send_command_str("SENTINEL", &["MASTERS"])
        .await
        .unwrap();
    let masters = resp_array(&response);
    assert_eq!(masters.len(), 1);
}

#[tokio::test]
async fn test_sentinel_master_info_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(802, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor a master
    client
        .send_command_str("SENTINEL", &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();

    // Get master info
    let response = client
        .send_command_str("SENTINEL", &["MASTER", "mymaster"])
        .await
        .unwrap();
    let fields = resp_array(&response);
    assert!(fields.len() > 10, "Master info should have many fields");

    // Find the "name" field
    let mut found_name = false;
    for i in (0..fields.len()).step_by(2) {
        if resp_string(&fields[i]) == "name" {
            assert_eq!(resp_string(&fields[i + 1]), "mymaster");
            found_name = true;
            break;
        }
    }
    assert!(found_name, "Should find 'name' field in master info");
}

#[tokio::test]
async fn test_sentinel_get_master_addr_by_name_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(803, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor a master
    client
        .send_command_str("SENTINEL", &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();

    // Get master address
    let response = client
        .send_command_str("SENTINEL", &["GET-MASTER-ADDR-BY-NAME", "mymaster"])
        .await
        .unwrap();
    let addr_arr = resp_array(&response);
    assert_eq!(addr_arr.len(), 2);
    assert_eq!(resp_string(&addr_arr[0]), "127.0.0.1");
    assert_eq!(resp_string(&addr_arr[1]), "6379");
}

#[tokio::test]
async fn test_sentinel_get_master_addr_nonexistent_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(804, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Get address for nonexistent master - should return null
    let response = client
        .send_command_str("SENTINEL", &["GET-MASTER-ADDR-BY-NAME", "nonexistent"])
        .await
        .unwrap();
    assert!(is_null(&response), "Should return null for nonexistent master, got: {:?}", response);
}

#[tokio::test]
async fn test_sentinel_remove_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(805, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor and then remove
    client
        .send_command_str("SENTINEL", &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();

    let response = client
        .send_command_str("SENTINEL", &["REMOVE", "mymaster"])
        .await
        .unwrap();
    assert_eq!(resp_string(&response), "OK");

    // Should have no masters now
    let response = client
        .send_command_str("SENTINEL", &["MASTERS"])
        .await
        .unwrap();
    let masters = resp_array(&response);
    assert_eq!(masters.len(), 0);
}

#[tokio::test]
async fn test_sentinel_set_options_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, sentinel) = setup_sentinel_server(806, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor
    client
        .send_command_str("SENTINEL", &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();

    // Set down-after-milliseconds
    let response = client
        .send_command_str("SENTINEL", &["SET", "mymaster", "down-after-milliseconds", "10000"])
        .await
        .unwrap();
    assert_eq!(resp_string(&response), "OK");

    // Verify via the sentinel state
    let state = sentinel.state().read().await;
    let master = state.masters.get("mymaster").unwrap();
    assert_eq!(master.down_after_ms, 10000);
}

#[tokio::test]
async fn test_sentinel_ckquorum_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(807, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor with quorum 1 (we alone are enough)
    client
        .send_command_str("SENTINEL", &["MONITOR", "mymaster", "127.0.0.1", "6379", "1"])
        .await
        .unwrap();

    let response = client
        .send_command_str("SENTINEL", &["CKQUORUM", "mymaster"])
        .await
        .unwrap();
    let msg = resp_string(&response);
    assert!(msg.starts_with("OK"), "CKQUORUM should succeed: {}", msg);
}

#[tokio::test]
async fn test_sentinel_replicas_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(808, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor
    client
        .send_command_str("SENTINEL", &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();

    // Get replicas (should be empty)
    let response = client
        .send_command_str("SENTINEL", &["REPLICAS", "mymaster"])
        .await
        .unwrap();
    let replicas = resp_array(&response);
    assert_eq!(replicas.len(), 0);

    // SLAVES alias should also work
    let response = client
        .send_command_str("SENTINEL", &["SLAVES", "mymaster"])
        .await
        .unwrap();
    let replicas = resp_array(&response);
    assert_eq!(replicas.len(), 0);
}

#[tokio::test]
async fn test_sentinel_sentinels_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(809, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor
    client
        .send_command_str("SENTINEL", &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();

    // Get sentinels (should be empty - only ourselves)
    let response = client
        .send_command_str("SENTINEL", &["SENTINELS", "mymaster"])
        .await
        .unwrap();
    let sentinels = resp_array(&response);
    assert_eq!(sentinels.len(), 0);
}

#[tokio::test]
async fn test_sentinel_reset_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(810, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor two masters
    client
        .send_command_str("SENTINEL", &["MONITOR", "master1", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();
    client
        .send_command_str("SENTINEL", &["MONITOR", "master2", "127.0.0.2", "6380", "2"])
        .await
        .unwrap();

    // Reset all
    let response = client
        .send_command_str("SENTINEL", &["RESET", "*"])
        .await
        .unwrap();
    assert_eq!(resp_int(&response), 2);
}

#[tokio::test]
async fn test_sentinel_is_master_down_by_addr_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(811, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor
    client
        .send_command_str("SENTINEL", &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();

    // Query master down status
    let response = client
        .send_command_str("SENTINEL", &["IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "6379", "0", "*"])
        .await
        .unwrap();
    let fields = resp_array(&response);
    assert_eq!(fields.len(), 3);
    // Master should not be down
    assert_eq!(resp_int(&fields[0]), 0);
}

#[tokio::test]
async fn test_sentinel_config_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(812, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Set announce-ip
    let response = client
        .send_command_str("SENTINEL", &["CONFIG", "SET", "announce-ip", "10.0.0.1"])
        .await
        .unwrap();
    assert_eq!(resp_string(&response), "OK");

    // Get announce-ip
    let response = client
        .send_command_str("SENTINEL", &["CONFIG", "GET", "announce-ip"])
        .await
        .unwrap();
    let fields = resp_array(&response);
    assert_eq!(fields.len(), 2);
    assert_eq!(resp_string(&fields[1]), "10.0.0.1");
}

#[tokio::test]
async fn test_sentinel_help_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(813, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    let response = client
        .send_command_str("SENTINEL", &["HELP"])
        .await
        .unwrap();
    let lines = resp_array(&response);
    assert!(lines.len() > 5, "HELP should return multiple lines");
}

#[tokio::test]
async fn test_sentinel_flushconfig_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(814, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    let response = client
        .send_command_str("SENTINEL", &["FLUSHCONFIG"])
        .await
        .unwrap();
    assert_eq!(resp_string(&response), "OK");
}

#[tokio::test]
async fn test_sentinel_disabled_mode_via_client() {
    let config = SentinelConfig {
        enabled: false,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(815, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // MYID should still work in disabled mode
    let response = client
        .send_command_str("SENTINEL", &["MYID"])
        .await
        .unwrap();
    let id = resp_string(&response);
    assert_eq!(id.len(), 40);

    // MASTERS should fail in disabled mode
    let response = client
        .send_command_str("SENTINEL", &["MASTERS"])
        .await
        .unwrap();
    assert!(is_error(&response), "MASTERS should fail when sentinel disabled");
}

#[tokio::test]
async fn test_sentinel_sdown_detection_via_manager() {
    let config = SentinelConfig {
        enabled: true,
        down_after_ms: 100,
        ..Default::default()
    };
    let (addr, sentinel) = setup_sentinel_server(816, config).await.unwrap();

    // Monitor a master
    sentinel
        .monitor_master("mymaster", "127.0.0.1", 9999, 1)
        .await
        .unwrap();

    // Create monitor and simulate timeout
    let monitor = rlightning::sentinel::monitor::MonitorLoop::new(
        Arc::clone(sentinel.state()),
        50,
    );

    // Simulate timeout by setting last_ping_reply to the past
    {
        let mut state = sentinel.state().write().await;
        let master = state.masters.get_mut("mymaster").unwrap();
        master.last_ping_reply =
            std::time::Instant::now() - Duration::from_millis(200);
    }

    // Run monitor check
    monitor.check_masters().await;

    // Verify SDOWN
    let state = sentinel.state().read().await;
    let master = state.masters.get("mymaster").unwrap();
    assert!(
        master.subjective_down,
        "Master should be subjectively down after timeout"
    );

    // Verify via client
    let mut client = create_client(addr).await.unwrap();
    let response = client
        .send_command_str("SENTINEL", &["IS-MASTER-DOWN-BY-ADDR", "127.0.0.1", "9999", "0", "*"])
        .await
        .unwrap();
    let fields = resp_array(&response);
    assert_eq!(
        resp_int(&fields[0]),
        1,
        "Master should be reported as down"
    );
}

#[tokio::test]
async fn test_sentinel_failover_via_manager() {
    let config = SentinelConfig {
        enabled: true,
        down_after_ms: 100,
        failover_timeout_ms: 60000,
        ..Default::default()
    };
    let (_addr, sentinel) = setup_sentinel_server(817, config).await.unwrap();

    // Monitor a master
    sentinel
        .monitor_master("mymaster", "127.0.0.1", 9999, 1)
        .await
        .unwrap();

    // Add a replica
    {
        let mut state = sentinel.state().write().await;
        let master = state.masters.get_mut("mymaster").unwrap();
        let mut replica = rlightning::sentinel::ReplicaEntry::new(
            "10.0.0.1".to_string(),
            9998,
        );
        replica.repl_offset = 1000;
        master.replicas.push(replica);
    }

    // Set ODOWN
    {
        let mut state = sentinel.state().write().await;
        let master = state.masters.get_mut("mymaster").unwrap();
        master.subjective_down = true;
        master.objective_down = true;
    }

    // Run failover
    let failover = rlightning::sentinel::failover::FailoverOrchestrator::new(
        Arc::clone(sentinel.state()),
    );

    let result = failover.run_full_failover("mymaster").await;
    assert!(result.is_ok(), "Failover should succeed: {:?}", result);

    // Verify master address changed
    let state = sentinel.state().read().await;
    let master = state.masters.get("mymaster").unwrap();
    assert_eq!(master.host, "10.0.0.1");
    assert_eq!(master.port, 9998);
}

#[tokio::test]
async fn test_sentinel_multiple_masters_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(818, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor multiple masters
    for i in 1..=3 {
        let name = format!("master{}", i);
        let port = format!("{}", 6379 + i);
        client
            .send_command_str("SENTINEL", &["MONITOR", &name, "127.0.0.1", &port, "2"])
            .await
            .unwrap();
    }

    // List masters
    let response = client
        .send_command_str("SENTINEL", &["MASTERS"])
        .await
        .unwrap();
    let masters = resp_array(&response);
    assert_eq!(masters.len(), 3);

    // Get each master's address
    for i in 1..=3 {
        let name = format!("master{}", i);
        let expected_port = format!("{}", 6379 + i);
        let response = client
            .send_command_str("SENTINEL", &["GET-MASTER-ADDR-BY-NAME", &name])
            .await
            .unwrap();
        let addr_arr = resp_array(&response);
        assert_eq!(resp_string(&addr_arr[1]), expected_port);
    }
}

#[tokio::test]
async fn test_sentinel_info_cache_via_client() {
    let config = SentinelConfig {
        enabled: true,
        ..Default::default()
    };
    let (addr, _sentinel) = setup_sentinel_server(819, config).await.unwrap();
    let mut client = create_client(addr).await.unwrap();

    // Monitor
    client
        .send_command_str("SENTINEL", &["MONITOR", "mymaster", "127.0.0.1", "6379", "2"])
        .await
        .unwrap();

    // Get info cache
    let response = client
        .send_command_str("SENTINEL", &["INFO-CACHE", "mymaster"])
        .await
        .unwrap();
    let entries = resp_array(&response);
    assert_eq!(entries.len(), 1, "Should have one cache entry");
}
