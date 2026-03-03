use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, info};

use super::{SentinelPeer, SentinelState};

/// Monitoring loop for sentinel instances.
/// In production, this would run as a background task periodically pinging
/// monitored masters and their replicas, updating SDOWN/ODOWN states,
/// and publishing hello messages on the __sentinel__:hello channel.
pub struct MonitorLoop {
    state: Arc<RwLock<SentinelState>>,
    ping_period: Duration,
}

impl MonitorLoop {
    pub fn new(state: Arc<RwLock<SentinelState>>, ping_period_ms: u64) -> Self {
        MonitorLoop {
            state,
            ping_period: Duration::from_millis(ping_period_ms),
        }
    }

    /// Run the monitoring loop (called as a background task)
    /// This checks SDOWN/ODOWN states for all monitored masters
    pub async fn check_masters(&self) {
        let mut state = self.state.write().await;
        for (_name, master) in state.masters.iter_mut() {
            // Check subjective down
            let was_sdown = master.subjective_down;
            master.check_subjective_down();

            if master.subjective_down && !was_sdown {
                info!(
                    "Sentinel +sdown master {} {}:{}",
                    master.name, master.host, master.port
                );
            } else if !master.subjective_down && was_sdown {
                info!(
                    "Sentinel -sdown master {} {}:{}",
                    master.name, master.host, master.port
                );
            }

            // Check objective down
            let was_odown = master.objective_down;
            master.check_objective_down();

            if master.objective_down && !was_odown {
                info!(
                    "Sentinel +odown master {} {}:{} #quorum {}/{}",
                    master.name, master.host, master.port, master.odown_vote_count, master.quorum
                );
            } else if !master.objective_down && was_odown {
                info!(
                    "Sentinel -odown master {} {}:{}",
                    master.name, master.host, master.port
                );
            }

            // Check replica SDOWN status
            for replica in master.replicas.iter_mut() {
                let elapsed = replica.last_ping_reply.elapsed().as_millis() as u64;
                let was_sdown = replica.sdown;
                replica.sdown = elapsed > master.down_after_ms;

                if replica.sdown && !was_sdown {
                    info!(
                        "Sentinel +sdown slave {}:{} {} {}:{}",
                        replica.host, replica.port, master.name, master.host, master.port
                    );
                } else if !replica.sdown && was_sdown {
                    info!(
                        "Sentinel -sdown slave {}:{} {} {}:{}",
                        replica.host, replica.port, master.name, master.host, master.port
                    );
                }
            }

            // Check sentinel peer SDOWN status
            for sentinel in master.sentinels.iter_mut() {
                let elapsed = sentinel.last_hello.elapsed().as_millis() as u64;
                // Sentinels use hello messages; if no hello in 5x the down_after_ms, mark as sdown
                let sentinel_timeout = master.down_after_ms * 5;
                let was_sdown = sentinel.sdown;
                sentinel.sdown = elapsed > sentinel_timeout;

                if sentinel.sdown && !was_sdown {
                    info!(
                        "Sentinel +sdown sentinel {} {}:{}",
                        sentinel.id, sentinel.host, sentinel.port
                    );
                }
            }
        }
    }

    /// Process a hello message from another sentinel
    /// Format: sentinel_ip,sentinel_port,sentinel_runid,current_epoch,master_name,master_ip,master_port,master_config_epoch
    pub async fn process_hello_message(&self, message: &str) {
        let parts: Vec<&str> = message.split(',').collect();
        if parts.len() < 8 {
            debug!("Invalid sentinel hello message: {}", message);
            return;
        }

        let sentinel_ip = parts[0];
        let sentinel_port: u16 = match parts[1].parse() {
            Ok(p) => p,
            Err(_) => return,
        };
        let sentinel_runid = parts[2];
        let current_epoch: u64 = match parts[3].parse() {
            Ok(e) => e,
            Err(_) => return,
        };
        let master_name = parts[4];
        let _master_ip = parts[5];
        let _master_port: u16 = match parts[6].parse() {
            Ok(p) => p,
            Err(_) => return,
        };
        let _master_config_epoch: u64 = match parts[7].parse() {
            Ok(e) => e,
            Err(_) => return,
        };

        let mut state = self.state.write().await;

        // Don't process our own hello messages
        if sentinel_runid == state.my_id {
            return;
        }

        // Update epoch if the other sentinel has a higher one
        if current_epoch > state.current_epoch {
            state.current_epoch = current_epoch;
        }

        // Find the master and update sentinel info
        if let Some(master) = state.masters.get_mut(master_name) {
            // Find or create sentinel entry
            if let Some(sentinel) = master.sentinels.iter_mut().find(|s| s.id == sentinel_runid) {
                sentinel.last_hello = Instant::now();
                sentinel.host = sentinel_ip.to_string();
                sentinel.port = sentinel_port;
                sentinel.current_epoch = current_epoch;
                sentinel.sdown = false;
            } else {
                let peer = SentinelPeer::new(
                    sentinel_runid.to_string(),
                    sentinel_ip.to_string(),
                    sentinel_port,
                );
                master.sentinels.push(peer);
                info!(
                    "Sentinel +sentinel sentinel {} {}:{} @ {} {}:{}",
                    sentinel_runid,
                    sentinel_ip,
                    sentinel_port,
                    master.name,
                    master.host,
                    master.port
                );
            }
        }
    }

    /// Build a hello message for publishing
    pub async fn build_hello_message(&self, master_name: &str) -> Option<String> {
        let state = self.state.read().await;
        let master = state.masters.get(master_name)?;
        let my_ip = state.announce_ip.as_deref().unwrap_or("127.0.0.1");
        let my_port = state.announce_port.unwrap_or(26379);
        Some(format!(
            "{},{},{},{},{},{},{},{}",
            my_ip,
            my_port,
            state.my_id,
            state.current_epoch,
            master.name,
            master.host,
            master.port,
            master.config_epoch,
        ))
    }

    /// Simulate receiving a PING reply from a master (for testing)
    pub async fn simulate_ping_reply(&self, master_name: &str) {
        let mut state = self.state.write().await;
        if let Some(master) = state.masters.get_mut(master_name) {
            master.ping_reply_received();
        }
    }

    /// Simulate receiving a PING reply from a replica (for testing)
    pub async fn simulate_replica_ping_reply(&self, master_name: &str, replica_addr: &str) {
        let mut state = self.state.write().await;
        if let Some(master) = state.masters.get_mut(master_name) {
            for replica in master.replicas.iter_mut() {
                if replica.addr() == replica_addr {
                    replica.last_ping_reply = Instant::now();
                    replica.last_ok_ping = Instant::now();
                    replica.sdown = false;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sentinel::{
        MasterStatus, ReplicaEntry, SentinelConfig, SentinelManager, SentinelPeer,
    };
    use crate::storage::engine::{StorageConfig, StorageEngine};

    fn create_test_monitor() -> (Arc<SentinelManager>, MonitorLoop) {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let sentinel_config = SentinelConfig {
            enabled: true,
            down_after_ms: 100, // Very short for testing
            ping_period_ms: 50,
            ..Default::default()
        };
        let mgr = SentinelManager::new(Arc::clone(&storage), sentinel_config);
        let monitor = MonitorLoop::new(Arc::clone(mgr.state()), 50);
        (mgr, monitor)
    }

    #[tokio::test]
    async fn test_monitor_check_sdown() {
        let (mgr, monitor) = create_test_monitor();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 2)
            .await
            .unwrap();

        // Initially should be UP
        monitor.check_masters().await;
        {
            let state = mgr.state().read().await;
            let master = state.masters.get("mymaster").unwrap();
            assert!(!master.subjective_down);
        }

        // Simulate timeout
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            master.last_ping_reply = Instant::now() - Duration::from_millis(200);
        }

        monitor.check_masters().await;
        {
            let state = mgr.state().read().await;
            let master = state.masters.get("mymaster").unwrap();
            assert!(master.subjective_down);
            assert_eq!(master.status, MasterStatus::SubjectiveDown);
        }
    }

    #[tokio::test]
    async fn test_monitor_check_odown() {
        let (mgr, monitor) = create_test_monitor();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 2)
            .await
            .unwrap();

        // Add a sentinel peer that votes master is down
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            let mut peer = SentinelPeer::new("peer1".to_string(), "10.0.0.1".to_string(), 26379);
            peer.master_down_vote = true;
            master.sentinels.push(peer);

            // Simulate timeout
            master.last_ping_reply = Instant::now() - Duration::from_millis(200);
        }

        monitor.check_masters().await;
        {
            let state = mgr.state().read().await;
            let master = state.masters.get("mymaster").unwrap();
            assert!(master.subjective_down);
            assert!(master.objective_down);
            assert_eq!(master.status, MasterStatus::ObjectiveDown);
        }
    }

    #[tokio::test]
    async fn test_monitor_recovery() {
        let (mgr, monitor) = create_test_monitor();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 2)
            .await
            .unwrap();

        // Simulate timeout
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            master.last_ping_reply = Instant::now() - Duration::from_millis(200);
        }

        monitor.check_masters().await;
        {
            let state = mgr.state().read().await;
            assert!(state.masters.get("mymaster").unwrap().subjective_down);
        }

        // Simulate ping reply
        monitor.simulate_ping_reply("mymaster").await;

        monitor.check_masters().await;
        {
            let state = mgr.state().read().await;
            let master = state.masters.get("mymaster").unwrap();
            assert!(!master.subjective_down);
            assert_eq!(master.status, MasterStatus::Up);
        }
    }

    #[tokio::test]
    async fn test_monitor_replica_sdown() {
        let (mgr, monitor) = create_test_monitor();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 2)
            .await
            .unwrap();

        // Add a replica
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            let mut replica = ReplicaEntry::new("10.0.0.1".to_string(), 6380);
            replica.last_ping_reply = Instant::now() - Duration::from_millis(200);
            master.replicas.push(replica);
        }

        monitor.check_masters().await;
        {
            let state = mgr.state().read().await;
            let master = state.masters.get("mymaster").unwrap();
            assert!(master.replicas[0].sdown);
        }
    }

    #[tokio::test]
    async fn test_hello_message_processing() {
        let (mgr, monitor) = create_test_monitor();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 2)
            .await
            .unwrap();

        // Process a hello message from another sentinel
        let msg =
            "10.0.0.2,26379,abcdef0123456789abcdef0123456789abcdef01,1,mymaster,127.0.0.1,6379,0";
        monitor.process_hello_message(msg).await;

        // Should have added a sentinel peer
        let state = mgr.state().read().await;
        let master = state.masters.get("mymaster").unwrap();
        assert_eq!(master.sentinels.len(), 1);
        assert_eq!(master.sentinels[0].host, "10.0.0.2");
        assert_eq!(master.sentinels[0].port, 26379);
    }

    #[tokio::test]
    async fn test_hello_message_update_epoch() {
        let (mgr, monitor) = create_test_monitor();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 2)
            .await
            .unwrap();

        // Process a hello with higher epoch
        let msg =
            "10.0.0.2,26379,abcdef0123456789abcdef0123456789abcdef01,5,mymaster,127.0.0.1,6379,0";
        monitor.process_hello_message(msg).await;

        let state = mgr.state().read().await;
        assert_eq!(state.current_epoch, 5);
    }

    #[tokio::test]
    async fn test_hello_message_ignore_self() {
        let (mgr, monitor) = create_test_monitor();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 2)
            .await
            .unwrap();

        let my_id = mgr.state().read().await.my_id.clone();

        // Process a hello from ourselves
        let msg = format!("10.0.0.1,26379,{},0,mymaster,127.0.0.1,6379,0", my_id);
        monitor.process_hello_message(&msg).await;

        // Should NOT add ourselves as a peer
        let state = mgr.state().read().await;
        let master = state.masters.get("mymaster").unwrap();
        assert_eq!(master.sentinels.len(), 0);
    }

    #[tokio::test]
    async fn test_build_hello_message() {
        let (mgr, monitor) = create_test_monitor();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 2)
            .await
            .unwrap();

        let msg = monitor.build_hello_message("mymaster").await.unwrap();
        let parts: Vec<&str> = msg.split(',').collect();
        assert_eq!(parts.len(), 8);
        assert_eq!(parts[4], "mymaster");
        assert_eq!(parts[5], "127.0.0.1");
        assert_eq!(parts[6], "6379");
    }

    #[tokio::test]
    async fn test_sentinel_peer_sdown() {
        let (mgr, monitor) = create_test_monitor();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 2)
            .await
            .unwrap();

        // Add a sentinel with old hello
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            let mut peer = SentinelPeer::new("peer1".to_string(), "10.0.0.1".to_string(), 26379);
            // Set last_hello to far in the past (5x down_after_ms = 500ms for our 100ms test config)
            peer.last_hello = Instant::now() - Duration::from_millis(600);
            master.sentinels.push(peer);
        }

        monitor.check_masters().await;
        {
            let state = mgr.state().read().await;
            let master = state.masters.get("mymaster").unwrap();
            assert!(master.sentinels[0].sdown);
        }
    }
}
