use std::sync::Arc;
use std::time::Instant;

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::{FailoverState, MonitoredMaster, SentinelState};

/// Orchestrates failover process for a monitored master.
/// This implements the sentinel failover state machine.
pub struct FailoverOrchestrator {
    state: Arc<RwLock<SentinelState>>,
}

impl FailoverOrchestrator {
    pub fn new(state: Arc<RwLock<SentinelState>>) -> Self {
        FailoverOrchestrator { state }
    }

    /// Advance the failover state machine for all masters
    pub async fn check_failovers(&self) {
        let mut state = self.state.write().await;
        let master_names: Vec<String> = state.masters.keys().cloned().collect();

        for name in master_names {
            // First, check if we need to start a new failover (requires epoch increment)
            let needs_failover_start = state
                .masters
                .get(&name)
                .map(|m| {
                    m.failover_state == FailoverState::None
                        && m.objective_down
                        && m.failover_start_time.is_none()
                })
                .unwrap_or(false);

            if needs_failover_start {
                state.current_epoch += 1;
                let new_epoch = state.current_epoch;
                if let Some(master) = state.masters.get_mut(&name) {
                    master.failover_state = FailoverState::WaitStart;
                    master.failover_start_time = Some(Instant::now());
                    master.failover_epoch = new_epoch;
                    info!(
                        "Sentinel starting failover for master '{}' epoch {}",
                        name, new_epoch
                    );
                }
                continue;
            }

            if let Some(master) = state.masters.get_mut(&name) {
                match master.failover_state {
                    FailoverState::None => {
                        // Already handled above
                    }
                    FailoverState::WaitStart => {
                        master.failover_state = FailoverState::SelectReplica;
                        debug!("Failover for '{}': selecting replica", name);
                    }
                    FailoverState::SelectReplica => {
                        if let Some(best) = master.select_best_replica().cloned() {
                            info!(
                                "Sentinel selected replica {}:{} for promotion (master '{}')",
                                best.host, best.port, name
                            );
                            master.promoted_replica = Some(best);
                            master.failover_state = FailoverState::SendSlaveofNoOne;
                        } else {
                            warn!(
                                "Sentinel failover aborted for '{}': no suitable replica found",
                                name
                            );
                            Self::abort_failover(master);
                        }
                    }
                    FailoverState::SendSlaveofNoOne => {
                        if let Some(ref replica) = master.promoted_replica {
                            info!(
                                "Sentinel sending SLAVEOF NO ONE to {}:{} (master '{}')",
                                replica.host, replica.port, name
                            );
                        }
                        master.failover_state = FailoverState::WaitPromotion;
                    }
                    FailoverState::WaitPromotion => {
                        if let Some(ref promoted) = master.promoted_replica {
                            info!(
                                "Sentinel +promoted-slave {}:{} is now master (was master of '{}')",
                                promoted.host, promoted.port, name
                            );
                            master.host = promoted.host.clone();
                            master.port = promoted.port;
                        }
                        master.failover_state = FailoverState::ReconfSlaves;
                    }
                    FailoverState::ReconfSlaves => {
                        if let Some(ref promoted) = master.promoted_replica {
                            info!(
                                "Sentinel reconfiguring replicas to follow new master {}:{} (master '{}')",
                                promoted.host, promoted.port, name
                            );
                        }
                        if let Some(ref promoted) = master.promoted_replica {
                            let promoted_addr = promoted.addr();
                            master.replicas.retain(|r| r.addr() != promoted_addr);
                        }
                        master.failover_state = FailoverState::Done;
                    }
                    FailoverState::Done => {
                        info!(
                            "Sentinel +failover-end master '{}' failover completed successfully",
                            name
                        );
                        master.failover_state = FailoverState::None;
                        master.failover_start_time = None;
                        master.promoted_replica = None;
                        master.subjective_down = false;
                        master.objective_down = false;
                        master.status = super::MasterStatus::Up;
                        master.config_epoch = master.failover_epoch;
                        master.last_ping_reply = Instant::now();
                        master.last_ok_ping = Instant::now();
                    }
                }

                // Check for failover timeout
                if master.failover_state != FailoverState::None
                    && master.failover_state != FailoverState::Done
                    && let Some(start_time) = master.failover_start_time
                {
                    let elapsed = start_time.elapsed().as_millis() as u64;
                    if elapsed > master.failover_timeout_ms {
                        warn!(
                            "Sentinel failover timeout for master '{}' ({}ms > {}ms)",
                            name, elapsed, master.failover_timeout_ms
                        );
                        Self::abort_failover(master);
                    }
                }
            }
        }
    }

    /// Abort a failover in progress
    fn abort_failover(master: &mut MonitoredMaster) {
        info!("Sentinel -failover-abort master '{}'", master.name);
        master.failover_state = FailoverState::None;
        // Keep failover_start_time set to prevent immediate re-trigger
        // It will be cleared when the master comes back up
        master.promoted_replica = None;
    }

    /// Run a complete failover cycle (for testing - advances through all states)
    pub async fn run_full_failover(&self, master_name: &str) -> Result<(), String> {
        // Advance through each state
        for _ in 0..10 {
            self.check_failovers().await;
            let state = self.state.read().await;
            if let Some(master) = state.masters.get(master_name) {
                if master.failover_state == FailoverState::None
                    && master.failover_start_time.is_none()
                {
                    // Either not started or completed
                    if master.config_epoch > 0 {
                        return Ok(()); // Successfully completed
                    }
                }
            } else {
                return Err(format!("Master '{}' not found", master_name));
            }
        }
        Err("Failover did not complete within expected iterations".to_string())
    }

    /// Check if a failover is in progress for a master
    pub async fn is_failover_in_progress(&self, master_name: &str) -> bool {
        let state = self.state.read().await;
        state
            .masters
            .get(master_name)
            .map(|m| m.failover_state != FailoverState::None)
            .unwrap_or(false)
    }

    /// Get the current failover state for a master
    pub async fn get_failover_state(&self, master_name: &str) -> Option<FailoverState> {
        let state = self.state.read().await;
        state.masters.get(master_name).map(|m| m.failover_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sentinel::{MasterStatus, ReplicaEntry, SentinelConfig, SentinelManager};
    use crate::storage::engine::{StorageConfig, StorageEngine};
    use std::time::Duration;

    fn create_test_failover() -> (Arc<SentinelManager>, FailoverOrchestrator) {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let sentinel_config = SentinelConfig {
            enabled: true,
            down_after_ms: 100,
            failover_timeout_ms: 60000,
            ..Default::default()
        };
        let mgr = SentinelManager::new(Arc::clone(&storage), sentinel_config);
        let failover = FailoverOrchestrator::new(Arc::clone(mgr.state()));
        (mgr, failover)
    }

    #[tokio::test]
    async fn test_failover_from_odown() {
        let (mgr, failover) = create_test_failover();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 1)
            .await
            .unwrap();

        // Add a replica
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            let mut replica = ReplicaEntry::new("10.0.0.1".to_string(), 6380);
            replica.repl_offset = 1000;
            master.replicas.push(replica);

            // Set ODOWN
            master.subjective_down = true;
            master.objective_down = true;
            master.status = MasterStatus::ObjectiveDown;
        }

        // Run failover through all states
        let result = failover.run_full_failover("mymaster").await;
        assert!(result.is_ok());

        // Verify master address changed to promoted replica
        let state = mgr.state().read().await;
        let master = state.masters.get("mymaster").unwrap();
        assert_eq!(master.host, "10.0.0.1");
        assert_eq!(master.port, 6380);
        assert_eq!(master.failover_state, FailoverState::None);
        assert_eq!(master.status, MasterStatus::Up);
    }

    #[tokio::test]
    async fn test_failover_no_replicas_aborts() {
        let (mgr, failover) = create_test_failover();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 1)
            .await
            .unwrap();

        // Set ODOWN without replicas
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            master.subjective_down = true;
            master.objective_down = true;
            master.status = MasterStatus::ObjectiveDown;
        }

        // Advance through states
        for _ in 0..5 {
            failover.check_failovers().await;
        }

        // Should abort because no replicas available
        let state = mgr.state().read().await;
        let master = state.masters.get("mymaster").unwrap();
        assert_eq!(master.failover_state, FailoverState::None);
        // Master address should not have changed
        assert_eq!(master.host, "127.0.0.1");
        assert_eq!(master.port, 6379);
    }

    #[tokio::test]
    async fn test_failover_timeout() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let sentinel_config = SentinelConfig {
            enabled: true,
            down_after_ms: 100,
            failover_timeout_ms: 1, // Very short timeout
            ..Default::default()
        };
        let mgr = SentinelManager::new(Arc::clone(&storage), sentinel_config);
        let failover = FailoverOrchestrator::new(Arc::clone(mgr.state()));

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 1)
            .await
            .unwrap();

        // Manually put into WaitStart with start time in the past
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            let mut replica = ReplicaEntry::new("10.0.0.1".to_string(), 6380);
            replica.repl_offset = 1000;
            master.replicas.push(replica);
            master.failover_state = FailoverState::WaitStart;
            master.failover_start_time = Some(Instant::now() - Duration::from_millis(100));
        }

        failover.check_failovers().await;

        let state = mgr.state().read().await;
        let master = state.masters.get("mymaster").unwrap();
        assert_eq!(master.failover_state, FailoverState::None);
    }

    #[tokio::test]
    async fn test_failover_state_progression() {
        let (mgr, failover) = create_test_failover();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 1)
            .await
            .unwrap();

        // Add replicas
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            master
                .replicas
                .push(ReplicaEntry::new("10.0.0.1".to_string(), 6380));
            master.subjective_down = true;
            master.objective_down = true;
        }

        // Check state progression
        // None -> WaitStart
        failover.check_failovers().await;
        assert_eq!(
            failover.get_failover_state("mymaster").await,
            Some(FailoverState::WaitStart)
        );

        // WaitStart -> SelectReplica
        failover.check_failovers().await;
        assert_eq!(
            failover.get_failover_state("mymaster").await,
            Some(FailoverState::SelectReplica)
        );

        // SelectReplica -> SendSlaveofNoOne
        failover.check_failovers().await;
        assert_eq!(
            failover.get_failover_state("mymaster").await,
            Some(FailoverState::SendSlaveofNoOne)
        );

        // SendSlaveofNoOne -> WaitPromotion
        failover.check_failovers().await;
        assert_eq!(
            failover.get_failover_state("mymaster").await,
            Some(FailoverState::WaitPromotion)
        );

        // WaitPromotion -> ReconfSlaves
        failover.check_failovers().await;
        assert_eq!(
            failover.get_failover_state("mymaster").await,
            Some(FailoverState::ReconfSlaves)
        );

        // ReconfSlaves -> Done
        failover.check_failovers().await;
        assert_eq!(
            failover.get_failover_state("mymaster").await,
            Some(FailoverState::Done)
        );

        // Done -> None (reset)
        failover.check_failovers().await;
        assert_eq!(
            failover.get_failover_state("mymaster").await,
            Some(FailoverState::None)
        );
    }

    #[tokio::test]
    async fn test_is_failover_in_progress() {
        let (mgr, failover) = create_test_failover();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 1)
            .await
            .unwrap();

        assert!(!failover.is_failover_in_progress("mymaster").await);

        // Set WaitStart
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            master.failover_state = FailoverState::WaitStart;
        }

        assert!(failover.is_failover_in_progress("mymaster").await);
    }

    #[tokio::test]
    async fn test_failover_promoted_replica_removed_from_list() {
        let (mgr, failover) = create_test_failover();

        mgr.monitor_master("mymaster", "127.0.0.1", 6379, 1)
            .await
            .unwrap();

        // Add two replicas
        {
            let mut state = mgr.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            let mut r1 = ReplicaEntry::new("10.0.0.1".to_string(), 6380);
            r1.repl_offset = 2000;
            r1.slave_priority = 100;
            let mut r2 = ReplicaEntry::new("10.0.0.2".to_string(), 6381);
            r2.repl_offset = 1000;
            r2.slave_priority = 100;
            master.replicas.push(r1);
            master.replicas.push(r2);

            // Set ODOWN
            master.subjective_down = true;
            master.objective_down = true;
        }

        // Run full failover
        failover.run_full_failover("mymaster").await.unwrap();

        // After failover, only 1 replica should remain (the non-promoted one)
        let state = mgr.state().read().await;
        let master = state.masters.get("mymaster").unwrap();
        assert_eq!(master.replicas.len(), 1);
    }
}
