pub mod monitor;
pub mod failover;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// Sentinel configuration
#[derive(Debug, Clone)]
pub struct SentinelConfig {
    /// Whether sentinel mode is enabled
    pub enabled: bool,
    /// Default down-after-milliseconds for monitored masters
    pub down_after_ms: u64,
    /// Default failover timeout in milliseconds
    pub failover_timeout_ms: u64,
    /// How often to send PING to monitored instances (milliseconds)
    pub ping_period_ms: u64,
    /// Parallel syncs during failover
    pub parallel_syncs: usize,
    /// Deny scripts during failover
    pub deny_scripts_reconfig: bool,
}

impl Default for SentinelConfig {
    fn default() -> Self {
        SentinelConfig {
            enabled: false,
            down_after_ms: 30000,
            failover_timeout_ms: 180000,
            ping_period_ms: 1000,
            parallel_syncs: 1,
            deny_scripts_reconfig: true,
        }
    }
}

/// Master status as seen by this sentinel
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterStatus {
    /// Master is up and responding
    Up,
    /// Subjectively down (this sentinel thinks it's down)
    SubjectiveDown,
    /// Objectively down (quorum of sentinels agree it's down)
    ObjectiveDown,
}

impl std::fmt::Display for MasterStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MasterStatus::Up => write!(f, "ok"),
            MasterStatus::SubjectiveDown => write!(f, "sdown"),
            MasterStatus::ObjectiveDown => write!(f, "odown"),
        }
    }
}

/// Failover state machine states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverState {
    /// No failover in progress
    None,
    /// Waiting to start failover
    WaitStart,
    /// Selecting the best replica to promote
    SelectReplica,
    /// Sending SLAVEOF NO ONE to selected replica
    SendSlaveofNoOne,
    /// Waiting for replica to become master
    WaitPromotion,
    /// Reconfiguring remaining replicas to point to new master
    ReconfSlaves,
    /// Failover completed
    Done,
}

impl std::fmt::Display for FailoverState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FailoverState::None => write!(f, "none"),
            FailoverState::WaitStart => write!(f, "wait-start"),
            FailoverState::SelectReplica => write!(f, "select-slave"),
            FailoverState::SendSlaveofNoOne => write!(f, "send-slaveof-noone"),
            FailoverState::WaitPromotion => write!(f, "wait-promotion"),
            FailoverState::ReconfSlaves => write!(f, "reconf-slaves"),
            FailoverState::Done => write!(f, "done"),
        }
    }
}

/// Information about a replica of a monitored master
#[derive(Debug, Clone)]
pub struct ReplicaEntry {
    /// Replica host
    pub host: String,
    /// Replica port
    pub port: u16,
    /// Replica's replication offset
    pub repl_offset: u64,
    /// Is this replica subjectively down?
    pub sdown: bool,
    /// Last PING response time
    pub last_ping_reply: Instant,
    /// Last successful PING time
    pub last_ok_ping: Instant,
    /// Info refresh timestamp
    pub info_refresh: Instant,
    /// Role reported by INFO
    pub role_reported: String,
    /// Slave priority (lower = preferred for promotion, 0 = never promote)
    pub slave_priority: u32,
}

impl ReplicaEntry {
    pub fn new(host: String, port: u16) -> Self {
        let now = Instant::now();
        ReplicaEntry {
            host,
            port,
            repl_offset: 0,
            sdown: false,
            last_ping_reply: now,
            last_ok_ping: now,
            info_refresh: now,
            role_reported: "slave".to_string(),
            slave_priority: 100,
        }
    }

    /// Get address as "host:port" string
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Information about another sentinel monitoring the same master
#[derive(Debug, Clone)]
pub struct SentinelPeer {
    /// Sentinel ID (40-char hex)
    pub id: String,
    /// Sentinel host
    pub host: String,
    /// Sentinel port
    pub port: u16,
    /// Last hello message time
    pub last_hello: Instant,
    /// Is this sentinel subjectively down?
    pub sdown: bool,
    /// Current epoch reported by this sentinel
    pub current_epoch: u64,
    /// Does this sentinel think the master is down?
    pub master_down_vote: bool,
    /// The leader epoch this sentinel voted for
    pub leader_epoch: u64,
    /// The leader ID this sentinel voted for
    pub leader_id: String,
}

impl SentinelPeer {
    pub fn new(id: String, host: String, port: u16) -> Self {
        SentinelPeer {
            id,
            host,
            port,
            last_hello: Instant::now(),
            sdown: false,
            current_epoch: 0,
            master_down_vote: false,
            leader_epoch: 0,
            leader_id: String::new(),
        }
    }
}

/// A monitored master instance
#[derive(Debug, Clone)]
pub struct MonitoredMaster {
    /// Master name (user-defined)
    pub name: String,
    /// Master host
    pub host: String,
    /// Master port
    pub port: u16,
    /// Quorum required for ODOWN
    pub quorum: usize,
    /// Current status
    pub status: MasterStatus,
    /// Last successful PING reply time
    pub last_ping_reply: Instant,
    /// Last time we sent a PING
    pub last_ping_sent: Instant,
    /// Last successful PING time
    pub last_ok_ping: Instant,
    /// Down after milliseconds for this master
    pub down_after_ms: u64,
    /// Failover timeout for this master
    pub failover_timeout_ms: u64,
    /// Parallel syncs during failover
    pub parallel_syncs: usize,
    /// Is subjectively down
    pub subjective_down: bool,
    /// Is objectively down
    pub objective_down: bool,
    /// Known replicas
    pub replicas: Vec<ReplicaEntry>,
    /// Known sentinels monitoring this master
    pub sentinels: Vec<SentinelPeer>,
    /// Current failover state
    pub failover_state: FailoverState,
    /// Failover epoch
    pub failover_epoch: u64,
    /// Failover start time
    pub failover_start_time: Option<Instant>,
    /// Selected replica for promotion during failover
    pub promoted_replica: Option<ReplicaEntry>,
    /// Config epoch for this master
    pub config_epoch: u64,
    /// Number of sentinels that agree master is down (for ODOWN calculation)
    pub odown_vote_count: usize,
    /// Info refresh time
    pub info_refresh: Instant,
    /// Auth password for connecting to this master
    pub auth_pass: Option<String>,
}

impl MonitoredMaster {
    pub fn new(name: String, host: String, port: u16, quorum: usize, config: &SentinelConfig) -> Self {
        let now = Instant::now();
        MonitoredMaster {
            name,
            host,
            port,
            quorum,
            status: MasterStatus::Up,
            last_ping_reply: now,
            last_ping_sent: now,
            last_ok_ping: now,
            down_after_ms: config.down_after_ms,
            failover_timeout_ms: config.failover_timeout_ms,
            parallel_syncs: config.parallel_syncs,
            subjective_down: false,
            objective_down: false,
            replicas: Vec::new(),
            sentinels: Vec::new(),
            failover_state: FailoverState::None,
            failover_epoch: 0,
            failover_start_time: None,
            promoted_replica: None,
            config_epoch: 0,
            odown_vote_count: 0,
            info_refresh: now,
            auth_pass: None,
        }
    }

    /// Get address as "host:port" string
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Check if SDOWN should be set based on ping timeout
    pub fn check_subjective_down(&mut self) -> bool {
        let elapsed = self.last_ping_reply.elapsed().as_millis() as u64;
        let was_down = self.subjective_down;
        self.subjective_down = elapsed > self.down_after_ms;
        if self.subjective_down {
            self.status = MasterStatus::SubjectiveDown;
        } else if was_down {
            self.status = MasterStatus::Up;
        }
        self.subjective_down
    }

    /// Check if ODOWN should be set based on quorum
    pub fn check_objective_down(&mut self) -> bool {
        if !self.subjective_down {
            self.objective_down = false;
            return false;
        }

        // Count sentinels that agree master is down (including ourselves)
        let mut votes = 1; // We count as 1 vote (ourselves)
        for sentinel in &self.sentinels {
            if sentinel.master_down_vote {
                votes += 1;
            }
        }
        self.odown_vote_count = votes;
        self.objective_down = votes >= self.quorum;
        if self.objective_down {
            self.status = MasterStatus::ObjectiveDown;
        }
        self.objective_down
    }

    /// Find the best replica for promotion
    /// Redis prefers: lower slave_priority (except 0 = never promote), then higher repl_offset
    pub fn select_best_replica(&self) -> Option<&ReplicaEntry> {
        self.replicas
            .iter()
            .filter(|r| !r.sdown && r.slave_priority > 0 && r.role_reported == "slave")
            .min_by(|a, b| {
                // Lower priority number is preferred (min_by picks the smallest)
                // If priority is equal, prefer higher repl_offset (reverse comparison)
                a.slave_priority
                    .cmp(&b.slave_priority)
                    .then_with(|| b.repl_offset.cmp(&a.repl_offset))
            })
    }

    /// Record a successful ping reply
    pub fn ping_reply_received(&mut self) {
        let now = Instant::now();
        self.last_ping_reply = now;
        self.last_ok_ping = now;
        if self.subjective_down {
            self.subjective_down = false;
            self.objective_down = false;
            self.status = MasterStatus::Up;
            self.odown_vote_count = 0;
        }
    }
}

/// Global sentinel state
#[derive(Debug)]
pub struct SentinelState {
    /// This sentinel's unique ID (40-char hex)
    pub my_id: String,
    /// Monitored masters by name
    pub masters: HashMap<String, MonitoredMaster>,
    /// Current epoch (for leader election)
    pub current_epoch: u64,
    /// Announce IP (if configured)
    pub announce_ip: Option<String>,
    /// Announce port (if configured)
    pub announce_port: Option<u16>,
}

/// The Sentinel Manager coordinates monitoring, failover, and sentinel commands
pub struct SentinelManager {
    #[allow(dead_code)]
    engine: Arc<StorageEngine>,
    config: SentinelConfig,
    state: Arc<RwLock<SentinelState>>,
}

impl SentinelManager {
    /// Create a new SentinelManager
    pub fn new(engine: Arc<StorageEngine>, config: SentinelConfig) -> Arc<Self> {
        let my_id = generate_sentinel_id();
        let state = Arc::new(RwLock::new(SentinelState {
            my_id,
            masters: HashMap::new(),
            current_epoch: 0,
            announce_ip: None,
            announce_port: None,
        }));

        Arc::new(Self {
            engine,
            config,
            state,
        })
    }

    /// Check if sentinel mode is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the sentinel config
    pub fn config(&self) -> &SentinelConfig {
        &self.config
    }

    /// Get the sentinel state
    pub fn state(&self) -> &Arc<RwLock<SentinelState>> {
        &self.state
    }

    /// Initialize the sentinel (start monitoring loops if masters are configured)
    pub async fn init(&self) {
        if !self.config.enabled {
            return;
        }
        info!("Sentinel mode enabled, sentinel ID: {}", self.state.read().await.my_id);
    }

    /// Monitor a new master
    pub async fn monitor_master(&self, name: &str, host: &str, port: u16, quorum: usize) -> Result<(), String> {
        let mut state = self.state.write().await;
        if state.masters.contains_key(name) {
            return Err(format!("ERR Duplicated master name '{}'", name));
        }
        let master = MonitoredMaster::new(
            name.to_string(),
            host.to_string(),
            port,
            quorum,
            &self.config,
        );
        info!(
            "Sentinel monitoring master '{}' at {}:{}  with quorum {}",
            name, host, port, quorum
        );
        state.masters.insert(name.to_string(), master);
        Ok(())
    }

    /// Remove a monitored master
    pub async fn remove_master(&self, name: &str) -> Result<(), String> {
        let mut state = self.state.write().await;
        if state.masters.remove(name).is_none() {
            return Err(format!("ERR No such master with that name"));
        }
        info!("Sentinel stopped monitoring master '{}'", name);
        Ok(())
    }

    /// Handle SENTINEL command - dispatches to subcommands
    pub async fn handle_sentinel_command(&self, args: &[Vec<u8>]) -> Result<RespValue, String> {
        if args.is_empty() {
            return Err("ERR wrong number of arguments for 'sentinel' command".to_string());
        }

        let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();

        if !self.config.enabled {
            // Some commands work even when not in sentinel mode
            return match subcommand.as_str() {
                "MYID" => {
                    let state = self.state.read().await;
                    Ok(RespValue::BulkString(Some(state.my_id.as_bytes().to_vec())))
                }
                "HELP" => Ok(sentinel_help()),
                _ => Err("ERR This instance has sentinel support disabled".to_string()),
            };
        }

        match subcommand.as_str() {
            "MASTERS" => self.cmd_masters().await,
            "MASTER" => {
                if args.len() < 2 {
                    return Err("ERR wrong number of arguments for 'sentinel master' command".to_string());
                }
                let name = String::from_utf8_lossy(&args[1]).to_string();
                self.cmd_master(&name).await
            }
            "REPLICAS" | "SLAVES" => {
                if args.len() < 2 {
                    return Err("ERR wrong number of arguments for 'sentinel replicas' command".to_string());
                }
                let name = String::from_utf8_lossy(&args[1]).to_string();
                self.cmd_replicas(&name).await
            }
            "SENTINELS" => {
                if args.len() < 2 {
                    return Err("ERR wrong number of arguments for 'sentinel sentinels' command".to_string());
                }
                let name = String::from_utf8_lossy(&args[1]).to_string();
                self.cmd_sentinels(&name).await
            }
            "MONITOR" => {
                if args.len() < 5 {
                    return Err("ERR wrong number of arguments for 'sentinel monitor' command".to_string());
                }
                let name = String::from_utf8_lossy(&args[1]).to_string();
                let host = String::from_utf8_lossy(&args[2]).to_string();
                let port: u16 = String::from_utf8_lossy(&args[3])
                    .parse()
                    .map_err(|_| "ERR Invalid port".to_string())?;
                let quorum: usize = String::from_utf8_lossy(&args[4])
                    .parse()
                    .map_err(|_| "ERR Invalid quorum".to_string())?;
                self.monitor_master(&name, &host, port, quorum).await?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            "REMOVE" => {
                if args.len() < 2 {
                    return Err("ERR wrong number of arguments for 'sentinel remove' command".to_string());
                }
                let name = String::from_utf8_lossy(&args[1]).to_string();
                self.remove_master(&name).await?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            "SET" => {
                if args.len() < 4 {
                    return Err("ERR wrong number of arguments for 'sentinel set' command".to_string());
                }
                let name = String::from_utf8_lossy(&args[1]).to_string();
                let option = String::from_utf8_lossy(&args[2]).to_lowercase();
                let value = String::from_utf8_lossy(&args[3]).to_string();
                self.cmd_set(&name, &option, &value).await
            }
            "GET-MASTER-ADDR-BY-NAME" => {
                if args.len() < 2 {
                    return Err("ERR wrong number of arguments for 'sentinel get-master-addr-by-name' command".to_string());
                }
                let name = String::from_utf8_lossy(&args[1]).to_string();
                self.cmd_get_master_addr_by_name(&name).await
            }
            "RESET" => {
                if args.len() < 2 {
                    return Err("ERR wrong number of arguments for 'sentinel reset' command".to_string());
                }
                let pattern = String::from_utf8_lossy(&args[1]).to_string();
                self.cmd_reset(&pattern).await
            }
            "FAILOVER" => {
                if args.len() < 2 {
                    return Err("ERR wrong number of arguments for 'sentinel failover' command".to_string());
                }
                let name = String::from_utf8_lossy(&args[1]).to_string();
                self.cmd_failover(&name).await
            }
            "CKQUORUM" => {
                if args.len() < 2 {
                    return Err("ERR wrong number of arguments for 'sentinel ckquorum' command".to_string());
                }
                let name = String::from_utf8_lossy(&args[1]).to_string();
                self.cmd_ckquorum(&name).await
            }
            "FLUSHCONFIG" => {
                self.cmd_flushconfig().await
            }
            "IS-MASTER-DOWN-BY-ADDR" => {
                if args.len() < 5 {
                    return Err("ERR wrong number of arguments for 'sentinel is-master-down-by-addr' command".to_string());
                }
                let ip = String::from_utf8_lossy(&args[1]).to_string();
                let port: u16 = String::from_utf8_lossy(&args[2])
                    .parse()
                    .map_err(|_| "ERR Invalid port".to_string())?;
                let current_epoch: u64 = String::from_utf8_lossy(&args[3])
                    .parse()
                    .map_err(|_| "ERR Invalid epoch".to_string())?;
                let run_id = String::from_utf8_lossy(&args[4]).to_string();
                self.cmd_is_master_down_by_addr(&ip, port, current_epoch, &run_id).await
            }
            "MYID" => {
                let state = self.state.read().await;
                Ok(RespValue::BulkString(Some(state.my_id.as_bytes().to_vec())))
            }
            "CONFIG" => {
                if args.len() < 3 {
                    return Err("ERR wrong number of arguments for 'sentinel config' command".to_string());
                }
                let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
                let param = String::from_utf8_lossy(&args[2]).to_string();
                match sub.as_str() {
                    "GET" => self.cmd_config_get(&param).await,
                    "SET" => {
                        if args.len() < 4 {
                            return Err("ERR wrong number of arguments for 'sentinel config set' command".to_string());
                        }
                        let val = String::from_utf8_lossy(&args[3]).to_string();
                        self.cmd_config_set(&param, &val).await
                    }
                    _ => Err(format!("ERR Unknown sentinel config subcommand '{}'", sub)),
                }
            }
            "INFO-CACHE" => {
                if args.len() < 2 {
                    return Err("ERR wrong number of arguments for 'sentinel info-cache' command".to_string());
                }
                let name = String::from_utf8_lossy(&args[1]).to_string();
                self.cmd_info_cache(&name).await
            }
            "SIMULATE-FAILURE" => {
                // Accept but do nothing (used for testing)
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            "PENDING-SCRIPTS" => {
                // Return empty array (no scripts pending)
                Ok(RespValue::Array(Some(vec![])))
            }
            "HELP" => Ok(sentinel_help()),
            _ => Err(format!("ERR Unknown sentinel subcommand '{}'", subcommand)),
        }
    }

    // --- Subcommand implementations ---

    /// SENTINEL MASTERS - list all monitored masters
    async fn cmd_masters(&self) -> Result<RespValue, String> {
        let state = self.state.read().await;
        let mut result = Vec::new();
        for master in state.masters.values() {
            result.push(master_to_resp(master));
        }
        Ok(RespValue::Array(Some(result)))
    }

    /// SENTINEL MASTER <name> - get info about a specific master
    async fn cmd_master(&self, name: &str) -> Result<RespValue, String> {
        let state = self.state.read().await;
        match state.masters.get(name) {
            Some(master) => Ok(master_to_resp(master)),
            None => Err(format!("ERR No such master with that name")),
        }
    }

    /// SENTINEL REPLICAS <name> - list replicas of a master
    async fn cmd_replicas(&self, name: &str) -> Result<RespValue, String> {
        let state = self.state.read().await;
        match state.masters.get(name) {
            Some(master) => {
                let replicas: Vec<RespValue> = master
                    .replicas
                    .iter()
                    .map(replica_to_resp)
                    .collect();
                Ok(RespValue::Array(Some(replicas)))
            }
            None => Err(format!("ERR No such master with that name")),
        }
    }

    /// SENTINEL SENTINELS <name> - list other sentinels for a master
    async fn cmd_sentinels(&self, name: &str) -> Result<RespValue, String> {
        let state = self.state.read().await;
        match state.masters.get(name) {
            Some(master) => {
                let sentinels: Vec<RespValue> = master
                    .sentinels
                    .iter()
                    .map(sentinel_peer_to_resp)
                    .collect();
                Ok(RespValue::Array(Some(sentinels)))
            }
            None => Err(format!("ERR No such master with that name")),
        }
    }

    /// SENTINEL GET-MASTER-ADDR-BY-NAME <name>
    async fn cmd_get_master_addr_by_name(&self, name: &str) -> Result<RespValue, String> {
        let state = self.state.read().await;
        match state.masters.get(name) {
            Some(master) => Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(master.host.as_bytes().to_vec())),
                RespValue::BulkString(Some(master.port.to_string().as_bytes().to_vec())),
            ]))),
            None => Ok(RespValue::Null),
        }
    }

    /// SENTINEL RESET <pattern> - reset masters matching pattern
    async fn cmd_reset(&self, pattern: &str) -> Result<RespValue, String> {
        let mut state = self.state.write().await;
        let mut count = 0;
        let names: Vec<String> = state.masters.keys().cloned().collect();
        for name in &names {
            if glob_match(pattern, name) {
                if let Some(master) = state.masters.get_mut(name) {
                    master.replicas.clear();
                    master.sentinels.clear();
                    master.subjective_down = false;
                    master.objective_down = false;
                    master.status = MasterStatus::Up;
                    master.failover_state = FailoverState::None;
                    master.odown_vote_count = 0;
                    count += 1;
                }
            }
        }
        Ok(RespValue::Integer(count))
    }

    /// SENTINEL FAILOVER <name> - force a failover
    async fn cmd_failover(&self, name: &str) -> Result<RespValue, String> {
        let mut state = self.state.write().await;
        // Check preconditions first without holding a mutable ref to the master
        {
            let master = match state.masters.get(name) {
                Some(m) => m,
                None => return Err(format!("ERR No such master with that name")),
            };
            if master.failover_state != FailoverState::None {
                return Err("ERR FAILOVER already in progress".to_string());
            }
            if master.replicas.is_empty() {
                return Err("ERR No good replica to promote".to_string());
            }
        }
        // Now update state
        state.current_epoch += 1;
        let new_epoch = state.current_epoch;
        let master = state.masters.get_mut(name).unwrap();
        master.failover_state = FailoverState::WaitStart;
        master.failover_epoch = new_epoch;
        master.failover_start_time = Some(Instant::now());
        info!("Sentinel forced failover for master '{}'", name);
        Ok(RespValue::SimpleString("OK".to_string()))
    }

    /// SENTINEL CKQUORUM <name> - check if quorum is reachable
    async fn cmd_ckquorum(&self, name: &str) -> Result<RespValue, String> {
        let state = self.state.read().await;
        match state.masters.get(name) {
            Some(master) => {
                let usable_sentinels = master.sentinels.iter().filter(|s| !s.sdown).count() + 1; // +1 for ourselves
                if usable_sentinels >= master.quorum {
                    Ok(RespValue::SimpleString(format!(
                        "OK {} usable Sentinels. Quorum and failover authorization is possible.",
                        usable_sentinels
                    )))
                } else {
                    Err(format!(
                        "NOQUORUM {} usable Sentinels. Not enough available Sentinels to reach the specified quorum of {}.",
                        usable_sentinels, master.quorum
                    ))
                }
            }
            None => Err(format!("ERR No such master with that name")),
        }
    }

    /// SENTINEL FLUSHCONFIG - rewrite config file
    async fn cmd_flushconfig(&self) -> Result<RespValue, String> {
        // In rLightning, we don't persist sentinel config to file
        // but we accept the command for compatibility
        Ok(RespValue::SimpleString("OK".to_string()))
    }

    /// SENTINEL IS-MASTER-DOWN-BY-ADDR - used for SDOWN/ODOWN voting
    async fn cmd_is_master_down_by_addr(
        &self,
        ip: &str,
        port: u16,
        current_epoch: u64,
        run_id: &str,
    ) -> Result<RespValue, String> {
        let state = self.state.read().await;
        let addr = format!("{}:{}", ip, port);

        // Find the master at this address
        let master = state.masters.values().find(|m| m.addr() == addr);

        let is_down = match master {
            Some(m) => m.subjective_down,
            None => false,
        };

        // If run_id is "*", this is just a down state query
        // If run_id is an actual ID, this is a vote request for leader election
        let leader_id = if run_id == "*" {
            "*".to_string()
        } else {
            // Vote for this sentinel as leader if their epoch is >= ours
            if current_epoch >= state.current_epoch {
                run_id.to_string()
            } else {
                "*".to_string()
            }
        };

        Ok(RespValue::Array(Some(vec![
            RespValue::Integer(if is_down { 1 } else { 0 }),
            RespValue::BulkString(Some(leader_id.as_bytes().to_vec())),
            RespValue::Integer(state.current_epoch as i64),
        ])))
    }

    /// SENTINEL SET <name> <option> <value>
    async fn cmd_set(&self, name: &str, option: &str, value: &str) -> Result<RespValue, String> {
        let mut state = self.state.write().await;
        match state.masters.get_mut(name) {
            Some(master) => {
                match option {
                    "down-after-milliseconds" => {
                        master.down_after_ms = value.parse().map_err(|_| "ERR Invalid value".to_string())?;
                    }
                    "failover-timeout" => {
                        master.failover_timeout_ms = value.parse().map_err(|_| "ERR Invalid value".to_string())?;
                    }
                    "parallel-syncs" => {
                        master.parallel_syncs = value.parse().map_err(|_| "ERR Invalid value".to_string())?;
                    }
                    "quorum" => {
                        master.quorum = value.parse().map_err(|_| "ERR Invalid value".to_string())?;
                    }
                    "auth-pass" => {
                        master.auth_pass = if value.is_empty() { None } else { Some(value.to_string()) };
                    }
                    _ => {
                        return Err(format!("ERR Invalid argument '{}' for SENTINEL SET", option));
                    }
                }
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            None => Err(format!("ERR No such master with that name")),
        }
    }

    /// SENTINEL CONFIG GET <param>
    async fn cmd_config_get(&self, param: &str) -> Result<RespValue, String> {
        let mut results = Vec::new();
        match param {
            "resolve-hostnames" => {
                results.push(RespValue::BulkString(Some(b"resolve-hostnames".to_vec())));
                results.push(RespValue::BulkString(Some(b"no".to_vec())));
            }
            "announce-ip" => {
                let state = self.state.read().await;
                results.push(RespValue::BulkString(Some(b"announce-ip".to_vec())));
                let val = state.announce_ip.as_deref().unwrap_or("");
                results.push(RespValue::BulkString(Some(val.as_bytes().to_vec())));
            }
            "announce-port" => {
                let state = self.state.read().await;
                results.push(RespValue::BulkString(Some(b"announce-port".to_vec())));
                let val = state.announce_port.map(|p| p.to_string()).unwrap_or_else(|| "0".to_string());
                results.push(RespValue::BulkString(Some(val.as_bytes().to_vec())));
            }
            _ => {
                // Return empty for unknown params
            }
        }
        Ok(RespValue::Array(Some(results)))
    }

    /// SENTINEL CONFIG SET <param> <value>
    async fn cmd_config_set(&self, param: &str, value: &str) -> Result<RespValue, String> {
        let mut state = self.state.write().await;
        match param {
            "announce-ip" => {
                state.announce_ip = if value.is_empty() { None } else { Some(value.to_string()) };
            }
            "announce-port" => {
                let port: u16 = value.parse().map_err(|_| "ERR Invalid port".to_string())?;
                state.announce_port = if port == 0 { None } else { Some(port) };
            }
            _ => {
                return Err(format!("ERR Unsupported parameter '{}'", param));
            }
        }
        Ok(RespValue::SimpleString("OK".to_string()))
    }

    /// SENTINEL INFO-CACHE <name>
    async fn cmd_info_cache(&self, name: &str) -> Result<RespValue, String> {
        let state = self.state.read().await;
        match state.masters.get(name) {
            Some(master) => {
                // Return cached info as array of [addr, info_age_ms, info_string]
                let now = Instant::now();
                let age_ms = now.duration_since(master.info_refresh).as_millis() as i64;
                let info_str = format!(
                    "# Server\r\nredis_version:7.0.0\r\n# Replication\r\nrole:master\r\nconnected_slaves:{}\r\nmaster_replid:0000000000000000000000000000000000000000\r\nmaster_repl_offset:0\r\n",
                    master.replicas.len()
                );
                Ok(RespValue::Array(Some(vec![
                    RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(master.addr().as_bytes().to_vec())),
                        RespValue::Integer(age_ms),
                        RespValue::BulkString(Some(info_str.as_bytes().to_vec())),
                    ])),
                ])))
            }
            None => Err(format!("ERR No such master with that name")),
        }
    }

    /// Get sentinel info for the INFO command
    pub async fn get_sentinel_info(&self) -> String {
        let state = self.state.read().await;
        let mut info = String::new();
        info.push_str("# Sentinel\r\n");
        info.push_str(&format!("sentinel_masters:{}\r\n", state.masters.len()));
        info.push_str(&format!("sentinel_tilt:0\r\n"));
        info.push_str(&format!("sentinel_tilt_since_seconds:-1\r\n"));
        info.push_str(&format!("sentinel_running_scripts:0\r\n"));
        info.push_str(&format!("sentinel_scripts_queue_length:0\r\n"));
        info.push_str(&format!("sentinel_simulate_failure_flags:0\r\n"));

        for (name, master) in &state.masters {
            info.push_str(&format!(
                "master{}:name={},status={},address={}:{},slaves={},sentinels={}\r\n",
                0, // Redis uses an index here
                name,
                master.status,
                master.host,
                master.port,
                master.replicas.len(),
                master.sentinels.len() + 1, // +1 for ourselves
            ));
        }

        info
    }
}

/// Convert a MonitoredMaster to RESP key-value array format
fn master_to_resp(master: &MonitoredMaster) -> RespValue {
    let flags = {
        let mut f = Vec::new();
        f.push("master");
        if master.subjective_down {
            f.push("s_down");
        }
        if master.objective_down {
            f.push("o_down");
        }
        f.join(",")
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let fields: Vec<RespValue> = vec![
        RespValue::BulkString(Some(b"name".to_vec())),
        RespValue::BulkString(Some(master.name.as_bytes().to_vec())),
        RespValue::BulkString(Some(b"ip".to_vec())),
        RespValue::BulkString(Some(master.host.as_bytes().to_vec())),
        RespValue::BulkString(Some(b"port".to_vec())),
        RespValue::BulkString(Some(master.port.to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"runid".to_vec())),
        RespValue::BulkString(Some(b"".to_vec())),
        RespValue::BulkString(Some(b"flags".to_vec())),
        RespValue::BulkString(Some(flags.as_bytes().to_vec())),
        RespValue::BulkString(Some(b"link-pending-commands".to_vec())),
        RespValue::BulkString(Some(b"0".to_vec())),
        RespValue::BulkString(Some(b"link-refcount".to_vec())),
        RespValue::BulkString(Some(b"1".to_vec())),
        RespValue::BulkString(Some(b"last-ping-sent".to_vec())),
        RespValue::BulkString(Some(b"0".to_vec())),
        RespValue::BulkString(Some(b"last-ok-ping-reply".to_vec())),
        RespValue::BulkString(Some(
            master.last_ok_ping.elapsed().as_millis().to_string().as_bytes().to_vec(),
        )),
        RespValue::BulkString(Some(b"last-ping-reply".to_vec())),
        RespValue::BulkString(Some(
            master.last_ping_reply.elapsed().as_millis().to_string().as_bytes().to_vec(),
        )),
        RespValue::BulkString(Some(b"down-after-milliseconds".to_vec())),
        RespValue::BulkString(Some(master.down_after_ms.to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"info-refresh".to_vec())),
        RespValue::BulkString(Some(
            master.info_refresh.elapsed().as_millis().to_string().as_bytes().to_vec(),
        )),
        RespValue::BulkString(Some(b"role-reported".to_vec())),
        RespValue::BulkString(Some(b"master".to_vec())),
        RespValue::BulkString(Some(b"role-reported-time".to_vec())),
        RespValue::BulkString(Some(now.as_millis().to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"config-epoch".to_vec())),
        RespValue::BulkString(Some(master.config_epoch.to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"num-slaves".to_vec())),
        RespValue::BulkString(Some(master.replicas.len().to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"num-other-sentinels".to_vec())),
        RespValue::BulkString(Some(master.sentinels.len().to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"quorum".to_vec())),
        RespValue::BulkString(Some(master.quorum.to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"failover-timeout".to_vec())),
        RespValue::BulkString(Some(master.failover_timeout_ms.to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"parallel-syncs".to_vec())),
        RespValue::BulkString(Some(master.parallel_syncs.to_string().as_bytes().to_vec())),
    ];

    RespValue::Array(Some(fields))
}

/// Convert a ReplicaEntry to RESP key-value array format
fn replica_to_resp(replica: &ReplicaEntry) -> RespValue {
    let flags = {
        let mut f = vec!["slave"];
        if replica.sdown {
            f.push("s_down");
        }
        f.join(",")
    };

    let fields: Vec<RespValue> = vec![
        RespValue::BulkString(Some(b"name".to_vec())),
        RespValue::BulkString(Some(replica.addr().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"ip".to_vec())),
        RespValue::BulkString(Some(replica.host.as_bytes().to_vec())),
        RespValue::BulkString(Some(b"port".to_vec())),
        RespValue::BulkString(Some(replica.port.to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"runid".to_vec())),
        RespValue::BulkString(Some(b"".to_vec())),
        RespValue::BulkString(Some(b"flags".to_vec())),
        RespValue::BulkString(Some(flags.as_bytes().to_vec())),
        RespValue::BulkString(Some(b"link-pending-commands".to_vec())),
        RespValue::BulkString(Some(b"0".to_vec())),
        RespValue::BulkString(Some(b"link-refcount".to_vec())),
        RespValue::BulkString(Some(b"1".to_vec())),
        RespValue::BulkString(Some(b"last-ping-sent".to_vec())),
        RespValue::BulkString(Some(b"0".to_vec())),
        RespValue::BulkString(Some(b"last-ok-ping-reply".to_vec())),
        RespValue::BulkString(Some(
            replica.last_ok_ping.elapsed().as_millis().to_string().as_bytes().to_vec(),
        )),
        RespValue::BulkString(Some(b"last-ping-reply".to_vec())),
        RespValue::BulkString(Some(
            replica.last_ping_reply.elapsed().as_millis().to_string().as_bytes().to_vec(),
        )),
        RespValue::BulkString(Some(b"slave-repl-offset".to_vec())),
        RespValue::BulkString(Some(replica.repl_offset.to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"slave-priority".to_vec())),
        RespValue::BulkString(Some(replica.slave_priority.to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"role-reported".to_vec())),
        RespValue::BulkString(Some(replica.role_reported.as_bytes().to_vec())),
    ];

    RespValue::Array(Some(fields))
}

/// Convert a SentinelPeer to RESP key-value array format
fn sentinel_peer_to_resp(sentinel: &SentinelPeer) -> RespValue {
    let flags = {
        let mut f = vec!["sentinel"];
        if sentinel.sdown {
            f.push("s_down");
        }
        f.join(",")
    };

    let fields: Vec<RespValue> = vec![
        RespValue::BulkString(Some(b"name".to_vec())),
        RespValue::BulkString(Some(sentinel.id.as_bytes().to_vec())),
        RespValue::BulkString(Some(b"ip".to_vec())),
        RespValue::BulkString(Some(sentinel.host.as_bytes().to_vec())),
        RespValue::BulkString(Some(b"port".to_vec())),
        RespValue::BulkString(Some(sentinel.port.to_string().as_bytes().to_vec())),
        RespValue::BulkString(Some(b"runid".to_vec())),
        RespValue::BulkString(Some(sentinel.id.as_bytes().to_vec())),
        RespValue::BulkString(Some(b"flags".to_vec())),
        RespValue::BulkString(Some(flags.as_bytes().to_vec())),
        RespValue::BulkString(Some(b"link-pending-commands".to_vec())),
        RespValue::BulkString(Some(b"0".to_vec())),
        RespValue::BulkString(Some(b"link-refcount".to_vec())),
        RespValue::BulkString(Some(b"1".to_vec())),
        RespValue::BulkString(Some(b"last-hello-message".to_vec())),
        RespValue::BulkString(Some(
            sentinel.last_hello.elapsed().as_millis().to_string().as_bytes().to_vec(),
        )),
        RespValue::BulkString(Some(b"voted-leader".to_vec())),
        RespValue::BulkString(Some(sentinel.leader_id.as_bytes().to_vec())),
        RespValue::BulkString(Some(b"voted-leader-epoch".to_vec())),
        RespValue::BulkString(Some(sentinel.leader_epoch.to_string().as_bytes().to_vec())),
    ];

    RespValue::Array(Some(fields))
}

/// Generate a 40-character hex sentinel ID (like Redis)
fn generate_sentinel_id() -> String {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let combined = format!("{:032x}{:032x}", uuid1.as_u128(), uuid2.as_u128());
    combined[..40].to_string()
}

/// Simple glob-style pattern matching (supports * and ?)
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();

    glob_match_inner(&p, &t, 0, 0)
}

fn glob_match_inner(pattern: &[char], text: &[char], mut pi: usize, mut ti: usize) -> bool {
    // Iterative two-pointer algorithm to avoid exponential backtracking
    let mut star_pi: Option<usize> = None;
    let mut star_ti: usize = 0;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == '*' {
            star_pi = Some(pi);
            star_ti = ti;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == '*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// SENTINEL HELP response
fn sentinel_help() -> RespValue {
    let help_lines = vec![
        "SENTINEL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "CKQUORUM <master-name>",
        "    Check if the current Sentinel configuration is able to reach the quorum.",
        "CONFIG GET <param>",
        "    Get global Sentinel configuration parameter.",
        "CONFIG SET <param> <value>",
        "    Set global Sentinel configuration parameter.",
        "FAILOVER <master-name>",
        "    Force a failover even without agreement with other Sentinels.",
        "FLUSHCONFIG",
        "    Force Sentinel to rewrite its configuration to disk.",
        "GET-MASTER-ADDR-BY-NAME <master-name>",
        "    Return the ip and port number of the master with the given name.",
        "INFO-CACHE <master-name>",
        "    Return cached INFO output from masters and replicas.",
        "IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>",
        "    Check if a master is down from current Sentinel's point of view.",
        "MASTER <master-name>",
        "    Show the state and info of the specified master.",
        "MASTERS",
        "    Show a list of monitored masters and their state.",
        "MONITOR <name> <ip> <port> <quorum>",
        "    Start monitoring a new master.",
        "MYID",
        "    Return the ID of the Sentinel instance.",
        "PENDING-SCRIPTS",
        "    Get pending scripts information.",
        "REPLICAS <master-name>",
        "    Show a list of replicas for this master, and their state.",
        "REMOVE <master-name>",
        "    Remove master from Sentinel's monitoring.",
        "RESET <pattern>",
        "    Reset masters matching the given pattern.",
        "SENTINELS <master-name>",
        "    Show a list of Sentinel instances for this master, and their state.",
        "SET <master-name> <option> <value>",
        "    Set configuration parameters for certain masters.",
        "SIMULATE-FAILURE (crash-after-election|crash-after-promotion|help)",
        "    Simulate a Sentinel crash for testing.",
        "HELP",
        "    Print this help.",
    ];

    RespValue::Array(Some(
        help_lines
            .into_iter()
            .map(|line| RespValue::BulkString(Some(line.as_bytes().to_vec())))
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use crate::storage::engine::StorageConfig;

    fn create_test_sentinel() -> (Arc<StorageEngine>, Arc<SentinelManager>) {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let sentinel_config = SentinelConfig {
            enabled: true,
            down_after_ms: 5000,
            ..Default::default()
        };
        let mgr = SentinelManager::new(Arc::clone(&storage), sentinel_config);
        (storage, mgr)
    }

    fn create_disabled_sentinel() -> (Arc<StorageEngine>, Arc<SentinelManager>) {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let sentinel_config = SentinelConfig {
            enabled: false,
            ..Default::default()
        };
        let mgr = SentinelManager::new(Arc::clone(&storage), sentinel_config);
        (storage, mgr)
    }

    #[tokio::test]
    async fn test_sentinel_myid() {
        let (_storage, sentinel) = create_test_sentinel();
        let result = sentinel
            .handle_sentinel_command(&[b"MYID".to_vec()])
            .await
            .unwrap();
        if let RespValue::BulkString(Some(id)) = result {
            assert_eq!(id.len(), 40);
        } else {
            panic!("Expected BulkString for MYID");
        }
    }

    #[tokio::test]
    async fn test_sentinel_monitor_master() {
        let (_storage, sentinel) = create_test_sentinel();

        // Monitor a master
        let result = sentinel
            .handle_sentinel_command(&[
                b"MONITOR".to_vec(),
                b"mymaster".to_vec(),
                b"127.0.0.1".to_vec(),
                b"6379".to_vec(),
                b"2".to_vec(),
            ])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify it appears in MASTERS
        let result = sentinel
            .handle_sentinel_command(&[b"MASTERS".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(masters)) = result {
            assert_eq!(masters.len(), 1);
        } else {
            panic!("Expected Array for MASTERS");
        }
    }

    #[tokio::test]
    async fn test_sentinel_master_info() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"MASTER".to_vec(), b"mymaster".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(fields)) = result {
            // Should have key-value pairs
            assert!(fields.len() > 10);
            // Check name field
            if let RespValue::BulkString(Some(ref val)) = fields[1] {
                assert_eq!(val, b"mymaster");
            }
        } else {
            panic!("Expected Array for MASTER");
        }
    }

    #[tokio::test]
    async fn test_sentinel_get_master_addr_by_name() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"GET-MASTER-ADDR-BY-NAME".to_vec(), b"mymaster".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(addr)) = result {
            assert_eq!(addr.len(), 2);
            if let RespValue::BulkString(Some(ref host)) = addr[0] {
                assert_eq!(host, b"127.0.0.1");
            }
            if let RespValue::BulkString(Some(ref port)) = addr[1] {
                assert_eq!(port, b"6379");
            }
        } else {
            panic!("Expected Array for GET-MASTER-ADDR-BY-NAME");
        }
    }

    #[tokio::test]
    async fn test_sentinel_get_master_addr_nonexistent() {
        let (_storage, sentinel) = create_test_sentinel();

        let result = sentinel
            .handle_sentinel_command(&[b"GET-MASTER-ADDR-BY-NAME".to_vec(), b"nonexistent".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[tokio::test]
    async fn test_sentinel_remove_master() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"REMOVE".to_vec(), b"mymaster".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Should be gone now
        let result = sentinel
            .handle_sentinel_command(&[b"MASTERS".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(masters)) = result {
            assert_eq!(masters.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_sentinel_duplicate_monitor() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let result = sentinel.monitor_master("mymaster", "127.0.0.2", 6380, 3).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sentinel_set_options() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        // Set down-after-milliseconds
        let result = sentinel
            .handle_sentinel_command(&[
                b"SET".to_vec(),
                b"mymaster".to_vec(),
                b"down-after-milliseconds".to_vec(),
                b"10000".to_vec(),
            ])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify it was set
        let state = sentinel.state().read().await;
        let master = state.masters.get("mymaster").unwrap();
        assert_eq!(master.down_after_ms, 10000);
    }

    #[tokio::test]
    async fn test_sentinel_replicas_empty() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"REPLICAS".to_vec(), b"mymaster".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(replicas)) = result {
            assert_eq!(replicas.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_sentinel_sentinels_empty() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"SENTINELS".to_vec(), b"mymaster".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(sentinels)) = result {
            assert_eq!(sentinels.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_sentinel_ckquorum() {
        let (_storage, sentinel) = create_test_sentinel();

        // With quorum 1, we alone should be enough
        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 1).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"CKQUORUM".to_vec(), b"mymaster".to_vec()])
            .await
            .unwrap();
        if let RespValue::SimpleString(msg) = result {
            assert!(msg.starts_with("OK"));
        } else {
            panic!("Expected SimpleString for CKQUORUM");
        }
    }

    #[tokio::test]
    async fn test_sentinel_ckquorum_insufficient() {
        let (_storage, sentinel) = create_test_sentinel();

        // With quorum 3, we alone are not enough
        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 3).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"CKQUORUM".to_vec(), b"mymaster".to_vec()])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sentinel_reset() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster1", "127.0.0.1", 6379, 2).await.unwrap();
        sentinel.monitor_master("mymaster2", "127.0.0.1", 6380, 2).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"RESET".to_vec(), b"*".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_sentinel_failover_no_replicas() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"FAILOVER".to_vec(), b"mymaster".to_vec()])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sentinel_is_master_down_by_addr() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[
                b"IS-MASTER-DOWN-BY-ADDR".to_vec(),
                b"127.0.0.1".to_vec(),
                b"6379".to_vec(),
                b"0".to_vec(),
                b"*".to_vec(),
            ])
            .await
            .unwrap();
        if let RespValue::Array(Some(fields)) = result {
            assert_eq!(fields.len(), 3);
            // Master should not be down
            assert_eq!(fields[0], RespValue::Integer(0));
        }
    }

    #[tokio::test]
    async fn test_sentinel_disabled_mode() {
        let (_storage, sentinel) = create_disabled_sentinel();

        // MYID should still work
        let result = sentinel
            .handle_sentinel_command(&[b"MYID".to_vec()])
            .await
            .unwrap();
        if let RespValue::BulkString(Some(id)) = result {
            assert_eq!(id.len(), 40);
        }

        // MASTERS should fail
        let result = sentinel
            .handle_sentinel_command(&[b"MASTERS".to_vec()])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sentinel_help() {
        let (_storage, sentinel) = create_test_sentinel();

        let result = sentinel
            .handle_sentinel_command(&[b"HELP".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(lines)) = result {
            assert!(lines.len() > 5);
        } else {
            panic!("Expected Array for HELP");
        }
    }

    #[tokio::test]
    async fn test_sentinel_flushconfig() {
        let (_storage, sentinel) = create_test_sentinel();

        let result = sentinel
            .handle_sentinel_command(&[b"FLUSHCONFIG".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_sentinel_config_get_set() {
        let (_storage, sentinel) = create_test_sentinel();

        // Set announce-ip
        let result = sentinel
            .handle_sentinel_command(&[
                b"CONFIG".to_vec(),
                b"SET".to_vec(),
                b"announce-ip".to_vec(),
                b"10.0.0.1".to_vec(),
            ])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Get announce-ip
        let result = sentinel
            .handle_sentinel_command(&[
                b"CONFIG".to_vec(),
                b"GET".to_vec(),
                b"announce-ip".to_vec(),
            ])
            .await
            .unwrap();
        if let RespValue::Array(Some(fields)) = result {
            assert_eq!(fields.len(), 2);
            if let RespValue::BulkString(Some(ref val)) = fields[1] {
                assert_eq!(val, b"10.0.0.1");
            }
        }
    }

    #[tokio::test]
    async fn test_sentinel_info() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let info = sentinel.get_sentinel_info().await;
        assert!(info.contains("# Sentinel"));
        assert!(info.contains("sentinel_masters:1"));
        assert!(info.contains("mymaster"));
    }

    #[tokio::test]
    async fn test_sentinel_info_cache() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"INFO-CACHE".to_vec(), b"mymaster".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 1);
        } else {
            panic!("Expected Array for INFO-CACHE");
        }
    }

    #[tokio::test]
    async fn test_subjective_down_detection() {
        let mut master = MonitoredMaster::new(
            "test".to_string(),
            "127.0.0.1".to_string(),
            6379,
            2,
            &SentinelConfig { down_after_ms: 100, ..Default::default() },
        );

        // Master should be up initially
        assert!(!master.check_subjective_down());
        assert_eq!(master.status, MasterStatus::Up);

        // Simulate timeout by setting last_ping_reply to the past
        master.last_ping_reply = Instant::now() - Duration::from_millis(200);
        assert!(master.check_subjective_down());
        assert_eq!(master.status, MasterStatus::SubjectiveDown);

        // Simulate recovery
        master.ping_reply_received();
        assert!(!master.check_subjective_down());
        assert_eq!(master.status, MasterStatus::Up);
    }

    #[tokio::test]
    async fn test_objective_down_detection() {
        let mut master = MonitoredMaster::new(
            "test".to_string(),
            "127.0.0.1".to_string(),
            6379,
            2,
            &SentinelConfig { down_after_ms: 100, ..Default::default() },
        );

        // Add a sentinel peer that thinks master is down
        let mut peer = SentinelPeer::new("abc123".to_string(), "10.0.0.1".to_string(), 26379);
        peer.master_down_vote = true;
        master.sentinels.push(peer);

        // Without SDOWN, ODOWN should not trigger
        assert!(!master.check_objective_down());

        // Set SDOWN
        master.last_ping_reply = Instant::now() - Duration::from_millis(200);
        master.check_subjective_down();

        // Now ODOWN should trigger (us + 1 peer = 2 >= quorum of 2)
        assert!(master.check_objective_down());
        assert_eq!(master.status, MasterStatus::ObjectiveDown);
    }

    #[tokio::test]
    async fn test_select_best_replica() {
        let mut master = MonitoredMaster::new(
            "test".to_string(),
            "127.0.0.1".to_string(),
            6379,
            2,
            &SentinelConfig::default(),
        );

        // Add replicas with different priorities and offsets
        let mut r1 = ReplicaEntry::new("10.0.0.1".to_string(), 6380);
        r1.slave_priority = 100;
        r1.repl_offset = 1000;

        let mut r2 = ReplicaEntry::new("10.0.0.2".to_string(), 6381);
        r2.slave_priority = 50;
        r2.repl_offset = 2000;

        let mut r3 = ReplicaEntry::new("10.0.0.3".to_string(), 6382);
        r3.slave_priority = 0; // Never promote
        r3.repl_offset = 3000;

        master.replicas.push(r1);
        master.replicas.push(r2);
        master.replicas.push(r3);

        let best = master.select_best_replica().unwrap();
        // r2 (priority 50) should be selected: lower priority number = preferred in Redis
        // r3 (priority 0) is excluded (never promote)
        assert_eq!(best.host, "10.0.0.2"); // priority 50 < priority 100
    }

    #[tokio::test]
    async fn test_glob_matching() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("my*", "mymaster"));
        assert!(glob_match("my*", "myother"));
        assert!(!glob_match("my*", "other"));
        assert!(glob_match("?y*", "mymaster"));
        assert!(glob_match("mymaster", "mymaster"));
        assert!(!glob_match("mymaster", "myother"));
    }

    #[tokio::test]
    async fn test_sentinel_slaves_alias() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        // SLAVES should work as alias for REPLICAS
        let result = sentinel
            .handle_sentinel_command(&[b"SLAVES".to_vec(), b"mymaster".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(replicas)) = result {
            assert_eq!(replicas.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_sentinel_simulate_failure() {
        let (_storage, sentinel) = create_test_sentinel();

        let result = sentinel
            .handle_sentinel_command(&[b"SIMULATE-FAILURE".to_vec(), b"crash-after-election".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_sentinel_pending_scripts() {
        let (_storage, sentinel) = create_test_sentinel();

        let result = sentinel
            .handle_sentinel_command(&[b"PENDING-SCRIPTS".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_sentinel_wrong_args() {
        let (_storage, sentinel) = create_test_sentinel();

        // Empty args
        let result = sentinel.handle_sentinel_command(&[]).await;
        assert!(result.is_err());

        // MASTER without name
        let result = sentinel
            .handle_sentinel_command(&[b"MASTER".to_vec()])
            .await;
        assert!(result.is_err());

        // MONITOR with insufficient args
        let result = sentinel
            .handle_sentinel_command(&[b"MONITOR".to_vec(), b"test".to_vec()])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sentinel_set_auth_pass() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[
                b"SET".to_vec(),
                b"mymaster".to_vec(),
                b"auth-pass".to_vec(),
                b"secret123".to_vec(),
            ])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let state = sentinel.state().read().await;
        let master = state.masters.get("mymaster").unwrap();
        assert_eq!(master.auth_pass, Some("secret123".to_string()));
    }

    #[tokio::test]
    async fn test_sentinel_failover_with_replicas() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("mymaster", "127.0.0.1", 6379, 2).await.unwrap();

        // Add a replica
        {
            let mut state = sentinel.state().write().await;
            let master = state.masters.get_mut("mymaster").unwrap();
            master.replicas.push(ReplicaEntry::new("10.0.0.1".to_string(), 6380));
        }

        // Now failover should work
        let result = sentinel
            .handle_sentinel_command(&[b"FAILOVER".to_vec(), b"mymaster".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Check failover state
        let state = sentinel.state().read().await;
        let master = state.masters.get("mymaster").unwrap();
        assert_eq!(master.failover_state, FailoverState::WaitStart);
    }

    #[tokio::test]
    async fn test_sentinel_multiple_masters() {
        let (_storage, sentinel) = create_test_sentinel();

        sentinel.monitor_master("master1", "127.0.0.1", 6379, 2).await.unwrap();
        sentinel.monitor_master("master2", "127.0.0.2", 6380, 3).await.unwrap();
        sentinel.monitor_master("master3", "127.0.0.3", 6381, 1).await.unwrap();

        let result = sentinel
            .handle_sentinel_command(&[b"MASTERS".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(masters)) = result {
            assert_eq!(masters.len(), 3);
        }

        // Reset only master1 and master3 using pattern
        let result = sentinel
            .handle_sentinel_command(&[b"RESET".to_vec(), b"master?".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(3)); // All match "master?"
    }

    #[tokio::test]
    async fn test_sentinel_unknown_subcommand() {
        let (_storage, sentinel) = create_test_sentinel();

        let result = sentinel
            .handle_sentinel_command(&[b"FOOBAR".to_vec()])
            .await;
        assert!(result.is_err());
    }
}
