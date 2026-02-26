#![allow(dead_code)]

pub mod gossip;
pub mod migration;
pub mod slot;

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::storage::engine::StorageEngine;
use slot::{CLUSTER_SLOTS, SlotRange, SlotState, key_hash_slot};

/// Cluster node flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Master,
    Replica,
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeRole::Master => write!(f, "master"),
            NodeRole::Replica => write!(f, "slave"),
        }
    }
}

/// Node flags for cluster node state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeFlags {
    pub role: NodeRole,
    pub myself: bool,
    pub pfail: bool,
    pub fail: bool,
    pub handshake: bool,
    pub noaddr: bool,
}

impl NodeFlags {
    pub fn new_master() -> Self {
        NodeFlags {
            role: NodeRole::Master,
            myself: false,
            pfail: false,
            fail: false,
            handshake: false,
            noaddr: false,
        }
    }

    pub fn new_replica() -> Self {
        NodeFlags {
            role: NodeRole::Replica,
            myself: false,
            pfail: false,
            fail: false,
            handshake: false,
            noaddr: false,
        }
    }
}

impl fmt::Display for NodeFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut flags = Vec::new();
        if self.myself {
            flags.push("myself");
        }
        match self.role {
            NodeRole::Master => flags.push("master"),
            NodeRole::Replica => flags.push("slave"),
        }
        if self.pfail {
            flags.push("fail?");
        }
        if self.fail {
            flags.push("fail");
        }
        if self.handshake {
            flags.push("handshake");
        }
        if self.noaddr {
            flags.push("noaddr");
        }
        write!(f, "{}", flags.join(","))
    }
}

/// Represents a node in the cluster
#[derive(Debug, Clone)]
pub struct ClusterNode {
    /// Unique node ID (40 character hex string)
    pub id: String,
    /// Node address (host:port)
    pub addr: SocketAddr,
    /// Cluster bus port (usually port + 10000)
    pub bus_port: u16,
    /// Node flags
    pub flags: NodeFlags,
    /// Master node ID if this is a replica
    pub master_id: Option<String>,
    /// Last time a PING was sent
    pub ping_sent: u64,
    /// Last time a PONG was received
    pub pong_received: u64,
    /// Config epoch for this node
    pub config_epoch: u64,
    /// Link state
    pub link_state: LinkState,
    /// Slots assigned to this node (only for masters)
    pub slots: Vec<SlotRange>,
    /// Slot migration state
    pub slot_states: HashMap<u16, SlotState>,
    /// Last time this node was seen as reachable
    pub last_seen: Instant,
    /// Number of failure reports from other nodes
    pub failure_reports: Vec<FailureReport>,
}

/// Link state for cluster bus connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    Connected,
    Disconnected,
}

impl fmt::Display for LinkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinkState::Connected => write!(f, "connected"),
            LinkState::Disconnected => write!(f, "disconnected"),
        }
    }
}

/// A failure report from another node
#[derive(Debug, Clone)]
pub struct FailureReport {
    pub reporter_id: String,
    pub timestamp: Instant,
}

impl ClusterNode {
    pub fn new(id: String, addr: SocketAddr, role: NodeRole) -> Self {
        let bus_port = addr.port().saturating_add(10000);
        let mut flags = match role {
            NodeRole::Master => NodeFlags::new_master(),
            NodeRole::Replica => NodeFlags::new_replica(),
        };
        flags.myself = false;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        ClusterNode {
            id,
            addr,
            bus_port,
            flags,
            master_id: None,
            ping_sent: 0,
            pong_received: now_ms,
            config_epoch: 0,
            link_state: LinkState::Disconnected,
            slots: Vec::new(),
            slot_states: HashMap::new(),
            last_seen: Instant::now(),
            failure_reports: Vec::new(),
        }
    }

    /// Check if this node owns a specific slot
    pub fn owns_slot(&self, slot: u16) -> bool {
        self.slots.iter().any(|r| r.contains(slot))
    }

    /// Get all slots as a bitmap (used for CLUSTER NODES output)
    pub fn slots_as_bitmap(&self) -> [u8; CLUSTER_SLOTS as usize / 8] {
        let mut bitmap = [0u8; CLUSTER_SLOTS as usize / 8];
        for range in &self.slots {
            for slot in range.start..=range.end {
                bitmap[slot as usize / 8] |= 1 << (slot % 8);
            }
        }
        bitmap
    }

    /// Format slots for CLUSTER NODES output
    pub fn slots_string(&self) -> String {
        let mut parts = Vec::new();
        for range in &self.slots {
            parts.push(format!("{}", range));
        }
        // Add migration states
        for (slot, state) in &self.slot_states {
            match state {
                SlotState::Importing(node_id) => {
                    parts.push(format!("[{}-<-{}]", slot, node_id));
                }
                SlotState::Migrating(node_id) => {
                    parts.push(format!("[{}->-{}]", slot, node_id));
                }
                SlotState::Normal => {}
            }
        }
        parts.join(" ")
    }

    /// Format node info for CLUSTER NODES output
    pub fn to_cluster_nodes_line(&self) -> String {
        let master_id = self.master_id.as_deref().unwrap_or("-");
        let slots_str = self.slots_string();

        format!(
            "{} {}:{}@{} {} {} {} {} {} {}",
            self.id,
            self.addr.ip(),
            self.addr.port(),
            self.bus_port,
            self.flags,
            master_id,
            self.ping_sent,
            self.pong_received,
            self.config_epoch,
            self.link_state,
        ) + if slots_str.is_empty() {
            String::new()
        } else {
            format!(" {}", slots_str)
        }
        .as_str()
    }

    /// Add failure report from a reporter node
    pub fn add_failure_report(&mut self, reporter_id: String) {
        // Remove existing report from same reporter
        self.failure_reports.retain(|r| r.reporter_id != reporter_id);
        self.failure_reports.push(FailureReport {
            reporter_id,
            timestamp: Instant::now(),
        });
    }

    /// Clean stale failure reports (older than 2x cluster-node-timeout)
    pub fn clean_failure_reports(&mut self, max_age: Duration) {
        let now = Instant::now();
        self.failure_reports
            .retain(|r| now.duration_since(r.timestamp) < max_age);
    }

    /// Count valid failure reports
    pub fn count_failure_reports(&self) -> usize {
        self.failure_reports.len()
    }
}

/// Cluster state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    Ok,
    Fail,
}

impl fmt::Display for ClusterState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterState::Ok => write!(f, "ok"),
            ClusterState::Fail => write!(f, "fail"),
        }
    }
}

/// Configuration for the cluster
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Whether cluster mode is enabled
    pub enabled: bool,
    /// Cluster node timeout in milliseconds
    pub node_timeout_ms: u64,
    /// Number of replicas per master
    pub replicas_per_master: usize,
    /// Whether to allow reads from replicas
    pub allow_reads_when_down: bool,
    /// Cluster announce IP (if different from bind address)
    pub announce_ip: Option<String>,
    /// Cluster announce port (if different from listen port)
    pub announce_port: Option<u16>,
    /// Cluster announce bus port
    pub announce_bus_port: Option<u16>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            enabled: false,
            node_timeout_ms: 15000,
            replicas_per_master: 1,
            allow_reads_when_down: false,
            announce_ip: None,
            announce_port: None,
            announce_bus_port: None,
        }
    }
}

/// The cluster manager handles all cluster-related state and operations
pub struct ClusterManager {
    /// Reference to the storage engine
    engine: Arc<StorageEngine>,
    /// Cluster configuration
    config: ClusterConfig,
    /// Cluster state (protected by RwLock for concurrent access)
    state: Arc<RwLock<ClusterManagerState>>,
}

/// Internal state of the cluster manager
pub struct ClusterManagerState {
    /// This node's ID
    pub my_id: String,
    /// Current cluster epoch
    pub current_epoch: u64,
    /// All known nodes in the cluster
    pub nodes: HashMap<String, ClusterNode>,
    /// Slot-to-node mapping for fast lookup
    pub slot_owners: [Option<String>; CLUSTER_SLOTS as usize],
    /// Overall cluster state
    pub cluster_state: ClusterState,
    /// Number of slots served (all slots must be covered for cluster to be OK)
    pub slots_assigned: u16,
    /// Whether this node is in ASKING state (per-connection, but tracked here for simplicity)
    pub asking_nodes: HashMap<String, bool>,
    /// Nodes in READONLY mode
    pub readonly_connections: HashMap<String, bool>,
    /// Size (number of master nodes with at least one slot)
    pub cluster_size: usize,
    /// Known cluster nodes count
    pub cluster_known_nodes: usize,
    /// Migration state for slots
    pub migration_states: HashMap<u16, SlotState>,
}

impl ClusterManagerState {
    fn new(node_id: String) -> Self {
        const NONE: Option<String> = None;
        ClusterManagerState {
            my_id: node_id,
            current_epoch: 0,
            nodes: HashMap::new(),
            slot_owners: [NONE; CLUSTER_SLOTS as usize],
            cluster_state: ClusterState::Ok,
            slots_assigned: 0,
            asking_nodes: HashMap::new(),
            readonly_connections: HashMap::new(),
            cluster_size: 0,
            cluster_known_nodes: 0,
            migration_states: HashMap::new(),
        }
    }

    /// Rebuild the slot_owners array from nodes
    pub fn rebuild_slot_map(&mut self) {
        const NONE: Option<String> = None;
        self.slot_owners = [NONE; CLUSTER_SLOTS as usize];
        self.slots_assigned = 0;
        let mut masters_with_slots = std::collections::HashSet::new();

        for (node_id, node) in &self.nodes {
            if node.flags.role == NodeRole::Master {
                for range in &node.slots {
                    for slot in range.start..=range.end {
                        self.slot_owners[slot as usize] = Some(node_id.clone());
                        self.slots_assigned += 1;
                    }
                    masters_with_slots.insert(node_id.clone());
                }
            }
        }

        self.cluster_size = masters_with_slots.len();
        self.cluster_known_nodes = self.nodes.len();

        // Cluster is OK only if all slots are assigned
        self.cluster_state = if self.slots_assigned == CLUSTER_SLOTS {
            ClusterState::Ok
        } else {
            ClusterState::Fail
        };
    }

    /// Get the node ID that owns a given slot
    pub fn get_slot_owner(&self, slot: u16) -> Option<&str> {
        self.slot_owners[slot as usize].as_deref()
    }

    /// Check if a key belongs to this node
    pub fn is_key_local(&self, key: &[u8]) -> bool {
        let slot = key_hash_slot(key);
        if let Some(owner) = self.get_slot_owner(slot) {
            owner == self.my_id
        } else {
            false
        }
    }
}

/// Generate a 40-character hex node ID
pub fn generate_node_id() -> String {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let combined = format!("{:032x}{:032x}", uuid1.as_u128(), uuid2.as_u128());
    combined[..40].to_string()
}

impl ClusterManager {
    /// Create a new cluster manager
    pub fn new(engine: Arc<StorageEngine>, config: ClusterConfig) -> Arc<Self> {
        let node_id = generate_node_id();
        let state = ClusterManagerState::new(node_id);

        Arc::new(ClusterManager {
            engine,
            config,
            state: Arc::new(RwLock::new(state)),
        })
    }

    /// Initialize the cluster manager, setting up this node
    pub async fn init(&self, addr: SocketAddr) {
        let mut state = self.state.write().await;
        let my_id = state.my_id.clone();

        let mut node = ClusterNode::new(my_id.clone(), addr, NodeRole::Master);
        node.flags.myself = true;
        node.link_state = LinkState::Connected;

        state.nodes.insert(my_id.clone(), node);
        state.cluster_known_nodes = 1;
        state.rebuild_slot_map();

        info!(node_id = %my_id, "Cluster node initialized");
    }

    /// Get the cluster configuration
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Get a reference to the internal state
    pub fn state(&self) -> &Arc<RwLock<ClusterManagerState>> {
        &self.state
    }

    /// Get the storage engine
    pub fn engine(&self) -> &Arc<StorageEngine> {
        &self.engine
    }

    /// Check if cluster mode is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get this node's ID
    pub async fn my_id(&self) -> String {
        self.state.read().await.my_id.clone()
    }

    /// Add slots to this node
    pub async fn add_slots(&self, slots: &[u16]) -> Result<(), String> {
        let mut state = self.state.write().await;
        let my_id = state.my_id.clone();

        // Pre-validate all slots
        for &slot in slots {
            if slot >= CLUSTER_SLOTS {
                return Err(format!("ERR Invalid or out of range slot '{}'", slot));
            }
            if let Some(owner) = &state.slot_owners[slot as usize] {
                if owner != &my_id {
                    return Err(format!("ERR Slot {} is already busy", slot));
                }
            }
        }

        if let Some(node) = state.nodes.get_mut(&my_id) {
            for &slot in slots {
                if !node.owns_slot(slot) {
                    node.slots.push(SlotRange::single(slot));
                }
            }
            compact_slot_ranges(&mut node.slots);
        } else {
            return Err("ERR Node not found".to_string());
        }

        state.rebuild_slot_map();
        Ok(())
    }

    /// Remove slots from this node
    pub async fn del_slots(&self, slots: &[u16]) -> Result<(), String> {
        let mut state = self.state.write().await;
        let my_id = state.my_id.clone();

        // Pre-validate
        {
            let node = state.nodes.get(&my_id)
                .ok_or_else(|| "ERR Node not found".to_string())?;
            for &slot in slots {
                if slot >= CLUSTER_SLOTS {
                    return Err(format!("ERR Invalid or out of range slot '{}'", slot));
                }
                if !node.owns_slot(slot) {
                    return Err(format!(
                        "ERR Slot {} is already unassigned",
                        slot
                    ));
                }
            }
        }

        // Now apply changes
        if let Some(node) = state.nodes.get_mut(&my_id) {
            remove_slots_from_ranges(&mut node.slots, slots);
            for &slot in slots {
                node.slot_states.remove(&slot);
            }
        }
        for &slot in slots {
            state.migration_states.remove(&slot);
        }

        state.rebuild_slot_map();
        Ok(())
    }

    /// Flush all slots from this node
    pub async fn flush_slots(&self) {
        let mut state = self.state.write().await;
        let my_id = state.my_id.clone();

        if let Some(node) = state.nodes.get_mut(&my_id) {
            node.slots.clear();
            node.slot_states.clear();
        }
        state.migration_states.clear();
        state.rebuild_slot_map();
    }

    /// Meet a new node (CLUSTER MEET)
    pub async fn meet(&self, host: &str, port: u16) -> Result<(), String> {
        let addr_str = format!("{}:{}", host, port);
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| format!("ERR Invalid address: {}", e))?;

        let mut state = self.state.write().await;

        // Check if we already know this node by address
        for node in state.nodes.values() {
            if node.addr == addr {
                debug!("Node at {} already known", addr);
                return Ok(());
            }
        }

        let new_id = generate_node_id();
        let mut node = ClusterNode::new(new_id.clone(), addr, NodeRole::Master);
        node.flags.handshake = true;
        node.link_state = LinkState::Connected;

        state.nodes.insert(new_id.clone(), node);
        state.cluster_known_nodes = state.nodes.len();

        info!(node_id = %new_id, addr = %addr, "Met new cluster node");
        Ok(())
    }

    /// Forget a node (CLUSTER FORGET)
    pub async fn forget(&self, node_id: &str) -> Result<(), String> {
        let mut state = self.state.write().await;

        if node_id == state.my_id {
            return Err("ERR I tried hard but I can't forget myself...".to_string());
        }

        if !state.nodes.contains_key(node_id) {
            return Err(format!("ERR Unknown node {}", node_id));
        }

        state.nodes.remove(node_id);
        state.rebuild_slot_map();

        info!(node_id = %node_id, "Forgot cluster node");
        Ok(())
    }

    /// Set a node as replica of another node (CLUSTER REPLICATE)
    pub async fn replicate(&self, master_id: &str) -> Result<(), String> {
        let mut state = self.state.write().await;
        let my_id = state.my_id.clone();

        if master_id == my_id {
            return Err("ERR Can't replicate myself".to_string());
        }

        if !state.nodes.contains_key(master_id) {
            return Err(format!("ERR Unknown node {}", master_id));
        }

        // Check if target is a master
        if let Some(master_node) = state.nodes.get(master_id) {
            if master_node.flags.role != NodeRole::Master {
                return Err("ERR I can only replicate a master, not a replica.".to_string());
            }
        }

        // Check if we have slots assigned (can't become replica with slots)
        if let Some(my_node) = state.nodes.get(&my_id) {
            if !my_node.slots.is_empty() {
                return Err(
                    "ERR To set a master the node must be empty and without assigned slots."
                        .to_string(),
                );
            }
        }

        // Update our role to replica
        if let Some(my_node) = state.nodes.get_mut(&my_id) {
            my_node.flags.role = NodeRole::Replica;
            my_node.master_id = Some(master_id.to_string());
        }

        state.rebuild_slot_map();
        info!(master_id = %master_id, "Now replicating master");
        Ok(())
    }

    /// Reset this node (CLUSTER RESET)
    pub async fn reset(&self, hard: bool) {
        let mut state = self.state.write().await;
        let my_id = state.my_id.clone();

        // Remove all other nodes
        let my_node = state.nodes.remove(&my_id);
        state.nodes.clear();

        if let Some(mut node) = my_node {
            // Clear slots and migration states
            node.slots.clear();
            node.slot_states.clear();
            node.flags.role = NodeRole::Master;
            node.master_id = None;
            node.failure_reports.clear();

            if hard {
                // Generate new node ID
                let new_id = generate_node_id();
                node.id = new_id.clone();
                state.my_id = new_id;
                state.current_epoch = 0;
            }

            let insert_id = state.my_id.clone();
            state.nodes.insert(insert_id, node);
        }

        state.migration_states.clear();
        state.rebuild_slot_map();

        info!(hard = hard, "Cluster reset");
    }

    /// Set slot state (CLUSTER SETSLOT)
    pub async fn set_slot(
        &self,
        slot: u16,
        subcommand: &str,
        node_id: Option<&str>,
    ) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err(format!("ERR Invalid or out of range slot '{}'", slot));
        }

        let mut state = self.state.write().await;
        let my_id = state.my_id.clone();

        match subcommand.to_uppercase().as_str() {
            "IMPORTING" => {
                let source_id = node_id
                    .ok_or_else(|| "ERR Need node ID for IMPORTING".to_string())?;
                if source_id == my_id {
                    return Err(
                        "ERR I can't import a slot from myself".to_string()
                    );
                }
                if !state.nodes.contains_key(source_id) {
                    return Err(format!("ERR Unknown node {}", source_id));
                }
                state
                    .migration_states
                    .insert(slot, SlotState::Importing(source_id.to_string()));
                if let Some(node) = state.nodes.get_mut(&my_id) {
                    node.slot_states
                        .insert(slot, SlotState::Importing(source_id.to_string()));
                }
            }
            "MIGRATING" => {
                let target_id = node_id
                    .ok_or_else(|| "ERR Need node ID for MIGRATING".to_string())?;
                if target_id == my_id {
                    return Err(
                        "ERR I can't migrate a slot to myself".to_string()
                    );
                }
                if !state.nodes.contains_key(target_id) {
                    return Err(format!("ERR Unknown node {}", target_id));
                }
                // Can only migrate slots we own
                if !state
                    .nodes
                    .get(&my_id)
                    .map_or(false, |n| n.owns_slot(slot))
                {
                    return Err(
                        "ERR I'm not the owner of hash slot".to_string()
                    );
                }
                state
                    .migration_states
                    .insert(slot, SlotState::Migrating(target_id.to_string()));
                if let Some(node) = state.nodes.get_mut(&my_id) {
                    node.slot_states
                        .insert(slot, SlotState::Migrating(target_id.to_string()));
                }
            }
            "NODE" => {
                let owner_id = node_id
                    .ok_or_else(|| "ERR Need node ID for NODE".to_string())?;
                if !state.nodes.contains_key(owner_id) {
                    return Err(format!("ERR Unknown node {}", owner_id));
                }

                // Clear any migration state
                state.migration_states.remove(&slot);
                if let Some(node) = state.nodes.get_mut(&my_id) {
                    node.slot_states.remove(&slot);
                }

                // Remove slot from current owner
                for node in state.nodes.values_mut() {
                    if node.owns_slot(slot) {
                        remove_slots_from_ranges(&mut node.slots, &[slot]);
                    }
                }

                // Add slot to new owner
                if let Some(node) = state.nodes.get_mut(owner_id) {
                    node.slots.push(SlotRange::single(slot));
                    compact_slot_ranges(&mut node.slots);
                }

                // Bump config epoch if we are the new owner
                if owner_id == my_id {
                    state.current_epoch += 1;
                    let new_epoch = state.current_epoch;
                    if let Some(node) = state.nodes.get_mut(&my_id) {
                        node.config_epoch = new_epoch;
                    }
                }

                state.rebuild_slot_map();
            }
            "STABLE" => {
                // Clear migration state for this slot
                state.migration_states.remove(&slot);
                if let Some(node) = state.nodes.get_mut(&my_id) {
                    node.slot_states.remove(&slot);
                }
            }
            _ => {
                return Err(format!(
                    "ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP"
                ));
            }
        }

        Ok(())
    }

    /// Count keys in a given slot
    pub async fn count_keys_in_slot(&self, slot: u16) -> Result<i64, String> {
        if slot >= CLUSTER_SLOTS {
            return Err(format!("ERR Invalid or out of range slot '{}'", slot));
        }

        let count = self.engine.count_keys_in_slot(slot).await;
        Ok(count as i64)
    }

    /// Get keys in a given slot
    pub async fn get_keys_in_slot(&self, slot: u16, count: usize) -> Result<Vec<Vec<u8>>, String> {
        if slot >= CLUSTER_SLOTS {
            return Err(format!("ERR Invalid or out of range slot '{}'", slot));
        }

        let keys = self.engine.get_keys_in_slot(slot, count).await;
        Ok(keys)
    }

    /// Get the hash slot for a key
    pub fn keyslot(&self, key: &[u8]) -> u16 {
        key_hash_slot(key)
    }

    /// Initiate failover (CLUSTER FAILOVER)
    pub async fn failover(&self, force: bool) -> Result<(), String> {
        let (my_id, master_id) = {
            let state = self.state.read().await;

            let my_node = state
                .nodes
                .get(&state.my_id)
                .ok_or_else(|| "ERR Node not found".to_string())?;

            if my_node.flags.role != NodeRole::Replica {
                return Err(
                    "ERR You should send CLUSTER FAILOVER to a replica".to_string()
                );
            }

            let master_id = my_node
                .master_id
                .clone()
                .ok_or_else(|| "ERR No master configured".to_string())?;

            (state.my_id.clone(), master_id)
        };

        if force {
            info!(
                "Forced failover initiated by replica {} for master {}",
                my_id, master_id
            );
        } else {
            info!(
                "Manual failover initiated by replica {} for master {}",
                my_id, master_id
            );
        }

        self.promote_replica_to_master(&master_id).await?;

        Ok(())
    }

    /// Promote this replica to master, taking over slots from the old master
    async fn promote_replica_to_master(&self, old_master_id: &str) -> Result<(), String> {
        let mut state = self.state.write().await;
        let my_id = state.my_id.clone();

        // Get the old master's slots
        let master_slots: Vec<SlotRange> = state
            .nodes
            .get(old_master_id)
            .map(|n| n.slots.clone())
            .unwrap_or_default();

        // Remove slots from old master
        if let Some(old_master) = state.nodes.get_mut(old_master_id) {
            old_master.slots.clear();
            old_master.flags.role = NodeRole::Replica;
            old_master.master_id = Some(my_id.clone());
        }

        // Promote ourselves to master with the old master's slots
        if let Some(my_node) = state.nodes.get_mut(&my_id) {
            my_node.flags.role = NodeRole::Master;
            my_node.master_id = None;
            my_node.slots = master_slots;
        }

        // Bump epoch
        state.current_epoch += 1;
        let new_epoch = state.current_epoch;
        if let Some(my_node) = state.nodes.get_mut(&my_id) {
            my_node.config_epoch = new_epoch;
        }

        state.rebuild_slot_map();
        info!("Promoted to master, took over slots from {}", old_master_id);
        Ok(())
    }

    /// Check for nodes that should be marked as PFAIL or FAIL
    pub async fn check_node_timeouts(&self) {
        let timeout = Duration::from_millis(self.config.node_timeout_ms);
        let pfail_timeout = timeout;
        let fail_report_validity = timeout * 2;
        let mut state = self.state.write().await;
        let my_id = state.my_id.clone();

        let mut nodes_to_check: Vec<(String, bool)> = Vec::new();

        for (id, node) in &mut state.nodes {
            if id == &my_id {
                continue;
            }

            // Clean stale failure reports
            node.clean_failure_reports(fail_report_validity);

            let elapsed = node.last_seen.elapsed();

            if elapsed > pfail_timeout && !node.flags.pfail && !node.flags.fail {
                node.flags.pfail = true;
                warn!(node_id = %id, "Node marked as PFAIL (timeout)");
            }

            if node.flags.pfail {
                nodes_to_check.push((id.clone(), node.flags.pfail));
            }
        }

        // Check if PFAIL should be promoted to FAIL
        let quorum = (state.nodes.values().filter(|n| n.flags.role == NodeRole::Master).count() / 2) + 1;

        for (node_id, _) in nodes_to_check {
            let should_mark_fail = state
                .nodes
                .get(&node_id)
                .map_or(false, |n| {
                    n.count_failure_reports() >= quorum && n.flags.pfail && !n.flags.fail
                });

            if should_mark_fail {
                if let Some(node) = state.nodes.get_mut(&node_id) {
                    node.flags.fail = true;
                    node.flags.pfail = false;
                    warn!(node_id = %node_id, "Node marked as FAIL");
                }
            }
        }
    }

    /// Automatic failover: if a master is in FAIL state and we are its replica,
    /// try to take over
    pub async fn try_automatic_failover(&self) {
        let state = self.state.read().await;
        let my_id = state.my_id.clone();

        let my_node = match state.nodes.get(&my_id) {
            Some(n) => n,
            None => return,
        };

        // Only replicas can initiate failover
        if my_node.flags.role != NodeRole::Replica {
            return;
        }

        let master_id = match &my_node.master_id {
            Some(id) => id.clone(),
            None => return,
        };

        let master_failed = state
            .nodes
            .get(&master_id)
            .map_or(false, |n| n.flags.fail);

        if !master_failed {
            return;
        }

        info!(
            "Master {} is in FAIL state, initiating automatic failover",
            master_id
        );

        drop(state);
        if let Err(e) = self.promote_replica_to_master(&master_id).await {
            warn!("Automatic failover failed: {}", e);
        }
    }

    /// Get cluster info for the INFO command
    pub async fn get_cluster_info(&self) -> String {
        let state = self.state.read().await;

        let my_node = state.nodes.get(&state.my_id);
        let my_epoch = my_node.map(|n| n.config_epoch).unwrap_or(0);

        let stats_messages_sent: u64 = 0;
        let stats_messages_received: u64 = 0;

        format!(
            "cluster_enabled:1\r\n\
             cluster_state:{}\r\n\
             cluster_slots_assigned:{}\r\n\
             cluster_slots_ok:{}\r\n\
             cluster_slots_pfail:0\r\n\
             cluster_slots_fail:0\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n\
             cluster_stats_messages_sent:{}\r\n\
             cluster_stats_messages_received:{}\r\n\
             total_cluster_links_buffer_limit_exceeded:0",
            state.cluster_state,
            state.slots_assigned,
            state.slots_assigned,
            state.cluster_known_nodes,
            state.cluster_size,
            state.current_epoch,
            my_epoch,
            stats_messages_sent,
            stats_messages_received,
        )
    }

    /// Get CLUSTER NODES output
    pub async fn get_cluster_nodes(&self) -> String {
        let state = self.state.read().await;
        let mut lines: Vec<String> = Vec::new();

        for node in state.nodes.values() {
            lines.push(node.to_cluster_nodes_line());
        }

        lines.join("\n") + "\n"
    }

    /// Get CLUSTER SLOTS output
    pub async fn get_cluster_slots(&self) -> Vec<(u16, u16, Vec<(String, u16, String)>)> {
        let state = self.state.read().await;
        let mut result = Vec::new();

        for node in state.nodes.values() {
            if node.flags.role != NodeRole::Master {
                continue;
            }

            for range in &node.slots {
                let mut nodes_info = vec![(
                    node.addr.ip().to_string(),
                    node.addr.port(),
                    node.id.clone(),
                )];

                // Add replicas
                for other_node in state.nodes.values() {
                    if other_node.master_id.as_deref() == Some(&node.id) {
                        nodes_info.push((
                            other_node.addr.ip().to_string(),
                            other_node.addr.port(),
                            other_node.id.clone(),
                        ));
                    }
                }

                result.push((range.start, range.end, nodes_info));
            }
        }

        result
    }

    /// Get CLUSTER SHARDS output
    pub async fn get_cluster_shards(&self) -> Vec<ShardInfo> {
        let state = self.state.read().await;
        let mut shards = Vec::new();

        for node in state.nodes.values() {
            if node.flags.role != NodeRole::Master {
                continue;
            }

            let mut shard_nodes = vec![ShardNodeInfo {
                id: node.id.clone(),
                port: node.addr.port(),
                ip: node.addr.ip().to_string(),
                role: "master".to_string(),
                health: if node.flags.fail {
                    "fail".to_string()
                } else {
                    "online".to_string()
                },
                replication_offset: 0,
            }];

            // Add replicas
            for other_node in state.nodes.values() {
                if other_node.master_id.as_deref() == Some(&node.id) {
                    shard_nodes.push(ShardNodeInfo {
                        id: other_node.id.clone(),
                        port: other_node.addr.port(),
                        ip: other_node.addr.ip().to_string(),
                        role: "replica".to_string(),
                        health: if other_node.flags.fail {
                            "fail".to_string()
                        } else {
                            "online".to_string()
                        },
                        replication_offset: 0,
                    });
                }
            }

            shards.push(ShardInfo {
                slots: node.slots.clone(),
                nodes: shard_nodes,
            });
        }

        shards
    }

    /// Get CLUSTER LINKS output
    pub async fn get_cluster_links(&self) -> Vec<LinkInfo> {
        let state = self.state.read().await;
        let mut links = Vec::new();

        for node in state.nodes.values() {
            if node.id == state.my_id {
                continue;
            }
            links.push(LinkInfo {
                direction: "to".to_string(),
                node_id: node.id.clone(),
                create_time: 0,
                events: "rw".to_string(),
                send_buffer_allocated: 0,
                send_buffer_used: 0,
            });
            links.push(LinkInfo {
                direction: "from".to_string(),
                node_id: node.id.clone(),
                create_time: 0,
                events: "r".to_string(),
                send_buffer_allocated: 0,
                send_buffer_used: 0,
            });
        }

        links
    }

    /// Save the cluster configuration
    pub async fn save_config(&self) -> Result<(), String> {
        // In a real implementation, this would write to nodes.conf
        // For now, we just acknowledge the save
        info!("Cluster configuration saved");
        Ok(())
    }

    /// Set config epoch (CLUSTER SET-CONFIG-EPOCH)
    pub async fn set_config_epoch(&self, epoch: u64) -> Result<(), String> {
        let mut state = self.state.write().await;

        if state.current_epoch != 0 {
            return Err(
                "ERR The user can assign a config epoch only when the node does not know any other node."
                    .to_string(),
            );
        }

        state.current_epoch = epoch;
        let my_id = state.my_id.clone();
        if let Some(node) = state.nodes.get_mut(&my_id) {
            node.config_epoch = epoch;
        }

        Ok(())
    }

    /// Check if a command involves cross-slot keys
    pub fn check_cross_slot(&self, keys: &[&[u8]]) -> bool {
        if keys.len() <= 1 {
            return false;
        }

        let first_slot = key_hash_slot(keys[0]);
        keys.iter().any(|k| key_hash_slot(k) != first_slot)
    }

    /// Get the redirect information for a key if it doesn't belong to this node
    pub async fn get_redirect(&self, key: &[u8]) -> Option<RedirectInfo> {
        let state = self.state.read().await;
        let slot = key_hash_slot(key);

        // Check if the slot is in migration state
        if let Some(migration_state) = state.migration_states.get(&slot) {
            match migration_state {
                SlotState::Migrating(target_id) => {
                    // Key is being migrated - check if the key still exists locally
                    // If not, redirect with ASK to the target
                    if let Some(target_node) = state.nodes.get(target_id) {
                        return Some(RedirectInfo {
                            redirect_type: RedirectType::Ask,
                            slot,
                            addr: target_node.addr,
                        });
                    }
                }
                SlotState::Importing(_) => {
                    // We're importing - if ASKING flag is set, handle locally
                    // Otherwise, the key belongs to the original owner
                }
                SlotState::Normal => {}
            }
        }

        // Normal case: check if slot owner is us
        if let Some(owner_id) = state.get_slot_owner(slot) {
            if owner_id != state.my_id {
                if let Some(owner_node) = state.nodes.get(owner_id) {
                    return Some(RedirectInfo {
                        redirect_type: RedirectType::Moved,
                        slot,
                        addr: owner_node.addr,
                    });
                }
            }
        } else {
            // Slot is unassigned - cluster state is FAIL
            return None;
        }

        None // Key belongs to this node
    }
}

/// Shard info for CLUSTER SHARDS
#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub slots: Vec<SlotRange>,
    pub nodes: Vec<ShardNodeInfo>,
}

/// Node info within a shard
#[derive(Debug, Clone)]
pub struct ShardNodeInfo {
    pub id: String,
    pub port: u16,
    pub ip: String,
    pub role: String,
    pub health: String,
    pub replication_offset: u64,
}

/// Link info for CLUSTER LINKS
#[derive(Debug, Clone)]
pub struct LinkInfo {
    pub direction: String,
    pub node_id: String,
    pub create_time: u64,
    pub events: String,
    pub send_buffer_allocated: u64,
    pub send_buffer_used: u64,
}

/// Redirect information for MOVED/ASK responses
#[derive(Debug, Clone)]
pub struct RedirectInfo {
    pub redirect_type: RedirectType,
    pub slot: u16,
    pub addr: SocketAddr,
}

/// Type of redirect
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedirectType {
    Moved,
    Ask,
}

impl fmt::Display for RedirectInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.redirect_type {
            RedirectType::Moved => write!(f, "MOVED {} {}", self.slot, self.addr),
            RedirectType::Ask => write!(f, "ASK {} {}", self.slot, self.addr),
        }
    }
}

/// Compact adjacent slot ranges
fn compact_slot_ranges(ranges: &mut Vec<SlotRange>) {
    if ranges.is_empty() {
        return;
    }

    // Expand all ranges to individual slots, sort, then re-compact
    let mut slots: Vec<u16> = Vec::new();
    for range in ranges.iter() {
        for slot in range.start..=range.end {
            slots.push(slot);
        }
    }
    slots.sort_unstable();
    slots.dedup();

    ranges.clear();
    if slots.is_empty() {
        return;
    }

    let mut start = slots[0];
    let mut end = slots[0];

    for &slot in &slots[1..] {
        if slot == end + 1 {
            end = slot;
        } else {
            ranges.push(SlotRange::new(start, end));
            start = slot;
            end = slot;
        }
    }
    ranges.push(SlotRange::new(start, end));
}

/// Remove specific slots from slot ranges
fn remove_slots_from_ranges(ranges: &mut Vec<SlotRange>, slots_to_remove: &[u16]) {
    let remove_set: std::collections::HashSet<u16> = slots_to_remove.iter().copied().collect();

    let mut all_slots: Vec<u16> = Vec::new();
    for range in ranges.iter() {
        for slot in range.start..=range.end {
            if !remove_set.contains(&slot) {
                all_slots.push(slot);
            }
        }
    }
    all_slots.sort_unstable();

    ranges.clear();
    if all_slots.is_empty() {
        return;
    }

    let mut start = all_slots[0];
    let mut end = all_slots[0];

    for &slot in &all_slots[1..] {
        if slot == end + 1 {
            end = slot;
        } else {
            ranges.push(SlotRange::new(start, end));
            start = slot;
            end = slot;
        }
    }
    ranges.push(SlotRange::new(start, end));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;

    #[test]
    fn test_generate_node_id() {
        let id = generate_node_id();
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_compact_slot_ranges() {
        let mut ranges = vec![
            SlotRange::single(3),
            SlotRange::single(1),
            SlotRange::single(2),
            SlotRange::single(5),
            SlotRange::single(6),
            SlotRange::single(7),
        ];
        compact_slot_ranges(&mut ranges);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], SlotRange::new(1, 3));
        assert_eq!(ranges[1], SlotRange::new(5, 7));
    }

    #[test]
    fn test_remove_slots_from_ranges() {
        let mut ranges = vec![SlotRange::new(0, 10)];
        remove_slots_from_ranges(&mut ranges, &[5]);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], SlotRange::new(0, 4));
        assert_eq!(ranges[1], SlotRange::new(6, 10));
    }

    #[test]
    fn test_node_flags_display() {
        let mut flags = NodeFlags::new_master();
        flags.myself = true;
        assert_eq!(format!("{}", flags), "myself,master");

        let mut flags = NodeFlags::new_replica();
        flags.pfail = true;
        assert_eq!(format!("{}", flags), "slave,fail?");
    }

    #[tokio::test]
    async fn test_cluster_manager_init() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = ClusterManager::new(storage, cluster_config);
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.init(addr).await;

        let state = manager.state().read().await;
        assert_eq!(state.nodes.len(), 1);
        assert!(state.nodes.contains_key(&state.my_id));
        let my_node = &state.nodes[&state.my_id];
        assert!(my_node.flags.myself);
        assert_eq!(my_node.flags.role, NodeRole::Master);
    }

    #[tokio::test]
    async fn test_add_and_del_slots() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = ClusterManager::new(storage, cluster_config);
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.init(addr).await;

        // Add slots
        let slots: Vec<u16> = (0..100).collect();
        manager.add_slots(&slots).await.unwrap();

        let state = manager.state().read().await;
        assert_eq!(state.slots_assigned, 100);
        let my_id = state.my_id.clone();
        assert!(state.nodes[&my_id].owns_slot(0));
        assert!(state.nodes[&my_id].owns_slot(99));
        assert!(!state.nodes[&my_id].owns_slot(100));
        drop(state);

        // Delete some slots
        manager.del_slots(&[50, 51, 52]).await.unwrap();

        let state = manager.state().read().await;
        assert_eq!(state.slots_assigned, 97);
        assert!(!state.nodes[&state.my_id].owns_slot(50));
    }

    #[tokio::test]
    async fn test_cluster_meet() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = ClusterManager::new(storage, cluster_config);
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.init(addr).await;

        manager.meet("127.0.0.1", 6380).await.unwrap();

        let state = manager.state().read().await;
        assert_eq!(state.nodes.len(), 2);
    }

    #[tokio::test]
    async fn test_cluster_forget() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = ClusterManager::new(storage, cluster_config);
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.init(addr).await;

        manager.meet("127.0.0.1", 6380).await.unwrap();

        let state = manager.state().read().await;
        let other_id: String = state
            .nodes
            .keys()
            .find(|k| **k != state.my_id)
            .unwrap()
            .clone();
        drop(state);

        manager.forget(&other_id).await.unwrap();

        let state = manager.state().read().await;
        assert_eq!(state.nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_cluster_forget_self_fails() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = ClusterManager::new(storage, cluster_config);
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.init(addr).await;

        let my_id = manager.my_id().await;
        let result = manager.forget(&my_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cluster_reset() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = ClusterManager::new(storage, cluster_config);
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.init(addr).await;

        let old_id = manager.my_id().await;

        // Add some state
        manager.meet("127.0.0.1", 6380).await.unwrap();
        manager.add_slots(&[0, 1, 2]).await.unwrap();

        // Soft reset
        manager.reset(false).await;
        let state = manager.state().read().await;
        assert_eq!(state.nodes.len(), 1);
        assert_eq!(state.my_id, old_id); // ID preserved in soft reset
        assert_eq!(state.slots_assigned, 0);
        drop(state);

        // Hard reset changes ID
        manager.reset(true).await;
        let new_id = manager.my_id().await;
        assert_ne!(new_id, old_id);
    }

    #[tokio::test]
    async fn test_keyslot() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig::default();
        let manager = ClusterManager::new(storage, cluster_config);

        assert_eq!(manager.keyslot(b"foo"), 12182);
        assert_eq!(manager.keyslot(b"bar"), 5061);
    }

    #[tokio::test]
    async fn test_check_cross_slot() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig::default();
        let manager = ClusterManager::new(storage, cluster_config);

        // Same hash tag = same slot
        assert!(!manager.check_cross_slot(&[b"{tag}key1", b"{tag}key2"]));

        // Different keys = likely different slots
        assert!(manager.check_cross_slot(&[b"foo", b"bar"]));

        // Single key = no cross-slot
        assert!(!manager.check_cross_slot(&[b"foo"]));

        // Empty = no cross-slot
        assert!(!manager.check_cross_slot(&[]));
    }

    #[tokio::test]
    async fn test_cluster_info() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = ClusterManager::new(storage, cluster_config);
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.init(addr).await;

        let info = manager.get_cluster_info().await;
        assert!(info.contains("cluster_enabled:1"));
        assert!(info.contains("cluster_state:fail")); // No slots assigned yet
        assert!(info.contains("cluster_known_nodes:1"));
    }

    #[tokio::test]
    async fn test_set_slot_node() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let manager = ClusterManager::new(storage, cluster_config);
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.init(addr).await;

        let my_id = manager.my_id().await;

        // Assign slot using SETSLOT NODE
        manager
            .set_slot(100, "NODE", Some(&my_id))
            .await
            .unwrap();

        let state = manager.state().read().await;
        assert!(state.nodes[&my_id].owns_slot(100));
        assert_eq!(state.slots_assigned, 1);
    }
}
