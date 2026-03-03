#![allow(dead_code)]

pub mod client;
pub mod config;
pub mod error;
pub mod server;
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock, mpsc};
use uuid::Uuid;

use crate::networking::resp::{RespCommand, RespValue};
use crate::storage::engine::StorageEngine;

/// Replication manager that handles all replication operations.
/// This is the central shared state for replication, accessible by both
/// the Server (for command propagation) and CommandHandler (for ROLE/REPLICAOF/WAIT).
pub struct ReplicationManager {
    engine: Arc<StorageEngine>,
    config: config::ReplicationConfig,
    state: Arc<RwLock<ReplicationState>>,
    /// Replication backlog - circular buffer of serialized commands with byte offsets
    backlog: Arc<RwLock<ReplicationBacklog>>,
    /// Channels to send commands to connected replicas
    replica_senders: Arc<RwLock<Vec<ReplicaSender>>>,
    /// Master replication ID (40-char hex string)
    master_replid: Arc<RwLock<String>>,
    /// Secondary replication ID (for partial resync after failover)
    master_replid2: Arc<RwLock<String>>,
    /// Current master replication offset (byte-based)
    master_repl_offset: Arc<AtomicU64>,
    /// Second replication offset (for partial resync after failover)
    second_repl_offset: Arc<AtomicU64>,
    /// Whether this server is in read-only replica mode
    read_only: Arc<AtomicBool>,
    /// Notify when replica offsets change (for WAIT)
    offset_changed: Arc<Notify>,
}

/// Replication state
#[derive(Debug, Clone)]
pub struct ReplicationState {
    /// Role of this server (master or replica)
    pub role: ReplicationRole,
    /// Master host (if this is a replica)
    pub master_host: Option<String>,
    /// Master port (if this is a replica)
    pub master_port: Option<u16>,
    /// Master connection state (if this is a replica)
    pub master_link_status: MasterLinkStatus,
    /// Replication offset
    pub replication_offset: u64,
    /// Connected replicas (if this is a master)
    pub connected_replicas: Vec<ReplicaInfo>,
}

/// Replication role
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    /// Master server
    Master,
    /// Replica server
    Replica,
}

/// Master link status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterLinkStatus {
    /// Connected to master
    Up,
    /// Disconnected from master
    Down,
}

/// Information about a connected replica
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    /// Replica ID
    pub id: String,
    /// Replica host
    pub host: String,
    /// Replica port
    pub port: u16,
    /// Replication offset acknowledged by this replica
    pub offset: u64,
    /// Last ping time
    pub last_ping: Instant,
}

/// Channel for sending commands to a specific replica
struct ReplicaSender {
    id: String,
    tx: mpsc::Sender<Vec<u8>>,
    offset: Arc<AtomicU64>,
}

/// Replication backlog - stores recent commands for partial resynchronization
pub struct ReplicationBacklog {
    /// The buffer storing serialized RESP commands
    buffer: VecDeque<u8>,
    /// Maximum size of the backlog in bytes
    max_size: usize,
    /// The replication offset of the first byte in the buffer
    first_byte_offset: u64,
    /// Total bytes ever written (current offset)
    current_offset: u64,
}

impl ReplicationBacklog {
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(max_size),
            max_size,
            first_byte_offset: 0,
            current_offset: 0,
        }
    }

    /// Append data to the backlog, trimming old data if needed
    pub fn append(&mut self, data: &[u8]) {
        self.current_offset += data.len() as u64;

        // If the data itself exceeds max_size, only keep the tail
        let data = if data.len() > self.max_size {
            self.buffer.clear();
            let skip = data.len() - self.max_size;
            self.first_byte_offset = self.current_offset - self.max_size as u64;
            &data[skip..]
        } else {
            // Trim from the front to make room
            let total_after = self.buffer.len() + data.len();
            if total_after > self.max_size {
                let to_remove = total_after - self.max_size;
                let actual_remove = to_remove.min(self.buffer.len());
                self.buffer.drain(..actual_remove);
                self.first_byte_offset += actual_remove as u64;
            }
            data
        };

        self.buffer.extend(data);
    }

    /// Get data from the backlog starting at a given offset.
    /// Returns None if the requested offset is no longer in the backlog.
    pub fn get_from_offset(&self, offset: u64) -> Option<Vec<u8>> {
        if offset < self.first_byte_offset {
            return None; // Data has been trimmed
        }
        if offset > self.current_offset {
            return None; // Offset is in the future
        }

        let start = (offset - self.first_byte_offset) as usize;
        if start > self.buffer.len() {
            return None;
        }

        let data: Vec<u8> = self.buffer.iter().skip(start).copied().collect();
        Some(data)
    }

    /// Get the current offset (total bytes written)
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// Check if partial resync is possible from a given offset
    pub fn can_partial_resync(&self, offset: u64) -> bool {
        offset >= self.first_byte_offset && offset <= self.current_offset
    }
}

/// Generate a 40-character hex replication ID (like Redis)
fn generate_replid() -> String {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let combined = format!("{:032x}{:032x}", uuid1.as_u128(), uuid2.as_u128());
    combined[..40].to_string()
}

impl ReplicationManager {
    /// Create a new replication manager
    pub fn new(engine: Arc<StorageEngine>, config: config::ReplicationConfig) -> Arc<Self> {
        let backlog_size = config.replication_backlog_size;
        let state = Arc::new(RwLock::new(ReplicationState {
            role: ReplicationRole::Master,
            master_host: None,
            master_port: None,
            master_link_status: MasterLinkStatus::Down,
            replication_offset: 0,
            connected_replicas: Vec::new(),
        }));

        Arc::new(Self {
            engine,
            config,
            state,
            backlog: Arc::new(RwLock::new(ReplicationBacklog::new(backlog_size))),
            replica_senders: Arc::new(RwLock::new(Vec::new())),
            master_replid: Arc::new(RwLock::new(generate_replid())),
            master_replid2: Arc::new(RwLock::new(
                "0000000000000000000000000000000000000000".to_string(),
            )),
            master_repl_offset: Arc::new(AtomicU64::new(0)),
            second_repl_offset: Arc::new(AtomicU64::new(-1i64 as u64)),
            read_only: Arc::new(AtomicBool::new(false)),
            offset_changed: Arc::new(Notify::new()),
        })
    }

    /// Initialize the replication manager
    pub async fn init(&self) -> Result<(), error::ReplicationError> {
        // If we're configured as a replica, connect to the master
        if let Some(master_host) = &self.config.master_host
            && let Some(master_port) = self.config.master_port
        {
            self.connect_to_master(master_host.clone(), master_port)
                .await?;
        }

        Ok(())
    }

    /// Connect to a master server (become a replica)
    pub async fn connect_to_master(
        &self,
        host: String,
        port: u16,
    ) -> Result<(), error::ReplicationError> {
        // Update our state to be a replica
        {
            let mut state = self.state.write().await;
            state.role = ReplicationRole::Replica;
            state.master_host = Some(host.clone());
            state.master_port = Some(port);
            state.master_link_status = MasterLinkStatus::Down;
        }

        // Set read-only mode
        self.read_only.store(true, Ordering::SeqCst);

        // Create a replication client and connect to the master
        let client =
            client::ReplicationClient::new(self.engine.clone(), host, port, self.state.clone());

        // Start the replication client in a background task
        tokio::spawn(async move {
            if let Err(e) = client.start().await {
                tracing::error!("Replication client error: {}", e);
            }
        });

        Ok(())
    }

    /// Disconnect from master and become a master
    pub async fn disconnect_from_master(&self) -> Result<(), error::ReplicationError> {
        let mut state = self.state.write().await;
        state.role = ReplicationRole::Master;
        state.master_host = None;
        state.master_port = None;
        state.master_link_status = MasterLinkStatus::Down;

        // Disable read-only mode
        self.read_only.store(false, Ordering::SeqCst);

        // Generate a new replication ID
        *self.master_replid.write().await = generate_replid();

        Ok(())
    }

    /// Propagate a write command to all connected replicas.
    /// Called by the server after processing a write command.
    pub async fn propagate_command(&self, command: &RespCommand) {
        let state = self.state.read().await;
        if state.role != ReplicationRole::Master {
            return;
        }
        drop(state);

        // Serialize the command to RESP format
        let serialized = Self::serialize_command(command);

        // Add to backlog
        {
            let mut backlog = self.backlog.write().await;
            backlog.append(&serialized);
        }

        // Update master offset
        self.master_repl_offset
            .fetch_add(serialized.len() as u64, Ordering::SeqCst);

        // Send to all connected replicas using try_send to avoid blocking
        // the write path if a replica's channel buffer is full
        let senders = self.replica_senders.read().await;
        let mut failed_ids = Vec::new();

        for sender in senders.iter() {
            match sender.tx.try_send(serialized.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(
                        "Replica {} channel full, disconnecting to force resync",
                        sender.id
                    );
                    failed_ids.push(sender.id.clone());
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    failed_ids.push(sender.id.clone());
                }
            }
        }

        drop(senders);

        // Clean up failed senders
        if !failed_ids.is_empty() {
            let mut senders = self.replica_senders.write().await;
            senders.retain(|s| !failed_ids.contains(&s.id));

            let mut state = self.state.write().await;
            state
                .connected_replicas
                .retain(|r| !failed_ids.contains(&r.id));
        }
    }

    /// Register a new replica and return a receiver for commands
    pub async fn register_replica(
        &self,
        id: String,
        host: String,
        port: u16,
    ) -> mpsc::Receiver<Vec<u8>> {
        let (tx, rx) = mpsc::channel(1000);
        let offset = Arc::new(AtomicU64::new(0));

        let sender = ReplicaSender {
            id: id.clone(),
            tx,
            offset,
        };

        self.replica_senders.write().await.push(sender);

        let mut state = self.state.write().await;
        state.connected_replicas.push(ReplicaInfo {
            id,
            host,
            port,
            offset: 0,
            last_ping: Instant::now(),
        });

        rx
    }

    /// Unregister a replica
    pub async fn unregister_replica(&self, id: &str) {
        self.replica_senders.write().await.retain(|s| s.id != id);
        let mut state = self.state.write().await;
        state.connected_replicas.retain(|r| r.id != id);
    }

    /// Update a replica's acknowledged offset (from REPLCONF ACK)
    pub async fn update_replica_offset(&self, id: &str, offset: u64) {
        // Update in senders
        let senders = self.replica_senders.read().await;
        for sender in senders.iter() {
            if sender.id == id {
                sender.offset.store(offset, Ordering::SeqCst);
                break;
            }
        }
        drop(senders);

        // Update in state
        let mut state = self.state.write().await;
        for replica in &mut state.connected_replicas {
            if replica.id == id {
                replica.offset = offset;
                replica.last_ping = Instant::now();
                break;
            }
        }

        // Notify WAIT waiters
        self.offset_changed.notify_waiters();
    }

    /// WAIT implementation - wait for replicas to acknowledge up to current offset
    pub async fn wait_for_replicas(&self, num_replicas: usize, timeout: Duration) -> i64 {
        let target_offset = self.master_repl_offset.load(Ordering::SeqCst);
        let deadline = Instant::now() + timeout;

        loop {
            // Count replicas that have acknowledged
            let count = self.count_replicas_at_offset(target_offset).await;
            if count >= num_replicas || Instant::now() >= deadline {
                return count as i64;
            }

            // Wait for offset change or timeout
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return count as i64;
            }

            tokio::select! {
                _ = self.offset_changed.notified() => {
                    // Re-check
                }
                _ = tokio::time::sleep(remaining) => {
                    return self.count_replicas_at_offset(target_offset).await as i64;
                }
            }
        }
    }

    /// Count replicas that have acknowledged at least the given offset
    async fn count_replicas_at_offset(&self, target_offset: u64) -> usize {
        let senders = self.replica_senders.read().await;
        senders
            .iter()
            .filter(|s| s.offset.load(Ordering::SeqCst) >= target_offset)
            .count()
    }

    /// Get the ROLE response for this server
    pub async fn get_role_response(&self) -> RespValue {
        let state = self.state.read().await;
        match state.role {
            ReplicationRole::Master => {
                let offset = self.master_repl_offset.load(Ordering::SeqCst) as i64;
                let mut replicas = Vec::new();
                for r in &state.connected_replicas {
                    replicas.push(RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(r.host.as_bytes().to_vec())),
                        RespValue::BulkString(Some(r.port.to_string().into_bytes())),
                        RespValue::BulkString(Some(r.offset.to_string().into_bytes())),
                    ])));
                }
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"master".to_vec())),
                    RespValue::Integer(offset),
                    RespValue::Array(Some(replicas)),
                ]))
            }
            ReplicationRole::Replica => {
                let host = state.master_host.clone().unwrap_or_default();
                let port = state.master_port.unwrap_or(0) as i64;
                let link_status = match state.master_link_status {
                    MasterLinkStatus::Up => "connected",
                    MasterLinkStatus::Down => "connect",
                };
                let offset = state.replication_offset as i64;
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"slave".to_vec())),
                    RespValue::BulkString(Some(host.into_bytes())),
                    RespValue::Integer(port),
                    RespValue::BulkString(Some(link_status.as_bytes().to_vec())),
                    RespValue::Integer(offset),
                ]))
            }
        }
    }

    /// Handle REPLICAOF/SLAVEOF command
    pub async fn handle_replicaof(
        &self,
        host: &str,
        port_str: &str,
    ) -> Result<RespValue, error::ReplicationError> {
        if host.eq_ignore_ascii_case("NO") && port_str.eq_ignore_ascii_case("ONE") {
            // REPLICAOF NO ONE - become a master
            self.disconnect_from_master().await?;
            Ok(RespValue::SimpleString("OK".to_string()))
        } else {
            let port: u16 = port_str.parse().map_err(|_| {
                error::ReplicationError::Protocol("Invalid port number".to_string())
            })?;
            self.connect_to_master(host.to_string(), port).await?;
            Ok(RespValue::SimpleString("OK".to_string()))
        }
    }

    /// Handle FAILOVER command
    pub async fn handle_failover(&self) -> Result<RespValue, error::ReplicationError> {
        let state = self.state.read().await;
        if state.role != ReplicationRole::Master {
            return Ok(RespValue::Error(
                "ERR FAILOVER requires the server to be a master".to_string(),
            ));
        }
        if state.connected_replicas.is_empty() {
            return Ok(RespValue::Error(
                "ERR FAILOVER requires connected replicas".to_string(),
            ));
        }
        drop(state);

        // Set read-only mode on master to stop accepting writes
        self.read_only.store(true, Ordering::SeqCst);

        // Wait briefly for replicas to catch up
        let _count = self.wait_for_replicas(1, Duration::from_secs(5)).await;

        // Spawn a background timeout to abort failover if it doesn't complete.
        // If the node is still a master after the timeout, reset read_only so
        // writes are no longer blocked indefinitely.
        let read_only = self.read_only.clone();
        let state_ref = self.state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let state = state_ref.read().await;
            if state.role == ReplicationRole::Master && read_only.load(Ordering::SeqCst) {
                tracing::warn!("FAILOVER timeout: resetting read-only mode on master");
                read_only.store(false, Ordering::SeqCst);
            }
        });

        Ok(RespValue::SimpleString("OK".to_string()))
    }

    /// Check if this server should reject write commands (replica read-only mode)
    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::SeqCst)
    }

    /// Get the current replication state
    pub async fn get_state(&self) -> ReplicationState {
        self.state.read().await.clone()
    }

    /// Get the master replication ID
    pub async fn get_master_replid(&self) -> String {
        self.master_replid.read().await.clone()
    }

    /// Get the secondary replication ID
    pub async fn get_master_replid2(&self) -> String {
        self.master_replid2.read().await.clone()
    }

    /// Get the current master replication offset
    pub fn get_master_repl_offset(&self) -> u64 {
        self.master_repl_offset.load(Ordering::SeqCst)
    }

    /// Get the second replication offset
    pub fn get_second_repl_offset(&self) -> u64 {
        self.second_repl_offset.load(Ordering::SeqCst)
    }

    /// Get a reference to the storage engine
    pub fn engine(&self) -> &Arc<StorageEngine> {
        &self.engine
    }

    /// Get a reference to the backlog
    pub fn backlog(&self) -> &Arc<RwLock<ReplicationBacklog>> {
        &self.backlog
    }

    /// Get a reference to the config
    pub fn config(&self) -> &config::ReplicationConfig {
        &self.config
    }

    /// Check if partial resync is possible for given replid and offset
    pub async fn can_partial_resync(&self, replid: &str, offset: u64) -> bool {
        let our_replid = self.master_replid.read().await;
        if *our_replid != replid {
            return false;
        }
        let backlog = self.backlog.read().await;
        backlog.can_partial_resync(offset)
    }

    /// Serialize a command to RESP wire format for replication
    fn serialize_command(command: &RespCommand) -> Vec<u8> {
        let mut parts = Vec::with_capacity(1 + command.args.len());
        parts.push(RespValue::BulkString(Some(command.name.clone())));
        parts.extend(
            command
                .args
                .iter()
                .map(|arg| RespValue::BulkString(Some(arg.clone()))),
        );

        let cmd_array = RespValue::Array(Some(parts));
        cmd_array.serialize().unwrap_or_default()
    }

    /// Check if a command is a write command that should be propagated to replicas
    pub fn is_write_command(cmd_name: &str) -> bool {
        matches!(
            cmd_name,
            "set"
                | "setnx"
                | "setex"
                | "psetex"
                | "mset"
                | "msetnx"
                | "getset"
                | "getex"
                | "getdel"
                | "append"
                | "setrange"
                | "incr"
                | "decr"
                | "incrby"
                | "decrby"
                | "incrbyfloat"
                | "del"
                | "unlink"
                | "expire"
                | "pexpire"
                | "expireat"
                | "pexpireat"
                | "persist"
                | "rename"
                | "renamenx"
                | "copy"
                | "move"
                | "sort"
                | "lpush"
                | "rpush"
                | "lpushx"
                | "rpushx"
                | "lpop"
                | "rpop"
                | "linsert"
                | "lset"
                | "ltrim"
                | "lmove"
                | "lmpop"
                | "lrem"
                | "rpoplpush"
                | "blpop"
                | "brpop"
                | "blmove"
                | "blmpop"
                | "hset"
                | "hdel"
                | "hmset"
                | "hincrby"
                | "hincrbyfloat"
                | "hsetnx"
                | "sadd"
                | "srem"
                | "spop"
                | "smove"
                | "sinterstore"
                | "sunionstore"
                | "sdiffstore"
                | "zadd"
                | "zrem"
                | "zincrby"
                | "zinterstore"
                | "zunionstore"
                | "zdiffstore"
                | "zmpop"
                | "zpopmin"
                | "zpopmax"
                | "zrangestore"
                | "zremrangebyrank"
                | "zremrangebyscore"
                | "zremrangebylex"
                | "bzpopmin"
                | "bzpopmax"
                | "bzmpop"
                | "setbit"
                | "bitop"
                | "bitfield"
                | "pfadd"
                | "pfmerge"
                | "geoadd"
                | "geosearchstore"
                | "georadius"
                | "georadiusbymember"
                | "xadd"
                | "xtrim"
                | "xdel"
                | "xgroup"
                | "xack"
                | "xclaim"
                | "xautoclaim"
                | "xreadgroup"
                | "flushall"
                | "flushdb"
                | "swapdb"
                | "select"
                | "restore"
                | "json.set"
                | "json.del"
                | "json.arrappend"
                | "json.arrtrim"
                | "json.numincrby"
        )
    }
}
