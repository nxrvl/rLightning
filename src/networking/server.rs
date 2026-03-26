use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BytesMut};
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, broadcast};
use tracing::{debug, error, info, warn};

use crate::cluster::ClusterManager;
use crate::command::Command;
use crate::command::handler::CommandHandler;
use crate::command::parser;
use crate::command::transaction::{self, TransactionState};
use crate::command::types::pubsub::{
    self as pubsub_commands, ClientId, subscription_message_to_resp,
};
use crate::networking::error::NetworkError;
use crate::networking::raw_command::RawCommand;
use crate::networking::resp::{ProtocolVersion, RespCommand, RespValue};
use crate::persistence::PersistenceManager;
use crate::persistence::config::AofSyncPolicy;
use crate::pubsub::PubSubManager;
use crate::replication::ReplicationManager;
use crate::security::SecurityManager;
use crate::sentinel::SentinelManager;
use crate::networking::raw_command::cmd_eq;
use crate::storage::batch::{self, PipelineEntry};
use crate::storage::clock::cached_now;
use crate::storage::engine::{CURRENT_DB_INDEX, StorageEngine};
use crate::utils::logging;

/// Information about a connected client, tracked for CLIENT LIST/INFO/ID
#[derive(Debug, Clone)]
pub struct ClientInfo {
    /// Unique client ID
    pub id: u64,
    /// Client socket address
    pub addr: String,
    /// Client-assigned name (via CLIENT SETNAME)
    pub name: String,
    /// Current database index
    pub db: usize,
    /// Number of channel subscriptions
    pub sub: usize,
    /// Number of pattern subscriptions
    pub psub: usize,
    /// Whether client is in MULTI state (-1 = no, otherwise queued count)
    pub multi: i64,
    /// Last command executed
    pub last_cmd: String,
    /// Connection creation time (unix timestamp in seconds, avoids Instant::now() syscall)
    pub created_at: u64,
}

impl ClientInfo {
    fn new(id: u64, addr: String) -> Self {
        ClientInfo {
            id,
            addr,
            name: String::new(),
            db: 0,
            sub: 0,
            psub: 0,
            multi: -1,
            last_cmd: String::new(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }

    /// Format client info as Redis CLIENT LIST line
    fn to_info_line(&self) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let age = now.saturating_sub(self.created_at);
        format!(
            "id={} addr={} fd=0 name={} db={} sub={} psub={} multi={} qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd={} age={}\r\n",
            self.id,
            self.addr,
            self.name,
            self.db,
            self.sub,
            self.psub,
            self.multi,
            if self.last_cmd.is_empty() {
                "NULL"
            } else {
                &self.last_cmd
            },
            age,
        )
    }
}

/// Global client ID counter
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// Buffered writer for client connections - coalesces small writes to reduce syscalls
type ClientWriter = tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>;

/// The Redis-compatible server
pub struct Server {
    addr: SocketAddr,
    command_handler: Arc<CommandHandler>,
    pubsub: Arc<PubSubManager>,
    persistence: Option<Arc<PersistenceManager>>,
    security: Option<Arc<SecurityManager>>,
    replication: Option<Arc<ReplicationManager>>,
    sentinel: Option<Arc<SentinelManager>>,
    cluster: Option<Arc<ClusterManager>>,
    aof_sync_policy: AofSyncPolicy,
    connection_limit: Arc<Semaphore>,
    buffer_size: usize,
    /// Active client connections tracked for CLIENT LIST/INFO/ID
    connections: Arc<DashMap<u64, ClientInfo>>,
}

/// Outcome of dispatching a parsed command
enum DispatchAction {
    /// Command was processed, continue to next command
    Continue,
    /// Connection should be closed (QUIT or PSYNC connection takeover)
    CloseConnection,
}

impl Server {
    /// Create a new server
    pub fn new(addr: SocketAddr, storage: Arc<StorageEngine>) -> Self {
        let command_handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));
        let pubsub = Arc::new(PubSubManager::new());

        Server {
            addr,
            command_handler,
            pubsub,
            persistence: None,
            security: None,
            replication: None,
            sentinel: None,
            cluster: None,
            aof_sync_policy: AofSyncPolicy::EverySecond,
            connection_limit: Arc::new(Semaphore::new(10000)), // Default to 10K connections max
            buffer_size: 64 * 1024, // 64KB initial buffer (grows on demand)
            connections: Arc::new(DashMap::new()),
        }
    }

    /// Set the persistence manager for this server
    pub fn with_persistence(
        mut self,
        persistence: Arc<PersistenceManager>,
        sync_policy: AofSyncPolicy,
    ) -> Self {
        self.persistence = Some(persistence);
        self.aof_sync_policy = sync_policy;
        self
    }

    /// Set the security manager for this server
    pub fn with_security(mut self, security: Arc<SecurityManager>) -> Self {
        self.security = Some(security);
        self
    }

    /// Set the replication manager for this server
    pub fn with_replication(mut self, replication: Arc<ReplicationManager>) -> Self {
        self.replication = Some(replication);
        self
    }

    /// Set the sentinel manager for this server
    pub fn with_sentinel(mut self, sentinel: Arc<SentinelManager>) -> Self {
        self.sentinel = Some(sentinel);
        self
    }

    /// Set the cluster manager for this server
    pub fn with_cluster(mut self, cluster: Arc<ClusterManager>) -> Self {
        self.cluster = Some(cluster);
        self
    }

    /// Set the maximum number of concurrent connections
    #[allow(dead_code)]
    pub fn with_connection_limit(mut self, max_connections: usize) -> Self {
        self.connection_limit = Arc::new(Semaphore::new(max_connections));
        self
    }

    /// Set the buffer size for socket operations
    #[allow(dead_code)]
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Start the server
    pub async fn start(&self) -> Result<(), NetworkError> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!(addr = %self.addr, "Server listening");

        loop {
            match listener.accept().await {
                Ok((mut socket, addr)) => {
                    // Try to acquire a connection permit
                    match self.connection_limit.clone().try_acquire_owned() {
                        Ok(permit) => {
                            // Disable Nagle's algorithm for lower latency
                            let _ = socket.set_nodelay(true);
                            let conn_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
                            info!(client_addr = %addr, client_id = conn_id, "New client connected");
                            let cmd_handler = Arc::clone(&self.command_handler);
                            let pubsub = Arc::clone(&self.pubsub);
                            let persistence = self.persistence.clone();
                            let security = self.security.clone();
                            let replication = self.replication.clone();
                            let sentinel = self.sentinel.clone();
                            let cluster = self.cluster.clone();
                            let aof_sync_policy = self.aof_sync_policy;
                            let buffer_size = self.buffer_size;
                            let connections = Arc::clone(&self.connections);

                            // Register the connection
                            connections.insert(conn_id, ClientInfo::new(conn_id, addr.to_string()));

                            // Spawn a task for each client
                            tokio::spawn(async move {
                                // Keep permit alive for the duration of the connection
                                let _permit = permit;

                                if let Err(e) = Self::handle_client(
                                    socket,
                                    cmd_handler,
                                    pubsub,
                                    persistence,
                                    security,
                                    replication,
                                    sentinel,
                                    cluster,
                                    aof_sync_policy,
                                    buffer_size,
                                    connections.clone(),
                                    conn_id,
                                )
                                .await
                                {
                                    error!(client_addr = %addr, "Error handling client: {}", e);
                                }
                                // Unregister the connection on disconnect
                                connections.remove(&conn_id);
                                info!(client_addr = %addr, client_id = conn_id, "Client disconnected");
                                // Permit is automatically released when _permit is dropped
                            });
                        }
                        Err(_) => {
                            // Connection limit reached, reject the client
                            warn!(client_addr = %addr, "Connection limit reached, rejecting client");
                            let _ = socket.shutdown().await;
                        }
                    }
                }
                Err(e) => {
                    error!(addr = %self.addr, "Error accepting connection: {}", e);
                }
            }
        }
    }

    /// Handle a client connection
    #[allow(clippy::too_many_arguments)]
    async fn handle_client(
        socket: TcpStream,
        command_handler: Arc<CommandHandler>,
        pubsub: Arc<PubSubManager>,
        persistence: Option<Arc<PersistenceManager>>,
        security: Option<Arc<SecurityManager>>,
        replication: Option<Arc<ReplicationManager>>,
        sentinel: Option<Arc<SentinelManager>>,
        cluster: Option<Arc<ClusterManager>>,
        aof_sync_policy: AofSyncPolicy,
        buffer_size: usize,
        connections: Arc<DashMap<u64, ClientInfo>>,
        conn_id: u64,
    ) -> Result<(), NetworkError> {
        use crate::networking::resp::RespError;
        let client_addr = socket.peer_addr()?;
        let client_addr_str = client_addr.to_string();

        info!(client_addr = %client_addr_str, "Client connected");

        // Register with PubSub manager
        let (client_id, mut pubsub_rx) = pubsub.register_client().await;
        debug!(client_addr = %client_addr_str, client_id = client_id, "Client registered with PubSub manager");

        // Create a buffer for reading client data with increased capacity for large JSON values
        let mut buffer = BytesMut::with_capacity(buffer_size);
        let mut response_buffer = Vec::with_capacity(buffer_size);
        let mut partial_command_buffer: Vec<u8> = Vec::new(); // For reassembling partial large commands
        const MAX_PARTIAL_BUFFER_SIZE: usize = 16 * 1024 * 1024; // 16MB limit for partial command buffer

        // Split the socket for concurrent read/write in subscription mode
        let (mut socket_reader, socket_writer_raw) = socket.into_split();
        // Wrap writer in BufWriter for write coalescing - buffers small responses
        // and flushes when buffer is full or explicitly flushed
        let mut socket_writer = tokio::io::BufWriter::with_capacity(4096, socket_writer_raw);

        // Track if we're in subscription mode
        let mut in_subscription_mode = false;

        // Track the per-connection protocol version (RESP2 by default)
        let mut protocol_version = ProtocolVersion::RESP2;

        // Per-connection transaction state for MULTI/EXEC/WATCH
        let mut tx_state = TransactionState::new();

        // Per-connection database index (SELECT command changes this)
        let mut db_index: usize = 0;

        // Track last command locally to avoid DashMap write on every command
        let mut last_cmd_local = String::new();

        // Per-connection ASKING flag for cluster ASK protocol
        let mut asking = false;

        loop {
            // In subscription mode, we need to handle both:
            // 1. Incoming commands from the client
            // 2. Messages from subscribed channels
            if in_subscription_mode {
                tokio::select! {
                    // Handle incoming pub/sub messages
                    msg_result = pubsub_rx.recv() => {
                        match msg_result {
                            Ok(msg) => {
                                let response = subscription_message_to_resp(&msg);
                                // Convert to RESP3 Push type if client uses RESP3
                                let response = if protocol_version == ProtocolVersion::RESP3 {
                                    response.convert_to_push()
                                } else {
                                    response
                                };
                                if let Ok(bytes) = response.serialize() {
                                    if let Err(e) = socket_writer.write_all(&bytes).await {
                                        error!(client_addr = %client_addr_str, "Failed to write pub/sub message: {}", e);
                                        break;
                                    }
                                    // Flush subscription messages immediately for low latency
                                    if let Err(e) = socket_writer.flush().await {
                                        error!(client_addr = %client_addr_str, "Failed to flush pub/sub message: {}", e);
                                        break;
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                warn!(client_addr = %client_addr_str, missed = n, "Client lagged behind, missed messages");
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                debug!(client_addr = %client_addr_str, "PubSub channel closed");
                                break;
                            }
                        }
                    }
                    // Handle incoming client commands
                    read_result = socket_reader.read_buf(&mut buffer) => {
                        match read_result {
                            Ok(0) => {
                                debug!(client_addr = %client_addr_str, "Connection closed by client");
                                break;
                            }
                            Ok(n) => {
                                debug!(client_addr = %client_addr_str, bytes_read = n, "Read data from client in subscription mode");
                                // Process commands in subscription mode
                                let result = Self::process_subscription_commands(
                                    &mut buffer,
                                    &mut partial_command_buffer,
                                    &mut socket_writer,
                                    &pubsub,
                                    client_id,
                                    &client_addr_str,
                                    &mut in_subscription_mode,
                                    &security,
                                    protocol_version,
                                    &connections,
                                    conn_id,
                                ).await;

                                if let Err(e) = result {
                                    error!(client_addr = %client_addr_str, "Error processing subscription commands: {}", e);
                                    break;
                                }
                                // Flush after processing subscription commands
                                if let Err(e) = socket_writer.flush().await {
                                    error!(client_addr = %client_addr_str, "Failed to flush after subscription commands: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                error!(client_addr = %client_addr_str, "Error reading from client: {}", e);
                                break;
                            }
                        }
                    }
                }
            } else {
                // Normal mode: read and process commands
                let read_result = socket_reader.read_buf(&mut buffer).await;

                match read_result {
                    Ok(0) => {
                        debug!(client_addr = %client_addr_str, "Connection closed by client");
                        break;
                    }
                    Ok(n) => {
                        debug!(client_addr = %client_addr_str, bytes_read = n, "Read data from client");
                        logging::log_protocol_data(
                            &client_addr_str,
                            "request",
                            &buffer[buffer.len() - n..],
                            true,
                        );
                    }
                    Err(e) => {
                        error!(client_addr = %client_addr_str, "Error reading from client: {}", e);
                        break;
                    }
                }

                // Clear the response buffer for a new batch of commands
                response_buffer.clear();
                // Shrink response buffer if it grew beyond 64KB (e.g. from a large LRANGE)
                const BUFFER_SHRINK_THRESHOLD: usize = 64 * 1024;
                if response_buffer.capacity() > BUFFER_SHRINK_THRESHOLD {
                    response_buffer.shrink_to(buffer_size);
                }
                let mut commands_processed = 0;

                // Phase 1: Pre-parse all commands from buffer
                // (separates parsing from execution to enable pipeline batching)
                const MAX_BATCH_COMMANDS: usize = 100;
                let mut parsed_commands: Vec<Command> = Vec::new();

                while !buffer.is_empty() && parsed_commands.len() < MAX_BATCH_COMMANDS {
                    if !partial_command_buffer.is_empty() {
                        let mut combined =
                            BytesMut::with_capacity(partial_command_buffer.len() + buffer.len());
                        combined.extend_from_slice(&partial_command_buffer);
                        combined.extend_from_slice(&buffer);
                        buffer = combined;
                        // Drop large partial buffers instead of just clearing
                        if partial_command_buffer.capacity() > BUFFER_SHRINK_THRESHOLD {
                            partial_command_buffer = Vec::new();
                        } else {
                            partial_command_buffer.clear();
                        }
                    }

                    let fast_parse_result = {
                        match RawCommand::try_parse(&buffer) {
                            Ok(Some(raw_cmd)) => {
                                let cmd = raw_cmd.to_command();
                                let consumed = raw_cmd.bytes_consumed;
                                Some(Ok((cmd, consumed)))
                            }
                            Ok(None) => None,
                            Err(e) => Some(Err(e)),
                        }
                    };

                    match fast_parse_result {
                        Some(Ok((cmd, consumed))) => {
                            buffer.advance(consumed);
                            parsed_commands.push(cmd);
                        }
                        None => {
                            match RespValue::parse(&mut buffer) {
                                Ok(Some(value)) => {
                                    match parser::parse_command(value) {
                                        Ok(cmd) => parsed_commands.push(cmd),
                                        Err(e) => {
                                            error!(client_addr = %client_addr_str, error = ?e, "Command parsing error");
                                            let error_resp = RespValue::Error(e.to_string());
                                            if let Ok(bytes) = error_resp.serialize() {
                                                response_buffer.extend_from_slice(&bytes);
                                            }
                                        }
                                    }
                                }
                                Ok(None) => {
                                    if partial_command_buffer.len() + buffer.len()
                                        > MAX_PARTIAL_BUFFER_SIZE
                                    {
                                        error!(client_addr = %client_addr_str, "Partial command buffer exceeded size limit");
                                        Self::send_error_to_writer(
                                            &mut socket_writer,
                                            "ERR Protocol error: command too large".to_string(),
                                            &client_addr_str,
                                        )
                                        .await?;
                                        return Ok(());
                                    }
                                    partial_command_buffer.extend_from_slice(&buffer);
                                    buffer.clear();
                                    break;
                                }
                                Err(e) => {
                                    error!(client_addr = %client_addr_str, error = ?e, "RESP protocol error");
                                    let error_msg = match &e {
                                        RespError::InvalidFormatDetails(details) => {
                                            format!("ERR Protocol error: Invalid RESP data format - {}", details)
                                        }
                                        RespError::ValueTooLarge(details) => {
                                            format!("ERR Protocol error: {}", details)
                                        }
                                        _ => format!("ERR Protocol error: {}", e),
                                    };
                                    let error_resp = RespValue::Error(error_msg);
                                    if let Ok(bytes) = error_resp.serialize() {
                                        response_buffer.extend_from_slice(&bytes);
                                    }
                                    buffer.clear();
                                    if partial_command_buffer.capacity() > BUFFER_SHRINK_THRESHOLD {
                                        partial_command_buffer = Vec::new();
                                    } else {
                                        partial_command_buffer.clear();
                                    }
                                    break;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!(client_addr = %client_addr_str, error = ?e, "Zero-copy parser error");
                            let error_msg = match &e {
                                RespError::InvalidFormatDetails(details) => {
                                    format!("ERR Protocol error: Invalid RESP data format - {}", details)
                                }
                                RespError::ValueTooLarge(details) => {
                                    format!("ERR Protocol error: {}", details)
                                }
                                _ => format!("ERR Protocol error: {}", e),
                            };
                            let error_resp = RespValue::Error(error_msg);
                            if let Ok(bytes) = error_resp.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                            buffer.clear();
                            if partial_command_buffer.capacity() > BUFFER_SHRINK_THRESHOLD {
                                partial_command_buffer = Vec::new();
                            } else {
                                partial_command_buffer.clear();
                            }
                            break;
                        }
                    }
                }

                // Phase 2: Execute commands with optional pipeline batching
                // Batch when: multiple commands, no MULTI, no subscription, RESP2,
                // no cluster (avoid slot routing), no security (ACL handled by dispatch),
                // and no transaction commands in the pipeline (MULTI/EXEC/WATCH/DISCARD
                // must go through individual dispatch to maintain transaction semantics).
                let has_tx_cmd = parsed_commands.iter().any(|cmd| {
                    let name = cmd.name.as_bytes();
                    matches!(name.len(), 4 | 5 | 7) && matches!(
                        name.first().map(|b| b.to_ascii_uppercase()),
                        Some(b'M') | Some(b'E') | Some(b'W') | Some(b'D')
                    ) && (
                        cmd_eq(name, b"MULTI")
                        || cmd_eq(name, b"EXEC")
                        || cmd_eq(name, b"WATCH")
                        || cmd_eq(name, b"DISCARD")
                    )
                });
                let batch_eligible = parsed_commands.len() > 1
                    && !tx_state.in_multi
                    && !has_tx_cmd
                    && !in_subscription_mode
                    && protocol_version == ProtocolVersion::RESP2
                    && cluster.is_none()
                    && security.is_none();

                if batch_eligible {
                    let mut active_db = command_handler.storage().get_db_by_index(db_index);
                    let groups = batch::group_pipeline(
                        &parsed_commands,
                        |k| active_db.shard_index(k),
                    );

                    let mut close_connection = false;
                    'batch_loop: for entry in &groups {
                        match entry {
                            PipelineEntry::Batch(group) => {
                                if group.is_read_only {
                                    // Read batch: single shard read lock
                                    let results = active_db.execute_on_shard_ref(
                                        group.shard_idx,
                                        |map| batch::execute_read_batch(map, &group.commands),
                                    );
                                    for result in results {
                                        let resp = match result {
                                            Ok(r) => r,
                                            Err(e) => RespValue::Error(e.to_string()),
                                        };
                                        if let Ok(bytes) = resp.serialize() {
                                            response_buffer.extend_from_slice(&bytes);
                                        }
                                        commands_processed += 1;
                                    }
                                } else {
                                    // Write batch: check read-only replica first
                                    if let Some(ref repl) = replication
                                        && repl.is_read_only()
                                    {
                                        for _ in &group.commands {
                                            let resp = RespValue::Error(
                                                "READONLY You can't write against a read only replica.".to_string(),
                                            );
                                            if let Ok(bytes) = resp.serialize() {
                                                response_buffer.extend_from_slice(&bytes);
                                            }
                                            commands_processed += 1;
                                        }
                                        continue;
                                    }

                                    // Pre-batch eviction: estimate the batch's memory needs and
                                    // try to free that much space before acquiring the shard lock.
                                    // Per-command budget in execute_write_batch handles individual
                                    // OOM rejection, allowing freeing commands like DEL/HDEL/SREM/ZREM
                                    // through even when over maxmemory.
                                    let estimated = batch::estimate_write_batch_memory(&group.commands);
                                    let _ = command_handler.storage().check_write_memory(estimated).await;

                                    // Write batch: single shard write lock
                                    let max_key_size = command_handler.storage().max_key_size();
                                    let max_value_size = command_handler.storage().max_value_size();
                                    let memory_budget = command_handler.storage().remaining_memory_budget();
                                    let results = active_db.execute_on_shard_mut(
                                        group.shard_idx,
                                        |map| batch::execute_write_batch(map, &group.commands, max_key_size, max_value_size, memory_budget),
                                    );
                                    let mut batch_write_count: u64 = 0;
                                    let mut batch_key_created: u32 = 0;
                                    let mut batch_key_deleted: u32 = 0;
                                    // Collect prefix index updates to batch into a single lock acquisition
                                    let mut prefix_updates: Vec<(Vec<u8>, bool)> = Vec::new();
                                    // Collect AOF-eligible commands to batch into a single log call
                                    let mut aof_batch: Vec<crate::networking::resp::RespCommand> = Vec::new();
                                    // Collect replication commands to batch into a single propagation call
                                    let mut repl_batch: Vec<crate::networking::resp::RespCommand> = Vec::new();
                                    for (i, (result, mem_delta, key_delta)) in results.iter().enumerate() {
                                        // Update per-shard and global memory counters
                                        if *mem_delta > 0 {
                                            active_db.add_memory_by_shard(
                                                group.shard_idx,
                                                *mem_delta as u64,
                                            );
                                            command_handler.storage().adjust_global_memory(*mem_delta);
                                        } else if *mem_delta < 0 {
                                            active_db.sub_memory_by_shard(
                                                group.shard_idx,
                                                (-*mem_delta) as u64,
                                            );
                                            command_handler.storage().adjust_global_memory(*mem_delta);
                                        }

                                        // Track key count changes and collect prefix index updates.
                                        // Use explicit db_index since the batch path does not
                                        // enter a CURRENT_DB_INDEX scope (unlike handler.rs:84).
                                        if *key_delta > 0 {
                                            batch_key_created += *key_delta as u32;
                                            if let Some(key) = group.commands[i].args.first() {
                                                prefix_updates.push((key.to_vec(), true));
                                            }
                                        } else if *key_delta < 0 {
                                            batch_key_deleted += (-*key_delta) as u32;
                                            if let Some(key) = group.commands[i].args.first() {
                                                prefix_updates.push((key.to_vec(), false));
                                            }
                                        }

                                        // AOF logging, replication, WATCH invalidation, and expiration heap for successful writes
                                        if result.is_ok() {
                                            let cmd = group.commands[i];
                                            let cmd_lower = &cmd.name;
                                            let cmd_name_bytes = cmd.name.as_bytes();

                                            // Only count actual mutations for RDB snapshot
                                            // triggering (matches KeepUnchanged semantics
                                            // in the non-batched atomic_modify path).
                                            // Detect no-op writes for RDB snapshot counting.
                                            // Expired-key cleanup (key_delta=-1 with zero-result)
                                            // is treated as a no-op, matching the non-batched
                                            // atomic_modify path which uses KeepUnchanged.
                                            let is_no_op = match cmd_name_bytes.first().map(|b| b.to_ascii_uppercase()) {
                                                Some(b'D') if cmd_eq(cmd_name_bytes, b"DEL") =>
                                                    matches!(result, Ok(RespValue::Integer(0))),
                                                Some(b'E') if cmd_eq(cmd_name_bytes, b"EXPIRE") =>
                                                    matches!(result, Ok(RespValue::Integer(0))),
                                                Some(b'G') if cmd_eq(cmd_name_bytes, b"GETDEL") =>
                                                    matches!(result, Ok(RespValue::BulkString(None))),
                                                Some(b'H') if cmd_eq(cmd_name_bytes, b"HDEL") =>
                                                    matches!(result, Ok(RespValue::Integer(0))),
                                                Some(b'P') =>
                                                    (cmd_eq(cmd_name_bytes, b"PEXPIRE") || cmd_eq(cmd_name_bytes, b"PERSIST"))
                                                    && matches!(result, Ok(RespValue::Integer(0))),
                                                Some(b'S') if cmd_eq(cmd_name_bytes, b"SREM") =>
                                                    matches!(result, Ok(RespValue::Integer(0))),
                                                Some(b'Z') if cmd_eq(cmd_name_bytes, b"ZREM") =>
                                                    matches!(result, Ok(RespValue::Integer(0))),
                                                _ => false,
                                            };
                                            if !is_no_op {
                                                batch_write_count += 1;
                                            }

                                            // Bump key version for WATCH invalidation.
                                            // The non-batched path uses KeepUnchanged in
                                            // atomic_modify to skip bumps for no-op
                                            // HDEL/SREM/ZREM (matching Redis semantics
                                            // where signalModifiedKey is only called on
                                            // actual mutation). Match that here by checking
                                            // the integer result for those commands.
                                            // DEL, EXPIRE, PEXPIRE, PERSIST, and GETDEL
                                            // also only bump on actual mutation.
                                            let should_bump = match cmd_name_bytes.first().map(|b| b.to_ascii_uppercase()) {
                                                Some(b'D') if cmd_eq(cmd_name_bytes, b"DEL") =>
                                                    matches!(result, Ok(RespValue::Integer(n)) if *n > 0),
                                                Some(b'E') if cmd_eq(cmd_name_bytes, b"EXPIRE") =>
                                                    matches!(result, Ok(RespValue::Integer(n)) if *n > 0),
                                                Some(b'G') if cmd_eq(cmd_name_bytes, b"GETDEL") =>
                                                    *key_delta != 0,
                                                Some(b'H') if cmd_eq(cmd_name_bytes, b"HDEL") =>
                                                    matches!(result, Ok(RespValue::Integer(n)) if *n > 0) || *key_delta != 0,
                                                Some(b'P') if cmd_eq(cmd_name_bytes, b"PEXPIRE") =>
                                                    matches!(result, Ok(RespValue::Integer(n)) if *n > 0),
                                                Some(b'P') if cmd_eq(cmd_name_bytes, b"PERSIST") =>
                                                    matches!(result, Ok(RespValue::Integer(n)) if *n > 0),
                                                Some(b'S') if cmd_eq(cmd_name_bytes, b"SREM") =>
                                                    matches!(result, Ok(RespValue::Integer(n)) if *n > 0) || *key_delta != 0,
                                                Some(b'Z') if cmd_eq(cmd_name_bytes, b"ZREM") =>
                                                    matches!(result, Ok(RespValue::Integer(n)) if *n > 0) || *key_delta != 0,
                                                _ => true,
                                            };
                                            if should_bump
                                                && let Some(key) = cmd.args.first()
                                            {
                                                command_handler.storage().bump_key_version_for_db(key, db_index);
                                            }

                                            // Notify blocking manager for LPUSH/RPUSH
                                            if (cmd_eq(cmd_name_bytes, b"LPUSH") || cmd_eq(cmd_name_bytes, b"RPUSH"))
                                                && let Some(key) = cmd.args.first()
                                            {
                                                command_handler.blocking_mgr().notify_key(key);
                                            }

                                            // Add to expiration heap for EXPIRE/PEXPIRE, remove for PERSIST
                                            if (cmd_eq(cmd_name_bytes, b"EXPIRE") || cmd_eq(cmd_name_bytes, b"PEXPIRE"))
                                                && matches!(result, Ok(RespValue::Integer(1)))
                                                && let Some(key) = cmd.args.first()
                                                && let Some(ttl_bytes) = cmd.args.get(1)
                                                && let Ok(ttl_str) = std::str::from_utf8(ttl_bytes)
                                                && let Ok(ttl) = ttl_str.parse::<i64>()
                                            {
                                                if ttl > 0 {
                                                    let expires_at = if cmd_eq(cmd_name_bytes, b"EXPIRE") {
                                                        cached_now() + std::time::Duration::from_secs(ttl as u64)
                                                    } else {
                                                        cached_now() + std::time::Duration::from_millis(ttl as u64)
                                                    };
                                                    active_db.add_expiration_by_shard(
                                                        group.shard_idx,
                                                        key.clone(),
                                                        expires_at,
                                                    );
                                                } else {
                                                    // TTL <= 0 deleted the key — clean up expiration heap
                                                    active_db.remove_expiration(key);
                                                }
                                            } else if cmd_eq(cmd_name_bytes, b"PERSIST")
                                                && matches!(result, Ok(RespValue::Integer(1)))
                                                && let Some(key) = cmd.args.first()
                                            {
                                                active_db.remove_expiration(key);
                                            }

                                            // Clean up expiration heap for commands that delete keys
                                            // or overwrite keys that may have had a TTL (SET with key_delta==0)
                                            if *key_delta < 0
                                                && let Some(key) = cmd.args.first()
                                            {
                                                active_db.remove_expiration(key);
                                            } else if *key_delta == 0
                                                && cmd_eq(cmd_name_bytes, b"SET")
                                                && let Some(key) = cmd.args.first()
                                            {
                                                active_db.remove_expiration(key);
                                            }

                                            if persistence.is_some() && !is_no_op {
                                                aof_batch.push(RespCommand {
                                                    name: cmd.name.as_bytes().to_vec(),
                                                    args: cmd.args.clone(),
                                                });
                                            }
                                            if replication.is_some()
                                                && !is_no_op
                                                && ReplicationManager::is_write_command(cmd_lower)
                                            {
                                                // Only emit SELECT once per batch group (db_index
                                                // is constant within a group)
                                                if repl_batch.is_empty() {
                                                    repl_batch.push(RespCommand {
                                                        name: b"SELECT".to_vec(),
                                                        args: vec![db_index.to_string().into_bytes()],
                                                    });
                                                }
                                                repl_batch.push(RespCommand {
                                                    name: cmd.name.as_bytes().to_vec(),
                                                    args: cmd.args.clone(),
                                                });
                                            }
                                        }
                                        let resp = match result {
                                            Ok(r) => r.clone(),
                                            Err(e) => RespValue::Error(e.to_string()),
                                        };
                                        if let Ok(bytes) = resp.serialize() {
                                            response_buffer.extend_from_slice(&bytes);
                                        }
                                        commands_processed += 1;
                                    }
                                    // Batch update prefix indices with a single lock acquisition
                                    if !prefix_updates.is_empty() {
                                        let refs: Vec<(&[u8], bool)> = prefix_updates.iter().map(|(k, i)| (k.as_slice(), *i)).collect();
                                        command_handler.storage().update_prefix_indices_batch_for_db(&refs, db_index).await;
                                    }
                                    // Batch AOF logging with a single call
                                    if !aof_batch.is_empty()
                                        && let Some(ref pm) = persistence
                                    {
                                        if let Err(e) = pm.log_commands_batch_for_db(aof_batch, aof_sync_policy, db_index).await {
                                            eprintln!("WARNING: Failed to log batch AOF for db {}: {}", db_index, e);
                                        }
                                    }
                                    // Batch replication propagation with a single call
                                    if !repl_batch.is_empty()
                                        && let Some(ref repl) = replication
                                    {
                                        repl.propagate_commands_batch(&repl_batch).await;
                                    }
                                    // Update per-shard and global key counts
                                    if batch_key_created > 0 {
                                        active_db.inc_key_count_by_shard(group.shard_idx, batch_key_created);
                                        command_handler.storage().adjust_key_count(batch_key_created as i64);
                                    }
                                    if batch_key_deleted > 0 {
                                        active_db.dec_key_count_by_shard(group.shard_idx, batch_key_deleted);
                                        command_handler.storage().adjust_key_count(-(batch_key_deleted as i64));
                                    }
                                    // Increment write counters for RDB snapshot triggering
                                    if batch_write_count > 0 {
                                        command_handler.storage().increment_write_counters_batch(batch_write_count).await;
                                    }
                                }
                            }
                            PipelineEntry::Individual(cmd) => {
                                match Self::dispatch_command(
                                    cmd,
                                    &mut socket_writer,
                                    &mut response_buffer,
                                    &mut db_index,
                                    &mut protocol_version,
                                    &mut in_subscription_mode,
                                    &mut tx_state,
                                    &pubsub,
                                    client_id,
                                    &security,
                                    &sentinel,
                                    &cluster,
                                    &replication,
                                    &command_handler,
                                    &persistence,
                                    aof_sync_policy,
                                    &client_addr_str,
                                    &connections,
                                    conn_id,
                                    &mut last_cmd_local,
                                    &mut asking,
                                )
                                .await?
                                {
                                    DispatchAction::CloseConnection => {
                                        close_connection = true;
                                        break 'batch_loop;
                                    }
                                    DispatchAction::Continue => {
                                        commands_processed += 1;
                                        // If SELECT changed the database, re-capture active_db
                                        // so subsequent batch groups target the correct DB
                                        if cmd_eq(cmd.name.as_bytes(), b"SELECT") {
                                            active_db = command_handler.storage().get_db_by_index(db_index);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if close_connection {
                        // Send any accumulated responses before closing
                        if !response_buffer.is_empty() {
                            let _ = socket_writer.write_all(&response_buffer).await;
                            let _ = socket_writer.flush().await;
                        }
                        if !tx_state.watched_keys.is_empty() {
                            command_handler.storage().decrement_watch_count();
                        }
                        pubsub.unregister_client(client_id).await;
                        if let Some(security_mgr) = security {
                            security_mgr.remove_client(&client_addr_str);
                        }
                        return Ok(());
                    }
                } else {
                    // No batching: process each command individually
                    for cmd in &parsed_commands {
                        match Self::dispatch_command(
                            cmd,
                            &mut socket_writer,
                            &mut response_buffer,
                            &mut db_index,
                            &mut protocol_version,
                            &mut in_subscription_mode,
                            &mut tx_state,
                            &pubsub,
                            client_id,
                            &security,
                            &sentinel,
                            &cluster,
                            &replication,
                            &command_handler,
                            &persistence,
                            aof_sync_policy,
                            &client_addr_str,
                            &connections,
                            conn_id,
                            &mut last_cmd_local,
                            &mut asking,
                        )
                        .await?
                        {
                            DispatchAction::CloseConnection => {
                                if !tx_state.watched_keys.is_empty() {
                                    command_handler.storage().decrement_watch_count();
                                }
                                pubsub.unregister_client(client_id).await;
                                if let Some(security_mgr) = security {
                                    security_mgr.remove_client(&client_addr_str);
                                }
                                return Ok(());
                            }
                            DispatchAction::Continue => {
                                commands_processed += 1;
                            }
                        }
                    }
                }

                // Send the batched responses if we have any
                if !response_buffer.is_empty() {
                    logging::log_protocol_data(
                        &client_addr_str,
                        "batched_response",
                        &response_buffer,
                        false,
                    );

                    if let Err(e) = socket_writer.write_all(&response_buffer).await {
                        error!(client_addr = %client_addr_str, "Failed to write batched responses: {}", e);
                        break;
                    }
                    debug!(client_addr = %client_addr_str, commands = commands_processed, bytes = response_buffer.len(), "Sent batched responses");

                    response_buffer.clear();
                    if response_buffer.capacity() > BUFFER_SHRINK_THRESHOLD {
                        response_buffer.shrink_to(buffer_size);
                    }
                }
                // Flush the write buffer before going back to reading
                if let Err(e) = socket_writer.flush().await {
                    error!(client_addr = %client_addr_str, "Failed to flush write buffer: {}", e);
                    break;
                }
            }
        }

        // Cleanup: decrement active watch count if this connection had watched keys
        if !tx_state.watched_keys.is_empty() {
            command_handler.storage().decrement_watch_count();
        }

        // Cleanup: unregister from PubSub and security
        pubsub.unregister_client(client_id).await;
        if let Some(ref security_mgr) = security {
            security_mgr.remove_client(&client_addr_str);
        }
        debug!(client_addr = %client_addr_str, client_id = client_id, "Client unregistered from PubSub manager");

        Ok(())
    }

    /// Unified command dispatch - handles auth, ACL, routing, transactions, replication for any parsed command.
    #[allow(clippy::too_many_arguments)]
    async fn dispatch_command(
        cmd: &Command,
        socket_writer: &mut ClientWriter,
        response_buffer: &mut Vec<u8>,
        db_index: &mut usize,
        protocol_version: &mut ProtocolVersion,
        in_subscription_mode: &mut bool,
        tx_state: &mut TransactionState,
        pubsub: &Arc<PubSubManager>,
        client_id: u64,
        security: &Option<Arc<SecurityManager>>,
        sentinel: &Option<Arc<SentinelManager>>,
        cluster: &Option<Arc<ClusterManager>>,
        replication: &Option<Arc<ReplicationManager>>,
        command_handler: &Arc<CommandHandler>,
        persistence: &Option<Arc<PersistenceManager>>,
        aof_sync_policy: AofSyncPolicy,
        client_addr_str: &str,
        connections: &Arc<DashMap<u64, ClientInfo>>,
        conn_id: u64,
        last_cmd_local: &mut String,
        asking: &mut bool,
    ) -> Result<DispatchAction, NetworkError> {
        let cmd_lower = cmd.name.to_lowercase();

        // ASKING command: set the per-connection flag and return +OK
        if cmd_lower == "asking" {
            *asking = true;
            let response = RespValue::SimpleString("OK".to_string());
            if let Ok(bytes) = response.serialize() {
                response_buffer.extend_from_slice(&bytes);
            }
            return Ok(DispatchAction::Continue);
        }

        // Consume and reset the asking flag for non-ASKING commands
        let current_asking = std::mem::replace(asking, false);

        // HELLO - protocol negotiation
        if cmd_lower == "hello" {
            let (response, new_version) = Self::handle_hello_command(
                &cmd.args,
                *protocol_version,
                security,
                client_addr_str,
                connections,
                conn_id,
            );
            *protocol_version = new_version;
            if let Ok(bytes) = response.serialize() {
                response_buffer.extend_from_slice(&bytes);
            }
            return Ok(DispatchAction::Continue);
        }

        // QUIT - close connection
        if cmd_lower == "quit" {
            let response = RespValue::SimpleString("OK".to_string());
            if let Ok(bytes) = response.serialize() {
                let _ = socket_writer.write_all(&bytes).await;
                let _ = socket_writer.flush().await;
            }
            return Ok(DispatchAction::CloseConnection);
        }

        // Auth check - block unauthenticated access to all commands except AUTH, HELLO, QUIT
        let is_auth_command = cmd_lower == "auth";
        if !is_auth_command
            && let Some(security_mgr) = security
            && security_mgr.require_auth()
            && !security_mgr.is_authenticated(client_addr_str)
        {
            // Write to response_buffer (not directly to socket) to maintain
            // correct response ordering in pipelined commands
            let error_resp = RespValue::Error("NOAUTH Authentication required.".to_string());
            if let Ok(bytes) = error_resp.serialize() {
                response_buffer.extend_from_slice(&bytes);
            }
            return Ok(DispatchAction::Continue);
        }

        // ACL permission checks for authenticated clients
        if let Some(security_mgr) = security
            && !is_auth_command
            && security_mgr.is_authenticated(client_addr_str)
        {
            // Check command permission
            if !security_mgr.check_command_permission(client_addr_str, &cmd_lower) {
                let username = security_mgr
                    .acl()
                    .get_username(client_addr_str)
                    .unwrap_or_else(|| "default".to_string());
                security_mgr
                    .acl()
                    .log_denial(client_addr_str, &username, &cmd_lower, "command");
                let error_msg = format!(
                    "NOPERM this user has no permissions to run the '{}' command",
                    cmd_lower
                );
                // Write to response_buffer (not directly to socket) to maintain
                // correct response ordering in pipelined commands
                let error_resp = RespValue::Error(error_msg);
                if let Ok(bytes) = error_resp.serialize() {
                    response_buffer.extend_from_slice(&bytes);
                }
                return Ok(DispatchAction::Continue);
            }

            // Check key permission
            let key_indices = crate::security::acl::get_key_indices(&cmd_lower, &cmd.args);
            for &idx in &key_indices {
                if idx < cmd.args.len()
                    && !security_mgr.check_key_permission(client_addr_str, &cmd.args[idx])
                {
                    let username = security_mgr
                        .acl()
                        .get_username(client_addr_str)
                        .unwrap_or_else(|| "default".to_string());
                    let key_str = String::from_utf8_lossy(&cmd.args[idx]);
                    security_mgr.acl().log_denial(
                        client_addr_str,
                        &username,
                        &cmd_lower,
                        &format!("key '{}'", key_str),
                    );
                    // Write to response_buffer (not directly to socket) to maintain
                    // correct response ordering in pipelined commands
                    let error_resp = RespValue::Error(
                        "NOPERM this user has no permissions to access one of the keys used as arguments".to_string(),
                    );
                    if let Ok(bytes) = error_resp.serialize() {
                        response_buffer.extend_from_slice(&bytes);
                    }
                    return Ok(DispatchAction::Continue);
                }
            }
        }

        // SELECT - database switching
        if cmd_lower == "select" {
            if cmd.args.len() != 1 {
                let response = RespValue::Error(
                    "ERR wrong number of arguments for 'select' command".to_string(),
                );
                if let Ok(bytes) = response.serialize() {
                    response_buffer.extend_from_slice(&bytes);
                }
            } else {
                match std::str::from_utf8(&cmd.args[0])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    Some(idx) if idx < 16 => {
                        *db_index = idx;
                        let response = RespValue::SimpleString("OK".to_string());
                        if let Ok(bytes) = response.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                        // SELECT is propagated atomically with each write command
                        // via propagate_commands_batch, not standalone, to prevent
                        // interleaving from concurrent connections.
                    }
                    _ => {
                        let response = RespValue::Error("ERR DB index is out of range".to_string());
                        if let Ok(bytes) = response.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                }
            }
            return Ok(DispatchAction::Continue);
        }

        // Track last command locally (avoids DashMap write lock on every command)
        last_cmd_local.clone_from(&cmd_lower);

        // CLIENT - connection management commands handled here with access to per-connection state
        if cmd_lower == "client" {
            // Sync local state to connection tracker for CLIENT LIST/INFO queries
            if let Some(mut info) = connections.get_mut(&conn_id) {
                info.last_cmd.clone_from(last_cmd_local);
                info.db = *db_index;
                info.multi = if tx_state.in_multi {
                    tx_state.queue.len() as i64
                } else {
                    -1
                };
            }
            let response = match Self::handle_client_command(&cmd.args, connections, conn_id) {
                Ok(resp) => resp,
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            };
            if let Ok(bytes) = response.serialize() {
                response_buffer.extend_from_slice(&bytes);
            }
            return Ok(DispatchAction::Continue);
        }

        // Special command routing (pub/sub, ACL, sentinel, replication)
        match cmd_lower.as_str() {
            "subscribe" => {
                if let Some(security_mgr) = security {
                    let denied = cmd
                        .args
                        .iter()
                        .find(|ch| !security_mgr.check_channel_permission(client_addr_str, ch));
                    if let Some(channel) = denied {
                        let error_msg = format!(
                            "NOPERM this user has no permissions to access the '{}' channel",
                            String::from_utf8_lossy(channel)
                        );
                        let error_resp = RespValue::Error(error_msg);
                        if let Ok(bytes) = error_resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                        return Ok(DispatchAction::Continue);
                    }
                }
                let responses = pubsub_commands::subscribe(pubsub, client_id, &cmd.args).await;
                match responses {
                    Ok(resp_list) => {
                        for resp in resp_list {
                            let resp = if *protocol_version == ProtocolVersion::RESP3 {
                                resp.convert_to_push()
                            } else {
                                resp
                            };
                            if let Ok(bytes) = resp.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                        }
                        *in_subscription_mode = true;
                        Self::update_sub_counts(pubsub, client_id, connections, conn_id).await;
                    }
                    Err(e) => {
                        let error_resp = RespValue::Error(e.to_string());
                        if let Ok(bytes) = error_resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                }
                return Ok(DispatchAction::Continue);
            }
            "psubscribe" => {
                if let Some(security_mgr) = security {
                    let denied = cmd
                        .args
                        .iter()
                        .find(|ch| !security_mgr.check_channel_permission(client_addr_str, ch));
                    if let Some(channel) = denied {
                        let error_msg = format!(
                            "NOPERM this user has no permissions to access the '{}' channel",
                            String::from_utf8_lossy(channel)
                        );
                        let error_resp = RespValue::Error(error_msg);
                        if let Ok(bytes) = error_resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                        return Ok(DispatchAction::Continue);
                    }
                }
                let responses = pubsub_commands::psubscribe(pubsub, client_id, &cmd.args).await;
                match responses {
                    Ok(resp_list) => {
                        for resp in resp_list {
                            let resp = if *protocol_version == ProtocolVersion::RESP3 {
                                resp.convert_to_push()
                            } else {
                                resp
                            };
                            if let Ok(bytes) = resp.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                        }
                        *in_subscription_mode = true;
                        Self::update_sub_counts(pubsub, client_id, connections, conn_id).await;
                    }
                    Err(e) => {
                        let error_resp = RespValue::Error(e.to_string());
                        if let Ok(bytes) = error_resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                }
                return Ok(DispatchAction::Continue);
            }
            "publish" => {
                if let Some(security_mgr) = security
                    && !cmd.args.is_empty()
                    && !security_mgr.check_channel_permission(client_addr_str, &cmd.args[0])
                {
                    let error_msg = format!(
                        "NOPERM this user has no permissions to access the '{}' channel",
                        String::from_utf8_lossy(&cmd.args[0])
                    );
                    let error_resp = RespValue::Error(error_msg);
                    if let Ok(bytes) = error_resp.serialize() {
                        response_buffer.extend_from_slice(&bytes);
                    }
                    return Ok(DispatchAction::Continue);
                }
                let response = pubsub_commands::publish(pubsub, &cmd.args).await;
                match response {
                    Ok(resp) => {
                        if let Ok(bytes) = resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                    Err(e) => {
                        let error_resp = RespValue::Error(e.to_string());
                        if let Ok(bytes) = error_resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                }
                return Ok(DispatchAction::Continue);
            }
            "ssubscribe" => {
                if let Some(security_mgr) = security {
                    let denied = cmd
                        .args
                        .iter()
                        .find(|ch| !security_mgr.check_channel_permission(client_addr_str, ch));
                    if let Some(channel) = denied {
                        let error_msg = format!(
                            "NOPERM this user has no permissions to access the '{}' channel",
                            String::from_utf8_lossy(channel)
                        );
                        let error_resp = RespValue::Error(error_msg);
                        if let Ok(bytes) = error_resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                        return Ok(DispatchAction::Continue);
                    }
                }
                let responses = pubsub_commands::ssubscribe(pubsub, client_id, &cmd.args).await;
                match responses {
                    Ok(resp_list) => {
                        for resp in resp_list {
                            let resp = if *protocol_version == ProtocolVersion::RESP3 {
                                resp.convert_to_push()
                            } else {
                                resp
                            };
                            if let Ok(bytes) = resp.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                        }
                        *in_subscription_mode = true;
                        Self::update_sub_counts(pubsub, client_id, connections, conn_id).await;
                    }
                    Err(e) => {
                        let error_resp = RespValue::Error(e.to_string());
                        if let Ok(bytes) = error_resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                }
                return Ok(DispatchAction::Continue);
            }
            "spublish" => {
                if let Some(security_mgr) = security
                    && !cmd.args.is_empty()
                    && !security_mgr.check_channel_permission(client_addr_str, &cmd.args[0])
                {
                    let error_msg = format!(
                        "NOPERM this user has no permissions to access the '{}' channel",
                        String::from_utf8_lossy(&cmd.args[0])
                    );
                    let error_resp = RespValue::Error(error_msg);
                    if let Ok(bytes) = error_resp.serialize() {
                        response_buffer.extend_from_slice(&bytes);
                    }
                    return Ok(DispatchAction::Continue);
                }
                let response = pubsub_commands::spublish(pubsub, &cmd.args).await;
                match response {
                    Ok(resp) => {
                        if let Ok(bytes) = resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                    Err(e) => {
                        let error_resp = RespValue::Error(e.to_string());
                        if let Ok(bytes) = error_resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                }
                return Ok(DispatchAction::Continue);
            }
            "pubsub" => {
                let response = pubsub_commands::pubsub_command(pubsub, &cmd.args).await;
                match response {
                    Ok(resp) => {
                        if let Ok(bytes) = resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                    Err(e) => {
                        let error_resp = RespValue::Error(e.to_string());
                        if let Ok(bytes) = error_resp.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                }
                return Ok(DispatchAction::Continue);
            }
            // ACL commands
            "acl" => {
                if let Some(security_mgr) = security {
                    let response = security_mgr
                        .acl()
                        .handle_acl_command(&cmd.args, client_addr_str);
                    match response {
                        Ok(resp) => {
                            if let Ok(bytes) = resp.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                        }
                        Err(e) => {
                            let error_resp = RespValue::Error(e.to_string());
                            if let Ok(bytes) = error_resp.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                        }
                    }
                } else {
                    // No security manager: provide Redis-compatible defaults for read-only
                    // subcommands, error for write subcommands that would give a false sense of security
                    let subcmd = cmd
                        .args
                        .first()
                        .map(|a| String::from_utf8_lossy(a).to_lowercase());
                    let response = match subcmd.as_deref() {
                        Some("whoami") => RespValue::BulkString(Some(b"default".to_vec())),
                        Some("users") => RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(b"default".to_vec())),
                        ])),
                        Some("list") => RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(b"user default on ~* &* +@all".to_vec())),
                        ])),
                        Some("cat") => RespValue::Array(Some(vec![
                            "keyspace", "read", "write", "set", "sortedset", "list", "hash",
                            "string", "bitmap", "hyperloglog", "geo", "stream", "pubsub",
                            "admin", "fast", "slow", "blocking", "dangerous", "connection",
                            "transaction", "scripting", "server",
                        ].into_iter().map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec()))).collect())),
                        Some("genpass") => {
                            let mut bytes = [0u8; 32];
                            getrandom::fill(&mut bytes).expect("OS RNG unavailable");
                            let pass: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
                            RespValue::BulkString(Some(pass.into_bytes()))
                        }
                        Some("log") => RespValue::Array(Some(vec![])),
                        _ => RespValue::Error("ERR ACL is not enabled. Set 'requirepass' or configure ACL users to enable it.".to_string()),
                    };
                    if let Ok(bytes) = response.serialize() {
                        response_buffer.extend_from_slice(&bytes);
                    }
                }
                return Ok(DispatchAction::Continue);
            }
            // Sentinel commands
            "sentinel" => {
                if let Some(sentinel_mgr) = sentinel {
                    match sentinel_mgr.handle_sentinel_command(&cmd.args).await {
                        Ok(resp) => {
                            if let Ok(bytes) = resp.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                        }
                        Err(e) => {
                            let error_resp = RespValue::Error(e);
                            if let Ok(bytes) = error_resp.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                        }
                    }
                } else {
                    let response = RespValue::Error(
                        "ERR This instance has sentinel support disabled".to_string(),
                    );
                    if let Ok(bytes) = response.serialize() {
                        response_buffer.extend_from_slice(&bytes);
                    }
                }
                return Ok(DispatchAction::Continue);
            }
            // Cluster commands - route to cluster manager when available
            "cluster" => {
                use crate::command::types::cluster::cluster_command;
                let storage = command_handler.storage();
                let response = match cluster_command(storage, &cmd.args, cluster.as_ref()).await {
                    Ok(resp) => resp,
                    Err(e) => RespValue::Error(e.to_string()),
                };
                if let Ok(bytes) = response.serialize() {
                    response_buffer.extend_from_slice(&bytes);
                }
                return Ok(DispatchAction::Continue);
            }
            // Replication commands
            "role" | "replicaof" | "slaveof" | "replconf" | "psync" | "failover" | "wait" => {
                let resp_cmd = RespCommand {
                    name: cmd.name.as_bytes().to_vec(),
                    args: cmd.args.clone(),
                };
                let result = Self::handle_replication_command_slow(
                    &cmd_lower,
                    cmd,
                    &resp_cmd,
                    replication,
                    socket_writer,
                    response_buffer,
                    client_addr_str,
                )
                .await;
                match result {
                    Ok(true) => return Ok(DispatchAction::CloseConnection),
                    Ok(false) => return Ok(DispatchAction::Continue),
                    Err(e) => {
                        error!(client_addr = %client_addr_str, "Replication command error: {}", e);
                        return Ok(DispatchAction::Continue);
                    }
                }
            }
            _ => {}
        }

        // Read-only mode check
        if let Some(repl) = replication
            && repl.is_read_only()
            && ReplicationManager::is_write_command(&cmd_lower)
        {
            let error_resp = RespValue::Error(
                "READONLY You can't write against a read only replica.".to_string(),
            );
            if let Ok(bytes) = error_resp.serialize() {
                response_buffer.extend_from_slice(&bytes);
            }
            return Ok(DispatchAction::Continue);
        }

        // Cluster mode: validate cross-slot access for multi-key commands
        if let Some(cluster_mgr) = cluster
            && cluster_mgr.is_enabled()
        {
            // Extract only actual key arguments per command's syntax
            let key_refs: Vec<&[u8]> = match cmd_lower.as_str() {
                // All args are keys
                "mget" | "del" | "unlink" | "exists" | "touch" | "sinter" | "sinterstore"
                | "sunion" | "sunionstore" | "sdiff" | "sdiffstore" | "pfcount" | "pfmerge"
                | "rename" | "renamenx" | "rpoplpush" => {
                    cmd.args.iter().map(|a| a.as_slice()).collect()
                }
                // Alternating key-value pairs: keys at even indices
                "mset" | "msetnx" => cmd.args.iter().step_by(2).map(|a| a.as_slice()).collect(),
                // First 2 args are keys
                "smove" | "copy" | "lmove" | "blmove" => {
                    cmd.args.iter().take(2).map(|a| a.as_slice()).collect()
                }
                // Skip first arg (operation/dest), rest are keys: BITOP op dest key [key ...]
                "bitop" => cmd.args.iter().skip(1).map(|a| a.as_slice()).collect(),
                // All args except last (timeout): BLPOP key [key ...] timeout
                "blpop" | "brpop" => {
                    if cmd.args.len() > 1 {
                        cmd.args[..cmd.args.len() - 1]
                            .iter()
                            .map(|a| a.as_slice())
                            .collect()
                    } else {
                        vec![]
                    }
                }
                // dest numkeys key [key ...] [options]: ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE
                "zunionstore" | "zinterstore" | "zdiffstore" => {
                    if cmd.args.len() >= 2 {
                        let numkeys = std::str::from_utf8(&cmd.args[1])
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(0);
                        let mut keys = vec![cmd.args[0].as_slice()]; // dest key
                        for arg in cmd.args.iter().skip(2).take(numkeys) {
                            keys.push(arg.as_slice());
                        }
                        keys
                    } else {
                        vec![]
                    }
                }
                // numkeys key [key ...] [options]: ZUNION/ZINTER/ZDIFF/LMPOP/SINTERCARD
                "zunion" | "zinter" | "zdiff" | "lmpop" | "sintercard" => {
                    if !cmd.args.is_empty() {
                        let numkeys = std::str::from_utf8(&cmd.args[0])
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(0);
                        cmd.args
                            .iter()
                            .skip(1)
                            .take(numkeys)
                            .map(|a| a.as_slice())
                            .collect()
                    } else {
                        vec![]
                    }
                }
                // timeout numkeys key [key ...] direction: BLMPOP
                "blmpop" => {
                    if cmd.args.len() >= 2 {
                        let numkeys = std::str::from_utf8(&cmd.args[1])
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(0);
                        cmd.args
                            .iter()
                            .skip(2)
                            .take(numkeys)
                            .map(|a| a.as_slice())
                            .collect()
                    } else {
                        vec![]
                    }
                }
                // XREAD/XREADGROUP: keys follow STREAMS keyword, count = remaining/2
                "xread" | "xreadgroup" => {
                    let streams_pos = cmd
                        .args
                        .iter()
                        .position(|a| a.eq_ignore_ascii_case(b"streams"));
                    if let Some(pos) = streams_pos {
                        let after_streams = &cmd.args[pos + 1..];
                        let num_keys = after_streams.len() / 2;
                        after_streams
                            .iter()
                            .take(num_keys)
                            .map(|a| a.as_slice())
                            .collect()
                    } else {
                        vec![]
                    }
                }
                // GEOSEARCHSTORE: args[0] dest, args[1] source
                "geosearchstore" => {
                    let mut keys: Vec<&[u8]> = Vec::new();
                    if !cmd.args.is_empty() {
                        keys.push(cmd.args[0].as_slice());
                    }
                    if cmd.args.len() > 1 {
                        keys.push(cmd.args[1].as_slice());
                    }
                    keys
                }
                // GEORADIUS/GEORADIUSBYMEMBER: args[0] source, may have STORE/STOREDIST dest
                "georadius" | "georadius_ro" | "georadiusbymember" | "georadiusbymember_ro" => {
                    let mut keys: Vec<&[u8]> = Vec::new();
                    if !cmd.args.is_empty() {
                        keys.push(cmd.args[0].as_slice());
                    }
                    for i in 1..cmd.args.len() {
                        if (cmd.args[i].eq_ignore_ascii_case(b"STORE")
                            || cmd.args[i].eq_ignore_ascii_case(b"STOREDIST"))
                            && let Some(dest) = cmd.args.get(i + 1)
                        {
                            keys.push(dest.as_slice());
                        }
                    }
                    keys
                }
                _ => vec![],
            };
            if cluster_mgr.check_cross_slot(&key_refs) {
                let error_resp = RespValue::Error(
                    "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
                );
                if let Ok(bytes) = error_resp.serialize() {
                    response_buffer.extend_from_slice(&bytes);
                }
                return Ok(DispatchAction::Continue);
            }

            // MOVED/ASK redirect: check slot ownership for key-bearing commands
            let slot_exempt = matches!(
                cmd_lower.as_str(),
                "ping"
                    | "info"
                    | "cluster"
                    | "command"
                    | "auth"
                    | "multi"
                    | "exec"
                    | "discard"
                    | "watch"
                    | "unwatch"
                    | "config"
                    | "dbsize"
                    | "flushdb"
                    | "flushall"
                    | "randomkey"
                    | "keys"
                    | "scan"
                    | "time"
                    | "debug"
                    | "slowlog"
                    | "latency"
                    | "memory"
                    | "module"
                    | "swapdb"
                    | "object"
                    | "wait"
                    | "save"
                    | "bgsave"
                    | "bgrewriteaof"
                    | "lastsave"
                    | "asking"
                    | "subscribe"
                    | "unsubscribe"
                    | "psubscribe"
                    | "punsubscribe"
                    | "ssubscribe"
                    | "sunsubscribe"
                    | "publish"
                    | "spublish"
                    | "select"
                    | "client"
                    | "hello"
                    | "quit"
                    | "reset"
                    | "lolwut"
                    | "echo"
            );

            if !slot_exempt {
                // Get the first key: prefer already-extracted multi-key refs, else args[0]
                let first_key: Option<&[u8]> = if !key_refs.is_empty() {
                    Some(key_refs[0])
                } else if !cmd.args.is_empty() {
                    Some(cmd.args[0].as_slice())
                } else {
                    None
                };

                if let Some(key) = first_key
                    && let Some(redirect) =
                        cluster_mgr.get_redirect(key, current_asking).await
                {
                    let error_msg = redirect.to_string();
                    // Write to response_buffer (not directly to socket) to maintain
                    // correct response ordering in pipelined commands
                    let error_resp = RespValue::Error(error_msg);
                    if let Ok(bytes) = error_resp.serialize() {
                        response_buffer.extend_from_slice(&bytes);
                    }
                    return Ok(DispatchAction::Continue);
                }
            }
        }

        // Capture queued commands before EXEC drains them (for replication)
        let queued_for_repl: Vec<Command> = if cmd_lower == "exec" {
            tx_state.queue.clone()
        } else {
            vec![]
        };

        // Transaction handling (MULTI/EXEC/DISCARD/WATCH/UNWATCH)
        let tx_result = Self::process_with_transaction(
            cmd,
            &cmd_lower,
            tx_state,
            command_handler,
            persistence,
            aof_sync_policy,
            is_auth_command,
            security,
            client_addr_str,
            *db_index,
        )
        .await;

        match tx_result {
            Ok(response) => {
                // Propagate write commands to replicas using batch propagation
                // to prevent interleaving from concurrent connections.
                // Each batch is prefixed with SELECT to establish DB context.
                // Only replicate commands that succeeded — skip error responses
                // (e.g. WRONGTYPE) to avoid polluting the replication stream.
                if let Some(repl) = replication
                    && !matches!(&response, RespValue::Error(_))
                {
                    let select_cmd = RespCommand {
                        name: b"SELECT".to_vec(),
                        args: vec![db_index.to_string().into_bytes()],
                    };
                    if cmd_lower == "exec" {
                        if let RespValue::Array(Some(_)) = &response {
                            // Bundle SELECT + MULTI + write commands + EXEC atomically
                            let mut batch = vec![
                                select_cmd,
                                RespCommand {
                                    name: b"MULTI".to_vec(),
                                    args: vec![],
                                },
                            ];
                            for queued_cmd in &queued_for_repl {
                                let qcmd_lower = queued_cmd.name.to_lowercase();
                                // Include SELECT (for DB context) and write commands
                                if qcmd_lower == "select"
                                    || ReplicationManager::is_write_command(&qcmd_lower)
                                {
                                    batch.push(RespCommand {
                                        name: queued_cmd.name.as_bytes().to_vec(),
                                        args: queued_cmd.args.clone(),
                                    });
                                }
                            }
                            batch.push(RespCommand {
                                name: b"EXEC".to_vec(),
                                args: vec![],
                            });
                            repl.propagate_commands_batch(&batch).await;
                        }
                    } else if ReplicationManager::is_write_command(&cmd_lower) {
                        // Convert blocking commands to non-blocking equivalents
                        // to avoid stalling the replication stream on replicas.
                        let repl_cmd = if is_blocking_write_command(&cmd_lower) {
                            convert_blocking_to_nonblocking(cmd, &response)
                        } else {
                            Some(RespCommand {
                                name: cmd.name.as_bytes().to_vec(),
                                args: cmd.args.clone(),
                            })
                        };
                        if let Some(repl_cmd) = repl_cmd {
                            repl.propagate_commands_batch(&[select_cmd, repl_cmd]).await;
                        }
                    }
                }
                // RESP3 conversion
                let response = if *protocol_version == ProtocolVersion::RESP3 {
                    if cmd_lower == "exec" {
                        // Convert each inner sub-result using its original command name
                        if let RespValue::Array(Some(items)) = response {
                            if items.len() == queued_for_repl.len() {
                                let converted: Vec<RespValue> = items
                                    .into_iter()
                                    .zip(queued_for_repl.iter())
                                    .map(|(item, cmd)| {
                                        let base = cmd.name.to_lowercase();
                                        let sub_cmd =
                                            Self::build_resp3_command_name(&base, &cmd.args);
                                        item.convert_for_resp3(&sub_cmd)
                                    })
                                    .collect();
                                RespValue::Array(Some(converted))
                            } else {
                                RespValue::Array(Some(items))
                            }
                        } else {
                            // WATCH abort returns nil; convert to RESP3 Null
                            response.convert_for_resp3("exec")
                        }
                    } else {
                        let resp3_cmd = Self::build_resp3_command_name(&cmd_lower, &cmd.args);
                        response.convert_for_resp3(&resp3_cmd)
                    }
                } else {
                    response
                };
                if let Ok(bytes) = response.serialize() {
                    response_buffer.extend_from_slice(&bytes);
                } else {
                    Self::send_response_to_writer(socket_writer, response, client_addr_str).await?;
                }
            }
            Err(err) => {
                // Serialize error into pipeline response buffer (not directly to socket)
                // to maintain correct response ordering in pipelined commands
                let error_resp = RespValue::Error(err.to_string());
                if let Ok(bytes) = error_resp.serialize() {
                    response_buffer.extend_from_slice(&bytes);
                } else {
                    Self::send_error_to_writer(socket_writer, err.to_string(), client_addr_str)
                        .await?;
                }
            }
        }

        // After EXEC completes (including AOF/replication), update the connection's
        // database index if SELECT was used inside the transaction.
        // In Redis, SELECT inside MULTI/EXEC permanently changes the connection's DB.
        if let Some(new_db) = tx_state.post_exec_db.take() {
            *db_index = new_db;
        }

        Ok(DispatchAction::Continue)
    }

    /// Update sub/psub counts on ClientInfo from PubSubManager state
    async fn update_sub_counts(
        pubsub: &Arc<PubSubManager>,
        client_id: ClientId,
        connections: &Arc<DashMap<u64, ClientInfo>>,
        conn_id: u64,
    ) {
        let (sub, psub) = pubsub.get_subscription_counts(client_id).await;
        if let Some(mut info) = connections.get_mut(&conn_id) {
            info.sub = sub;
            info.psub = psub;
        }
    }

    /// Handle CLIENT subcommands with access to real connection tracking data
    fn handle_client_command(
        args: &[Vec<u8>],
        connections: &Arc<DashMap<u64, ClientInfo>>,
        conn_id: u64,
    ) -> Result<RespValue, NetworkError> {
        use crate::command::utils::bytes_to_string;

        if args.is_empty() {
            return Ok(RespValue::Error(
                "ERR wrong number of arguments for 'client' command".to_string(),
            ));
        }

        let subcmd = bytes_to_string(&args[0])
            .map_err(|_| {
                NetworkError::Serialization("Invalid UTF-8 in CLIENT subcommand".to_string())
            })?
            .to_uppercase();

        match subcmd.as_str() {
            "LIST" => {
                let mut output = String::new();
                for entry in connections.iter() {
                    output.push_str(&entry.value().to_info_line());
                }
                Ok(RespValue::BulkString(Some(output.into_bytes())))
            }
            "INFO" => {
                if let Some(info) = connections.get(&conn_id) {
                    Ok(RespValue::BulkString(Some(
                        info.to_info_line().into_bytes(),
                    )))
                } else {
                    Ok(RespValue::BulkString(Some(b"".to_vec())))
                }
            }
            "ID" => Ok(RespValue::Integer(conn_id as i64)),
            "SETNAME" => {
                if args.len() != 2 {
                    return Ok(RespValue::Error(
                        "ERR wrong number of arguments for 'client|setname' command".to_string(),
                    ));
                }
                let name = bytes_to_string(&args[1]).map_err(|_| {
                    NetworkError::Serialization("Invalid UTF-8 in client name".to_string())
                })?;
                // Redis disallows spaces, newlines, and control characters in client names
                if name.chars().any(|c| c == ' ' || c.is_control()) {
                    return Ok(RespValue::Error(
                        "ERR Client names cannot contain spaces, newlines or special characters."
                            .to_string(),
                    ));
                }
                if let Some(mut info) = connections.get_mut(&conn_id) {
                    info.name = name;
                }
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            "GETNAME" => {
                if let Some(info) = connections.get(&conn_id) {
                    if info.name.is_empty() {
                        Ok(RespValue::BulkString(None))
                    } else {
                        Ok(RespValue::BulkString(Some(info.name.as_bytes().to_vec())))
                    }
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            "KILL" => {
                if args.len() < 2 {
                    return Ok(RespValue::Error(
                        "ERR wrong number of arguments for 'client|kill' command".to_string(),
                    ));
                }
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            "PAUSE" => {
                if args.len() < 2 {
                    return Ok(RespValue::Error(
                        "ERR wrong number of arguments for 'client|pause' command".to_string(),
                    ));
                }
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            "UNPAUSE" => Ok(RespValue::SimpleString("OK".to_string())),
            "REPLY" => {
                if args.len() != 2 {
                    return Ok(RespValue::Error(
                        "ERR wrong number of arguments for 'client|reply' command".to_string(),
                    ));
                }
                let mode = bytes_to_string(&args[1])
                    .map_err(|_| NetworkError::Serialization("Invalid UTF-8".to_string()))?
                    .to_uppercase();
                match mode.as_str() {
                    "ON" | "OFF" | "SKIP" => Ok(RespValue::SimpleString("OK".to_string())),
                    _ => Ok(RespValue::Error(
                        "ERR CLIENT REPLY mode must be ON, OFF, or SKIP".to_string(),
                    )),
                }
            }
            "NO-EVICT" => {
                if args.len() != 2 {
                    return Ok(RespValue::Error(
                        "ERR wrong number of arguments for 'client|no-evict' command".to_string(),
                    ));
                }
                let mode = bytes_to_string(&args[1])
                    .map_err(|_| NetworkError::Serialization("Invalid UTF-8".to_string()))?
                    .to_uppercase();
                match mode.as_str() {
                    "ON" | "OFF" => Ok(RespValue::SimpleString("OK".to_string())),
                    _ => Ok(RespValue::Error(
                        "ERR CLIENT NO-EVICT must be ON or OFF".to_string(),
                    )),
                }
            }
            "NO-TOUCH" => {
                if args.len() != 2 {
                    return Ok(RespValue::Error(
                        "ERR wrong number of arguments for 'client|no-touch' command".to_string(),
                    ));
                }
                let mode = bytes_to_string(&args[1])
                    .map_err(|_| NetworkError::Serialization("Invalid UTF-8".to_string()))?
                    .to_uppercase();
                match mode.as_str() {
                    "ON" | "OFF" => Ok(RespValue::SimpleString("OK".to_string())),
                    _ => Ok(RespValue::Error(
                        "ERR CLIENT NO-TOUCH must be ON or OFF".to_string(),
                    )),
                }
            }
            "TRACKING" => {
                if args.len() < 2 {
                    return Ok(RespValue::Error(
                        "ERR wrong number of arguments for 'client|tracking' command".to_string(),
                    ));
                }
                let mode = bytes_to_string(&args[1])
                    .map_err(|_| NetworkError::Serialization("Invalid UTF-8".to_string()))?
                    .to_uppercase();
                match mode.as_str() {
                    "ON" | "OFF" => Ok(RespValue::SimpleString("OK".to_string())),
                    _ => Ok(RespValue::Error(
                        "ERR CLIENT TRACKING must be ON or OFF".to_string(),
                    )),
                }
            }
            "CACHING" => {
                if args.len() != 2 {
                    return Ok(RespValue::Error(
                        "ERR wrong number of arguments for 'client|caching' command".to_string(),
                    ));
                }
                let mode = bytes_to_string(&args[1])
                    .map_err(|_| NetworkError::Serialization("Invalid UTF-8".to_string()))?
                    .to_uppercase();
                match mode.as_str() {
                    "YES" | "NO" => Ok(RespValue::SimpleString("OK".to_string())),
                    _ => Ok(RespValue::Error(
                        "ERR CLIENT CACHING must be YES or NO".to_string(),
                    )),
                }
            }
            "HELP" => {
                let help = vec![
                    RespValue::BulkString(Some(b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".to_vec())),
                    RespValue::BulkString(Some(b"CACHING (YES|NO)".to_vec())),
                    RespValue::BulkString(Some(b"GETNAME".to_vec())),
                    RespValue::BulkString(Some(b"ID".to_vec())),
                    RespValue::BulkString(Some(b"INFO".to_vec())),
                    RespValue::BulkString(Some(b"KILL <option> ...".to_vec())),
                    RespValue::BulkString(Some(b"LIST [TYPE (NORMAL|MASTER|REPLICA|PUBSUB)]".to_vec())),
                    RespValue::BulkString(Some(b"NO-EVICT (ON|OFF)".to_vec())),
                    RespValue::BulkString(Some(b"NO-TOUCH (ON|OFF)".to_vec())),
                    RespValue::BulkString(Some(b"PAUSE <timeout> [WRITE|ALL]".to_vec())),
                    RespValue::BulkString(Some(b"REPLY (ON|OFF|SKIP)".to_vec())),
                    RespValue::BulkString(Some(b"SETNAME <connection-name>".to_vec())),
                    RespValue::BulkString(Some(b"TRACKING (ON|OFF) [REDIRECT <id>] [PREFIX <prefix>] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]".to_vec())),
                    RespValue::BulkString(Some(b"UNPAUSE".to_vec())),
                ];
                Ok(RespValue::Array(Some(help)))
            }
            _ => Ok(RespValue::Error(format!(
                "ERR unknown subcommand or wrong number of arguments for 'client|{}' command",
                subcmd.to_lowercase()
            ))),
        }
    }

    /// Process a command with transaction state handling.
    /// Handles MULTI/EXEC/DISCARD/WATCH/UNWATCH and queuing during transactions.
    #[allow(clippy::too_many_arguments)]
    async fn process_with_transaction(
        cmd: &Command,
        cmd_lower: &str,
        tx_state: &mut TransactionState,
        command_handler: &Arc<CommandHandler>,
        persistence: &Option<Arc<PersistenceManager>>,
        aof_sync_policy: AofSyncPolicy,
        is_auth_command: bool,
        security: &Option<Arc<SecurityManager>>,
        client_addr_str: &str,
        db_index: usize,
    ) -> Result<RespValue, crate::command::CommandError> {
        let engine = command_handler.storage();

        // Handle transaction commands
        match cmd_lower {
            "multi" => return transaction::handle_multi(tx_state),
            "discard" => {
                let had_watches = !tx_state.watched_keys.is_empty();
                let result = transaction::handle_discard(tx_state);
                if had_watches {
                    engine.decrement_watch_count();
                }
                return result;
            }
            "exec" => {
                let had_watches = !tx_state.watched_keys.is_empty();
                // Capture queued commands before EXEC drains them (for AOF logging)
                let queued_commands: Vec<Command> = tx_state.queue.clone();
                let result = CURRENT_DB_INDEX
                    .scope(db_index, async {
                        transaction::handle_exec(tx_state, command_handler, engine, db_index).await
                    })
                    .await?;
                // EXEC always clears watched keys (via reset())
                if had_watches {
                    engine.decrement_watch_count();
                }
                // Log transaction commands to AOF wrapped in MULTI/EXEC as a single
                // atomic batch to prevent interleaving with concurrent clients
                if let Some(persistence_mgr) = persistence
                    && let RespValue::Array(Some(_)) = &result
                {
                    // Transaction succeeded (not aborted by WATCH)
                    // Build the entire transaction as a single batch
                    let mut batch = Vec::with_capacity(queued_commands.len() + 2);
                    batch.push(RespCommand {
                        name: b"MULTI".to_vec(),
                        args: vec![],
                    });
                    for queued_cmd in &queued_commands {
                        let qcmd_lower = queued_cmd.name.to_lowercase();
                        // Include SELECT (for DB context) and write commands
                        if qcmd_lower == "select"
                            || ReplicationManager::is_write_command(&qcmd_lower)
                        {
                            batch.push(RespCommand {
                                name: queued_cmd.name.as_bytes().to_vec(),
                                args: queued_cmd.args.clone(),
                            });
                        }
                    }
                    batch.push(RespCommand {
                        name: b"EXEC".to_vec(),
                        args: vec![],
                    });
                    let _ = persistence_mgr
                        .log_commands_batch_for_db(batch, aof_sync_policy, db_index)
                        .await;
                }
                return Ok(result);
            }
            "watch" => {
                let was_empty = tx_state.watched_keys.is_empty();
                // Increment watch count BEFORE recording key versions to close
                // the race window where a concurrent writer could check
                // active_watch_count == 0, skip bump_key_version, and have
                // the write go undetected by this WATCH session.
                if was_empty {
                    engine.increment_watch_count();
                }
                let result = transaction::handle_watch(tx_state, engine, &cmd.args, db_index);
                // If handle_watch failed or added no keys, undo the increment
                if was_empty && tx_state.watched_keys.is_empty() {
                    engine.decrement_watch_count();
                }
                return result;
            }
            "unwatch" => {
                let had_watches = !tx_state.watched_keys.is_empty();
                let result = transaction::handle_unwatch(tx_state);
                if had_watches {
                    engine.decrement_watch_count();
                }
                return result;
            }
            _ => {}
        }

        // If in a MULTI block, queue the command instead of executing
        if tx_state.in_multi {
            return transaction::queue_command(tx_state, cmd.clone());
        }

        // Execute the command first, then log to AOF only on success
        // Catch CommandError and convert to error response instead of propagating,
        // so pipeline error isolation works correctly (each command's error is independent)
        let result = match command_handler
            .process_bytes(cmd.name.as_bytes(), &cmd.args, db_index)
            .await
        {
            Ok(value) => value,
            Err(err) => return Ok(RespValue::Error(err.to_string())),
        };

        // Log to AOF if persistence is enabled, command succeeded, and it's a write command.
        // Skip AUTH commands to avoid persisting passwords in the AOF file.
        // Skip error responses — failed writes (e.g. WRONGTYPE) must not be persisted.
        if let Some(persistence_mgr) = persistence
            && !is_auth_command
            && !matches!(&result, RespValue::Error(_))
            && ReplicationManager::is_write_command(cmd_lower)
        {
            // Convert blocking commands to non-blocking equivalents for AOF.
            // Blocking commands (BLPOP, BRPOP, etc.) would deadlock during replay,
            // so we log the equivalent non-blocking mutation (LPOP, RPOP, etc.).
            let aof_cmd = if is_blocking_write_command(cmd_lower) {
                convert_blocking_to_nonblocking(cmd, &result)
            } else {
                Some(RespCommand {
                    name: cmd.name.as_bytes().to_vec(),
                    args: cmd.args.clone(),
                })
            };
            if let Some(resp_cmd) = aof_cmd
                && let Err(e) = persistence_mgr
                    .log_command_for_db(resp_cmd, aof_sync_policy, db_index)
                    .await
            {
                error!(client_addr = %client_addr_str, command = ?cmd.name, error = ?e, "Failed to log command to AOF");
                // Command already executed; log the error but don't fail the response
            }
        }

        // For AUTH command, update authentication status
        if is_auth_command
            && let Some(security_mgr) = security
            && let RespValue::SimpleString(ref s) = result
            && s == "OK"
        {
            if cmd.args.len() == 1 {
                // AUTH password (default user)
                let password_str = String::from_utf8_lossy(&cmd.args[0]).to_string();
                match security_mgr.authenticate_with_username(
                    client_addr_str,
                    "default",
                    &password_str,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        return Ok(RespValue::Error(format!("ERR {}", e)));
                    }
                }
            } else if cmd.args.len() == 2 {
                // AUTH username password
                let username = String::from_utf8_lossy(&cmd.args[0]).to_string();
                let password_str = String::from_utf8_lossy(&cmd.args[1]).to_string();
                match security_mgr.authenticate_with_username(
                    client_addr_str,
                    &username,
                    &password_str,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        return Ok(RespValue::Error(format!("ERR {}", e)));
                    }
                }
            }
        }

        Ok(result)
    }

    /// Process commands while in subscription mode
    /// Only (P|S)SUBSCRIBE, (P|S)UNSUBSCRIBE, PING, and QUIT are allowed
    #[allow(clippy::too_many_arguments)]
    async fn process_subscription_commands(
        buffer: &mut BytesMut,
        partial_command_buffer: &mut Vec<u8>,
        socket_writer: &mut ClientWriter,
        pubsub: &Arc<PubSubManager>,
        client_id: ClientId,
        client_addr_str: &str,
        in_subscription_mode: &mut bool,
        security: &Option<Arc<SecurityManager>>,
        protocol_version: ProtocolVersion,
        connections: &Arc<DashMap<u64, ClientInfo>>,
        conn_id: u64,
    ) -> Result<(), NetworkError> {
        // Combine partial buffer if needed
        const BUFFER_SHRINK_THRESHOLD_SUB: usize = 64 * 1024;
        if !partial_command_buffer.is_empty() {
            let mut combined = BytesMut::with_capacity(partial_command_buffer.len() + buffer.len());
            combined.extend_from_slice(partial_command_buffer);
            combined.extend_from_slice(buffer);
            *buffer = combined;
            if partial_command_buffer.capacity() > BUFFER_SHRINK_THRESHOLD_SUB {
                *partial_command_buffer = Vec::new();
            } else {
                partial_command_buffer.clear();
            }
        }

        while !buffer.is_empty() {
            match RespValue::parse(buffer) {
                Ok(Some(value)) => {
                    match parser::parse_command(value) {
                        Ok(cmd) => {
                            let cmd_lower = cmd.name.to_lowercase();

                            match cmd_lower.as_str() {
                                "subscribe" => {
                                    // ACL channel check in subscription mode
                                    if let Some(security_mgr) = security
                                        && let Some(denied) = cmd.args.iter().find(|ch| {
                                            !security_mgr
                                                .check_channel_permission(client_addr_str, ch)
                                        })
                                    {
                                        let ch_str = String::from_utf8_lossy(denied);
                                        let err = RespValue::Error(format!(
                                            "NOPERM No permissions to access channel '{}'",
                                            ch_str
                                        ));
                                        if let Ok(bytes) = err.serialize() {
                                            socket_writer.write_all(&bytes).await?;
                                        }
                                        continue;
                                    }
                                    if let Ok(responses) =
                                        pubsub_commands::subscribe(pubsub, client_id, &cmd.args)
                                            .await
                                    {
                                        for resp in responses {
                                            let resp = if protocol_version == ProtocolVersion::RESP3
                                            {
                                                resp.convert_to_push()
                                            } else {
                                                resp
                                            };
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
                                        Self::update_sub_counts(
                                            pubsub,
                                            client_id,
                                            connections,
                                            conn_id,
                                        )
                                        .await;
                                    }
                                }
                                "unsubscribe" => {
                                    if let Ok(responses) =
                                        pubsub_commands::unsubscribe(pubsub, client_id, &cmd.args)
                                            .await
                                    {
                                        for resp in responses {
                                            let resp = if protocol_version == ProtocolVersion::RESP3
                                            {
                                                resp.convert_to_push()
                                            } else {
                                                resp
                                            };
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
                                        Self::update_sub_counts(
                                            pubsub,
                                            client_id,
                                            connections,
                                            conn_id,
                                        )
                                        .await;
                                        // Check if we should exit subscription mode
                                        let count = pubsub.get_subscription_count(client_id).await;
                                        if count == 0 {
                                            *in_subscription_mode = false;
                                            debug!(client_addr = %client_addr_str, "Exited subscription mode");
                                        }
                                    }
                                }
                                "psubscribe" => {
                                    // ACL channel check in subscription mode
                                    if let Some(security_mgr) = security
                                        && let Some(denied) = cmd.args.iter().find(|ch| {
                                            !security_mgr
                                                .check_channel_permission(client_addr_str, ch)
                                        })
                                    {
                                        let ch_str = String::from_utf8_lossy(denied);
                                        let err = RespValue::Error(format!(
                                            "NOPERM No permissions to access channel '{}'",
                                            ch_str
                                        ));
                                        if let Ok(bytes) = err.serialize() {
                                            socket_writer.write_all(&bytes).await?;
                                        }
                                        continue;
                                    }
                                    if let Ok(responses) =
                                        pubsub_commands::psubscribe(pubsub, client_id, &cmd.args)
                                            .await
                                    {
                                        for resp in responses {
                                            let resp = if protocol_version == ProtocolVersion::RESP3
                                            {
                                                resp.convert_to_push()
                                            } else {
                                                resp
                                            };
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
                                        Self::update_sub_counts(
                                            pubsub,
                                            client_id,
                                            connections,
                                            conn_id,
                                        )
                                        .await;
                                    }
                                }
                                "punsubscribe" => {
                                    if let Ok(responses) =
                                        pubsub_commands::punsubscribe(pubsub, client_id, &cmd.args)
                                            .await
                                    {
                                        for resp in responses {
                                            let resp = if protocol_version == ProtocolVersion::RESP3
                                            {
                                                resp.convert_to_push()
                                            } else {
                                                resp
                                            };
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
                                        Self::update_sub_counts(
                                            pubsub,
                                            client_id,
                                            connections,
                                            conn_id,
                                        )
                                        .await;
                                        // Check if we should exit subscription mode
                                        let count = pubsub.get_subscription_count(client_id).await;
                                        if count == 0 {
                                            *in_subscription_mode = false;
                                            debug!(client_addr = %client_addr_str, "Exited subscription mode");
                                        }
                                    }
                                }
                                "ssubscribe" => {
                                    // ACL channel check in subscription mode
                                    if let Some(security_mgr) = security
                                        && let Some(denied) = cmd.args.iter().find(|ch| {
                                            !security_mgr
                                                .check_channel_permission(client_addr_str, ch)
                                        })
                                    {
                                        let ch_str = String::from_utf8_lossy(denied);
                                        let err = RespValue::Error(format!(
                                            "NOPERM No permissions to access channel '{}'",
                                            ch_str
                                        ));
                                        if let Ok(bytes) = err.serialize() {
                                            socket_writer.write_all(&bytes).await?;
                                        }
                                        continue;
                                    }
                                    if let Ok(responses) =
                                        pubsub_commands::ssubscribe(pubsub, client_id, &cmd.args)
                                            .await
                                    {
                                        for resp in responses {
                                            let resp = if protocol_version == ProtocolVersion::RESP3
                                            {
                                                resp.convert_to_push()
                                            } else {
                                                resp
                                            };
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
                                        Self::update_sub_counts(
                                            pubsub,
                                            client_id,
                                            connections,
                                            conn_id,
                                        )
                                        .await;
                                    }
                                }
                                "sunsubscribe" => {
                                    if let Ok(responses) =
                                        pubsub_commands::sunsubscribe(pubsub, client_id, &cmd.args)
                                            .await
                                    {
                                        for resp in responses {
                                            let resp = if protocol_version == ProtocolVersion::RESP3
                                            {
                                                resp.convert_to_push()
                                            } else {
                                                resp
                                            };
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
                                        Self::update_sub_counts(
                                            pubsub,
                                            client_id,
                                            connections,
                                            conn_id,
                                        )
                                        .await;
                                        // Check if we should exit subscription mode
                                        let count = pubsub.get_subscription_count(client_id).await;
                                        if count == 0 {
                                            *in_subscription_mode = false;
                                            debug!(client_addr = %client_addr_str, "Exited subscription mode");
                                        }
                                    }
                                }
                                "ping" => {
                                    let response = if cmd.args.is_empty() {
                                        RespValue::SimpleString("PONG".to_string())
                                    } else {
                                        RespValue::BulkString(Some(cmd.args[0].clone()))
                                    };
                                    if let Ok(bytes) = response.serialize() {
                                        socket_writer.write_all(&bytes).await?;
                                    }
                                }
                                "quit" => {
                                    let response = RespValue::SimpleString("OK".to_string());
                                    if let Ok(bytes) = response.serialize() {
                                        let _ = socket_writer.write_all(&bytes).await;
                                    }
                                    return Err(NetworkError::ConnectionClosed);
                                }
                                _ => {
                                    let error_msg = "ERR only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT allowed in this context";
                                    Self::send_error_to_writer(
                                        socket_writer,
                                        error_msg.to_string(),
                                        client_addr_str,
                                    )
                                    .await?;
                                }
                            }
                        }
                        Err(e) => {
                            Self::send_error_to_writer(
                                socket_writer,
                                e.to_string(),
                                client_addr_str,
                            )
                            .await?;
                        }
                    }
                }
                Ok(None) => {
                    // Incomplete message
                    partial_command_buffer.extend_from_slice(buffer);
                    buffer.clear();
                    break;
                }
                Err(e) => {
                    let error_msg = format!("ERR Protocol error: {}", e);
                    Self::send_error_to_writer(socket_writer, error_msg, client_addr_str).await?;
                    buffer.clear();
                    if partial_command_buffer.capacity() > BUFFER_SHRINK_THRESHOLD_SUB {
                        *partial_command_buffer = Vec::new();
                    } else {
                        partial_command_buffer.clear();
                    }
                    break;
                }
            }
        }

        Ok(())
    }

    /// Helper function to serialize and send a response to a split socket writer
    async fn send_response_to_writer(
        writer: &mut ClientWriter,
        response: RespValue,
        client_addr: &str,
    ) -> Result<(), NetworkError> {
        match response.serialize() {
            Ok(response_bytes) => {
                logging::log_protocol_data(client_addr, "response", &response_bytes, false);

                if let Err(e) = writer.write_all(&response_bytes).await {
                    error!(client_addr = %client_addr, "Failed to write response: {}", e);
                    Err(NetworkError::Io(e))
                } else {
                    debug!(client_addr = %client_addr, response = ?response, "Sent response");
                    Ok(())
                }
            }
            Err(e) => {
                error!(client_addr = %client_addr, response = ?response, "Failed to serialize response: {:?}", e);
                let error_msg = format!("ERR Internal error during response serialization: {}", e);
                Self::send_error_to_writer(writer, error_msg, client_addr)
                    .await
                    .map_err(|_| NetworkError::Serialization(e.to_string()))
            }
        }
    }

    /// Helper function to serialize and send an error response to a split socket writer
    /// Build a compound command name for RESP3 conversion.
    /// For subcommand-based commands (XINFO STREAM, COMMAND DOCS, etc.),
    /// returns "base subcmd"; otherwise returns just the base command name.
    fn build_resp3_command_name(cmd_lower: &str, args: &[Vec<u8>]) -> String {
        match cmd_lower {
            "xinfo" | "command" | "object" | "memory" | "client" | "cluster" | "slowlog"
            | "latency" | "debug" | "config" => {
                if let Some(sub) = args.first() {
                    format!(
                        "{} {}",
                        cmd_lower,
                        String::from_utf8_lossy(sub).to_lowercase()
                    )
                } else {
                    cmd_lower.to_string()
                }
            }
            _ => cmd_lower.to_string(),
        }
    }

    async fn send_error_to_writer(
        writer: &mut ClientWriter,
        error_message: String,
        client_addr: &str,
    ) -> Result<(), NetworkError> {
        let error_resp = RespValue::Error(error_message);
        match error_resp.serialize() {
            Ok(bytes) => {
                logging::log_protocol_data(client_addr, "error_response", &bytes, false);

                if let Err(e) = writer.write_all(&bytes).await {
                    error!(client_addr = %client_addr, "Failed to write error response: {}", e);
                    Err(NetworkError::Io(e))
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                error!(client_addr = %client_addr, error_resp = ?error_resp, "Failed to serialize error response itself: {:?}", e);
                Err(NetworkError::Serialization(e.to_string()))
            }
        }
    }

    /// Handle PSYNC command - transitions connection into replication stream mode.
    /// After this, the connection only receives propagated write commands.
    async fn handle_psync_command(
        writer: &mut ClientWriter,
        args: &[Vec<u8>],
        replication: &Arc<ReplicationManager>,
        client_addr: &str,
    ) -> Result<(), NetworkError> {
        use tokio::io::AsyncWriteExt;

        if args.len() < 2 {
            let err =
                RespValue::Error("ERR wrong number of arguments for 'psync' command".to_string());
            if let Ok(bytes) = err.serialize() {
                writer.write_all(&bytes).await?;
            }
            return Ok(());
        }

        let replid = String::from_utf8_lossy(&args[0]).to_string();
        let offset_str = String::from_utf8_lossy(&args[1]).to_string();
        let offset: i64 = offset_str.parse().unwrap_or(-1);

        let master_replid = replication.get_master_replid().await;
        let master_offset = replication.get_master_repl_offset();

        // Check if partial resync is possible
        let partial_possible =
            offset >= 0 && replication.can_partial_resync(&replid, offset as u64).await;

        if partial_possible {
            // Partial resync - send CONTINUE and the missing data
            let response = RespValue::SimpleString(format!("CONTINUE {}", master_replid));
            let bytes = response
                .serialize()
                .map_err(|e| NetworkError::Serialization(e.to_string()))?;
            writer.write_all(&bytes).await?;

            // Send backlog data from the replica's offset
            let backlog = replication.backlog().read().await;
            if let Some(data) = backlog.get_from_offset(offset as u64) {
                writer.write_all(&data).await?;
            }
        } else {
            // Full resync - send FULLRESYNC, then RDB, then stream
            let response =
                RespValue::SimpleString(format!("FULLRESYNC {} {}", master_replid, master_offset));
            let bytes = response
                .serialize()
                .map_err(|e| NetworkError::Serialization(e.to_string()))?;
            writer.write_all(&bytes).await?;

            // Generate and send RDB snapshot
            let engine = replication.engine();
            let snapshot = engine
                .snapshot()
                .await
                .map_err(|e| NetworkError::Internal(format!("Failed to get snapshot: {}", e)))?;

            // Serialize snapshot using serde_json (our internal format)
            let rdb_data = serde_json::to_vec(&snapshot).unwrap_or_default();

            // Send RDB as a bulk string: $<length>\r\n<data>
            let header = format!("${}\r\n", rdb_data.len());
            writer.write_all(header.as_bytes()).await?;
            writer.write_all(&rdb_data).await?;
        }

        // Register this replica for command propagation
        let replica_id = client_addr.to_string();
        let (host, port) = client_addr
            .split_once(':')
            .map(|(h, p)| (h.to_string(), p.parse::<u16>().unwrap_or(0)))
            .unwrap_or((client_addr.to_string(), 0));

        let mut rx = replication
            .register_replica(replica_id.clone(), host, port)
            .await;

        info!(client_addr = %client_addr, "Replica entered replication stream mode");

        // Flush any buffered data before entering the replication stream
        writer.flush().await?;

        // Enter replication stream: forward propagated commands to this replica
        loop {
            match rx.recv().await {
                Some(data) => {
                    if let Err(e) = writer.write_all(&data).await {
                        error!(client_addr = %client_addr, "Error writing to replica: {}", e);
                        break;
                    }
                    // Flush replication data immediately for timely delivery
                    if let Err(e) = writer.flush().await {
                        error!(client_addr = %client_addr, "Error flushing replica data: {}", e);
                        break;
                    }
                }
                None => {
                    // Channel closed, replica was unregistered
                    info!(client_addr = %client_addr, "Replication channel closed");
                    break;
                }
            }
        }

        // Clean up
        replication.unregister_replica(&replica_id).await;
        info!(client_addr = %client_addr, "Replica disconnected from replication stream");

        Ok(())
    }

    /// Handle replication commands in the slow path.
    /// Returns Ok(true) if the connection was taken over (PSYNC), Ok(false) otherwise.
    async fn handle_replication_command_slow(
        cmd_lower: &str,
        cmd: &Command,
        _resp_cmd: &RespCommand,
        replication: &Option<Arc<ReplicationManager>>,
        socket_writer: &mut ClientWriter,
        response_buffer: &mut Vec<u8>,
        client_addr_str: &str,
    ) -> Result<bool, NetworkError> {
        match cmd_lower {
            "role" => {
                if let Some(repl) = replication {
                    let response = repl.get_role_response().await;
                    if let Ok(bytes) = response.serialize() {
                        response_buffer.extend_from_slice(&bytes);
                    }
                } else {
                    let response = RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(b"master".to_vec())),
                        RespValue::Integer(0),
                        RespValue::Array(Some(vec![])),
                    ]));
                    if let Ok(bytes) = response.serialize() {
                        response_buffer.extend_from_slice(&bytes);
                    }
                }
            }
            "replicaof" | "slaveof" => {
                if cmd.args.len() != 2 {
                    Self::send_error_to_writer(
                        socket_writer,
                        "ERR wrong number of arguments for 'replicaof' command".to_string(),
                        client_addr_str,
                    )
                    .await?;
                } else if let Some(repl) = replication {
                    let host = String::from_utf8_lossy(&cmd.args[0]).to_string();
                    let port_str = String::from_utf8_lossy(&cmd.args[1]).to_string();
                    match repl.handle_replicaof(&host, &port_str).await {
                        Ok(response) => {
                            if let Ok(bytes) = response.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                        }
                        Err(e) => {
                            Self::send_error_to_writer(
                                socket_writer,
                                format!("ERR {}", e),
                                client_addr_str,
                            )
                            .await?;
                        }
                    }
                } else {
                    let response = RespValue::SimpleString("OK".to_string());
                    if let Ok(bytes) = response.serialize() {
                        response_buffer.extend_from_slice(&bytes);
                    }
                }
            }
            "replconf" => {
                if let Some(repl) = replication
                    && cmd.args.len() >= 2
                {
                    let subcmd = String::from_utf8_lossy(&cmd.args[0]).to_uppercase();
                    if subcmd == "ACK" {
                        if let Ok(offset) = String::from_utf8_lossy(&cmd.args[1]).parse::<u64>() {
                            repl.update_replica_offset(client_addr_str, offset).await;
                        }
                        return Ok(false);
                    }
                }
                let response = RespValue::SimpleString("OK".to_string());
                if let Ok(bytes) = response.serialize() {
                    response_buffer.extend_from_slice(&bytes);
                }
            }
            "psync" => {
                if let Some(repl) = replication {
                    if !response_buffer.is_empty() {
                        socket_writer.write_all(response_buffer).await?;
                        response_buffer.clear();
                    }
                    Self::handle_psync_command(socket_writer, &cmd.args, repl, client_addr_str)
                        .await?;
                    return Ok(true); // Connection taken over
                } else {
                    Self::send_error_to_writer(
                        socket_writer,
                        "ERR PSYNC not supported".to_string(),
                        client_addr_str,
                    )
                    .await?;
                }
            }
            "failover" => {
                if let Some(repl) = replication {
                    match repl.handle_failover().await {
                        Ok(response) => {
                            if let Ok(bytes) = response.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                        }
                        Err(e) => {
                            Self::send_error_to_writer(
                                socket_writer,
                                format!("ERR {}", e),
                                client_addr_str,
                            )
                            .await?;
                        }
                    }
                } else {
                    Self::send_error_to_writer(
                        socket_writer,
                        "ERR FAILOVER not supported".to_string(),
                        client_addr_str,
                    )
                    .await?;
                }
            }
            "wait" => {
                if cmd.args.len() != 2 {
                    let error_resp = RespValue::Error(
                        "ERR wrong number of arguments for 'wait' command".to_string(),
                    );
                    if let Ok(bytes) = error_resp.serialize() {
                        response_buffer.extend_from_slice(&bytes);
                    }
                } else {
                    let numreplicas = match String::from_utf8_lossy(&cmd.args[0]).parse::<usize>() {
                        Ok(n) => n,
                        Err(_) => {
                            let error_resp = RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            );
                            if let Ok(bytes) = error_resp.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                            return Ok(false);
                        }
                    };
                    let timeout_ms = match String::from_utf8_lossy(&cmd.args[1]).parse::<u64>() {
                        Ok(n) => n,
                        Err(_) => {
                            let error_resp = RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            );
                            if let Ok(bytes) = error_resp.serialize() {
                                response_buffer.extend_from_slice(&bytes);
                            }
                            return Ok(false);
                        }
                    };
                    if let Some(repl) = replication {
                        let timeout = if timeout_ms == 0 {
                            std::time::Duration::from_secs(300)
                        } else {
                            std::time::Duration::from_millis(timeout_ms)
                        };
                        if !response_buffer.is_empty() {
                            let _ = socket_writer.write_all(response_buffer).await;
                            response_buffer.clear();
                        }
                        let count = repl.wait_for_replicas(numreplicas, timeout).await;
                        let response = RespValue::Integer(count);
                        if let Ok(bytes) = response.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    } else {
                        let response = RespValue::Integer(0);
                        if let Ok(bytes) = response.serialize() {
                            response_buffer.extend_from_slice(&bytes);
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(false)
    }

    /// Handle the HELLO command for RESP3 protocol negotiation.
    /// Returns the HELLO response and optionally the new protocol version.
    fn handle_hello_command(
        args: &[Vec<u8>],
        current_version: ProtocolVersion,
        security: &Option<Arc<crate::security::SecurityManager>>,
        client_addr: &str,
        connections: &Arc<DashMap<u64, ClientInfo>>,
        conn_id: u64,
    ) -> (RespValue, ProtocolVersion) {
        // HELLO [protover [AUTH username password] [SETNAME clientname]]
        let mut new_version = current_version;

        if !args.is_empty() {
            let proto_str = String::from_utf8_lossy(&args[0]);
            match proto_str.as_ref() {
                "2" => new_version = ProtocolVersion::RESP2,
                "3" => new_version = ProtocolVersion::RESP3,
                _ => {
                    return (
                        RespValue::Error(format!(
                            "NOPROTO unsupported protocol version: {}",
                            proto_str
                        )),
                        current_version,
                    );
                }
            }
        }

        // Process optional AUTH and SETNAME arguments
        let mut i = 1;
        while i < args.len() {
            let option = String::from_utf8_lossy(&args[i]).to_uppercase();
            match option.as_str() {
                "AUTH" => {
                    // AUTH username password
                    if i + 2 >= args.len() {
                        return (
                            RespValue::Error("ERR Syntax error in HELLO option 'auth'".to_string()),
                            current_version,
                        );
                    }
                    let username = String::from_utf8_lossy(&args[i + 1]).to_string();
                    let password = String::from_utf8_lossy(&args[i + 2]).to_string();
                    if let Some(security_mgr) = security
                        && let Err(e) = security_mgr.authenticate_with_username(
                            client_addr,
                            &username,
                            &password,
                        )
                    {
                        return (RespValue::Error(e), current_version);
                    }
                    // When no security manager is configured (no auth required),
                    // silently ignore the AUTH option (matching Redis behavior)
                    i += 3;
                }
                "SETNAME" => {
                    if i + 1 >= args.len() {
                        return (
                            RespValue::Error(
                                "ERR Syntax error in HELLO option 'setname'".to_string(),
                            ),
                            current_version,
                        );
                    }
                    let name = String::from_utf8_lossy(&args[i + 1]).to_string();
                    if name.chars().any(|c| c == ' ' || c.is_control()) {
                        return (
                            RespValue::Error("ERR Client names cannot contain spaces, newlines or special characters.".to_string()),
                            current_version,
                        );
                    }
                    if let Some(mut info) = connections.get_mut(&conn_id) {
                        info.name = name;
                    }
                    i += 2;
                }
                _ => {
                    return (
                        RespValue::Error(format!("ERR Unrecognized HELLO option: {}", option)),
                        current_version,
                    );
                }
            }
        }

        // Build the HELLO response as key-value pairs
        // Redis returns a map with server info
        let server_name = "rLightning";
        let version = env!("CARGO_PKG_VERSION");
        let proto = match new_version {
            ProtocolVersion::RESP2 => 2,
            ProtocolVersion::RESP3 => 3,
        };

        // For RESP2, we return an array of alternating keys and values
        // For RESP3, we return a Map type
        let pairs: Vec<(RespValue, RespValue)> = vec![
            (
                RespValue::BulkString(Some(b"server".to_vec())),
                RespValue::BulkString(Some(server_name.as_bytes().to_vec())),
            ),
            (
                RespValue::BulkString(Some(b"version".to_vec())),
                RespValue::BulkString(Some(version.as_bytes().to_vec())),
            ),
            (
                RespValue::BulkString(Some(b"proto".to_vec())),
                RespValue::Integer(proto),
            ),
            (
                RespValue::BulkString(Some(b"id".to_vec())),
                RespValue::Integer(1),
            ),
            (
                RespValue::BulkString(Some(b"mode".to_vec())),
                RespValue::BulkString(Some(b"standalone".to_vec())),
            ),
            (
                RespValue::BulkString(Some(b"role".to_vec())),
                RespValue::BulkString(Some(b"master".to_vec())),
            ),
            (
                RespValue::BulkString(Some(b"modules".to_vec())),
                RespValue::Array(Some(vec![])),
            ),
        ];

        let response = match new_version {
            ProtocolVersion::RESP3 => RespValue::Map(pairs),
            ProtocolVersion::RESP2 => {
                // Flatten to array of alternating key/value pairs
                let mut flat = Vec::with_capacity(pairs.len() * 2);
                for (k, v) in pairs {
                    flat.push(k);
                    flat.push(v);
                }
                RespValue::Array(Some(flat))
            }
        };

        (response, new_version)
    }
}

/// Convert a blocking command to its non-blocking equivalent for AOF logging
/// and replication propagation. Returns None if the command timed out (nil result,
/// no mutation to persist). This matches Redis behavior where the replication stream
/// and AOF use non-blocking equivalents of blocking commands.
fn convert_blocking_to_nonblocking(cmd: &Command, result: &RespValue) -> Option<RespCommand> {
    let cmd_upper = cmd.name.to_uppercase();
    match cmd_upper.as_str() {
        "BLPOP" | "BRPOP" => {
            // Result: Array(Some([key, value])) on success, Array(None) on timeout
            if let RespValue::Array(Some(items)) = result
                && items.len() >= 2
                && let RespValue::BulkString(Some(key)) = &items[0]
            {
                let name = if cmd_upper == "BLPOP" {
                    b"LPOP".to_vec()
                } else {
                    b"RPOP".to_vec()
                };
                return Some(RespCommand {
                    name,
                    args: vec![key.clone()],
                });
            }
            None
        }
        "BLMOVE" => {
            // BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout
            // -> LMOVE src dst LEFT|RIGHT LEFT|RIGHT (drop last arg = timeout)
            if !matches!(result, RespValue::BulkString(None) | RespValue::Null)
                && cmd.args.len() >= 5
            {
                return Some(RespCommand {
                    name: b"LMOVE".to_vec(),
                    args: cmd.args[..4].to_vec(),
                });
            }
            None
        }
        "BLMPOP" => {
            // BLMPOP timeout numkeys key... LEFT|RIGHT [COUNT count]
            // -> LMPOP numkeys key... LEFT|RIGHT [COUNT count] (drop first arg = timeout)
            if let RespValue::Array(Some(items)) = result
                && !items.is_empty()
                && cmd.args.len() >= 2
            {
                return Some(RespCommand {
                    name: b"LMPOP".to_vec(),
                    args: cmd.args[1..].to_vec(),
                });
            }
            None
        }
        "BZPOPMIN" | "BZPOPMAX" => {
            // Result: Array(Some([key, member, score])) on success
            if let RespValue::Array(Some(items)) = result
                && items.len() >= 3
                && let RespValue::BulkString(Some(key)) = &items[0]
            {
                let name = if cmd_upper == "BZPOPMIN" {
                    b"ZPOPMIN".to_vec()
                } else {
                    b"ZPOPMAX".to_vec()
                };
                return Some(RespCommand {
                    name,
                    args: vec![key.clone()],
                });
            }
            None
        }
        "BZMPOP" => {
            // BZMPOP timeout numkeys key... MIN|MAX [COUNT count]
            // -> ZMPOP numkeys key... MIN|MAX [COUNT count] (drop first arg = timeout)
            if let RespValue::Array(Some(items)) = result
                && !items.is_empty()
                && cmd.args.len() >= 2
            {
                return Some(RespCommand {
                    name: b"ZMPOP".to_vec(),
                    args: cmd.args[1..].to_vec(),
                });
            }
            None
        }
        _ => None,
    }
}

/// Check if a command is a blocking variant that needs conversion for AOF/replication.
fn is_blocking_write_command(cmd_lower: &str) -> bool {
    matches!(
        cmd_lower,
        "blpop" | "brpop" | "blmove" | "blmpop" | "bzpopmin" | "bzpopmax" | "bzmpop"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_connections() -> Arc<DashMap<u64, ClientInfo>> {
        Arc::new(DashMap::new())
    }

    fn args(strs: &[&str]) -> Vec<Vec<u8>> {
        strs.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn test_client_id_returns_correct_id() {
        let connections = make_connections();
        let conn_id = 42;
        connections.insert(
            conn_id,
            ClientInfo::new(conn_id, "127.0.0.1:12345".to_string()),
        );

        let result = Server::handle_client_command(&args(&["ID"]), &connections, conn_id).unwrap();
        assert_eq!(result, RespValue::Integer(42));
    }

    #[test]
    fn test_client_setname_and_getname() {
        let connections = make_connections();
        let conn_id = 1;
        connections.insert(
            conn_id,
            ClientInfo::new(conn_id, "127.0.0.1:12345".to_string()),
        );

        // GETNAME before setting returns nil
        let result =
            Server::handle_client_command(&args(&["GETNAME"]), &connections, conn_id).unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // SETNAME
        let result =
            Server::handle_client_command(&args(&["SETNAME", "myconn"]), &connections, conn_id)
                .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // GETNAME after setting returns the name
        let result =
            Server::handle_client_command(&args(&["GETNAME"]), &connections, conn_id).unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myconn".to_vec())));
    }

    #[test]
    fn test_client_setname_rejects_spaces() {
        let connections = make_connections();
        let conn_id = 1;
        connections.insert(
            conn_id,
            ClientInfo::new(conn_id, "127.0.0.1:12345".to_string()),
        );

        let result =
            Server::handle_client_command(&args(&["SETNAME", "my conn"]), &connections, conn_id)
                .unwrap();
        match result {
            RespValue::Error(msg) => assert!(msg.contains("cannot contain spaces")),
            _ => panic!("Expected error for name with spaces"),
        }
    }

    #[test]
    fn test_client_list_includes_all_connections() {
        let connections = make_connections();
        connections.insert(1, ClientInfo::new(1, "127.0.0.1:1000".to_string()));
        connections.insert(2, ClientInfo::new(2, "127.0.0.1:2000".to_string()));
        connections.insert(3, ClientInfo::new(3, "127.0.0.1:3000".to_string()));

        let result = Server::handle_client_command(&args(&["LIST"]), &connections, 1).unwrap();
        match result {
            RespValue::BulkString(Some(data)) => {
                let output = String::from_utf8(data).unwrap();
                assert!(output.contains("id=1"), "Should contain client 1");
                assert!(output.contains("id=2"), "Should contain client 2");
                assert!(output.contains("id=3"), "Should contain client 3");
                assert!(output.contains("addr=127.0.0.1:1000"));
                assert!(output.contains("addr=127.0.0.1:2000"));
                assert!(output.contains("addr=127.0.0.1:3000"));
                // Each entry should end with \r\n
                let lines: Vec<&str> = output.trim().split("\r\n").collect();
                assert_eq!(lines.len(), 3, "Should have 3 client lines");
            }
            _ => panic!("Expected BulkString for CLIENT LIST"),
        }
    }

    #[test]
    fn test_client_info_returns_current_connection() {
        let connections = make_connections();
        let conn_id = 7;
        let mut info = ClientInfo::new(conn_id, "10.0.0.1:5555".to_string());
        info.name = "worker".to_string();
        info.db = 3;
        connections.insert(conn_id, info);

        let result =
            Server::handle_client_command(&args(&["INFO"]), &connections, conn_id).unwrap();
        match result {
            RespValue::BulkString(Some(data)) => {
                let output = String::from_utf8(data).unwrap();
                assert!(output.contains("id=7"));
                assert!(output.contains("addr=10.0.0.1:5555"));
                assert!(output.contains("name=worker"));
                assert!(output.contains("db=3"));
            }
            _ => panic!("Expected BulkString for CLIENT INFO"),
        }
    }

    #[test]
    fn test_client_list_reflects_state_updates() {
        let connections = make_connections();
        let conn_id = 1;
        connections.insert(
            conn_id,
            ClientInfo::new(conn_id, "127.0.0.1:1234".to_string()),
        );

        // Update state as dispatch_command would
        if let Some(mut info) = connections.get_mut(&conn_id) {
            info.last_cmd = "set".to_string();
            info.db = 5;
            info.multi = 2;
        }

        let result =
            Server::handle_client_command(&args(&["INFO"]), &connections, conn_id).unwrap();
        match result {
            RespValue::BulkString(Some(data)) => {
                let output = String::from_utf8(data).unwrap();
                assert!(output.contains("cmd=set"));
                assert!(output.contains("db=5"));
                assert!(output.contains("multi=2"));
            }
            _ => panic!("Expected BulkString for CLIENT INFO"),
        }
    }

    #[test]
    fn test_client_no_args_returns_error() {
        let connections = make_connections();
        let result = Server::handle_client_command(&args(&[]), &connections, 1).unwrap();
        match result {
            RespValue::Error(msg) => assert!(msg.contains("wrong number of arguments")),
            _ => panic!("Expected error for no args"),
        }
    }

    #[test]
    fn test_client_unknown_subcommand() {
        let connections = make_connections();
        connections.insert(1, ClientInfo::new(1, "127.0.0.1:1234".to_string()));

        let result = Server::handle_client_command(&args(&["BADCMD"]), &connections, 1).unwrap();
        match result {
            RespValue::Error(msg) => assert!(msg.contains("unknown subcommand")),
            _ => panic!("Expected error for unknown subcommand"),
        }
    }

    #[test]
    fn test_connection_info_line_format() {
        let info = ClientInfo::new(42, "127.0.0.1:6379".to_string());
        let line = info.to_info_line();
        assert!(line.starts_with("id=42 addr=127.0.0.1:6379"));
        assert!(line.contains("db=0"));
        assert!(line.contains("multi=-1"));
        assert!(line.ends_with("\r\n"));
    }

    #[test]
    fn test_client_id_counter_increments() {
        // Verify that NEXT_CLIENT_ID produces unique IDs
        let id1 = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        let id2 = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        assert!(id2 > id1, "Client IDs should be monotonically increasing");
    }

    #[test]
    fn test_hello_setname_updates_connection_tracker() {
        let connections = make_connections();
        let conn_id = 1;
        connections.insert(
            conn_id,
            ClientInfo::new(conn_id, "127.0.0.1:12345".to_string()),
        );

        // HELLO 3 SETNAME "myconn" should store the client name
        let (response, version) = Server::handle_hello_command(
            &args(&["3", "SETNAME", "myconn"]),
            ProtocolVersion::RESP2,
            &None,
            "127.0.0.1:12345",
            &connections,
            conn_id,
        );

        // Should negotiate RESP3
        assert_eq!(version, ProtocolVersion::RESP3);

        // Response should not be an error
        match &response {
            RespValue::Error(e) => panic!("Expected success, got error: {}", e),
            _ => {}
        }

        // CLIENT GETNAME should now return "myconn"
        let result =
            Server::handle_client_command(&args(&["GETNAME"]), &connections, conn_id).unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myconn".to_vec())));
    }

    #[test]
    fn test_hello_setname_rejects_spaces() {
        let connections = make_connections();
        let conn_id = 1;
        connections.insert(
            conn_id,
            ClientInfo::new(conn_id, "127.0.0.1:12345".to_string()),
        );

        let (response, _version) = Server::handle_hello_command(
            &args(&["3", "SETNAME", "my conn"]),
            ProtocolVersion::RESP2,
            &None,
            "127.0.0.1:12345",
            &connections,
            conn_id,
        );

        match response {
            RespValue::Error(msg) => assert!(
                msg.contains("cannot contain spaces"),
                "Expected space rejection error, got: {}",
                msg
            ),
            _ => panic!("Expected error for name with spaces"),
        }
    }

    #[test]
    fn test_hello_setname_without_protover() {
        let connections = make_connections();
        let conn_id = 1;
        connections.insert(
            conn_id,
            ClientInfo::new(conn_id, "127.0.0.1:12345".to_string()),
        );

        // HELLO with no args (just negotiate, keep current protocol)
        let (response, version) = Server::handle_hello_command(
            &args(&[]),
            ProtocolVersion::RESP2,
            &None,
            "127.0.0.1:12345",
            &connections,
            conn_id,
        );

        assert_eq!(version, ProtocolVersion::RESP2);
        match &response {
            RespValue::Error(e) => panic!("Expected success, got error: {}", e),
            _ => {}
        }
    }

    #[test]
    fn test_hello_setname_missing_value() {
        let connections = make_connections();
        let conn_id = 1;
        connections.insert(
            conn_id,
            ClientInfo::new(conn_id, "127.0.0.1:12345".to_string()),
        );

        // HELLO 3 SETNAME (missing client name value)
        let (response, _version) = Server::handle_hello_command(
            &args(&["3", "SETNAME"]),
            ProtocolVersion::RESP2,
            &None,
            "127.0.0.1:12345",
            &connections,
            conn_id,
        );

        match response {
            RespValue::Error(msg) => assert!(
                msg.contains("Syntax error"),
                "Expected syntax error, got: {}",
                msg
            ),
            _ => panic!("Expected error for missing SETNAME value"),
        }
    }

    #[tokio::test]
    async fn test_tcp_nodelay_set_on_accepted_connections() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn a client connection
        let client = tokio::spawn(async move {
            tokio::net::TcpStream::connect(addr).await.unwrap()
        });

        // Accept and set TCP_NODELAY (same as our server accept path)
        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        assert!(socket.nodelay().unwrap(), "TCP_NODELAY should be enabled on accepted connections");

        let _ = client.await;
    }

    #[test]
    fn test_created_at_uses_unix_timestamp() {
        let info = ClientInfo::new(1, "127.0.0.1:1234".to_string());
        // created_at should be a reasonable unix timestamp (after year 2020)
        assert!(info.created_at > 1_577_836_800, "created_at should be a unix timestamp after 2020");
        // Age should be 0 or 1 second since we just created it
        let line = info.to_info_line();
        assert!(line.contains("age=0") || line.contains("age=1"),
            "Newly created connection should have age of 0 or 1, got: {}", line);
    }

    #[test]
    fn test_last_cmd_deferred_update() {
        // Verify that last_cmd field still exists and works when set directly
        // (as it would be when synced from local tracking on CLIENT command)
        let connections = make_connections();
        let conn_id = 1;
        connections.insert(conn_id, ClientInfo::new(conn_id, "127.0.0.1:1234".to_string()));

        // Simulate local last_cmd sync (as dispatch_command does before CLIENT)
        if let Some(mut info) = connections.get_mut(&conn_id) {
            info.last_cmd = "get".to_string();
            info.db = 2;
            info.multi = -1;
        }

        let result = Server::handle_client_command(&args(&["INFO"]), &connections, conn_id).unwrap();
        match result {
            RespValue::BulkString(Some(data)) => {
                let output = String::from_utf8(data).unwrap();
                assert!(output.contains("cmd=get"), "Should show synced last_cmd");
                assert!(output.contains("db=2"), "Should show synced db index");
            }
            _ => panic!("Expected BulkString for CLIENT INFO"),
        }
    }

    #[test]
    fn test_buffer_shrink_behavior() {
        const BUFFER_SHRINK_THRESHOLD: usize = 64 * 1024;
        let buffer_size = 8192;

        // Simulate response_buffer growing large then being cleared + shrunk
        let mut response_buffer = Vec::with_capacity(buffer_size);

        // Fill buffer beyond threshold
        response_buffer.extend_from_slice(&vec![0u8; 128 * 1024]);
        assert!(response_buffer.capacity() > BUFFER_SHRINK_THRESHOLD);

        // Clear and shrink (mimics the hot path logic)
        response_buffer.clear();
        if response_buffer.capacity() > BUFFER_SHRINK_THRESHOLD {
            response_buffer.shrink_to(buffer_size);
        }

        // Capacity should be reduced to approximately buffer_size
        assert!(
            response_buffer.capacity() <= BUFFER_SHRINK_THRESHOLD,
            "Buffer should have shrunk: capacity={}, threshold={}",
            response_buffer.capacity(),
            BUFFER_SHRINK_THRESHOLD
        );

        // Simulate partial_command_buffer replacement
        let mut partial_buf: Vec<u8> = Vec::new();
        partial_buf.extend_from_slice(&vec![0u8; 128 * 1024]);
        assert!(partial_buf.capacity() > BUFFER_SHRINK_THRESHOLD);

        // Replace with new Vec (mimics the hot path logic)
        if partial_buf.capacity() > BUFFER_SHRINK_THRESHOLD {
            partial_buf = Vec::new();
        }

        // Should be a fresh allocation
        assert_eq!(partial_buf.capacity(), 0);
    }
}
