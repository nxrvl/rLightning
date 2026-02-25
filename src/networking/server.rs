use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Semaphore};
use tracing::{debug, error, info, warn};

use crate::command::Command;
use crate::command::handler::CommandHandler;
use crate::command::parser;
use crate::command::transaction::{self, TransactionState};
use crate::command::types::pubsub::{
    self as pubsub_commands, subscription_message_to_resp, ClientId,
};
use crate::networking::error::NetworkError;
use crate::networking::resp::{ProtocolVersion, RespCommand, RespValue};
use crate::persistence::PersistenceManager;
use crate::persistence::config::AofSyncPolicy;
use crate::pubsub::PubSubManager;
use crate::replication::ReplicationManager;
use crate::security::SecurityManager;
use crate::sentinel::SentinelManager;
use crate::storage::engine::StorageEngine;
use crate::utils::logging;

/// The Redis-compatible server
pub struct Server {
    addr: SocketAddr,
    command_handler: Arc<CommandHandler>,
    pubsub: Arc<PubSubManager>,
    persistence: Option<Arc<PersistenceManager>>,
    security: Option<Arc<SecurityManager>>,
    replication: Option<Arc<ReplicationManager>>,
    sentinel: Option<Arc<SentinelManager>>,
    aof_sync_policy: AofSyncPolicy,
    connection_limit: Arc<Semaphore>,
    buffer_size: usize,
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
            aof_sync_policy: AofSyncPolicy::EverySecond,
            connection_limit: Arc::new(Semaphore::new(10000)), // Default to 10K connections max
            buffer_size: 64 * 1024,                              // 64KB initial buffer (grows on demand)
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
                            info!(client_addr = %addr, "New client connected");
                            let cmd_handler = Arc::clone(&self.command_handler);
                            let pubsub = Arc::clone(&self.pubsub);
                            let persistence = self.persistence.clone();
                            let security = self.security.clone();
                            let replication = self.replication.clone();
                            let sentinel = self.sentinel.clone();
                            let aof_sync_policy = self.aof_sync_policy;
                            let buffer_size = self.buffer_size;

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
                                    aof_sync_policy,
                                    buffer_size,
                                )
                                .await
                                {
                                    error!(client_addr = %addr, "Error handling client: {}", e);
                                }
                                info!(client_addr = %addr, "Client disconnected");
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
    async fn handle_client(
        socket: TcpStream,
        command_handler: Arc<CommandHandler>,
        pubsub: Arc<PubSubManager>,
        persistence: Option<Arc<PersistenceManager>>,
        security: Option<Arc<SecurityManager>>,
        replication: Option<Arc<ReplicationManager>>,
        sentinel: Option<Arc<SentinelManager>>,
        aof_sync_policy: AofSyncPolicy,
        buffer_size: usize,
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
        const MAX_PARTIAL_BUFFER_SIZE: usize = 256 * 1024 * 1024; // 256MB limit for partial command buffer

        // Split the socket for concurrent read/write in subscription mode
        let (mut socket_reader, mut socket_writer) = socket.into_split();

        // Track if we're in subscription mode
        let mut in_subscription_mode = false;

        // Track the per-connection protocol version (RESP2 by default)
        let mut _protocol_version = ProtocolVersion::RESP2;

        // Per-connection transaction state for MULTI/EXEC/WATCH
        let mut tx_state = TransactionState::new();

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
                                if let Ok(bytes) = response.serialize() {
                                    if let Err(e) = socket_writer.write_all(&bytes).await {
                                        error!(client_addr = %client_addr_str, "Failed to write pub/sub message: {}", e);
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
                                ).await;

                                if let Err(e) = result {
                                    error!(client_addr = %client_addr_str, "Error processing subscription commands: {}", e);
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
                let mut commands_processed = 0;

                // Process complete RESP messages from the buffer directly
                while !buffer.is_empty() {
                    // If we have leftover data from previous read, prepend it to the buffer
                    if !partial_command_buffer.is_empty() {
                        let mut combined =
                            BytesMut::with_capacity(partial_command_buffer.len() + buffer.len());
                        combined.extend_from_slice(&partial_command_buffer);
                        combined.extend_from_slice(&buffer);
                        buffer = combined;
                        partial_command_buffer.clear();
                        debug!(client_addr = %client_addr_str, combined_buffer = buffer.len(), "Combined partial buffer with new data");
                    }

                    // Try the fast path for common commands first
                    match RespValue::try_parse_common_command(&mut buffer) {
                        Ok(Some(resp_cmd)) => {
                            debug!(client_addr = %client_addr_str, command = ?resp_cmd, "Fast path command");

                            let cmd = Command {
                                name: String::from_utf8_lossy(&resp_cmd.name).to_string(),
                                args: resp_cmd.args.clone(),
                            };

                            let cmd_lower = cmd.name.to_lowercase();

                            // Check for HELLO command (protocol negotiation)
                            if cmd_lower == "hello" {
                                let (response, new_version) = Self::handle_hello_command(&cmd.args, _protocol_version, &security, &client_addr_str);
                                _protocol_version = new_version;
                                if let Ok(bytes) = response.serialize() {
                                    response_buffer.extend_from_slice(&bytes);
                                }
                                commands_processed += 1;
                                continue;
                            }

                            // Early authentication check - block unauthenticated access
                            // to all commands except AUTH, HELLO, and QUIT
                            let is_auth_command = cmd_lower == "auth";
                            if !is_auth_command && cmd_lower != "quit" {
                                if let Some(ref security_mgr) = security {
                                    if security_mgr.require_auth()
                                        && !security_mgr.is_authenticated(&client_addr_str)
                                    {
                                        let error_msg = "NOAUTH Authentication required.";
                                        Self::send_error_to_writer(
                                            &mut socket_writer,
                                            error_msg.to_string(),
                                            &client_addr_str,
                                        )
                                        .await?;
                                        commands_processed += 1;
                                        continue;
                                    }
                                }
                            }

                            // ACL permission checks for authenticated clients
                            // Must run BEFORE dispatching to special command handlers (pubsub, acl, sentinel, replication)
                            if let Some(ref security_mgr) = security {
                                if !is_auth_command && security_mgr.is_authenticated(&client_addr_str) {
                                    // Check command permission
                                    if !security_mgr.check_command_permission(&client_addr_str, &cmd_lower) {
                                        let username = security_mgr.acl().get_username(&client_addr_str).unwrap_or_else(|| "default".to_string());
                                        security_mgr.acl().log_denial(&client_addr_str, &username, &cmd_lower, "command");
                                        let error_msg = format!("NOPERM this user has no permissions to run the '{}' command", cmd_lower);
                                        Self::send_error_to_writer(
                                            &mut socket_writer,
                                            error_msg,
                                            &client_addr_str,
                                        )
                                        .await?;
                                        commands_processed += 1;
                                        continue;
                                    }

                                    // Check key permission
                                    let key_indices = crate::security::acl::get_key_indices(&cmd_lower, &cmd.args);
                                    let mut key_denied = false;
                                    for &idx in &key_indices {
                                        if idx < cmd.args.len() && !security_mgr.check_key_permission(&client_addr_str, &cmd.args[idx]) {
                                            let username = security_mgr.acl().get_username(&client_addr_str).unwrap_or_else(|| "default".to_string());
                                            let key_str = String::from_utf8_lossy(&cmd.args[idx]);
                                            security_mgr.acl().log_denial(&client_addr_str, &username, &cmd_lower, &format!("key '{}'", key_str));
                                            let error_msg = format!("NOPERM this user has no permissions to access one of the keys used as arguments");
                                            Self::send_error_to_writer(
                                                &mut socket_writer,
                                                error_msg,
                                                &client_addr_str,
                                            )
                                            .await?;
                                            key_denied = true;
                                            break;
                                        }
                                    }
                                    if key_denied {
                                        commands_processed += 1;
                                        continue;
                                    }
                                }
                            }

                            // Check for connection-closing commands
                            if cmd_lower == "quit" {
                                let response = RespValue::SimpleString("OK".to_string());
                                if let Ok(bytes) = response.serialize() {
                                    let _ = socket_writer.write_all(&bytes).await;
                                    let _ = socket_writer.flush().await;
                                }
                                // Cleanup before exiting
                                pubsub.unregister_client(client_id).await;
                                if let Some(ref security_mgr) = security {
                                    security_mgr.remove_client(&client_addr_str);
                                }
                                return Ok(());
                            }

                            // Check for pub/sub commands
                            match cmd_lower.as_str() {
                                "subscribe" => {
                                    // ACL channel check
                                    if let Some(ref security_mgr) = security {
                                        let denied = cmd.args.iter().find(|ch| !security_mgr.check_channel_permission(&client_addr_str, ch));
                                        if let Some(channel) = denied {
                                            let error_msg = format!("NOPERM this user has no permissions to access the '{}' channel", String::from_utf8_lossy(channel));
                                            Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                            commands_processed += 1;
                                            continue;
                                        }
                                    }
                                    let responses = pubsub_commands::subscribe(&pubsub, client_id, &cmd.args).await;
                                    match responses {
                                        Ok(resp_list) => {
                                            for resp in resp_list {
                                                if let Ok(bytes) = resp.serialize() {
                                                    response_buffer.extend_from_slice(&bytes);
                                                }
                                            }
                                            in_subscription_mode = true;
                                        }
                                        Err(e) => {
                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                        }
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                "psubscribe" => {
                                    // ACL channel check (patterns are checked as-is)
                                    if let Some(ref security_mgr) = security {
                                        let denied = cmd.args.iter().find(|ch| !security_mgr.check_channel_permission(&client_addr_str, ch));
                                        if let Some(channel) = denied {
                                            let error_msg = format!("NOPERM this user has no permissions to access the '{}' channel", String::from_utf8_lossy(channel));
                                            Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                            commands_processed += 1;
                                            continue;
                                        }
                                    }
                                    let responses = pubsub_commands::psubscribe(&pubsub, client_id, &cmd.args).await;
                                    match responses {
                                        Ok(resp_list) => {
                                            for resp in resp_list {
                                                if let Ok(bytes) = resp.serialize() {
                                                    response_buffer.extend_from_slice(&bytes);
                                                }
                                            }
                                            in_subscription_mode = true;
                                        }
                                        Err(e) => {
                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                        }
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                "publish" => {
                                    // ACL channel check (first arg is channel)
                                    if let Some(ref security_mgr) = security {
                                        if !cmd.args.is_empty() && !security_mgr.check_channel_permission(&client_addr_str, &cmd.args[0]) {
                                            let error_msg = format!("NOPERM this user has no permissions to access the '{}' channel", String::from_utf8_lossy(&cmd.args[0]));
                                            Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                            commands_processed += 1;
                                            continue;
                                        }
                                    }
                                    let response = pubsub_commands::publish(&pubsub, &cmd.args).await;
                                    match response {
                                        Ok(resp) => {
                                            if let Ok(bytes) = resp.serialize() {
                                                response_buffer.extend_from_slice(&bytes);
                                            }
                                        }
                                        Err(e) => {
                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                        }
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                "ssubscribe" => {
                                    // ACL channel check
                                    if let Some(ref security_mgr) = security {
                                        let denied = cmd.args.iter().find(|ch| !security_mgr.check_channel_permission(&client_addr_str, ch));
                                        if let Some(channel) = denied {
                                            let error_msg = format!("NOPERM this user has no permissions to access the '{}' channel", String::from_utf8_lossy(channel));
                                            Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                            commands_processed += 1;
                                            continue;
                                        }
                                    }
                                    let responses = pubsub_commands::ssubscribe(&pubsub, client_id, &cmd.args).await;
                                    match responses {
                                        Ok(resp_list) => {
                                            for resp in resp_list {
                                                if let Ok(bytes) = resp.serialize() {
                                                    response_buffer.extend_from_slice(&bytes);
                                                }
                                            }
                                            in_subscription_mode = true;
                                        }
                                        Err(e) => {
                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                        }
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                "spublish" => {
                                    // ACL channel check (first arg is channel)
                                    if let Some(ref security_mgr) = security {
                                        if !cmd.args.is_empty() && !security_mgr.check_channel_permission(&client_addr_str, &cmd.args[0]) {
                                            let error_msg = format!("NOPERM this user has no permissions to access the '{}' channel", String::from_utf8_lossy(&cmd.args[0]));
                                            Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                            commands_processed += 1;
                                            continue;
                                        }
                                    }
                                    let response = pubsub_commands::spublish(&pubsub, &cmd.args).await;
                                    match response {
                                        Ok(resp) => {
                                            if let Ok(bytes) = resp.serialize() {
                                                response_buffer.extend_from_slice(&bytes);
                                            }
                                        }
                                        Err(e) => {
                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                        }
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                "pubsub" => {
                                    let response = pubsub_commands::pubsub_command(&pubsub, &cmd.args).await;
                                    match response {
                                        Ok(resp) => {
                                            if let Ok(bytes) = resp.serialize() {
                                                response_buffer.extend_from_slice(&bytes);
                                            }
                                        }
                                        Err(e) => {
                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                        }
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                // Handle ACL commands
                                "acl" => {
                                    if let Some(ref security_mgr) = security {
                                        let response = security_mgr.acl().handle_acl_command(&cmd.args, &client_addr_str);
                                        match response {
                                            Ok(resp) => {
                                                if let Ok(bytes) = resp.serialize() {
                                                    response_buffer.extend_from_slice(&bytes);
                                                }
                                            }
                                            Err(e) => {
                                                Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                            }
                                        }
                                    } else {
                                        let response = RespValue::SimpleString("OK".to_string());
                                        if let Ok(bytes) = response.serialize() {
                                            response_buffer.extend_from_slice(&bytes);
                                        }
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                // Handle sentinel commands
                                "sentinel" => {
                                    if let Some(ref sentinel_mgr) = sentinel {
                                        match sentinel_mgr.handle_sentinel_command(&cmd.args).await {
                                            Ok(resp) => {
                                                if let Ok(bytes) = resp.serialize() {
                                                    response_buffer.extend_from_slice(&bytes);
                                                }
                                            }
                                            Err(e) => {
                                                Self::send_error_to_writer(&mut socket_writer, e, &client_addr_str).await?;
                                            }
                                        }
                                    } else {
                                        let response = RespValue::Error("ERR This instance has sentinel support disabled".to_string());
                                        if let Ok(bytes) = response.serialize() {
                                            response_buffer.extend_from_slice(&bytes);
                                        }
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                // Handle replication commands
                                "role" => {
                                    if let Some(ref repl) = replication {
                                        let response = repl.get_role_response().await;
                                        if let Ok(bytes) = response.serialize() {
                                            response_buffer.extend_from_slice(&bytes);
                                        }
                                    } else {
                                        // Default: we're a master with no replicas
                                        let response = RespValue::Array(Some(vec![
                                            RespValue::BulkString(Some(b"master".to_vec())),
                                            RespValue::Integer(0),
                                            RespValue::Array(Some(vec![])),
                                        ]));
                                        if let Ok(bytes) = response.serialize() {
                                            response_buffer.extend_from_slice(&bytes);
                                        }
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                "replicaof" | "slaveof" => {
                                    if cmd.args.len() != 2 {
                                        Self::send_error_to_writer(&mut socket_writer, "ERR wrong number of arguments for 'replicaof' command".to_string(), &client_addr_str).await?;
                                    } else if let Some(ref repl) = replication {
                                        let host = String::from_utf8_lossy(&cmd.args[0]).to_string();
                                        let port_str = String::from_utf8_lossy(&cmd.args[1]).to_string();
                                        match repl.handle_replicaof(&host, &port_str).await {
                                            Ok(response) => {
                                                if let Ok(bytes) = response.serialize() {
                                                    response_buffer.extend_from_slice(&bytes);
                                                }
                                            }
                                            Err(e) => {
                                                Self::send_error_to_writer(&mut socket_writer, format!("ERR {}", e), &client_addr_str).await?;
                                            }
                                        }
                                    } else {
                                        let response = RespValue::SimpleString("OK".to_string());
                                        if let Ok(bytes) = response.serialize() {
                                            response_buffer.extend_from_slice(&bytes);
                                        }
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                "replconf" => {
                                    // Handle REPLCONF from replicas (ACK mainly)
                                    if let Some(ref repl) = replication {
                                        if cmd.args.len() >= 2 {
                                            let subcmd = String::from_utf8_lossy(&cmd.args[0]).to_uppercase();
                                            if subcmd == "ACK" {
                                                if let Ok(offset) = String::from_utf8_lossy(&cmd.args[1]).parse::<u64>() {
                                                    repl.update_replica_offset(&client_addr_str, offset).await;
                                                }
                                                // REPLCONF ACK doesn't get a response in replication stream
                                                commands_processed += 1;
                                                continue;
                                            }
                                        }
                                    }
                                    let response = RespValue::SimpleString("OK".to_string());
                                    if let Ok(bytes) = response.serialize() {
                                        response_buffer.extend_from_slice(&bytes);
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                "psync" => {
                                    // PSYNC is handled specially - transitions to replication stream
                                    if let Some(ref repl) = replication {
                                        // Flush any pending responses first
                                        if !response_buffer.is_empty() {
                                            if let Err(e) = socket_writer.write_all(&response_buffer).await {
                                                error!(client_addr = %client_addr_str, "Failed to flush before PSYNC: {}", e);
                                                break;
                                            }
                                            response_buffer.clear();
                                        }

                                        // Handle the PSYNC and enter replication stream mode
                                        if let Err(e) = Self::handle_psync_command(
                                            &mut socket_writer,
                                            &cmd.args,
                                            &repl,
                                            &client_addr_str,
                                        ).await {
                                            error!(client_addr = %client_addr_str, "PSYNC error: {}", e);
                                        }
                                        // After PSYNC, the connection is in replication mode
                                        // The replica will only receive propagated commands
                                        // We exit the normal client loop
                                        // Cleanup before exiting
                                        pubsub.unregister_client(client_id).await;
                                        if let Some(ref security_mgr) = security {
                                            security_mgr.remove_client(&client_addr_str);
                                        }
                                        return Ok(());
                                    } else {
                                        Self::send_error_to_writer(&mut socket_writer, "ERR PSYNC not supported".to_string(), &client_addr_str).await?;
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                "failover" => {
                                    if let Some(ref repl) = replication {
                                        match repl.handle_failover().await {
                                            Ok(response) => {
                                                if let Ok(bytes) = response.serialize() {
                                                    response_buffer.extend_from_slice(&bytes);
                                                }
                                            }
                                            Err(e) => {
                                                Self::send_error_to_writer(&mut socket_writer, format!("ERR {}", e), &client_addr_str).await?;
                                            }
                                        }
                                    } else {
                                        Self::send_error_to_writer(&mut socket_writer, "ERR FAILOVER not supported".to_string(), &client_addr_str).await?;
                                    }
                                    commands_processed += 1;
                                    continue;
                                }
                                "wait" => {
                                    if cmd.args.len() != 2 {
                                        Self::send_error_to_writer(&mut socket_writer, "ERR wrong number of arguments for 'wait' command".to_string(), &client_addr_str).await?;
                                    } else if let Some(ref repl) = replication {
                                        let numreplicas = String::from_utf8_lossy(&cmd.args[0]).parse::<usize>().unwrap_or(0);
                                        let timeout_ms = String::from_utf8_lossy(&cmd.args[1]).parse::<u64>().unwrap_or(0);
                                        let timeout = if timeout_ms == 0 {
                                            std::time::Duration::from_secs(300) // 5 min max
                                        } else {
                                            std::time::Duration::from_millis(timeout_ms)
                                        };
                                        // Flush responses before blocking
                                        if !response_buffer.is_empty() {
                                            let _ = socket_writer.write_all(&response_buffer).await;
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
                                    commands_processed += 1;
                                    continue;
                                }
                                _ => {}
                            }

                            // Check read-only mode (replica rejecting write commands)
                            if let Some(ref repl) = replication {
                                if repl.is_read_only() && ReplicationManager::is_write_command(&cmd_lower) {
                                    Self::send_error_to_writer(&mut socket_writer, "READONLY You can't write against a read only replica.".to_string(), &client_addr_str).await?;
                                    commands_processed += 1;
                                    continue;
                                }
                            }

                            // Capture queued commands before EXEC drains them (for replication)
                            let queued_for_repl: Vec<Command> = if cmd_lower == "exec" {
                                tx_state.queue.clone()
                            } else {
                                vec![]
                            };

                            // Handle transaction commands (MULTI/EXEC/DISCARD/WATCH/UNWATCH)
                            let tx_result = Self::process_with_transaction(
                                &cmd,
                                &cmd_lower,
                                &mut tx_state,
                                &command_handler,
                                &persistence,
                                aof_sync_policy,
                                is_auth_command,
                                &security,
                                &client_addr_str,
                            ).await;

                            match tx_result {
                                Ok(response) => {
                                    // Propagate write commands to replicas
                                    if let Some(ref repl) = replication {
                                        if cmd_lower == "exec" {
                                            // For EXEC, propagate queued write commands individually
                                            if let RespValue::Array(Some(_)) = &response {
                                                for queued_cmd in &queued_for_repl {
                                                    let qcmd_lower = queued_cmd.name.to_lowercase();
                                                    if ReplicationManager::is_write_command(&qcmd_lower) {
                                                        let repl_cmd = RespCommand {
                                                            name: queued_cmd.name.as_bytes().to_vec(),
                                                            args: queued_cmd.args.clone(),
                                                        };
                                                        repl.propagate_command(&repl_cmd).await;
                                                    }
                                                }
                                            }
                                        } else if ReplicationManager::is_write_command(&cmd_lower) {
                                            repl.propagate_command(&resp_cmd).await;
                                        }
                                    }
                                    if let Ok(bytes) = response.serialize() {
                                        response_buffer.extend_from_slice(&bytes);
                                    } else {
                                        Self::send_response_to_writer(&mut socket_writer, response, &client_addr_str)
                                            .await?;
                                    }
                                }
                                Err(err) => {
                                    Self::send_error_to_writer(&mut socket_writer, err.to_string(), &client_addr_str)
                                        .await?;
                                }
                            }

                            commands_processed += 1;
                        }
                        Ok(None) => {
                            // Fast path didn't match, try regular parsing
                            match RespValue::parse(&mut buffer) {
                                Ok(Some(value)) => {
                                    debug!(client_addr = %client_addr_str, value = ?value, "Received RESP value");

                                    // Convert to a command
                                    match parser::parse_command(value) {
                                        Ok(cmd) => {
                                            debug!(client_addr = %client_addr_str, command = ?cmd, "Processing command");

                                            let cmd_lower = cmd.name.to_lowercase();

                                            // Check for HELLO command (protocol negotiation)
                                            if cmd_lower == "hello" {
                                                let (response, new_version) = Self::handle_hello_command(&cmd.args, _protocol_version, &security, &client_addr_str);
                                                _protocol_version = new_version;
                                                if let Ok(bytes) = response.serialize() {
                                                    response_buffer.extend_from_slice(&bytes);
                                                }
                                                commands_processed += 1;
                                                continue;
                                            }

                                            // Handle QUIT - close connection (slow path)
                                            if cmd_lower == "quit" {
                                                let response = RespValue::SimpleString("OK".to_string());
                                                if let Ok(bytes) = response.serialize() {
                                                    let _ = socket_writer.write_all(&bytes).await;
                                                    let _ = socket_writer.flush().await;
                                                }
                                                // Cleanup before exiting
                                                pubsub.unregister_client(client_id).await;
                                                if let Some(ref security_mgr) = security {
                                                    security_mgr.remove_client(&client_addr_str);
                                                }
                                                return Ok(());
                                            }

                                            // Early authentication check for slow path
                                            let is_auth_command = cmd_lower == "auth";
                                            if !is_auth_command {
                                                if let Some(ref security_mgr) = security {
                                                    if security_mgr.require_auth()
                                                        && !security_mgr.is_authenticated(&client_addr_str)
                                                    {
                                                        let error_msg = "NOAUTH Authentication required.";
                                                        Self::send_error_to_writer(
                                                            &mut socket_writer,
                                                            error_msg.to_string(),
                                                            &client_addr_str,
                                                        )
                                                        .await?;
                                                        commands_processed += 1;
                                                        continue;
                                                    }
                                                }
                                            }

                                            // ACL permission checks for authenticated clients (slow path)
                                            // Must run BEFORE dispatching to special command handlers
                                            if let Some(ref security_mgr) = security {
                                                if !is_auth_command && security_mgr.is_authenticated(&client_addr_str) {
                                                    // Check command permission
                                                    if !security_mgr.check_command_permission(&client_addr_str, &cmd_lower) {
                                                        let username = security_mgr.acl().get_username(&client_addr_str).unwrap_or_else(|| "default".to_string());
                                                        security_mgr.acl().log_denial(&client_addr_str, &username, &cmd_lower, "command");
                                                        let error_msg = format!("NOPERM this user has no permissions to run the '{}' command", cmd_lower);
                                                        Self::send_error_to_writer(
                                                            &mut socket_writer,
                                                            error_msg,
                                                            &client_addr_str,
                                                        )
                                                        .await?;
                                                        commands_processed += 1;
                                                        continue;
                                                    }

                                                    // Check key permission
                                                    let key_indices = crate::security::acl::get_key_indices(&cmd_lower, &cmd.args);
                                                    let mut key_denied = false;
                                                    for &idx in &key_indices {
                                                        if idx < cmd.args.len() && !security_mgr.check_key_permission(&client_addr_str, &cmd.args[idx]) {
                                                            let username = security_mgr.acl().get_username(&client_addr_str).unwrap_or_else(|| "default".to_string());
                                                            let key_str = String::from_utf8_lossy(&cmd.args[idx]);
                                                            security_mgr.acl().log_denial(&client_addr_str, &username, &cmd_lower, &format!("key '{}'", key_str));
                                                            let error_msg = format!("NOPERM this user has no permissions to access one of the keys used as arguments");
                                                            Self::send_error_to_writer(
                                                                &mut socket_writer,
                                                                error_msg,
                                                                &client_addr_str,
                                                            )
                                                            .await?;
                                                            key_denied = true;
                                                            break;
                                                        }
                                                    }
                                                    if key_denied {
                                                        commands_processed += 1;
                                                        continue;
                                                    }
                                                }
                                            }

                                            // Check for pub/sub commands
                                            match cmd_lower.as_str() {
                                                "subscribe" => {
                                                    // ACL channel check
                                                    if let Some(ref security_mgr) = security {
                                                        let denied = cmd.args.iter().find(|ch| !security_mgr.check_channel_permission(&client_addr_str, ch));
                                                        if let Some(channel) = denied {
                                                            let error_msg = format!("NOPERM this user has no permissions to access the '{}' channel", String::from_utf8_lossy(channel));
                                                            Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                                            commands_processed += 1;
                                                            continue;
                                                        }
                                                    }
                                                    let responses = pubsub_commands::subscribe(&pubsub, client_id, &cmd.args).await;
                                                    match responses {
                                                        Ok(resp_list) => {
                                                            for resp in resp_list {
                                                                if let Ok(bytes) = resp.serialize() {
                                                                    response_buffer.extend_from_slice(&bytes);
                                                                }
                                                            }
                                                            in_subscription_mode = true;
                                                        }
                                                        Err(e) => {
                                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                                        }
                                                    }
                                                    commands_processed += 1;
                                                    continue;
                                                }
                                                "psubscribe" => {
                                                    // ACL channel check
                                                    if let Some(ref security_mgr) = security {
                                                        let denied = cmd.args.iter().find(|ch| !security_mgr.check_channel_permission(&client_addr_str, ch));
                                                        if let Some(channel) = denied {
                                                            let error_msg = format!("NOPERM this user has no permissions to access the '{}' channel", String::from_utf8_lossy(channel));
                                                            Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                                            commands_processed += 1;
                                                            continue;
                                                        }
                                                    }
                                                    let responses = pubsub_commands::psubscribe(&pubsub, client_id, &cmd.args).await;
                                                    match responses {
                                                        Ok(resp_list) => {
                                                            for resp in resp_list {
                                                                if let Ok(bytes) = resp.serialize() {
                                                                    response_buffer.extend_from_slice(&bytes);
                                                                }
                                                            }
                                                            in_subscription_mode = true;
                                                        }
                                                        Err(e) => {
                                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                                        }
                                                    }
                                                    commands_processed += 1;
                                                    continue;
                                                }
                                                "publish" => {
                                                    // ACL channel check
                                                    if let Some(ref security_mgr) = security {
                                                        if !cmd.args.is_empty() && !security_mgr.check_channel_permission(&client_addr_str, &cmd.args[0]) {
                                                            let error_msg = format!("NOPERM this user has no permissions to access the '{}' channel", String::from_utf8_lossy(&cmd.args[0]));
                                                            Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                                            commands_processed += 1;
                                                            continue;
                                                        }
                                                    }
                                                    let response = pubsub_commands::publish(&pubsub, &cmd.args).await;
                                                    match response {
                                                        Ok(resp) => {
                                                            if let Ok(bytes) = resp.serialize() {
                                                                response_buffer.extend_from_slice(&bytes);
                                                            }
                                                        }
                                                        Err(e) => {
                                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                                        }
                                                    }
                                                    commands_processed += 1;
                                                    continue;
                                                }
                                                "ssubscribe" => {
                                                    // ACL channel check
                                                    if let Some(ref security_mgr) = security {
                                                        let denied = cmd.args.iter().find(|ch| !security_mgr.check_channel_permission(&client_addr_str, ch));
                                                        if let Some(channel) = denied {
                                                            let error_msg = format!("NOPERM this user has no permissions to access the '{}' channel", String::from_utf8_lossy(channel));
                                                            Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                                            commands_processed += 1;
                                                            continue;
                                                        }
                                                    }
                                                    let responses = pubsub_commands::ssubscribe(&pubsub, client_id, &cmd.args).await;
                                                    match responses {
                                                        Ok(resp_list) => {
                                                            for resp in resp_list {
                                                                if let Ok(bytes) = resp.serialize() {
                                                                    response_buffer.extend_from_slice(&bytes);
                                                                }
                                                            }
                                                            in_subscription_mode = true;
                                                        }
                                                        Err(e) => {
                                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                                        }
                                                    }
                                                    commands_processed += 1;
                                                    continue;
                                                }
                                                "spublish" => {
                                                    // ACL channel check
                                                    if let Some(ref security_mgr) = security {
                                                        if !cmd.args.is_empty() && !security_mgr.check_channel_permission(&client_addr_str, &cmd.args[0]) {
                                                            let error_msg = format!("NOPERM this user has no permissions to access the '{}' channel", String::from_utf8_lossy(&cmd.args[0]));
                                                            Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                                            commands_processed += 1;
                                                            continue;
                                                        }
                                                    }
                                                    let response = pubsub_commands::spublish(&pubsub, &cmd.args).await;
                                                    match response {
                                                        Ok(resp) => {
                                                            if let Ok(bytes) = resp.serialize() {
                                                                response_buffer.extend_from_slice(&bytes);
                                                            }
                                                        }
                                                        Err(e) => {
                                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                                        }
                                                    }
                                                    commands_processed += 1;
                                                    continue;
                                                }
                                                "pubsub" => {
                                                    let response = pubsub_commands::pubsub_command(&pubsub, &cmd.args).await;
                                                    match response {
                                                        Ok(resp) => {
                                                            if let Ok(bytes) = resp.serialize() {
                                                                response_buffer.extend_from_slice(&bytes);
                                                            }
                                                        }
                                                        Err(e) => {
                                                            Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                                        }
                                                    }
                                                    commands_processed += 1;
                                                    continue;
                                                }
                                                // Handle ACL commands
                                                "acl" => {
                                                    if let Some(ref security_mgr) = security {
                                                        let response = security_mgr.acl().handle_acl_command(&cmd.args, &client_addr_str);
                                                        match response {
                                                            Ok(resp) => {
                                                                if let Ok(bytes) = resp.serialize() {
                                                                    response_buffer.extend_from_slice(&bytes);
                                                                }
                                                            }
                                                            Err(e) => {
                                                                Self::send_error_to_writer(&mut socket_writer, e.to_string(), &client_addr_str).await?;
                                                            }
                                                        }
                                                    } else {
                                                        let response = RespValue::SimpleString("OK".to_string());
                                                        if let Ok(bytes) = response.serialize() {
                                                            response_buffer.extend_from_slice(&bytes);
                                                        }
                                                    }
                                                    commands_processed += 1;
                                                    continue;
                                                }
                                                // Handle sentinel commands (slow path)
                                                "sentinel" => {
                                                    if let Some(ref sentinel_mgr) = sentinel {
                                                        match sentinel_mgr.handle_sentinel_command(&cmd.args).await {
                                                            Ok(resp) => {
                                                                if let Ok(bytes) = resp.serialize() {
                                                                    response_buffer.extend_from_slice(&bytes);
                                                                }
                                                            }
                                                            Err(e) => {
                                                                Self::send_error_to_writer(&mut socket_writer, e, &client_addr_str).await?;
                                                            }
                                                        }
                                                    } else {
                                                        let response = RespValue::Error("ERR This instance has sentinel support disabled".to_string());
                                                        if let Ok(bytes) = response.serialize() {
                                                            response_buffer.extend_from_slice(&bytes);
                                                        }
                                                    }
                                                    commands_processed += 1;
                                                    continue;
                                                }
                                                // Handle replication commands (slow path)
                                                "role" | "replicaof" | "slaveof" | "replconf" | "psync" | "failover" | "wait" => {
                                                    // Build a RespCommand for replication handlers
                                                    let resp_cmd = RespCommand {
                                                        name: cmd.name.as_bytes().to_vec(),
                                                        args: cmd.args.clone(),
                                                    };
                                                    let response = Self::handle_replication_command_slow(
                                                        &cmd_lower,
                                                        &cmd,
                                                        &resp_cmd,
                                                        &replication,
                                                        &mut socket_writer,
                                                        &mut response_buffer,
                                                        &client_addr_str,
                                                    ).await;
                                                    match response {
                                                        Ok(true) => {
                                                            // PSYNC - connection taken over, exit
                                                            // Cleanup before exiting
                                                            pubsub.unregister_client(client_id).await;
                                                            if let Some(ref security_mgr) = security {
                                                                security_mgr.remove_client(&client_addr_str);
                                                            }
                                                            return Ok(());
                                                        }
                                                        Ok(false) => {
                                                            commands_processed += 1;
                                                            continue;
                                                        }
                                                        Err(e) => {
                                                            error!(client_addr = %client_addr_str, "Replication command error: {}", e);
                                                            commands_processed += 1;
                                                            continue;
                                                        }
                                                    }
                                                }
                                                _ => {}
                                            }

                                            // Check read-only mode (replica rejecting write commands)
                                            if let Some(ref repl) = replication {
                                                if repl.is_read_only() && ReplicationManager::is_write_command(&cmd_lower) {
                                                    Self::send_error_to_writer(&mut socket_writer, "READONLY You can't write against a read only replica.".to_string(), &client_addr_str).await?;
                                                    commands_processed += 1;
                                                    continue;
                                                }
                                            }

                                            // Capture queued commands before EXEC drains them (for replication)
                                            let queued_for_repl: Vec<Command> = if cmd_lower == "exec" {
                                                tx_state.queue.clone()
                                            } else {
                                                vec![]
                                            };

                                            // Handle transaction commands (MULTI/EXEC/DISCARD/WATCH/UNWATCH)
                                            let tx_result = Self::process_with_transaction(
                                                &cmd,
                                                &cmd_lower,
                                                &mut tx_state,
                                                &command_handler,
                                                &persistence,
                                                aof_sync_policy,
                                                is_auth_command,
                                                &security,
                                                &client_addr_str,
                                            ).await;

                                            match tx_result {
                                                Ok(response) => {
                                                    // Propagate write commands to replicas
                                                    if let Some(ref repl) = replication {
                                                        if cmd_lower == "exec" {
                                                            // For EXEC, propagate queued write commands individually
                                                            if let RespValue::Array(Some(_)) = &response {
                                                                for queued_cmd in &queued_for_repl {
                                                                    let qcmd_lower = queued_cmd.name.to_lowercase();
                                                                    if ReplicationManager::is_write_command(&qcmd_lower) {
                                                                        let repl_cmd = RespCommand {
                                                                            name: queued_cmd.name.as_bytes().to_vec(),
                                                                            args: queued_cmd.args.clone(),
                                                                        };
                                                                        repl.propagate_command(&repl_cmd).await;
                                                                    }
                                                                }
                                                            }
                                                        } else if ReplicationManager::is_write_command(&cmd_lower) {
                                                            let resp_cmd = RespCommand {
                                                                name: cmd.name.as_bytes().to_vec(),
                                                                args: cmd.args.clone(),
                                                            };
                                                            repl.propagate_command(&resp_cmd).await;
                                                        }
                                                    }
                                                    if let Ok(bytes) = response.serialize() {
                                                        response_buffer.extend_from_slice(&bytes);
                                                    } else {
                                                        Self::send_response_to_writer(
                                                            &mut socket_writer,
                                                            response,
                                                            &client_addr_str,
                                                        )
                                                        .await?;
                                                    }
                                                }
                                                Err(err) => {
                                                    Self::send_error_to_writer(
                                                        &mut socket_writer,
                                                        err.to_string(),
                                                        &client_addr_str,
                                                    )
                                                    .await?;
                                                }
                                            }

                                            commands_processed += 1;
                                        }
                                        Err(e) => {
                                            error!(client_addr = %client_addr_str, error = ?e, "Command parsing error");
                                            Self::send_error_to_writer(
                                                &mut socket_writer,
                                                e.to_string(),
                                                &client_addr_str,
                                            )
                                            .await?;
                                        }
                                    }
                                }
                                Ok(None) => {
                                    debug!(client_addr = %client_addr_str, buffer_len = buffer.len(), "Incomplete RESP message, waiting for more data");
                                    if partial_command_buffer.len() + buffer.len() > MAX_PARTIAL_BUFFER_SIZE {
                                        error!(client_addr = %client_addr_str, "Partial command buffer exceeded size limit, disconnecting client");
                                        Self::send_error_to_writer(&mut socket_writer, "ERR Protocol error: command too large".to_string(), &client_addr_str).await?;
                                        return Ok(());
                                    }
                                    partial_command_buffer.extend_from_slice(&buffer);
                                    buffer.clear();
                                    debug!(client_addr = %client_addr_str, partial_buffer_len = partial_command_buffer.len(), "Saved partial command data");
                                    break;
                                }
                                Err(e) => {
                                    error!(client_addr = %client_addr_str, error = ?e, "RESP protocol error");

                                    let error_msg = match &e {
                                        RespError::InvalidFormatDetails(details) => {
                                            format!(
                                                "ERR Protocol error: Invalid RESP data format - {}",
                                                details
                                            )
                                        }
                                        RespError::ValueTooLarge(details) => {
                                            format!("ERR Protocol error: {}", details)
                                        }
                                        _ => {
                                            format!("ERR Protocol error: {}", e)
                                        }
                                    };

                                    if buffer.len() > 1024 * 50 {
                                        debug!(client_addr = %client_addr_str, buffer_len = buffer.len(),
                                               "Large buffer in error state, checking for JSON");

                                        if buffer.len() > 5 && (buffer[0] == b'{' || buffer[0] == b'[')
                                        {
                                            debug!(client_addr = %client_addr_str, "JSON-like data detected in error state");
                                        }
                                    }

                                    Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;

                                    buffer.clear();
                                    partial_command_buffer.clear();
                                    debug!(client_addr = %client_addr_str, "Cleared buffers after protocol error");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            error!(client_addr = %client_addr_str, error = ?e, "Fast path error");
                            if let RespError::Incomplete = e {
                                debug!(client_addr = %client_addr_str, buffer_len = buffer.len(), "Incomplete RESP message in fast path, waiting for more data");
                                if partial_command_buffer.len() + buffer.len() > MAX_PARTIAL_BUFFER_SIZE {
                                    error!(client_addr = %client_addr_str, "Partial command buffer exceeded size limit, disconnecting client");
                                    Self::send_error_to_writer(&mut socket_writer, "ERR Protocol error: command too large".to_string(), &client_addr_str).await?;
                                    return Ok(());
                                }
                                partial_command_buffer.extend_from_slice(&buffer);
                                buffer.clear();
                                break;
                            } else {
                                let error_msg = match &e {
                                    RespError::InvalidFormatDetails(details) => {
                                        format!(
                                            "ERR Protocol error: Invalid RESP data format - {}",
                                            details
                                        )
                                    }
                                    RespError::ValueTooLarge(details) => {
                                        format!("ERR Protocol error: {}", details)
                                    }
                                    _ => {
                                        format!("ERR Protocol error: {}", e)
                                    }
                                };

                                if buffer.len() > 1024 * 50 {
                                    debug!(client_addr = %client_addr_str, buffer_len = buffer.len(),
                                           "Large buffer in fast path error, possible JSON data");
                                }

                                Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                buffer.clear();
                                partial_command_buffer.clear();
                                debug!(client_addr = %client_addr_str, "Cleared buffers after fast path protocol error");
                                break;
                            }
                        }
                    }

                    // If we've accumulated enough commands or the response buffer is getting large,
                    // send the batch to avoid excessive memory usage
                    const MAX_BATCH_COMMANDS: usize = 100;
                    const MAX_BATCH_SIZE: usize = 64 * 1024; // 64KB

                    if commands_processed >= MAX_BATCH_COMMANDS
                        || response_buffer.len() >= MAX_BATCH_SIZE
                    {
                        break;
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
                }
            }
        }

        // Cleanup: unregister from PubSub and security
        pubsub.unregister_client(client_id).await;
        if let Some(ref security_mgr) = security {
            security_mgr.remove_client(&client_addr_str);
        }
        debug!(client_addr = %client_addr_str, client_id = client_id, "Client unregistered from PubSub manager");

        Ok(())
    }

    /// Process a command with transaction state handling.
    /// Handles MULTI/EXEC/DISCARD/WATCH/UNWATCH and queuing during transactions.
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
    ) -> Result<RespValue, crate::command::CommandError> {
        let engine = command_handler.storage();

        // Handle transaction commands
        match cmd_lower {
            "multi" => return transaction::handle_multi(tx_state),
            "discard" => return transaction::handle_discard(tx_state),
            "exec" => {
                // Capture queued commands before EXEC drains them (for AOF logging)
                let queued_commands: Vec<Command> = tx_state.queue.clone();
                let result = transaction::handle_exec(tx_state, command_handler, engine).await?;
                // Log transaction commands to AOF wrapped in MULTI/EXEC
                if let Some(persistence_mgr) = persistence {
                    if let RespValue::Array(Some(_)) = &result {
                        // Transaction succeeded (not aborted by WATCH)
                        // Log MULTI first
                        let multi_cmd = RespCommand {
                            name: b"MULTI".to_vec(),
                            args: vec![],
                        };
                        let _ = persistence_mgr.log_command(multi_cmd, aof_sync_policy).await;
                        // Log each write command
                        for queued_cmd in &queued_commands {
                            let qcmd_lower = queued_cmd.name.to_lowercase();
                            if ReplicationManager::is_write_command(&qcmd_lower) {
                                let resp_cmd = RespCommand {
                                    name: queued_cmd.name.as_bytes().to_vec(),
                                    args: queued_cmd.args.clone(),
                                };
                                let _ = persistence_mgr.log_command(resp_cmd, aof_sync_policy).await;
                            }
                        }
                        // Log EXEC to complete the transaction boundary
                        let exec_cmd = RespCommand {
                            name: b"EXEC".to_vec(),
                            args: vec![],
                        };
                        let _ = persistence_mgr.log_command(exec_cmd, aof_sync_policy).await;
                    }
                }
                return Ok(result);
            },
            "watch" => return transaction::handle_watch(tx_state, engine, &cmd.args),
            "unwatch" => return transaction::handle_unwatch(tx_state),
            _ => {}
        }

        // If in a MULTI block, queue the command instead of executing
        if tx_state.in_multi {
            return transaction::queue_command(tx_state, cmd.clone());
        }

        // Execute the command first, then log to AOF only on success
        let result = command_handler.process(cmd.clone()).await?;

        // Log to AOF if persistence is enabled, command succeeded, and it's a write command
        // Skip AUTH commands to avoid persisting passwords in the AOF file
        if let Some(persistence_mgr) = persistence {
            if !is_auth_command && ReplicationManager::is_write_command(&cmd_lower) {
                let resp_cmd = RespCommand {
                    name: cmd.name.as_bytes().to_vec(),
                    args: cmd.args.clone(),
                };
                if let Err(e) = persistence_mgr.log_command(resp_cmd, aof_sync_policy).await {
                    error!(client_addr = %client_addr_str, command = ?cmd.name, error = ?e, "Failed to log command to AOF");
                    // Command already executed; log the error but don't fail the response
                }
            }
        }

        // For AUTH command, update authentication status
        if is_auth_command {
            if let Some(security_mgr) = security {
                if let RespValue::SimpleString(ref s) = result {
                    if s == "OK" {
                        if cmd.args.len() == 1 {
                            // AUTH password (default user)
                            let password_str = String::from_utf8_lossy(&cmd.args[0]).to_string();
                            match security_mgr.authenticate_with_username(client_addr_str, "default", &password_str) {
                                Ok(_) => {},
                                Err(e) => {
                                    return Err(crate::command::CommandError::InvalidArgument(e));
                                }
                            }
                        } else if cmd.args.len() == 2 {
                            // AUTH username password
                            let username = String::from_utf8_lossy(&cmd.args[0]).to_string();
                            let password_str = String::from_utf8_lossy(&cmd.args[1]).to_string();
                            match security_mgr.authenticate_with_username(client_addr_str, &username, &password_str) {
                                Ok(_) => {},
                                Err(e) => {
                                    return Err(crate::command::CommandError::InvalidArgument(e));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Process commands while in subscription mode
    /// Only (P|S)SUBSCRIBE, (P|S)UNSUBSCRIBE, PING, and QUIT are allowed
    async fn process_subscription_commands(
        buffer: &mut BytesMut,
        partial_command_buffer: &mut Vec<u8>,
        socket_writer: &mut tokio::net::tcp::OwnedWriteHalf,
        pubsub: &Arc<PubSubManager>,
        client_id: ClientId,
        client_addr_str: &str,
        in_subscription_mode: &mut bool,
        security: &Option<Arc<SecurityManager>>,
    ) -> Result<(), NetworkError> {
        // Combine partial buffer if needed
        if !partial_command_buffer.is_empty() {
            let mut combined = BytesMut::with_capacity(partial_command_buffer.len() + buffer.len());
            combined.extend_from_slice(partial_command_buffer);
            combined.extend_from_slice(buffer);
            *buffer = combined;
            partial_command_buffer.clear();
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
                                    if let Some(security_mgr) = security {
                                        if let Some(denied) = cmd.args.iter().find(|ch| !security_mgr.check_channel_permission(client_addr_str, ch)) {
                                            let ch_str = String::from_utf8_lossy(denied);
                                            let err = RespValue::Error(format!("NOPERM No permissions to access channel '{}'", ch_str));
                                            if let Ok(bytes) = err.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                            continue;
                                        }
                                    }
                                    if let Ok(responses) = pubsub_commands::subscribe(pubsub, client_id, &cmd.args).await {
                                        for resp in responses {
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
                                    }
                                }
                                "unsubscribe" => {
                                    if let Ok(responses) = pubsub_commands::unsubscribe(pubsub, client_id, &cmd.args).await {
                                        for resp in responses {
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
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
                                    if let Some(security_mgr) = security {
                                        if let Some(denied) = cmd.args.iter().find(|ch| !security_mgr.check_channel_permission(client_addr_str, ch)) {
                                            let ch_str = String::from_utf8_lossy(denied);
                                            let err = RespValue::Error(format!("NOPERM No permissions to access channel '{}'", ch_str));
                                            if let Ok(bytes) = err.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                            continue;
                                        }
                                    }
                                    if let Ok(responses) = pubsub_commands::psubscribe(pubsub, client_id, &cmd.args).await {
                                        for resp in responses {
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
                                    }
                                }
                                "punsubscribe" => {
                                    if let Ok(responses) = pubsub_commands::punsubscribe(pubsub, client_id, &cmd.args).await {
                                        for resp in responses {
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
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
                                    if let Some(security_mgr) = security {
                                        if let Some(denied) = cmd.args.iter().find(|ch| !security_mgr.check_channel_permission(client_addr_str, ch)) {
                                            let ch_str = String::from_utf8_lossy(denied);
                                            let err = RespValue::Error(format!("NOPERM No permissions to access channel '{}'", ch_str));
                                            if let Ok(bytes) = err.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                            continue;
                                        }
                                    }
                                    if let Ok(responses) = pubsub_commands::ssubscribe(pubsub, client_id, &cmd.args).await {
                                        for resp in responses {
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
                                    }
                                }
                                "sunsubscribe" => {
                                    if let Ok(responses) = pubsub_commands::sunsubscribe(pubsub, client_id, &cmd.args).await {
                                        for resp in responses {
                                            if let Ok(bytes) = resp.serialize() {
                                                socket_writer.write_all(&bytes).await?;
                                            }
                                        }
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
                                    Self::send_error_to_writer(socket_writer, error_msg.to_string(), client_addr_str).await?;
                                }
                            }
                        }
                        Err(e) => {
                            Self::send_error_to_writer(socket_writer, e.to_string(), client_addr_str).await?;
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
                    partial_command_buffer.clear();
                    break;
                }
            }
        }

        Ok(())
    }

    /// Helper function to serialize and send a response
    async fn send_response(
        socket: &mut TcpStream,
        response: RespValue,
        client_addr: &str,
    ) -> Result<(), NetworkError> {
        match response.serialize() {
            Ok(response_bytes) => {
                // Log the response data when debug is enabled
                logging::log_protocol_data(client_addr, "response", &response_bytes, false);

                if let Err(e) = socket.write_all(&response_bytes).await {
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
                Self::send_error(socket, error_msg, client_addr)
                    .await
                    .map_err(|_| NetworkError::Serialization(e.to_string()))
            }
        }
    }

    /// Helper function to serialize and send an error response string
    async fn send_error(
        socket: &mut TcpStream,
        error_message: String,
        client_addr: &str,
    ) -> Result<(), NetworkError> {
        let error_resp = RespValue::Error(error_message);
        match error_resp.serialize() {
            Ok(bytes) => {
                // Log the error response data when debug is enabled
                logging::log_protocol_data(client_addr, "error_response", &bytes, false);

                if let Err(e) = socket.write_all(&bytes).await {
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

    /// Helper function to serialize and send a response to a split socket writer
    async fn send_response_to_writer(
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
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
    async fn send_error_to_writer(
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
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
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        args: &[Vec<u8>],
        replication: &Arc<ReplicationManager>,
        client_addr: &str,
    ) -> Result<(), NetworkError> {
        use tokio::io::AsyncWriteExt;

        if args.len() < 2 {
            let err = RespValue::Error("ERR wrong number of arguments for 'psync' command".to_string());
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
        let partial_possible = offset >= 0 && replication.can_partial_resync(&replid, offset as u64).await;

        if partial_possible {
            // Partial resync - send CONTINUE and the missing data
            let response = RespValue::SimpleString(format!("CONTINUE {}", master_replid));
            let bytes = response.serialize().map_err(|e| NetworkError::Serialization(e.to_string()))?;
            writer.write_all(&bytes).await?;

            // Send backlog data from the replica's offset
            let backlog = replication.backlog().read().await;
            if let Some(data) = backlog.get_from_offset(offset as u64) {
                writer.write_all(&data).await?;
            }
        } else {
            // Full resync - send FULLRESYNC, then RDB, then stream
            let response = RespValue::SimpleString(format!("FULLRESYNC {} {}", master_replid, master_offset));
            let bytes = response.serialize().map_err(|e| NetworkError::Serialization(e.to_string()))?;
            writer.write_all(&bytes).await?;

            // Generate and send RDB snapshot
            let engine = replication.engine();
            let snapshot = engine.snapshot().await.map_err(|e| {
                NetworkError::Internal(format!("Failed to get snapshot: {}", e))
            })?;

            // Serialize snapshot using serde_json (our internal format)
            let rdb_data = serde_json::to_vec(&snapshot).unwrap_or_default();

            // Send RDB as a bulk string: $<length>\r\n<data>
            let header = format!("${}\r\n", rdb_data.len());
            writer.write_all(header.as_bytes()).await?;
            writer.write_all(&rdb_data).await?;
        }

        // Register this replica for command propagation
        let replica_id = client_addr.to_string();
        let (host, port) = client_addr.split_once(':').map(|(h, p)| {
            (h.to_string(), p.parse::<u16>().unwrap_or(0))
        }).unwrap_or((client_addr.to_string(), 0));

        let mut rx = replication.register_replica(replica_id.clone(), host, port).await;

        info!(client_addr = %client_addr, "Replica entered replication stream mode");

        // Enter replication stream: forward propagated commands to this replica
        loop {
            match rx.recv().await {
                Some(data) => {
                    if let Err(e) = writer.write_all(&data).await {
                        error!(client_addr = %client_addr, "Error writing to replica: {}", e);
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
        socket_writer: &mut tokio::net::tcp::OwnedWriteHalf,
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
                    Self::send_error_to_writer(socket_writer, "ERR wrong number of arguments for 'replicaof' command".to_string(), client_addr_str).await?;
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
                            Self::send_error_to_writer(socket_writer, format!("ERR {}", e), client_addr_str).await?;
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
                if let Some(repl) = replication {
                    if cmd.args.len() >= 2 {
                        let subcmd = String::from_utf8_lossy(&cmd.args[0]).to_uppercase();
                        if subcmd == "ACK" {
                            if let Ok(offset) = String::from_utf8_lossy(&cmd.args[1]).parse::<u64>() {
                                repl.update_replica_offset(client_addr_str, offset).await;
                            }
                            return Ok(false);
                        }
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
                    Self::handle_psync_command(socket_writer, &cmd.args, repl, client_addr_str).await?;
                    return Ok(true); // Connection taken over
                } else {
                    Self::send_error_to_writer(socket_writer, "ERR PSYNC not supported".to_string(), client_addr_str).await?;
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
                            Self::send_error_to_writer(socket_writer, format!("ERR {}", e), client_addr_str).await?;
                        }
                    }
                } else {
                    Self::send_error_to_writer(socket_writer, "ERR FAILOVER not supported".to_string(), client_addr_str).await?;
                }
            }
            "wait" => {
                if cmd.args.len() != 2 {
                    Self::send_error_to_writer(socket_writer, "ERR wrong number of arguments for 'wait' command".to_string(), client_addr_str).await?;
                } else if let Some(repl) = replication {
                    let numreplicas = String::from_utf8_lossy(&cmd.args[0]).parse::<usize>().unwrap_or(0);
                    let timeout_ms = String::from_utf8_lossy(&cmd.args[1]).parse::<u64>().unwrap_or(0);
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
                        RespValue::Error(format!("NOPROTO unsupported protocol version: {}", proto_str)),
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
                    if let Some(security_mgr) = security {
                        if let Err(e) = security_mgr.authenticate_with_username(client_addr, &username, &password) {
                            return (RespValue::Error(e), current_version);
                        }
                    }
                    // When no security manager is configured (no auth required),
                    // silently ignore the AUTH option (matching Redis behavior)
                    i += 3;
                }
                "SETNAME" => {
                    // SETNAME clientname - we acknowledge but don't track client names yet
                    if i + 1 >= args.len() {
                        return (
                            RespValue::Error("ERR Syntax error in HELLO option 'setname'".to_string()),
                            current_version,
                        );
                    }
                    // Client name tracking is a future enhancement
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
