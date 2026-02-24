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
use crate::command::types::pubsub::{
    self as pubsub_commands, subscription_message_to_resp, ClientId,
};
use crate::networking::error::NetworkError;
use crate::networking::resp::{ProtocolVersion, RespCommand, RespValue};
use crate::persistence::PersistenceManager;
use crate::persistence::config::AofSyncPolicy;
use crate::pubsub::PubSubManager;
use crate::security::SecurityManager;
use crate::storage::engine::StorageEngine;
use crate::utils::logging;

/// The Redis-compatible server
pub struct Server {
    addr: SocketAddr,
    command_handler: Arc<CommandHandler>,
    pubsub: Arc<PubSubManager>,
    persistence: Option<Arc<PersistenceManager>>,
    security: Option<Arc<SecurityManager>>,
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
            aof_sync_policy: AofSyncPolicy::EverySecond,
            connection_limit: Arc::new(Semaphore::new(10000)), // Default to 10K connections max
            buffer_size: 64 * 1024 * 1024,                     // 64MB buffer size for large JSON values
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

        // CRITICAL FIX: Use stateful parser to prevent data/command confusion
        use crate::networking::resp_parser_state::StatefulRespParser;
        let mut state_parser = StatefulRespParser::new();

        // Split the socket for concurrent read/write in subscription mode
        let (mut socket_reader, mut socket_writer) = socket.into_split();

        // Track if we're in subscription mode
        let mut in_subscription_mode = false;

        // Track the per-connection protocol version (RESP2 by default)
        let mut _protocol_version = ProtocolVersion::RESP2;

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
                                let (response, new_version) = Self::handle_hello_command(&cmd.args, _protocol_version);
                                _protocol_version = new_version;
                                if let Ok(bytes) = response.serialize() {
                                    response_buffer.extend_from_slice(&bytes);
                                }
                                commands_processed += 1;
                                continue;
                            }

                            // Check for pub/sub commands
                            match cmd_lower.as_str() {
                                "subscribe" => {
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
                                _ => {}
                            }

                            // Check if this is an AUTH command
                            let is_auth_command = cmd_lower == "auth";

                            // If authentication is required and the client is not authenticated
                            // and this is not an AUTH command, return an error
                            if let Some(ref security_mgr) = security {
                                if !is_auth_command
                                    && security_mgr.require_auth()
                                    && !security_mgr.is_authenticated(&client_addr_str)
                                {
                                    let error_msg = "NOAUTH Authentication required.";
                                    Self::send_error_to_writer(
                                        &mut socket_writer,
                                        error_msg.to_string(),
                                        &client_addr_str,
                                    )
                                    .await?;
                                    continue;
                                }
                            }

                            // If persistence is enabled, log the command to AOF
                            if let Some(ref persistence_mgr) = persistence {
                                if let Err(e) = persistence_mgr
                                    .log_command(resp_cmd.clone(), aof_sync_policy)
                                    .await
                                {
                                    error!(client_addr = %client_addr_str, command = ?resp_cmd.name, error = ?e, "Failed to log command to AOF");
                                    let error_msg = format!("ERR Failed to persist command: {}", e);
                                    Self::send_error_to_writer(&mut socket_writer, error_msg, &client_addr_str).await?;
                                    continue;
                                }
                            }

                            // Execute the command with error handling
                            match command_handler.process(cmd.clone()).await {
                                Ok(response) => {
                                    // For AUTH command, update authentication status
                                    if is_auth_command {
                                        if let Some(ref security_mgr) = security {
                                            if let RespValue::SimpleString(ref s) = response {
                                                if s == "OK" && cmd.args.len() == 1 {
                                                    security_mgr
                                                        .authenticate(&client_addr_str, &cmd.args[0]);
                                                }
                                            }
                                        }
                                    }

                                    // Add the response to the batch
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
                                                let (response, new_version) = Self::handle_hello_command(&cmd.args, _protocol_version);
                                                _protocol_version = new_version;
                                                if let Ok(bytes) = response.serialize() {
                                                    response_buffer.extend_from_slice(&bytes);
                                                }
                                                commands_processed += 1;
                                                continue;
                                            }

                                            // Check for pub/sub commands
                                            match cmd_lower.as_str() {
                                                "subscribe" => {
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
                                                _ => {}
                                            }

                                            // Check if this is an AUTH command
                                            let is_auth_command = cmd_lower == "auth";

                                            // If authentication is required and the client is not authenticated
                                            // and this is not an AUTH command, return an error
                                            if let Some(ref security_mgr) = security {
                                                if !is_auth_command
                                                    && security_mgr.require_auth()
                                                    && !security_mgr.is_authenticated(&client_addr_str)
                                                {
                                                    let error_msg = "NOAUTH Authentication required.";
                                                    Self::send_error_to_writer(
                                                        &mut socket_writer,
                                                        error_msg.to_string(),
                                                        &client_addr_str,
                                                    )
                                                    .await?;
                                                    continue;
                                                }
                                            }

                                            // Prepare a RESP command for persistence
                                            if let Some(ref persistence_mgr) = persistence {
                                                let resp_cmd = RespCommand {
                                                    name: cmd.name.as_bytes().to_vec(),
                                                    args: cmd.args.clone(),
                                                };

                                                // Log command to AOF
                                                if let Err(e) = persistence_mgr
                                                    .log_command(resp_cmd, aof_sync_policy)
                                                    .await
                                                {
                                                    error!(client_addr = %client_addr_str, command = ?cmd.name, error = ?e, "Failed to log command to AOF");
                                                    let error_msg =
                                                        format!("ERR Failed to persist command: {}", e);
                                                    Self::send_error_to_writer(
                                                        &mut socket_writer,
                                                        error_msg,
                                                        &client_addr_str,
                                                    )
                                                    .await?;
                                                    continue;
                                                }
                                            }

                                            // Execute the command with error handling
                                            match command_handler.process(cmd.clone()).await {
                                                Ok(response) => {
                                                    // For AUTH command, update authentication status
                                                    if is_auth_command {
                                                        if let Some(ref security_mgr) = security {
                                                            if let RespValue::SimpleString(ref s) =
                                                                response
                                                            {
                                                                if s == "OK" && cmd.args.len() == 1 {
                                                                    security_mgr.authenticate(
                                                                        &client_addr_str,
                                                                        &cmd.args[0],
                                                                    );
                                                                }
                                                            }
                                                        }
                                                    }

                                                    // Add the response to the batch
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

                // Reset parser state after processing commands
                if commands_processed > 0 {
                    state_parser.reset();
                    debug!(client_addr = %client_addr_str, "Reset parser state after {} commands", commands_processed);
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

    /// Process commands while in subscription mode
    /// Only SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, and QUIT are allowed
    async fn process_subscription_commands(
        buffer: &mut BytesMut,
        partial_command_buffer: &mut Vec<u8>,
        socket_writer: &mut tokio::net::tcp::OwnedWriteHalf,
        pubsub: &Arc<PubSubManager>,
        client_id: ClientId,
        client_addr_str: &str,
        in_subscription_mode: &mut bool,
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
                                    let error_msg = "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context";
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

    /// Handle the HELLO command for RESP3 protocol negotiation.
    /// Returns the HELLO response and optionally the new protocol version.
    fn handle_hello_command(args: &[Vec<u8>], current_version: ProtocolVersion) -> (RespValue, ProtocolVersion) {
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
