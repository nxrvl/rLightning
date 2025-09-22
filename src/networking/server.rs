use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use crate::command::Command;
use crate::command::handler::CommandHandler;
use crate::command::parser;
use crate::networking::error::NetworkError;
use crate::networking::resp::{RespCommand, RespValue};
use crate::persistence::PersistenceManager;
use crate::persistence::config::AofSyncPolicy;
use crate::security::SecurityManager;
use crate::storage::engine::StorageEngine;
use crate::utils::logging;

/// The Redis-compatible server
pub struct Server {
    addr: SocketAddr,
    command_handler: Arc<CommandHandler>,
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

        Server {
            addr,
            command_handler,
            persistence: None,
            security: None,
            aof_sync_policy: AofSyncPolicy::EverySecond,
            connection_limit: Arc::new(Semaphore::new(10000)), // Default to 10K connections max
            buffer_size: 1024 * 1024,                          // 1MB buffer size
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
        mut socket: TcpStream,
        command_handler: Arc<CommandHandler>,
        persistence: Option<Arc<PersistenceManager>>,
        security: Option<Arc<SecurityManager>>,
        aof_sync_policy: AofSyncPolicy,
        buffer_size: usize,
    ) -> Result<(), NetworkError> {
        use crate::networking::resp::RespError;
        let client_addr = socket.peer_addr()?;
        let client_addr_str = client_addr.to_string();

        info!(client_addr = %client_addr_str, "Client connected");

        // Create a buffer for reading client data with increased capacity for large JSON values
        let mut buffer = BytesMut::with_capacity(buffer_size);
        let mut response_buffer = Vec::with_capacity(buffer_size);
        let mut partial_command_buffer: Vec<u8> = Vec::new(); // For reassembling partial large commands

        // CRITICAL FIX: Use stateful parser to prevent data/command confusion
        use crate::networking::resp_parser_state::StatefulRespParser;
        let mut state_parser = StatefulRespParser::new();

        loop {
            // Read data from the client
            let read_result = socket.read_buf(&mut buffer).await;

            match read_result {
                Ok(0) => {
                    // Connection closed by client
                    debug!(client_addr = %client_addr_str, "Connection closed by client");
                    // Remove client from authentication list if security is enabled
                    if let Some(ref security_mgr) = security {
                        security_mgr.remove_client(&client_addr_str);
                    }
                    return Ok(());
                }
                Ok(n) => {
                    debug!(client_addr = %client_addr_str, bytes_read = n, "Read data from client");

                    // Log the raw request data when debug is enabled
                    logging::log_protocol_data(
                        &client_addr_str,
                        "request",
                        &buffer[buffer.len() - n..],
                        true,
                    );
                }
                Err(e) => {
                    error!(client_addr = %client_addr_str, "Error reading from client: {}", e);
                    return Err(NetworkError::Io(e));
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

                        // Convert to a Command
                        let cmd = Command {
                            name: String::from_utf8_lossy(&resp_cmd.name).to_string(),
                            args: resp_cmd.args.clone(),
                        };

                        // Check if this is an AUTH command
                        let is_auth_command = cmd.name.to_lowercase() == "auth";

                        // If authentication is required and the client is not authenticated
                        // and this is not an AUTH command, return an error
                        if let Some(ref security_mgr) = security {
                            if !is_auth_command
                                && security_mgr.require_auth()
                                && !security_mgr.is_authenticated(&client_addr_str)
                            {
                                let error_msg = "NOAUTH Authentication required.";
                                Self::send_error(
                                    &mut socket,
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
                                Self::send_error(&mut socket, error_msg, &client_addr_str).await?;
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
                                    // Fallback if serialization fails
                                    Self::send_response(&mut socket, response, &client_addr_str)
                                        .await?;
                                }
                            }
                            Err(err) => {
                                // Send error response instead of dropping the connection
                                Self::send_error(&mut socket, err.to_string(), &client_addr_str)
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
                                        let cmd_name_str =
                                            String::from_utf8_lossy(cmd.name.as_bytes())
                                                .to_string();
                                        debug!(client_addr = %client_addr_str, command = ?cmd, "Processing command");

                                        // Check if this is an AUTH command
                                        let is_auth_command = cmd.name.to_lowercase() == "auth";

                                        // If authentication is required and the client is not authenticated
                                        // and this is not an AUTH command, return an error
                                        if let Some(ref security_mgr) = security {
                                            if !is_auth_command
                                                && security_mgr.require_auth()
                                                && !security_mgr.is_authenticated(&client_addr_str)
                                            {
                                                let error_msg = "NOAUTH Authentication required.";
                                                Self::send_error(
                                                    &mut socket,
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
                                                error!(client_addr = %client_addr_str, command = ?cmd_name_str, error = ?e, "Failed to log command to AOF");
                                                let error_msg =
                                                    format!("ERR Failed to persist command: {}", e);
                                                Self::send_error(
                                                    &mut socket,
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
                                                    // Fallback if serialization fails
                                                    Self::send_response(
                                                        &mut socket,
                                                        response,
                                                        &client_addr_str,
                                                    )
                                                    .await?;
                                                }
                                            }
                                            Err(err) => {
                                                // Send error response instead of dropping the connection
                                                Self::send_error(
                                                    &mut socket,
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
                                        Self::send_error(
                                            &mut socket,
                                            e.to_string(),
                                            &client_addr_str,
                                        )
                                        .await?;
                                    }
                                }
                            }
                            Ok(None) => {
                                debug!(client_addr = %client_addr_str, buffer_len = buffer.len(), "Incomplete RESP message, waiting for more data");
                                // Save the partial data for the next iteration
                                partial_command_buffer.extend_from_slice(&buffer);
                                buffer.clear();
                                debug!(client_addr = %client_addr_str, partial_buffer_len = partial_command_buffer.len(), "Saved partial command data");
                                break;
                            }
                            Err(e) => {
                                error!(client_addr = %client_addr_str, error = ?e, "RESP protocol error");

                                // Create a more detailed error message based on the type of error
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

                                // Log additional details for large payloads
                                if buffer.len() > 1024 * 50 {
                                    debug!(client_addr = %client_addr_str, buffer_len = buffer.len(),
                                           "Large buffer in error state, checking for JSON");

                                    // Check if this seems to be JSON data
                                    if buffer.len() > 5 && (buffer[0] == b'{' || buffer[0] == b'[')
                                    {
                                        debug!(client_addr = %client_addr_str, "JSON-like data detected in error state");
                                    }
                                }

                                Self::send_error(&mut socket, error_msg, &client_addr_str).await?;

                                // Don't terminate the connection, just continue processing
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        error!(client_addr = %client_addr_str, error = ?e, "Fast path error");
                        // If it's just incomplete data, save for next iteration instead of returning error
                        if let RespError::Incomplete = e {
                            debug!(client_addr = %client_addr_str, buffer_len = buffer.len(), "Incomplete RESP message in fast path, waiting for more data");
                            partial_command_buffer.extend_from_slice(&buffer);
                            buffer.clear();
                            break;
                        } else {
                            // Create a more detailed error message based on the type of error
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

                            // Check for potential JSON content in large buffers
                            if buffer.len() > 1024 * 50 {
                                debug!(client_addr = %client_addr_str, buffer_len = buffer.len(),
                                       "Large buffer in fast path error, possible JSON data");
                            }

                            Self::send_error(&mut socket, error_msg, &client_addr_str).await?;
                            // Clear buffer to start fresh after error
                            buffer.clear();
                            continue;
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

            // Buffer is already updated by the parsing functions, no need to advance manually

            // Reset parser state after processing commands
            if commands_processed > 0 {
                state_parser.reset();
                debug!(client_addr = %client_addr_str, "Reset parser state after {} commands", commands_processed);
            }

            // Send the batched responses if we have any
            if !response_buffer.is_empty() {
                // Log the batched response data when debug is enabled
                logging::log_protocol_data(
                    &client_addr_str,
                    "batched_response",
                    &response_buffer,
                    false,
                );

                if let Err(e) = socket.write_all(&response_buffer).await {
                    error!(client_addr = %client_addr_str, "Failed to write batched responses: {}", e);
                    return Err(NetworkError::Io(e));
                }
                debug!(client_addr = %client_addr_str, commands = commands_processed, bytes = response_buffer.len(), "Sent batched responses");

                // IMPORTANT: Ensure response data doesn't get mixed with input buffer
                // This was a major source of data corruption
                response_buffer.clear();
            }
        }
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
}
