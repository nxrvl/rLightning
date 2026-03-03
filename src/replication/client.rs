use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time;
use bytes::BytesMut;
use tracing::{debug, error, info, warn};

use crate::networking::resp::RespValue;
// Removed unused import: RespCommand
use crate::storage::engine::StorageEngine;
use crate::replication::{ReplicationState, MasterLinkStatus};
// Removed unused import: ReplicationRole
use crate::replication::error::ReplicationError;

/// Client for connecting to a master server
pub struct ReplicationClient {
    engine: Arc<StorageEngine>,
    host: String,
    port: u16,
    state: Arc<RwLock<ReplicationState>>,
    buffer_size: usize,
    reconnect_delay: Duration,
}

impl ReplicationClient {
    /// Create a new replication client
    pub fn new(
        engine: Arc<StorageEngine>,
        host: String,
        port: u16,
        state: Arc<RwLock<ReplicationState>>,
    ) -> Self {
        Self {
            engine,
            host,
            port,
            state,
            buffer_size: 65536, // 64KB buffer size
            reconnect_delay: Duration::from_secs(1),
        }
    }

    /// Start the replication client
    pub async fn start(&self) -> Result<(), ReplicationError> {
        loop {
            // Try to connect to the master
            match self.connect_and_sync().await {
                Ok(()) => {
                    // If we get here, the connection was closed normally
                    info!("Connection to master closed, reconnecting in {:?}", self.reconnect_delay);
                }
                Err(e) => {
                    // Connection error, update state and retry
                    error!("Error connecting to master: {}", e);
                    
                    // Update the master link status
                    let mut state = self.state.write().await;
                    state.master_link_status = MasterLinkStatus::Down;
                }
            }

            // Wait before reconnecting
            time::sleep(self.reconnect_delay).await;
        }
    }

    /// Connect to the master and perform initial synchronization
    async fn connect_and_sync(&self) -> Result<(), ReplicationError> {
        // Connect to the master
        let addr = format!("{}:{}", self.host, self.port);
        info!("Connecting to master at {}", addr);
        
        let mut socket = match TcpStream::connect(&addr).await {
            Ok(socket) => socket,
            Err(e) => {
                return Err(ReplicationError::Connection(format!("Failed to connect to master: {}", e)));
            }
        };

        // Set TCP_NODELAY for better latency
        if let Err(e) = socket.set_nodelay(true) {
            warn!("Failed to set TCP_NODELAY: {}", e);
        }

        // Update the master link status
        {
            let mut state = self.state.write().await;
            state.master_link_status = MasterLinkStatus::Up;
        }

        info!("Connected to master at {}", addr);

        // Send PING to check connection
        self.send_command(&mut socket, "PING", &[]).await?;
        let response = self.read_response(&mut socket).await?;
        
        match response {
            RespValue::SimpleString(s) if s == "PONG" => {
                debug!("Received PONG from master");
            }
            _ => {
                return Err(ReplicationError::Protocol(format!("Unexpected response to PING: {:?}", response)));
            }
        }

        // Send REPLCONF to identify as a replica
        self.send_command(&mut socket, "REPLCONF", &["listening-port", &self.port.to_string()]).await?;
        let response = self.read_response(&mut socket).await?;
        
        match response {
            RespValue::SimpleString(s) if s == "OK" => {
                debug!("REPLCONF accepted by master");
            }
            _ => {
                return Err(ReplicationError::Protocol(format!("Unexpected response to REPLCONF: {:?}", response)));
            }
        }

        // Send PSYNC to start replication
        // For simplicity, we'll use PSYNC ? -1 to request a full resync
        self.send_command(&mut socket, "PSYNC", &["?", "-1"]).await?;
        let response = self.read_response(&mut socket).await?;
        
        match response {
            RespValue::SimpleString(s) if s.starts_with("FULLRESYNC") => {
                debug!("Master requested FULLRESYNC: {}", s);
                
                // Parse the master's replication ID and offset
                let parts: Vec<&str> = s.split_whitespace().collect();
                if parts.len() >= 2 {
                    let master_replid = parts[1].to_string();
                    let offset = parts[2].parse::<u64>().unwrap_or(0);
                    
                    info!("Starting full resync with master, replication ID: {}, offset: {}", master_replid, offset);
                    
                    // Update our replication state
                    let mut state = self.state.write().await;
                    state.replication_offset = offset;
                }
                
                // The next response should be the RDB file
                self.receive_rdb(&mut socket).await?;
            }
            RespValue::SimpleString(s) if s.starts_with("CONTINUE") => {
                debug!("Master requested CONTINUE: {}", s);
                
                // Parse the master's replication ID
                let parts: Vec<&str> = s.split_whitespace().collect();
                if parts.len() >= 2 {
                    let master_replid = parts[1].to_string();
                    
                    info!("Continuing replication with master, replication ID: {}", master_replid);
                }
                
                // No RDB file in this case, just start processing commands
            }
            _ => {
                return Err(ReplicationError::Protocol(format!("Unexpected response to PSYNC: {:?}", response)));
            }
        }

        // Start processing commands from the master
        self.process_commands(&mut socket).await?;

        Ok(())
    }

    /// Receive and process the RDB file from the master
    async fn receive_rdb(&self, socket: &mut TcpStream) -> Result<(), ReplicationError> {
        // The RDB file is sent as a bulk string
        let response = self.read_response(socket).await?;
        
        match response {
            RespValue::BulkString(Some(rdb_data)) => {
                info!("Received RDB file from master, size: {} bytes", rdb_data.len());
                
                // Process the RDB file
                // In a real implementation, you would parse the RDB file and load it into the storage engine
                // For simplicity, we'll just log the size and pretend we loaded it
                
                // Clear the current database
                self.engine.flush_all().await.map_err(|e| {
                    ReplicationError::Internal(format!("Failed to flush database: {}", e))
                })?;
                
                // TODO: Parse and load the RDB file
                // This would involve implementing an RDB parser
                
                info!("Loaded RDB file from master");
            }
            RespValue::BulkString(None) => {
                return Err(ReplicationError::Protocol("Received null RDB file from master".to_string()));
            }
            _ => {
                return Err(ReplicationError::Protocol(format!("Expected RDB file, got: {:?}", response)));
            }
        }
        
        Ok(())
    }

    /// Process commands from the master
    async fn process_commands(&self, socket: &mut TcpStream) -> Result<(), ReplicationError> {
        let mut buffer = BytesMut::with_capacity(self.buffer_size);
        let mut current_db: usize = 0;
        
        loop {
            // Read data from the master
            let read_result = socket.read_buf(&mut buffer).await;
            
            match read_result {
                Ok(0) => {
                    // Connection closed by master
                    info!("Connection closed by master");
                    return Ok(());
                }
                Ok(n) => {
                    debug!("Read {} bytes from master", n);
                }
                Err(e) => {
                    error!("Error reading from master: {}", e);
                    return Err(ReplicationError::Io(e));
                }
            }
            
            // Process complete RESP messages
            while !buffer.is_empty() {
                match RespValue::parse(&mut buffer) {
                    Ok(Some(value)) => {
                        debug!("Received command from master: {:?}", value);
                        
                        // Convert to a command
                        match crate::command::parser::parse_command(value) {
                            Ok(cmd) => {
                                debug!("Processing command from master: {:?}", cmd);
                                
                                // Track SELECT commands to maintain correct database context
                                if cmd.name.eq_ignore_ascii_case("SELECT")
                                    && let Some(db_arg) = cmd.args.first()
                                    && let Ok(db_str) = std::str::from_utf8(db_arg)
                                    && let Ok(db_idx) = db_str.parse::<usize>()
                                {
                                    current_db = db_idx;
                                    debug!("Replication: switched to database {}", current_db);
                                }
                                
                                // Execute the command with the tracked database index
                                let cmd_handler = crate::command::handler::CommandHandler::new(self.engine.clone());
                                if let Err(e) = cmd_handler.process(cmd, current_db).await {
                                    error!("Error processing command from master: {}", e);
                                }
                                
                                // Update replication offset
                                let mut state = self.state.write().await;
                                state.replication_offset += 1;
                            }
                            Err(e) => {
                                error!("Command parsing error: {:?}", e);
                            }
                        }
                    }
                    Ok(None) => {
                        // Incomplete message, wait for more data
                        debug!("Incomplete message, waiting for more data");
                        break;
                    }
                    Err(e) => {
                        error!("RESP protocol error: {:?}", e);
                        return Err(ReplicationError::Protocol(format!("Protocol error: {}", e)));
                    }
                }
            }
        }
    }

    /// Send a command to the master
    async fn send_command(&self, socket: &mut TcpStream, command: &str, args: &[&str]) -> Result<(), ReplicationError> {
        let mut cmd_parts = Vec::with_capacity(1 + args.len());
        cmd_parts.push(command.to_string());
        cmd_parts.extend(args.iter().map(|s| s.to_string()));

        let resp_array = RespValue::Array(Some(
            cmd_parts.into_iter()
                .map(|s| RespValue::BulkString(Some(s.into_bytes())))
                .collect()
        ));

        let serialized = resp_array.serialize().map_err(|e| {
            ReplicationError::Protocol(format!("Failed to serialize command: {}", e))
        })?;
        
        socket.write_all(&serialized).await.map_err(ReplicationError::Io)?;

        Ok(())
    }

    /// Read a response from the master
    async fn read_response(&self, socket: &mut TcpStream) -> Result<RespValue, ReplicationError> {
        let mut buffer = BytesMut::with_capacity(self.buffer_size);
        
        loop {
            // Read data from the master
            let read_result = socket.read_buf(&mut buffer).await;
            
            match read_result {
                Ok(0) => {
                    // Connection closed by master
                    return Err(ReplicationError::Connection("Connection closed by master".to_string()));
                }
                Ok(_) => {
                    // Try to parse a complete RESP message
                    match RespValue::parse(&mut buffer) {
                        Ok(Some(value)) => {
                            return Ok(value);
                        }
                        Ok(None) => {
                            // Incomplete message, continue reading
                            continue;
                        }
                        Err(e) => {
                            return Err(ReplicationError::Protocol(format!("Protocol error: {}", e)));
                        }
                    }
                }
                Err(e) => {
                    return Err(ReplicationError::Io(e));
                }
            }
        }
    }
}