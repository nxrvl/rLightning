use std::collections::HashMap;
use std::net::SocketAddr;
// Removed unused import: IpAddr
use std::sync::Arc;
use std::time::Instant;
// Removed unused import: Duration
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
// Removed unused import: self
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
// Removed unused import: tokio::time
use bytes::BytesMut;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::networking::resp::{RespCommand, RespValue};
use crate::replication::{ReplicaInfo, ReplicationState};
use crate::storage::engine::StorageEngine;
// Removed unused imports: ReplicationRole, MasterLinkStatus
use crate::replication::config::ReplicationConfig;
use crate::replication::error::ReplicationError;

/// Server for handling replica connections
pub struct ReplicationServer {
    engine: Arc<StorageEngine>,
    state: Arc<RwLock<ReplicationState>>,
    config: ReplicationConfig,
    replica_channels: Arc<RwLock<HashMap<String, mpsc::Sender<RespCommand>>>>,
    buffer_size: usize,
}

impl ReplicationServer {
    /// Create a new replication server
    pub fn new(
        engine: Arc<StorageEngine>,
        state: Arc<RwLock<ReplicationState>>,
        config: ReplicationConfig,
    ) -> Self {
        Self {
            engine,
            state,
            config,
            replica_channels: Arc::new(RwLock::new(HashMap::new())),
            buffer_size: 65536, // 64KB buffer size
        }
    }

    /// Start the replication server
    pub async fn start(&self) -> Result<(), ReplicationError> {
        // Only start if we're configured to accept replicas
        if !self.config.accept_replicas {
            return Ok(());
        }

        // Determine the address to bind to
        // We'll use the same port as the main server but on a different socket
        let addr = SocketAddr::from(([0, 0, 0, 0], 0)); // Let the OS choose a port

        // Create a TCP listener
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        info!("Replication server listening on {}", local_addr);

        // Start accepting connections
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!("New replica connected from {}", addr);

                    // Handle the replica connection in a separate task
                    let engine = self.engine.clone();
                    let state = self.state.clone();
                    let replica_channels = self.replica_channels.clone();
                    let buffer_size = self.buffer_size;

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_replica(
                            socket,
                            addr,
                            engine,
                            state,
                            replica_channels,
                            buffer_size,
                        )
                        .await
                        {
                            error!("Error handling replica {}: {}", addr, e);
                        }
                        info!("Replica {} disconnected", addr);
                    });
                }
                Err(e) => {
                    error!("Error accepting replica connection: {}", e);
                }
            }
        }
    }

    /// Handle a replica connection
    async fn handle_replica(
        mut socket: TcpStream,
        addr: SocketAddr,
        engine: Arc<StorageEngine>,
        state: Arc<RwLock<ReplicationState>>,
        replica_channels: Arc<RwLock<HashMap<String, mpsc::Sender<RespCommand>>>>,
        buffer_size: usize,
    ) -> Result<(), ReplicationError> {
        // Initialize buffer for reading data
        let mut buffer = BytesMut::with_capacity(buffer_size);

        // Set TCP_NODELAY for better latency
        if let Err(e) = socket.set_nodelay(true) {
            warn!("Failed to set TCP_NODELAY: {}", e);
        }

        // Get the socket address for reconnecting later if needed
        let socket_addr = socket.peer_addr()?;

        // Generate a unique ID for this replica
        let replica_id = Uuid::new_v4().to_string();
        info!(
            "New replica connection from {}, assigned ID: {}",
            addr, replica_id
        );

        // Process the initial handshake
        let mut replica_info =
            Self::process_handshake(&mut socket, &mut buffer, &replica_id, addr).await?;

        // Create a channel for sending commands to this replica
        let (tx, mut rx) = mpsc::channel::<RespCommand>(100);

        // Add the channel to our registry
        {
            let mut channels = replica_channels.write().await;
            channels.insert(replica_id.clone(), tx);
        }

        // Add the replica to our state
        {
            let mut state = state.write().await;
            state.connected_replicas.push(replica_info.clone());
        }

        // Send initial sync data
        Self::send_initial_sync(&mut socket, &engine).await?;

        // Start a task to handle commands from the channel
        let replica_id_clone = replica_id.clone();
        let state_clone = state.clone();

        // Create a separate TCP connection for the writer task
        let mut writer_socket = TcpStream::connect(socket_addr).await?;

        // Start a task to handle commands from the channel
        let writer_task = tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                if let Err(e) = Self::send_command(&mut writer_socket, cmd).await {
                    error!(
                        "Error sending command to replica {}: {}",
                        replica_id_clone, e
                    );
                    break;
                }

                // Update the replica's offset
                let mut state = state_clone.write().await;
                for replica in &mut state.connected_replicas {
                    if replica.id == replica_id_clone {
                        replica.offset += 1;
                        replica.last_ping = Instant::now();
                        break;
                    }
                }
            }

            Result::<_, ReplicationError>::Ok(())
        });

        // Process commands from the replica
        loop {
            // Check if the writer task has completed
            if writer_task.is_finished() {
                break;
            }

            // Read data from the replica
            let read_result = socket.read_buf(&mut buffer).await;

            match read_result {
                Ok(0) => {
                    // Connection closed by replica
                    info!("Connection closed by replica {}", addr);
                    break;
                }
                Ok(n) => {
                    debug!("Read {} bytes from replica {}", n, addr);
                }
                Err(e) => {
                    error!("Error reading from replica {}: {}", addr, e);
                    return Err(ReplicationError::Io(e));
                }
            }

            // Process complete RESP messages
            while !buffer.is_empty() {
                match RespValue::parse(&mut buffer) {
                    Ok(Some(value)) => {
                        debug!("Received command from replica {}: {:?}", addr, value);

                        // Convert to a command
                        match crate::command::parser::parse_command(value) {
                            Ok(cmd) => {
                                debug!("Processing command from replica {}: {:?}", addr, cmd);

                                // Handle special replication commands
                                match cmd.name.to_uppercase().as_str() {
                                    "REPLCONF" => {
                                        // Handle REPLCONF command
                                        Self::handle_replconf(
                                            &mut socket,
                                            &cmd.args,
                                            &mut replica_info,
                                        )
                                        .await?;
                                    }
                                    "PSYNC" => {
                                        // Handle PSYNC command
                                        Self::handle_psync(&mut socket, &cmd.args, &replica_info)
                                            .await?;
                                    }
                                    _ => {
                                        // For other commands, just acknowledge
                                        let ok_response = RespValue::SimpleString("OK".to_string());
                                        let bytes = ok_response.serialize().map_err(|e| {
                                            ReplicationError::Protocol(format!(
                                                "Failed to serialize response: {}",
                                                e
                                            ))
                                        })?;
                                        socket
                                            .write_all(&bytes)
                                            .await
                                            .map_err(ReplicationError::Io)?;
                                    }
                                }

                                // Update the replica's last ping time
                                let mut state = state.write().await;
                                for replica in &mut state.connected_replicas {
                                    if replica.id == replica_id {
                                        replica.last_ping = Instant::now();
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Command parsing error from replica {}: {:?}", addr, e);
                            }
                        }
                    }
                    Ok(None) => {
                        // Incomplete message, wait for more data
                        debug!(
                            "Incomplete message from replica {}, waiting for more data",
                            addr
                        );
                        break;
                    }
                    Err(e) => {
                        error!("RESP protocol error from replica {}: {:?}", addr, e);
                        return Err(ReplicationError::Protocol(format!("Protocol error: {}", e)));
                    }
                }
            }
        }

        // Remove the replica from our state
        {
            let mut state = state.write().await;
            state.connected_replicas.retain(|r| r.id != replica_id);
        }

        // Remove the channel
        {
            let mut channels = replica_channels.write().await;
            channels.remove(&replica_id);
        }

        Ok(())
    }

    /// Process the initial handshake with a replica
    async fn process_handshake(
        socket: &mut TcpStream,
        buffer: &mut BytesMut,
        replica_id: &str,
        addr: SocketAddr,
    ) -> Result<ReplicaInfo, ReplicationError> {
        // Wait for the replica to send PING
        loop {
            // Read data from the replica
            let read_result = socket.read_buf(buffer).await;

            match read_result {
                Ok(0) => {
                    // Connection closed by replica
                    return Err(ReplicationError::Connection(
                        "Connection closed by replica during handshake".to_string(),
                    ));
                }
                Ok(_) => {
                    // Try to parse a complete RESP message
                    match RespValue::parse(buffer) {
                        Ok(Some(value)) => {
                            // Convert to a command
                            match crate::command::parser::parse_command(value) {
                                Ok(cmd) => {
                                    if cmd.name.to_uppercase() == "PING" {
                                        // Respond with PONG
                                        let pong_response =
                                            RespValue::SimpleString("PONG".to_string());
                                        let bytes = pong_response.serialize().map_err(|e| {
                                            ReplicationError::Protocol(format!(
                                                "Failed to serialize PONG: {}",
                                                e
                                            ))
                                        })?;
                                        socket.write_all(&bytes).await?;

                                        // Break out of the loop
                                        break;
                                    } else {
                                        // Unexpected command
                                        return Err(ReplicationError::Protocol(format!(
                                            "Expected PING, got: {}",
                                            cmd.name
                                        )));
                                    }
                                }
                                Err(e) => {
                                    return Err(ReplicationError::Protocol(format!(
                                        "Command parsing error: {}",
                                        e
                                    )));
                                }
                            }
                        }
                        Ok(None) => {
                            // Incomplete message, continue reading
                            continue;
                        }
                        Err(e) => {
                            return Err(ReplicationError::Protocol(format!(
                                "Protocol error: {}",
                                e
                            )));
                        }
                    }
                }
                Err(e) => {
                    return Err(ReplicationError::Io(e));
                }
            }
        }

        // Wait for the replica to send REPLCONF
        let mut replica_port = 0;

        loop {
            // Read data from the replica
            let read_result = socket.read_buf(buffer).await;

            match read_result {
                Ok(0) => {
                    // Connection closed by replica
                    return Err(ReplicationError::Connection(
                        "Connection closed by replica during handshake".to_string(),
                    ));
                }
                Ok(_) => {
                    // Try to parse a complete RESP message
                    match RespValue::parse(buffer) {
                        Ok(Some(value)) => {
                            // Convert to a command
                            match crate::command::parser::parse_command(value) {
                                Ok(cmd) => {
                                    if cmd.name.to_uppercase() == "REPLCONF" {
                                        // Process REPLCONF arguments
                                        for i in 0..cmd.args.len() - 1 {
                                            if let Ok(arg) = std::str::from_utf8(&cmd.args[i])
                                                && arg.to_lowercase() == "listening-port"
                                                && let Ok(port_str) =
                                                    std::str::from_utf8(&cmd.args[i + 1])
                                                && let Ok(port) = port_str.parse::<u16>()
                                            {
                                                replica_port = port;
                                            }
                                        }

                                        // Respond with OK
                                        let ok_response = RespValue::SimpleString("OK".to_string());
                                        let bytes = ok_response.serialize().map_err(|e| {
                                            ReplicationError::Protocol(format!(
                                                "Failed to serialize OK: {}",
                                                e
                                            ))
                                        })?;
                                        socket.write_all(&bytes).await?;

                                        // Break out of the loop
                                        break;
                                    } else {
                                        // Unexpected command
                                        return Err(ReplicationError::Protocol(format!(
                                            "Expected REPLCONF, got: {}",
                                            cmd.name
                                        )));
                                    }
                                }
                                Err(e) => {
                                    return Err(ReplicationError::Protocol(format!(
                                        "Command parsing error: {}",
                                        e
                                    )));
                                }
                            }
                        }
                        Ok(None) => {
                            // Incomplete message, continue reading
                            continue;
                        }
                        Err(e) => {
                            return Err(ReplicationError::Protocol(format!(
                                "Protocol error: {}",
                                e
                            )));
                        }
                    }
                }
                Err(e) => {
                    return Err(ReplicationError::Io(e));
                }
            }
        }

        // Create replica info
        let replica_info = ReplicaInfo {
            id: replica_id.to_string(),
            host: addr.ip().to_string(),
            port: replica_port,
            offset: 0,
            last_ping: Instant::now(),
        };

        Ok(replica_info)
    }

    /// Send initial synchronization data to a replica
    async fn send_initial_sync(
        socket: &mut (impl AsyncWrite + Unpin),
        engine: &Arc<StorageEngine>,
    ) -> Result<(), ReplicationError> {
        // Get a snapshot of the current database
        let snapshot = engine.snapshot().await.map_err(|e| {
            ReplicationError::Internal(format!("Failed to get database snapshot: {}", e))
        })?;

        // Serialize the snapshot to a binary format
        // In a real implementation, this would generate an RDB file
        let rdb_data = serde_json::to_vec(&snapshot).map_err(|e| {
            ReplicationError::Internal(format!("Failed to serialize database snapshot: {}", e))
        })?;

        // Send the RDB file as a bulk string
        let rdb_response = RespValue::BulkString(Some(rdb_data));
        let bytes = rdb_response.serialize().map_err(|e| {
            ReplicationError::Protocol(format!("Failed to serialize RDB response: {}", e))
        })?;

        socket
            .write_all(&bytes)
            .await
            .map_err(ReplicationError::Io)?;

        Ok(())
    }

    /// Handle REPLCONF command from a replica
    async fn handle_replconf(
        socket: &mut (impl AsyncWrite + Unpin),
        args: &[Vec<u8>],
        replica_info: &mut ReplicaInfo,
    ) -> Result<(), ReplicationError> {
        // Process REPLCONF arguments
        for i in 0..args.len() - 1 {
            if let Ok(arg) = std::str::from_utf8(&args[i]) {
                match arg.to_lowercase().as_str() {
                    "ack" => {
                        if let Ok(offset_str) = std::str::from_utf8(&args[i + 1])
                            && let Ok(offset) = offset_str.parse::<u64>()
                        {
                            replica_info.offset = offset;
                        }
                    }
                    "listening-port" => {
                        if let Ok(port_str) = std::str::from_utf8(&args[i + 1])
                            && let Ok(port) = port_str.parse::<u16>()
                        {
                            replica_info.port = port;
                        }
                    }
                    _ => {
                        // Ignore unknown arguments
                    }
                }
            }
        }

        // Respond with OK
        let ok_response = RespValue::SimpleString("OK".to_string());
        let bytes = ok_response
            .serialize()
            .map_err(|e| ReplicationError::Protocol(format!("Failed to serialize OK: {}", e)))?;
        socket.write_all(&bytes).await?;

        Ok(())
    }

    /// Handle PSYNC command from a replica
    async fn handle_psync(
        socket: &mut (impl AsyncWrite + Unpin),
        args: &[Vec<u8>],
        replica_info: &ReplicaInfo,
    ) -> Result<(), ReplicationError> {
        // Parse the arguments
        if args.len() < 2 {
            return Err(ReplicationError::Protocol(
                "PSYNC requires at least 2 arguments".to_string(),
            ));
        }

        let replica_id = String::from_utf8_lossy(&args[0]);
        let offset = String::from_utf8_lossy(&args[1]);

        debug!(
            "Received PSYNC from replica {}, offset: {}",
            replica_id, offset
        );

        // For simplicity, we'll always perform a full resync
        // In a real implementation, we'd check if incremental sync is possible

        // Generate a new replication ID
        let repl_id = format!("{}-{}", replica_info.id, uuid::Uuid::new_v4());

        // Send FULLRESYNC response
        let fullresync_response = RespValue::SimpleString(format!("FULLRESYNC {} 0", repl_id));
        let bytes = fullresync_response.serialize().map_err(|e| {
            ReplicationError::Protocol(format!("Failed to serialize FULLRESYNC response: {}", e))
        })?;

        socket
            .write_all(&bytes)
            .await
            .map_err(ReplicationError::Io)?;

        // Send an empty RDB file
        let rdb_response = RespValue::BulkString(Some(Vec::new()));
        let bytes = rdb_response.serialize().map_err(|e| {
            ReplicationError::Protocol(format!("Failed to serialize empty RDB response: {}", e))
        })?;

        socket
            .write_all(&bytes)
            .await
            .map_err(ReplicationError::Io)?;

        Ok(())
    }

    /// Send a command to a replica
    async fn send_command(
        socket: &mut (impl AsyncWrite + Unpin),
        command: RespCommand,
    ) -> Result<(), ReplicationError> {
        // Convert the command to a RESP array
        let mut cmd_parts = Vec::with_capacity(1 + command.args.len());
        cmd_parts.push(RespValue::BulkString(Some(command.name)));
        cmd_parts.extend(
            command
                .args
                .into_iter()
                .map(|arg| RespValue::BulkString(Some(arg))),
        );

        let cmd_array = RespValue::Array(Some(cmd_parts));

        // Serialize and send
        let bytes = cmd_array.serialize().map_err(|e| {
            ReplicationError::Protocol(format!("Failed to serialize command: {}", e))
        })?;

        socket
            .write_all(&bytes)
            .await
            .map_err(ReplicationError::Io)?;

        Ok(())
    }

    /// Propagate a command to all connected replicas
    pub async fn propagate_command(&self, command: RespCommand) -> Result<(), ReplicationError> {
        let channels = self.replica_channels.read().await;

        for (replica_id, tx) in channels.iter() {
            // Clone the command for each replica
            let cmd_clone = RespCommand {
                name: command.name.clone(),
                args: command.args.clone(),
            };

            // Send the command to the replica
            if let Err(e) = tx.send(cmd_clone).await {
                warn!("Failed to send command to replica {}: {}", replica_id, e);
            }
        }

        Ok(())
    }
}
