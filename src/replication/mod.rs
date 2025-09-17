pub mod config;
pub mod error;
pub mod manager;
pub mod client;
pub mod server;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use crate::storage::engine::StorageEngine;
use crate::networking::resp::RespCommand;

/// Replication manager that handles all replication operations
pub struct ReplicationManager {
    engine: Arc<StorageEngine>,
    config: config::ReplicationConfig,
    state: Arc<tokio::sync::RwLock<ReplicationState>>,
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
    #[allow(dead_code)]
    pub host: String,
    /// Replica port
    pub port: u16,
    /// Replication offset
    pub offset: u64,
    /// Last ping time
    pub last_ping: std::time::Instant,
}

impl ReplicationManager {
    /// Create a new replication manager
    pub fn new(engine: Arc<StorageEngine>, config: config::ReplicationConfig) -> Arc<Self> {
        let state = Arc::new(tokio::sync::RwLock::new(ReplicationState {
            role: ReplicationRole::Master, // Default to master
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
        })
    }

    /// Initialize the replication manager
    pub async fn init(&self) -> Result<(), error::ReplicationError> {
        // If we're configured as a replica, connect to the master
        if let Some(master_host) = &self.config.master_host {
            if let Some(master_port) = self.config.master_port {
                self.connect_to_master(master_host.clone(), master_port).await?;
            }
        }

        // Start the replication server to accept connections from replicas
        if self.config.accept_replicas {
            self.start_replication_server().await?;
        }

        Ok(())
    }

    /// Connect to a master server
    pub async fn connect_to_master(&self, host: String, port: u16) -> Result<(), error::ReplicationError> {
        // Update our state to be a replica
        {
            let mut state = self.state.write().await;
            state.role = ReplicationRole::Replica;
            state.master_host = Some(host.clone());
            state.master_port = Some(port);
            state.master_link_status = MasterLinkStatus::Down;
        }

        // Create a replication client and connect to the master
        let client = client::ReplicationClient::new(
            self.engine.clone(),
            host,
            port,
            self.state.clone(),
        );

        // Start the replication client in a background task
        tokio::spawn(async move {
            if let Err(e) = client.start().await {
                tracing::error!("Replication client error: {}", e);
            }
        });

        Ok(())
    }

    /// Start the replication server to accept connections from replicas
    async fn start_replication_server(&self) -> Result<(), error::ReplicationError> {
        let server = server::ReplicationServer::new(
            self.engine.clone(),
            self.state.clone(),
            self.config.clone(),
        );

        // Start the replication server in a background task
        tokio::spawn(async move {
            if let Err(e) = server.start().await {
                tracing::error!("Replication server error: {}", e);
            }
        });

        Ok(())
    }

    /// Propagate a command to all connected replicas
    #[allow(dead_code)]
    pub async fn propagate_command(&self, command: RespCommand) -> Result<(), error::ReplicationError> {
        // Only propagate if we're a master
        let state = self.state.read().await;
        if state.role == ReplicationRole::Master && !state.connected_replicas.is_empty() {
            // Delegate to the replication server
            let server = server::ReplicationServer::new(
                self.engine.clone(),
                self.state.clone(),
                self.config.clone(),
            );
            server.propagate_command(command).await?;
        }

        Ok(())
    }

    /// Get the current replication state
    #[allow(dead_code)]
    pub async fn get_state(&self) -> ReplicationState {
        self.state.read().await.clone()
    }

    /// Disconnect from the master and become a master
    #[allow(dead_code)]
    pub async fn disconnect_from_master(&self) -> Result<(), error::ReplicationError> {
        let mut state = self.state.write().await;
        state.role = ReplicationRole::Master;
        state.master_host = None;
        state.master_port = None;
        state.master_link_status = MasterLinkStatus::Down;

        Ok(())
    }
}