use std::time::Duration;
use serde::Deserialize;

/// Replication configuration
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Master host (if this is a replica)
    pub master_host: Option<String>,
    /// Master port (if this is a replica)
    pub master_port: Option<u16>,
    /// Master password (if authentication is required)
    #[allow(dead_code)]
    pub master_password: Option<String>,
    /// Whether to accept connections from replicas
    pub accept_replicas: bool,
    /// Replication timeout
    #[allow(dead_code)]
    pub replication_timeout: Duration,
    /// Replication backlog size
    #[allow(dead_code)]
    pub replication_backlog_size: usize,
    /// Minimum number of replicas to write
    #[allow(dead_code)]
    pub min_replicas_to_write: usize,
    /// Minimum number of replicas to write with max lag
    #[allow(dead_code)]
    pub min_replicas_max_lag: Duration,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            master_host: None,
            master_port: None,
            master_password: None,
            accept_replicas: true,
            replication_timeout: Duration::from_secs(60),
            replication_backlog_size: 1024 * 1024, // 1MB
            min_replicas_to_write: 0,
            min_replicas_max_lag: Duration::from_secs(10),
        }
    }
}

/// Configuration for serializing to TOML
#[derive(Debug, Deserialize)]
pub struct ReplicationTomlConfig {
    /// Master host (if this is a replica)
    pub master_host: Option<String>,
    /// Master port (if this is a replica)
    pub master_port: Option<u16>,
    /// Master password (if authentication is required)
    pub master_password: Option<String>,
    /// Whether to accept connections from replicas
    pub accept_replicas: Option<bool>,
    /// Replication timeout in seconds
    pub replication_timeout_secs: Option<u64>,
    /// Replication backlog size in bytes
    pub replication_backlog_size: Option<usize>,
    /// Minimum number of replicas to write
    pub min_replicas_to_write: Option<usize>,
    /// Minimum number of replicas to write with max lag in seconds
    pub min_replicas_max_lag_secs: Option<u64>,
}

impl TryFrom<ReplicationTomlConfig> for ReplicationConfig {
    type Error = String;

    fn try_from(config: ReplicationTomlConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            master_host: config.master_host,
            master_port: config.master_port,
            master_password: config.master_password,
            accept_replicas: config.accept_replicas.unwrap_or(true),
            replication_timeout: Duration::from_secs(config.replication_timeout_secs.unwrap_or(60)),
            replication_backlog_size: config.replication_backlog_size.unwrap_or(1024 * 1024),
            min_replicas_to_write: config.min_replicas_to_write.unwrap_or(0),
            min_replicas_max_lag: Duration::from_secs(config.min_replicas_max_lag_secs.unwrap_or(10)),
        })
    }
}