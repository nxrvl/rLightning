use std::path::PathBuf;
use std::time::Duration;
use serde::Deserialize;

/// Persistence mode options
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceMode {
    /// No persistence
    None,
    /// RDB snapshot persistence
    RDB,
    /// Append-Only File persistence
    AOF,
    /// Hybrid persistence (RDB + AOF)
    Hybrid,
}

impl Default for PersistenceMode {
    fn default() -> Self {
        Self::None
    }
}

// Implement clap::ValueEnum for command-line parsing
impl clap::ValueEnum for PersistenceMode {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::None, Self::RDB, Self::AOF, Self::Hybrid]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(match self {
            Self::None => clap::builder::PossibleValue::new("none"),
            Self::RDB => clap::builder::PossibleValue::new("rdb"),
            Self::AOF => clap::builder::PossibleValue::new("aof"),
            Self::Hybrid => clap::builder::PossibleValue::new("hybrid"),
        })
    }
}

// Implement FromStr for easier conversions
impl std::str::FromStr for PersistenceMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "rdb" => Ok(Self::RDB),
            "aof" => Ok(Self::AOF),
            "hybrid" => Ok(Self::Hybrid),
            _ => Err(format!("Unknown persistence mode: {}", s)),
        }
    }
}

/// Policy for AOF fsync operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AofSyncPolicy {
    /// Sync on every write
    Always,
    /// Sync every second
    EverySecond,
    /// Let OS handle syncing
    None,
}

impl Default for AofSyncPolicy {
    fn default() -> Self {
        Self::EverySecond
    }
}

// Implement clap::ValueEnum for command-line parsing
impl clap::ValueEnum for AofSyncPolicy {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Always, Self::EverySecond, Self::None]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(match self {
            Self::Always => clap::builder::PossibleValue::new("always"),
            Self::EverySecond => clap::builder::PossibleValue::new("everysec"),
            Self::None => clap::builder::PossibleValue::new("none"),
        })
    }
}

// Implement FromStr for easier conversions
impl std::str::FromStr for AofSyncPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "always" => Ok(Self::Always),
            "everysec" => Ok(Self::EverySecond),
            "none" => Ok(Self::None),
            _ => Err(format!("Unknown AOF sync policy: {}", s)),
        }
    }
}

/// Persistence configuration
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Persistence mode
    pub mode: PersistenceMode,
    /// Path to the RDB snapshot file
    pub rdb_path: Option<PathBuf>,
    /// Path to the AOF log file
    pub aof_path: Option<PathBuf>,
    /// Sync policy for AOF
    #[allow(dead_code)]
    pub aof_sync_policy: AofSyncPolicy,
    /// Interval between RDB snapshots (in seconds)
    pub rdb_snapshot_interval: Duration,
    /// Number of writes after which to trigger a snapshot
    pub rdb_snapshot_threshold: u64,
    /// Size threshold for AOF rewrite (in bytes)
    pub aof_rewrite_min_size: usize,
    /// Percentage growth to trigger AOF rewrite
    pub aof_rewrite_percentage: u8,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            mode: PersistenceMode::None,
            rdb_path: Some(PathBuf::from("dump.rdb")),
            aof_path: Some(PathBuf::from("appendonly.aof")),
            aof_sync_policy: AofSyncPolicy::EverySecond,
            rdb_snapshot_interval: Duration::from_secs(300), // 5 minutes
            rdb_snapshot_threshold: 10000,
            aof_rewrite_min_size: 64 * 1024 * 1024, // 64MB
            aof_rewrite_percentage: 100, // 100% growth
        }
    }
}

/// Configuration for serializing to TOML
#[derive(Debug, Deserialize)]
pub struct PersistenceTomlConfig {
    /// Persistence mode (none, rdb, aof, hybrid)
    pub mode: String,
    /// Path to the RDB snapshot file
    pub rdb_path: Option<String>,
    /// Path to the AOF log file
    pub aof_path: Option<String>,
    /// Sync policy for AOF (always, everysec, none)
    pub aof_sync_policy: Option<String>,
    /// Interval between RDB snapshots (in seconds)
    pub rdb_snapshot_interval: Option<u64>,
    /// Number of writes after which to trigger a snapshot
    pub rdb_snapshot_threshold: Option<u64>,
    /// Size threshold for AOF rewrite (in MB)
    pub aof_rewrite_min_size: Option<usize>,
    /// Percentage growth to trigger AOF rewrite
    pub aof_rewrite_percentage: Option<u8>,
}

impl TryFrom<PersistenceTomlConfig> for PersistenceConfig {
    type Error = String;

    fn try_from(config: PersistenceTomlConfig) -> Result<Self, Self::Error> {
        let mode = match config.mode.to_lowercase().as_str() {
            "none" => PersistenceMode::None,
            "rdb" => PersistenceMode::RDB,
            "aof" => PersistenceMode::AOF,
            "hybrid" => PersistenceMode::Hybrid,
            _ => return Err(format!("Invalid persistence mode: {}", config.mode)),
        };

        let aof_sync_policy = match config.aof_sync_policy.as_deref().unwrap_or("everysec").to_lowercase().as_str() {
            "always" => AofSyncPolicy::Always,
            "everysec" => AofSyncPolicy::EverySecond,
            "none" => AofSyncPolicy::None,
            _ => return Err(format!("Invalid AOF sync policy: {:?}", config.aof_sync_policy)),
        };

        Ok(Self {
            mode,
            rdb_path: config.rdb_path.map(PathBuf::from),
            aof_path: config.aof_path.map(PathBuf::from),
            aof_sync_policy,
            rdb_snapshot_interval: Duration::from_secs(config.rdb_snapshot_interval.unwrap_or(300)),
            rdb_snapshot_threshold: config.rdb_snapshot_threshold.unwrap_or(10000),
            aof_rewrite_min_size: config.aof_rewrite_min_size.unwrap_or(64) * 1024 * 1024,
            aof_rewrite_percentage: config.aof_rewrite_percentage.unwrap_or(100),
        })
    }
} 