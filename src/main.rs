use std::env;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use figment::{
    Error as FigmentError, Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::Deserialize;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

// Use jemalloc as the global allocator for better memory management
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod command;
mod networking;
mod persistence;
mod pubsub;
mod replication;
mod scripting;
mod security;
mod storage;
mod utils;

use crate::networking::server::Server;
use crate::persistence::PersistenceManager;
use crate::persistence::config::{AofSyncPolicy, PersistenceConfig, PersistenceMode};
use crate::replication::ReplicationManager;
use crate::replication::config::ReplicationConfig;
use crate::security::{SecurityConfig, SecurityManager};
use crate::storage::engine::{EvictionPolicy, StorageConfig, StorageEngine};

// --- Command Line Arguments ---

#[derive(Parser, Debug)]
#[clap(
    name = "rLightning",
    version,
    about = "A Redis-compatible in-memory key-value store"
)]
struct Args {
    /// Path to configuration file
    #[clap(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Host to bind to (overrides config file)
    #[clap(long)]
    host: Option<String>,

    /// Port to listen on (overrides config file)
    #[clap(short, long)]
    port: Option<u16>,

    /// Maximum memory usage in MB (overrides config file)
    #[clap(long)]
    max_memory_mb: Option<usize>,

    /// Eviction policy to use (overrides config file)
    #[clap(long, value_enum)]
    eviction_policy: Option<EvictionPolicy>,

    /// Persistence mode (overrides config file)
    #[clap(long, value_enum)]
    persistence_mode: Option<PersistenceMode>,

    /// Path to RDB file (overrides config file)
    #[clap(long)]
    rdb_path: Option<PathBuf>,

    /// Path to AOF file (overrides config file)
    #[clap(long)]
    aof_path: Option<PathBuf>,

    /// AOF sync policy (overrides config file)
    #[clap(long, value_enum)]
    aof_sync_policy: Option<AofSyncPolicy>,

    /// Master host (if this is a replica)
    #[clap(long)]
    master_host: Option<String>,

    /// Master port (if this is a replica)
    #[clap(long)]
    master_port: Option<u16>,

    /// Master password (if authentication is required)
    #[clap(long)]
    master_password: Option<String>,

    /// Whether to accept connections from replicas
    #[clap(long)]
    accept_replicas: Option<bool>,
}

// --- Unified Application Settings (for Figment) ---

#[derive(Deserialize, Debug, Default, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct AppSettings {
    server: ServerSettings,
    storage: StorageSettings,
    security: SecuritySettings,
    persistence: PersistenceSettings,
    replication: ReplicationSettings,
    logging: LoggingSettings,
    // Removed keys section for simplicity, storage section has relevant limits
}

#[derive(Deserialize, Debug, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct ServerSettings {
    host: String,
    port: u16,
}

// Default values for ServerSettings
impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings {
            host: "0.0.0.0".to_string(),
            port: 6379,
        }
    }
}

#[derive(Deserialize, Debug, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct StorageSettings {
    max_memory_mb: usize,
    eviction_policy: EvictionPolicy,
    default_ttl_seconds: u64,
    max_key_size: usize,
    max_value_size: usize,
}

// Default values for StorageSettings
impl Default for StorageSettings {
    fn default() -> Self {
        // Mirror defaults from StorageConfig
        StorageSettings {
            max_memory_mb: 128,
            eviction_policy: EvictionPolicy::default(),
            default_ttl_seconds: 0,
            max_key_size: 1024,
            max_value_size: 5 * 1024 * 1024,
        }
    }
}

#[derive(Deserialize, Debug, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct PersistenceSettings {
    mode: PersistenceMode,
    rdb_path: Option<PathBuf>,
    aof_path: Option<PathBuf>,
    aof_sync_policy: AofSyncPolicy,
    rdb_snapshot_interval_secs: u64,
    rdb_snapshot_threshold: u64,
    aof_rewrite_min_size_mb: usize,
    aof_rewrite_percentage: u8,
}

// Default values for PersistenceSettings
impl Default for PersistenceSettings {
    fn default() -> Self {
        // Mirror defaults from PersistenceConfig
        PersistenceSettings {
            mode: PersistenceMode::default(),
            rdb_path: Some(PathBuf::from("dump.rdb")),
            aof_path: Some(PathBuf::from("appendonly.aof")),
            aof_sync_policy: AofSyncPolicy::default(),
            rdb_snapshot_interval_secs: 300,
            rdb_snapshot_threshold: 10000,
            aof_rewrite_min_size_mb: 64,
            aof_rewrite_percentage: 100,
        }
    }
}

#[derive(Deserialize, Debug, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct ReplicationSettings {
    master_host: Option<String>,
    master_port: Option<u16>,
    master_password: Option<String>,
    accept_replicas: bool,
    replication_timeout_secs: u64,
    replication_backlog_size: usize,
    min_replicas_to_write: usize,
    min_replicas_max_lag_secs: u64,
}

// Default values for ReplicationSettings
impl Default for ReplicationSettings {
    fn default() -> Self {
        ReplicationSettings {
            master_host: None,
            master_port: None,
            master_password: None,
            accept_replicas: true,
            replication_timeout_secs: 60,
            replication_backlog_size: 1024 * 1024, // 1MB
            min_replicas_to_write: 0,
            min_replicas_max_lag_secs: 10,
        }
    }
}

#[derive(Deserialize, Debug, serde::Serialize, Default)]
#[serde(rename_all = "kebab-case")]
struct SecuritySettings {
    #[serde(default)]
    require_auth: bool,
    #[serde(default)]
    password: String,
    #[serde(default)]
    acl_file: Option<PathBuf>,
}

#[derive(Deserialize, Debug, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct LoggingSettings {
    level: String,
    file: Option<String>,
}

// Default values for LoggingSettings
impl Default for LoggingSettings {
    fn default() -> Self {
        LoggingSettings {
            level: "debug".to_string(),
            file: None,
        }
    }
}

// --- Configuration Loading Function ---

fn load_settings(args: &Args) -> Result<AppSettings, FigmentError> {
    let mut figment = Figment::new().merge(Serialized::defaults(AppSettings::default()));

    // Load settings from config file if specified
    let mut require_auth_in_config = false;
    if let Some(config_path) = &args.config {
        // Check if require_auth is true in the config file
        if let Ok(content) = std::fs::read_to_string(config_path) {
            // Simple string check for require_auth = true
            if content.contains("require_auth = true") {
                require_auth_in_config = true;
                eprintln!("DEBUG: Found require_auth = true in config file");
            }
        }

        // Now merge the config file
        figment = figment.merge(Toml::file(config_path));
    }

    // Merge environment variables (prefixed with RLIGHTNING_)
    figment = figment.merge(Env::prefixed("RLIGHTNING_"));

    // Check if settings were loaded correctly from the config file
    if let Ok(settings) = figment.extract::<AppSettings>() {
        eprintln!(
            "DEBUG: Settings after config file: require_auth = {}",
            settings.security.require_auth
        );
    }

    // Merge command-line arguments (overriding file and env)
    // Create a temporary struct with only the fields present in Args
    #[derive(Deserialize, Debug, Default, serde::Serialize)]
    struct CliOverrides {
        #[serde(skip_serializing_if = "Option::is_none")]
        server: Option<ServerOverrides>,
        #[serde(skip_serializing_if = "Option::is_none")]
        storage: Option<StorageOverrides>,
        #[serde(skip_serializing_if = "Option::is_none")]
        security: Option<SecurityOverrides>,
        #[serde(skip_serializing_if = "Option::is_none")]
        persistence: Option<PersistenceOverrides>,
        #[serde(skip_serializing_if = "Option::is_none")]
        replication: Option<ReplicationOverrides>,
    }
    #[derive(Deserialize, Debug, Default, serde::Serialize)]
    struct ServerOverrides {
        #[serde(skip_serializing_if = "Option::is_none")]
        host: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
    }
    #[derive(Deserialize, Debug, Default, serde::Serialize)]
    struct StorageOverrides {
        #[serde(skip_serializing_if = "Option::is_none")]
        max_memory_mb: Option<usize>,
        #[serde(skip_serializing_if = "Option::is_none")]
        eviction_policy: Option<EvictionPolicy>,
    }
    #[derive(Deserialize, Debug, Default, serde::Serialize)]
    struct PersistenceOverrides {
        #[serde(skip_serializing_if = "Option::is_none")]
        mode: Option<PersistenceMode>,
        #[serde(skip_serializing_if = "Option::is_none")]
        rdb_path: Option<PathBuf>,
        #[serde(skip_serializing_if = "Option::is_none")]
        aof_path: Option<PathBuf>,
        #[serde(skip_serializing_if = "Option::is_none")]
        aof_sync_policy: Option<AofSyncPolicy>,
    }
    #[derive(Deserialize, Debug, Default, serde::Serialize)]
    struct ReplicationOverrides {
        #[serde(skip_serializing_if = "Option::is_none")]
        master_host: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        master_port: Option<u16>,
        #[serde(skip_serializing_if = "Option::is_none")]
        master_password: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        accept_replicas: Option<bool>,
    }
    #[derive(Deserialize, Debug, Default, serde::Serialize)]
    struct SecurityOverrides {
        #[serde(skip_serializing_if = "Option::is_none")]
        require_auth: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<String>,
    }

    let cli_overrides = CliOverrides {
        server: Some(ServerOverrides {
            host: args.host.clone(),
            port: args.port,
        }),
        storage: Some(StorageOverrides {
            max_memory_mb: args.max_memory_mb,
            eviction_policy: args.eviction_policy,
        }),
        security: Some(SecurityOverrides {
            // If require_auth is true in config, preserve it but don't set it yet
            require_auth: None,
            password: Some(args.master_password.clone().unwrap_or_default()),
        }),
        persistence: Some(PersistenceOverrides {
            mode: args.persistence_mode,
            rdb_path: args.rdb_path.clone(),
            aof_path: args.aof_path.clone(),
            aof_sync_policy: args.aof_sync_policy,
        }),
        replication: Some(ReplicationOverrides {
            master_host: args.master_host.clone(),
            master_port: args.master_port,
            master_password: args.master_password.clone(),
            accept_replicas: args.accept_replicas,
        }),
    };

    // Merge the overrides only if they contain values
    figment = figment.merge(Serialized::defaults(cli_overrides));

    // Extract the final configuration
    let mut settings = figment.extract::<AppSettings>()?;

    // Now override the require_auth field directly
    if require_auth_in_config {
        eprintln!("DEBUG: Setting require_auth = true directly in the final settings");
        settings.security.require_auth = true;
    }

    eprintln!(
        "DEBUG: Final settings: require_auth = {}",
        settings.security.require_auth
    );

    Ok(settings)
}

// --- Main Application Logic ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber (before parsing args to potentially use RUST_LOG)
    // First try RUST_LOG env var, then fall back to config file, then default to info
    let default_level = EnvFilter::new("info");
    let filter = EnvFilter::try_from_default_env().unwrap_or(default_level);

    fmt::Subscriber::builder().with_env_filter(filter).init();

    // Parse command line arguments
    let args = Args::parse();

    // Load configuration using Figment
    let settings = match load_settings(&args) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Configuration Error: {}\n", e);
            return Err(Box::<dyn std::error::Error>::from(e));
        }
    };

    // Update the tracing filter based on the loaded configuration if RUST_LOG isn't set
    if env::var("RUST_LOG").is_err() {
        // Create a new filter based on the config file
        let log_level = &settings.logging.level;
        // Validate that the log level is valid
        if EnvFilter::try_new(log_level).is_ok() {
            // Apply the new filter - this is a bit tricky since we can't replace the global subscriber
            // Instead, we'll log what's happening and set the env var for future log calls
            info!("Setting log level to '{}' from configuration", log_level);
            unsafe {
                std::env::set_var("RUST_LOG", log_level);
            }
        } else {
            warn!(
                "Invalid log level '{}' in configuration, using default",
                log_level
            );
        }
    }

    info!(config = ?settings, "Loaded configuration");

    // --- Map AppSettings to internal Config structs ---

    // Server Address
    let addr_str = format!("{}:{}", settings.server.host, settings.server.port);
    let addr = match addr_str.to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(addr) => addr,
            None => {
                let err = std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    format!("Could not resolve address: {}", addr_str),
                );
                return Err(Box::<dyn std::error::Error>::from(err));
            }
        },
        Err(e) => {
            return Err(Box::<dyn std::error::Error>::from(e));
        }
    };

    // Create storage configuration
    let storage_config = StorageConfig {
        max_memory: settings.storage.max_memory_mb * 1024 * 1024,
        eviction_policy: settings.storage.eviction_policy,
        default_ttl: Duration::from_secs(settings.storage.default_ttl_seconds),
        max_key_size: settings.storage.max_key_size,
        max_value_size: settings.storage.max_value_size,
    };

    // Create persistence configuration
    let persistence_config = PersistenceConfig {
        mode: settings.persistence.mode,
        rdb_path: settings.persistence.rdb_path.clone(),
        aof_path: settings.persistence.aof_path.clone(),
        aof_sync_policy: settings.persistence.aof_sync_policy,
        rdb_snapshot_interval: Duration::from_secs(settings.persistence.rdb_snapshot_interval_secs),
        rdb_snapshot_threshold: settings.persistence.rdb_snapshot_threshold,
        aof_rewrite_min_size: settings.persistence.aof_rewrite_min_size_mb * 1024 * 1024,
        aof_rewrite_percentage: settings.persistence.aof_rewrite_percentage,
    };

    // Initialize the storage engine
    let storage = StorageEngine::new(storage_config);

    // Initialize persistence manager with the configured settings
    let persistence = PersistenceManager::new(Arc::clone(&storage), persistence_config);

    // Create replication configuration
    let replication_config = ReplicationConfig {
        master_host: settings.replication.master_host.clone(),
        master_port: settings.replication.master_port,
        master_password: settings.replication.master_password.clone(),
        accept_replicas: settings.replication.accept_replicas,
        replication_timeout: Duration::from_secs(settings.replication.replication_timeout_secs),
        replication_backlog_size: settings.replication.replication_backlog_size,
        min_replicas_to_write: settings.replication.min_replicas_to_write,
        min_replicas_max_lag: Duration::from_secs(settings.replication.min_replicas_max_lag_secs),
    };

    // Initialize replication manager
    let replication = ReplicationManager::new(Arc::clone(&storage), replication_config);

    // Create security configuration
    let security_config = SecurityConfig {
        require_auth: settings.security.require_auth,
        password: settings.security.password.clone(),
        acl_file: settings.security.acl_file.clone(),
    };

    // Initialize security manager
    let security = Arc::new(SecurityManager::new(security_config));

    // Initialize replication if needed
    if let Err(e) = replication.init().await {
        error!("Failed to initialize replication: {}", e);
        return Err(Box::<dyn std::error::Error>::from(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Replication initialization error: {}", e),
        )));
    }

    // Initialize the server with replication support
    let server = Server::new(addr, Arc::clone(&storage))
        .with_persistence(Arc::new(persistence), settings.persistence.aof_sync_policy)
        .with_security(security)
        .with_replication(replication);

    // Start the server (this will block until shutdown)
    if let Err(e) = server.start().await {
        error!("Server error: {}", e);
        return Err(Box::<dyn std::error::Error>::from(e));
    }

    Ok(())
}
