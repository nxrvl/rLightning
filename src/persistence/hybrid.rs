use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, error, info, warn};
use tokio::sync::RwLock;
use tokio::time::{self, Duration};

use crate::persistence::aof::AofPersistence;
use crate::persistence::config::{AofSyncPolicy, PersistenceConfig};
use crate::persistence::error::PersistenceError;
use crate::persistence::rdb::RdbPersistence;
use crate::storage::engine::StorageEngine;
use crate::networking::resp::RespCommand;

/// Hybrid persistence implementation combining RDB snapshots and AOF logging
pub struct HybridPersistence {
    engine: Arc<StorageEngine>,
    rdb: Arc<RdbPersistence>,
    aof: Arc<AofPersistence>,
    last_rdb_save: Arc<RwLock<Instant>>,
}

impl HybridPersistence {
    /// Create a new hybrid persistence instance
    pub fn new(engine: Arc<StorageEngine>, rdb_path: Option<PathBuf>, aof_path: Option<PathBuf>) -> Self {
        // Default paths if none provided
        let rdb_path = rdb_path.unwrap_or_else(|| PathBuf::from("dump.rdb"));
        let aof_path = aof_path.unwrap_or_else(|| PathBuf::from("appendonly.aof"));
        
        // Initialize both persistence systems
        let rdb = Arc::new(RdbPersistence::new(engine.clone(), rdb_path));
        let aof = Arc::new(AofPersistence::new(engine.clone(), aof_path));
        
        Self {
            engine,
            rdb,
            aof,
            last_rdb_save: Arc::new(RwLock::new(Instant::now())),
        }
    }
    
    /// Load data on startup
    pub async fn load(&self) -> Result<(), PersistenceError> {
        info!("Loading data with hybrid persistence (RDB + AOF)");
        
        // First, try to load from RDB snapshot
        if let Err(e) = self.rdb.load().await {
            warn!(error = ?e, "Failed to load RDB snapshot. Continuing with AOF only.");
        }
        
        // Then replay commands from AOF
        // This will apply only changes that occurred after the snapshot
        self.aof.load().await?;
        
        info!("Hybrid persistence load completed");
        Ok(())
    }
    
    /// Append a single command to the AOF
    #[allow(dead_code)]
    pub async fn append_command(&self, command: RespCommand, sync_policy: AofSyncPolicy) -> Result<(), PersistenceError> {
        self.aof.append_command(command, sync_policy).await
    }

    /// Append a batch of commands to the AOF atomically
    pub async fn append_commands_batch(&self, commands: Vec<RespCommand>, sync_policy: AofSyncPolicy) -> Result<(), PersistenceError> {
        self.aof.append_commands_batch(commands, sync_policy).await
    }
    
    /// Schedule periodic RDB snapshots and AOF rewrites
    pub async fn schedule_snapshots(&self, config: &PersistenceConfig) -> Result<(), PersistenceError> {
        // Store configuration settings
        let rdb_interval = config.rdb_snapshot_interval;
        let rdb_threshold = config.rdb_snapshot_threshold;
        let hybrid = Arc::new(self.clone());
        
        // Spawn a task to periodically create snapshots
        tokio::spawn(async move {
            let mut timer = time::interval(Duration::from_secs(60)); // Check every minute
            
            loop {
                timer.tick().await;
                
                // Check if it's time for an RDB snapshot
                let last_save = *hybrid.last_rdb_save.read().await;
                if last_save.elapsed() >= rdb_interval {
                    debug!("Triggering time-based RDB snapshot in hybrid mode");
                    
                    // Perform RDB snapshot
                    if let Err(e) = hybrid.rdb.save().await {
                        error!(error = ?e, "Error during hybrid RDB snapshot");
                    } else {
                        // If RDB save was successful, also rewrite the AOF
                        info!("RDB snapshot completed, now rewriting AOF");
                        if let Err(e) = hybrid.aof.rewrite().await {
                            error!(error = ?e, "Error during AOF rewrite after RDB snapshot");
                        }
                        
                        // Update last save time
                        *hybrid.last_rdb_save.write().await = Instant::now();
                    }
                }
                
                // Check write count
                let write_count = hybrid.rdb.get_write_count();
                if write_count >= rdb_threshold {
                    debug!(write_count, threshold = rdb_threshold, "Triggering write-threshold RDB snapshot in hybrid mode");
                    
                    // Perform RDB snapshot
                    if let Err(e) = hybrid.rdb.save().await {
                        error!(error = ?e, "Error during hybrid RDB snapshot (write threshold)");
                    } else {
                        // If RDB save was successful, also rewrite the AOF
                        info!("RDB snapshot completed, now rewriting AOF");
                        if let Err(e) = hybrid.aof.rewrite().await {
                            error!(error = ?e, "Error during AOF rewrite after RDB snapshot (write threshold)");
                        }
                        
                        // Update last save time
                        *hybrid.last_rdb_save.write().await = Instant::now();
                    }
                }
            }
        });
        
        Ok(())
    }
    
    /// Create an RDB snapshot and rewrite the AOF
    #[allow(dead_code)]
    pub async fn save(&self) -> Result<(), PersistenceError> {
        // First, create an RDB snapshot
        self.rdb.save().await?;
        
        // Then rewrite the AOF
        self.aof.rewrite().await?;
        
        // Update last save time
        *self.last_rdb_save.write().await = Instant::now();
        
        Ok(())
    }
}

impl Clone for HybridPersistence {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            rdb: self.rdb.clone(),
            aof: self.aof.clone(),
            last_rdb_save: self.last_rdb_save.clone(),
        }
    }
} 