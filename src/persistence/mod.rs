pub mod rdb;
pub mod aof;
pub mod hybrid;
pub mod config;
pub mod error;
#[cfg(test)]
mod tests;

use crate::storage::engine::StorageEngine;
use std::sync::Arc;

/// Persistence manager that handles all persistence operations
pub struct PersistenceManager {
    engine: Arc<StorageEngine>,
    config: config::PersistenceConfig,
}

impl PersistenceManager {
    /// Create a new persistence manager
    pub fn new(engine: Arc<StorageEngine>, config: config::PersistenceConfig) -> Self {
        Self { engine, config }
    }

    /// Initialize the persistence manager
    #[allow(dead_code)]
    pub async fn init(&self) -> Result<(), error::PersistenceError> {
        match self.config.mode {
            config::PersistenceMode::RDB => {
                // Initialize RDB persistence
                if let Some(ref path) = self.config.rdb_path {
                    let rdb = rdb::RdbPersistence::new(self.engine.clone(), path.clone());
                    rdb.load().await?;
                    rdb.schedule_snapshots(&self.config).await?;
                }
            }
            config::PersistenceMode::AOF => {
                // Initialize AOF persistence
                if let Some(ref path) = self.config.aof_path {
                    let aof = aof::AofPersistence::new(self.engine.clone(), path.clone());
                    aof.load().await?;
                    aof.start_background_rewrite_check(&self.config).await?;
                }
            }
            config::PersistenceMode::Hybrid => {
                // Initialize hybrid persistence (RDB + AOF)
                let hybrid = hybrid::HybridPersistence::new(
                    self.engine.clone(),
                    self.config.rdb_path.clone(),
                    self.config.aof_path.clone(),
                );
                hybrid.load().await?;
                hybrid.schedule_snapshots(&self.config).await?;
            }
            config::PersistenceMode::None => {
                // No persistence, do nothing
            }
        }
        
        Ok(())
    }
    
    /// Log a command to the AOF
    pub async fn log_command(&self, command: crate::networking::resp::RespCommand, sync_policy: config::AofSyncPolicy) -> Result<(), error::PersistenceError> {
        match self.config.mode {
            config::PersistenceMode::AOF => {
                if let Some(ref path) = self.config.aof_path {
                    let aof = aof::AofPersistence::new(self.engine.clone(), path.clone());
                    aof.append_command(command, sync_policy).await?;
                }
            }
            config::PersistenceMode::Hybrid => {
                if let Some(ref aof_path) = self.config.aof_path {
                    let hybrid = hybrid::HybridPersistence::new(
                        self.engine.clone(),
                        self.config.rdb_path.clone(),
                        Some(aof_path.clone()),
                    );
                    hybrid.append_command(command, sync_policy).await?;
                }
            }
            _ => {
                // RDB-only or no persistence, do nothing
            }
        }
        
        Ok(())
    }
} 