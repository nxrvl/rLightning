pub mod aof;
pub mod config;
pub mod error;
pub mod hybrid;
pub mod rdb;
#[cfg(test)]
mod tests;

use crate::storage::engine::StorageEngine;
use std::sync::Arc;

/// Cached writer backend to avoid creating new instances per log call.
/// Each variant holds a single long-lived instance whose writer task
/// serializes all AOF writes through one channel.
enum AofWriter {
    Aof(Arc<aof::AofPersistence>),
    Hybrid(Arc<hybrid::HybridPersistence>),
}

/// Persistence manager that handles all persistence operations
pub struct PersistenceManager {
    engine: Arc<StorageEngine>,
    config: config::PersistenceConfig,
    /// Cached AOF/hybrid writer, created once on first use.
    /// Using tokio::sync::OnceCell for thread-safe lazy initialization.
    aof_writer: tokio::sync::OnceCell<AofWriter>,
}

impl PersistenceManager {
    /// Create a new persistence manager
    pub fn new(engine: Arc<StorageEngine>, config: config::PersistenceConfig) -> Self {
        Self {
            engine,
            config,
            aof_writer: tokio::sync::OnceCell::new(),
        }
    }

    /// Get or create the cached AOF writer instance
    async fn get_aof_writer(&self) -> Option<&AofWriter> {
        match self.config.mode {
            config::PersistenceMode::AOF => {
                if self.config.aof_path.is_some() {
                    Some(
                        self.aof_writer
                            .get_or_init(|| async {
                                let path = self.config.aof_path.clone().unwrap();
                                AofWriter::Aof(Arc::new(aof::AofPersistence::new(
                                    self.engine.clone(),
                                    path,
                                )))
                            })
                            .await,
                    )
                } else {
                    None
                }
            }
            config::PersistenceMode::Hybrid => {
                if self.config.aof_path.is_some() {
                    Some(
                        self.aof_writer
                            .get_or_init(|| async {
                                AofWriter::Hybrid(Arc::new(hybrid::HybridPersistence::new(
                                    self.engine.clone(),
                                    self.config.rdb_path.clone(),
                                    self.config.aof_path.clone(),
                                )))
                            })
                            .await,
                    )
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Initialize the persistence manager.
    /// For AOF and Hybrid modes, the writer is stored in the OnceCell so that
    /// `get_aof_writer()` returns the same instance used for background rewrite
    /// checks.  Previously, `init()` created a throwaway instance whose
    /// file_size/writer channel diverged from the lazily-created writer, making
    /// rewrite triggering inaccurate.
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
                // Initialize AOF persistence using the shared writer instance
                if self.config.aof_path.is_some() {
                    let writer = self
                        .aof_writer
                        .get_or_init(|| async {
                            let path = self.config.aof_path.clone().unwrap();
                            AofWriter::Aof(Arc::new(aof::AofPersistence::new(
                                self.engine.clone(),
                                path,
                            )))
                        })
                        .await;
                    if let AofWriter::Aof(aof_instance) = writer {
                        aof_instance.load().await?;
                        aof_instance
                            .start_background_rewrite_check(&self.config)
                            .await?;
                    }
                }
            }
            config::PersistenceMode::Hybrid => {
                // Initialize hybrid persistence using the shared writer instance
                if self.config.aof_path.is_some() {
                    let writer = self
                        .aof_writer
                        .get_or_init(|| async {
                            AofWriter::Hybrid(Arc::new(hybrid::HybridPersistence::new(
                                self.engine.clone(),
                                self.config.rdb_path.clone(),
                                self.config.aof_path.clone(),
                            )))
                        })
                        .await;
                    if let AofWriter::Hybrid(hybrid_instance) = writer {
                        hybrid_instance.load().await?;
                        hybrid_instance.schedule_snapshots(&self.config).await?;
                    }
                }
            }
            config::PersistenceMode::None => {
                // No persistence, do nothing
            }
        }

        Ok(())
    }

    /// Log a command to the AOF, defaulting to DB 0. Convenience wrapper around log_command_for_db.
    #[allow(dead_code)]
    pub async fn log_command(
        &self,
        command: crate::networking::resp::RespCommand,
        sync_policy: config::AofSyncPolicy,
    ) -> Result<(), error::PersistenceError> {
        self.log_command_for_db(command, sync_policy, 0).await
    }

    /// Log a command to the AOF with explicit database index.
    /// A SELECT command is always prepended atomically with the write command to establish
    /// explicit DB context and prevent replay corruption across databases.
    pub async fn log_command_for_db(
        &self,
        command: crate::networking::resp::RespCommand,
        sync_policy: config::AofSyncPolicy,
        db_index: usize,
    ) -> Result<(), error::PersistenceError> {
        let writer = match self.get_aof_writer().await {
            Some(w) => w,
            None => return Ok(()),
        };

        // Build a command batch: always prepend SELECT to establish explicit DB context.
        // This is critical for replay correctness: without an explicit SELECT 0, a DB0
        // write that follows a non-zero DB write would replay into the wrong database.
        let select_cmd = crate::networking::resp::RespCommand {
            name: b"SELECT".to_vec(),
            args: vec![db_index.to_string().into_bytes()],
        };
        let commands = vec![select_cmd, command];

        match writer {
            AofWriter::Aof(aof) => aof.append_commands_batch(commands, sync_policy).await?,
            AofWriter::Hybrid(hybrid) => {
                hybrid.append_commands_batch(commands, sync_policy).await?
            }
        }

        Ok(())
    }

    /// Log a batch of commands to the AOF as a single atomic unit.
    /// Used for MULTI/EXEC transactions to prevent interleaving with concurrent clients.
    /// A SELECT command is always prepended to establish explicit DB context for replay.
    pub async fn log_commands_batch_for_db(
        &self,
        commands: Vec<crate::networking::resp::RespCommand>,
        sync_policy: config::AofSyncPolicy,
        db_index: usize,
    ) -> Result<(), error::PersistenceError> {
        let writer = match self.get_aof_writer().await {
            Some(w) => w,
            None => return Ok(()),
        };

        let mut batch = Vec::with_capacity(commands.len() + 1);
        batch.push(crate::networking::resp::RespCommand {
            name: b"SELECT".to_vec(),
            args: vec![db_index.to_string().into_bytes()],
        });
        batch.extend(commands);

        match writer {
            AofWriter::Aof(aof) => aof.append_commands_batch(batch, sync_policy).await?,
            AofWriter::Hybrid(hybrid) => hybrid.append_commands_batch(batch, sync_policy).await?,
        }

        Ok(())
    }
}
