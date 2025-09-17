use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use tracing::{debug, error, info, warn};
use tokio::sync::{Mutex, RwLock};
use tokio::time;

use crate::persistence::config::PersistenceConfig;
use crate::persistence::error::PersistenceError;
use crate::storage::engine::StorageEngine;

// Constants for RDB file format
#[allow(dead_code)]
const RDB_VERSION: u8 = 1;
#[allow(dead_code)]
const RDB_MAGIC_STRING: &[u8] = b"RLDB";

// Type markers for data structures
#[allow(dead_code)]
const TYPE_STRING: u8 = 0;
// Comment out unused constants
// const TYPE_LIST: u8 = 1;
// const TYPE_SET: u8 = 2;
// const TYPE_ZSET: u8 = 3;
// const TYPE_HASH: u8 = 4;
#[allow(dead_code)]
const TYPE_EOF: u8 = 255;

/// RDB persistence implementation
pub struct RdbPersistence {
    engine: Arc<StorageEngine>,
    path: PathBuf,
    write_count: Arc<AtomicU64>,
    last_save: Arc<RwLock<Instant>>,
    in_progress: Arc<Mutex<bool>>,
}

impl RdbPersistence {
    /// Create a new RDB persistence instance
    pub fn new(engine: Arc<StorageEngine>, path: PathBuf) -> Self {
        Self {
            engine,
            path,
            write_count: Arc::new(AtomicU64::new(0)),
            last_save: Arc::new(RwLock::new(Instant::now())),
            in_progress: Arc::new(Mutex::new(false)),
        }
    }
    
    /// Load data from RDB file on startup
    pub async fn load(&self) -> Result<(), PersistenceError> {
        if !self.path.exists() {
            debug!(path = ?self.path, "No RDB file found, skipping load");
            return Ok(());
        }
        
        info!(path = ?self.path, "Loading RDB file");
        
        // Perform file reading in a blocking task to avoid blocking the event loop
        let path = self.path.clone();
        let engine = self.engine.clone();
        
        tokio::task::spawn_blocking(move || -> Result<(), PersistenceError> {
            let file = File::open(&path)
                .map_err(PersistenceError::Io)?;
            
            let mut reader = BufReader::new(file);
            
            // Read and verify magic string
            let mut magic = [0u8; 4];
            reader.read_exact(&mut magic)
                .map_err(PersistenceError::Io)?;
            
            if magic != RDB_MAGIC_STRING {
                return Err(PersistenceError::CorruptedFile(
                    "Invalid RDB file format (wrong magic string)".to_string()
                ));
            }
            
            // Read version
            let version = reader.read_u8()
                .map_err(PersistenceError::Io)?;
            
            if version != RDB_VERSION {
                return Err(PersistenceError::CorruptedFile(
                    format!("Unsupported RDB version: {}", version)
                ));
            }
            
            // Create a CRC32 hasher to verify the file integrity
            let mut hasher = Hasher::new();
            hasher.update(&magic);
            hasher.update(&[version]);
            
            // Read data
            let mut data = HashMap::new();
            
            loop {
                let type_marker = reader.read_u8()
                    .map_err(PersistenceError::Io)?;
                
                hasher.update(&[type_marker]);
                
                if type_marker == TYPE_EOF {
                    // End of file
                    break;
                }
                
                // Read key
                let key_len = reader.read_u32::<BigEndian>()
                    .map_err(PersistenceError::Io)?;
                hasher.update(&key_len.to_be_bytes());
                
                let mut key = vec![0u8; key_len as usize];
                reader.read_exact(&mut key)
                    .map_err(PersistenceError::Io)?;
                hasher.update(&key);
                
                // Read expiry (if any)
                let has_expiry = reader.read_u8()
                    .map_err(PersistenceError::Io)?;
                hasher.update(&[has_expiry]);
                
                let expiry = if has_expiry == 1 {
                    let secs = reader.read_u64::<BigEndian>()
                        .map_err(PersistenceError::Io)?;
                    hasher.update(&secs.to_be_bytes());
                    
                    // Convert to Instant
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    
                    if secs > now {
                        Some(Duration::from_secs(secs - now))
                    } else {
                        // Already expired
                        None
                    }
                } else {
                    None
                };
                
                match type_marker {
                    TYPE_STRING => {
                        // Read string value
                        let value_len = reader.read_u32::<BigEndian>()
                            .map_err(PersistenceError::Io)?;
                        hasher.update(&value_len.to_be_bytes());
                        
                        let mut value = vec![0u8; value_len as usize];
                        reader.read_exact(&mut value)
                            .map_err(PersistenceError::Io)?;
                        hasher.update(&value);
                        
                        if expiry.is_some() {
                            data.insert(key, (value, expiry));
                        } else {
                            data.insert(key, (value, None));
                        }
                    }
                    // Handle other types here...
                    _ => {
                        return Err(PersistenceError::CorruptedFile(
                            format!("Unknown data type: {}", type_marker)
                        ));
                    }
                }
            }
            
            // Read and verify checksum
            let expected_checksum = reader.read_u32::<BigEndian>()
                .map_err(PersistenceError::Io)?;
            
            let computed_checksum = hasher.finalize();
            
            if expected_checksum != computed_checksum {
                return Err(PersistenceError::CrcValidationFailed(
                    format!("CRC check failed: expected {}, got {}", 
                            expected_checksum, computed_checksum)
                ));
            }
            
            // Transfer loaded data to the storage engine
            tokio::runtime::Handle::current().block_on(async {
                for (key, (value, ttl)) in data {
                    // Use the storage engine's set method
                    if let Err(e) = engine.set(key.clone(), value, ttl).await {
                        warn!(key = ?String::from_utf8_lossy(&key), "Error restoring key from RDB: {:?}", e);
                    }
                }
            });
            
            info!(path = ?path, "Successfully loaded RDB file");
            Ok(())
        }).await.map_err(|e| PersistenceError::BackgroundTaskFailed(e.to_string()))?
    }
    
    /// Save a snapshot of the current dataset to an RDB file
    pub async fn save(&self) -> Result<(), PersistenceError> {
        // Avoid multiple concurrent snapshots
        let mut in_progress = self.in_progress.lock().await;
        if *in_progress {
            debug!("RDB save already in progress, skipping");
            return Ok(());
        }
        
        *in_progress = true;
        
        info!(path = ?self.path, "Starting RDB snapshot");
        
        // Create a temporary file path
        let temp_path = self.path.with_extension("tmp");
        
        // Get a consistent snapshot of the data
        let data = self.engine.snapshot().await
            .map_err(|e| PersistenceError::RdbSnapshotFailed(format!("Failed to get data snapshot: {:?}", e)))?;
        
        // Perform file writing in a blocking task
        let temp_path_clone = temp_path.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<(), PersistenceError> {
            // Create parent directory if it doesn't exist
            if let Some(parent) = temp_path_clone.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| PersistenceError::DirectoryCreationFailed(e.to_string()))?;
            }
            
            let file = File::create(&temp_path_clone)
                .map_err(PersistenceError::Io)?;
            
            let mut writer = BufWriter::new(file);
            
            // Initialize CRC32 hasher
            let mut hasher = Hasher::new();
            
            // Write magic string and version
            writer.write_all(RDB_MAGIC_STRING)
                .map_err(PersistenceError::Io)?;
            hasher.update(RDB_MAGIC_STRING);
            
            writer.write_u8(RDB_VERSION)
                .map_err(PersistenceError::Io)?;
            hasher.update(&[RDB_VERSION]);
            
            // Write data
            for (key, item) in data.iter() {
                // Determine the type marker based on the item type
                // For now, we're only handling string types
                let type_marker = TYPE_STRING;
                
                writer.write_u8(type_marker)
                    .map_err(PersistenceError::Io)?;
                hasher.update(&[type_marker]);
                
                // Write key
                writer.write_u32::<BigEndian>(key.len() as u32)
                    .map_err(PersistenceError::Io)?;
                hasher.update(&(key.len() as u32).to_be_bytes());
                
                writer.write_all(key)
                    .map_err(PersistenceError::Io)?;
                hasher.update(key);
                
                // Write expiry information
                if let Some(ttl) = item.ttl() {
                    writer.write_u8(1) // Has expiry
                        .map_err(PersistenceError::Io)?;
                    hasher.update(&[1]);
                    
                    // Calculate absolute expiry time as a Unix timestamp
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    
                    let expiry_time = now + ttl.as_secs();
                    
                    writer.write_u64::<BigEndian>(expiry_time)
                        .map_err(PersistenceError::Io)?;
                    hasher.update(&expiry_time.to_be_bytes());
                } else {
                    writer.write_u8(0) // No expiry
                        .map_err(PersistenceError::Io)?;
                    hasher.update(&[0]);
                }
                
                // Write value based on type
                // For string type
                writer.write_u32::<BigEndian>(item.value.len() as u32)
                    .map_err(PersistenceError::Io)?;
                hasher.update(&(item.value.len() as u32).to_be_bytes());
                
                writer.write_all(&item.value)
                    .map_err(PersistenceError::Io)?;
                hasher.update(&item.value);
            }
            
            // Write EOF marker
            writer.write_u8(TYPE_EOF)
                .map_err(PersistenceError::Io)?;
            hasher.update(&[TYPE_EOF]);
            
            // Write CRC32 checksum
            let checksum = hasher.finalize();
            writer.write_u32::<BigEndian>(checksum)
                .map_err(PersistenceError::Io)?;
            
            // Ensure all data is flushed to disk
            writer.flush()
                .map_err(PersistenceError::Io)?;
            
            Ok(())
        }).await.map_err(|e| PersistenceError::BackgroundTaskFailed(e.to_string()))?;
        
        // If successful, atomically rename the temporary file to the final location
        if result.is_ok() {
            if let Err(e) = fs::rename(&temp_path, &self.path) {
                // Clean up temp file on error
                let _ = fs::remove_file(&temp_path);
                return Err(PersistenceError::AtomicRenameFailed(e.to_string()));
            }
            
            // Update last save time
            *self.last_save.write().await = Instant::now();
            
            // Reset write counter
            self.write_count.store(0, Ordering::SeqCst);
            
            info!(path = ?self.path, "RDB snapshot completed successfully");
        } else {
            // Clean up temp file on error
            let _ = fs::remove_file(&temp_path);
            error!(path = ?self.path, "RDB snapshot failed");
        }
        
        // Mark snapshot as no longer in progress
        *in_progress = false;
        
        result
    }
    
    /// Schedule periodic RDB snapshots
    pub async fn schedule_snapshots(&self, config: &PersistenceConfig) -> Result<(), PersistenceError> {
        let engine = self.engine.clone();
        let rdb = Arc::new(self.clone());
        let interval = config.rdb_snapshot_interval;
        let threshold = config.rdb_snapshot_threshold;
        
        // Register the write counter with the storage engine
        engine.register_write_counter(self.write_count.clone()).await;
        
        // Spawn a background task for time-based snapshots
        tokio::spawn(async move {
            let mut timer = time::interval(interval);
            
            loop {
                timer.tick().await;
                
                // Check if enough time has passed since the last save
                let last_save = *rdb.last_save.read().await;
                if last_save.elapsed() >= interval {
                    debug!("Triggering time-based RDB snapshot");
                    if let Err(e) = rdb.save().await {
                        error!("Error during scheduled RDB snapshot: {:?}", e);
                    }
                }
                
                // Check if we've reached the write threshold
                let write_count = rdb.write_count.load(Ordering::SeqCst);
                if write_count >= threshold {
                    debug!("Triggering write-threshold RDB snapshot after {} writes", write_count);
                    if let Err(e) = rdb.save().await {
                        error!("Error during threshold-based RDB snapshot: {:?}", e);
                    }
                }
            }
        });
        
        Ok(())
    }
    
    /// Increment the write counter
    /// This is called by the StorageEngine when a write operation is performed
    /// Used to trigger RDB snapshots based on write threshold
    #[allow(dead_code)]
    pub fn increment_write_count(&self) {
        self.write_count.fetch_add(1, Ordering::SeqCst);
    }
    
    /// Get the current write count
    pub fn get_write_count(&self) -> u64 {
        self.write_count.load(Ordering::SeqCst)
    }
}

impl Clone for RdbPersistence {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            path: self.path.clone(),
            write_count: self.write_count.clone(),
            last_save: self.last_save.clone(),
            in_progress: self.in_progress.clone(),
        }
    }
}

// Comment out or remove unused function
/*
fn ensure_dir_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}
*/ 