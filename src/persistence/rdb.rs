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
use crate::storage::item::RedisDataType;

// Constants for RDB file format
const RDB_VERSION: u8 = 1;
const RDB_MAGIC_STRING: &[u8] = b"RLDB";

// Type markers for data structures
const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
const TYPE_ZSET: u8 = 3;
const TYPE_HASH: u8 = 4;
const TYPE_STREAM: u8 = 5;
const TYPE_EOF: u8 = 255;

fn data_type_to_marker(dt: &RedisDataType) -> u8 {
    match dt {
        RedisDataType::String => TYPE_STRING,
        RedisDataType::List => TYPE_LIST,
        RedisDataType::Set => TYPE_SET,
        RedisDataType::ZSet => TYPE_ZSET,
        RedisDataType::Hash => TYPE_HASH,
        RedisDataType::Stream => TYPE_STREAM,
    }
}

fn marker_to_data_type(marker: u8) -> Option<RedisDataType> {
    match marker {
        TYPE_STRING => Some(RedisDataType::String),
        TYPE_LIST => Some(RedisDataType::List),
        TYPE_SET => Some(RedisDataType::Set),
        TYPE_ZSET => Some(RedisDataType::ZSet),
        TYPE_HASH => Some(RedisDataType::Hash),
        TYPE_STREAM => Some(RedisDataType::Stream),
        _ => None,
    }
}

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
            
            // Read data: (key, value, data_type, ttl)
            let mut data: Vec<(Vec<u8>, Vec<u8>, RedisDataType, Option<Duration>)> = Vec::new();

            loop {
                let type_marker = reader.read_u8()
                    .map_err(PersistenceError::Io)?;

                hasher.update(&[type_marker]);

                if type_marker == TYPE_EOF {
                    break;
                }

                let data_type = marker_to_data_type(type_marker)
                    .ok_or_else(|| PersistenceError::CorruptedFile(
                        format!("Unknown data type marker: {}", type_marker)
                    ))?;

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

                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();

                    if secs > now {
                        Some(Duration::from_secs(secs - now))
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Read value (same binary format for all types)
                let value_len = reader.read_u32::<BigEndian>()
                    .map_err(PersistenceError::Io)?;
                hasher.update(&value_len.to_be_bytes());

                let mut value = vec![0u8; value_len as usize];
                reader.read_exact(&mut value)
                    .map_err(PersistenceError::Io)?;
                hasher.update(&value);

                data.push((key, value, data_type, expiry));
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

            // Transfer loaded data to the storage engine with correct data types
            tokio::runtime::Handle::current().block_on(async {
                for (key, value, data_type, ttl) in data {
                    let result = engine.set_with_type(key.clone(), value, data_type.clone(), ttl).await;
                    if let Err(e) = result {
                        warn!(key = ?String::from_utf8_lossy(&key), data_type = ?data_type, "Error restoring key from RDB: {:?}", e);
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
                let type_marker = data_type_to_marker(&item.data_type);
                
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
                
                // Write value (raw bytes for all types - collection types are already bincode-serialized)
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap as StdHashMap, HashSet};
    use crate::storage::engine::StorageEngine;
    use crate::storage::item::RedisDataType;
    use crate::command::types::sorted_set::SortedSetData;
    use tempfile::NamedTempFile;

    fn create_test_engine() -> Arc<StorageEngine> {
        StorageEngine::new(Default::default())
    }

    #[tokio::test]
    async fn test_rdb_save_load_string() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        // Set a string key
        engine.set(b"mykey".to_vec(), b"myvalue".to_vec(), None).await.unwrap();

        // Save
        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        // Load into a new engine
        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        let val = engine2.get(b"mykey").await.unwrap();
        assert_eq!(val, Some(b"myvalue".to_vec()));
    }

    #[tokio::test]
    async fn test_rdb_save_load_list() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        // Create a list
        let list: Vec<Vec<u8>> = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let serialized = bincode::serialize(&list).unwrap();
        engine.set_with_type(b"mylist".to_vec(), serialized, RedisDataType::List, None).await.unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        // Verify type and value
        let tp = engine2.get_type(b"mylist").await.unwrap();
        assert_eq!(tp, "list");
        let raw = engine2.get(b"mylist").await.unwrap().unwrap();
        let loaded: Vec<Vec<u8>> = bincode::deserialize(&raw).unwrap();
        assert_eq!(loaded, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[tokio::test]
    async fn test_rdb_save_load_set() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        let mut set = HashSet::new();
        set.insert(b"x".to_vec());
        set.insert(b"y".to_vec());
        let serialized = bincode::serialize(&set).unwrap();
        engine.set_with_type(b"myset".to_vec(), serialized, RedisDataType::Set, None).await.unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        let tp = engine2.get_type(b"myset").await.unwrap();
        assert_eq!(tp, "set");
        let raw = engine2.get(b"myset").await.unwrap().unwrap();
        let loaded: HashSet<Vec<u8>> = bincode::deserialize(&raw).unwrap();
        assert!(loaded.contains(&b"x".to_vec()));
        assert!(loaded.contains(&b"y".to_vec()));
    }

    #[tokio::test]
    async fn test_rdb_save_load_sorted_set() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        let mut zset = SortedSetData::new();
        zset.insert(1.0, b"alice".to_vec());
        zset.insert(2.5, b"bob".to_vec());
        let serialized = bincode::serialize(&zset).unwrap();
        engine.set_with_type(b"myzset".to_vec(), serialized, RedisDataType::ZSet, None).await.unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        let tp = engine2.get_type(b"myzset").await.unwrap();
        assert_eq!(tp, "zset");
        let raw = engine2.get(b"myzset").await.unwrap().unwrap();
        let loaded: SortedSetData = bincode::deserialize(&raw).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.scores.get(&b"alice".to_vec()), Some(&1.0));
        assert_eq!(loaded.scores.get(&b"bob".to_vec()), Some(&2.5));
    }

    #[tokio::test]
    async fn test_rdb_save_load_hash() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        let mut hash = StdHashMap::new();
        hash.insert(b"field1".to_vec(), b"val1".to_vec());
        hash.insert(b"field2".to_vec(), b"val2".to_vec());
        let serialized = bincode::serialize(&hash).unwrap();
        engine.set_with_type(b"myhash".to_vec(), serialized, RedisDataType::Hash, None).await.unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        let tp = engine2.get_type(b"myhash").await.unwrap();
        assert_eq!(tp, "hash");
        let raw = engine2.get(b"myhash").await.unwrap().unwrap();
        let loaded: StdHashMap<Vec<u8>, Vec<u8>> = bincode::deserialize(&raw).unwrap();
        assert_eq!(loaded.get(&b"field1".to_vec()), Some(&b"val1".to_vec()));
        assert_eq!(loaded.get(&b"field2".to_vec()), Some(&b"val2".to_vec()));
    }

    #[tokio::test]
    async fn test_rdb_save_load_stream() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        let mut stream = crate::storage::stream::StreamData::new();
        let entry = crate::storage::stream::StreamEntry {
            id: crate::storage::stream::StreamEntryId::new(1000, 0),
            fields: vec![
                (b"temp".to_vec(), b"25".to_vec()),
                (b"humidity".to_vec(), b"60".to_vec()),
            ],
        };
        stream.entries.insert(entry.id.clone(), entry);
        stream.last_id = crate::storage::stream::StreamEntryId::new(1000, 0);
        let serialized = bincode::serialize(&stream).unwrap();
        engine.set_with_type(b"mystream".to_vec(), serialized, RedisDataType::Stream, None).await.unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        let tp = engine2.get_type(b"mystream").await.unwrap();
        assert_eq!(tp, "stream");
        let raw = engine2.get(b"mystream").await.unwrap().unwrap();
        let loaded: crate::storage::stream::StreamData = bincode::deserialize(&raw).unwrap();
        assert_eq!(loaded.entries.len(), 1);
    }

    #[tokio::test]
    async fn test_rdb_save_load_mixed_types() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        // String
        engine.set(b"str".to_vec(), b"hello".to_vec(), None).await.unwrap();

        // List
        let list: Vec<Vec<u8>> = vec![b"1".to_vec(), b"2".to_vec()];
        engine.set_with_type(b"lst".to_vec(), bincode::serialize(&list).unwrap(), RedisDataType::List, None).await.unwrap();

        // Set
        let mut set = HashSet::new();
        set.insert(b"a".to_vec());
        engine.set_with_type(b"st".to_vec(), bincode::serialize(&set).unwrap(), RedisDataType::Set, None).await.unwrap();

        // Hash
        let mut hash = StdHashMap::new();
        hash.insert(b"f".to_vec(), b"v".to_vec());
        engine.set_with_type(b"hs".to_vec(), bincode::serialize(&hash).unwrap(), RedisDataType::Hash, None).await.unwrap();

        // Sorted set
        let mut zset = SortedSetData::new();
        zset.insert(3.0, b"m".to_vec());
        engine.set_with_type(b"zs".to_vec(), bincode::serialize(&zset).unwrap(), RedisDataType::ZSet, None).await.unwrap();

        // Save
        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        // Load into fresh engine
        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        // Verify all types
        assert_eq!(engine2.get_type(b"str").await.unwrap(), "string");
        assert_eq!(engine2.get(b"str").await.unwrap(), Some(b"hello".to_vec()));

        assert_eq!(engine2.get_type(b"lst").await.unwrap(), "list");
        assert_eq!(engine2.get_type(b"st").await.unwrap(), "set");
        assert_eq!(engine2.get_type(b"hs").await.unwrap(), "hash");
        assert_eq!(engine2.get_type(b"zs").await.unwrap(), "zset");
    }

    #[tokio::test]
    async fn test_rdb_save_load_with_ttl() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        // String with TTL
        engine.set(b"expiring".to_vec(), b"val".to_vec(), Some(Duration::from_secs(3600))).await.unwrap();

        // List with TTL
        let list: Vec<Vec<u8>> = vec![b"item".to_vec()];
        engine.set_with_type(b"explist".to_vec(), bincode::serialize(&list).unwrap(), RedisDataType::List, Some(Duration::from_secs(7200))).await.unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        // Both should still exist (TTL far in the future)
        assert!(engine2.get(b"expiring").await.unwrap().is_some());
        assert!(engine2.get(b"explist").await.unwrap().is_some());
        assert_eq!(engine2.get_type(b"explist").await.unwrap(), "list");
    }

    #[tokio::test]
    async fn test_rdb_checksum_validation() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        engine.set(b"key".to_vec(), b"val".to_vec(), None).await.unwrap();
        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        // Corrupt the file by modifying a byte
        let mut contents = fs::read(&path).unwrap();
        if contents.len() > 10 {
            contents[10] ^= 0xFF;
        }
        fs::write(&path, contents).unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2, path);
        let result = rdb2.load().await;
        assert!(result.is_err());
    }
}