use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use tokio::sync::{Mutex, RwLock};
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::persistence::config::PersistenceConfig;
use crate::persistence::error::PersistenceError;
use crate::storage::engine::StorageEngine;
use crate::storage::item::RedisDataType;
use crate::storage::value::StoreValue;

/// Serialize a StoreValue to bytes for RDB persistence.
fn serialize_store_value(value: &StoreValue) -> Result<Vec<u8>, String> {
    match value {
        StoreValue::Str(v) => Ok(v.to_vec()),
        StoreValue::Hash(m) => {
            let std_map: std::collections::HashMap<Vec<u8>, Vec<u8>> =
                m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            bincode::serialize(&std_map).map_err(|e| format!("Hash serialization: {}", e))
        }
        StoreValue::Set(s) => {
            let std_set: std::collections::HashSet<Vec<u8>> = s.iter().cloned().collect();
            bincode::serialize(&std_set).map_err(|e| format!("Set serialization: {}", e))
        }
        StoreValue::ZSet(ss) => {
            bincode::serialize(ss).map_err(|e| format!("ZSet serialization: {}", e))
        }
        StoreValue::List(deque) => {
            let vec: Vec<Vec<u8>> = deque.iter().cloned().collect();
            bincode::serialize(&vec).map_err(|e| format!("List serialization: {}", e))
        }
        StoreValue::Stream(s) => {
            bincode::serialize(s).map_err(|e| format!("Stream serialization: {}", e))
        }
    }
}

/// Deserialize bytes to a StoreValue for RDB loading.
fn deserialize_store_value(data_type: &RedisDataType, bytes: &[u8]) -> Result<StoreValue, String> {
    match data_type {
        RedisDataType::String => Ok(StoreValue::Str(bytes.to_vec().into())),
        RedisDataType::Hash => {
            let std_map: std::collections::HashMap<Vec<u8>, Vec<u8>> =
                bincode::deserialize(bytes).map_err(|e| format!("Hash deserialization: {}", e))?;
            let native: crate::storage::value::NativeHashMap = std_map.into_iter().collect();
            Ok(StoreValue::Hash(native))
        }
        RedisDataType::Set => {
            let std_set: std::collections::HashSet<Vec<u8>> =
                bincode::deserialize(bytes).map_err(|e| format!("Set deserialization: {}", e))?;
            let native: crate::storage::value::NativeHashSet = std_set.into_iter().collect();
            Ok(StoreValue::Set(native))
        }
        RedisDataType::ZSet => {
            let ss: crate::storage::value::SortedSetData =
                bincode::deserialize(bytes).map_err(|e| format!("ZSet deserialization: {}", e))?;
            Ok(StoreValue::ZSet(ss))
        }
        RedisDataType::List => {
            let vec: Vec<Vec<u8>> =
                bincode::deserialize(bytes).map_err(|e| format!("List deserialization: {}", e))?;
            Ok(StoreValue::List(vec.into()))
        }
        RedisDataType::Stream => {
            let s: crate::storage::stream::StreamData =
                bincode::deserialize(bytes).map_err(|e| format!("Stream deserialization: {}", e))?;
            Ok(StoreValue::Stream(s))
        }
    }
}

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
const TYPE_DB_SELECT: u8 = 254;
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
            let file = File::open(&path).map_err(PersistenceError::Io)?;

            let mut reader = BufReader::new(file);

            // Read and verify magic string
            let mut magic = [0u8; 4];
            reader
                .read_exact(&mut magic)
                .map_err(PersistenceError::Io)?;

            if magic != RDB_MAGIC_STRING {
                return Err(PersistenceError::CorruptedFile(
                    "Invalid RDB file format (wrong magic string)".to_string(),
                ));
            }

            // Read version
            let version = reader.read_u8().map_err(PersistenceError::Io)?;

            if version != RDB_VERSION {
                return Err(PersistenceError::CorruptedFile(format!(
                    "Unsupported RDB version: {}",
                    version
                )));
            }

            // Create a CRC32 hasher to verify the file integrity
            let mut hasher = Hasher::new();
            hasher.update(&magic);
            hasher.update(&[version]);

            // Read data: (db_index, key, value, data_type, ttl)
            #[allow(clippy::type_complexity)]
            let mut data: Vec<(
                usize,
                Vec<u8>,
                Vec<u8>,
                RedisDataType,
                Option<Duration>,
            )> = Vec::new();
            let mut current_db: usize = 0;

            loop {
                let type_marker = reader.read_u8().map_err(PersistenceError::Io)?;

                hasher.update(&[type_marker]);

                if type_marker == TYPE_EOF {
                    break;
                }

                // Handle DB selector
                if type_marker == TYPE_DB_SELECT {
                    let db_idx = reader.read_u8().map_err(PersistenceError::Io)?;
                    hasher.update(&[db_idx]);
                    let idx = db_idx as usize;
                    if idx >= crate::storage::engine::NUM_DATABASES {
                        return Err(PersistenceError::CorruptedFile(format!(
                            "RDB contains DB selector {} which exceeds max databases ({})",
                            idx,
                            crate::storage::engine::NUM_DATABASES
                        )));
                    }
                    current_db = idx;
                    continue;
                }

                let data_type = marker_to_data_type(type_marker).ok_or_else(|| {
                    PersistenceError::CorruptedFile(format!(
                        "Unknown data type marker: {}",
                        type_marker
                    ))
                })?;

                // Read key
                let key_len = reader
                    .read_u32::<BigEndian>()
                    .map_err(PersistenceError::Io)?;
                hasher.update(&key_len.to_be_bytes());

                let mut key = vec![0u8; key_len as usize];
                reader.read_exact(&mut key).map_err(PersistenceError::Io)?;
                hasher.update(&key);

                // Read expiry (if any)
                let has_expiry = reader.read_u8().map_err(PersistenceError::Io)?;
                hasher.update(&[has_expiry]);

                let expiry = if has_expiry == 1 {
                    let secs = reader
                        .read_u64::<BigEndian>()
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
                let value_len = reader
                    .read_u32::<BigEndian>()
                    .map_err(PersistenceError::Io)?;
                hasher.update(&value_len.to_be_bytes());

                let mut value = vec![0u8; value_len as usize];
                reader
                    .read_exact(&mut value)
                    .map_err(PersistenceError::Io)?;
                hasher.update(&value);

                data.push((current_db, key, value, data_type, expiry));
            }

            // Read and verify checksum
            let expected_checksum = reader
                .read_u32::<BigEndian>()
                .map_err(PersistenceError::Io)?;

            let computed_checksum = hasher.finalize();

            if expected_checksum != computed_checksum {
                return Err(PersistenceError::CrcValidationFailed(format!(
                    "CRC check failed: expected {}, got {}",
                    expected_checksum, computed_checksum
                )));
            }

            // Transfer loaded data to the storage engine with correct data types
            // Uses CURRENT_DB_INDEX task-local to route to the correct database
            tokio::runtime::Handle::current().block_on(async {
                use crate::storage::engine::CURRENT_DB_INDEX;
                for (db_idx, key, value, data_type, ttl) in data {
                    let engine_ref = &engine;
                    let store_value = match deserialize_store_value(&data_type, &value) {
                        Ok(sv) => sv,
                        Err(e) => {
                            warn!(db = db_idx, "Error deserializing RDB value: {}", e);
                            continue;
                        }
                    };
                    let result = CURRENT_DB_INDEX
                        .scope(db_idx, async move {
                            engine_ref
                                .set_with_type(key.clone(), store_value, ttl)
                                .await
                        })
                        .await;
                    if let Err(e) = result {
                        warn!(db = db_idx, "Error restoring key from RDB: {:?}", e);
                    }
                }
            });

            info!(path = ?path, "Successfully loaded RDB file");
            Ok(())
        })
        .await
        .map_err(|e| PersistenceError::BackgroundTaskFailed(e.to_string()))?
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

        // Get a consistent snapshot of all databases
        let all_dbs = self.engine.snapshot_all_dbs().await.map_err(|e| {
            PersistenceError::RdbSnapshotFailed(format!("Failed to get data snapshot: {:?}", e))
        })?;

        // Perform file writing in a blocking task
        let temp_path_clone = temp_path.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<(), PersistenceError> {
            // Create parent directory if it doesn't exist
            if let Some(parent) = temp_path_clone.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| PersistenceError::DirectoryCreationFailed(e.to_string()))?;
            }

            let file = File::create(&temp_path_clone).map_err(PersistenceError::Io)?;

            let mut writer = BufWriter::new(file);

            // Initialize CRC32 hasher
            let mut hasher = Hasher::new();

            // Write magic string and version
            writer
                .write_all(RDB_MAGIC_STRING)
                .map_err(PersistenceError::Io)?;
            hasher.update(RDB_MAGIC_STRING);

            writer.write_u8(RDB_VERSION).map_err(PersistenceError::Io)?;
            hasher.update(&[RDB_VERSION]);

            // Write data from all databases
            for (db_idx, data) in all_dbs.iter().enumerate() {
                if data.is_empty() {
                    continue; // Skip empty databases
                }

                // Write DB selector marker
                writer
                    .write_u8(TYPE_DB_SELECT)
                    .map_err(PersistenceError::Io)?;
                hasher.update(&[TYPE_DB_SELECT]);
                writer
                    .write_u8(db_idx as u8)
                    .map_err(PersistenceError::Io)?;
                hasher.update(&[db_idx as u8]);

                for (key, item) in data.iter() {
                    let type_marker = data_type_to_marker(&item.data_type());

                    writer.write_u8(type_marker).map_err(PersistenceError::Io)?;
                    hasher.update(&[type_marker]);

                    // Write key
                    writer
                        .write_u32::<BigEndian>(key.len() as u32)
                        .map_err(PersistenceError::Io)?;
                    hasher.update(&(key.len() as u32).to_be_bytes());

                    writer.write_all(key).map_err(PersistenceError::Io)?;
                    hasher.update(key);

                    // Write expiry information
                    if let Some(ttl) = item.ttl() {
                        writer
                            .write_u8(1) // Has expiry
                            .map_err(PersistenceError::Io)?;
                        hasher.update(&[1]);

                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();

                        let expiry_time = now + ttl.as_secs();

                        writer
                            .write_u64::<BigEndian>(expiry_time)
                            .map_err(PersistenceError::Io)?;
                        hasher.update(&expiry_time.to_be_bytes());
                    } else {
                        writer
                            .write_u8(0) // No expiry
                            .map_err(PersistenceError::Io)?;
                        hasher.update(&[0]);
                    }

                    // Serialize value to bytes for RDB storage
                    let serialized_value: Vec<u8> = serialize_store_value(&item.value)
                        .map_err(|e| PersistenceError::Io(std::io::Error::other(e)))?;

                    writer
                        .write_u32::<BigEndian>(serialized_value.len() as u32)
                        .map_err(PersistenceError::Io)?;
                    hasher.update(&(serialized_value.len() as u32).to_be_bytes());

                    writer
                        .write_all(&serialized_value)
                        .map_err(PersistenceError::Io)?;
                    hasher.update(&serialized_value);
                }
            }

            // Write EOF marker
            writer.write_u8(TYPE_EOF).map_err(PersistenceError::Io)?;
            hasher.update(&[TYPE_EOF]);

            // Write CRC32 checksum
            let checksum = hasher.finalize();
            writer
                .write_u32::<BigEndian>(checksum)
                .map_err(PersistenceError::Io)?;

            // Ensure all data is flushed to disk
            writer.flush().map_err(PersistenceError::Io)?;

            Ok(())
        })
        .await
        .map_err(|e| PersistenceError::BackgroundTaskFailed(e.to_string()))?;

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
    pub async fn schedule_snapshots(
        &self,
        config: &PersistenceConfig,
    ) -> Result<(), PersistenceError> {
        let engine = self.engine.clone();
        let rdb = Arc::new(self.clone());
        let interval = config.rdb_snapshot_interval;
        let threshold = config.rdb_snapshot_threshold;

        // Register the write counter with the storage engine
        engine
            .register_write_counter(self.write_count.clone())
            .await;

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
                    debug!(
                        "Triggering write-threshold RDB snapshot after {} writes",
                        write_count
                    );
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
    use crate::command::types::sorted_set::SortedSetData;
    use crate::storage::engine::StorageEngine;
    use crate::storage::value::{StoreValue, NativeHashMap, NativeHashSet};
    use std::collections::VecDeque;
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
        engine
            .set(b"mykey".to_vec(), b"myvalue".to_vec(), None)
            .await
            .unwrap();

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
        let list = VecDeque::from(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        engine
            .set_with_type(b"mylist".to_vec(), StoreValue::List(list), None)
            .await
            .unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        // Verify type and data
        let tp = engine2.get_type(b"mylist").await.unwrap();
        assert_eq!(tp, "list");
        let item = engine2.get_item(b"mylist").await.unwrap().unwrap();
        match item.value {
            StoreValue::List(list) => {
                let vals: Vec<&[u8]> = list.iter().map(|v| v.as_slice()).collect();
                assert_eq!(vals, vec![b"a".as_slice(), b"b", b"c"]);
            }
            _ => panic!("Expected list"),
        }
    }

    #[tokio::test]
    async fn test_rdb_save_load_set() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        let mut set = NativeHashSet::default();
        set.insert(b"x".to_vec());
        set.insert(b"y".to_vec());
        engine
            .set_with_type(b"myset".to_vec(), StoreValue::Set(set), None)
            .await
            .unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        let tp = engine2.get_type(b"myset").await.unwrap();
        assert_eq!(tp, "set");
        let item = engine2.get_item(b"myset").await.unwrap().unwrap();
        match item.value {
            StoreValue::Set(set) => {
                assert!(set.contains(&b"x".to_vec()));
                assert!(set.contains(&b"y".to_vec()));
                assert_eq!(set.len(), 2);
            }
            _ => panic!("Expected set"),
        }
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
        engine
            .set_with_type(b"myzset".to_vec(), StoreValue::ZSet(zset), None)
            .await
            .unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        let tp = engine2.get_type(b"myzset").await.unwrap();
        assert_eq!(tp, "zset");
        let item = engine2.get_item(b"myzset").await.unwrap().unwrap();
        match item.value {
            StoreValue::ZSet(zset) => {
                assert_eq!(zset.get_score(b"alice"), Some(1.0));
                assert_eq!(zset.get_score(b"bob"), Some(2.5));
                assert_eq!(zset.len(), 2);
            }
            _ => panic!("Expected zset"),
        }
    }

    #[tokio::test]
    async fn test_rdb_save_load_hash() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        let mut hash = NativeHashMap::default();
        hash.insert(b"field1".to_vec(), b"val1".to_vec());
        hash.insert(b"field2".to_vec(), b"val2".to_vec());
        engine
            .set_with_type(b"myhash".to_vec(), StoreValue::Hash(hash), None)
            .await
            .unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        let tp = engine2.get_type(b"myhash").await.unwrap();
        assert_eq!(tp, "hash");
        let fields = engine2.hash_getall(b"myhash").unwrap();
        let field_map: std::collections::HashMap<Vec<u8>, Vec<u8>> = fields.into_iter().collect();
        assert_eq!(field_map.get(&b"field1".to_vec()), Some(&b"val1".to_vec()));
        assert_eq!(field_map.get(&b"field2".to_vec()), Some(&b"val2".to_vec()));
        assert_eq!(field_map.len(), 2);
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
        engine
            .set_with_type(
                b"mystream".to_vec(),
                StoreValue::Stream(stream),
                None,
            )
            .await
            .unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        let tp = engine2.get_type(b"mystream").await.unwrap();
        assert_eq!(tp, "stream");
    }

    #[tokio::test]
    async fn test_rdb_save_load_mixed_types() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        // String
        engine
            .set(b"str".to_vec(), b"hello".to_vec(), None)
            .await
            .unwrap();

        // List
        let list = VecDeque::from(vec![b"1".to_vec(), b"2".to_vec()]);
        engine
            .set_with_type(
                b"lst".to_vec(),
                StoreValue::List(list),
                None,
            )
            .await
            .unwrap();

        // Set
        let mut set = NativeHashSet::default();
        set.insert(b"a".to_vec());
        engine
            .set_with_type(
                b"st".to_vec(),
                StoreValue::Set(set),
                None,
            )
            .await
            .unwrap();

        // Hash
        let mut hash = NativeHashMap::default();
        hash.insert(b"f".to_vec(), b"v".to_vec());
        engine
            .set_with_type(
                b"hs".to_vec(),
                StoreValue::Hash(hash),
                None,
            )
            .await
            .unwrap();

        // Sorted set
        let mut zset = SortedSetData::new();
        zset.insert(3.0, b"m".to_vec());
        engine
            .set_with_type(
                b"zs".to_vec(),
                StoreValue::ZSet(zset),
                None,
            )
            .await
            .unwrap();

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
        engine
            .set(
                b"expiring".to_vec(),
                b"val".to_vec(),
                Some(Duration::from_secs(3600)),
            )
            .await
            .unwrap();

        // List with TTL
        let list = VecDeque::from(vec![b"item".to_vec()]);
        engine
            .set_with_type(
                b"explist".to_vec(),
                StoreValue::List(list),
                Some(Duration::from_secs(7200)),
            )
            .await
            .unwrap();

        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        // Both should still exist (TTL far in the future)
        assert!(engine2.get(b"expiring").await.unwrap().is_some());
        assert!(engine2.get_item(b"explist").await.unwrap().is_some());
        assert_eq!(engine2.get_type(b"explist").await.unwrap(), "list");
    }

    #[tokio::test]
    async fn test_rdb_checksum_validation() {
        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        engine
            .set(b"key".to_vec(), b"val".to_vec(), None)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn test_rdb_save_load_multi_db() {
        use crate::storage::engine::CURRENT_DB_INDEX;

        let engine = create_test_engine();
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();
        drop(temp);

        // Set keys in DB 0
        engine
            .set(b"db0_key".to_vec(), b"db0_value".to_vec(), None)
            .await
            .unwrap();

        // Set keys in DB 3
        CURRENT_DB_INDEX
            .scope(3, async {
                engine
                    .set(b"db3_key".to_vec(), b"db3_value".to_vec(), None)
                    .await
                    .unwrap();
                engine
                    .set(b"db3_another".to_vec(), b"another_value".to_vec(), None)
                    .await
                    .unwrap();
            })
            .await;

        // Set keys in DB 7 with TTL
        CURRENT_DB_INDEX
            .scope(7, async {
                engine
                    .set(
                        b"db7_key".to_vec(),
                        b"db7_value".to_vec(),
                        Some(Duration::from_secs(3600)),
                    )
                    .await
                    .unwrap();
            })
            .await;

        // Save
        let rdb = RdbPersistence::new(engine.clone(), path.clone());
        rdb.save().await.unwrap();

        // Load into a new engine
        let engine2 = create_test_engine();
        let rdb2 = RdbPersistence::new(engine2.clone(), path);
        rdb2.load().await.unwrap();

        // Verify DB 0
        let val = engine2.get(b"db0_key").await.unwrap();
        assert_eq!(val, Some(b"db0_value".to_vec()));

        // Verify DB 0 does NOT have DB 3's keys
        let val = engine2.get(b"db3_key").await.unwrap();
        assert_eq!(val, None);

        // Verify DB 3
        let val = CURRENT_DB_INDEX
            .scope(3, async { engine2.get(b"db3_key").await.unwrap() })
            .await;
        assert_eq!(val, Some(b"db3_value".to_vec()));

        let val = CURRENT_DB_INDEX
            .scope(3, async { engine2.get(b"db3_another").await.unwrap() })
            .await;
        assert_eq!(val, Some(b"another_value".to_vec()));

        // Verify DB 3 does NOT have DB 0's keys
        let val = CURRENT_DB_INDEX
            .scope(3, async { engine2.get(b"db0_key").await.unwrap() })
            .await;
        assert_eq!(val, None);

        // Verify DB 7 (with TTL)
        let val = CURRENT_DB_INDEX
            .scope(7, async { engine2.get(b"db7_key").await.unwrap() })
            .await;
        assert_eq!(val, Some(b"db7_value".to_vec()));
    }
}
