use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{debug, error, info, warn};
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncWriteExt, BufWriter as TokioBufWriter};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{self, Duration, MissedTickBehavior};

use crate::networking::resp::{RespCommand, RespValue};
use crate::persistence::config::{AofSyncPolicy, PersistenceConfig};
use crate::persistence::error::PersistenceError;
use crate::storage::engine::StorageEngine;
use crate::storage::item::{RedisDataType, StorageItem};

// Constants
const BUFFER_SIZE: usize = 4096; // 4KB buffer
// These constants are defined in PersistenceConfig instead

/// Represents a command to be written to the AOF
#[derive(Debug, Clone)]
pub struct AofEntry {
    /// The RESP command
    pub command: RespCommand,
    /// Whether to sync to disk after writing this entry
    pub sync: bool,
}

/// AOF persistence implementation
pub struct AofPersistence {
    engine: Arc<StorageEngine>,
    path: PathBuf,
    writer_tx: mpsc::Sender<AofEntry>,
    file_size: Arc<RwLock<u64>>,
    last_rewrite: Arc<RwLock<Instant>>,
    in_progress: Arc<Mutex<bool>>,
    next_sync: AtomicBool,
}

impl AofPersistence {
    /// Create a new AOF persistence instance
    pub fn new(engine: Arc<StorageEngine>, path: PathBuf) -> Self {
        // Create a channel for the writer task
        let (writer_tx, writer_rx) = mpsc::channel::<AofEntry>(1000);
        
        // Create the persistence instance
        let aof = Self {
            engine,
            path,
            writer_tx,
            file_size: Arc::new(RwLock::new(0)),
            last_rewrite: Arc::new(RwLock::new(Instant::now())),
            in_progress: Arc::new(Mutex::new(false)),
            next_sync: AtomicBool::new(false),
        };
        
        // Start the writer task
        aof.start_writer_task(writer_rx);
        
        aof
    }
    
    /// Start the background writer task
    fn start_writer_task(&self, mut writer_rx: mpsc::Receiver<AofEntry>) {
        let path = self.path.clone();
        let file_size = self.file_size.clone();
        
        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        
        // Spawn the writer task
        tokio::spawn(async move {
            // Open the AOF file in append mode
            let file = match TokioFile::options()
                .create(true)
                .append(true)
                .open(&path).await {
                    Ok(file) => file,
                    Err(e) => {
                        error!(path = ?path, "Failed to open AOF file: {:?}", e);
                        return;
                    }
                };
            
            // Get the current file size
            let metadata = match file.metadata().await {
                Ok(metadata) => metadata,
                Err(e) => {
                    error!(path = ?path, "Failed to get AOF file metadata: {:?}", e);
                    return;
                }
            };
            
            // Update the file size
            *file_size.write().await = metadata.len();
            
            // Create a buffered writer
            let mut writer = TokioBufWriter::with_capacity(BUFFER_SIZE, file);
            
            // Periodically fsync for EverySecond policy
            let mut fsync_interval = time::interval(Duration::from_secs(1));
            fsync_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            
            // Track whether we need to fsync (for EverySecond policy)
            let mut need_fsync = false;

            loop {
                tokio::select! {
                    // Handle new entries
                    Some(entry) = writer_rx.recv() => {
                        // Serialize the command to RESP format
                        let resp_cmd = RespValue::Array(Some(
                            std::iter::once(RespValue::BulkString(Some(entry.command.name.clone())))
                                .chain(entry.command.args.into_iter().map(|arg| RespValue::BulkString(Some(arg))))
                                .collect()
                        ));

                        // Serialize and handle errors
                        let serialized = match resp_cmd.serialize() {
                            Ok(data) => data,
                            Err(e) => {
                                error!(command = ?resp_cmd, "Failed to serialize command for AOF: {:?}", e);
                                continue;
                            }
                        };

                        // Write to the file
                        if let Err(e) = writer.write_all(&serialized).await {
                            error!(path = ?path, "Failed to write to AOF file: {:?}", e);
                            continue;
                        }

                        // Update the file size
                        *file_size.write().await += serialized.len() as u64;

                        if entry.sync {
                            // Always policy: flush and fsync immediately
                            if let Err(e) = writer.flush().await {
                                error!(path = ?path, "Failed to flush AOF buffer: {:?}", e);
                            }
                            if let Err(e) = writer.get_ref().sync_all().await {
                                error!(path = ?path, "Failed to fsync AOF file: {:?}", e);
                            }
                        } else {
                            // EverySecond policy: mark that we have unfsynced data
                            need_fsync = true;
                        }
                    }

                    // Handle periodic fsync (for EverySecond policy)
                    _ = fsync_interval.tick() => {
                        if need_fsync {
                            // Flush the buffer
                            if let Err(e) = writer.flush().await {
                                error!(path = ?path, "Failed to flush AOF buffer during periodic fsync: {:?}", e);
                            }

                            // Sync to disk
                            if let Err(e) = writer.get_ref().sync_all().await {
                                error!(path = ?path, "Failed to fsync AOF file during periodic fsync: {:?}", e);
                            }

                            need_fsync = false;
                        }
                    }
                }
            }
        });
    }
    
    /// Load data from AOF file on startup
    pub async fn load(&self) -> Result<(), PersistenceError> {
        if !self.path.exists() {
            debug!(path = ?self.path, "No AOF file found, skipping load");
            return Ok(());
        }
        
        info!(path = ?self.path, "Loading AOF file");
        
        // Perform file reading in a blocking task
        let path = self.path.clone();
        let engine = self.engine.clone();
        
        tokio::task::spawn_blocking(move || -> Result<(), PersistenceError> {
            let file = File::open(&path)
                .map_err(PersistenceError::Io)?;
            
            let reader = BufReader::new(file);
            let mut line_count = 0;
            let mut command_count = 0;
            
            // Process each line
            let mut parser = resp_parser::RespParser::new(reader);
            
            while let Ok(Some(value)) = parser.next() {
                line_count += 1;
                
                // Convert value to command
                let command = match value {
                    RespValue::Array(Some(array)) => {
                        if array.is_empty() {
                            continue;
                        }
                        
                        let name = match &array[0] {
                            RespValue::BulkString(Some(s)) => s.clone(),
                            RespValue::SimpleString(s) => s.as_bytes().to_vec(),
                            _ => continue,
                        };
                        
                        let args = array[1..].iter()
                            .filter_map(|arg| match arg {
                                RespValue::BulkString(Some(s)) => Some(s.clone()),
                                RespValue::SimpleString(s) => Some(s.as_bytes().to_vec()),
                                _ => None,
                            })
                            .collect();
                        
                        RespCommand { name, args }
                    },
                    _ => continue,
                };
                
                // Execute the command
                tokio::runtime::Handle::current().block_on(async {
                    if let Err(e) = engine.process_command(&command).await {
                        warn!(command = ?command, "Error processing AOF command: {:?}", e);
                    } else {
                        command_count += 1;
                    }
                });
            }
            
            info!(path = ?path, commands = command_count, lines = line_count,
                  "Successfully loaded commands from AOF file");
            Ok(())
        }).await.map_err(|e| PersistenceError::BackgroundTaskFailed(e.to_string()))?
    }
    
    /// Append a command to the AOF
    pub async fn append_command(&self, command: RespCommand, sync_policy: AofSyncPolicy) -> Result<(), PersistenceError> {
        // Skip commands that don't modify data
        if is_read_only_command(&command.name) {
            return Ok(());
        }
        
        // Determine if we need to sync based on the policy
        let sync = match sync_policy {
            AofSyncPolicy::Always => true,
            AofSyncPolicy::EverySecond => false,
            AofSyncPolicy::None => false,
        };
        
        // Create an AOF entry
        let entry = AofEntry {
            command,
            sync,
        };
        
        // Send to the writer task
        self.writer_tx.send(entry).await
            .map_err(|_| PersistenceError::Other("Failed to send command to AOF writer".to_string()))?;
        
        Ok(())
    }
    
    /// Start background AOF rewrite check
    pub async fn start_background_rewrite_check(&self, config: &PersistenceConfig) -> Result<(), PersistenceError> {
        let aof = Arc::new(self.clone());
        let min_size = config.aof_rewrite_min_size;
        let percentage = config.aof_rewrite_percentage;
        
        // Spawn a background task to check if we need to rewrite the AOF
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));
            
            loop {
                interval.tick().await;
                
                // Check if we need to rewrite the AOF
                let current_size = *aof.file_size.read().await;
                
                if current_size > min_size as u64 {
                    debug!("AOF file size: {} bytes, checking if rewrite is needed", current_size);
                    
                    // Check if the file has grown enough to warrant a rewrite
                    let last_rewrite = *aof.last_rewrite.read().await;
                    let rewrite_threshold = min_size as u64 * (1 + percentage as u64 / 100);
                    
                    if current_size > rewrite_threshold && last_rewrite.elapsed() > Duration::from_secs(3600) {
                        info!("AOF file size ({} bytes) exceeds threshold ({} bytes), triggering rewrite",
                              current_size, rewrite_threshold);
                        if let Err(e) = aof.rewrite().await {
                            error!("AOF rewrite failed: {:?}", e);
                        }
                    }
                }
            }
        });
        
        Ok(())
    }
    
    /// Rewrite the AOF file with the current dataset
    pub async fn rewrite(&self) -> Result<(), PersistenceError> {
        // Create a temporary file path
        let temp_path = self.path.with_extension("tmp");
        let temp_path_cleanup = temp_path.clone();
        
        debug!("Starting AOF rewrite to temporary file: {:?}", temp_path);
        
        // Get a consistent snapshot of the data
        let snapshot = match self.engine.snapshot().await {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to create snapshot for AOF rewrite: {:?}", e);
                return Err(PersistenceError::AofRewriteFailed(e.to_string()));
            }
        };
        
        // Mark rewrite as in progress
        let mut in_progress = self.in_progress.lock().await;
        if *in_progress {
            debug!("AOF rewrite already in progress, skipping");
            return Ok(());
        }
        *in_progress = true;
        
        // Spawn a blocking task for file operations
        let path = self.path.clone();
        
        let result = tokio::task::spawn_blocking(move || -> Result<(), PersistenceError> {
            // Create parent directory if it doesn't exist
            ensure_dir_exists(&temp_path)?;
            
            let file = File::create(&temp_path)
                .map_err(PersistenceError::Io)?;
            
            let mut writer = BufWriter::new(file);
            
            // Write each key using type-appropriate commands
            for (key, item) in snapshot.iter() {
                let commands = aof_rewrite_commands_for_item(key, item);
                for args in commands {
                    let resp_cmd = RespValue::Array(Some(
                        args.into_iter().map(|arg: Vec<u8>| RespValue::BulkString(Some(arg))).collect()
                    ));

                    let serialized = resp_cmd.serialize()
                        .map_err(|e| PersistenceError::Serialization(e.to_string()))?;

                    writer.write_all(&serialized)
                        .map_err(PersistenceError::Io)?;
                }
            }
            
            // Ensure all data is flushed to disk
            writer.flush()
                .map_err(PersistenceError::Io)?;
            
            // Rename temp file to target file atomically
            let rename_result = fs::rename(&temp_path, &path);
            
            if rename_result.is_err() {
                error!("Failed to rename temp file during AOF rewrite: {:?}", rename_result);
            }
            
            rename_result.map_err(|e| PersistenceError::AtomicRenameFailed(e.to_string()))?;
            
            Ok(())
        }).await;
        
        // Handle the result
        let rewrite_result = match result {
            Ok(Ok(())) => {
                debug!("AOF rewrite completed successfully");
                // Update the file size
                if let Ok(metadata) = tokio::fs::metadata(&self.path).await {
                    *self.file_size.write().await = metadata.len();
                }
                *self.last_rewrite.write().await = Instant::now();
                Ok(())
            }
            Ok(Err(e)) => {
                // Clean up temp file on error
                let _ = fs::remove_file(&temp_path_cleanup);
                error!("AOF rewrite failed: {}", e);
                Err(e)
            }
            Err(e) => {
                // Clean up temp file on join error
                let _ = fs::remove_file(&temp_path_cleanup);
                error!("AOF rewrite task panicked: {}", e);
                Err(PersistenceError::CorruptedFile(format!("AOF rewrite task panicked: {}", e)))
            }
        };

        // Mark rewrite as complete
        *in_progress = false;

        rewrite_result
    }
}

impl Clone for AofPersistence {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            path: self.path.clone(),
            writer_tx: self.writer_tx.clone(),
            file_size: self.file_size.clone(),
            last_rewrite: self.last_rewrite.clone(),
            in_progress: self.in_progress.clone(),
            // AtomicBool doesn't have clone, so we create a new one with the same value
            next_sync: AtomicBool::new(self.next_sync.load(Ordering::SeqCst)),
        }
    }
}

/// Generate AOF-rewrite commands for a single key/item based on its data type.
/// Returns a Vec of commands, where each command is a Vec of byte arguments.
/// For collection types, we emit the appropriate Redis command to reconstruct the data.
/// If a key has a TTL, we append a PEXPIRE command.
fn aof_rewrite_commands_for_item(key: &[u8], item: &StorageItem) -> Vec<Vec<Vec<u8>>> {
    let mut commands: Vec<Vec<Vec<u8>>> = Vec::new();

    match item.data_type {
        RedisDataType::String => {
            let mut args = vec![b"SET".to_vec(), key.to_vec(), item.value.clone()];
            if let Some(ttl) = item.ttl()
                && ttl.as_millis() > 0 {
                    args.push(b"PX".to_vec());
                    args.push(ttl.as_millis().to_string().into_bytes());
                }
            commands.push(args);
        }
        RedisDataType::List => {
            if let Ok(list) = bincode::deserialize::<Vec<Vec<u8>>>(&item.value)
                && !list.is_empty() {
                    // Emit RPUSH key elem1 elem2 ...
                    let mut args = vec![b"RPUSH".to_vec(), key.to_vec()];
                    args.extend(list);
                    commands.push(args);
                }
        }
        RedisDataType::Set => {
            if let Ok(set) = bincode::deserialize::<std::collections::HashSet<Vec<u8>>>(&item.value)
                && !set.is_empty() {
                    // Emit SADD key member1 member2 ...
                    let mut args = vec![b"SADD".to_vec(), key.to_vec()];
                    args.extend(set);
                    commands.push(args);
                }
        }
        RedisDataType::ZSet => {
            // SortedSetData: BTreeSet<(OrderedFloat<f64>, Vec<u8>)> + HashMap<Vec<u8>, f64>
            if let Ok(ss) = bincode::deserialize::<crate::command::types::sorted_set::SortedSetData>(&item.value)
                && !ss.is_empty() {
                    let mut args = vec![b"ZADD".to_vec(), key.to_vec()];
                    for (member, score) in &ss.scores {
                        args.push(score.to_string().into_bytes());
                        args.push(member.clone());
                    }
                    commands.push(args);
                }
        }
        RedisDataType::Hash => {
            // Hash stored as bincode-serialized HashMap<Vec<u8>, Vec<u8>>
            if let Ok(hash) = bincode::deserialize::<std::collections::HashMap<Vec<u8>, Vec<u8>>>(&item.value)
                && !hash.is_empty() {
                    let mut args = vec![b"HSET".to_vec(), key.to_vec()];
                    for (field, value) in hash {
                        args.push(field);
                        args.push(value);
                    }
                    commands.push(args);
                }
        }
        RedisDataType::Stream => {
            if let Ok(stream) = bincode::deserialize::<crate::storage::stream::StreamData>(&item.value) {
                // Emit XADD for each entry in the stream
                for entry in stream.entries.values() {
                    let mut args = vec![
                        b"XADD".to_vec(),
                        key.to_vec(),
                        entry.id.to_string().into_bytes(),
                    ];
                    for (field, value) in &entry.fields {
                        args.push(field.clone());
                        args.push(value.clone());
                    }
                    commands.push(args);
                }
            }
        }
    }

    // For non-string types, append a PEXPIRE command if TTL is set
    if item.data_type != RedisDataType::String
        && let Some(ttl) = item.ttl()
            && ttl.as_millis() > 0 {
                commands.push(vec![
                    b"PEXPIRE".to_vec(),
                    key.to_vec(),
                    ttl.as_millis().to_string().into_bytes(),
                ]);
            }

    commands
}

/// Helper function to check if a command is read-only
fn is_read_only_command(command: &[u8]) -> bool {
    let upper: Vec<u8> = command.iter().map(|b| b.to_ascii_uppercase()).collect();
    matches!(upper.as_slice(),
        b"GET" | b"EXISTS" | b"TTL" | b"PTTL" | b"PING" | b"KEYS" | b"INFO" | b"TYPE" |
        b"MGET" | b"STRLEN" | b"LLEN" | b"HLEN" | b"SCARD" | b"ZCARD" |
        b"HGET" | b"HGETALL" | b"HKEYS" | b"HVALS" | b"SMEMBERS" | b"SISMEMBER" |
        b"LRANGE" | b"ZRANGE" | b"ZSCORE" |
        b"DBSIZE" | b"RANDOMKEY" | b"SCAN" | b"HSCAN" | b"SSCAN" | b"ZSCAN" |
        b"OBJECT" | b"TIME" | b"COMMAND" | b"CLIENT" |
        b"LPOS" | b"SRANDMEMBER" | b"ZRANGEBYSCORE" | b"ZRANGEBYLEX" |
        b"ZREVRANGE" | b"ZREVRANGEBYSCORE" | b"ZREVRANGEBYLEX" | b"ZRANK" | b"ZREVRANK" |
        b"ZCOUNT" | b"ZLEXCOUNT" | b"GEODIST" | b"GEOPOS" | b"GEOHASH" | b"GEOSEARCH" |
        b"XRANGE" | b"XREVRANGE" | b"XLEN" | b"XINFO" | b"XREAD" | b"XPENDING" |
        b"BITCOUNT" | b"BITPOS" | b"GETBIT" | b"SUBSTR" | b"GETRANGE" |
        b"LINDEX" | b"HEXISTS" | b"SMISMEMBER" |
        b"PFCOUNT" | b"ECHO" | b"SELECT" | b"WAIT" | b"DEBUG" |
        b"SLOWLOG" | b"MEMORY" | b"LATENCY" | b"MODULE" |
        b"CLUSTER" | b"SENTINEL" | b"SUBSCRIBE" | b"UNSUBSCRIBE" |
        b"PSUBSCRIBE" | b"PUNSUBSCRIBE" | b"PUBSUB" |
        b"AUTH" | b"HELLO" | b"RESET" | b"QUIT")
}

/// Helper function to ensure a directory exists
fn ensure_dir_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    Ok(())
}

/// Module for parsing RESP protocol data from a reader
mod resp_parser {
    use std::io::{self, BufRead};
    
    use crate::networking::resp::RespValue;
    
    /// Parser for RESP protocol
    pub struct RespParser<R: BufRead> {
        reader: R,
        buffer: String,
    }
    
    impl<R: BufRead> RespParser<R> {
        /// Create a new RESP parser
        pub fn new(reader: R) -> Self {
            Self {
                reader,
                buffer: String::with_capacity(4096),
            }
        }
        
        /// Get the next value from the stream
        pub fn next(&mut self) -> io::Result<Option<RespValue>> {
            // Clear the buffer
            self.buffer.clear();
            
            // Read a line
            if self.reader.read_line(&mut self.buffer)? == 0 {
                return Ok(None);
            }
            
            // Parse the value
            match self.buffer.chars().next() {
                Some('+') => Ok(Some(RespValue::SimpleString(
                    self.buffer[1..self.buffer.len() - 2].to_string()
                ))),
                Some('-') => Ok(Some(RespValue::Error(
                    self.buffer[1..self.buffer.len() - 2].to_string()
                ))),
                Some(':') => {
                    let num = self.buffer[1..self.buffer.len() - 2].parse::<i64>()
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid integer"))?;
                    Ok(Some(RespValue::Integer(num)))
                },
                Some('$') => {
                    let size = self.buffer[1..self.buffer.len() - 2].parse::<i64>()
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk string size"))?;
                    
                    if size < 0 {
                        return Ok(Some(RespValue::SimpleString("nil".to_string())));
                    }
                    
                    let mut bulk = vec![0; size as usize];
                    self.reader.read_exact(&mut bulk)?;
                    
                    // Read and discard the CRLF
                    self.buffer.clear();
                    self.reader.read_line(&mut self.buffer)?;
                    
                    Ok(Some(RespValue::BulkString(Some(bulk))))
                },
                Some('*') => {
                    let count = self.buffer[1..self.buffer.len() - 2].parse::<i64>()
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid array size"))?;
                    
                    if count < 0 {
                        return Ok(Some(RespValue::SimpleString("nil".to_string())));
                    }
                    
                    let mut array = Vec::with_capacity(count as usize);
                    for _ in 0..count {
                        if let Some(value) = self.next()? {
                            array.push(value);
                        } else {
                            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF"));
                        }
                    }
                    
                    Ok(Some(RespValue::Array(Some(array))))
                },
                _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RESP data type")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use crate::storage::engine::StorageEngine;
    use crate::storage::item::RedisDataType;
    use crate::command::types::sorted_set::SortedSetData;

    fn create_test_engine() -> Arc<StorageEngine> {
        StorageEngine::new(Default::default())
    }

    fn make_item(value: Vec<u8>, data_type: RedisDataType) -> StorageItem {
        StorageItem::new_with_type(value, data_type)
    }

    #[test]
    fn test_aof_rewrite_string() {
        let key = b"mykey".to_vec();
        let item = make_item(b"myval".to_vec(), RedisDataType::String);
        let cmds = aof_rewrite_commands_for_item(&key, &item);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0][0], b"SET");
        assert_eq!(cmds[0][1], b"mykey");
        assert_eq!(cmds[0][2], b"myval");
    }

    #[test]
    fn test_aof_rewrite_list() {
        let key = b"mylist".to_vec();
        let list: Vec<Vec<u8>> = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let item = make_item(bincode::serialize(&list).unwrap(), RedisDataType::List);
        let cmds = aof_rewrite_commands_for_item(&key, &item);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0][0], b"RPUSH");
        assert_eq!(cmds[0][1], b"mylist");
        assert_eq!(cmds[0][2], b"a");
        assert_eq!(cmds[0][3], b"b");
        assert_eq!(cmds[0][4], b"c");
    }

    #[test]
    fn test_aof_rewrite_set() {
        let key = b"myset".to_vec();
        let mut set = HashSet::new();
        set.insert(b"x".to_vec());
        set.insert(b"y".to_vec());
        let item = make_item(bincode::serialize(&set).unwrap(), RedisDataType::Set);
        let cmds = aof_rewrite_commands_for_item(&key, &item);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0][0], b"SADD");
        assert_eq!(cmds[0][1], b"myset");
        assert_eq!(cmds[0].len(), 4); // SADD + key + 2 members
    }

    #[test]
    fn test_aof_rewrite_sorted_set() {
        let key = b"myzset".to_vec();
        let mut zset = SortedSetData::new();
        zset.insert(1.5, b"alice".to_vec());
        zset.insert(2.0, b"bob".to_vec());
        let item = make_item(bincode::serialize(&zset).unwrap(), RedisDataType::ZSet);
        let cmds = aof_rewrite_commands_for_item(&key, &item);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0][0], b"ZADD");
        assert_eq!(cmds[0][1], b"myzset");
        // ZADD key score member score member -> 6 args
        assert_eq!(cmds[0].len(), 6);
    }

    #[test]
    fn test_aof_rewrite_hash() {
        let key = b"myhash".to_vec();
        let mut hash = HashMap::new();
        hash.insert(b"f1".to_vec(), b"v1".to_vec());
        hash.insert(b"f2".to_vec(), b"v2".to_vec());
        let item = make_item(bincode::serialize(&hash).unwrap(), RedisDataType::Hash);
        let cmds = aof_rewrite_commands_for_item(&key, &item);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0][0], b"HSET");
        assert_eq!(cmds[0][1], b"myhash");
        // HSET key field val field val -> 6 args
        assert_eq!(cmds[0].len(), 6);
    }

    #[test]
    fn test_aof_rewrite_stream() {
        let key = b"mystream".to_vec();
        let mut stream = crate::storage::stream::StreamData::new();
        let entry = crate::storage::stream::StreamEntry {
            id: crate::storage::stream::StreamEntryId::new(1000, 0),
            fields: vec![(b"temp".to_vec(), b"25".to_vec())],
        };
        stream.entries.insert(entry.id.clone(), entry);
        stream.last_id = crate::storage::stream::StreamEntryId::new(1000, 0);
        let item = make_item(bincode::serialize(&stream).unwrap(), RedisDataType::Stream);
        let cmds = aof_rewrite_commands_for_item(&key, &item);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0][0], b"XADD");
        assert_eq!(cmds[0][1], b"mystream");
        assert_eq!(cmds[0].len(), 5); // XADD key id field value
    }

    #[tokio::test]
    async fn test_aof_replay_rpush() {
        let engine = create_test_engine();
        let cmd = RespCommand {
            name: b"RPUSH".to_vec(),
            args: vec![b"mylist".to_vec(), b"a".to_vec(), b"b".to_vec()],
        };
        engine.process_command(&cmd).await.unwrap();
        let val = engine.get(b"mylist").await.unwrap().unwrap();
        let list: Vec<Vec<u8>> = bincode::deserialize(&val).unwrap();
        assert_eq!(list, vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(engine.get_type(b"mylist").await.unwrap(), "list");
    }

    #[tokio::test]
    async fn test_aof_replay_sadd() {
        let engine = create_test_engine();
        let cmd = RespCommand {
            name: b"SADD".to_vec(),
            args: vec![b"myset".to_vec(), b"x".to_vec(), b"y".to_vec()],
        };
        engine.process_command(&cmd).await.unwrap();
        let val = engine.get(b"myset").await.unwrap().unwrap();
        let set: HashSet<Vec<u8>> = bincode::deserialize(&val).unwrap();
        assert!(set.contains(&b"x".to_vec()));
        assert!(set.contains(&b"y".to_vec()));
        assert_eq!(engine.get_type(b"myset").await.unwrap(), "set");
    }

    #[tokio::test]
    async fn test_aof_replay_zadd() {
        let engine = create_test_engine();
        let cmd = RespCommand {
            name: b"ZADD".to_vec(),
            args: vec![b"myzset".to_vec(), b"1.5".to_vec(), b"alice".to_vec(), b"2.0".to_vec(), b"bob".to_vec()],
        };
        engine.process_command(&cmd).await.unwrap();
        let val = engine.get(b"myzset").await.unwrap().unwrap();
        let zset: SortedSetData = bincode::deserialize(&val).unwrap();
        assert_eq!(zset.len(), 2);
        assert_eq!(zset.scores.get(&b"alice".to_vec()), Some(&1.5));
        assert_eq!(zset.scores.get(&b"bob".to_vec()), Some(&2.0));
        assert_eq!(engine.get_type(b"myzset").await.unwrap(), "zset");
    }

    #[tokio::test]
    async fn test_aof_replay_hset() {
        let engine = create_test_engine();
        let cmd = RespCommand {
            name: b"HSET".to_vec(),
            args: vec![b"myhash".to_vec(), b"f1".to_vec(), b"v1".to_vec(), b"f2".to_vec(), b"v2".to_vec()],
        };
        engine.process_command(&cmd).await.unwrap();
        let fields = engine.hash_getall(b"myhash").unwrap();
        assert_eq!(fields.len(), 2);
    }

    #[tokio::test]
    async fn test_aof_replay_xadd() {
        let engine = create_test_engine();
        let cmd = RespCommand {
            name: b"XADD".to_vec(),
            args: vec![b"mystream".to_vec(), b"1000-0".to_vec(), b"temp".to_vec(), b"25".to_vec()],
        };
        engine.process_command(&cmd).await.unwrap();
        let val = engine.get(b"mystream").await.unwrap().unwrap();
        let stream: crate::storage::stream::StreamData = bincode::deserialize(&val).unwrap();
        assert_eq!(stream.entries.len(), 1);
        assert_eq!(engine.get_type(b"mystream").await.unwrap(), "stream");
    }

    #[tokio::test]
    async fn test_aof_replay_set_with_px() {
        let engine = create_test_engine();
        let cmd = RespCommand {
            name: b"SET".to_vec(),
            args: vec![b"k".to_vec(), b"v".to_vec(), b"PX".to_vec(), b"60000".to_vec()],
        };
        engine.process_command(&cmd).await.unwrap();
        let val = engine.get(b"k").await.unwrap();
        assert_eq!(val, Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn test_aof_round_trip_all_types() {
        let engine = create_test_engine();

        // String
        let str_item = make_item(b"hello".to_vec(), RedisDataType::String);
        for args in aof_rewrite_commands_for_item(&b"str".to_vec(), &str_item) {
            engine.process_command(&RespCommand { name: args[0].clone(), args: args[1..].to_vec() }).await.unwrap();
        }

        // List
        let list: Vec<Vec<u8>> = vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()];
        let list_item = make_item(bincode::serialize(&list).unwrap(), RedisDataType::List);
        for args in aof_rewrite_commands_for_item(&b"lst".to_vec(), &list_item) {
            engine.process_command(&RespCommand { name: args[0].clone(), args: args[1..].to_vec() }).await.unwrap();
        }

        // Set
        let mut set = HashSet::new();
        set.insert(b"a".to_vec());
        set.insert(b"b".to_vec());
        let set_item = make_item(bincode::serialize(&set).unwrap(), RedisDataType::Set);
        for args in aof_rewrite_commands_for_item(&b"st".to_vec(), &set_item) {
            engine.process_command(&RespCommand { name: args[0].clone(), args: args[1..].to_vec() }).await.unwrap();
        }

        // Hash
        let mut hash = HashMap::new();
        hash.insert(b"f".to_vec(), b"v".to_vec());
        let hash_item = make_item(bincode::serialize(&hash).unwrap(), RedisDataType::Hash);
        for args in aof_rewrite_commands_for_item(&b"hs".to_vec(), &hash_item) {
            engine.process_command(&RespCommand { name: args[0].clone(), args: args[1..].to_vec() }).await.unwrap();
        }

        // Sorted set
        let mut zset = SortedSetData::new();
        zset.insert(1.0, b"m1".to_vec());
        zset.insert(2.0, b"m2".to_vec());
        let zset_item = make_item(bincode::serialize(&zset).unwrap(), RedisDataType::ZSet);
        for args in aof_rewrite_commands_for_item(&b"zs".to_vec(), &zset_item) {
            engine.process_command(&RespCommand { name: args[0].clone(), args: args[1..].to_vec() }).await.unwrap();
        }

        // Verify all restored correctly
        assert_eq!(engine.get(b"str").await.unwrap(), Some(b"hello".to_vec()));
        assert_eq!(engine.get_type(b"str").await.unwrap(), "string");

        let list_val = engine.get(b"lst").await.unwrap().unwrap();
        let loaded_list: Vec<Vec<u8>> = bincode::deserialize(&list_val).unwrap();
        assert_eq!(loaded_list, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);

        let set_val = engine.get(b"st").await.unwrap().unwrap();
        let loaded_set: HashSet<Vec<u8>> = bincode::deserialize(&set_val).unwrap();
        assert!(loaded_set.contains(&b"a".to_vec()));

        let hash_fields = engine.hash_getall(b"hs").unwrap();
        assert_eq!(hash_fields.len(), 1);

        let zset_val = engine.get(b"zs").await.unwrap().unwrap();
        let loaded_zset: SortedSetData = bincode::deserialize(&zset_val).unwrap();
        assert_eq!(loaded_zset.len(), 2);
    }
}