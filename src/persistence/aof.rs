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
fn aof_rewrite_commands_for_item(key: &Vec<u8>, item: &StorageItem) -> Vec<Vec<Vec<u8>>> {
    let mut commands: Vec<Vec<Vec<u8>>> = Vec::new();

    match item.data_type {
        RedisDataType::String => {
            let mut args = vec![b"SET".to_vec(), key.clone(), item.value.clone()];
            if let Some(ttl) = item.ttl() {
                if ttl.as_millis() > 0 {
                    args.push(b"PX".to_vec());
                    args.push(ttl.as_millis().to_string().into_bytes());
                }
            }
            commands.push(args);
        }
        RedisDataType::List => {
            if let Ok(list) = bincode::deserialize::<Vec<Vec<u8>>>(&item.value) {
                if !list.is_empty() {
                    // Emit RPUSH key elem1 elem2 ...
                    let mut args = vec![b"RPUSH".to_vec(), key.clone()];
                    args.extend(list);
                    commands.push(args);
                }
            }
        }
        RedisDataType::Set => {
            if let Ok(set) = bincode::deserialize::<std::collections::HashSet<Vec<u8>>>(&item.value) {
                if !set.is_empty() {
                    // Emit SADD key member1 member2 ...
                    let mut args = vec![b"SADD".to_vec(), key.clone()];
                    args.extend(set.into_iter());
                    commands.push(args);
                }
            }
        }
        RedisDataType::ZSet => {
            // SortedSet is stored as Vec<(f64, Vec<u8>)> - (score, member) pairs
            if let Ok(ss) = bincode::deserialize::<Vec<(f64, Vec<u8>)>>(&item.value) {
                if !ss.is_empty() {
                    // Emit ZADD key score1 member1 score2 member2 ...
                    let mut args = vec![b"ZADD".to_vec(), key.clone()];
                    for (score, member) in ss {
                        args.push(score.to_string().into_bytes());
                        args.push(member);
                    }
                    commands.push(args);
                }
            }
        }
        RedisDataType::Hash => {
            // Hash fields are stored as composite keys (key:field) in the global keyspace,
            // so individual hash field entries appear as String type items.
            // A key marked as Hash type shouldn't normally appear in snapshot,
            // but emit SET as fallback to avoid data loss.
            let mut args = vec![b"SET".to_vec(), key.clone(), item.value.clone()];
            if let Some(ttl) = item.ttl() {
                if ttl.as_millis() > 0 {
                    args.push(b"PX".to_vec());
                    args.push(ttl.as_millis().to_string().into_bytes());
                }
            }
            commands.push(args);
        }
        RedisDataType::Stream => {
            if let Ok(stream) = bincode::deserialize::<crate::storage::stream::StreamData>(&item.value) {
                // Emit XADD for each entry in the stream
                for (_id, entry) in &stream.entries {
                    let mut args = vec![
                        b"XADD".to_vec(),
                        key.clone(),
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
    if item.data_type != RedisDataType::String {
        if let Some(ttl) = item.ttl() {
            if ttl.as_millis() > 0 {
                commands.push(vec![
                    b"PEXPIRE".to_vec(),
                    key.clone(),
                    ttl.as_millis().to_string().into_bytes(),
                ]);
            }
        }
    }

    commands
}

/// Helper function to check if a command is read-only
fn is_read_only_command(command: &[u8]) -> bool {
    matches!(command,
        b"GET" | b"EXISTS" | b"TTL" | b"PTTL" | b"PING" | b"KEYS" | b"INFO" | b"TYPE" |
        b"MGET" | b"STRLEN" | b"LLEN" | b"HLEN" | b"SCARD" | b"ZCARD" |
        b"HGET" | b"HGETALL" | b"HKEYS" | b"HVALS" | b"SMEMBERS" | b"SISMEMBER" |
        b"LRANGE" | b"ZRANGE" | b"ZSCORE")
}

/// Helper function to ensure a directory exists
fn ensure_dir_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
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