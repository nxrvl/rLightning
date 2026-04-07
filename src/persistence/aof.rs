use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use tokio::fs::File as TokioFile;
use tokio::io::{AsyncWriteExt, BufWriter as TokioBufWriter};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::time::{self, Duration, MissedTickBehavior};
use tracing::{debug, error, info, warn};

use crate::command::Command;
use crate::command::handler::CommandHandler;
use crate::networking::resp::{RespCommand, RespValue};
use crate::persistence::config::{AofSyncPolicy, PersistenceConfig};
use crate::persistence::error::PersistenceError;
use crate::storage::engine::StorageEngine;
use crate::storage::item::{RedisDataType, StorageItem};

// Constants
const BUFFER_SIZE: usize = 4096; // 4KB buffer
// These constants are defined in PersistenceConfig instead

/// Represents one or more commands to be written to the AOF atomically.
/// Using a Vec allows SELECT + write-command pairs to be sent as a single
/// unit through the channel, preventing interleaving with concurrent writers.
#[derive(Debug, Clone)]
pub struct AofEntry {
    /// The RESP commands (written together without yielding)
    pub commands: Vec<RespCommand>,
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
                .open(&path)
                .await
            {
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

            // Track whether the writer is in a valid state. Set to false when a
            // reopen fails after an I/O error, preventing the next iteration from
            // flushing stale partial data from the old BufWriter.
            let mut writer_valid = true;

            // Periodically fsync for EverySecond policy
            let mut fsync_interval = time::interval(Duration::from_secs(1));
            fsync_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            // Track whether we need to fsync (for EverySecond policy)
            let mut need_fsync = false;

            loop {
                tokio::select! {
                    // Handle new entries — batch-drain: collect the first entry plus
                    // all immediately available entries from the channel to write them
                    // in a single I/O operation, reducing syscall overhead.
                    Some(entry) = writer_rx.recv() => {
                        // If the writer was poisoned by a prior failed reopen,
                        // try to recover before processing any new batches.
                        if !writer_valid {
                            match TokioFile::options()
                                .create(true)
                                .append(true)
                                .open(&path).await {
                                Ok(file) => {
                                    // Recover file_size from the reopened handle.
                                    match file.metadata().await {
                                        Ok(meta) => {
                                            *file_size.write().await = meta.len();
                                        }
                                        Err(me) => {
                                            error!(path = ?path, "Failed to recover AOF file size from reopened handle: {:?}", me);
                                        }
                                    }
                                    writer = TokioBufWriter::with_capacity(BUFFER_SIZE, file);
                                    writer_valid = true;
                                }
                                Err(e) => {
                                    error!(path = ?path, "AOF writer still poisoned, cannot reopen file — discarding batch: {:?}", e);
                                    continue;
                                }
                            }
                        }

                        // Batch-drain: collect all immediately available entries
                        let mut entries = vec![entry];
                        while let Ok(extra) = writer_rx.try_recv() {
                            entries.push(extra);
                        }

                        // Determine if any entry in the batch requires immediate sync
                        let mut force_sync = false;

                        // Stage all entries in a single memory buffer before writing.
                        // This prevents partial RESP commands from reaching the file
                        // if serialization or I/O fails mid-batch.
                        let mut batch_buf: Vec<u8> = Vec::new();
                        let mut serialization_failed = false;
                        let mut total_commands = 0usize;

                        for batch_entry in &entries {
                            if batch_entry.sync {
                                force_sync = true;
                            }
                            for command in &batch_entry.commands {
                                total_commands += 1;
                                let resp_cmd = RespValue::Array(Some(
                                    std::iter::once(RespValue::BulkString(Some(command.name.clone())))
                                        .chain(command.args.iter().cloned().map(|arg| RespValue::BulkString(Some(arg))))
                                        .collect()
                                ));

                                match resp_cmd.serialize() {
                                    Ok(data) => batch_buf.extend_from_slice(&data),
                                    Err(e) => {
                                        error!(
                                            command_name = ?command.name,
                                            batch_size = total_commands,
                                            "Failed to serialize command for AOF, discarding entire batch: {:?}", e
                                        );
                                        serialization_failed = true;
                                        break;
                                    }
                                }
                            }
                            if serialization_failed {
                                break;
                            }
                        }

                        // If any command failed to serialize, discard the entire batch
                        if serialization_failed {
                            continue;
                        }

                        // Flush any previously buffered data to disk before recording
                        // the pre-write size. BufWriter may hold unflushed data from
                        // prior batches, making file_size ahead of the actual on-disk
                        // position. Flushing first ensures set_len won't zero-extend
                        // the file on error.
                        if let Err(e) = writer.flush().await {
                            error!(path = ?path, "Failed to flush AOF buffer before batch write: {:?}", e);
                            // The BufWriter may hold unflushed data, making the in-memory
                            // file_size counter ahead of actual on-disk bytes. Recover the
                            // real file size and recreate the writer to avoid stale-size
                            // truncation hazards on subsequent write failures.
                            let mut size_recovered = false;
                            match tokio::fs::metadata(&path).await {
                                Ok(meta) => {
                                    *file_size.write().await = meta.len();
                                    size_recovered = true;
                                }
                                Err(me) => {
                                    error!(path = ?path, "Failed to stat AOF file after flush failure: {:?}", me);
                                }
                            }
                            match TokioFile::options()
                                .create(true)
                                .append(true)
                                .open(&path).await {
                                Ok(file) => {
                                    // If metadata() failed above, recover file size from the
                                    // reopened handle to avoid stale pre_write_size on later errors.
                                    if !size_recovered {
                                        match file.metadata().await {
                                            Ok(meta) => {
                                                *file_size.write().await = meta.len();
                                            }
                                            Err(me) => {
                                                error!(path = ?path, "Failed to recover AOF file size from reopened handle: {:?}", me);
                                            }
                                        }
                                    }
                                    writer = TokioBufWriter::with_capacity(BUFFER_SIZE, file);
                                }
                                Err(reopen_err) => {
                                    error!(path = ?path, "Failed to reopen AOF file after flush failure: {:?}", reopen_err);
                                    writer_valid = false;
                                }
                            }
                            continue;
                        }
                        // Record pre-write file size so we can truncate on partial failure.
                        let pre_write_size = *file_size.read().await;
                        let total_written = batch_buf.len() as u64;
                        if let Err(e) = writer.write_all(&batch_buf).await {
                            error!(
                                path = ?path,
                                batch_size = total_commands,
                                batch_bytes = total_written,
                                "Failed to write batch to AOF file, discarding batch: {:?}", e
                            );
                            // Re-create the writer to discard any partial data
                            // that BufWriter may have buffered before the error.
                            // Truncate the file to the pre-write size to remove any
                            // partial bytes that BufWriter may have flushed to disk.
                            match TokioFile::options()
                                .write(true)
                                .open(&path).await {
                                Ok(truncate_file) => {
                                    if let Err(te) = truncate_file.set_len(pre_write_size).await {
                                        error!(path = ?path, "Failed to truncate AOF file after partial write: {:?}", te);
                                    }
                                }
                                Err(te) => {
                                    error!(path = ?path, "Failed to open AOF file for truncation: {:?}", te);
                                }
                            }
                            match TokioFile::options()
                                .create(true)
                                .append(true)
                                .open(&path).await {
                                Ok(file) => {
                                    writer = TokioBufWriter::with_capacity(BUFFER_SIZE, file);
                                }
                                Err(reopen_err) => {
                                    error!(path = ?path, "Failed to reopen AOF file after write error: {:?}", reopen_err);
                                    writer_valid = false;
                                }
                            }
                            continue;
                        }

                        // Update the file size
                        *file_size.write().await += total_written;

                        if force_sync {
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
                        if need_fsync && writer_valid {
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
            let mut current_db: usize = 0;
            let mut skip_until_valid_select = false;

            // Create a CommandHandler for full command replay.
            // This routes through the same dispatch as live client commands,
            // ensuring all 400+ commands are properly handled during recovery.
            // Uses new_for_replay to avoid spawning a background cleanup task
            // that would leak when the handler is dropped after AOF load.
            let cmd_handler = CommandHandler::new_for_replay(engine.clone());

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

                // Execute the command (handling SELECT for DB routing)
                tokio::runtime::Handle::current().block_on(async {
                    let cmd_name = String::from_utf8_lossy(&command.name).to_string();
                    let upper_name = cmd_name.to_uppercase();

                    // Handle SELECT to switch DB context
                    if upper_name == "SELECT" {
                        if let Some(arg) = command.args.first()
                            && let Ok(s) = std::str::from_utf8(arg)
                                && let Ok(idx) = s.parse::<usize>() {
                                    if idx >= crate::storage::engine::NUM_DATABASES {
                                        warn!(db_index = idx, "AOF contains SELECT with invalid DB index >= {}, skipping subsequent commands until next valid SELECT", crate::storage::engine::NUM_DATABASES);
                                        skip_until_valid_select = true;
                                    } else {
                                        current_db = idx;
                                        skip_until_valid_select = false;
                                        command_count += 1;
                                    }
                                }
                        return;
                    }

                    // Skip commands if we encountered an invalid SELECT -
                    // we don't know which DB they were intended for
                    if skip_until_valid_select {
                        warn!("Skipping AOF command after invalid SELECT: {}", cmd_name);
                        return;
                    }

                    // Skip commands that don't make sense during AOF replay
                    if is_replay_skip_command(&upper_name) {
                        return;
                    }

                    // Route through the full CommandHandler for complete command coverage
                    let cmd = Command {
                        name: cmd_name,
                        args: command.args,
                    };

                    match cmd_handler.process(cmd, current_db).await {
                        Ok(_) => {
                            command_count += 1;
                        }
                        Err(e) => {
                            warn!("Error during AOF replay for '{}': {:?}", upper_name, e);
                        }
                    }
                });
            }

            // Refresh cached_mem_size for all entries for O(1) memory tracking
            engine.refresh_all_cached_mem_sizes();

            info!(path = ?path, commands = command_count, lines = line_count,
                  "Successfully loaded commands from AOF file");
            Ok(())
        }).await.map_err(|e| PersistenceError::BackgroundTaskFailed(e.to_string()))?
    }

    /// Append a single command to the AOF
    #[allow(dead_code)]
    pub async fn append_command(
        &self,
        command: RespCommand,
        sync_policy: AofSyncPolicy,
    ) -> Result<(), PersistenceError> {
        self.append_commands_batch(vec![command], sync_policy).await
    }

    /// Append a batch of commands to the AOF atomically.
    /// All commands in the batch are written together by the writer task without yielding,
    /// ensuring that e.g. SELECT + write-command pairs cannot be interleaved with
    /// concurrent calls from other connections.
    pub async fn append_commands_batch(
        &self,
        commands: Vec<RespCommand>,
        sync_policy: AofSyncPolicy,
    ) -> Result<(), PersistenceError> {
        // Filter out read-only commands
        let commands: Vec<RespCommand> = commands
            .into_iter()
            .filter(|cmd| !is_read_only_command(&cmd.name))
            .collect();
        if commands.is_empty() {
            return Ok(());
        }

        // Determine if we need to sync based on the policy
        let sync = match sync_policy {
            AofSyncPolicy::Always => true,
            AofSyncPolicy::EverySecond => false,
            AofSyncPolicy::None => false,
        };

        // Create an AOF entry with all commands
        let entry = AofEntry { commands, sync };

        // Send to the writer task
        self.writer_tx.send(entry).await.map_err(|_| {
            PersistenceError::Other("Failed to send command to AOF writer".to_string())
        })?;

        Ok(())
    }

    /// Start background AOF rewrite check
    pub async fn start_background_rewrite_check(
        &self,
        config: &PersistenceConfig,
    ) -> Result<(), PersistenceError> {
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
                    debug!(
                        "AOF file size: {} bytes, checking if rewrite is needed",
                        current_size
                    );

                    // Check if the file has grown enough to warrant a rewrite
                    let last_rewrite = *aof.last_rewrite.read().await;
                    let rewrite_threshold =
                        min_size as u64 + (min_size as u64 * percentage as u64) / 100;

                    if current_size > rewrite_threshold
                        && last_rewrite.elapsed() > Duration::from_secs(3600)
                    {
                        info!(
                            "AOF file size ({} bytes) exceeds threshold ({} bytes), triggering rewrite",
                            current_size, rewrite_threshold
                        );
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

        // Check if rewrite is already in progress before taking the
        // expensive snapshot (which acquires cross_db_lock and clones all data).
        let mut in_progress = self.in_progress.lock().await;
        if *in_progress {
            debug!("AOF rewrite already in progress, skipping");
            return Ok(());
        }
        *in_progress = true;
        drop(in_progress);

        // Get a consistent snapshot of all databases
        let all_dbs = match self.engine.snapshot_all_dbs().await {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to create snapshot for AOF rewrite: {:?}", e);
                // Reset in_progress on failure
                *self.in_progress.lock().await = false;
                return Err(PersistenceError::AofRewriteFailed(e.to_string()));
            }
        };

        // Spawn a blocking task for file operations
        let path = self.path.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<(), PersistenceError> {
            // Create parent directory if it doesn't exist
            ensure_dir_exists(&temp_path)?;

            let file = File::create(&temp_path).map_err(PersistenceError::Io)?;

            let mut writer = BufWriter::new(file);

            // Write each database's data with SELECT commands
            for (db_idx, snapshot) in all_dbs.iter().enumerate() {
                if snapshot.is_empty() {
                    continue; // Skip empty databases
                }

                // Always write SELECT command to establish explicit DB context.
                // This ensures replay correctness regardless of iteration order.
                let select_cmd = RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"SELECT".to_vec())),
                    RespValue::BulkString(Some(db_idx.to_string().into_bytes())),
                ]));
                let serialized = select_cmd
                    .serialize()
                    .map_err(|e| PersistenceError::Serialization(e.to_string()))?;
                writer
                    .write_all(&serialized)
                    .map_err(PersistenceError::Io)?;

                // Write each key using type-appropriate commands
                for (key, item) in snapshot.iter() {
                    let commands = aof_rewrite_commands_for_item(key, item);
                    for args in commands {
                        let resp_cmd = RespValue::Array(Some(
                            args.into_iter()
                                .map(|arg: Vec<u8>| RespValue::BulkString(Some(arg)))
                                .collect(),
                        ));

                        let serialized = resp_cmd
                            .serialize()
                            .map_err(|e| PersistenceError::Serialization(e.to_string()))?;

                        writer
                            .write_all(&serialized)
                            .map_err(PersistenceError::Io)?;
                    }
                }
            }

            // Ensure all data is flushed and synced to disk before rename.
            // flush() only pushes BufWriter's buffer to the OS;
            // sync_all() ensures data reaches persistent storage.
            writer.flush().map_err(PersistenceError::Io)?;
            writer.get_ref().sync_all().map_err(PersistenceError::Io)?;

            // Rename temp file to target file atomically
            let rename_result = fs::rename(&temp_path, &path);

            if rename_result.is_err() {
                error!(
                    "Failed to rename temp file during AOF rewrite: {:?}",
                    rename_result
                );
            }

            rename_result.map_err(|e| PersistenceError::AtomicRenameFailed(e.to_string()))?;

            Ok(())
        })
        .await;

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
                Err(PersistenceError::CorruptedFile(format!(
                    "AOF rewrite task panicked: {}",
                    e
                )))
            }
        };

        // Mark rewrite as complete
        *self.in_progress.lock().await = false;

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
    use crate::storage::value::StoreValue;
    let mut commands: Vec<Vec<Vec<u8>>> = Vec::new();

    match &item.value {
        StoreValue::Str(v) => {
            let mut args = vec![b"SET".to_vec(), key.to_vec(), v.to_vec()];
            if let Some(ttl) = item.ttl()
                && ttl.as_millis() > 0
            {
                args.push(b"PX".to_vec());
                args.push(ttl.as_millis().to_string().into_bytes());
            }
            commands.push(args);
        }
        StoreValue::List(deque) => {
            if !deque.is_empty() {
                // Emit RPUSH key elem1 elem2 ...
                let mut args = vec![b"RPUSH".to_vec(), key.to_vec()];
                args.extend(deque.iter().cloned());
                commands.push(args);
            }
        }
        StoreValue::Set(set) => {
            if !set.is_empty() {
                // Emit SADD key member1 member2 ...
                let mut args = vec![b"SADD".to_vec(), key.to_vec()];
                args.extend(set.iter().cloned());
                commands.push(args);
            }
        }
        StoreValue::ZSet(ss) => {
            if !ss.is_empty() {
                let mut args = vec![b"ZADD".to_vec(), key.to_vec()];
                for (member, score) in &ss.scores {
                    args.push(score.to_string().into_bytes());
                    args.push(member.clone());
                }
                commands.push(args);
            }
        }
        StoreValue::Hash(hash) => {
            if !hash.is_empty() {
                let mut args = vec![b"HSET".to_vec(), key.to_vec()];
                for (field, value) in hash.iter() {
                    args.push(field.clone());
                    args.push(value.clone());
                }
                commands.push(args);
            }
        }
        StoreValue::Stream(stream) => {
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

            // Emit XGROUP CREATE for each consumer group
            for group in stream.groups.values() {
                commands.push(vec![
                    b"XGROUP".to_vec(),
                    b"CREATE".to_vec(),
                    key.to_vec(),
                    group.name.as_bytes().to_vec(),
                    group.last_delivered_id.to_string().into_bytes(),
                    b"MKSTREAM".to_vec(),
                ]);

                // Emit XCLAIM with FORCE for each pending entry to restore consumer PEL state.
                // FORCE is needed because entries are not yet in the PEL after XGROUP CREATE.
                for pending in group.pel.values() {
                    commands.push(vec![
                        b"XCLAIM".to_vec(),
                        key.to_vec(),
                        group.name.as_bytes().to_vec(),
                        pending.consumer.as_bytes().to_vec(),
                        b"0".to_vec(),
                        pending.id.to_string().into_bytes(),
                        b"FORCE".to_vec(),
                    ]);
                }
            }
        }
    }

    // For non-string types, append a PEXPIRE command if TTL is set
    if item.data_type() != RedisDataType::String
        && let Some(ttl) = item.ttl()
        && ttl.as_millis() > 0
    {
        commands.push(vec![
            b"PEXPIRE".to_vec(),
            key.to_vec(),
            ttl.as_millis().to_string().into_bytes(),
        ]);
    }

    commands
}

/// Helper function to check if a command is read-only
/// Commands that should be skipped during AOF replay.
/// These include transaction markers (individual commands within transactions
/// are already logged separately), blocking commands (which would deadlock
/// during replay), and server/connection/replication commands that don't
/// contribute to data state reconstruction.
fn is_replay_skip_command(upper_name: &str) -> bool {
    matches!(
        upper_name,
        // Transaction markers — commands within the transaction are logged individually
        "MULTI" | "EXEC" | "DISCARD" |
        // Note: blocking commands (BLPOP, BRPOP, etc.) are converted to non-blocking
        // equivalents (LPOP, RPOP, etc.) at the AOF logging point in server.rs,
        // so they never appear in the AOF as blocking variants.
        //
        // Pub/Sub — no subscribers exist during replay
        "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" |
        "SSUBSCRIBE" | "SUNSUBSCRIBE" | "PUBLISH" | "SPUBLISH" |
        // Persistence commands — must not trigger saves during replay
        "SAVE" | "BGSAVE" | "BGREWRITEAOF" | "SHUTDOWN" |
        // Monitoring/debug — not data-modifying
        "MONITOR" | "DEBUG" | "SLOWLOG" | "LATENCY" |
        // Connection — no client context during replay
        "AUTH" | "HELLO" | "QUIT" | "RESET" | "CLIENT" | "COMMAND" |
        // Replication wait — no replicas during replay
        "WAIT" | "WAITAOF" |
        // Cluster/replication/sentinel — server-level, not data operations
        "CLUSTER" | "SENTINEL" | "REPLICAOF" | "SLAVEOF" |
        "REPLCONF" | "PSYNC" | "FAILOVER" |
        "ASKING" | "READONLY" | "READWRITE" |
        // Module/ACL — handled at server level
        "MODULE" | "ACL"
    )
}

fn is_read_only_command(command: &[u8]) -> bool {
    let upper: Vec<u8> = command.iter().map(|b| b.to_ascii_uppercase()).collect();
    matches!(
        upper.as_slice(),
        b"GET"
            | b"EXISTS"
            | b"TTL"
            | b"PTTL"
            | b"PING"
            | b"KEYS"
            | b"INFO"
            | b"TYPE"
            | b"MGET"
            | b"STRLEN"
            | b"LLEN"
            | b"HLEN"
            | b"SCARD"
            | b"ZCARD"
            | b"HGET"
            | b"HGETALL"
            | b"HKEYS"
            | b"HVALS"
            | b"SMEMBERS"
            | b"SISMEMBER"
            | b"LRANGE"
            | b"ZRANGE"
            | b"ZSCORE"
            | b"DBSIZE"
            | b"RANDOMKEY"
            | b"SCAN"
            | b"HSCAN"
            | b"SSCAN"
            | b"ZSCAN"
            | b"OBJECT"
            | b"TIME"
            | b"COMMAND"
            | b"CLIENT"
            | b"LPOS"
            | b"SRANDMEMBER"
            | b"ZRANGEBYSCORE"
            | b"ZRANGEBYLEX"
            | b"ZREVRANGE"
            | b"ZREVRANGEBYSCORE"
            | b"ZREVRANGEBYLEX"
            | b"ZRANK"
            | b"ZREVRANK"
            | b"ZCOUNT"
            | b"ZLEXCOUNT"
            | b"GEODIST"
            | b"GEOPOS"
            | b"GEOHASH"
            | b"GEOSEARCH"
            | b"XRANGE"
            | b"XREVRANGE"
            | b"XLEN"
            | b"XINFO"
            | b"XREAD"
            | b"XPENDING"
            | b"BITCOUNT"
            | b"BITPOS"
            | b"GETBIT"
            | b"SUBSTR"
            | b"GETRANGE"
            | b"LINDEX"
            | b"HEXISTS"
            | b"SMISMEMBER"
            | b"PFCOUNT"
            | b"ECHO"
            | b"WAIT"
            | b"DEBUG"
            | b"SLOWLOG"
            | b"MEMORY"
            | b"LATENCY"
            | b"MODULE"
            | b"CLUSTER"
            | b"SENTINEL"
            | b"SUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"PUBSUB"
            | b"AUTH"
            | b"HELLO"
            | b"RESET"
            | b"QUIT"
    )
}

/// Helper function to ensure a directory exists
fn ensure_dir_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.exists()
    {
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
                    self.buffer[1..self.buffer.len() - 2].to_string(),
                ))),
                Some('-') => Ok(Some(RespValue::Error(
                    self.buffer[1..self.buffer.len() - 2].to_string(),
                ))),
                Some(':') => {
                    let num = self.buffer[1..self.buffer.len() - 2]
                        .parse::<i64>()
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "Invalid integer")
                        })?;
                    Ok(Some(RespValue::Integer(num)))
                }
                Some('$') => {
                    let size = self.buffer[1..self.buffer.len() - 2]
                        .parse::<i64>()
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk string size")
                        })?;

                    if size < 0 {
                        return Ok(Some(RespValue::BulkString(None)));
                    }

                    // Redis proto-max-bulk-len default is 512MB
                    if size > 512 * 1024 * 1024 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Bulk string size {} exceeds maximum allowed (512MB)", size),
                        ));
                    }

                    let mut bulk = vec![0; size as usize];
                    self.reader.read_exact(&mut bulk)?;

                    // Read and discard the CRLF
                    self.buffer.clear();
                    self.reader.read_line(&mut self.buffer)?;

                    Ok(Some(RespValue::BulkString(Some(bulk))))
                }
                Some('*') => {
                    let count = self.buffer[1..self.buffer.len() - 2]
                        .parse::<i64>()
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "Invalid array size")
                        })?;

                    if count < 0 {
                        return Ok(Some(RespValue::Array(None)));
                    }

                    // Sanity limit: Redis commands rarely exceed a few thousand elements
                    if count > 1_048_576 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Array size {} exceeds maximum allowed (1048576)", count),
                        ));
                    }

                    let mut array = Vec::with_capacity(count as usize);
                    for _ in 0..count {
                        if let Some(value) = self.next()? {
                            array.push(value);
                        } else {
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "Unexpected EOF",
                            ));
                        }
                    }

                    Ok(Some(RespValue::Array(Some(array))))
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid RESP data type",
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::types::sorted_set::SortedSetData;
    use crate::storage::engine::StorageEngine;
    use crate::storage::value::{NativeHashMap, NativeHashSet, StoreValue};
    use std::collections::VecDeque;

    fn create_test_engine() -> Arc<StorageEngine> {
        StorageEngine::new(Default::default())
    }

    fn make_item(value: StoreValue) -> StorageItem {
        StorageItem::new(value)
    }

    #[test]
    fn test_aof_rewrite_string() {
        let key = b"mykey".to_vec();
        let item = make_item(StoreValue::Str(b"myval".to_vec().into()));
        let cmds = aof_rewrite_commands_for_item(&key, &item);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0][0], b"SET");
        assert_eq!(cmds[0][1], b"mykey");
        assert_eq!(cmds[0][2], b"myval");
    }

    #[test]
    fn test_aof_rewrite_list() {
        let key = b"mylist".to_vec();
        let list = VecDeque::from(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let item = make_item(StoreValue::List(list));
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
        let mut set = NativeHashSet::default();
        set.insert(b"x".to_vec());
        set.insert(b"y".to_vec());
        let item = make_item(StoreValue::Set(set));
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
        let item = make_item(StoreValue::ZSet(zset));
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
        let mut hash = NativeHashMap::default();
        hash.insert(b"f1".to_vec(), b"v1".to_vec());
        hash.insert(b"f2".to_vec(), b"v2".to_vec());
        let item = make_item(StoreValue::Hash(hash));
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
        stream.entries.insert(entry.id, entry);
        stream.last_id = crate::storage::stream::StreamEntryId::new(1000, 0);
        let item = make_item(StoreValue::Stream(stream));
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
        let item = engine.get_item(b"mylist").await.unwrap().unwrap();
        if let StoreValue::List(deque) = &item.value {
            let list: Vec<Vec<u8>> = deque.iter().cloned().collect();
            assert_eq!(list, vec![b"a".to_vec(), b"b".to_vec()]);
        } else {
            panic!("Expected List StoreValue");
        }
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
        let item = engine.get_item(b"myset").await.unwrap().unwrap();
        if let StoreValue::Set(set) = &item.value {
            assert!(set.contains(&b"x".to_vec()));
            assert!(set.contains(&b"y".to_vec()));
        } else {
            panic!("Expected Set StoreValue");
        }
        assert_eq!(engine.get_type(b"myset").await.unwrap(), "set");
    }

    #[tokio::test]
    async fn test_aof_replay_zadd() {
        let engine = create_test_engine();
        let cmd = RespCommand {
            name: b"ZADD".to_vec(),
            args: vec![
                b"myzset".to_vec(),
                b"1.5".to_vec(),
                b"alice".to_vec(),
                b"2.0".to_vec(),
                b"bob".to_vec(),
            ],
        };
        engine.process_command(&cmd).await.unwrap();
        let item = engine.get_item(b"myzset").await.unwrap().unwrap();
        if let StoreValue::ZSet(zset) = &item.value {
            assert_eq!(zset.len(), 2);
            assert_eq!(zset.scores.get(&b"alice".to_vec()), Some(&1.5));
            assert_eq!(zset.scores.get(&b"bob".to_vec()), Some(&2.0));
        } else {
            panic!("Expected ZSet StoreValue");
        }
        assert_eq!(engine.get_type(b"myzset").await.unwrap(), "zset");
    }

    #[tokio::test]
    async fn test_aof_replay_hset() {
        let engine = create_test_engine();
        let cmd = RespCommand {
            name: b"HSET".to_vec(),
            args: vec![
                b"myhash".to_vec(),
                b"f1".to_vec(),
                b"v1".to_vec(),
                b"f2".to_vec(),
                b"v2".to_vec(),
            ],
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
            args: vec![
                b"mystream".to_vec(),
                b"1000-0".to_vec(),
                b"temp".to_vec(),
                b"25".to_vec(),
            ],
        };
        engine.process_command(&cmd).await.unwrap();
        let item = engine.get_item(b"mystream").await.unwrap().unwrap();
        if let StoreValue::Stream(stream) = &item.value {
            assert_eq!(stream.entries.len(), 1);
        } else {
            panic!("Expected Stream StoreValue");
        }
        assert_eq!(engine.get_type(b"mystream").await.unwrap(), "stream");
    }

    #[tokio::test]
    async fn test_aof_replay_set_with_px() {
        let engine = create_test_engine();
        let cmd = RespCommand {
            name: b"SET".to_vec(),
            args: vec![
                b"k".to_vec(),
                b"v".to_vec(),
                b"PX".to_vec(),
                b"60000".to_vec(),
            ],
        };
        engine.process_command(&cmd).await.unwrap();
        let val = engine.get(b"k").await.unwrap();
        assert_eq!(val, Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn test_aof_round_trip_all_types() {
        let engine = create_test_engine();

        // String
        let str_item = make_item(StoreValue::Str(b"hello".to_vec().into()));
        for args in aof_rewrite_commands_for_item(b"str", &str_item) {
            engine
                .process_command(&RespCommand {
                    name: args[0].clone(),
                    args: args[1..].to_vec(),
                })
                .await
                .unwrap();
        }

        // List
        let list = VecDeque::from(vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
        let list_item = make_item(StoreValue::List(list));
        for args in aof_rewrite_commands_for_item(b"lst", &list_item) {
            engine
                .process_command(&RespCommand {
                    name: args[0].clone(),
                    args: args[1..].to_vec(),
                })
                .await
                .unwrap();
        }

        // Set
        let mut set = NativeHashSet::default();
        set.insert(b"a".to_vec());
        set.insert(b"b".to_vec());
        let set_item = make_item(StoreValue::Set(set));
        for args in aof_rewrite_commands_for_item(b"st", &set_item) {
            engine
                .process_command(&RespCommand {
                    name: args[0].clone(),
                    args: args[1..].to_vec(),
                })
                .await
                .unwrap();
        }

        // Hash
        let mut hash = NativeHashMap::default();
        hash.insert(b"f".to_vec(), b"v".to_vec());
        let hash_item = make_item(StoreValue::Hash(hash));
        for args in aof_rewrite_commands_for_item(b"hs", &hash_item) {
            engine
                .process_command(&RespCommand {
                    name: args[0].clone(),
                    args: args[1..].to_vec(),
                })
                .await
                .unwrap();
        }

        // Sorted set
        let mut zset = SortedSetData::new();
        zset.insert(1.0, b"m1".to_vec());
        zset.insert(2.0, b"m2".to_vec());
        let zset_item = make_item(StoreValue::ZSet(zset));
        for args in aof_rewrite_commands_for_item(b"zs", &zset_item) {
            engine
                .process_command(&RespCommand {
                    name: args[0].clone(),
                    args: args[1..].to_vec(),
                })
                .await
                .unwrap();
        }

        // Verify all restored correctly
        assert_eq!(engine.get(b"str").await.unwrap(), Some(b"hello".to_vec()));
        assert_eq!(engine.get_type(b"str").await.unwrap(), "string");

        let list_item = engine.get_item(b"lst").await.unwrap().unwrap();
        if let StoreValue::List(deque) = &list_item.value {
            let loaded_list: Vec<Vec<u8>> = deque.iter().cloned().collect();
            assert_eq!(
                loaded_list,
                vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]
            );
        } else {
            panic!("Expected List StoreValue");
        }

        let set_item = engine.get_item(b"st").await.unwrap().unwrap();
        if let StoreValue::Set(loaded_set) = &set_item.value {
            assert!(loaded_set.contains(&b"a".to_vec()));
        } else {
            panic!("Expected Set StoreValue");
        }

        let hash_fields = engine.hash_getall(b"hs").unwrap();
        assert_eq!(hash_fields.len(), 1);

        let zset_item = engine.get_item(b"zs").await.unwrap().unwrap();
        if let StoreValue::ZSet(loaded_zset) = &zset_item.value {
            assert_eq!(loaded_zset.len(), 2);
        } else {
            panic!("Expected ZSet StoreValue");
        }
    }

    #[tokio::test]
    async fn test_aof_batch_write_produces_valid_resp() {
        // Test that a batch of commands is written as complete, valid RESP
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine, aof_path.clone());

        // Write a batch of 3 SET commands
        let commands = vec![
            RespCommand {
                name: b"SET".to_vec(),
                args: vec![b"key1".to_vec(), b"val1".to_vec()],
            },
            RespCommand {
                name: b"SET".to_vec(),
                args: vec![b"key2".to_vec(), b"val2".to_vec()],
            },
            RespCommand {
                name: b"SET".to_vec(),
                args: vec![b"key3".to_vec(), b"val3".to_vec()],
            },
        ];
        aof.append_commands_batch(commands, AofSyncPolicy::Always)
            .await
            .unwrap();

        // Give the writer task time to process and flush
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Read the AOF file and verify it contains valid RESP
        let content = std::fs::read(&aof_path).unwrap();
        assert!(!content.is_empty(), "AOF file should not be empty");

        // Parse the file as RESP - each command should be a complete RESP array
        let file = File::open(&aof_path).unwrap();
        let reader = BufReader::new(file);
        let mut parser = resp_parser::RespParser::new(reader);

        let mut cmd_count = 0;
        while let Ok(Some(value)) = parser.next() {
            // Each entry should be an Array
            if let RespValue::Array(Some(parts)) = value {
                assert!(
                    parts.len() >= 2,
                    "Each command should have name + at least 1 arg"
                );
                cmd_count += 1;
            } else {
                panic!("Expected RESP Array, got something else");
            }
        }
        assert_eq!(cmd_count, 3, "All 3 commands should be in the AOF file");
    }

    #[tokio::test]
    async fn test_aof_batch_atomicity_multiple_batches() {
        // Test that multiple batches are independently atomic
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test_multi.aof");
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine, aof_path.clone());

        // Write two separate batches
        let batch1 = vec![
            RespCommand {
                name: b"SET".to_vec(),
                args: vec![b"a".to_vec(), b"1".to_vec()],
            },
            RespCommand {
                name: b"SET".to_vec(),
                args: vec![b"b".to_vec(), b"2".to_vec()],
            },
        ];
        let batch2 = vec![RespCommand {
            name: b"SET".to_vec(),
            args: vec![b"c".to_vec(), b"3".to_vec()],
        }];
        aof.append_commands_batch(batch1, AofSyncPolicy::Always)
            .await
            .unwrap();
        aof.append_commands_batch(batch2, AofSyncPolicy::Always)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Parse and verify all 3 commands are present as valid RESP
        let file = File::open(&aof_path).unwrap();
        let reader = BufReader::new(file);
        let mut parser = resp_parser::RespParser::new(reader);

        let mut cmd_count = 0;
        while let Ok(Some(_)) = parser.next() {
            cmd_count += 1;
        }
        assert_eq!(
            cmd_count, 3,
            "All commands from both batches should be in the AOF"
        );
    }

    #[tokio::test]
    async fn test_aof_batch_reload_integrity() {
        // Test that data written through AOF batch writer can be replayed correctly
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test_reload.aof");
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine, aof_path.clone());

        // Write a batch with various commands
        let commands = vec![
            RespCommand {
                name: b"SET".to_vec(),
                args: vec![b"mykey".to_vec(), b"myvalue".to_vec()],
            },
            RespCommand {
                name: b"SET".to_vec(),
                args: vec![b"counter".to_vec(), b"42".to_vec()],
            },
        ];
        aof.append_commands_batch(commands, AofSyncPolicy::Always)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create a fresh engine and reload from the AOF
        let engine2 = create_test_engine();
        let aof2 = AofPersistence::new(engine2.clone(), aof_path);
        aof2.load().await.unwrap();

        // Verify the data was correctly restored
        assert_eq!(
            engine2.get(b"mykey").await.unwrap(),
            Some(b"myvalue".to_vec())
        );
        assert_eq!(engine2.get(b"counter").await.unwrap(), Some(b"42".to_vec()));
    }

    /// Write a list of RespCommands directly to a file in RESP format,
    /// then load them via `AofPersistence::load()` which uses CommandHandler.
    fn write_resp_commands_to_file(path: &std::path::Path, commands: &[RespCommand]) {
        use std::io::Write;
        let mut file = File::create(path).unwrap();
        for cmd in commands {
            // Write RESP array: *N\r\n then each element as $len\r\ndata\r\n
            let total = 1 + cmd.args.len();
            write!(file, "*{}\r\n", total).unwrap();
            // Command name
            write!(file, "${}\r\n", cmd.name.len()).unwrap();
            file.write_all(&cmd.name).unwrap();
            write!(file, "\r\n").unwrap();
            // Arguments
            for arg in &cmd.args {
                write!(file, "${}\r\n", arg.len()).unwrap();
                file.write_all(arg).unwrap();
                write!(file, "\r\n").unwrap();
            }
        }
        file.flush().unwrap();
    }

    #[tokio::test]
    async fn test_aof_full_replay_lpush() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[RespCommand {
                name: b"LPUSH".to_vec(),
                args: vec![
                    b"mylist".to_vec(),
                    b"c".to_vec(),
                    b"b".to_vec(),
                    b"a".to_vec(),
                ],
            }],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        // LPUSH pushes left, so order is a, b, c from left to right
        let item = engine.get_item(b"mylist").await.unwrap().unwrap();
        if let StoreValue::List(deque) = &item.value {
            let list: Vec<Vec<u8>> = deque.iter().cloned().collect();
            assert_eq!(list, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        } else {
            panic!("Expected List StoreValue");
        }
    }

    #[tokio::test]
    async fn test_aof_full_replay_srem() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[
                RespCommand {
                    name: b"SADD".to_vec(),
                    args: vec![
                        b"myset".to_vec(),
                        b"a".to_vec(),
                        b"b".to_vec(),
                        b"c".to_vec(),
                    ],
                },
                RespCommand {
                    name: b"SREM".to_vec(),
                    args: vec![b"myset".to_vec(), b"b".to_vec()],
                },
            ],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        let item = engine.get_item(b"myset").await.unwrap().unwrap();
        if let StoreValue::Set(set) = &item.value {
            assert!(set.contains(&b"a".to_vec()));
            assert!(!set.contains(&b"b".to_vec()));
            assert!(set.contains(&b"c".to_vec()));
        } else {
            panic!("Expected Set StoreValue");
        }
    }

    #[tokio::test]
    async fn test_aof_full_replay_hdel() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[
                RespCommand {
                    name: b"HSET".to_vec(),
                    args: vec![
                        b"myhash".to_vec(),
                        b"f1".to_vec(),
                        b"v1".to_vec(),
                        b"f2".to_vec(),
                        b"v2".to_vec(),
                        b"f3".to_vec(),
                        b"v3".to_vec(),
                    ],
                },
                RespCommand {
                    name: b"HDEL".to_vec(),
                    args: vec![b"myhash".to_vec(), b"f2".to_vec()],
                },
            ],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        let fields = engine.hash_getall(b"myhash").unwrap();
        assert_eq!(fields.len(), 2);
        assert!(fields.iter().any(|(k, v)| k == b"f1" && v == b"v1"));
        assert!(fields.iter().any(|(k, v)| k == b"f3" && v == b"v3"));
        assert!(!fields.iter().any(|(k, _)| k == b"f2"));
    }

    #[tokio::test]
    async fn test_aof_full_replay_zincrby() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[
                RespCommand {
                    name: b"ZADD".to_vec(),
                    args: vec![b"myzset".to_vec(), b"1.0".to_vec(), b"alice".to_vec()],
                },
                RespCommand {
                    name: b"ZINCRBY".to_vec(),
                    args: vec![b"myzset".to_vec(), b"2.5".to_vec(), b"alice".to_vec()],
                },
            ],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        let item = engine.get_item(b"myzset").await.unwrap().unwrap();
        if let StoreValue::ZSet(zset) = &item.value {
            assert_eq!(zset.scores.get(&b"alice".to_vec()), Some(&3.5));
        } else {
            panic!("Expected ZSet StoreValue");
        }
    }

    #[tokio::test]
    async fn test_aof_full_replay_geoadd() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[RespCommand {
                name: b"GEOADD".to_vec(),
                args: vec![
                    b"mygeo".to_vec(),
                    b"13.361389".to_vec(),
                    b"38.115556".to_vec(),
                    b"Palermo".to_vec(),
                    b"15.087269".to_vec(),
                    b"37.502669".to_vec(),
                    b"Catania".to_vec(),
                ],
            }],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        // Geo data is stored as a sorted set
        let item = engine.get_item(b"mygeo").await.unwrap();
        assert!(item.is_some(), "GEOADD should have created the geo key");
        assert_eq!(engine.get_type(b"mygeo").await.unwrap(), "zset");
    }

    #[tokio::test]
    async fn test_aof_full_replay_pfadd() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[RespCommand {
                name: b"PFADD".to_vec(),
                args: vec![
                    b"myhll".to_vec(),
                    b"a".to_vec(),
                    b"b".to_vec(),
                    b"c".to_vec(),
                ],
            }],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        let val = engine.get(b"myhll").await.unwrap();
        assert!(val.is_some(), "PFADD should have created the HLL key");
    }

    #[tokio::test]
    async fn test_aof_full_replay_setbit() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[
                RespCommand {
                    name: b"SETBIT".to_vec(),
                    args: vec![b"mybits".to_vec(), b"7".to_vec(), b"1".to_vec()],
                },
                RespCommand {
                    name: b"SETBIT".to_vec(),
                    args: vec![b"mybits".to_vec(), b"0".to_vec(), b"1".to_vec()],
                },
            ],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        let val = engine.get(b"mybits").await.unwrap();
        assert!(val.is_some(), "SETBIT should have created the bitmap key");
    }

    #[tokio::test]
    async fn test_aof_full_replay_xgroup() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[
                RespCommand {
                    name: b"XADD".to_vec(),
                    args: vec![
                        b"mystream".to_vec(),
                        b"1-0".to_vec(),
                        b"field".to_vec(),
                        b"value".to_vec(),
                    ],
                },
                RespCommand {
                    name: b"XGROUP".to_vec(),
                    args: vec![
                        b"CREATE".to_vec(),
                        b"mystream".to_vec(),
                        b"mygroup".to_vec(),
                        b"0".to_vec(),
                    ],
                },
            ],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        // Verify the stream exists with the consumer group
        let item = engine.get_item(b"mystream").await.unwrap();
        assert!(item.is_some(), "XADD should have created the stream");
        if let StoreValue::Stream(stream) = &item.unwrap().value {
            assert!(
                stream.groups.contains_key("mygroup"),
                "XGROUP CREATE should have created the consumer group"
            );
        } else {
            panic!("Expected Stream StoreValue");
        }
    }

    #[tokio::test]
    async fn test_aof_full_replay_append() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"mykey".to_vec(), b"Hello".to_vec()],
                },
                RespCommand {
                    name: b"APPEND".to_vec(),
                    args: vec![b"mykey".to_vec(), b" World".to_vec()],
                },
            ],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        assert_eq!(
            engine.get(b"mykey").await.unwrap(),
            Some(b"Hello World".to_vec())
        );
    }

    #[tokio::test]
    async fn test_aof_full_replay_incr() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"counter".to_vec(), b"10".to_vec()],
                },
                RespCommand {
                    name: b"INCR".to_vec(),
                    args: vec![b"counter".to_vec()],
                },
                RespCommand {
                    name: b"INCRBY".to_vec(),
                    args: vec![b"counter".to_vec(), b"5".to_vec()],
                },
                RespCommand {
                    name: b"DECR".to_vec(),
                    args: vec![b"counter".to_vec()],
                },
            ],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        // 10 + 1 + 5 - 1 = 15
        assert_eq!(engine.get(b"counter").await.unwrap(), Some(b"15".to_vec()));
    }

    #[tokio::test]
    async fn test_aof_full_replay_rename() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"oldkey".to_vec(), b"myvalue".to_vec()],
                },
                RespCommand {
                    name: b"RENAME".to_vec(),
                    args: vec![b"oldkey".to_vec(), b"newkey".to_vec()],
                },
            ],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        assert_eq!(engine.get(b"oldkey").await.unwrap(), None);
        assert_eq!(
            engine.get(b"newkey").await.unwrap(),
            Some(b"myvalue".to_vec())
        );
    }

    #[tokio::test]
    async fn test_aof_full_replay_mixed_commands() {
        // Comprehensive test: replay a sequence of diverse commands through CommandHandler
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[
                // String operations
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"str".to_vec(), b"hello".to_vec()],
                },
                RespCommand {
                    name: b"APPEND".to_vec(),
                    args: vec![b"str".to_vec(), b" world".to_vec()],
                },
                // List operations
                RespCommand {
                    name: b"RPUSH".to_vec(),
                    args: vec![b"list".to_vec(), b"1".to_vec(), b"2".to_vec()],
                },
                RespCommand {
                    name: b"LPUSH".to_vec(),
                    args: vec![b"list".to_vec(), b"0".to_vec()],
                },
                // Set operations
                RespCommand {
                    name: b"SADD".to_vec(),
                    args: vec![b"set".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
                },
                RespCommand {
                    name: b"SREM".to_vec(),
                    args: vec![b"set".to_vec(), b"b".to_vec()],
                },
                // Hash operations
                RespCommand {
                    name: b"HSET".to_vec(),
                    args: vec![b"hash".to_vec(), b"f1".to_vec(), b"v1".to_vec()],
                },
                RespCommand {
                    name: b"HDEL".to_vec(),
                    args: vec![b"hash".to_vec(), b"f1".to_vec()],
                },
                RespCommand {
                    name: b"HSET".to_vec(),
                    args: vec![b"hash".to_vec(), b"f2".to_vec(), b"v2".to_vec()],
                },
                // Sorted set operations
                RespCommand {
                    name: b"ZADD".to_vec(),
                    args: vec![
                        b"zset".to_vec(),
                        b"1.0".to_vec(),
                        b"x".to_vec(),
                        b"2.0".to_vec(),
                        b"y".to_vec(),
                    ],
                },
                RespCommand {
                    name: b"ZINCRBY".to_vec(),
                    args: vec![b"zset".to_vec(), b"3.0".to_vec(), b"x".to_vec()],
                },
                // Counter
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"cnt".to_vec(), b"0".to_vec()],
                },
                RespCommand {
                    name: b"INCR".to_vec(),
                    args: vec![b"cnt".to_vec()],
                },
                RespCommand {
                    name: b"INCR".to_vec(),
                    args: vec![b"cnt".to_vec()],
                },
                // Bitmap
                RespCommand {
                    name: b"SETBIT".to_vec(),
                    args: vec![b"bits".to_vec(), b"7".to_vec(), b"1".to_vec()],
                },
                // HyperLogLog
                RespCommand {
                    name: b"PFADD".to_vec(),
                    args: vec![b"hll".to_vec(), b"x".to_vec(), b"y".to_vec()],
                },
                // Key management
                RespCommand {
                    name: b"RENAME".to_vec(),
                    args: vec![b"str".to_vec(), b"str2".to_vec()],
                },
            ],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();

        // Verify string (renamed)
        assert_eq!(engine.get(b"str").await.unwrap(), None);
        assert_eq!(
            engine.get(b"str2").await.unwrap(),
            Some(b"hello world".to_vec())
        );

        // Verify list: RPUSH [1,2] then LPUSH [0] -> [0,1,2]
        let list_item = engine.get_item(b"list").await.unwrap().unwrap();
        if let StoreValue::List(deque) = &list_item.value {
            let list: Vec<Vec<u8>> = deque.iter().cloned().collect();
            assert_eq!(list, vec![b"0".to_vec(), b"1".to_vec(), b"2".to_vec()]);
        } else {
            panic!("Expected List StoreValue");
        }

        // Verify set: {a, c} (b removed)
        let set_item = engine.get_item(b"set").await.unwrap().unwrap();
        if let StoreValue::Set(set) = &set_item.value {
            assert_eq!(set.len(), 2);
            assert!(set.contains(&b"a".to_vec()));
            assert!(set.contains(&b"c".to_vec()));
        } else {
            panic!("Expected Set StoreValue");
        }

        // Verify hash: {f2: v2} (f1 deleted)
        let fields = engine.hash_getall(b"hash").unwrap();
        assert_eq!(fields.len(), 1);
        assert!(fields.iter().any(|(k, v)| k == b"f2" && v == b"v2"));

        // Verify sorted set: x=4.0 (1.0 + 3.0), y=2.0
        let zset_item = engine.get_item(b"zset").await.unwrap().unwrap();
        if let StoreValue::ZSet(zset) = &zset_item.value {
            assert_eq!(zset.scores.get(&b"x".to_vec()), Some(&4.0));
            assert_eq!(zset.scores.get(&b"y".to_vec()), Some(&2.0));
        } else {
            panic!("Expected ZSet StoreValue");
        }

        // Verify counter: 2
        assert_eq!(engine.get(b"cnt").await.unwrap(), Some(b"2".to_vec()));

        // Verify bitmap exists
        assert!(engine.get(b"bits").await.unwrap().is_some());

        // Verify HLL exists
        assert!(engine.get(b"hll").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_aof_full_replay_skips_transaction_markers() {
        // MULTI/EXEC/DISCARD should be silently skipped
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        write_resp_commands_to_file(
            &aof_path,
            &[
                RespCommand {
                    name: b"MULTI".to_vec(),
                    args: vec![],
                },
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"k1".to_vec(), b"v1".to_vec()],
                },
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"k2".to_vec(), b"v2".to_vec()],
                },
                RespCommand {
                    name: b"EXEC".to_vec(),
                    args: vec![],
                },
            ],
        );
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();
        assert_eq!(engine.get(b"k1").await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get(b"k2").await.unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_aof_rewrite_stream_consumer_groups() {
        use crate::storage::stream::{
            ConsumerGroup, PendingEntry, StreamConsumer, StreamData, StreamEntry, StreamEntryId,
        };
        use std::collections::{BTreeMap, HashMap};

        let key = b"mystream".to_vec();
        let mut stream = StreamData::new();

        // Add two entries
        let entry1 = StreamEntry {
            id: StreamEntryId::new(1000, 0),
            fields: vec![(b"field1".to_vec(), b"value1".to_vec())],
        };
        let entry2 = StreamEntry {
            id: StreamEntryId::new(2000, 0),
            fields: vec![(b"field2".to_vec(), b"value2".to_vec())],
        };
        stream.entries.insert(entry1.id, entry1);
        stream.entries.insert(entry2.id, entry2);
        stream.last_id = StreamEntryId::new(2000, 0);

        // Create a consumer group with pending entries
        let mut pel = BTreeMap::new();
        pel.insert(
            StreamEntryId::new(1000, 0),
            PendingEntry {
                id: StreamEntryId::new(1000, 0),
                consumer: "consumer1".to_string(),
                delivery_time: 12345,
                delivery_count: 1,
            },
        );
        pel.insert(
            StreamEntryId::new(2000, 0),
            PendingEntry {
                id: StreamEntryId::new(2000, 0),
                consumer: "consumer2".to_string(),
                delivery_time: 12346,
                delivery_count: 2,
            },
        );

        let mut consumers = HashMap::new();
        consumers.insert(
            "consumer1".to_string(),
            StreamConsumer {
                name: "consumer1".to_string(),
                seen_time: 12345,
                pending: vec![StreamEntryId::new(1000, 0)],
            },
        );
        consumers.insert(
            "consumer2".to_string(),
            StreamConsumer {
                name: "consumer2".to_string(),
                seen_time: 12346,
                pending: vec![StreamEntryId::new(2000, 0)],
            },
        );

        let group = ConsumerGroup {
            name: "mygroup".to_string(),
            last_delivered_id: StreamEntryId::new(2000, 0),
            consumers,
            pel,
            entries_read: Some(2),
        };
        stream.groups.insert("mygroup".to_string(), group);

        let item = make_item(StoreValue::Stream(stream));
        let cmds = aof_rewrite_commands_for_item(&key, &item);

        // Should have: 2 XADD + 1 XGROUP CREATE + 2 XCLAIM = 5 commands
        assert_eq!(cmds.len(), 5);

        // First two: XADD commands
        assert_eq!(cmds[0][0], b"XADD");
        assert_eq!(cmds[1][0], b"XADD");

        // Third: XGROUP CREATE
        assert_eq!(cmds[2][0], b"XGROUP");
        assert_eq!(cmds[2][1], b"CREATE");
        assert_eq!(cmds[2][2], b"mystream");
        assert_eq!(cmds[2][3], b"mygroup");
        assert_eq!(cmds[2][4], b"2000-0");
        assert_eq!(cmds[2][5], b"MKSTREAM");

        // Fourth and fifth: XCLAIM commands with FORCE (order from BTreeMap iteration)
        assert_eq!(cmds[3][0], b"XCLAIM");
        assert_eq!(cmds[3][1], b"mystream");
        assert_eq!(cmds[3][2], b"mygroup");
        // consumer name for entry 1000-0
        assert_eq!(cmds[3][3], b"consumer1");
        assert_eq!(cmds[3][4], b"0");
        assert_eq!(cmds[3][5], b"1000-0");
        assert_eq!(cmds[3][6], b"FORCE");

        assert_eq!(cmds[4][0], b"XCLAIM");
        assert_eq!(cmds[4][1], b"mystream");
        assert_eq!(cmds[4][2], b"mygroup");
        assert_eq!(cmds[4][3], b"consumer2");
        assert_eq!(cmds[4][4], b"0");
        assert_eq!(cmds[4][5], b"2000-0");
        assert_eq!(cmds[4][6], b"FORCE");
    }

    #[tokio::test]
    async fn test_aof_rewrite_stream_consumer_group_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        // Build AOF with XADD, XGROUP CREATE, XREADGROUP (to create pending entries), then XCLAIM
        write_resp_commands_to_file(
            &aof_path,
            &[
                // Add entries to stream
                RespCommand {
                    name: b"XADD".to_vec(),
                    args: vec![
                        b"mystream".to_vec(),
                        b"1-0".to_vec(),
                        b"field1".to_vec(),
                        b"value1".to_vec(),
                    ],
                },
                RespCommand {
                    name: b"XADD".to_vec(),
                    args: vec![
                        b"mystream".to_vec(),
                        b"2-0".to_vec(),
                        b"field2".to_vec(),
                        b"value2".to_vec(),
                    ],
                },
                RespCommand {
                    name: b"XADD".to_vec(),
                    args: vec![
                        b"mystream".to_vec(),
                        b"3-0".to_vec(),
                        b"field3".to_vec(),
                        b"value3".to_vec(),
                    ],
                },
                // Create consumer group starting at 0
                RespCommand {
                    name: b"XGROUP".to_vec(),
                    args: vec![
                        b"CREATE".to_vec(),
                        b"mystream".to_vec(),
                        b"mygroup".to_vec(),
                        b"0".to_vec(),
                    ],
                },
                // Read entries via consumer group (creates pending entries)
                RespCommand {
                    name: b"XREADGROUP".to_vec(),
                    args: vec![
                        b"GROUP".to_vec(),
                        b"mygroup".to_vec(),
                        b"consumer1".to_vec(),
                        b"COUNT".to_vec(),
                        b"2".to_vec(),
                        b"STREAMS".to_vec(),
                        b"mystream".to_vec(),
                        b">".to_vec(),
                    ],
                },
            ],
        );

        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path.clone());
        aof.load().await.unwrap();

        // Verify initial state: stream with group and pending entries
        let stream_item = engine
            .get_item(b"mystream")
            .await
            .unwrap()
            .expect("stream should exist");
        let stream = if let StoreValue::Stream(s) = &stream_item.value {
            s
        } else {
            panic!("Expected Stream StoreValue")
        };
        assert_eq!(stream.entries.len(), 3, "should have 3 entries");
        assert!(
            stream.groups.contains_key("mygroup"),
            "should have consumer group"
        );
        let group = &stream.groups["mygroup"];
        assert!(
            !group.pel.is_empty(),
            "should have pending entries after XREADGROUP"
        );
        let pending_count = group.pel.len();

        // Now do AOF rewrite: engine → new AOF commands
        let rewrite_path = dir.path().join("rewrite.aof");
        {
            use std::io::Write;
            let mut file = File::create(&rewrite_path).unwrap();
            // Snapshot DB 0 and generate rewrite commands
            let snapshot = engine.snapshot_db(0).await.unwrap();
            for (key, item) in snapshot.iter() {
                let cmds = aof_rewrite_commands_for_item(key, item);
                for cmd in cmds {
                    let total = cmd.len();
                    write!(file, "*{}\r\n", total).unwrap();
                    for arg in &cmd {
                        write!(file, "${}\r\n", arg.len()).unwrap();
                        file.write_all(arg).unwrap();
                        write!(file, "\r\n").unwrap();
                    }
                }
            }
            file.flush().unwrap();
        }

        // Load rewritten AOF into a fresh engine
        let engine2 = create_test_engine();
        let aof2 = AofPersistence::new(engine2.clone(), rewrite_path);
        aof2.load().await.unwrap();

        // Verify the consumer group state survived the round-trip
        let stream_item2 = engine2
            .get_item(b"mystream")
            .await
            .unwrap()
            .expect("stream should exist after rewrite reload");
        let stream2 = if let StoreValue::Stream(s) = &stream_item2.value {
            s
        } else {
            panic!("Expected Stream StoreValue")
        };
        assert_eq!(stream2.entries.len(), 3, "entries should survive rewrite");
        assert!(
            stream2.groups.contains_key("mygroup"),
            "consumer group should survive rewrite"
        );
        let group2 = &stream2.groups["mygroup"];
        assert_eq!(
            group2.pel.len(),
            pending_count,
            "pending entries should survive rewrite"
        );
        // Verify consumer exists
        assert!(
            group2.consumers.contains_key("consumer1"),
            "consumer should be recreated via XCLAIM"
        );
    }

    #[tokio::test]
    async fn test_aof_xreadgroup_persistence_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        // Write AOF with XADD entries, XGROUP CREATE, and XREADGROUP to create consumer state
        write_resp_commands_to_file(
            &aof_path,
            &[
                // Add entries
                RespCommand {
                    name: b"XADD".to_vec(),
                    args: vec![
                        b"events".to_vec(),
                        b"1-0".to_vec(),
                        b"type".to_vec(),
                        b"login".to_vec(),
                    ],
                },
                RespCommand {
                    name: b"XADD".to_vec(),
                    args: vec![
                        b"events".to_vec(),
                        b"2-0".to_vec(),
                        b"type".to_vec(),
                        b"logout".to_vec(),
                    ],
                },
                RespCommand {
                    name: b"XADD".to_vec(),
                    args: vec![
                        b"events".to_vec(),
                        b"3-0".to_vec(),
                        b"type".to_vec(),
                        b"purchase".to_vec(),
                    ],
                },
                // Create consumer group
                RespCommand {
                    name: b"XGROUP".to_vec(),
                    args: vec![
                        b"CREATE".to_vec(),
                        b"events".to_vec(),
                        b"processors".to_vec(),
                        b"0".to_vec(),
                    ],
                },
                // XREADGROUP: consumer reads entries, creating PEL entries
                RespCommand {
                    name: b"XREADGROUP".to_vec(),
                    args: vec![
                        b"GROUP".to_vec(),
                        b"processors".to_vec(),
                        b"worker1".to_vec(),
                        b"COUNT".to_vec(),
                        b"2".to_vec(),
                        b"STREAMS".to_vec(),
                        b"events".to_vec(),
                        b">".to_vec(),
                    ],
                },
            ],
        );

        // Load AOF into first engine
        let engine1 = create_test_engine();
        let aof1 = AofPersistence::new(engine1.clone(), aof_path.clone());
        aof1.load().await.unwrap();

        // Verify consumer state after initial load
        let item1 = engine1
            .get_item(b"events")
            .await
            .unwrap()
            .expect("stream should exist");
        let stream1 = if let StoreValue::Stream(s) = &item1.value {
            s
        } else {
            panic!("Expected Stream StoreValue")
        };
        assert_eq!(stream1.entries.len(), 3, "should have 3 stream entries");
        let group1 = &stream1.groups["processors"];
        assert_eq!(
            group1.pel.len(),
            2,
            "should have 2 pending entries from XREADGROUP"
        );
        assert!(
            group1.consumers.contains_key("worker1"),
            "worker1 consumer should exist"
        );
        let worker1 = &group1.consumers["worker1"];
        assert_eq!(
            worker1.pending.len(),
            2,
            "worker1 should have 2 pending IDs"
        );

        // Now reload from the same AOF into a fresh engine to verify persistence
        let engine2 = create_test_engine();
        let aof2 = AofPersistence::new(engine2.clone(), aof_path);
        aof2.load().await.unwrap();

        let item2 = engine2
            .get_item(b"events")
            .await
            .unwrap()
            .expect("stream should exist after reload");
        let stream2 = if let StoreValue::Stream(s) = &item2.value {
            s
        } else {
            panic!("Expected Stream StoreValue")
        };
        assert_eq!(stream2.entries.len(), 3, "entries survive reload");
        assert!(
            stream2.groups.contains_key("processors"),
            "consumer group survives reload"
        );
        let group2 = &stream2.groups["processors"];
        assert_eq!(group2.pel.len(), 2, "pending entries survive reload");
        assert!(
            group2.consumers.contains_key("worker1"),
            "consumer survives reload"
        );
    }

    #[tokio::test]
    async fn test_aof_multi_db_persistence() {
        use crate::storage::engine::CURRENT_DB_INDEX;

        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        // Write an AOF file with commands targeting multiple databases
        write_resp_commands_to_file(
            &aof_path,
            &[
                // DB 0: SET db0_key db0_value
                RespCommand {
                    name: b"SELECT".to_vec(),
                    args: vec![b"0".to_vec()],
                },
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"db0_key".to_vec(), b"db0_value".to_vec()],
                },
                // DB 3: SET db3_key db3_value, SET db3_another another_value
                RespCommand {
                    name: b"SELECT".to_vec(),
                    args: vec![b"3".to_vec()],
                },
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"db3_key".to_vec(), b"db3_value".to_vec()],
                },
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"db3_another".to_vec(), b"another_value".to_vec()],
                },
                // Back to DB 0: SET db0_second second_value
                RespCommand {
                    name: b"SELECT".to_vec(),
                    args: vec![b"0".to_vec()],
                },
                RespCommand {
                    name: b"SET".to_vec(),
                    args: vec![b"db0_second".to_vec(), b"second_value".to_vec()],
                },
            ],
        );

        // Load into engine
        let engine = create_test_engine();
        let aof = AofPersistence::new(engine.clone(), aof_path);
        aof.load().await.unwrap();

        // Verify DB 0 keys
        let val = engine.get(b"db0_key").await.unwrap();
        assert_eq!(val, Some(b"db0_value".to_vec()));

        let val = engine.get(b"db0_second").await.unwrap();
        assert_eq!(val, Some(b"second_value".to_vec()));

        // Verify DB 0 does NOT have DB 3's keys
        let val = engine.get(b"db3_key").await.unwrap();
        assert_eq!(val, None, "DB 3 key should not be in DB 0");

        // Verify DB 3 keys
        let val = CURRENT_DB_INDEX
            .scope(3, async { engine.get(b"db3_key").await.unwrap() })
            .await;
        assert_eq!(val, Some(b"db3_value".to_vec()));

        let val = CURRENT_DB_INDEX
            .scope(3, async { engine.get(b"db3_another").await.unwrap() })
            .await;
        assert_eq!(val, Some(b"another_value".to_vec()));

        // Verify DB 3 does NOT have DB 0's keys
        let val = CURRENT_DB_INDEX
            .scope(3, async { engine.get(b"db0_key").await.unwrap() })
            .await;
        assert_eq!(val, None, "DB 0 key should not be in DB 3");
    }

    #[tokio::test]
    async fn test_aof_rewrite_multi_db_round_trip() {
        use crate::storage::engine::CURRENT_DB_INDEX;

        let dir = tempfile::tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        // Create engine with data in multiple databases
        let engine = create_test_engine();

        // DB 0
        engine
            .set(b"key0".to_vec(), b"val0".to_vec(), None)
            .await
            .unwrap();

        // DB 3
        CURRENT_DB_INDEX
            .scope(3, async {
                engine
                    .set(b"key3".to_vec(), b"val3".to_vec(), None)
                    .await
                    .unwrap();
            })
            .await;

        // DB 7
        CURRENT_DB_INDEX
            .scope(7, async {
                engine
                    .set(b"key7".to_vec(), b"val7".to_vec(), None)
                    .await
                    .unwrap();
            })
            .await;

        // Rewrite AOF
        let aof = AofPersistence::new(engine.clone(), aof_path.clone());
        aof.rewrite().await.unwrap();

        // Load into a fresh engine and verify
        let engine2 = create_test_engine();
        let aof2 = AofPersistence::new(engine2.clone(), aof_path);
        aof2.load().await.unwrap();

        // DB 0
        let val = engine2.get(b"key0").await.unwrap();
        assert_eq!(val, Some(b"val0".to_vec()));

        // DB 3
        let val = CURRENT_DB_INDEX
            .scope(3, async { engine2.get(b"key3").await.unwrap() })
            .await;
        assert_eq!(val, Some(b"val3".to_vec()));

        // DB 7
        let val = CURRENT_DB_INDEX
            .scope(7, async { engine2.get(b"key7").await.unwrap() })
            .await;
        assert_eq!(val, Some(b"val7".to_vec()));

        // Cross-DB isolation
        let val = engine2.get(b"key3").await.unwrap();
        assert_eq!(val, None, "DB 3 key should not be in DB 0");

        let val = CURRENT_DB_INDEX
            .scope(3, async { engine2.get(b"key0").await.unwrap() })
            .await;
        assert_eq!(val, None, "DB 0 key should not be in DB 3");
    }
}
