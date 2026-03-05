use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::sync::RwLock;
use tokio::time::{self, Interval};

use crate::networking::resp::RespCommand;
use crate::storage::error::StorageError;
use crate::storage::item::{RedisDataType, StorageItem};

tokio::task_local! {
    /// The currently active database index for this task/connection.
    /// Defaults to 0 (primary database) if not set.
    pub static CURRENT_DB_INDEX: usize;
    /// Whether the current task is inside a MULTI/EXEC transaction execution.
    /// When true, commands should skip their own key locking since EXEC already holds locks.
    pub static IN_TRANSACTION: bool;
}

/// Number of databases supported (Redis default is 16)
pub const NUM_DATABASES: usize = 16;

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

/// Result of a SET operation with options (NX/XX/GET)
#[derive(Debug, Clone, PartialEq)]
pub enum SetResult {
    /// Key was set successfully
    Ok,
    /// Key was not set (NX condition failed or XX condition failed)
    NotSet,
    /// Key was set and old value is returned (GET flag)
    OldValue(Option<Vec<u8>>),
}

/// Entry in the expiration priority queue
#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpirationEntry {
    expires_at: Instant,
    key: Vec<u8>,
    /// Database index this key belongs to (for multi-DB expiration)
    db_index: usize,
}

impl PartialOrd for ExpirationEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExpirationEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering so earliest expiration comes first in min-heap
        other.expires_at.cmp(&self.expires_at)
    }
}

/// Eviction policy for the storage engine
#[derive(Debug, Clone, Copy, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
#[allow(clippy::upper_case_acronyms)]
pub enum EvictionPolicy {
    /// Remove the least recently used item
    #[default]
    LRU,
    /// Remove the least frequently used item
    LFU,
    /// Remove a random item
    Random,
    /// Don't evict items
    NoEviction,
}

impl EvictionPolicy {
    /// Convert to u8 for atomic storage.
    pub const fn to_u8(self) -> u8 {
        match self {
            Self::LRU => 0,
            Self::LFU => 1,
            Self::Random => 2,
            Self::NoEviction => 3,
        }
    }

    /// Convert from u8 (atomic load). Falls back to NoEviction for unknown values.
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::LRU,
            1 => Self::LFU,
            2 => Self::Random,
            _ => Self::NoEviction,
        }
    }
}

// Implement clap::ValueEnum for command-line parsing
impl clap::ValueEnum for EvictionPolicy {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::LRU, Self::LFU, Self::Random, Self::NoEviction]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(match self {
            Self::LRU => clap::builder::PossibleValue::new("lru"),
            Self::LFU => clap::builder::PossibleValue::new("lfu"),
            Self::Random => clap::builder::PossibleValue::new("random"),
            Self::NoEviction => clap::builder::PossibleValue::new("noeviction"),
        })
    }
}

/// Configuration for the storage engine
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Maximum memory usage in bytes
    pub max_memory: usize,
    /// Eviction policy
    pub eviction_policy: EvictionPolicy,
    /// Default TTL for keys (0 = no expiration)
    #[allow(dead_code)]
    pub default_ttl: Duration,
    /// Maximum key size in bytes
    pub max_key_size: usize,
    /// Maximum value size in bytes
    pub max_value_size: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            // Default to 128MB
            max_memory: 128 * 1024 * 1024,
            eviction_policy: EvictionPolicy::LRU,
            default_ttl: Duration::from_secs(0),
            max_key_size: 1024,
            max_value_size: 5 * 1024 * 1024, // 5MB
        }
    }
}

/// Guard that holds per-key locks acquired during MULTI/EXEC transactions.
/// Locks are released automatically when the guard is dropped.
pub struct KeyLockGuard {
    _guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
}

/// Main storage engine for the in-memory key-value store
pub struct StorageEngine {
    data: DashMap<Vec<u8>, StorageItem>,
    config: StorageConfig,
    /// Atomic memory counter for better performance (eliminates lock contention)
    current_memory: AtomicU64,
    expiration_timer: RwLock<Interval>,
    write_counters: RwLock<Vec<Arc<AtomicU64>>>,
    /// Atomic counter for the total number of keys (for O(1) DBSIZE)
    key_count: AtomicU64,
    /// Priority queue for efficient TTL management (O(log n) operations)
    expiration_queue: RwLock<BinaryHeap<ExpirationEntry>>,
    /// Pattern index for common prefixes (optimizes KEYS command), keyed by (db_index, prefix)
    #[allow(clippy::type_complexity)]
    prefix_index: RwLock<HashMap<(usize, String), Vec<Vec<u8>>>>,
    /// Additional databases (1-15) for multi-database support (SELECT/MOVE)
    extra_dbs: Vec<DashMap<Vec<u8>, StorageItem>>,
    /// Per-key version counter for WATCH (optimistic locking) support
    key_versions: DashMap<Vec<u8>, u64>,
    /// Global version counter for generating unique version numbers
    global_version: AtomicU64,
    /// Per-key mutexes for transaction-level locking (MULTI/EXEC isolation)
    /// Keys are DB-scoped (prefixed with db_index) to prevent cross-DB contention
    key_locks: DashMap<Vec<u8>, Arc<tokio::sync::Mutex<()>>>,
    /// Per-database flush epoch counters for WATCH invalidation on FLUSHDB/FLUSHALL.
    /// Each database has its own epoch so FLUSHDB on one DB doesn't invalidate watches on another.
    flush_epochs: Vec<AtomicU64>,
    /// Cross-database operation lock for snapshot consistency.
    /// snapshot_all_dbs acquires a write lock to get a consistent view across all DBs.
    /// Cross-DB operations (MOVE, SWAPDB) acquire a read lock so they don't run during snapshots.
    cross_db_lock: RwLock<()>,
    /// Packed mapping from logical database index to physical database index.
    /// Each logical DB occupies 4 bits (one nibble): logical DB i is at bits [i*4, i*4+3].
    /// SWAPDB atomically swaps both nibbles via a single atomic store, eliminating the
    /// race window where two separate AtomicUsize stores could let concurrent readers
    /// see an intermediate state (both logical DBs pointing to the same physical DB).
    db_mapping: AtomicU64,
    /// Runtime configuration store for CONFIG GET/SET support.
    /// Seeded from startup config, modified by CONFIG SET, queried by CONFIG GET.
    runtime_config: DashMap<String, String>,
    /// Runtime-mutable eviction policy. Updated atomically by CONFIG SET maxmemory-policy.
    /// Read by maybe_evict() during eviction. Stored as u8 via EvictionPolicy::to_u8/from_u8.
    active_eviction_policy: AtomicU8,
    /// Runtime-mutable max memory limit. Updated atomically by CONFIG SET maxmemory.
    /// Read by maybe_evict() during eviction. 0 means use config.max_memory (startup default).
    active_max_memory: AtomicUsize,
}

impl StorageEngine {
    /// Create a new storage engine with the given configuration
    pub fn new(config: StorageConfig) -> Arc<Self> {
        let mut extra_dbs = Vec::with_capacity(NUM_DATABASES - 1);
        for _ in 0..(NUM_DATABASES - 1) {
            extra_dbs.push(DashMap::with_capacity(64));
        }

        let initial_policy = config.eviction_policy.to_u8();
        // In Redis, maxmemory 0 means "no memory limit". Normalize to usize::MAX
        // to match the CONFIG SET path behavior.
        let initial_max_memory = if config.max_memory == 0 {
            usize::MAX
        } else {
            config.max_memory
        };
        let engine = Arc::new(Self {
            data: DashMap::with_capacity(1024), // Starting with a reasonable capacity
            config,
            current_memory: AtomicU64::new(0),
            expiration_timer: RwLock::new(time::interval(Duration::from_secs(1))),
            write_counters: RwLock::new(Vec::new()),
            key_count: AtomicU64::new(0),
            expiration_queue: RwLock::new(BinaryHeap::new()),
            prefix_index: RwLock::new(HashMap::new()),
            extra_dbs,
            key_versions: DashMap::with_capacity(256),
            global_version: AtomicU64::new(1),
            key_locks: DashMap::with_capacity(256),
            flush_epochs: (0..NUM_DATABASES).map(|_| AtomicU64::new(0)).collect(),
            cross_db_lock: RwLock::new(()),
            db_mapping: AtomicU64::new(Self::identity_db_mapping()),
            runtime_config: DashMap::new(),
            active_eviction_policy: AtomicU8::new(initial_policy),
            active_max_memory: AtomicUsize::new(initial_max_memory),
        });

        // Seed runtime config from startup config
        engine.seed_runtime_config();

        // Start the expiration task
        Self::start_expiration_task(Arc::clone(&engine));

        engine
    }

    /// Start the background task for efficient TTL-based expiration
    fn start_expiration_task(engine: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = engine.expiration_timer.write().await;
            let mut metadata_cleanup_counter: u64 = 0;

            loop {
                interval.tick().await;
                // Use priority queue for efficient expiration + probabilistic sampling
                engine.process_expired_keys().await;

                // Periodically clean up stale key_versions and key_locks entries
                // to prevent unbounded memory growth from WATCH/transaction metadata
                metadata_cleanup_counter += 1;
                if metadata_cleanup_counter >= 60 {
                    metadata_cleanup_counter = 0;
                    engine.cleanup_stale_metadata();
                }
            }
        });
    }

    /// Build the identity db_mapping: logical DB i maps to physical DB i.
    /// Packed as nibbles in a u64: DB 0 at bits [0:3], DB 1 at bits [4:7], etc.
    const fn identity_db_mapping() -> u64 {
        let mut mapping = 0u64;
        let mut i = 0;
        while i < NUM_DATABASES {
            mapping |= (i as u64) << (i * 4);
            i += 1;
        }
        mapping
    }

    /// Seed the runtime_config DashMap from the startup StorageConfig.
    fn seed_runtime_config(&self) {
        let c = &self.config;
        self.runtime_config
            .insert("maxmemory".to_string(), c.max_memory.to_string());
        let policy_str = match c.eviction_policy {
            EvictionPolicy::LRU => "allkeys-lru",
            EvictionPolicy::LFU => "allkeys-lfu",
            EvictionPolicy::Random => "allkeys-random",
            EvictionPolicy::NoEviction => "noeviction",
        };
        self.runtime_config
            .insert("maxmemory-policy".to_string(), policy_str.to_string());
        self.runtime_config
            .insert("databases".to_string(), NUM_DATABASES.to_string());
        self.runtime_config
            .insert("timeout".to_string(), "0".to_string());
        self.runtime_config
            .insert("hz".to_string(), "10".to_string());
        self.runtime_config
            .insert("tcp-backlog".to_string(), "511".to_string());
        self.runtime_config
            .insert("tcp-keepalive".to_string(), "300".to_string());
        self.runtime_config
            .insert("bind".to_string(), "0.0.0.0".to_string());
        self.runtime_config
            .insert("port".to_string(), "6379".to_string());
        self.runtime_config
            .insert("requirepass".to_string(), String::new());
        self.runtime_config
            .insert("appendonly".to_string(), "no".to_string());
        self.runtime_config
            .insert("appendfsync".to_string(), "everysec".to_string());
        self.runtime_config
            .insert("save".to_string(), "3600 1 300 100 60 10000".to_string());
    }

    /// Seed additional runtime config values from the application settings (called from main after engine creation).
    pub fn seed_from_settings(
        &self,
        port: u16,
        bind: &str,
        requirepass: &str,
        appendonly: bool,
        appendfsync: &str,
        save: &str,
    ) {
        self.runtime_config
            .insert("port".to_string(), port.to_string());
        self.runtime_config
            .insert("bind".to_string(), bind.to_string());
        if !requirepass.is_empty() {
            self.runtime_config
                .insert("requirepass".to_string(), requirepass.to_string());
        }
        if appendonly {
            self.runtime_config
                .insert("appendonly".to_string(), "yes".to_string());
        }
        if !appendfsync.is_empty() {
            self.runtime_config
                .insert("appendfsync".to_string(), appendfsync.to_string());
        }
        if !save.is_empty() {
            self.runtime_config
                .insert("save".to_string(), save.to_string());
        }
    }

    /// Get all runtime config entries matching a glob pattern.
    /// Returns pairs of (param_name, value) matching the pattern.
    pub fn config_get(&self, pattern: &str) -> Vec<(String, String)> {
        use crate::utils::glob::glob_match;
        let mut results = Vec::new();
        for entry in self.runtime_config.iter() {
            if glob_match(pattern, entry.key()) {
                results.push((entry.key().clone(), entry.value().clone()));
            }
        }
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
    }

    /// Set a runtime config value. Returns Ok(()) if the value was set successfully.
    /// For `maxmemory-policy`, also updates the actual eviction policy in the engine config.
    pub fn config_set(&self, param: &str, value: &str) -> Result<(), String> {
        let key = param.to_lowercase();
        match key.as_str() {
            "maxmemory" => {
                // Parse memory value (supports plain bytes or suffixes like kb, mb, gb)
                let bytes = Self::parse_memory_value(value).map_err(|_| {
                    format!(
                        "ERR Invalid argument '{}' for CONFIG SET 'maxmemory'",
                        value
                    )
                })?;
                // In Redis, maxmemory 0 means "no memory limit". Represent this as
                // usize::MAX so that maybe_evict() never triggers eviction.
                let effective = if bytes == 0 { usize::MAX } else { bytes };
                self.active_max_memory.store(effective, Ordering::Release);
                // Store the normalized byte count (not the raw string) so CONFIG GET
                // returns a numeric value matching Redis behavior.
                self.runtime_config.insert(key, bytes.to_string());
                Ok(())
            }
            "maxmemory-policy" => {
                // Parse and apply the eviction policy
                let policy = match value.to_lowercase().as_str() {
                    "allkeys-lru" | "volatile-lru" => EvictionPolicy::LRU,
                    "allkeys-lfu" | "volatile-lfu" => EvictionPolicy::LFU,
                    "allkeys-random" | "volatile-random" => EvictionPolicy::Random,
                    "noeviction" => EvictionPolicy::NoEviction,
                    _ => {
                        return Err(format!(
                            "ERR Invalid argument '{}' for CONFIG SET 'maxmemory-policy'",
                            value
                        ));
                    }
                };
                // Atomically update the active eviction policy used by maybe_evict()
                self.active_eviction_policy
                    .store(policy.to_u8(), Ordering::Release);
                self.runtime_config.insert(key, value.to_lowercase());
                Ok(())
            }
            // Immutable parameters that cannot be changed at runtime
            "bind" | "port" | "tls-port" | "tls-cert-file" | "tls-key-file"
            | "tls-ca-cert-file" | "daemonize" | "pidfile" | "logfile" => {
                Err(format!(
                    "ERR Unsupported CONFIG parameter: {}",
                    param
                ))
            }
            // Security parameters that require SecurityManager updates which
            // CONFIG SET cannot perform - reject to avoid silent misconfiguration
            "requirepass" => {
                Err(
                    "ERR CONFIG SET requirepass is not supported at runtime. Use ACL SETUSER to manage authentication.".to_string()
                )
            }
            // Known runtime-configurable parameters that we store but don't need
            // special handling for (Redis accepts these via CONFIG SET)
            "hz" | "timeout" | "tcp-keepalive" | "tcp-backlog" | "databases"
            | "appendonly" | "appendfsync" | "save" | "no-appendfsync-on-rewrite"
            | "auto-aof-rewrite-percentage" | "auto-aof-rewrite-min-size"
            | "aof-use-rdb-preamble" | "slowlog-log-slower-than" | "slowlog-max-len"
            | "latency-monitor-threshold" | "notify-keyspace-events"
            | "list-max-ziplist-size" | "list-compress-depth"
            | "set-max-intset-entries" | "zset-max-ziplist-entries"
            | "zset-max-ziplist-value" | "hash-max-ziplist-entries"
            | "hash-max-ziplist-value" | "hll-sparse-max-bytes"
            | "stream-node-max-bytes" | "stream-node-max-entries"
            | "activedefrag" | "active-defrag-enabled"
            | "lazyfree-lazy-eviction" | "lazyfree-lazy-expire"
            | "lazyfree-lazy-server-del" | "lazyfree-lazy-user-del"
            | "replica-lazy-flush" | "lfu-log-factor" | "lfu-decay-time"
            | "maxmemory-samples" | "min-replicas-to-write" | "min-replicas-max-lag"
            | "replica-read-only" | "repl-backlog-size" | "repl-backlog-ttl"
            | "cluster-node-timeout" | "cluster-migration-barrier"
            | "close-on-oom" | "proto-max-bulk-len" | "lua-time-limit"
            | "loglevel" | "maxclients" | "dir" | "dbfilename"
            | "rdbcompression" | "rdbchecksum" | "stop-writes-on-bgsave-error" => {
                self.runtime_config.insert(key, value.to_string());
                Ok(())
            }
            _ => Err(format!("ERR Unsupported CONFIG parameter: {}", param)),
        }
    }

    /// Parse a memory value string (e.g. "100mb", "1gb", "1048576") into bytes.
    fn parse_memory_value(value: &str) -> Result<usize, ()> {
        let s = value.trim().to_lowercase();
        if let Ok(bytes) = s.parse::<usize>() {
            return Ok(bytes);
        }
        // Try suffixes: kb, mb, gb
        for (suffix, multiplier) in &[("gb", 1024 * 1024 * 1024), ("mb", 1024 * 1024), ("kb", 1024)] {
            if let Some(num_str) = s.strip_suffix(suffix) {
                return num_str
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| ())
                    .and_then(|n| n.checked_mul(*multiplier).ok_or(()));
            }
        }
        Err(())
    }

    /// Extract the physical database index for a given logical index from the packed mapping.
    #[inline(always)]
    fn physical_index_from_mapping(mapping: u64, logical_idx: usize) -> usize {
        ((mapping >> (logical_idx * 4)) & 0xF) as usize
    }

    /// Returns a reference to a physical database by its physical index.
    /// Physical index 0 = self.data, physical index 1-15 = self.extra_dbs[0..14].
    fn get_physical_db(&self, physical_idx: usize) -> &DashMap<Vec<u8>, StorageItem> {
        if physical_idx == 0 {
            &self.data
        } else if physical_idx < NUM_DATABASES {
            &self.extra_dbs[physical_idx - 1]
        } else {
            &self.data // Fallback to physical DB 0
        }
    }

    /// Returns a reference to the currently active database based on the task-local db index.
    /// Falls back to database 0 if no task-local is set (e.g., background tasks).
    /// Uses packed db_mapping for logical-to-physical translation so SWAPDB works atomically
    /// (a single AtomicU64 load gives a consistent snapshot of the entire mapping).
    pub fn active_db(&self) -> &DashMap<Vec<u8>, StorageItem> {
        let logical_idx = CURRENT_DB_INDEX.try_with(|v| *v).unwrap_or(0);
        if logical_idx < NUM_DATABASES {
            let mapping = self.db_mapping.load(Ordering::Acquire);
            let physical_idx = Self::physical_index_from_mapping(mapping, logical_idx);
            self.get_physical_db(physical_idx)
        } else {
            self.get_physical_db(0) // Fallback to DB 0 for invalid index
        }
    }

    /// Get the current flush epoch for a specific database (for WATCH invalidation on FLUSH operations)
    pub fn get_flush_epoch_for_db(&self, db_index: usize) -> u64 {
        self.flush_epochs[db_index].load(Ordering::Acquire)
    }

    /// Helper: get the current task-local database index (defaults to 0)
    fn current_db_idx() -> usize {
        CURRENT_DB_INDEX.try_with(|v| *v).unwrap_or(0)
    }

    /// Helper: create a DB-scoped key for version tracking and key locks.
    /// Prefixes the key with the db_index to prevent cross-DB collisions.
    fn db_scoped_key(db_index: usize, key: &[u8]) -> Vec<u8> {
        let mut scoped = Vec::with_capacity(key.len() + 8);
        scoped.extend_from_slice(&(db_index as u64).to_le_bytes());
        scoped.extend_from_slice(key);
        scoped
    }

    /// Get a reference to a specific database by logical index.
    /// Uses packed db_mapping for logical-to-physical translation.
    pub fn get_db_by_index(&self, db_index: usize) -> &DashMap<Vec<u8>, StorageItem> {
        if db_index < NUM_DATABASES {
            let mapping = self.db_mapping.load(Ordering::Acquire);
            let physical_idx = Self::physical_index_from_mapping(mapping, db_index);
            self.get_physical_db(physical_idx)
        } else {
            self.get_physical_db(0)
        }
    }

    /// Efficient expiration using priority queue + probabilistic sampling (all databases)
    async fn process_expired_keys(&self) {
        let now = Instant::now();
        let mut removed_count = 0;
        const MAX_REMOVALS_PER_CYCLE: usize = 100;

        // Phase 1: Process expired keys from priority queue (O(log n) per removal)
        {
            let mut queue = self.expiration_queue.write().await;

            while let Some(entry) = queue.peek() {
                if entry.expires_at > now {
                    break; // No more expired keys in queue
                }

                let expired_entry = queue.pop().unwrap();
                drop(queue); // Release queue lock early

                // Get the correct database for this entry
                let db = self.get_db_by_index(expired_entry.db_index);

                // Only remove the key if it is actually expired (it may have been refreshed with a new TTL)
                let should_remove = match db.get(&expired_entry.key) {
                    Some(item) => item.is_expired(),
                    None => false, // Key already deleted
                };
                if should_remove && let Some((k, item)) = db.remove(&expired_entry.key) {
                    if !item.is_expired() {
                        // Key was refreshed concurrently — put it back
                        db.insert(k, item);
                    } else {
                        let size = Self::calculate_size(&k, &item.value) as u64;
                        self.current_memory.fetch_sub(size, Ordering::AcqRel);
                        self.key_count.fetch_sub(1, Ordering::AcqRel);
                        removed_count += 1;
                    }
                }

                // Limit removals per cycle to avoid blocking
                if removed_count >= MAX_REMOVALS_PER_CYCLE {
                    break;
                }

                // Reacquire queue lock for next iteration
                queue = self.expiration_queue.write().await;
            }
        }

        // Phase 2: Probabilistic sampling for keys not in queue (cleanup across all DBs)
        // This handles keys that might have been missed or expired through lazy deletion
        if removed_count < MAX_REMOVALS_PER_CYCLE / 2 {
            self.probabilistic_cleanup().await;
        }
    }

    /// Probabilistic cleanup for keys not tracked in expiration queue (all databases)
    async fn probabilistic_cleanup(&self) {
        const SAMPLE_SIZE: usize = 20;

        // Sample from all databases, not just DB 0
        for db_idx in 0..NUM_DATABASES {
            let db = self.get_db_by_index(db_idx);
            let mut keys_to_remove = Vec::new();

            let len = db.len();
            if len == 0 {
                continue;
            }
            let skip = if len > SAMPLE_SIZE {
                fastrand::usize(0..len)
            } else {
                0
            };
            let mut sampled_count = 0;
            for entry in db.iter().skip(skip) {
                if sampled_count >= SAMPLE_SIZE {
                    break;
                }
                sampled_count += 1;

                if entry.value().is_expired() {
                    keys_to_remove.push(entry.key().clone());
                }
            }
            // Wrap around to sample from the beginning if we hit the end
            if sampled_count < SAMPLE_SIZE && skip > 0 {
                for entry in db.iter().take(SAMPLE_SIZE - sampled_count) {
                    if entry.value().is_expired() {
                        keys_to_remove.push(entry.key().clone());
                    }
                }
            }

            // Remove expired keys found in this sample
            for key in keys_to_remove {
                if let Some((k, item)) = db.remove(&key) {
                    let size = Self::calculate_size(&k, &item.value) as u64;
                    self.current_memory.fetch_sub(size, Ordering::AcqRel);
                    self.key_count.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
    }

    /// Clean up stale entries in key_locks DashMap.
    /// For key_locks, only removes entries where no one is holding the lock
    /// (Arc strong_count == 1, meaning only the DashMap holds a reference).
    ///
    /// NOTE: key_versions are intentionally NOT cleaned up here. Removing version
    /// entries for deleted keys would cause WATCH correctness bugs: if a key is
    /// watched at version 0 (non-existent), then created and deleted, cleanup would
    /// reset its version to 0 (the fallback), causing EXEC to incorrectly proceed
    /// instead of aborting. The memory cost of retaining version entries (Vec<u8> + u64
    /// per unique key ever modified) is negligible compared to the data itself.
    ///
    /// Called periodically from the background expiration task.
    fn cleanup_stale_metadata(&self) {
        // Clean up key_locks
        // Only remove lock entries where no one is holding the mutex
        // (Arc strong_count == 1 means only the DashMap holds a reference)
        let stale_locks: Vec<Vec<u8>> = self
            .key_locks
            .iter()
            .filter(|entry| {
                let scoped_key = entry.key();
                let arc = entry.value();

                // If someone is holding the lock, don't remove it
                if Arc::strong_count(arc) > 1 {
                    return false;
                }

                // Extract db_index and original key from the scoped key
                if scoped_key.len() < 8 {
                    return true; // Malformed key, remove it
                }
                let db_index_bytes: [u8; 8] = scoped_key[..8].try_into().unwrap_or([0; 8]);
                let db_index = u64::from_le_bytes(db_index_bytes) as usize;
                let original_key = &scoped_key[8..];

                if db_index >= NUM_DATABASES {
                    return true; // Invalid db_index, remove it
                }

                // Check if key exists in the corresponding database
                let db = self.get_db_by_index(db_index);
                !db.contains_key(original_key)
            })
            .map(|entry| entry.key().clone())
            .collect();

        for key in stale_locks {
            // Use remove_if pattern: only remove if the Arc is still unshared
            // This prevents removing a lock that was just acquired by another task
            self.key_locks
                .remove_if(&key, |_, arc| Arc::strong_count(arc) == 1);
        }
    }

    /// Get the size of a key-value pair in bytes
    fn calculate_size(key: &[u8], value: &[u8]) -> usize {
        key.len() + value.len()
    }

    /// Helper method to remove expired key and update memory/counters
    async fn remove_expired_key(&self, key: &[u8]) {
        let db = self.active_db();
        if let Some((k, item)) = db.remove(key) {
            if !item.is_expired() {
                // Key was refreshed concurrently — put it back
                db.insert(k, item);
                return;
            }
            // Use atomic operations for memory tracking
            let size = Self::calculate_size(&k, &item.value) as u64;
            self.current_memory.fetch_sub(size, Ordering::AcqRel);
            self.key_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Lazy expiration: if the key exists but is expired, remove it now.
    /// Called by read commands (GET, etc.) after atomic_read returns None to clean up
    /// expired keys eagerly rather than waiting for the periodic expiration task.
    pub async fn lazy_expire(&self, key: &[u8]) {
        if self.active_db().contains_key(key) {
            self.remove_expired_key(key).await;
        }
    }

    /// Add a key to the expiration queue (with current database index)
    async fn add_to_expiration_queue(&self, key: Vec<u8>, expires_at: Instant) {
        let db_index = Self::current_db_idx();
        self.add_to_expiration_queue_for_db(key, expires_at, db_index)
            .await;
    }

    /// Add a key to the expiration queue for a specific database.
    /// Used by cross-DB operations (e.g. MOVE) that need to enqueue TTL
    /// in a database other than the current task-local one.
    async fn add_to_expiration_queue_for_db(
        &self,
        key: Vec<u8>,
        expires_at: Instant,
        db_index: usize,
    ) {
        let mut queue = self.expiration_queue.write().await;
        queue.push(ExpirationEntry {
            expires_at,
            key,
            db_index,
        });
    }

    /// Remove a key from expiration queue for the current database (used when TTL is removed).
    /// Only removes entries matching both the key AND the current database index,
    /// so removing TTL from `foo` in DB 0 won't affect `foo` in DB 1.
    async fn remove_from_expiration_queue(&self, key: &[u8]) {
        let db_index = Self::current_db_idx();
        let mut queue = self.expiration_queue.write().await;
        // Note: BinaryHeap doesn't support efficient removal by key
        // In practice, we'll let the background task clean up stale entries
        // This is acceptable since the queue will filter out non-existent keys

        // Rebuild the queue without the target key+db entry (expensive but rare operation)
        let entries: Vec<ExpirationEntry> = queue.drain().collect();
        for entry in entries {
            if !(entry.key == key && entry.db_index == db_index) {
                queue.push(entry);
            }
        }
    }

    /// Common prefixes to index for faster KEYS operations
    const INDEXED_PREFIXES: &'static [&'static str] = &[
        "user:", "session:", "cache:", "temp:", "auth:", "token:", "data:", "config:", "stats:",
        "log:", "queue:", "job:",
    ];

    /// Update prefix indices when a key is added or removed (DB-scoped)
    async fn update_prefix_indices(&self, key: &[u8], is_insert: bool) {
        let db_idx = Self::current_db_idx();
        self.update_prefix_indices_for_db(key, is_insert, db_idx)
            .await;
    }

    /// Update prefix indices for a specific database index.
    /// Used by cross-DB operations (MOVE) that need to update the destination DB's prefix index.
    async fn update_prefix_indices_for_db(&self, key: &[u8], is_insert: bool, db_idx: usize) {
        if let Ok(key_str) = std::str::from_utf8(key) {
            let mut index = self.prefix_index.write().await;

            for &prefix in Self::INDEXED_PREFIXES {
                if key_str.starts_with(prefix) {
                    let idx_key = (db_idx, prefix.to_string());
                    let entry = index.entry(idx_key.clone()).or_insert_with(Vec::new);
                    if is_insert {
                        entry.push(key.to_vec());
                    } else {
                        entry.retain(|k| k != key);
                        // Clean up empty entries
                        if entry.is_empty() {
                            index.remove(&idx_key);
                        }
                    }
                    break; // Only index the first matching prefix
                }
            }
        }
    }

    /// Extract prefix from a pattern for index lookup (returns DB-scoped key)
    fn extract_prefix_from_pattern(&self, pattern: &str) -> Option<(usize, String)> {
        // Check if pattern is a simple prefix pattern like "user:*"
        if pattern.ends_with('*') && !pattern[..pattern.len() - 1].contains('*') {
            let prefix = &pattern[..pattern.len() - 1];
            // Only return if it's one of our indexed prefixes
            if Self::INDEXED_PREFIXES.contains(&prefix) {
                let db_idx = Self::current_db_idx();
                return Some((db_idx, prefix.to_string()));
            }
        }
        None
    }

    /// Public pre-write memory check: ensures memory is under maxmemory before
    /// a write that uses `atomic_modify` (which is synchronous and cannot call
    /// async eviction internally). Call with estimated_additional=0 to match Redis
    /// behavior (evict if already over limit before executing the write command).
    pub async fn check_write_memory(&self, estimated_additional: usize) -> StorageResult<()> {
        self.maybe_evict(estimated_additional).await
    }

    /// Validate that all key-value pairs satisfy size limits.
    /// Returns an error on the first pair that exceeds max_key_size or max_value_size.
    pub fn validate_kv_sizes(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> StorageResult<()> {
        for (key, value) in pairs {
            if key.len() > self.config.max_key_size {
                return Err(StorageError::ValueTooLarge);
            }
            if value.len() > self.config.max_value_size {
                return Err(StorageError::ValueTooLarge);
            }
        }
        Ok(())
    }

    /// Check if we need to evict items to make room
    async fn maybe_evict(&self, required_size: usize) -> StorageResult<()> {
        // Read the active max memory limit (may have been changed at runtime via CONFIG SET)
        let max_memory = self.active_max_memory.load(Ordering::Acquire);
        // Check if adding this item would exceed our memory limit
        let current_memory = self.current_memory.load(Ordering::Acquire) as usize;
        if current_memory + required_size > max_memory {
            // Read the active eviction policy (may have been changed at runtime via CONFIG SET)
            let eviction_policy =
                EvictionPolicy::from_u8(self.active_eviction_policy.load(Ordering::Acquire));

            // If we're not allowed to evict, fail
            if eviction_policy == EvictionPolicy::NoEviction {
                return Err(StorageError::MemoryLimitExceeded);
            }

            // Evict items until we have enough space (search across all databases)
            while self.current_memory.load(Ordering::Acquire) as usize + required_size
                > max_memory
            {
                // Check if there are any items to evict across all DBs
                let total_keys: usize =
                    self.data.len() + self.extra_dbs.iter().map(|db| db.len()).sum::<usize>();
                if total_keys == 0 {
                    return Err(StorageError::MemoryLimitExceeded);
                }

                // Helper: find victim in a single DB
                let find_victim_in_db = |db: &DashMap<Vec<u8>, StorageItem>| -> Option<Vec<u8>> {
                    match eviction_policy {
                        EvictionPolicy::LRU | EvictionPolicy::LFU => db
                            .iter()
                            .min_by_key(|item| item.value().last_accessed)
                            .map(|item| item.key().clone()),
                        EvictionPolicy::Random => {
                            let len = db.len();
                            if len == 0 {
                                None
                            } else {
                                let skip = fastrand::usize(0..len);
                                db.iter().nth(skip).map(|item| item.key().clone())
                            }
                        }
                        _ => None,
                    }
                };

                // Find the best victim across all databases
                let mut best_victim: Option<(Vec<u8>, usize, Instant)> = None; // (key, db_idx, last_accessed)
                for db_idx in 0..NUM_DATABASES {
                    let db = self.get_db_by_index(db_idx);
                    if let Some(victim_key) = find_victim_in_db(db) {
                        let should_replace =
                            if eviction_policy == EvictionPolicy::Random {
                                best_victim.is_none() // For random, just pick the first one found
                            } else {
                                // For LRU/LFU, compare last_accessed times
                                if let Some(entry) = db.get(&victim_key) {
                                    match &best_victim {
                                        None => true,
                                        Some((_, _, best_time)) => {
                                            entry.value().last_accessed < *best_time
                                        }
                                    }
                                } else {
                                    false
                                }
                            };
                        if should_replace {
                            let la = db
                                .get(&victim_key)
                                .map(|e| e.value().last_accessed)
                                .unwrap_or(Instant::now());
                            best_victim = Some((victim_key, db_idx, la));
                        }
                    }
                }

                // Remove the victim from the correct database
                if let Some((key, db_idx, _)) = best_victim {
                    let db = self.get_db_by_index(db_idx);
                    if let Some((k, item)) = db.remove(&key) {
                        let size = Self::calculate_size(&k, &item.value) as u64;
                        self.current_memory.fetch_sub(size, Ordering::AcqRel);
                        self.key_count.fetch_sub(1, Ordering::AcqRel);
                    }
                } else {
                    return Err(StorageError::MemoryLimitExceeded);
                }
            }
        }

        Ok(())
    }

    /// Set a key-value pair in the storage engine (defaults to String type)
    pub async fn set(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> StorageResult<()> {
        self.set_with_type(key, value, RedisDataType::String, ttl)
            .await
    }

    /// Set a key-value pair with explicit data type
    pub async fn set_with_type(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        data_type: RedisDataType,
        ttl: Option<Duration>,
    ) -> StorageResult<()> {
        self.set_with_type_inner(key, value, data_type, ttl, true)
            .await
    }

    /// Set a key-value pair, skipping per-key eviction checks.
    /// Caller must have already performed a best-effort memory check via
    /// `check_write_memory` for the total batch size. Used by MSET to prevent
    /// partial writes that would violate its all-or-nothing guarantee.
    pub async fn set_preevicted(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> StorageResult<()> {
        self.set_with_type_inner(key, value, RedisDataType::String, ttl, false)
            .await
    }

    /// Inner implementation for set operations.
    /// When `do_evict` is false, skips the per-key `maybe_evict` call.
    async fn set_with_type_inner(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        data_type: RedisDataType,
        ttl: Option<Duration>,
        do_evict: bool,
    ) -> StorageResult<()> {
        // Check size limits
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }

        if value.len() > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }

        // For large values (likely JSON), do deeper validation
        if value.len() > 10240 {
            // 10KB
            // Log large SET operations for debugging
            tracing::debug!(
                "Large value SET operation: key={:?}, value_len={}, type={}",
                String::from_utf8_lossy(&key),
                value.len(),
                data_type.as_str()
            );
        }

        let required_size = Self::calculate_size(&key, &value);

        // Ensure we have enough memory (skipped when caller pre-checked for batch)
        if do_evict {
            self.maybe_evict(required_size).await?;
        }

        // Create a new item with explicit type
        let mut item = StorageItem::new_with_type(value, data_type);
        let expires_at = if let Some(ttl) = ttl {
            item.expire(ttl);
            Some(Instant::now() + ttl)
        } else {
            None
        };

        // Use entry API for atomic read-modify-write to avoid TOCTOU races
        let is_new_key = match self.active_db().entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let old_size = Self::calculate_size(entry.key(), &entry.get().value);
                if old_size > 0 {
                    self.current_memory
                        .fetch_sub(old_size as u64, Ordering::AcqRel);
                }
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::AcqRel);
                entry.insert(item);
                // Bump version while OccupiedEntry still holds the shard lock,
                // preventing a TOCTOU race with del_if_version_matches (MSETNX rollback).
                self.bump_key_version(&key);
                false
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::AcqRel);
                // Bind RefMut to keep shard lock held during version bump
                let _ref = entry.insert(item);
                self.bump_key_version(&key);
                true
            }
        };

        // Add to expiration queue if TTL is set
        if let Some(expires_at) = expires_at {
            self.add_to_expiration_queue(key.clone(), expires_at).await;
        }

        // Update prefix index and key count for new keys
        if is_new_key {
            self.update_prefix_indices(&key, true).await;
            self.key_count.fetch_add(1, Ordering::AcqRel);
        }
        // Increment write counters
        self.increment_write_counters().await;

        Ok(())
    }

    /// Set a key-value pair preserving the existing TTL if present.
    /// Use this for collection-type modifications (lists, sets, sorted sets, streams, geo)
    /// where updating the value should not reset the key's expiration.
    ///
    /// This implementation uses a single DashMap entry operation to atomically
    /// read the existing TTL and write the new value, avoiding TOCTOU races.
    pub async fn set_with_type_preserve_ttl(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        data_type: RedisDataType,
    ) -> StorageResult<()> {
        // Check size limits
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }

        if value.len() > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }

        let required_size = Self::calculate_size(&key, &value);

        // Ensure we have enough memory
        self.maybe_evict(required_size).await?;

        let mut item = StorageItem::new_with_type(value, data_type);

        // Single DashMap entry operation: read existing TTL + write new value atomically
        let (is_new_key, preserved_expires_at) = match self.active_db().entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                // Read TTL from existing entry while holding the shard lock
                let existing_expires_at = if !entry.get().is_expired() {
                    entry.get().expires_at
                } else {
                    None
                };

                // Preserve the existing expiration on the new item
                item.expires_at = existing_expires_at;

                let old_size = Self::calculate_size(entry.key(), &entry.get().value);
                if old_size > 0 {
                    self.current_memory
                        .fetch_sub(old_size as u64, Ordering::AcqRel);
                }
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::AcqRel);
                entry.insert(item);
                self.bump_key_version(&key);
                (false, existing_expires_at)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::AcqRel);
                let _ref = entry.insert(item);
                self.bump_key_version(&key);
                (true, None)
            }
        };

        // Re-add to expiration queue if TTL was preserved (ensures queue consistency)
        if let Some(expires_at) = preserved_expires_at {
            self.add_to_expiration_queue(key.clone(), expires_at).await;
        }

        // Update prefix index and key count for new keys
        if is_new_key {
            self.update_prefix_indices(&key, true).await;
            self.key_count.fetch_add(1, Ordering::AcqRel);
        }
        // Increment write counters
        self.increment_write_counters().await;

        Ok(())
    }

    /// Get a value from the storage engine
    pub async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let db = self.active_db();
        let result = if let Some(mut entry) = db.get_mut(key) {
            let item = entry.value_mut();

            if item.is_expired() {
                // We'll remove the key in a separate step
                None
            } else {
                // Update the last accessed time
                item.touch();

                Some(item.value.clone())
            }
        } else {
            None
        };

        // If the item was expired, remove it now using lazy expiration
        if result.is_none() && db.contains_key(key) {
            self.remove_expired_key(key).await;
        }

        Ok(result)
    }

    /// Delete a key from the storage engine
    pub async fn del(&self, key: &[u8]) -> StorageResult<bool> {
        if let Some((k, item)) = self.active_db().remove(key) {
            // Use atomic operations for memory tracking - much faster than write lock
            let size = Self::calculate_size(&k, &item.value) as u64;
            self.current_memory.fetch_sub(size, Ordering::AcqRel);
            // Update prefix index and decrement key count
            self.update_prefix_indices(&k, false).await;
            self.key_count.fetch_sub(1, Ordering::AcqRel);
            // Bump key version for WATCH support
            self.bump_key_version(&k);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Atomically delete a key only if its version matches the expected value.
    /// This prevents a TOCTOU race where a concurrent SET between a version check
    /// and a del could have its value incorrectly deleted.
    /// Used by MSETNX rollback to safely undo insertions without affecting concurrent writers.
    pub async fn del_if_version_matches(&self, key: &[u8], expected_version: u64) -> bool {
        use dashmap::mapref::entry::Entry;
        match self.active_db().entry(key.to_vec()) {
            Entry::Occupied(entry) => {
                // While holding the entry's shard lock, check the version.
                // A concurrent SET would need this same shard lock to write AND bump the
                // version, so the version we read here is guaranteed consistent with the entry.
                let db_idx = Self::current_db_idx();
                let scoped = Self::db_scoped_key(db_idx, key);
                let current_version = self.key_versions.get(&scoped).map(|v| *v).unwrap_or(0);
                if current_version != expected_version {
                    // Version changed - a concurrent writer overwrote this key, leave it alone
                    return false;
                }
                // Version matches - safe to delete
                let (k, item) = entry.remove_entry();
                let size = Self::calculate_size(&k, &item.value) as u64;
                self.current_memory.fetch_sub(size, Ordering::AcqRel);
                self.update_prefix_indices(&k, false).await;
                self.key_count.fetch_sub(1, Ordering::AcqRel);
                self.bump_key_version(&k);
                true
            }
            Entry::Vacant(_) => false, // Key already gone
        }
    }

    /// Check if a key exists in the storage engine
    pub async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        // Check if the key exists and is not expired
        if let Some(entry) = self.active_db().get(key) {
            if entry.value().is_expired() {
                // Use lazy expiration - remove expired key immediately
                drop(entry); // Release the reference before removal
                self.remove_expired_key(key).await;
                Ok(false)
            } else {
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }

    /// Set the TTL (time-to-live) for a key
    pub async fn expire(&self, key: &[u8], ttl: Option<Duration>) -> StorageResult<bool> {
        // Mutate the entry under the DashMap lock, then drop the guard before any .await
        let queue_action = if let Some(mut entry) = self.active_db().get_mut(key) {
            let item = entry.value_mut();

            if item.is_expired() {
                return Ok(false);
            }

            if let Some(ttl) = ttl {
                item.expire(ttl);
                let expires_at = Instant::now() + ttl;
                Some((true, expires_at))
            } else {
                item.remove_expiry();
                Some((false, Instant::now()))
            }
            // DashMap guard dropped here at end of `if let` block
        } else {
            None
        };

        if let Some((is_set, expires_at)) = queue_action {
            if is_set {
                self.add_to_expiration_queue(key.to_vec(), expires_at).await;
            } else {
                self.remove_from_expiration_queue(key).await;
            }
            self.bump_key_version(key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get the TTL (time-to-live) for a key
    pub async fn ttl(&self, key: &[u8]) -> StorageResult<Option<Duration>> {
        if let Some(entry) = self.active_db().get(key) {
            if entry.value().is_expired() {
                // Use lazy expiration - remove expired key and return None
                drop(entry); // Release the reference before removal
                self.remove_expired_key(key).await;
                Ok(None)
            } else {
                Ok(entry.value().ttl())
            }
        } else {
            Ok(None)
        }
    }

    /// Get all keys matching a pattern
    pub async fn keys(&self, pattern: &str) -> StorageResult<Vec<Vec<u8>>> {
        let db = self.active_db();
        // Try to use prefix index for common patterns
        if let Some(prefix) = self.extract_prefix_from_pattern(pattern) {
            let index = self.prefix_index.read().await;
            if let Some(indexed_keys) = index.get(&prefix) {
                // Filter indexed keys for exact pattern match and expiration
                let mut keys = Vec::new();
                for key in indexed_keys {
                    // Check if key still exists and is not expired
                    if let Some(entry) = db.get(key)
                        && !entry.value().is_expired()
                        && let Ok(key_str) = std::str::from_utf8(key)
                        && crate::utils::glob::glob_match(pattern, key_str)
                    {
                        keys.push(key.clone());
                    }
                }
                return Ok(keys);
            }
        }

        // Fall back to full scan for complex patterns or non-indexed prefixes
        let mut keys = Vec::new();

        for item in db.iter() {
            if item.value().is_expired() {
                continue;
            }

            // Convert key to string for pattern matching
            if let Ok(key_str) = std::str::from_utf8(item.key())
                && crate::utils::glob::glob_match(pattern, key_str)
            {
                keys.push(item.key().clone());
            }
        }

        Ok(keys)
    }

    /// Flush all data (remove all keys from all databases)
    pub async fn flush_all(&self) -> StorageResult<()> {
        self.data.clear();

        // Clear all extra databases (1-15)
        for db in &self.extra_dbs {
            db.clear();
        }

        // Reset memory and key count atomically
        self.current_memory.store(0, Ordering::Release);
        self.key_count.store(0, Ordering::Release);

        // Clear prefix index
        let mut prefix_index = self.prefix_index.write().await;
        prefix_index.clear();

        // Bump flush epoch for ALL databases so WATCH detects the flush
        for epoch in &self.flush_epochs {
            epoch.fetch_add(1, Ordering::SeqCst);
        }

        // Increment write counters
        self.increment_write_counters().await;

        Ok(())
    }

    /// Flush the current database (the active database, not all databases)
    pub async fn flush_db(&self) -> StorageResult<()> {
        let db = self.active_db();
        let db_idx = Self::current_db_idx();
        db.clear();
        // Recalculate memory across all databases to avoid drift from concurrent writes
        let total_memory: u64 = std::iter::once(&self.data)
            .chain(self.extra_dbs.iter())
            .map(|db| {
                db.iter()
                    .map(|entry| Self::calculate_size(entry.key(), &entry.value().value) as u64)
                    .sum::<u64>()
            })
            .sum();
        self.current_memory.store(total_memory, Ordering::Release);
        // Recalculate total key count across all databases
        let total: u64 =
            self.data.len() as u64 + self.extra_dbs.iter().map(|db| db.len() as u64).sum::<u64>();
        self.key_count.store(total, Ordering::Release);

        // Clear prefix index entries for this DB
        let mut prefix_index = self.prefix_index.write().await;
        prefix_index.retain(|(idx, _), _| *idx != db_idx);

        // Bump flush epoch only for the flushed database (not other DBs)
        self.flush_epochs[db_idx].fetch_add(1, Ordering::SeqCst);

        // Increment write counters so BGSAVE thresholds detect the flush
        self.increment_write_counters().await;

        Ok(())
    }

    /// Get a snapshot of database 0 (for backward compatibility).
    pub async fn snapshot(&self) -> StorageResult<HashMap<Vec<u8>, StorageItem>> {
        self.snapshot_db(0).await
    }

    /// Get a snapshot of a specific database by index.
    pub async fn snapshot_db(
        &self,
        db_index: usize,
    ) -> StorageResult<HashMap<Vec<u8>, StorageItem>> {
        let db = self.get_db_by_index(db_index);
        let mut snapshot = HashMap::with_capacity(db.len());

        for item in db.iter() {
            if !item.value().is_expired() {
                snapshot.insert(item.key().clone(), item.value().clone());
            }
        }

        Ok(snapshot)
    }

    /// Get a snapshot of all databases (for multi-DB persistence).
    /// Returns a Vec indexed by database number, each containing the DB's data.
    /// Acquires an exclusive cross-DB lock to prevent MOVE/SWAPDB from running
    /// during the snapshot, ensuring a consistent view across all databases.
    pub async fn snapshot_all_dbs(&self) -> StorageResult<Vec<HashMap<Vec<u8>, StorageItem>>> {
        let _guard = self.cross_db_lock.write().await;
        let mut snapshots = Vec::with_capacity(NUM_DATABASES);
        for db_idx in 0..NUM_DATABASES {
            snapshots.push(self.snapshot_db(db_idx).await?);
        }
        Ok(snapshots)
    }

    /// Register a write counter to be incremented on data modifications
    pub async fn register_write_counter(&self, counter: Arc<AtomicU64>) {
        let mut write_counters = self.write_counters.write().await;
        write_counters.push(counter);
    }

    /// Increment all registered write counters
    async fn increment_write_counters(&self) {
        let counters = self.write_counters.read().await;
        for counter in counters.iter() {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Synchronous variant of increment_write_counters for use in non-async contexts
    /// (e.g. atomic_modify). Uses try_read() which succeeds in virtually all cases.
    fn increment_write_counters_sync(&self) {
        if let Ok(counters) = self.write_counters.try_read() {
            for counter in counters.iter() {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// Bump the version counter for a specific key (DB-scoped for WATCH isolation)
    pub fn bump_key_version(&self, key: &[u8]) {
        let db_idx = Self::current_db_idx();
        self.bump_key_version_for_db(key, db_idx);
    }

    /// Bump the version counter for a specific key and return the new version.
    /// This combines bump + get into a single operation to avoid a race window
    /// where another writer could bump the version between separate bump/get calls.
    pub fn bump_and_get_key_version(&self, key: &[u8]) -> u64 {
        let db_idx = Self::current_db_idx();
        let scoped = Self::db_scoped_key(db_idx, key);
        let new_version = self.global_version.fetch_add(1, Ordering::SeqCst);
        self.key_versions.insert(scoped, new_version);
        new_version
    }

    /// Bump the version counter for a specific key in a specific database.
    /// Used by cross-DB operations (e.g. MOVE) that need to invalidate WATCH
    /// in the destination database.
    pub fn bump_key_version_for_db(&self, key: &[u8], db_index: usize) {
        let scoped = Self::db_scoped_key(db_index, key);
        let new_version = self.global_version.fetch_add(1, Ordering::SeqCst);
        self.key_versions.insert(scoped, new_version);
    }

    /// Get the current version of a key using the task-local DB index
    #[allow(dead_code)]
    pub fn get_key_version(&self, key: &[u8]) -> u64 {
        let db_idx = Self::current_db_idx();
        self.get_key_version_for_db(key, db_idx)
    }

    /// Get the current version of a key for a specific database
    pub fn get_key_version_for_db(&self, key: &[u8], db_index: usize) -> u64 {
        let scoped = Self::db_scoped_key(db_index, key);
        self.key_versions.get(&scoped).map(|v| *v).unwrap_or(0)
    }

    /// Get the number of entries in the key_versions map (for testing/monitoring)
    #[cfg(test)]
    pub fn key_versions_len(&self) -> usize {
        self.key_versions.len()
    }

    /// Get the number of entries in the key_locks map (for testing/monitoring)
    #[cfg(test)]
    pub fn key_locks_len(&self) -> usize {
        self.key_locks.len()
    }

    /// Trigger metadata cleanup (exposed for testing)
    #[cfg(test)]
    pub fn trigger_metadata_cleanup(&self) {
        self.cleanup_stale_metadata();
    }

    /// Check if the current task is inside a MULTI/EXEC transaction execution.
    pub fn in_transaction() -> bool {
        IN_TRANSACTION.try_with(|v| *v).unwrap_or(false)
    }

    /// Lock multiple keys in sorted order for transaction isolation.
    /// Keys are DB-scoped, sorted and deduplicated to prevent deadlocks.
    /// Returns a KeyLockGuard that releases all locks when dropped.
    /// If already inside a transaction (EXEC holds locks), returns an empty guard to avoid deadlock.
    pub async fn lock_keys(&self, keys: &[Vec<u8>]) -> KeyLockGuard {
        // If we're already inside a MULTI/EXEC execution, EXEC already holds the locks.
        // Acquiring them again would deadlock (tokio::sync::Mutex is not re-entrant).
        if Self::in_transaction() {
            return KeyLockGuard {
                _guards: Vec::new(),
            };
        }

        let db_idx = Self::current_db_idx();
        let mut sorted_keys: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| Self::db_scoped_key(db_idx, k))
            .collect();
        sorted_keys.sort();
        sorted_keys.dedup();

        let mut guards = Vec::with_capacity(sorted_keys.len());
        for key in &sorted_keys {
            let mutex = self
                .key_locks
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .value()
                .clone();
            guards.push(mutex.lock_owned().await);
        }

        KeyLockGuard { _guards: guards }
    }

    /// Bootstrap fallback for AOF replay — handles a limited subset of commands.
    /// The primary replay path now uses `CommandHandler::process()` for full coverage.
    /// This method is retained for compatibility with existing tests and as a
    /// lightweight fallback that doesn't require a full CommandHandler instance.
    #[allow(dead_code)]
    pub async fn process_command(&self, command: &RespCommand) -> StorageResult<()> {
        let upper_name: Vec<u8> = command
            .name
            .iter()
            .map(|b| b.to_ascii_uppercase())
            .collect();

        match upper_name.as_slice() {
            b"SET" => {
                if command.args.len() >= 2 {
                    let key = &command.args[0];
                    let value = &command.args[1];

                    let mut ttl = None;
                    let mut i = 2;
                    while i < command.args.len() {
                        let flag: Vec<u8> = command.args[i]
                            .iter()
                            .map(|b| b.to_ascii_uppercase())
                            .collect();
                        match flag.as_slice() {
                            b"EX" if i + 1 < command.args.len() => {
                                if let Ok(s) = std::str::from_utf8(&command.args[i + 1])
                                    && let Ok(v) = s.parse::<u64>()
                                {
                                    ttl = Some(Duration::from_secs(v));
                                }
                                i += 2;
                            }
                            b"PX" if i + 1 < command.args.len() => {
                                if let Ok(s) = std::str::from_utf8(&command.args[i + 1])
                                    && let Ok(v) = s.parse::<u64>()
                                {
                                    ttl = Some(Duration::from_millis(v));
                                }
                                i += 2;
                            }
                            _ => {
                                i += 1;
                            }
                        }
                    }

                    self.set(key.clone(), value.clone(), ttl).await?;
                }
            }
            b"DEL" => {
                for arg in &command.args {
                    let _ = self.del(arg).await?;
                }
            }
            b"EXPIRE" => {
                if command.args.len() >= 2
                    && let Ok(secs) = std::str::from_utf8(&command.args[1])
                    && let Ok(v) = secs.parse::<u64>()
                {
                    self.expire(&command.args[0], Some(Duration::from_secs(v)))
                        .await?;
                }
            }
            b"PEXPIRE" => {
                if command.args.len() >= 2
                    && let Ok(millis) = std::str::from_utf8(&command.args[1])
                    && let Ok(v) = millis.parse::<u64>()
                {
                    self.expire(&command.args[0], Some(Duration::from_millis(v)))
                        .await?;
                }
            }
            b"RPUSH" => {
                if command.args.len() >= 2 {
                    let key = &command.args[0];
                    let elements = &command.args[1..];
                    let _: () = self.atomic_modify(
                        key,
                        crate::storage::item::RedisDataType::List,
                        |existing| {
                            let mut list: Vec<Vec<u8>> = existing
                                .as_deref()
                                .and_then(|v| bincode::deserialize(v).ok())
                                .unwrap_or_default();
                            for elem in elements {
                                list.push(elem.clone());
                            }
                            Ok((Some(bincode::serialize(&list).unwrap()), ()))
                        },
                    )?;
                }
            }
            b"SADD" => {
                if command.args.len() >= 2 {
                    let key = &command.args[0];
                    let members = &command.args[1..];
                    let _: () = self.atomic_modify(
                        key,
                        crate::storage::item::RedisDataType::Set,
                        |existing| {
                            let mut set: std::collections::HashSet<Vec<u8>> = existing
                                .as_deref()
                                .and_then(|v| bincode::deserialize(v).ok())
                                .unwrap_or_default();
                            for m in members {
                                set.insert(m.clone());
                            }
                            Ok((Some(bincode::serialize(&set).unwrap()), ()))
                        },
                    )?;
                }
            }
            b"ZADD" => {
                if command.args.len() >= 3 {
                    let key = &command.args[0];
                    let pairs = &command.args[1..];
                    let _: () = self.atomic_modify(
                        key,
                        crate::storage::item::RedisDataType::ZSet,
                        |existing| {
                            let mut zset: crate::command::types::sorted_set::SortedSetData =
                                existing
                                    .as_deref()
                                    .and_then(|v| bincode::deserialize(v).ok())
                                    .unwrap_or_else(
                                        crate::command::types::sorted_set::SortedSetData::new,
                                    );
                            let mut i = 0;
                            while i + 1 < pairs.len() {
                                if let Ok(score_str) = std::str::from_utf8(&pairs[i])
                                    && let Ok(score) = score_str.parse::<f64>()
                                {
                                    zset.insert(score, pairs[i + 1].clone());
                                }
                                i += 2;
                            }
                            Ok((Some(bincode::serialize(&zset).unwrap()), ()))
                        },
                    )?;
                }
            }
            b"HSET" => {
                if command.args.len() >= 3 {
                    let key = &command.args[0];
                    let pairs = &command.args[1..];
                    let mut fields = Vec::new();
                    let mut i = 0;
                    while i + 1 < pairs.len() {
                        fields.push((pairs[i].as_slice(), pairs[i + 1].as_slice()));
                        i += 2;
                    }
                    let _ = self.hash_set(key, &fields);
                }
            }
            b"XADD" => {
                if command.args.len() >= 4 {
                    let key = &command.args[0];
                    let id_bytes = &command.args[1];
                    let field_args = &command.args[2..];
                    let _: () = self.atomic_modify(
                        key,
                        crate::storage::item::RedisDataType::Stream,
                        |existing| {
                            let mut stream: crate::storage::stream::StreamData = existing
                                .as_deref()
                                .and_then(|v| bincode::deserialize(v).ok())
                                .unwrap_or_else(crate::storage::stream::StreamData::new);
                            if let Ok(id_str) = std::str::from_utf8(id_bytes)
                                && let Some(id) =
                                    crate::storage::stream::StreamEntryId::parse(id_str)
                            {
                                let mut fields = Vec::new();
                                let mut j = 0;
                                while j + 1 < field_args.len() {
                                    fields.push((field_args[j].clone(), field_args[j + 1].clone()));
                                    j += 2;
                                }
                                let entry = crate::storage::stream::StreamEntry {
                                    id: id.clone(),
                                    fields,
                                };
                                stream.entries.insert(id.clone(), entry);
                                if id > stream.last_id {
                                    stream.last_id = id.clone();
                                }
                                stream.entries_added += 1;
                                if stream.first_entry_id.is_none() {
                                    stream.first_entry_id = Some(id);
                                }
                            }
                            Ok((Some(bincode::serialize(&stream).unwrap()), ()))
                        },
                    )?;
                }
            }
            // Transaction markers are no-ops during replay - the commands within
            // the transaction have already been executed individually
            b"MULTI" | b"EXEC" | b"DISCARD" => {}
            _ => {
                tracing::warn!(
                    "Unknown command during AOF replay: {:?}",
                    String::from_utf8_lossy(&command.name)
                );
            }
        }

        Ok(())
    }

    /// Get the type of a key
    pub async fn get_type(&self, key: &[u8]) -> StorageResult<String> {
        // Check if key exists and is not expired
        if let Some(entry) = self.active_db().get(key) {
            if entry.value().is_expired() {
                return Ok("none".to_string());
            }

            // Use the stored data type authoritatively
            let data_type = entry.value().data_type.as_str().to_string();

            Ok(data_type)
        } else {
            Ok("none".to_string())
        }
    }

    /// Get the stored data type for a key without legacy heuristic detection.
    /// Returns the raw data_type field from StorageItem, which is authoritative
    /// for keys created with the current storage format.
    pub async fn get_raw_data_type(&self, key: &[u8]) -> StorageResult<String> {
        if let Some(entry) = self.active_db().get(key) {
            if entry.value().is_expired() {
                drop(entry);
                self.remove_expired_key(key).await;
                return Ok("none".to_string());
            }
            Ok(entry.value().data_type.as_str().to_string())
        } else {
            Ok("none".to_string())
        }
    }

    /// Get the current number of keys (O(1) operation for DBSIZE)
    pub fn get_key_count(&self) -> u64 {
        self.key_count.load(Ordering::Acquire)
    }

    /// Get the key count for a specific database (logical index, uses db_mapping).
    #[allow(dead_code)]
    pub fn get_db_key_count(&self, db_index: usize) -> u64 {
        if db_index < NUM_DATABASES {
            self.get_db_by_index(db_index).len() as u64
        } else {
            0
        }
    }

    /// Get the current memory usage in bytes
    pub fn get_used_memory(&self) -> u64 {
        self.current_memory.load(Ordering::Acquire)
    }

    /// Get a random key from the storage engine (O(1) operation for RANDOMKEY)
    pub async fn get_random_key(&self) -> StorageResult<Option<Vec<u8>>> {
        let db = self.active_db();
        // Check if database is empty
        if db.is_empty() {
            return Ok(None);
        }

        // Use reservoir sampling to get a random non-expired key efficiently
        // This avoids the O(n) scan of keys("*")
        let mut random_key = None;
        let mut count = 0;
        let max_samples = 100; // Limit sampling to avoid long iterations

        for entry in db.iter() {
            // Skip expired keys
            if entry.value().is_expired() {
                continue;
            }

            count += 1;

            // Reservoir sampling algorithm: probability of selection is 1/count
            if fastrand::f32() < 1.0 / count as f32 {
                random_key = Some(entry.key().clone());
            }

            // Break early for large datasets to maintain O(1) average performance
            if count >= max_samples {
                break;
            }
        }

        Ok(random_key)
    }

    /// Get a reference to a specific database by logical index (0-15).
    /// Uses db_mapping for logical-to-physical translation.
    pub fn get_db(&self, index: usize) -> Option<&DashMap<Vec<u8>, StorageItem>> {
        if index < NUM_DATABASES {
            let mapping = self.db_mapping.load(Ordering::Acquire);
            let physical_idx = Self::physical_index_from_mapping(mapping, index);
            Some(self.get_physical_db(physical_idx))
        } else {
            None
        }
    }

    /// Touch a key (update its last access time). Returns true if the key exists.
    pub async fn touch(&self, key: &[u8]) -> StorageResult<bool> {
        if let Some(mut entry) = self.active_db().get_mut(key) {
            let item = entry.value_mut();
            if item.is_expired() {
                drop(entry);
                self.remove_expired_key(key).await;
                return Ok(false);
            }
            item.touch();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Set expiration using an absolute Unix timestamp (seconds since epoch)
    pub async fn expire_at(&self, key: &[u8], unix_timestamp: i64) -> StorageResult<bool> {
        let now_system = SystemTime::now();
        let now_unix = now_system
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        if unix_timestamp <= now_unix {
            // Timestamp is in the past - delete the key
            return self.del(key).await;
        }

        let duration_from_now = Duration::from_secs((unix_timestamp - now_unix) as u64);
        self.expire(key, Some(duration_from_now)).await
    }

    /// Set expiration using an absolute Unix timestamp in milliseconds
    pub async fn pexpire_at(&self, key: &[u8], unix_timestamp_ms: i64) -> StorageResult<bool> {
        let now_system = SystemTime::now();
        let now_unix_ms = now_system
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        if unix_timestamp_ms <= now_unix_ms {
            // Timestamp is in the past - delete the key
            return self.del(key).await;
        }

        let duration_from_now = Duration::from_millis((unix_timestamp_ms - now_unix_ms) as u64);
        self.expire(key, Some(duration_from_now)).await
    }

    /// Get the absolute Unix timestamp (seconds) when a key will expire
    pub async fn expiretime(&self, key: &[u8]) -> StorageResult<Option<i64>> {
        if let Some(entry) = self.active_db().get(key) {
            if entry.value().is_expired() {
                drop(entry);
                self.remove_expired_key(key).await;
                return Ok(None);
            }
            if let Some(expires_at) = entry.value().expires_at {
                let now = Instant::now();
                if expires_at <= now {
                    return Ok(None);
                }
                let remaining = expires_at - now;
                let now_system = SystemTime::now();
                let expire_system = now_system + remaining;
                let unix_ts = expire_system
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                Ok(Some(unix_ts))
            } else {
                // Key exists but has no expiration: return -1 (handled by caller)
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Get the absolute Unix timestamp (milliseconds) when a key will expire
    pub async fn pexpiretime(&self, key: &[u8]) -> StorageResult<Option<i64>> {
        if let Some(entry) = self.active_db().get(key) {
            if entry.value().is_expired() {
                drop(entry);
                self.remove_expired_key(key).await;
                return Ok(None);
            }
            if let Some(expires_at) = entry.value().expires_at {
                let now = Instant::now();
                if expires_at <= now {
                    return Ok(None);
                }
                let remaining = expires_at - now;
                let now_system = SystemTime::now();
                let expire_system = now_system + remaining;
                let unix_ms = expire_system
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                Ok(Some(unix_ms))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Copy a key to a new destination key. Optionally replace existing destination.
    pub async fn copy_key(&self, src: &[u8], dst: Vec<u8>, replace: bool) -> StorageResult<bool> {
        let db = self.active_db();
        // Get the source item
        let src_item = if let Some(entry) = db.get(src) {
            if entry.value().is_expired() {
                drop(entry);
                self.remove_expired_key(src).await;
                return Ok(false);
            }
            entry.value().clone()
        } else {
            return Ok(false);
        };

        // Clone with fresh timestamps but same TTL
        let mut new_item =
            StorageItem::new_with_type(src_item.value.clone(), src_item.data_type.clone());
        if let Some(expires_at) = src_item.expires_at {
            let now = Instant::now();
            if expires_at > now {
                let remaining = expires_at - now;
                new_item.expire(remaining);
                self.add_to_expiration_queue(dst.clone(), Instant::now() + remaining)
                    .await;
            }
        }

        let required_size = Self::calculate_size(&dst, &src_item.value);

        // When replace=false, do a quick non-locking check to avoid evicting unrelated
        // keys for a COPY that will be a no-op.  The authoritative check is still inside
        // the entry API below (under the shard lock), so a concurrent insert between
        // this peek and the entry() call is handled correctly.
        if !replace
            && let Some(existing) = db.get(&dst)
            && !existing.is_expired()
        {
            return Ok(false);
        }

        self.maybe_evict(required_size).await?;

        // Use entry API for atomic check-and-insert, eliminating the TOCTOU race
        // where a concurrent insert between a separate exists-check and the entry()
        // call could be silently overwritten even when replace=false.
        use dashmap::mapref::entry::Entry;
        let is_new_key = match db.entry(dst.clone()) {
            Entry::Occupied(mut occ) => {
                // Atomically check replace permission under the shard lock
                if !replace && !occ.get().is_expired() {
                    return Ok(false);
                }
                let old_size = Self::calculate_size(&dst, &occ.get().value) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::AcqRel);
                occ.insert(new_item);
                // Bump version while OccupiedEntry still holds the shard lock
                self.bump_key_version(&dst);
                false
            }
            Entry::Vacant(vac) => {
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::AcqRel);
                // Bind RefMut to keep shard lock held during version bump
                let _ref = vac.insert(new_item);
                self.bump_key_version(&dst);
                true
            }
        };

        if is_new_key {
            self.update_prefix_indices(&dst, true).await;
            self.key_count.fetch_add(1, Ordering::AcqRel);
        }
        self.increment_write_counters().await;
        Ok(true)
    }

    /// Move a key from the current (active) database to another database.
    /// Acquires a shared cross-DB lock to prevent snapshot_all_dbs from running
    /// concurrently, which would otherwise see inconsistent cross-DB state.
    ///
    /// The source key is atomically extracted (entry API remove) before touching the
    /// destination, preventing a TOCTOU race where a concurrent write to the source
    /// between a clone and a delete would be silently lost.
    pub async fn move_key(&self, key: &[u8], dst_db: usize) -> StorageResult<bool> {
        let _cross_db_guard = self.cross_db_lock.read().await;
        let src_db_idx = CURRENT_DB_INDEX.try_with(|v| *v).unwrap_or(0);
        if dst_db == src_db_idx {
            return Ok(false); // Cannot move to the same database
        }
        if dst_db >= NUM_DATABASES {
            return Ok(false);
        }

        let dst = self.get_db(dst_db).unwrap();
        let src = self.active_db();

        // Atomically extract the source item using the entry API.
        // This eliminates the TOCTOU race where a concurrent SET to the source
        // between a clone() and a del() would lose the concurrent write's data.
        use dashmap::mapref::entry::Entry;
        let (src_key, src_item) = match src.entry(key.to_vec()) {
            Entry::Occupied(occ) => {
                if occ.get().is_expired() {
                    let (k, item) = occ.remove_entry();
                    // Bookkeeping for expired key removal
                    let size = Self::calculate_size(&k, &item.value) as u64;
                    self.current_memory.fetch_sub(size, Ordering::AcqRel);
                    self.key_count.fetch_sub(1, Ordering::AcqRel);
                    return Ok(false);
                }
                occ.remove_entry()
            }
            Entry::Vacant(_) => return Ok(false),
        };

        // Source is now atomically removed.  Try to insert into destination.
        let src_expires_at = src_item.expires_at;
        match dst.entry(key.to_vec()) {
            Entry::Occupied(_) => {
                // Destination already exists - MOVE fails (Redis semantics).
                // Re-insert the source item to restore original state.
                src.insert(src_key, src_item);
                return Ok(false);
            }
            Entry::Vacant(vac) => {
                drop(vac.insert(src_item));
            }
        }

        // Move succeeded - do bookkeeping for both source removal and destination insertion.
        // Memory: source removal subtracts, destination insertion adds the same size → net 0.
        // Key count: source removal subtracts 1, destination insertion adds 1 → net 0.
        // So neither global counter needs updating.

        // Source-side bookkeeping: prefix index removal and WATCH invalidation.
        self.update_prefix_indices(&src_key, false).await;
        self.bump_key_version(&src_key);

        // Destination-side bookkeeping: prefix index, WATCH, and TTL re-enqueue.
        self.update_prefix_indices_for_db(key, true, dst_db).await;
        self.bump_key_version_for_db(key, dst_db);

        if let Some(expires_at) = src_expires_at
            && expires_at > Instant::now()
        {
            self.add_to_expiration_queue_for_db(key.to_vec(), expires_at, dst_db)
                .await;
        }

        Ok(true)
    }

    /// Swap two databases atomically using logical-to-physical mapping.
    /// Instead of physically moving data (collect/clear/reinsert which races with concurrent
    /// writers), this swaps the db_mapping entries so logical indices point to swapped physical
    /// databases. The data swap is effectively instant, eliminating the race window.
    pub async fn swap_db(&self, db1: usize, db2: usize) -> StorageResult<()> {
        if db1 >= NUM_DATABASES || db2 >= NUM_DATABASES {
            return Err(StorageError::InvalidDatabase);
        }
        if db1 == db2 {
            return Ok(());
        }

        // Acquire exclusive cross-DB lock to prevent concurrent snapshots and cross-DB ops
        let _guard = self.cross_db_lock.write().await;

        // Compute the new mapping but do NOT store it yet.  We rewrite the
        // expiration queue and prefix index first, while concurrent writers still
        // use the OLD mapping and therefore add entries with old-style logical
        // db indices.  Only after the metadata is consistent do we flip the
        // mapping in a single atomic store.  This eliminates the window where a
        // post-swap concurrent write's metadata entry gets incorrectly
        // double-swapped by the rewrite.
        let mapping = self.db_mapping.load(Ordering::Acquire);
        let phys1 = Self::physical_index_from_mapping(mapping, db1);
        let phys2 = Self::physical_index_from_mapping(mapping, db2);
        let mut new_mapping = mapping;
        new_mapping &= !(0xF_u64 << (db1 * 4)); // Clear db1's nibble
        new_mapping |= (phys2 as u64) << (db1 * 4); // Set db1 → phys2
        new_mapping &= !(0xF_u64 << (db2 * 4)); // Clear db2's nibble
        new_mapping |= (phys1 as u64) << (db2 * 4); // Set db2 → phys1

        // Fix expiration queue BEFORE the mapping swap: entries still carry
        // pre-swap logical db indices, so the rewrite is straightforward.
        {
            let mut queue = self.expiration_queue.write().await;
            let old_entries: Vec<ExpirationEntry> = std::iter::from_fn(|| queue.pop()).collect();
            for entry in old_entries {
                let new_db_index = if entry.db_index == db1 {
                    db2
                } else if entry.db_index == db2 {
                    db1
                } else {
                    entry.db_index
                };
                queue.push(ExpirationEntry {
                    expires_at: entry.expires_at,
                    key: entry.key,
                    db_index: new_db_index,
                });
            }
        }

        // Swap prefix index entries BEFORE the mapping swap.
        {
            let mut prefix_index = self.prefix_index.write().await;
            // Collect entries for both databases
            let db1_entries: Vec<_> = prefix_index
                .iter()
                .filter(|((idx, _), _)| *idx == db1)
                .map(|((_, prefix), keys)| (prefix.clone(), keys.clone()))
                .collect();
            let db2_entries: Vec<_> = prefix_index
                .iter()
                .filter(|((idx, _), _)| *idx == db2)
                .map(|((_, prefix), keys)| (prefix.clone(), keys.clone()))
                .collect();
            // Remove old entries for both databases
            prefix_index.retain(|(idx, _), _| *idx != db1 && *idx != db2);
            // Re-insert with swapped db indices
            for (prefix, keys) in db1_entries {
                prefix_index.insert((db2, prefix), keys);
            }
            for (prefix, keys) in db2_entries {
                prefix_index.insert((db1, prefix), keys);
            }
        }

        // NOW flip the mapping in a single atomic store.  From this point on,
        // active_db()/get_db() resolve logical indices through the new mapping.
        self.db_mapping.store(new_mapping, Ordering::Release);

        // Bump key versions for all keys in both databases to invalidate WATCHes.
        // Use the physical db references directly since we just swapped the mapping.
        let db1_phys = self.get_physical_db(phys1);
        let db2_phys = self.get_physical_db(phys2);
        for entry in db1_phys.iter() {
            self.bump_key_version_for_db(entry.key(), db1);
            self.bump_key_version_for_db(entry.key(), db2);
        }
        for entry in db2_phys.iter() {
            self.bump_key_version_for_db(entry.key(), db1);
            self.bump_key_version_for_db(entry.key(), db2);
        }

        Ok(())
    }

    /// Get a raw StorageItem reference for OBJECT/DUMP commands
    pub async fn get_item(&self, key: &[u8]) -> StorageResult<Option<StorageItem>> {
        if let Some(entry) = self.active_db().get(key) {
            if entry.value().is_expired() {
                drop(entry);
                self.remove_expired_key(key).await;
                return Ok(None);
            }
            Ok(Some(entry.value().clone()))
        } else {
            Ok(None)
        }
    }

    /// Get the idle time (seconds since last access) for a key
    pub async fn get_idle_time(&self, key: &[u8]) -> StorageResult<Option<u64>> {
        if let Some(entry) = self.active_db().get(key) {
            if entry.value().is_expired() {
                drop(entry);
                self.remove_expired_key(key).await;
                return Ok(None);
            }
            let idle = entry.value().last_accessed.elapsed().as_secs();
            Ok(Some(idle))
        } else {
            Ok(None)
        }
    }

    /// Get the encoding type string for a key (for OBJECT ENCODING)
    ///
    /// Returns encoding strings matching Redis 7.x behavior:
    /// - String: "int" (parseable as i64), "embstr" (<=44 bytes), "raw" (>44 bytes)
    /// - List: "listpack" (<=128 elements, all <=64 bytes), "quicklist" otherwise
    /// - Set: "listpack" (<=128 elements, all <=64 bytes), "hashtable" otherwise
    /// - Hash: "listpack" (<=128 fields, all keys+values <=64 bytes), "hashtable" otherwise
    /// - ZSet: "listpack" (<=128 elements, all <=64 bytes), "skiplist" otherwise
    /// - Stream: "stream"
    pub async fn get_encoding(&self, key: &[u8]) -> StorageResult<Option<String>> {
        if let Some(entry) = self.active_db().get(key) {
            if entry.value().is_expired() {
                drop(entry);
                self.remove_expired_key(key).await;
                return Ok(None);
            }
            let encoding = match &entry.value().data_type {
                RedisDataType::String => {
                    if let Ok(s) = std::str::from_utf8(&entry.value().value) {
                        if s.parse::<i64>().is_ok() {
                            "int"
                        } else if entry.value().value.len() <= 44 {
                            "embstr"
                        } else {
                            "raw"
                        }
                    } else {
                        "raw"
                    }
                }
                RedisDataType::List => {
                    if let Ok(list) = bincode::deserialize::<Vec<Vec<u8>>>(&entry.value().value) {
                        if list.len() <= 128 && list.iter().all(|el| el.len() <= 64) {
                            "listpack"
                        } else {
                            "quicklist"
                        }
                    } else {
                        "quicklist"
                    }
                }
                RedisDataType::Set => {
                    if let Ok(set) = bincode::deserialize::<HashSet<Vec<u8>>>(&entry.value().value)
                    {
                        if set.len() <= 128 && set.iter().all(|el| el.len() <= 64) {
                            "listpack"
                        } else {
                            "hashtable"
                        }
                    } else {
                        "hashtable"
                    }
                }
                RedisDataType::Hash => {
                    if let Ok(map) =
                        bincode::deserialize::<HashMap<Vec<u8>, Vec<u8>>>(&entry.value().value)
                    {
                        if map.len() <= 128
                            && map.iter().all(|(k, v)| k.len() <= 64 && v.len() <= 64)
                        {
                            "listpack"
                        } else {
                            "hashtable"
                        }
                    } else {
                        "hashtable"
                    }
                }
                RedisDataType::ZSet => {
                    use crate::command::types::sorted_set::SortedSetData;
                    if let Ok(ss) = bincode::deserialize::<SortedSetData>(&entry.value().value) {
                        if ss.scores.len() <= 128 && ss.scores.keys().all(|k| k.len() <= 64) {
                            "listpack"
                        } else {
                            "skiplist"
                        }
                    } else {
                        "skiplist"
                    }
                }
                RedisDataType::Stream => "stream",
            };
            Ok(Some(encoding.to_string()))
        } else {
            Ok(None)
        }
    }

    /// Restore a key from a serialized value (for RESTORE command)
    pub async fn restore_key(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        data_type: RedisDataType,
        ttl: Option<Duration>,
        replace: bool,
    ) -> StorageResult<bool> {
        // Check if key exists
        if self.active_db().contains_key(&key) {
            if !replace {
                return Err(StorageError::KeyExists);
            }
            // Remove old key
            self.del(&key).await?;
        }

        self.set_with_type(key, value, data_type, ttl).await?;
        Ok(true)
    }

    /// Count keys that belong to a given hash slot (cluster mode uses logical DB 0)
    pub async fn count_keys_in_slot(&self, slot: u16) -> usize {
        use crate::cluster::slot::key_hash_slot;
        let db = self.get_db_by_index(0);
        let mut count = 0;
        for entry in db.iter() {
            if key_hash_slot(entry.key()) == slot {
                count += 1;
            }
        }
        count
    }

    /// Get keys that belong to a given hash slot (up to count, cluster mode uses logical DB 0)
    pub async fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<Vec<u8>> {
        use crate::cluster::slot::key_hash_slot;
        let db = self.get_db_by_index(0);
        let mut keys = Vec::new();
        for entry in db.iter() {
            if key_hash_slot(entry.key()) == slot {
                keys.push(entry.key().clone());
                if keys.len() >= count {
                    break;
                }
            }
        }
        keys
    }

    /// Dump a key's value as serialized bytes (for MIGRATE)
    pub async fn dump_key(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(entry) = self.active_db().get(key) {
            if entry.is_expired() {
                // Key is expired; clean it up and return None
                drop(entry);
                self.remove_expired_key(key).await;
                None
            } else {
                Some(entry.value.clone())
            }
        } else {
            None
        }
    }

    // ========== Atomic Primitives ==========

    /// Atomically set a key only if it does NOT exist (SET NX).
    /// Returns (inserted, version) where version is the key version set on insert (0 if not inserted).
    /// Returning the version allows callers (e.g. MSETNX rollback) to detect concurrent overwrites
    /// without a gap between insert and version capture.
    #[allow(dead_code)]
    pub async fn set_nx(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> StorageResult<(bool, u64)> {
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }
        if value.len() > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }

        let required_size = Self::calculate_size(&key, &value);
        self.maybe_evict(required_size).await?;

        let mut item = StorageItem::new(value);
        let expires_at = if let Some(ttl) = ttl {
            item.expire(ttl);
            Some(Instant::now() + ttl)
        } else {
            None
        };

        use dashmap::mapref::entry::Entry;
        // Capture version inside the entry match block while the DashMap shard lock
        // is still held. This prevents a concurrent SET from overwriting the key
        // between our insert and version bump, which would cause MSETNX rollback
        // to incorrectly delete the concurrent writer's value.
        let (inserted, replaced_expired, version) = match self.active_db().entry(key.clone()) {
            Entry::Occupied(occ) => {
                // Key exists - check if expired
                if occ.get().is_expired() {
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    let mut occ = occ;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    self.current_memory
                        .fetch_add(required_size as u64, Ordering::AcqRel);
                    occ.insert(item);
                    // Bump version while OccupiedEntry still holds the shard lock
                    let v = self.bump_and_get_key_version(&key);
                    // Replaced expired key - DashMap entry already exists, don't increment key_count
                    (true, true, v)
                } else {
                    (false, false, 0)
                }
            }
            Entry::Vacant(vac) => {
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::AcqRel);
                // insert() returns a RefMut that holds the shard lock
                let _ref = vac.insert(item);
                // Bump version while RefMut still holds the shard lock
                let v = self.bump_and_get_key_version(&key);
                (true, false, v)
            }
        };

        if inserted {
            if let Some(expires_at) = expires_at {
                self.add_to_expiration_queue(key.clone(), expires_at).await;
            }
            self.update_prefix_indices(&key, true).await;
            if !replaced_expired {
                self.key_count.fetch_add(1, Ordering::AcqRel);
            }
            self.increment_write_counters().await;
        }

        Ok((inserted, version))
    }

    /// Atomically set a key only if it DOES exist (SET XX).
    /// Returns true if the key was set, false if it did not exist.
    #[allow(dead_code)]
    pub async fn set_xx(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> StorageResult<bool> {
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }
        if value.len() > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }

        let required_size = Self::calculate_size(&key, &value);
        self.maybe_evict(required_size).await?;

        let mut item = StorageItem::new(value);
        let expires_at = if let Some(ttl) = ttl {
            item.expire(ttl);
            Some(Instant::now() + ttl)
        } else {
            None
        };

        use dashmap::mapref::entry::Entry;
        let updated = match self.active_db().entry(key.clone()) {
            Entry::Occupied(mut occ) => {
                if occ.get().is_expired() {
                    // Expired key doesn't count as existing
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    occ.remove();
                    self.key_count.fetch_sub(1, Ordering::AcqRel);
                    false
                } else {
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    self.current_memory
                        .fetch_add(required_size as u64, Ordering::AcqRel);
                    occ.insert(item);
                    // Bump version while OccupiedEntry still holds the shard lock
                    self.bump_key_version(&key);
                    true
                }
            }
            Entry::Vacant(_) => false,
        };

        if updated {
            if let Some(expires_at) = expires_at {
                self.add_to_expiration_queue(key.clone(), expires_at).await;
            }
            self.increment_write_counters().await;
        }

        Ok(updated)
    }

    /// Unified SET with all flag combinations (NX, XX, GET).
    /// Returns SetResult indicating what happened.
    #[allow(clippy::too_many_arguments)]
    pub async fn set_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        nx: bool,
        xx: bool,
        get_old: bool,
        keepttl: bool,
    ) -> StorageResult<SetResult> {
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }
        if value.len() > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }

        let required_size = Self::calculate_size(&key, &value);
        self.maybe_evict(required_size).await?;

        let mut item = StorageItem::new(value);
        let new_expires_at = if let Some(ttl) = ttl {
            item.expire(ttl);
            Some(Instant::now() + ttl)
        } else {
            None
        };

        use dashmap::mapref::entry::Entry;
        let (did_set, old_value, is_new_key, final_expires_at) =
            match self.active_db().entry(key.clone()) {
                Entry::Occupied(mut occ) => {
                    let expired = occ.get().is_expired();
                    let effectively_exists = !expired;

                    if nx && effectively_exists {
                        // NX: don't set if key exists
                        let old = if get_old {
                            Some(occ.get().value.clone())
                        } else {
                            None
                        };
                        (false, old, false, None)
                    } else if xx && !effectively_exists {
                        // XX: don't set if key doesn't exist
                        if expired {
                            let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                            self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                            occ.remove();
                            self.key_count.fetch_sub(1, Ordering::AcqRel);
                        }
                        (false, None, false, None)
                    } else {
                        // Set the value
                        let old = if get_old && effectively_exists {
                            Some(occ.get().value.clone())
                        } else {
                            None
                        };
                        // KEEPTTL: preserve old expiration if key exists and keepttl is set
                        let preserved_expires = if keepttl && effectively_exists {
                            occ.get().expires_at
                        } else {
                            None
                        };
                        let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                        self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                        self.current_memory
                            .fetch_add(required_size as u64, Ordering::AcqRel);
                        if keepttl && effectively_exists {
                            // Preserve the old TTL by copying expires_at
                            item.expires_at = preserved_expires;
                        }
                        occ.insert(item);
                        // Bump version while OccupiedEntry still holds the shard lock,
                        // preventing TOCTOU race with del_if_version_matches (MSETNX rollback).
                        self.bump_key_version(&key);
                        let effective_expires = if keepttl && effectively_exists {
                            preserved_expires
                        } else {
                            new_expires_at
                        };
                        // is_new_key is false for Occupied entries: the DashMap entry already
                        // exists (even if expired), so key_count must not be incremented.
                        (true, old, false, effective_expires)
                    }
                }
                Entry::Vacant(vac) => {
                    if xx {
                        // XX: key must exist
                        (false, None, false, None)
                    } else {
                        self.current_memory
                            .fetch_add(required_size as u64, Ordering::AcqRel);
                        // Bind RefMut to keep shard lock held during version bump
                        let _ref = vac.insert(item);
                        self.bump_key_version(&key);
                        (true, None, true, new_expires_at)
                    }
                }
            };

        if did_set {
            if let Some(expires_at) = final_expires_at {
                self.add_to_expiration_queue(key.clone(), expires_at).await;
            }
            if is_new_key {
                self.update_prefix_indices(&key, true).await;
                self.key_count.fetch_add(1, Ordering::AcqRel);
            }
            self.increment_write_counters().await;
        }

        if get_old {
            Ok(SetResult::OldValue(old_value))
        } else if did_set {
            Ok(SetResult::Ok)
        } else {
            Ok(SetResult::NotSet)
        }
    }

    /// Atomically increment a key's integer value by delta.
    /// If the key doesn't exist, it is created with value 0 before incrementing.
    /// Returns the new value after increment.
    pub fn atomic_incr(&self, key: &[u8], delta: i64) -> StorageResult<i64> {
        use dashmap::mapref::entry::Entry;
        match self.active_db().entry(key.to_vec()) {
            Entry::Occupied(mut occ) => {
                if occ.get().is_expired() {
                    // Treat expired as non-existent: set to delta
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    let new_val_str = delta.to_string();
                    let new_val_bytes = new_val_str.as_bytes().to_vec();
                    let new_size = Self::calculate_size(occ.key(), &new_val_bytes) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    self.current_memory.fetch_add(new_size, Ordering::AcqRel);
                    let mut new_item = StorageItem::new(new_val_bytes);
                    new_item.expires_at = None;
                    occ.insert(new_item);
                    self.bump_key_version(key);
                    return Ok(delta);
                }
                // Check type
                if occ.get().data_type != RedisDataType::String {
                    return Err(StorageError::WrongType);
                }
                // Parse current value
                let current_str =
                    std::str::from_utf8(&occ.get().value).map_err(|_| StorageError::NotANumber)?;
                let current: i64 = current_str.parse().map_err(|_| StorageError::NotANumber)?;
                let new_val = current.checked_add(delta).ok_or(StorageError::NotANumber)?;
                let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                let new_val_str = new_val.to_string();
                let new_val_bytes = new_val_str.as_bytes().to_vec();
                let new_size = Self::calculate_size(occ.key(), &new_val_bytes) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                self.current_memory.fetch_add(new_size, Ordering::AcqRel);
                occ.get_mut().value = new_val_bytes;
                occ.get_mut().touch();
                self.bump_key_version(key);
                Ok(new_val)
            }
            Entry::Vacant(vac) => {
                let new_val_str = delta.to_string();
                let new_val_bytes = new_val_str.as_bytes().to_vec();
                let size = Self::calculate_size(key, &new_val_bytes) as u64;
                self.current_memory.fetch_add(size, Ordering::AcqRel);
                self.key_count.fetch_add(1, Ordering::AcqRel);
                vac.insert(StorageItem::new(new_val_bytes));
                self.bump_key_version(key);
                Ok(delta)
            }
        }
    }

    /// Atomically increment a key's float value by delta.
    /// If the key doesn't exist, it is created with value 0 before incrementing.
    /// Returns the new value after increment.
    pub fn atomic_incr_float(&self, key: &[u8], delta: f64) -> StorageResult<f64> {
        use dashmap::mapref::entry::Entry;
        match self.active_db().entry(key.to_vec()) {
            Entry::Occupied(mut occ) => {
                if occ.get().is_expired() {
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    let new_val_str = format_float(delta);
                    let new_val_bytes = new_val_str.as_bytes().to_vec();
                    let new_size = Self::calculate_size(occ.key(), &new_val_bytes) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    self.current_memory.fetch_add(new_size, Ordering::AcqRel);
                    let mut new_item = StorageItem::new(new_val_bytes);
                    new_item.expires_at = None;
                    occ.insert(new_item);
                    self.bump_key_version(key);
                    return Ok(delta);
                }
                if occ.get().data_type != RedisDataType::String {
                    return Err(StorageError::WrongType);
                }
                let current_str =
                    std::str::from_utf8(&occ.get().value).map_err(|_| StorageError::NotAFloat)?;
                let current: f64 = current_str.parse().map_err(|_| StorageError::NotAFloat)?;
                if current.is_nan() || current.is_infinite() {
                    return Err(StorageError::NotAFloat);
                }
                let new_val = current + delta;
                if new_val.is_nan() || new_val.is_infinite() {
                    return Err(StorageError::NotAFloat);
                }
                let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                let new_val_str = format_float(new_val);
                let new_val_bytes = new_val_str.as_bytes().to_vec();
                let new_size = Self::calculate_size(occ.key(), &new_val_bytes) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                self.current_memory.fetch_add(new_size, Ordering::AcqRel);
                occ.get_mut().value = new_val_bytes;
                occ.get_mut().touch();
                self.bump_key_version(key);
                Ok(new_val)
            }
            Entry::Vacant(vac) => {
                let new_val_str = format_float(delta);
                let new_val_bytes = new_val_str.as_bytes().to_vec();
                let size = Self::calculate_size(key, &new_val_bytes) as u64;
                self.current_memory.fetch_add(size, Ordering::AcqRel);
                self.key_count.fetch_add(1, Ordering::AcqRel);
                vac.insert(StorageItem::new(new_val_bytes));
                self.bump_key_version(key);
                Ok(delta)
            }
        }
    }

    /// Atomically append a value to a key's string value.
    /// If the key doesn't exist, it is created with the appended value.
    /// Returns the length of the string after append.
    pub fn atomic_append(&self, key: &[u8], append_value: &[u8]) -> StorageResult<usize> {
        use dashmap::mapref::entry::Entry;
        match self.active_db().entry(key.to_vec()) {
            Entry::Occupied(mut occ) => {
                if occ.get().is_expired() {
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    let new_val = append_value.to_vec();
                    let new_len = new_val.len();
                    let new_size = Self::calculate_size(occ.key(), &new_val) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    self.current_memory.fetch_add(new_size, Ordering::AcqRel);
                    let mut new_item = StorageItem::new(new_val);
                    new_item.expires_at = None;
                    occ.insert(new_item);
                    self.bump_key_version(key);
                    return Ok(new_len);
                }
                if occ.get().data_type != RedisDataType::String {
                    return Err(StorageError::WrongType);
                }
                let old_val_size = occ.get().value.len();
                let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                occ.get_mut().value.extend_from_slice(append_value);
                let new_len = occ.get().value.len();
                let new_size = old_size + (new_len - old_val_size) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                self.current_memory.fetch_add(new_size, Ordering::AcqRel);
                occ.get_mut().touch();
                self.bump_key_version(key);
                Ok(new_len)
            }
            Entry::Vacant(vac) => {
                let new_val = append_value.to_vec();
                let new_len = new_val.len();
                let size = Self::calculate_size(key, &new_val) as u64;
                self.current_memory.fetch_add(size, Ordering::AcqRel);
                self.key_count.fetch_add(1, Ordering::AcqRel);
                vac.insert(StorageItem::new(new_val));
                self.bump_key_version(key);
                Ok(new_len)
            }
        }
    }

    /// Generic atomic read-modify-write for collection types.
    /// The closure receives `Option<&mut Vec<u8>>` (None if key doesn't exist or is expired)
    /// and returns `Option<Vec<u8>>` (None to delete the key, Some to set the new value).
    /// The closure runs while holding the DashMap entry lock, ensuring atomicity.
    /// Returns the closure's return value.
    pub fn atomic_modify<F, R>(
        &self,
        key: &[u8],
        data_type: RedisDataType,
        f: F,
    ) -> StorageResult<R>
    where
        F: FnOnce(Option<&mut Vec<u8>>) -> Result<(Option<Vec<u8>>, R), StorageError>,
    {
        use dashmap::mapref::entry::Entry;
        match self.active_db().entry(key.to_vec()) {
            Entry::Occupied(mut occ) => {
                let expired = occ.get().is_expired();
                if !expired && occ.get().data_type != data_type {
                    return Err(StorageError::WrongType);
                }
                let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;

                let (new_val, result) = if expired {
                    f(None)?
                } else {
                    f(Some(&mut occ.get_mut().value))?
                };

                match new_val {
                    Some(val) => {
                        let new_size = Self::calculate_size(key, &val) as u64;
                        self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                        self.current_memory.fetch_add(new_size, Ordering::AcqRel);
                        if expired {
                            let mut new_item = StorageItem::new_with_type(val, data_type);
                            new_item.expires_at = None;
                            occ.insert(new_item);
                        } else {
                            occ.get_mut().value = val;
                            occ.get_mut().touch();
                        }
                    }
                    None => {
                        // Delete the key
                        self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                        occ.remove();
                        self.key_count.fetch_sub(1, Ordering::AcqRel);
                    }
                }
                self.bump_key_version(key);
                self.increment_write_counters_sync();
                Ok(result)
            }
            Entry::Vacant(vac) => {
                let (new_val, result) = f(None)?;
                if let Some(val) = new_val {
                    let size = Self::calculate_size(key, &val) as u64;
                    self.current_memory.fetch_add(size, Ordering::AcqRel);
                    self.key_count.fetch_add(1, Ordering::AcqRel);
                    vac.insert(StorageItem::new_with_type(val, data_type));
                    self.bump_key_version(key);
                    self.increment_write_counters_sync();
                }
                Ok(result)
            }
        }
    }

    /// Atomic read access to a key with LRU/LFU tracking. Uses an exclusive shard lock
    /// (DashMap `get_mut()`) to update last-accessed time for correct eviction behavior.
    /// Does NOT bump key version or write data back. Returns `(result, was_expired)` where
    /// `was_expired` indicates the caller should trigger lazy expiration cleanup.
    pub fn atomic_read<F, R>(&self, key: &[u8], data_type: RedisDataType, f: F) -> StorageResult<R>
    where
        F: FnOnce(Option<&Vec<u8>>) -> Result<R, StorageError>,
    {
        match self.active_db().get_mut(key) {
            Some(mut entry) => {
                if entry.value().is_expired() {
                    f(None)
                } else if entry.value().data_type != data_type {
                    Err(StorageError::WrongType)
                } else {
                    entry.value_mut().touch();
                    f(Some(&entry.value().value))
                }
            }
            None => f(None),
        }
    }

    /// Atomically get and delete a key. Returns the old value if it existed.
    /// Returns Err(WrongType) if the key exists but is not a string type.
    pub fn atomic_getdel(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        use dashmap::mapref::entry::Entry;
        match self.active_db().entry(key.to_vec()) {
            Entry::Occupied(occ) => {
                if occ.get().is_expired() {
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    self.key_count.fetch_sub(1, Ordering::AcqRel);
                    occ.remove();
                    self.bump_key_version(key);
                    Ok(None)
                } else if occ.get().data_type != RedisDataType::String {
                    Err(StorageError::WrongType)
                } else {
                    let old_value = occ.get().value.clone();
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    self.key_count.fetch_sub(1, Ordering::AcqRel);
                    occ.remove();
                    self.bump_key_version(key);
                    Ok(Some(old_value))
                }
            }
            Entry::Vacant(_) => Ok(None),
        }
    }

    /// Atomically get a string value and optionally modify its expiration.
    /// Used by GETEX for thread-safe get-with-expiry-change.
    ///
    /// `new_expiry`:
    ///   - None: don't change expiry
    ///   - Some(None): remove expiry (PERSIST)
    ///   - Some(Some(duration)): set new expiry
    ///
    /// Returns the value if the key exists and is a string type.
    pub async fn atomic_get_set_expiry(
        &self,
        key: &[u8],
        new_expiry: Option<Option<Duration>>,
    ) -> StorageResult<Option<Vec<u8>>> {
        // Phase 1: Synchronous DashMap access (holds shard lock)
        let (value, queue_action, expiry_changed) = {
            if let Some(mut entry) = self.active_db().get_mut(key) {
                let item = entry.value_mut();

                if item.is_expired() {
                    (None, None, false)
                } else if item.data_type != RedisDataType::String {
                    return Err(StorageError::WrongType);
                } else {
                    let value = item.value.clone();

                    let (queue_action, changed) = match new_expiry {
                        None => (None, false),
                        Some(None) => {
                            item.remove_expiry();
                            (None, true)
                        }
                        Some(Some(duration)) => {
                            item.expire(duration);
                            let expires_at = Instant::now() + duration;
                            (Some(expires_at), true)
                        }
                    };

                    item.touch();
                    (Some(value), queue_action, changed)
                }
            } else {
                (None, None, false)
            }
            // DashMap shard lock released here
        };

        // Phase 2: Async expiration queue work (no DashMap lock held)
        if let Some(expires_at) = queue_action {
            self.add_to_expiration_queue(key.to_vec(), expires_at).await;
        }

        // Bump key version so WATCH detects the expiry mutation
        if expiry_changed {
            self.bump_key_version(key);
        }

        Ok(value)
    }

    /// Atomically get the old value and set a new value.
    pub async fn atomic_getset(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> StorageResult<Option<Vec<u8>>> {
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }
        if value.len() > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }

        let required_size = Self::calculate_size(&key, &value);
        self.maybe_evict(required_size).await?;

        use dashmap::mapref::entry::Entry;
        let old_value = match self.active_db().entry(key.clone()) {
            Entry::Occupied(mut occ) => {
                let expired = occ.get().is_expired();
                if !expired && occ.get().data_type != RedisDataType::String {
                    return Err(StorageError::WrongType);
                }
                let old = if expired {
                    None
                } else {
                    Some(occ.get().value.clone())
                };
                let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::AcqRel);
                let item = StorageItem::new(value);
                occ.insert(item);
                // Bump version while OccupiedEntry still holds the shard lock
                self.bump_key_version(&key);
                old
            }
            Entry::Vacant(vac) => {
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::AcqRel);
                self.key_count.fetch_add(1, Ordering::AcqRel);
                // Bind RefMut to keep shard lock held during version bump
                let _ref = vac.insert(StorageItem::new(value));
                self.bump_key_version(&key);
                None
            }
        };

        self.increment_write_counters().await;
        Ok(old_value)
    }

    // ========== Hash Primitives ==========
    // Hash data is stored as a single key with RedisDataType::Hash.
    // The value is a bincode-serialized HashMap<Vec<u8>, Vec<u8>>.

    /// Deserialize hash data from bytes. Returns empty HashMap if bytes is None.
    fn hash_deserialize(data: Option<&Vec<u8>>) -> HashMap<Vec<u8>, Vec<u8>> {
        match data {
            Some(bytes) if !bytes.is_empty() => bincode::deserialize(bytes).unwrap_or_default(),
            _ => HashMap::new(),
        }
    }

    /// Serialize hash data to bytes.
    fn hash_serialize(map: &HashMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
        bincode::serialize(map).unwrap_or_default()
    }

    /// Set one or more fields in a hash. Returns the number of NEW fields added.
    pub fn hash_set(&self, key: &[u8], fields: &[(&[u8], &[u8])]) -> StorageResult<i64> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let mut map = Self::hash_deserialize(existing.as_deref());
            let mut new_count = 0i64;
            for &(field, value) in fields {
                if map.insert(field.to_vec(), value.to_vec()).is_none() {
                    new_count += 1;
                }
            }
            Ok((Some(Self::hash_serialize(&map)), new_count))
        })
    }

    /// Set a field only if it does NOT exist. Returns true if the field was set.
    pub fn hash_set_nx(&self, key: &[u8], field: &[u8], value: &[u8]) -> StorageResult<bool> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let mut map = Self::hash_deserialize(existing.as_deref());
            if map.contains_key(field) {
                Ok((Some(Self::hash_serialize(&map)), false))
            } else {
                map.insert(field.to_vec(), value.to_vec());
                Ok((Some(Self::hash_serialize(&map)), true))
            }
        })
    }

    /// Get a single field from a hash.
    pub fn hash_get(&self, key: &[u8], field: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| {
            let map = Self::hash_deserialize(existing);
            Ok(map.get(field).cloned())
        })
    }

    /// Get multiple fields from a hash. Returns a Vec of Option<Vec<u8>> in order.
    pub fn hash_mget(&self, key: &[u8], fields: &[&[u8]]) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| {
            let map = Self::hash_deserialize(existing);
            let results: Vec<Option<Vec<u8>>> =
                fields.iter().map(|f| map.get(*f).cloned()).collect();
            Ok(results)
        })
    }

    /// Get all field-value pairs from a hash.
    pub fn hash_getall(&self, key: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| {
            let map = Self::hash_deserialize(existing);
            let pairs: Vec<(Vec<u8>, Vec<u8>)> = map.into_iter().collect();
            Ok(pairs)
        })
    }

    /// Delete one or more fields from a hash. Returns the number of fields removed.
    pub fn hash_del(&self, key: &[u8], fields: &[&[u8]]) -> StorageResult<i64> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            if existing.is_none() {
                return Ok((None, 0));
            }
            let mut map = Self::hash_deserialize(existing.as_deref());
            let mut removed = 0i64;
            for &field in fields {
                if map.remove(field).is_some() {
                    removed += 1;
                }
            }
            if map.is_empty() {
                Ok((None, removed))
            } else {
                Ok((Some(Self::hash_serialize(&map)), removed))
            }
        })
    }

    /// Check if a field exists in a hash.
    pub fn hash_exists(&self, key: &[u8], field: &[u8]) -> StorageResult<bool> {
        self.atomic_read(key, RedisDataType::Hash, |existing| {
            let map = Self::hash_deserialize(existing);
            Ok(map.contains_key(field))
        })
    }

    /// Get the number of fields in a hash.
    pub fn hash_len(&self, key: &[u8]) -> StorageResult<i64> {
        self.atomic_read(key, RedisDataType::Hash, |existing| {
            let map = Self::hash_deserialize(existing);
            Ok(map.len() as i64)
        })
    }

    /// Get all field names in a hash.
    pub fn hash_keys(&self, key: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| {
            let map = Self::hash_deserialize(existing);
            let keys: Vec<Vec<u8>> = map.keys().cloned().collect();
            Ok(keys)
        })
    }

    /// Get all values in a hash.
    pub fn hash_vals(&self, key: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| {
            let map = Self::hash_deserialize(existing);
            let vals: Vec<Vec<u8>> = map.values().cloned().collect();
            Ok(vals)
        })
    }

    /// Increment a hash field's integer value by delta. Returns the new value.
    pub fn hash_incr_by(&self, key: &[u8], field: &[u8], delta: i64) -> StorageResult<i64> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let mut map = Self::hash_deserialize(existing.as_deref());
            let current = match map.get(field) {
                Some(v) => {
                    let s = std::str::from_utf8(v).map_err(|_| StorageError::NotANumber)?;
                    s.parse::<i64>().map_err(|_| StorageError::NotANumber)?
                }
                None => 0,
            };
            let new_val = current.checked_add(delta).ok_or(StorageError::NotANumber)?;
            map.insert(field.to_vec(), new_val.to_string().into_bytes());
            Ok((Some(Self::hash_serialize(&map)), new_val))
        })
    }

    /// Increment a hash field's float value by delta. Returns the new value as string.
    pub fn hash_incr_by_float(
        &self,
        key: &[u8],
        field: &[u8],
        delta: f64,
    ) -> StorageResult<String> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let mut map = Self::hash_deserialize(existing.as_deref());
            let current = match map.get(field) {
                Some(v) => {
                    let s = std::str::from_utf8(v).map_err(|_| StorageError::NotAFloat)?;
                    let f: f64 = s.parse().map_err(|_| StorageError::NotAFloat)?;
                    if !f.is_finite() {
                        return Err(StorageError::NotAFloat);
                    }
                    f
                }
                None => 0.0,
            };
            let new_val = current + delta;
            if !new_val.is_finite() {
                return Err(StorageError::NotAFloat);
            }
            let new_val_str = format_float(new_val);
            map.insert(field.to_vec(), new_val_str.as_bytes().to_vec());
            Ok((Some(Self::hash_serialize(&map)), new_val_str))
        })
    }

    /// Get the string length of a hash field's value.
    pub fn hash_strlen(&self, key: &[u8], field: &[u8]) -> StorageResult<i64> {
        self.atomic_read(key, RedisDataType::Hash, |existing| {
            let map = Self::hash_deserialize(existing);
            Ok(map.get(field).map(|v| v.len() as i64).unwrap_or(0))
        })
    }

    /// Get random fields from a hash. Returns (fields, values) pairs.
    /// If the hash doesn't exist, returns empty vec.
    pub fn hash_randfield(&self, key: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| {
            let map = Self::hash_deserialize(existing);
            let pairs: Vec<(Vec<u8>, Vec<u8>)> = map.into_iter().collect();
            Ok(pairs)
        })
    }
}

/// Format a float value the way Redis does (remove trailing zeros, use integer form when possible)
fn format_float(val: f64) -> String {
    if val == val.trunc() && val.abs() < 1e15 {
        // Integer-like value
        let s = format!("{:.1}", val); // e.g. "3.0" -> keep one decimal for Redis compat
        // But Redis actually outputs "3" for INCRBYFLOAT result when it's integer-like with .0
        // Actually Redis uses %g-like formatting: remove trailing zeros
        let trimmed = s.trim_end_matches('0');
        let trimmed = trimmed.trim_end_matches('.');
        trimmed.to_string()
    } else {
        // Use enough precision
        let s = format!("{:.17}", val);
        let trimmed = s.trim_end_matches('0');
        let trimmed = trimmed.trim_end_matches('.');
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_set_get() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set a value
        storage
            .set(b"key1".to_vec(), b"value1".to_vec(), None)
            .await
            .unwrap();

        // Get the value
        let value = storage.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Get a non-existent key
        let value = storage.get(b"nonexistent").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_expire() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set a value
        storage
            .set(b"expire_key".to_vec(), b"value".to_vec(), None)
            .await
            .unwrap();

        // Set expiration
        let ttl = Duration::from_millis(100);
        storage.expire(b"expire_key", Some(ttl)).await.unwrap();

        // Check TTL
        let remaining = storage.ttl(b"expire_key").await.unwrap();
        assert!(remaining.is_some());
        assert!(remaining.unwrap() <= ttl);

        // Wait for expiration
        sleep(Duration::from_millis(150)).await;

        // Key should be gone
        let value = storage.get(b"expire_key").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_set_with_expiry() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set with expiry
        let ttl = Duration::from_millis(100);
        storage
            .set(
                b"expiring_key".to_vec(),
                b"expiring_value".to_vec(),
                Some(ttl),
            )
            .await
            .unwrap();

        // Check value exists
        let value = storage.get(b"expiring_key").await.unwrap();
        assert_eq!(value, Some(b"expiring_value".to_vec()));

        // Wait for expiration
        sleep(Duration::from_millis(150)).await;

        // Key should be gone
        let value = storage.get(b"expiring_key").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_del() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set multiple keys
        storage
            .set(b"del_key1".to_vec(), b"value1".to_vec(), None)
            .await
            .unwrap();
        storage
            .set(b"del_key2".to_vec(), b"value2".to_vec(), None)
            .await
            .unwrap();
        storage
            .set(b"keep_key".to_vec(), b"keep_value".to_vec(), None)
            .await
            .unwrap();

        // Delete keys one by one
        let deleted1 = storage.del(b"del_key1").await.unwrap();
        let deleted2 = storage.del(b"del_key2").await.unwrap();
        let deleted3 = storage.del(b"nonexistent").await.unwrap();

        assert!(deleted1);
        assert!(deleted2);
        assert!(!deleted3);

        // Check deleted keys
        let val1 = storage.get(b"del_key1").await.unwrap();
        let val2 = storage.get(b"del_key2").await.unwrap();
        let val3 = storage.get(b"keep_key").await.unwrap();

        assert_eq!(val1, None);
        assert_eq!(val2, None);
        assert_eq!(val3, Some(b"keep_value".to_vec()));
    }

    #[tokio::test]
    async fn test_exists() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set a key
        storage
            .set(b"exists_key".to_vec(), b"value".to_vec(), None)
            .await
            .unwrap();

        // Check existence
        let exists = storage.exists(b"exists_key").await.unwrap();
        assert!(exists);

        // Check non-existent key
        let exists = storage.exists(b"nonexistent").await.unwrap();
        assert!(!exists);
    }

    #[tokio::test]
    async fn test_expiry_cleanup() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set keys with short TTL
        for i in 0..5 {
            let key = format!("cleanup_key{}", i).into_bytes();
            storage
                .set(key, b"value".to_vec(), Some(Duration::from_millis(30)))
                .await
                .unwrap();
        }

        // Set one key with longer TTL
        storage
            .set(
                b"cleanup_survivor".to_vec(),
                b"survivor".to_vec(),
                Some(Duration::from_millis(200)),
            )
            .await
            .unwrap();

        // Wait for cleanup to run
        sleep(Duration::from_millis(100)).await;

        // Check that short TTL keys are gone
        for i in 0..5 {
            let key = format!("cleanup_key{}", i);
            let exists = storage.exists(key.as_bytes()).await.unwrap();
            assert!(!exists, "Key '{}' should have been cleaned up", key);
        }

        // Check that longer TTL key still exists
        let exists = storage.exists(b"cleanup_survivor").await.unwrap();
        assert!(exists, "Survivor key should still exist");
    }

    // ========== Atomic Primitives Tests ==========

    #[tokio::test]
    async fn test_set_nx_basic() {
        let storage = StorageEngine::new(StorageConfig::default());

        // First set_nx should succeed
        let (result, version) = storage
            .set_nx(b"nx_key".to_vec(), b"value1".to_vec(), None)
            .await
            .unwrap();
        assert!(result);
        assert!(
            version > 0,
            "version should be non-zero on successful insert"
        );

        // Second set_nx on same key should fail
        let (result, _) = storage
            .set_nx(b"nx_key".to_vec(), b"value2".to_vec(), None)
            .await
            .unwrap();
        assert!(!result);

        // Value should still be the first one
        let val = storage.get(b"nx_key").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_set_nx_expired_key() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Set key with short TTL
        storage
            .set(
                b"nx_exp".to_vec(),
                b"old".to_vec(),
                Some(Duration::from_millis(30)),
            )
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;

        // set_nx should succeed since key is expired
        let (result, _) = storage
            .set_nx(b"nx_exp".to_vec(), b"new".to_vec(), None)
            .await
            .unwrap();
        assert!(result);

        let val = storage.get(b"nx_exp").await.unwrap();
        assert_eq!(val, Some(b"new".to_vec()));
    }

    #[tokio::test]
    async fn test_set_xx_basic() {
        let storage = StorageEngine::new(StorageConfig::default());

        // set_xx on non-existent key should fail
        let result = storage
            .set_xx(b"xx_key".to_vec(), b"value1".to_vec(), None)
            .await
            .unwrap();
        assert!(!result);

        // Set the key first
        storage
            .set(b"xx_key".to_vec(), b"value1".to_vec(), None)
            .await
            .unwrap();

        // set_xx should now succeed
        let result = storage
            .set_xx(b"xx_key".to_vec(), b"value2".to_vec(), None)
            .await
            .unwrap();
        assert!(result);

        let val = storage.get(b"xx_key").await.unwrap();
        assert_eq!(val, Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_set_with_options_nx() {
        let storage = StorageEngine::new(StorageConfig::default());

        let result = storage
            .set_with_options(
                b"opt_key".to_vec(),
                b"v1".to_vec(),
                None,
                true,
                false,
                false,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result, SetResult::Ok);

        // NX should fail on existing key
        let result = storage
            .set_with_options(
                b"opt_key".to_vec(),
                b"v2".to_vec(),
                None,
                true,
                false,
                false,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result, SetResult::NotSet);
    }

    #[tokio::test]
    async fn test_set_with_options_get() {
        let storage = StorageEngine::new(StorageConfig::default());

        // GET on non-existent key
        let result = storage
            .set_with_options(
                b"get_key".to_vec(),
                b"v1".to_vec(),
                None,
                false,
                false,
                true,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result, SetResult::OldValue(None));

        // GET on existing key
        let result = storage
            .set_with_options(
                b"get_key".to_vec(),
                b"v2".to_vec(),
                None,
                false,
                false,
                true,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result, SetResult::OldValue(Some(b"v1".to_vec())));
    }

    #[tokio::test]
    async fn test_atomic_incr() {
        let storage = StorageEngine::new(StorageConfig::default());

        // INCR on non-existent key (should create with value 1)
        let val = storage.atomic_incr(b"counter", 1).unwrap();
        assert_eq!(val, 1);

        // INCR again
        let val = storage.atomic_incr(b"counter", 1).unwrap();
        assert_eq!(val, 2);

        // INCRBY 10
        let val = storage.atomic_incr(b"counter", 10).unwrap();
        assert_eq!(val, 12);

        // DECRBY (negative delta)
        let val = storage.atomic_incr(b"counter", -5).unwrap();
        assert_eq!(val, 7);
    }

    #[tokio::test]
    async fn test_atomic_incr_not_a_number() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Set a non-numeric value
        storage
            .set(b"not_num".to_vec(), b"hello".to_vec(), None)
            .await
            .unwrap();

        let result = storage.atomic_incr(b"not_num", 1);
        assert!(matches!(result, Err(StorageError::NotANumber)));
    }

    #[tokio::test]
    async fn test_atomic_incr_wrong_type() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Set a list type value
        storage
            .set_with_type(
                b"list_key".to_vec(),
                b"data".to_vec(),
                RedisDataType::List,
                None,
            )
            .await
            .unwrap();

        let result = storage.atomic_incr(b"list_key", 1);
        assert!(matches!(result, Err(StorageError::WrongType)));
    }

    #[tokio::test]
    async fn test_atomic_incr_float() {
        let storage = StorageEngine::new(StorageConfig::default());

        let val = storage.atomic_incr_float(b"float_counter", 1.5).unwrap();
        assert!((val - 1.5).abs() < f64::EPSILON);

        let val = storage.atomic_incr_float(b"float_counter", 2.5).unwrap();
        assert!((val - 4.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_atomic_append() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Append to non-existent key
        let len = storage.atomic_append(b"append_key", b"hello").unwrap();
        assert_eq!(len, 5);

        // Append more
        let len = storage.atomic_append(b"append_key", b" world").unwrap();
        assert_eq!(len, 11);

        let val = storage.get(b"append_key").await.unwrap();
        assert_eq!(val, Some(b"hello world".to_vec()));
    }

    #[tokio::test]
    async fn test_atomic_modify_collection() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Use atomic_modify to create a list-like structure
        let result: usize = storage
            .atomic_modify(b"my_list", RedisDataType::List, |existing| {
                match existing {
                    None => {
                        // Create new list with one element
                        let data = bincode::serialize(&vec![b"item1".to_vec()]).unwrap();
                        Ok((Some(data), 1))
                    }
                    Some(_) => unreachable!(),
                }
            })
            .unwrap();
        assert_eq!(result, 1);

        // Modify the list to add another element
        let result: usize = storage
            .atomic_modify(b"my_list", RedisDataType::List, |existing| match existing {
                Some(data) => {
                    let mut list: Vec<Vec<u8>> = bincode::deserialize(data).unwrap();
                    list.push(b"item2".to_vec());
                    let new_len = list.len();
                    let new_data = bincode::serialize(&list).unwrap();
                    Ok((Some(new_data), new_len))
                }
                None => unreachable!(),
            })
            .unwrap();
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn test_atomic_modify_wrong_type() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Set a string key
        storage
            .set(b"str_key".to_vec(), b"value".to_vec(), None)
            .await
            .unwrap();

        // Try to modify as a list - should fail with WrongType
        let result = storage.atomic_modify(b"str_key", RedisDataType::List, |_| {
            Ok((Some(vec![1, 2, 3]), ()))
        });
        assert!(matches!(result, Err(StorageError::WrongType)));
    }

    #[tokio::test]
    async fn test_atomic_getdel() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Set a key
        storage
            .set(b"gd_key".to_vec(), b"value".to_vec(), None)
            .await
            .unwrap();

        // GETDEL should return the value and delete
        let val = storage.atomic_getdel(b"gd_key").unwrap();
        assert_eq!(val, Some(b"value".to_vec()));

        // Key should be gone
        let val = storage.get(b"gd_key").await.unwrap();
        assert_eq!(val, None);

        // GETDEL on non-existent key
        let val = storage.atomic_getdel(b"gd_key").unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_atomic_getset() {
        let storage = StorageEngine::new(StorageConfig::default());

        // GETSET on non-existent key
        let old = storage
            .atomic_getset(b"gs_key".to_vec(), b"v1".to_vec())
            .await
            .unwrap();
        assert_eq!(old, None);

        // GETSET on existing key
        let old = storage
            .atomic_getset(b"gs_key".to_vec(), b"v2".to_vec())
            .await
            .unwrap();
        assert_eq!(old, Some(b"v1".to_vec()));

        // Verify new value
        let val = storage.get(b"gs_key").await.unwrap();
        assert_eq!(val, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_concurrent_set_nx() {
        let storage = StorageEngine::new(StorageConfig::default());

        let mut handles = vec![];
        let success_count = Arc::new(AtomicU64::new(0));

        for i in 0..10 {
            let s = Arc::clone(&storage);
            let sc = Arc::clone(&success_count);
            handles.push(tokio::spawn(async move {
                let val = format!("value_{}", i).into_bytes();
                if s.set_nx(b"race_key".to_vec(), val, None).await.unwrap().0 {
                    sc.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Exactly one task should have succeeded
        assert_eq!(success_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_concurrent_incr() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Set initial value
        storage
            .set(b"race_counter".to_vec(), b"0".to_vec(), None)
            .await
            .unwrap();

        let mut handles = vec![];
        for _ in 0..100 {
            let s = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                s.atomic_incr(b"race_counter", 1).unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // All increments should be accounted for
        let val = storage.get(b"race_counter").await.unwrap().unwrap();
        let count: i64 = std::str::from_utf8(&val).unwrap().parse().unwrap();
        assert_eq!(count, 100);
    }

    #[tokio::test]
    async fn test_memory_ordering_consistency() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Set and delete many keys concurrently to stress memory accounting
        let mut handles = vec![];
        for i in 0..50 {
            let s = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                let key = format!("mem_key_{}", i).into_bytes();
                s.set(key.clone(), b"value".to_vec(), None).await.unwrap();
                s.del(&key).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Memory should be back to 0 (or very close due to racing)
        let mem = storage.get_used_memory();
        assert_eq!(
            mem, 0,
            "Memory should be 0 after setting and deleting all keys"
        );
    }

    #[tokio::test]
    async fn test_set_with_type_preserve_ttl_atomic() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set a key with a TTL
        let ttl = Duration::from_secs(300);
        storage
            .set_with_type(
                b"ttl_key".to_vec(),
                b"original_value".to_vec(),
                RedisDataType::ZSet,
                Some(ttl),
            )
            .await
            .unwrap();

        // Verify TTL exists
        let remaining = storage.ttl(b"ttl_key").await.unwrap();
        assert!(remaining.is_some(), "Key should have a TTL");
        assert!(
            remaining.unwrap().as_secs() > 200,
            "TTL should be close to 300s"
        );

        // Now overwrite value with set_with_type_preserve_ttl
        storage
            .set_with_type_preserve_ttl(
                b"ttl_key".to_vec(),
                b"new_value".to_vec(),
                RedisDataType::ZSet,
            )
            .await
            .unwrap();

        // Value should be updated
        let val = storage.get(b"ttl_key").await.unwrap();
        assert_eq!(val, Some(b"new_value".to_vec()));

        // TTL should be preserved
        let remaining = storage.ttl(b"ttl_key").await.unwrap();
        assert!(remaining.is_some(), "TTL should be preserved after update");
        assert!(
            remaining.unwrap().as_secs() > 200,
            "TTL should still be close to 300s"
        );
    }

    #[tokio::test]
    async fn test_set_with_type_preserve_ttl_no_existing_ttl() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set a key without TTL
        storage
            .set_with_type(
                b"no_ttl_key".to_vec(),
                b"original".to_vec(),
                RedisDataType::ZSet,
                None,
            )
            .await
            .unwrap();

        // Overwrite with preserve_ttl - should have no TTL since original had none
        storage
            .set_with_type_preserve_ttl(
                b"no_ttl_key".to_vec(),
                b"updated".to_vec(),
                RedisDataType::ZSet,
            )
            .await
            .unwrap();

        let val = storage.get(b"no_ttl_key").await.unwrap();
        assert_eq!(val, Some(b"updated".to_vec()));

        let remaining = storage.ttl(b"no_ttl_key").await.unwrap();
        assert!(remaining.is_none(), "Key should not have TTL");
    }

    #[tokio::test]
    async fn test_set_with_type_preserve_ttl_new_key() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Call preserve_ttl on a key that doesn't exist yet
        storage
            .set_with_type_preserve_ttl(
                b"brand_new_key".to_vec(),
                b"value".to_vec(),
                RedisDataType::ZSet,
            )
            .await
            .unwrap();

        let val = storage.get(b"brand_new_key").await.unwrap();
        assert_eq!(val, Some(b"value".to_vec()));

        let remaining = storage.ttl(b"brand_new_key").await.unwrap();
        assert!(remaining.is_none(), "New key should not have TTL");
    }

    #[tokio::test]
    async fn test_set_with_type_preserve_ttl_concurrent() {
        let config = StorageConfig::default();
        let storage = Arc::new(StorageEngine::new(config));

        // Set a key with TTL
        let ttl = Duration::from_secs(300);
        storage
            .set_with_type(
                b"concurrent_ttl".to_vec(),
                b"initial".to_vec(),
                RedisDataType::ZSet,
                Some(ttl),
            )
            .await
            .unwrap();

        // Concurrently update the value multiple times with preserve_ttl
        let mut handles = Vec::new();
        for i in 0..20 {
            let s = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                let value = format!("value_{}", i).into_bytes();
                s.set_with_type_preserve_ttl(
                    b"concurrent_ttl".to_vec(),
                    value,
                    RedisDataType::ZSet,
                )
                .await
                .unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Key should still exist with some value
        let val = storage.get(b"concurrent_ttl").await.unwrap();
        assert!(val.is_some(), "Key should exist after concurrent updates");

        // TTL should still be preserved
        let remaining = storage.ttl(b"concurrent_ttl").await.unwrap();
        assert!(
            remaining.is_some(),
            "TTL should be preserved after concurrent updates"
        );
        assert!(
            remaining.unwrap().as_secs() > 200,
            "TTL should still be close to 300s"
        );
    }

    #[tokio::test]
    async fn test_cleanup_stale_metadata() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Create many keys and WATCH them (bump their versions)
        let num_keys = 10_000;
        for i in 0..num_keys {
            let key = format!("watched_key_{}", i).into_bytes();
            storage
                .set(key.clone(), b"value".to_vec(), None)
                .await
                .unwrap();
            // Bump key version to simulate WATCH
            storage.bump_key_version(&key);
        }

        // Also create some key_locks entries by using lock_keys
        for i in 0..100 {
            let key = format!("locked_key_{}", i).into_bytes();
            storage
                .set(key.clone(), b"value".to_vec(), None)
                .await
                .unwrap();
            // Lock and immediately release to populate key_locks map
            let _guard = storage.lock_keys(&[key]).await;
            // guard drops here, releasing the lock
        }

        // Verify maps have entries
        assert!(
            storage.key_versions_len() >= num_keys,
            "key_versions should have at least {} entries, got {}",
            num_keys,
            storage.key_versions_len()
        );
        assert!(
            storage.key_locks_len() >= 100,
            "key_locks should have at least 100 entries, got {}",
            storage.key_locks_len()
        );

        // Delete all keys from data store
        for i in 0..num_keys {
            let key = format!("watched_key_{}", i).into_bytes();
            storage.del(&key).await.unwrap();
        }
        for i in 0..100 {
            let key = format!("locked_key_{}", i).into_bytes();
            storage.del(&key).await.unwrap();
        }

        // Before cleanup, maps should still have entries
        assert!(
            storage.key_versions_len() >= num_keys,
            "key_versions should still have entries before cleanup"
        );
        assert!(
            storage.key_locks_len() >= 100,
            "key_locks should still have entries before cleanup"
        );

        // Run cleanup
        storage.trigger_metadata_cleanup();

        // After cleanup: key_versions are intentionally preserved (WATCH correctness
        // requires versions to persist for deleted keys so that EXEC detects modifications).
        assert!(
            storage.key_versions_len() >= num_keys,
            "key_versions should be preserved after cleanup for WATCH correctness, got {}",
            storage.key_versions_len()
        );
        // key_locks should have shrunk (locks with no holders are cleaned up)
        assert!(
            storage.key_locks_len() < 50,
            "key_locks should have shrunk significantly, got {}",
            storage.key_locks_len()
        );
    }

    #[tokio::test]
    async fn test_cleanup_preserves_active_keys() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Create keys that will remain
        for i in 0..100 {
            let key = format!("active_key_{}", i).into_bytes();
            storage
                .set(key.clone(), b"value".to_vec(), None)
                .await
                .unwrap();
            storage.bump_key_version(&key);
        }

        // Create keys that will be deleted
        for i in 0..100 {
            let key = format!("stale_key_{}", i).into_bytes();
            storage
                .set(key.clone(), b"value".to_vec(), None)
                .await
                .unwrap();
            storage.bump_key_version(&key);
        }

        // Delete only the stale keys
        for i in 0..100 {
            let key = format!("stale_key_{}", i).into_bytes();
            storage.del(&key).await.unwrap();
        }

        let versions_before = storage.key_versions_len();
        assert!(
            versions_before >= 200,
            "Should have at least 200 version entries"
        );

        // Run cleanup
        storage.trigger_metadata_cleanup();

        // All key versions should be preserved (cleanup no longer removes them for
        // WATCH correctness — deleted keys must retain their versions so EXEC detects
        // modifications even after the key is gone).
        let versions_after = storage.key_versions_len();
        assert_eq!(
            versions_after, versions_before,
            "key_versions should be unchanged after cleanup ({} should equal {})",
            versions_after, versions_before
        );

        // Verify active keys still have valid versions (non-zero since we bumped them)
        for i in 0..100 {
            let key = format!("active_key_{}", i).into_bytes();
            let version = storage.get_key_version(&key);
            assert!(version > 0, "Active key {} should have non-zero version", i);
        }
    }

    #[tokio::test]
    async fn test_cleanup_skips_held_locks() {
        let config = StorageConfig::default();
        let storage = Arc::new(StorageEngine::new(config));

        // Create a key and acquire its lock
        let key = b"held_key".to_vec();
        storage
            .set(key.clone(), b"value".to_vec(), None)
            .await
            .unwrap();

        // Acquire lock (don't drop the guard)
        let guard = storage.lock_keys(&[key.clone()]).await;

        // Delete the key from data store while lock is held
        storage.del(&key).await.unwrap();

        let locks_before = storage.key_locks_len();
        assert!(locks_before >= 1, "Should have at least 1 lock entry");

        // Run cleanup - should NOT remove the held lock
        storage.trigger_metadata_cleanup();

        let locks_after = storage.key_locks_len();
        assert!(
            locks_after >= 1,
            "Held lock should not be removed by cleanup, got {} locks",
            locks_after
        );

        // Now drop the guard
        drop(guard);

        // Run cleanup again - now it should be removable
        storage.trigger_metadata_cleanup();

        // The lock for the deleted key should now be cleaned up
        // (it might still be there if there are other entries, but at least it's eligible)
    }
}
