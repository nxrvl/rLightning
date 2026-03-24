use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use crate::storage::clock::{cached_now, start_clock_updater};
use crate::storage::sharded::{ShardedStore, ShardEntry};
use tokio::sync::RwLock;
use tokio::time;

use crate::networking::resp::RespCommand;
use crate::storage::error::StorageError;
use crate::storage::item::{Entry, RedisDataType, StorageItem};
use crate::storage::value::{ModifyResult, NativeHashMap, NativeHashSet, SortedSetData, StoreValue};

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

// ExpirationEntry is now per-shard in ShardedStore (ShardExpirationEntry)

/// Eviction policy for the storage engine
#[derive(Debug, Clone, Copy, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
#[allow(clippy::upper_case_acronyms)]
pub enum EvictionPolicy {
    /// Remove the least recently used item (all keys)
    #[default]
    LRU,
    /// Remove the least frequently used item (all keys)
    LFU,
    /// Remove a random item (all keys)
    Random,
    /// Remove the least recently used item (only keys with TTL)
    VolatileLRU,
    /// Remove the least frequently used item (only keys with TTL)
    VolatileLFU,
    /// Remove a random item (only keys with TTL)
    VolatileRandom,
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
            Self::VolatileLRU => 4,
            Self::VolatileLFU => 5,
            Self::VolatileRandom => 6,
            Self::NoEviction => 3,
        }
    }

    /// Convert from u8 (atomic load). Falls back to NoEviction for unknown values.
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::LRU,
            1 => Self::LFU,
            2 => Self::Random,
            4 => Self::VolatileLRU,
            5 => Self::VolatileLFU,
            6 => Self::VolatileRandom,
            _ => Self::NoEviction,
        }
    }

    /// Whether this policy only targets keys with an expiry (volatile-* policies).
    pub const fn is_volatile(self) -> bool {
        matches!(
            self,
            Self::VolatileLRU | Self::VolatileLFU | Self::VolatileRandom
        )
    }
}

// Implement clap::ValueEnum for command-line parsing
impl clap::ValueEnum for EvictionPolicy {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            Self::LRU,
            Self::LFU,
            Self::Random,
            Self::VolatileLRU,
            Self::VolatileLFU,
            Self::VolatileRandom,
            Self::NoEviction,
        ]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(match self {
            Self::LRU => clap::builder::PossibleValue::new("lru"),
            Self::LFU => clap::builder::PossibleValue::new("lfu"),
            Self::Random => clap::builder::PossibleValue::new("random"),
            Self::VolatileLRU => clap::builder::PossibleValue::new("volatile-lru"),
            Self::VolatileLFU => clap::builder::PossibleValue::new("volatile-lfu"),
            Self::VolatileRandom => clap::builder::PossibleValue::new("volatile-random"),
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
    data: ShardedStore,
    config: StorageConfig,
    /// Atomic memory counter for better performance (eliminates lock contention)
    current_memory: AtomicU64,
    write_counters: RwLock<Vec<Arc<AtomicU64>>>,
    /// Atomic counter for the total number of keys (for O(1) DBSIZE)
    key_count: AtomicU64,
    /// Pattern index for common prefixes (optimizes KEYS command), keyed by (db_index, prefix)
    #[allow(clippy::type_complexity)]
    prefix_index: RwLock<HashMap<(usize, String), Vec<Vec<u8>>>>,
    /// Additional databases (1-15) for multi-database support (SELECT/MOVE)
    extra_dbs: Vec<ShardedStore>,
    /// Per-key version counter for WATCH (optimistic locking) support
    key_versions: DashMap<Vec<u8>, u64>,
    /// Global version counter for generating unique version numbers
    global_version: AtomicU64,
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
    /// Count of connections that currently have active WATCH keys.
    /// Used to skip bump_key_version when no connections are watching (common case).
    active_watch_count: AtomicU32,
}

impl StorageEngine {
    /// Create a new storage engine with the given configuration
    pub fn new(config: StorageConfig) -> Arc<Self> {
        let mut extra_dbs = Vec::with_capacity(NUM_DATABASES - 1);
        for _ in 0..(NUM_DATABASES - 1) {
            extra_dbs.push(ShardedStore::with_capacity(64));
        }

        let initial_policy = config.eviction_policy.to_u8();
        // In Redis, maxmemory 0 means "no memory limit". Normalize to usize::MAX
        // to match the CONFIG SET path behavior.
        let initial_max_memory = if config.max_memory == 0 {
            usize::MAX
        } else {
            config.max_memory
        };
        // Start the cached clock updater (idempotent if already started)
        start_clock_updater();

        let engine = Arc::new(Self {
            data: ShardedStore::with_capacity(1024), // Starting with a reasonable capacity
            config,
            current_memory: AtomicU64::new(0),
            write_counters: RwLock::new(Vec::new()),
            key_count: AtomicU64::new(0),
            prefix_index: RwLock::new(HashMap::new()),
            extra_dbs,
            key_versions: DashMap::with_capacity(256),
            global_version: AtomicU64::new(1),
            flush_epochs: (0..NUM_DATABASES).map(|_| AtomicU64::new(0)).collect(),
            cross_db_lock: RwLock::new(()),
            db_mapping: AtomicU64::new(Self::identity_db_mapping()),
            runtime_config: DashMap::new(),
            active_eviction_policy: AtomicU8::new(initial_policy),
            active_max_memory: AtomicUsize::new(initial_max_memory),
            active_watch_count: AtomicU32::new(0),
        });

        // Seed runtime config from startup config
        engine.seed_runtime_config();

        // Start the expiration task
        Self::start_expiration_task(Arc::clone(&engine));

        engine
    }

    /// Start the background task for per-shard round-robin TTL expiration.
    /// Processes a few shards per tick (100ms interval) to spread work evenly.
    fn start_expiration_task(engine: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(100));
            let mut metadata_cleanup_counter: u64 = 0;
            // Round-robin index across all databases and shards
            let mut rr_db: usize = 0;
            let mut rr_shard: usize = 0;

            loop {
                interval.tick().await;
                let now = cached_now();

                // Process a batch of shards per tick (round-robin across all DBs)
                // We process multiple shards per tick for throughput
                const SHARDS_PER_TICK: usize = 4;
                const MAX_EXPIRES_PER_SHARD: usize = 25;

                // Hold cross_db_lock read guard to prevent SWAPDB from
                // changing db_mapping between get_db_by_index and
                // bump_key_version_for_db, which would cause the version
                // bump to target the wrong logical database.
                {
                    let _db_guard = engine.cross_db_lock.read().await;
                    for _ in 0..SHARDS_PER_TICK {
                        let db = engine.get_db_by_index(rr_db);
                        let num_shards = db.num_shards();

                        if rr_shard < num_shards {
                            let removed = db.expire_shard(rr_shard, now, MAX_EXPIRES_PER_SHARD);
                            for (k, entry) in &removed {
                                let size = if entry.cached_mem_size > 0 {
                                    k.len() as u64 + entry.cached_mem_size
                                } else {
                                    Self::calculate_entry_size(k, &entry.value) as u64
                                };
                                engine.current_memory.fetch_sub(size, Ordering::Relaxed);
                                engine.key_count.fetch_sub(1, Ordering::Relaxed);
                                // Bump key version so WATCH detects expiry
                                engine.bump_key_version_for_db(k, rr_db);
                            }
                        }

                        // Advance round-robin
                        rr_shard += 1;
                        let db_shards = engine.get_db_by_index(rr_db).num_shards();
                        if rr_shard >= db_shards {
                            rr_shard = 0;
                            rr_db += 1;
                            if rr_db >= NUM_DATABASES {
                                rr_db = 0;
                            }
                        }
                    }
                }

                // Also do probabilistic sampling every 10 ticks (1 second)
                metadata_cleanup_counter += 1;
                if metadata_cleanup_counter.is_multiple_of(10) {
                    // Hold cross_db_lock read guard to prevent SWAPDB from
                    // changing db_mapping between get_db_by_index and
                    // bump_key_version_for_db inside probabilistic cleanup.
                    let _db_guard = engine.cross_db_lock.read().await;
                    engine.probabilistic_cleanup_sharded(now);
                }
                if metadata_cleanup_counter >= 600 {
                    metadata_cleanup_counter = 0;
                    // Clean up stale key_versions entries for keys that no longer exist.
                    // Runs every 60 seconds to prevent unbounded growth of the WATCH version map.
                    engine.cleanup_stale_key_versions();
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
            EvictionPolicy::VolatileLRU => "volatile-lru",
            EvictionPolicy::VolatileLFU => "volatile-lfu",
            EvictionPolicy::VolatileRandom => "volatile-random",
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
        self.runtime_config
            .insert("cluster-enabled".to_string(), "no".to_string());
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

    /// Set the cluster-enabled runtime config flag.
    pub fn set_cluster_enabled(&self, enabled: bool) {
        self.runtime_config.insert(
            "cluster-enabled".to_string(),
            if enabled { "yes" } else { "no" }.to_string(),
        );
    }

    /// Get all runtime config entries matching a glob pattern.
    /// Returns pairs of (param_name, value) matching the pattern.
    pub fn config_get(&self, pattern: &str) -> Vec<(String, String)> {
        use crate::utils::glob::glob_match;
        let mut results = Vec::new();
        for entry in self.runtime_config.iter() {
            if glob_match(pattern, entry.key()) {
                results.push((entry.key().clone(), (*entry).clone()));
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
                    "allkeys-lru" => EvictionPolicy::LRU,
                    "allkeys-lfu" => EvictionPolicy::LFU,
                    "allkeys-random" => EvictionPolicy::Random,
                    "volatile-lru" => EvictionPolicy::VolatileLRU,
                    "volatile-lfu" => EvictionPolicy::VolatileLFU,
                    "volatile-random" => EvictionPolicy::VolatileRandom,
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
            "bind" | "port" | "databases" | "tls-port" | "tls-cert-file" | "tls-key-file"
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
            "hz" | "timeout" | "tcp-keepalive" | "tcp-backlog"
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
        for (suffix, multiplier) in &[
            ("gb", 1024 * 1024 * 1024),
            ("mb", 1024 * 1024),
            ("kb", 1024),
        ] {
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
    fn get_physical_db(&self, physical_idx: usize) -> &ShardedStore {
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
    pub fn active_db(&self) -> &ShardedStore {
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
    pub fn get_db_by_index(&self, db_index: usize) -> &ShardedStore {
        if db_index < NUM_DATABASES {
            let mapping = self.db_mapping.load(Ordering::Acquire);
            let physical_idx = Self::physical_index_from_mapping(mapping, db_index);
            self.get_physical_db(physical_idx)
        } else {
            self.get_physical_db(0)
        }
    }

    /// Probabilistic cleanup using per-shard sampling (all databases).
    /// Called periodically to catch keys that expired but weren't in the heap
    /// (e.g., keys whose TTL was refreshed but stale heap entries weren't cleaned).
    fn probabilistic_cleanup_sharded(&self, now: Instant) {
        const SAMPLE_SIZE: usize = 20;

        for db_idx in 0..NUM_DATABASES {
            let db = self.get_db_by_index(db_idx);
            if db.is_empty() {
                continue;
            }
            let keys_to_remove: Vec<Vec<u8>> = db.sample(SAMPLE_SIZE, |key, entry| {
                if let Some(exp) = entry.expires_at
                    && exp <= now {
                        return Some(key.clone());
                    }
                None
            });

            for key in keys_to_remove {
                let mut entry_size = 0u64;
                let removed = db.remove_if(&key, |k, item| {
                    if let Some(exp) = item.expires_at
                        && exp <= now {
                            entry_size = Self::calculate_entry_size(k, &item.value) as u64;
                            return true;
                        }
                    false
                });
                if removed {
                    self.current_memory.fetch_sub(entry_size, Ordering::Relaxed);
                    self.key_count.fetch_sub(1, Ordering::Relaxed);
                    // Bump key version so WATCH detects expiry
                    self.bump_key_version_for_db(&key, db_idx);
                }
            }
        }
    }

    /// Refresh cached_mem_size for all entries across all databases.
    /// Called after RDB/AOF restore to ensure accurate O(1) memory tracking.
    pub fn refresh_all_cached_mem_sizes(&self) {
        for db_idx in 0..NUM_DATABASES {
            let db = self.get_db_by_index(db_idx);
            db.for_each_mut(|_key, entry| {
                entry.refresh_cached_mem_size();
            });
        }
    }

    /// Get the size of a key-value pair in bytes
    fn calculate_entry_size(key: &[u8], value: &StoreValue) -> usize {
        key.len() + value.mem_size()
    }

    /// Helper method to remove expired key and update memory/counters.
    /// Uses remove_if to atomically check expiration and remove under the same lock,
    /// avoiding TOCTOU races where a concurrent writer could refresh the key between
    /// remove and re-insert.
    async fn remove_expired_key(&self, key: &[u8]) {
        let now = cached_now();
        let db = self.active_db();
        let mut entry_size = 0u64;
        let removed = db.remove_if(key, |k, item| {
            if let Some(exp) = item.expires_at
                && exp <= now {
                    entry_size = Self::calculate_entry_size(k, &item.value) as u64;
                    return true;
                }
            false
        });
        if removed {
            self.current_memory.fetch_sub(entry_size, Ordering::Relaxed);
            self.key_count.fetch_sub(1, Ordering::Relaxed);
            // Bump key version so WATCH detects expiry
            self.bump_key_version(key);
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

    /// Add a key to the per-shard expiration heap (current database).
    async fn add_to_expiration_queue(&self, key: Vec<u8>, expires_at: Instant) {
        let db = self.active_db();
        db.add_expiration(&key, expires_at);
    }

    /// Add a key to the expiration heap for a specific database.
    /// Used by cross-DB operations (e.g. MOVE) that need to enqueue TTL
    /// in a database other than the current task-local one.
    async fn add_to_expiration_queue_for_db(
        &self,
        key: Vec<u8>,
        expires_at: Instant,
        db_index: usize,
    ) {
        let db = self.get_db_by_index(db_index);
        db.add_expiration(&key, expires_at);
    }

    /// Remove a key from its shard's expiration heap (current database).
    async fn remove_from_expiration_queue(&self, key: &[u8]) {
        let db = self.active_db();
        db.remove_expiration(key);
    }

    /// Common prefixes to index for faster KEYS operations
    const INDEXED_PREFIXES: &'static [&'static str] = &[
        "user:", "session:", "cache:", "temp:", "auth:", "token:", "data:", "config:", "stats:",
        "log:", "queue:", "job:",
    ];

    /// Update prefix indices when a key is added or removed (DB-scoped)
    pub async fn update_prefix_indices(&self, key: &[u8], is_insert: bool) {
        let db_idx = Self::current_db_idx();
        self.update_prefix_indices_for_db(key, is_insert, db_idx)
            .await;
    }

    /// Update prefix indices for a specific database index.
    /// Used by cross-DB operations (MOVE) and the batch pipeline path (which lacks
    /// a CURRENT_DB_INDEX scope) to update the correct DB's prefix index.
    pub async fn update_prefix_indices_for_db(&self, key: &[u8], is_insert: bool, db_idx: usize) {
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

    /// Batch update prefix indices for a specific database index.
    /// Accepts a slice of (key, is_insert) pairs and acquires the write lock once,
    /// reducing lock contention in the pipeline batch path.
    pub async fn update_prefix_indices_batch_for_db(
        &self,
        updates: &[(&[u8], bool)],
        db_idx: usize,
    ) {
        if updates.is_empty() {
            return;
        }
        let mut index = self.prefix_index.write().await;

        for &(key, is_insert) in updates {
            if let Ok(key_str) = std::str::from_utf8(key) {
                for &prefix in Self::INDEXED_PREFIXES {
                    if key_str.starts_with(prefix) {
                        let idx_key = (db_idx, prefix.to_string());
                        let entry = index.entry(idx_key.clone()).or_insert_with(Vec::new);
                        if is_insert {
                            entry.push(key.to_vec());
                        } else {
                            entry.retain(|k| k != key);
                            if entry.is_empty() {
                                index.remove(&idx_key);
                            }
                        }
                        break;
                    }
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
        let current_memory = self.current_memory.load(Ordering::Relaxed) as usize;
        if current_memory + required_size > max_memory {
            // Read the active eviction policy (may have been changed at runtime via CONFIG SET)
            let eviction_policy =
                EvictionPolicy::from_u8(self.active_eviction_policy.load(Ordering::Acquire));

            // If we're not allowed to evict, fail
            if eviction_policy == EvictionPolicy::NoEviction {
                return Err(StorageError::MemoryLimitExceeded);
            }

            // Evict items until we have enough space (search across all databases)
            while self.current_memory.load(Ordering::Relaxed) as usize + required_size > max_memory
            {
                // Check if there are any items to evict across all DBs
                let total_keys: usize =
                    self.data.len() + self.extra_dbs.iter().map(|db| db.len()).sum::<usize>();
                if total_keys == 0 {
                    return Err(StorageError::MemoryLimitExceeded);
                }

                // Per-shard probabilistic eviction: sample candidates from random shards
                // across all databases, then pick the best victim from the sample.
                let volatile_only = eviction_policy.is_volatile();
                let is_lfu = matches!(
                    eviction_policy,
                    EvictionPolicy::LFU | EvictionPolicy::VolatileLFU
                );
                let is_random = matches!(
                    eviction_policy,
                    EvictionPolicy::Random | EvictionPolicy::VolatileRandom
                );

                let mut best_victim: Option<(Vec<u8>, usize)> = None; // (key, db_idx)
                let mut best_score: u64 = u64::MAX;

                for db_idx in 0..NUM_DATABASES {
                    let db = self.get_db_by_index(db_idx);
                    let num_shards = db.num_shards();
                    if num_shards == 0 {
                        continue;
                    }

                    // Sample from a random shard in this DB
                    let shard_idx = fastrand::usize(0..num_shards);
                    let candidates = db.sample_eviction_candidates(shard_idx, volatile_only);

                    for candidate in &candidates {
                        if is_random {
                            // For random eviction, just pick the first candidate found
                            if best_victim.is_none() {
                                best_victim = Some((candidate.key.clone(), db_idx));
                            }
                        } else {
                            let score = if is_lfu {
                                candidate.access_count as u64
                            } else {
                                candidate.lru_clock as u64
                            };
                            if score < best_score {
                                best_score = score;
                                best_victim = Some((candidate.key.clone(), db_idx));
                            }
                        }
                    }
                }

                // Remove the victim from the correct database
                if let Some((key, db_idx)) = best_victim {
                    let db = self.get_db_by_index(db_idx);
                    if let Some((k, item)) = db.remove(&key) {
                        let size = Self::calculate_entry_size(&k, &item.value) as u64;
                        self.current_memory.fetch_sub(size, Ordering::Relaxed);
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
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
        self.set_value(key, StoreValue::Str(value.into()), ttl, true).await
    }

    /// Set a key-value pair with native StoreValue type
    pub async fn set_with_type(
        &self,
        key: Vec<u8>,
        value: StoreValue,
        ttl: Option<Duration>,
    ) -> StorageResult<()> {
        self.set_value(key, value, ttl, true).await
    }

    /// Set a key-value pair, skipping per-key eviction checks.
    pub async fn set_preevicted(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> StorageResult<()> {
        self.set_value(key, StoreValue::Str(value.into()), ttl, false)
            .await
    }

    /// Inner implementation for set operations.
    async fn set_value(
        &self,
        key: Vec<u8>,
        value: StoreValue,
        ttl: Option<Duration>,
        do_evict: bool,
    ) -> StorageResult<()> {
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }

        let val_size = value.mem_size();
        if val_size > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }

        if val_size > 10240 {
            tracing::debug!(
                "Large value SET operation: key={:?}, value_len={}, type={}",
                String::from_utf8_lossy(&key),
                val_size,
                value.data_type().as_str()
            );
        }

        let required_size = key.len() + val_size;

        if do_evict {
            self.maybe_evict(required_size).await?;
        }

        let mut item = Entry::new(value);
        let expires_at = if let Some(ttl) = ttl {
            item.expire(ttl);
            Some(cached_now() + ttl)
        } else {
            None
        };

        let is_new_key = match self.active_db().entry(key.clone()) {
            ShardEntry::Occupied(mut entry) => {
                let old_size = Self::calculate_entry_size(entry.key(), &entry.get().value);
                if old_size > 0 {
                    self.current_memory
                        .fetch_sub(old_size as u64, Ordering::Relaxed);
                }
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::Relaxed);
                entry.insert(item);
                self.bump_key_version(&key);
                false
            }
            ShardEntry::Vacant(entry) => {
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::Relaxed);
                entry.insert(item);
                self.bump_key_version(&key);
                true
            }
        };

        if let Some(expires_at) = expires_at {
            self.add_to_expiration_queue(key.clone(), expires_at).await;
        }

        if is_new_key {
            self.update_prefix_indices(&key, true).await;
            self.key_count.fetch_add(1, Ordering::Relaxed);
        }
        self.increment_write_counters().await;

        Ok(())
    }

    /// Set a key-value pair preserving the existing TTL if present.
    pub async fn set_with_type_preserve_ttl(
        &self,
        key: Vec<u8>,
        value: StoreValue,
    ) -> StorageResult<()> {
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }

        let val_size = value.mem_size();
        if val_size > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }

        let required_size = key.len() + val_size;
        self.maybe_evict(required_size).await?;

        let mut item = Entry::new(value);

        let (is_new_key, preserved_expires_at) = match self.active_db().entry(key.clone()) {
            ShardEntry::Occupied(mut entry) => {
                let existing_expires_at = if !entry.get().is_expired() {
                    entry.get().expires_at
                } else {
                    None
                };
                item.expires_at = existing_expires_at;

                let old_size = Self::calculate_entry_size(entry.key(), &entry.get().value);
                if old_size > 0 {
                    self.current_memory
                        .fetch_sub(old_size as u64, Ordering::Relaxed);
                }
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::Relaxed);
                entry.insert(item);
                self.bump_key_version(&key);
                (false, existing_expires_at)
            }
            ShardEntry::Vacant(entry) => {
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::Relaxed);
                entry.insert(item);
                self.bump_key_version(&key);
                (true, None)
            }
        };

        if let Some(expires_at) = preserved_expires_at {
            self.add_to_expiration_queue(key.clone(), expires_at).await;
        }

        if is_new_key {
            self.update_prefix_indices(&key, true).await;
            self.key_count.fetch_add(1, Ordering::Relaxed);
        }
        self.increment_write_counters().await;

        Ok(())
    }

    /// Get a string value from the storage engine
    pub async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let db = self.active_db();
        // Use read lock for maximum concurrent reader throughput.
        // LRU/LFU touch is skipped on reads - eviction sampling handles approximation.
        let (result, needs_expire) = {
            if let Some(entry) = db.get(key) {
                if entry.is_expired() {
                    (None, true)
                } else {
                    let val = match &entry.value {
                        StoreValue::Str(v) => Some(v.to_vec()),
                        _ => None,
                    };
                    (val, false)
                }
            } else {
                (None, false)
            }
        };

        if needs_expire {
            self.remove_expired_key(key).await;
        }

        Ok(result)
    }

    /// Delete a key from the storage engine
    pub async fn del(&self, key: &[u8]) -> StorageResult<bool> {
        if let Some((k, item)) = self.active_db().remove(key) {
            // Use atomic operations for memory tracking - much faster than write lock
            let size = Self::calculate_entry_size(&k, &item.value) as u64;
            self.current_memory.fetch_sub(size, Ordering::Relaxed);
            // Update prefix index and decrement key count
            self.update_prefix_indices(&k, false).await;
            self.key_count.fetch_sub(1, Ordering::Relaxed);
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
        // Perform atomic check-and-delete in a sync block to avoid !Send guard across await.
        let removed = {
            match self.active_db().entry(key.to_vec()) {
                ShardEntry::Occupied(entry) => {
                    let db_idx = Self::current_db_idx();
                    let scoped = Self::db_scoped_key(db_idx, key);
                    let current_version = self.key_versions.get(&scoped).map(|v| *v).unwrap_or(0);
                    if current_version != expected_version {
                        None
                    } else {
                        let (k, item) = entry.remove_entry();
                        let size = Self::calculate_entry_size(&k, &item.value) as u64;
                        self.current_memory.fetch_sub(size, Ordering::Relaxed);
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
                        self.bump_key_version(&k);
                        Some(k)
                    }
                }
                ShardEntry::Vacant(_) => None,
            }
        };
        if let Some(k) = removed {
            self.update_prefix_indices(&k, false).await;
            true
        } else {
            false
        }
    }

    /// Check if a key exists in the storage engine
    pub async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        // Check if the key exists and is not expired.
        // Extract the result before any .await to avoid holding a !Send guard across await.
        let expired = {
            if let Some(entry) = self.active_db().get(key) {
                if entry.is_expired() {
                    Some(true)
                } else {
                    Some(false)
                }
            } else {
                None
            }
        };
        match expired {
            Some(true) => {
                self.remove_expired_key(key).await;
                Ok(false)
            }
            Some(false) => Ok(true),
            None => Ok(false),
        }
    }

    /// Set the TTL (time-to-live) for a key
    pub async fn expire(&self, key: &[u8], ttl: Option<Duration>) -> StorageResult<bool> {
        // Mutate the entry under the shard lock in sync block, then do async work.
        let queue_action = {
            if let Some(mut entry) = self.active_db().get_mut(key) {
                if entry.is_expired() {
                    None // expired = not found
                } else if let Some(ttl) = ttl {
                    entry.expire(ttl);
                    let expires_at = cached_now() + ttl;
                    Some(Some((true, expires_at)))
                } else {
                    entry.remove_expiry();
                    Some(Some((false, cached_now())))
                }
            } else {
                None
            }
        };

        if let Some(Some((is_set, expires_at))) = queue_action {
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
        // Extract TTL info before any .await to avoid holding a !Send guard across await.
        let result = {
            if let Some(entry) = self.active_db().get(key) {
                if entry.is_expired() {
                    Some(None) // expired, need lazy cleanup
                } else {
                    Some(Some(entry.ttl()))
                }
            } else {
                None // key doesn't exist
            }
        };
        match result {
            Some(None) => {
                self.remove_expired_key(key).await;
                Ok(None)
            }
            Some(Some(ttl)) => Ok(ttl),
            None => Ok(None),
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
                        && !entry.is_expired()
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
        let keys = db.filter_map(|key, entry| {
            if entry.is_expired() {
                return None;
            }
            if let Ok(key_str) = std::str::from_utf8(key)
                && crate::utils::glob::glob_match(pattern, key_str) {
                    return Some(key.clone());
                }
            None
        });

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
        self.current_memory.store(0, Ordering::Relaxed);
        self.key_count.store(0, Ordering::Relaxed);

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
        let mut size: u64 = 0;
        db.for_each(|key, entry| {
            size += Self::calculate_entry_size(key, &entry.value) as u64;
        });
        db.clear();
        self.current_memory.fetch_sub(
            size.min(self.current_memory.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
        // Recalculate total key count across all databases
        let total: u64 =
            self.data.len() as u64 + self.extra_dbs.iter().map(|db| db.len() as u64).sum::<u64>();
        self.key_count.store(total, Ordering::Relaxed);

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

        db.for_each(|key, entry| {
            if !entry.is_expired() {
                snapshot.insert(key.clone(), entry.clone());
            }
        });

        Ok(snapshot)
    }

    /// Get a snapshot of all databases (for multi-DB persistence).
    /// Returns a Vec indexed by database number, each containing the DB's data.
    ///
    /// Uses COW-based per-shard snapshotting: each shard's read lock is held only
    /// long enough to clone its HashMap, then released before moving to the next.
    /// This eliminates the global cross_db_lock write lock from the snapshot path,
    /// allowing concurrent reads and writes to proceed on other shards during save.
    ///
    /// Trade-off: the snapshot is not perfectly atomic across all databases (a MOVE
    /// or SWAPDB running concurrently could cause minor inconsistency), but this
    /// matches Redis's fork-based snapshot semantics and vastly reduces lock
    /// contention during persistence.
    pub async fn snapshot_all_dbs(&self) -> StorageResult<Vec<HashMap<Vec<u8>, StorageItem>>> {
        let mut snapshots = Vec::with_capacity(NUM_DATABASES);
        for db_idx in 0..NUM_DATABASES {
            let db = self.get_db_by_index(db_idx);
            let entries = db.snapshot_cow();
            let mut map = HashMap::with_capacity(entries.len());
            for (key, entry) in entries {
                map.insert(key, entry);
            }
            snapshots.push(map);
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

    /// Return the remaining memory budget before maxmemory is hit.
    /// Returns None if no memory limit is configured (unlimited).
    pub fn remaining_memory_budget(&self) -> Option<usize> {
        let max_memory = self.active_max_memory.load(Ordering::Acquire);
        if max_memory == usize::MAX {
            return None;
        }
        let current = self.current_memory.load(Ordering::Relaxed) as usize;
        Some(max_memory.saturating_sub(current))
    }

    /// Adjust the global memory counter by a signed delta (for batch path synchronization).
    pub fn adjust_global_memory(&self, delta: i64) {
        if delta > 0 {
            self.current_memory.fetch_add(delta as u64, Ordering::Relaxed);
        } else if delta < 0 {
            // Use saturating subtraction to avoid wrapping
            let sub = (-delta) as u64;
            self.current_memory.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(sub))
            }).ok();
        }
    }

    /// Adjust the global key count by a delta (positive = keys created, negative = keys deleted).
    /// Used by the batch pipeline path which tracks key creations/deletions separately.
    pub fn adjust_key_count(&self, delta: i64) {
        if delta > 0 {
            self.key_count.fetch_add(delta as u64, Ordering::Relaxed);
        } else if delta < 0 {
            let sub = (-delta) as u64;
            self.key_count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(sub))
            }).ok();
        }
    }

    /// Increment write counters by a batch count (for batch path RDB snapshot triggering).
    pub async fn increment_write_counters_batch(&self, count: u64) {
        let counters = self.write_counters.read().await;
        for counter in counters.iter() {
            counter.fetch_add(count, Ordering::SeqCst);
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

    /// Increment the active WATCH connection count.
    /// Called when a connection first issues WATCH (transitions from 0 watched keys to >0).
    pub fn increment_watch_count(&self) {
        self.active_watch_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the active WATCH connection count.
    /// Called when a connection's watched keys are cleared (EXEC/DISCARD/UNWATCH/disconnect).
    pub fn decrement_watch_count(&self) {
        self.active_watch_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get the current active WATCH connection count.
    #[cfg(test)]
    pub fn active_watch_count(&self) -> u32 {
        self.active_watch_count.load(Ordering::Relaxed)
    }

    /// Remove key_versions entries for keys that no longer exist in any database.
    /// Called periodically from the expiration task to prevent unbounded growth.
    pub fn cleanup_stale_key_versions(&self) {
        self.key_versions.retain(|scoped_key, _| {
            self.key_exists_in_any_db(scoped_key)
        });
    }

    /// Check if a scoped key (db_index prefix + key) exists in the corresponding database.
    /// Used for key_versions cleanup - checks if the actual data key still exists.
    fn key_exists_in_any_db(&self, scoped_key: &[u8]) -> bool {
        // Scoped key format: db_index as u64 le bytes (8 bytes) followed by the actual key.
        if scoped_key.len() <= 8 {
            return false;
        }
        let db_bytes: [u8; 8] = scoped_key[..8].try_into().unwrap_or_default();
        let db_index = u64::from_le_bytes(db_bytes) as usize;
        let key = &scoped_key[8..];
        if db_index >= NUM_DATABASES {
            return false;
        }
        let db = self.get_db_by_index(db_index);
        db.contains_key(key)
    }

    /// Check if the current task is inside a MULTI/EXEC transaction execution.
    pub fn in_transaction() -> bool {
        IN_TRANSACTION.try_with(|v| *v).unwrap_or(false)
    }

    /// Lock multiple keys in sorted order for transaction isolation.
    /// Uses per-shard transaction locks from ShardedStore, sorted and deduplicated to prevent deadlocks.
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

        let store = self.active_db();
        let shard_indices = store.unique_shard_indices(keys);
        let mut guards = Vec::with_capacity(shard_indices.len());
        for &idx in &shard_indices {
            let mutex = store.shard_tx_lock(idx);
            guards.push(mutex.lock_owned().await);
        }

        KeyLockGuard { _guards: guards }
    }

    /// Lock keys across multiple databases in sorted order for transaction isolation.
    /// Uses per-shard transaction locks from ShardedStore. Collects (db_index, shard_index)
    /// pairs across multiple databases, sorted and deduplicated to prevent deadlocks.
    /// Used by EXEC to lock both command keys (current DB) and WATCH keys (which may be in different DBs).
    pub async fn lock_keys_multi_db(
        &self,
        current_db_keys: &[Vec<u8>],
        db_scoped_pairs: &[(usize, Vec<u8>)],
    ) -> KeyLockGuard {
        if Self::in_transaction() {
            return KeyLockGuard {
                _guards: Vec::new(),
            };
        }

        let db_idx = Self::current_db_idx();
        // Collect (db_index, shard_index) pairs
        let mut shard_ids: Vec<(usize, usize)> = Vec::new();
        let current_store = self.get_db_by_index(db_idx);
        for key in current_db_keys {
            shard_ids.push((db_idx, current_store.shard_index(key)));
        }
        for (db, key) in db_scoped_pairs {
            let store = self.get_db_by_index(*db);
            shard_ids.push((*db, store.shard_index(key)));
        }
        shard_ids.sort();
        shard_ids.dedup();

        let mut guards = Vec::with_capacity(shard_ids.len());
        for (db, shard_idx) in &shard_ids {
            let store = self.get_db_by_index(*db);
            let mutex = store.shard_tx_lock(*shard_idx);
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
                            let elems: Vec<Vec<u8>> = elements.iter().map(|e| e.to_vec()).collect();
                            match existing {
                                Some(StoreValue::List(deque)) => {
                                    let mut delta: i64 = 0;
                                    for elem in elems {
                                        delta += elem.len() as i64 + 24;
                                        deque.push_back(elem);
                                    }
                                    Ok((ModifyResult::Keep(delta), ()))
                                }
                                None => {
                                    let deque: std::collections::VecDeque<Vec<u8>> = elems.into_iter().collect();
                                    Ok((ModifyResult::Set(StoreValue::List(deque)), ()))
                                }
                                _ => unreachable!(),
                            }
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
                            match existing {
                                Some(StoreValue::Set(set)) => {
                                    let mut delta: i64 = 0;
                                    for m in members {
                                        if set.insert(m.clone()) {
                                            delta += m.len() as i64 + 32;
                                        }
                                    }
                                    Ok((ModifyResult::Keep(delta), ()))
                                }
                                None => {
                                    let mut set = NativeHashSet::default();
                                    for m in members {
                                        set.insert(m.clone());
                                    }
                                    Ok((ModifyResult::Set(StoreValue::Set(set)), ()))
                                }
                                _ => unreachable!(),
                            }
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
                            match existing {
                                Some(StoreValue::ZSet(zset)) => {
                                    let mut delta: i64 = 0;
                                    let mut i = 0;
                                    while i + 1 < pairs.len() {
                                        if let Ok(score_str) = std::str::from_utf8(&pairs[i])
                                            && let Ok(score) = score_str.parse::<f64>()
                                            && zset.insert(score, pairs[i + 1].clone())
                                        {
                                            delta += pairs[i + 1].len() as i64 * 2 + 40;
                                        }
                                        i += 2;
                                    }
                                    Ok((ModifyResult::Keep(delta), ()))
                                }
                                None => {
                                    let mut zset = SortedSetData::new();
                                    let mut i = 0;
                                    while i + 1 < pairs.len() {
                                        if let Ok(score_str) = std::str::from_utf8(&pairs[i])
                                            && let Ok(score) = score_str.parse::<f64>()
                                        {
                                            zset.insert(score, pairs[i + 1].clone());
                                        }
                                        i += 2;
                                    }
                                    Ok((ModifyResult::Set(StoreValue::ZSet(zset)), ()))
                                }
                                _ => unreachable!(),
                            }
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
                            match existing {
                                Some(StoreValue::Stream(stream)) => {
                                    let mut delta: i64 = 0;
                                    if let Ok(id_str) = std::str::from_utf8(id_bytes)
                                        && let Some(id) =
                                            crate::storage::stream::StreamEntryId::parse(id_str)
                                    {
                                        let mut fields = Vec::new();
                                        let mut j = 0;
                                        while j + 1 < field_args.len() {
                                            delta += field_args[j].len() as i64 + field_args[j + 1].len() as i64 + 16;
                                            fields.push((field_args[j].clone(), field_args[j + 1].clone()));
                                            j += 2;
                                        }
                                        delta += 32; // per-entry overhead
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
                                    Ok((ModifyResult::Keep(delta), ()))
                                }
                                None => {
                                    let mut stream = crate::storage::stream::StreamData::new();
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
                                    Ok((ModifyResult::Set(StoreValue::Stream(stream)), ()))
                                }
                                _ => unreachable!(),
                            }
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
            if entry.is_expired() {
                return Ok("none".to_string());
            }

            // Use the stored data type authoritatively
            let data_type = entry.data_type().as_str().to_string();

            Ok(data_type)
        } else {
            Ok("none".to_string())
        }
    }

    /// Get the stored data type for a key without legacy heuristic detection.
    /// Returns the raw data_type field from StorageItem, which is authoritative
    /// for keys created with the current storage format.
    pub async fn get_raw_data_type(&self, key: &[u8]) -> StorageResult<String> {
        let result = {
            if let Some(entry) = self.active_db().get(key) {
                if entry.is_expired() {
                    None // expired, need cleanup
                } else {
                    Some(entry.data_type().as_str().to_string())
                }
            } else {
                Some("none".to_string())
            }
        };
        match result {
            None => {
                self.remove_expired_key(key).await;
                Ok("none".to_string())
            }
            Some(dt) => Ok(dt),
        }
    }

    /// Get the current number of keys (O(1) operation for DBSIZE)
    pub fn get_key_count(&self) -> u64 {
        self.key_count.load(Ordering::Relaxed)
    }

    /// Get the configured maximum key size in bytes.
    pub fn max_key_size(&self) -> usize {
        self.config.max_key_size
    }

    /// Get the configured maximum value size in bytes.
    pub fn max_value_size(&self) -> usize {
        self.config.max_value_size
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
        self.current_memory.load(Ordering::Relaxed)
    }

    /// Get a random key from the storage engine (O(1) operation for RANDOMKEY)
    pub async fn get_random_key(&self) -> StorageResult<Option<Vec<u8>>> {
        let db = self.active_db();
        // Check if database is empty
        if db.is_empty() {
            return Ok(None);
        }

        // Sample random non-expired keys
        let sampled = db.sample(100, |key, entry| {
            if entry.is_expired() { None } else { Some(key.clone()) }
        });

        if sampled.is_empty() {
            Ok(None)
        } else {
            let idx = fastrand::usize(0..sampled.len());
            Ok(Some(sampled[idx].clone()))
        }
    }

    /// Get a reference to a specific database by logical index (0-15).
    /// Uses db_mapping for logical-to-physical translation.
    pub fn get_db(&self, index: usize) -> Option<&ShardedStore> {
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
        let expired = {
            if let Some(mut entry) = self.active_db().get_mut(key) {
                if entry.is_expired() {
                    Some(true)
                } else {
                    entry.touch();
                    Some(false)
                }
            } else {
                None
            }
        };
        match expired {
            Some(true) => {
                self.remove_expired_key(key).await;
                Ok(false)
            }
            Some(false) => Ok(true),
            None => Ok(false),
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
        // Extract result in sync block to avoid holding !Send guard across await.
        enum ExpiretimeResult { Expired, HasExpiry(i64), NoExpiry, NotFound }
        let result = {
            if let Some(entry) = self.active_db().get(key) {
                if entry.is_expired() {
                    ExpiretimeResult::Expired
                } else if let Some(expires_at) = entry.expires_at {
                    let now = cached_now();
                    if expires_at <= now {
                        ExpiretimeResult::NoExpiry
                    } else {
                        let remaining = expires_at - now;
                        let now_system = SystemTime::now();
                        let expire_system = now_system + remaining;
                        let unix_ts = expire_system
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64;
                        ExpiretimeResult::HasExpiry(unix_ts)
                    }
                } else {
                    ExpiretimeResult::NoExpiry
                }
            } else {
                ExpiretimeResult::NotFound
            }
        };
        match result {
            ExpiretimeResult::Expired => {
                self.remove_expired_key(key).await;
                Ok(None)
            }
            ExpiretimeResult::HasExpiry(ts) => Ok(Some(ts)),
            ExpiretimeResult::NoExpiry | ExpiretimeResult::NotFound => Ok(None),
        }
    }

    /// Get the absolute Unix timestamp (milliseconds) when a key will expire
    pub async fn pexpiretime(&self, key: &[u8]) -> StorageResult<Option<i64>> {
        // Extract result in sync block to avoid holding !Send guard across await.
        let result: Option<Option<i64>> = {
            if let Some(entry) = self.active_db().get(key) {
                if entry.is_expired() {
                    None // signals need for lazy cleanup
                } else if let Some(expires_at) = entry.expires_at {
                    let now = cached_now();
                    if expires_at <= now {
                        Some(None)
                    } else {
                        let remaining = expires_at - now;
                        let now_system = SystemTime::now();
                        let expire_system = now_system + remaining;
                        let unix_ms = expire_system
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;
                        Some(Some(unix_ms))
                    }
                } else {
                    Some(None)
                }
            } else {
                Some(None) // key doesn't exist
            }
        };
        match result {
            None => {
                self.remove_expired_key(key).await;
                Ok(None)
            }
            Some(val) => Ok(val),
        }
    }

    /// Copy a key to a new destination key. Optionally replace existing destination.
    pub async fn copy_key(&self, src: &[u8], dst: Vec<u8>, replace: bool) -> StorageResult<bool> {
        let db = self.active_db();
        // Get the source item - extract in sync block to avoid !Send guard across await
        let src_item_opt = {
            if let Some(entry) = db.get(src) {
                if entry.is_expired() {
                    None // expired, need cleanup
                } else {
                    Some((*entry).clone())
                }
            } else {
                return Ok(false); // key doesn't exist
            }
        };
        let src_item = match src_item_opt {
            Some(item) => item,
            None => {
                self.remove_expired_key(src).await;
                return Ok(false);
            }
        };

        // Clone with fresh timestamps but same TTL
        let mut new_item = crate::storage::item::Entry::new(src_item.value.clone());
        if let Some(expires_at) = src_item.expires_at {
            let now = cached_now();
            if expires_at > now {
                let remaining = expires_at - now;
                new_item.expire(remaining);
                self.add_to_expiration_queue(dst.clone(), cached_now() + remaining)
                    .await;
            }
        }

        let required_size = Self::calculate_entry_size(&dst, &src_item.value);

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
        let is_new_key = match db.entry(dst.clone()) {
            ShardEntry::Occupied(mut occ) => {
                // Atomically check replace permission under the shard lock
                if !replace && !occ.get().is_expired() {
                    return Ok(false);
                }
                let old_size = Self::calculate_entry_size(&dst, &occ.get().value) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::Relaxed);
                occ.insert(new_item);
                // Bump version while OccupiedEntry still holds the shard lock
                self.bump_key_version(&dst);
                false
            }
            ShardEntry::Vacant(vac) => {
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::Relaxed);
                vac.insert(new_item);
                self.bump_key_version(&dst);
                true
            }
        };

        if is_new_key {
            self.update_prefix_indices(&dst, true).await;
            self.key_count.fetch_add(1, Ordering::Relaxed);
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
        let (src_key, src_item) = match src.entry(key.to_vec()) {
            ShardEntry::Occupied(occ) => {
                if occ.get().is_expired() {
                    let (k, item) = occ.remove_entry();
                    // Bookkeeping for expired key removal
                    let size = Self::calculate_entry_size(&k, &item.value) as u64;
                    self.current_memory.fetch_sub(size, Ordering::Relaxed);
                    self.key_count.fetch_sub(1, Ordering::Relaxed);
                    return Ok(false);
                }
                occ.remove_entry()
            }
            ShardEntry::Vacant(_) => return Ok(false),
        };

        // Source is now atomically removed.  Try to insert into destination.
        let src_expires_at = src_item.expires_at;
        match dst.entry(key.to_vec()) {
            ShardEntry::Occupied(_) => {
                // Destination already exists - MOVE fails (Redis semantics).
                // Re-insert the source item to restore original state.
                src.insert(src_key, src_item);
                return Ok(false);
            }
            ShardEntry::Vacant(vac) => {
                vac.insert(src_item);
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
            && expires_at > cached_now()
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

        // Per-shard expiration heaps live inside each ShardedStore, so they
        // follow the db_mapping swap automatically. No heap rewrite needed.

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
        db1_phys.for_each(|key, _entry| {
            self.bump_key_version_for_db(key, db1);
            self.bump_key_version_for_db(key, db2);
        });
        db2_phys.for_each(|key, _entry| {
            self.bump_key_version_for_db(key, db1);
            self.bump_key_version_for_db(key, db2);
        });

        Ok(())
    }

    /// Get a raw StorageItem reference for OBJECT/DUMP commands
    pub async fn get_item(&self, key: &[u8]) -> StorageResult<Option<StorageItem>> {
        let result = {
            if let Some(entry) = self.active_db().get(key) {
                if entry.is_expired() {
                    None // need cleanup
                } else {
                    Some(Some((*entry).clone()))
                }
            } else {
                Some(None)
            }
        };
        match result {
            None => {
                self.remove_expired_key(key).await;
                Ok(None)
            }
            Some(val) => Ok(val),
        }
    }

    /// Get the idle time (seconds since last access) for a key
    pub async fn get_idle_time(&self, key: &[u8]) -> StorageResult<Option<u64>> {
        let result = {
            if let Some(entry) = self.active_db().get(key) {
                if entry.is_expired() {
                    None
                } else {
                    let idle = (crate::storage::item::lru_now() as u64)
                        .saturating_sub(entry.lru_clock as u64);
                    Some(Some(idle))
                }
            } else {
                Some(None)
            }
        };
        match result {
            None => {
                self.remove_expired_key(key).await;
                Ok(None)
            }
            Some(val) => Ok(val),
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
        let result = {
            if let Some(entry) = self.active_db().get(key) {
                if entry.is_expired() {
                    None // need cleanup
                } else {
                    let encoding = match &entry.value {
                StoreValue::Str(bytes) => {
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        if s.parse::<i64>().is_ok() {
                            "int"
                        } else if bytes.len() <= 44 {
                            "embstr"
                        } else {
                            "raw"
                        }
                    } else {
                        "raw"
                    }
                }
                StoreValue::List(list) => {
                    if list.len() <= 128 && list.iter().all(|el| el.len() <= 64) {
                        "listpack"
                    } else {
                        "quicklist"
                    }
                }
                StoreValue::Set(set) => {
                    // Redis 7 default: set-max-intset-entries = 512
                    if set.len() <= 512
                        && set.iter().all(|el| {
                            std::str::from_utf8(el).is_ok_and(|s| s.parse::<i64>().is_ok())
                        })
                    {
                        "intset"
                    } else if set.len() <= 128 && set.iter().all(|el| el.len() <= 64) {
                        "listpack"
                    } else {
                        "hashtable"
                    }
                }
                StoreValue::Hash(map) => {
                    if map.len() <= 128
                        && map.iter().all(|(k, v)| k.len() <= 64 && v.len() <= 64)
                    {
                        "listpack"
                    } else {
                        "hashtable"
                    }
                }
                StoreValue::ZSet(ss) => {
                    if ss.scores.len() <= 128 && ss.scores.keys().all(|k| k.len() <= 64) {
                        "listpack"
                    } else {
                        "skiplist"
                    }
                }
                StoreValue::Stream(_) => "stream",
            };
                    Some(Some(encoding.to_string()))
                }
            } else {
                Some(None)
            }
        };
        match result {
            None => {
                self.remove_expired_key(key).await;
                Ok(None)
            }
            Some(val) => Ok(val),
        }
    }

    /// Restore a key from a serialized value (for RESTORE command)
    pub async fn restore_key(
        &self,
        key: Vec<u8>,
        value: StoreValue,
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

        self.set_with_type(key, value, ttl).await?;
        Ok(true)
    }

    /// Count keys that belong to a given hash slot (cluster mode uses logical DB 0)
    pub async fn count_keys_in_slot(&self, slot: u16) -> usize {
        use crate::cluster::slot::key_hash_slot;
        let db = self.get_db_by_index(0);
        let mut count = 0;
        db.for_each(|key, _entry| {
            if key_hash_slot(key) == slot {
                count += 1;
            }
        });
        count
    }

    /// Get keys that belong to a given hash slot (up to count, cluster mode uses logical DB 0)
    pub async fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<Vec<u8>> {
        use crate::cluster::slot::key_hash_slot;
        let db = self.get_db_by_index(0);
        let mut keys = Vec::new();
        db.for_each(|key, _entry| {
            if keys.len() < count && key_hash_slot(key) == slot {
                keys.push(key.clone());
            }
        });
        keys
    }

    /// Dump a key's value as a cloned StoreValue (for MIGRATE/DUMP)
    #[allow(dead_code)]
    pub async fn dump_key(&self, key: &[u8]) -> Option<StoreValue> {
        let result = {
            if let Some(entry) = self.active_db().get(key) {
                if entry.is_expired() {
                    None // need cleanup
                } else {
                    Some(Some(entry.value.clone()))
                }
            } else {
                Some(None)
            }
        };
        match result {
            None => {
                self.remove_expired_key(key).await;
                None
            }
            Some(val) => val,
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

        let store_value = StoreValue::Str(value.into());
        let required_size = Self::calculate_entry_size(&key, &store_value);
        self.maybe_evict(required_size).await?;

        let mut item = crate::storage::item::Entry::new(store_value);
        let expires_at = if let Some(ttl) = ttl {
            item.expire(ttl);
            Some(cached_now() + ttl)
        } else {
            None
        };

        // Capture version inside the entry match block while the shard lock
        // is still held. This prevents a concurrent SET from overwriting the key
        // between our insert and version bump, which would cause MSETNX rollback
        // to incorrectly delete the concurrent writer's value.
        let (inserted, replaced_expired, version) = match self.active_db().entry(key.clone()) {
            ShardEntry::Occupied(mut occ) => {
                // Key exists - check if expired
                if occ.get().is_expired() {
                    let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                    self.current_memory
                        .fetch_add(required_size as u64, Ordering::Relaxed);
                    occ.insert(item);
                    // Bump version while OccupiedEntry still holds the shard lock
                    let v = self.bump_and_get_key_version(&key);
                    // Replaced expired key - entry already exists, don't increment key_count
                    (true, true, v)
                } else {
                    (false, false, 0)
                }
            }
            ShardEntry::Vacant(vac) => {
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::Relaxed);
                vac.insert(item);
                // Bump version while shard lock is still held
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
                self.key_count.fetch_add(1, Ordering::Relaxed);
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

        let store_value = StoreValue::Str(value.into());
        let required_size = Self::calculate_entry_size(&key, &store_value);
        self.maybe_evict(required_size).await?;

        let mut item = crate::storage::item::Entry::new(store_value);
        let expires_at = if let Some(ttl) = ttl {
            item.expire(ttl);
            Some(cached_now() + ttl)
        } else {
            None
        };

        let updated = match self.active_db().entry(key.clone()) {
            ShardEntry::Occupied(mut occ) => {
                if occ.get().is_expired() {
                    // Expired key doesn't count as existing
                    let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                    occ.remove();
                    self.key_count.fetch_sub(1, Ordering::Relaxed);
                    false
                } else {
                    let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                    self.current_memory
                        .fetch_add(required_size as u64, Ordering::Relaxed);
                    occ.insert(item);
                    // Bump version while OccupiedEntry still holds the shard lock
                    self.bump_key_version(&key);
                    true
                }
            }
            ShardEntry::Vacant(_) => false,
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

        let store_value = StoreValue::Str(value.into());
        let required_size = Self::calculate_entry_size(&key, &store_value);
        self.maybe_evict(required_size).await?;

        let mut item = crate::storage::item::Entry::new(store_value);
        let new_expires_at = if let Some(ttl) = ttl {
            item.expire(ttl);
            Some(cached_now() + ttl)
        } else {
            None
        };

        let (did_set, old_value, is_new_key, final_expires_at) =
            match self.active_db().entry(key.clone()) {
                ShardEntry::Occupied(mut occ) => {
                    let expired = occ.get().is_expired();
                    let effectively_exists = !expired;

                    if nx && effectively_exists {
                        // NX: don't set if key exists
                        let old: Option<Vec<u8>> = if get_old {
                            occ.get().value.as_str().map(|b| b.to_vec())
                        } else {
                            None
                        };
                        (false, old, false, None)
                    } else if xx && !effectively_exists {
                        // XX: don't set if key doesn't exist
                        if expired {
                            let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                            self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                            occ.remove();
                            self.key_count.fetch_sub(1, Ordering::Relaxed);
                        }
                        (false, None, false, None)
                    } else {
                        // Set the value
                        let old: Option<Vec<u8>> = if get_old && effectively_exists {
                            occ.get().value.as_str().map(|b| b.to_vec())
                        } else {
                            None
                        };
                        // KEEPTTL: preserve old expiration if key exists and keepttl is set
                        let preserved_expires = if keepttl && effectively_exists {
                            occ.get().expires_at
                        } else {
                            None
                        };
                        let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                        self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                        self.current_memory
                            .fetch_add(required_size as u64, Ordering::Relaxed);
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
                        // is_new_key is false for Occupied entries: the entry already
                        // exists (even if expired), so key_count must not be incremented.
                        (true, old, false, effective_expires)
                    }
                }
                ShardEntry::Vacant(vac) => {
                    if xx {
                        // XX: key must exist
                        (false, None, false, None)
                    } else {
                        self.current_memory
                            .fetch_add(required_size as u64, Ordering::Relaxed);
                        vac.insert(item);
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
                self.key_count.fetch_add(1, Ordering::Relaxed);
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
        // Stack buffer for itoa serialization (avoids String allocation).
        // i64::MIN = "-9223372036854775808" = 20 bytes, so 24 is plenty.
        let mut itoa_buf = itoa::Buffer::new();

        match self.active_db().entry(key.to_vec()) {
            ShardEntry::Occupied(mut occ) => {
                if occ.get().is_expired() {
                    // Treat expired as non-existent: set to delta
                    let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                    let new_val_bytes = itoa_buf.format(delta).as_bytes().to_vec();
                    let new_sv = StoreValue::Str(new_val_bytes.into());
                    let new_size = Self::calculate_entry_size(occ.key(), &new_sv) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                    self.current_memory.fetch_add(new_size, Ordering::Relaxed);
                    let mut new_item = crate::storage::item::Entry::new(new_sv);
                    new_item.expires_at = None;
                    occ.insert(new_item);
                    self.bump_key_version(key);
                    return Ok(delta);
                }
                // Check type
                if occ.get().data_type() != RedisDataType::String {
                    return Err(StorageError::WrongType);
                }
                // Parse current value directly from bytes
                let bytes = occ.get().value.as_str().ok_or(StorageError::WrongType)?;
                let current_str =
                    std::str::from_utf8(bytes).map_err(|_| StorageError::NotANumber)?;
                let current: i64 = current_str.parse().map_err(|_| StorageError::NotANumber)?;
                let new_val = current.checked_add(delta).ok_or(StorageError::NotANumber)?;
                let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                // Use itoa for direct integer serialization (no String allocation)
                let new_val_bytes = itoa_buf.format(new_val).as_bytes().to_vec();
                let new_sv = StoreValue::Str(new_val_bytes.into());
                let new_size = Self::calculate_entry_size(occ.key(), &new_sv) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                self.current_memory.fetch_add(new_size, Ordering::Relaxed);
                occ.get_mut().value = new_sv;
                occ.get_mut().touch();
                self.bump_key_version(key);
                Ok(new_val)
            }
            ShardEntry::Vacant(vac) => {
                let new_val_bytes = itoa_buf.format(delta).as_bytes().to_vec();
                let new_sv = StoreValue::Str(new_val_bytes.into());
                let size = Self::calculate_entry_size(key, &new_sv) as u64;
                self.current_memory.fetch_add(size, Ordering::Relaxed);
                self.key_count.fetch_add(1, Ordering::Relaxed);
                vac.insert(crate::storage::item::Entry::new(new_sv));
                self.bump_key_version(key);
                Ok(delta)
            }
        }
    }

    /// Atomically increment a key's float value by delta.
    /// If the key doesn't exist, it is created with value 0 before incrementing.
    /// Returns the new value after increment.
    pub fn atomic_incr_float(&self, key: &[u8], delta: f64) -> StorageResult<f64> {
        if delta.is_nan() || delta.is_infinite() {
            return Err(StorageError::NotAFloat);
        }
        match self.active_db().entry(key.to_vec()) {
            ShardEntry::Occupied(mut occ) => {
                if occ.get().is_expired() {
                    let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                    let new_val_str = format_float(delta);
                    let new_val_bytes = new_val_str.as_bytes().to_vec();
                    let new_sv = StoreValue::Str(new_val_bytes.into());
                    let new_size = Self::calculate_entry_size(occ.key(), &new_sv) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                    self.current_memory.fetch_add(new_size, Ordering::Relaxed);
                    let mut new_item = crate::storage::item::Entry::new(new_sv);
                    new_item.expires_at = None;
                    occ.insert(new_item);
                    self.bump_key_version(key);
                    return Ok(delta);
                }
                if occ.get().data_type() != RedisDataType::String {
                    return Err(StorageError::WrongType);
                }
                let bytes = occ.get().value.as_str().ok_or(StorageError::WrongType)?;
                let current_str =
                    std::str::from_utf8(bytes).map_err(|_| StorageError::NotAFloat)?;
                let current: f64 = current_str.parse().map_err(|_| StorageError::NotAFloat)?;
                if current.is_nan() || current.is_infinite() {
                    return Err(StorageError::NotAFloat);
                }
                let new_val = current + delta;
                if new_val.is_nan() || new_val.is_infinite() {
                    return Err(StorageError::NotAFloat);
                }
                let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                let new_val_str = format_float(new_val);
                let new_val_bytes = new_val_str.as_bytes().to_vec();
                let new_sv = StoreValue::Str(new_val_bytes.into());
                let new_size = Self::calculate_entry_size(occ.key(), &new_sv) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                self.current_memory.fetch_add(new_size, Ordering::Relaxed);
                occ.get_mut().value = new_sv;
                occ.get_mut().touch();
                self.bump_key_version(key);
                Ok(new_val)
            }
            ShardEntry::Vacant(vac) => {
                let new_val_str = format_float(delta);
                let new_val_bytes = new_val_str.as_bytes().to_vec();
                let new_sv = StoreValue::Str(new_val_bytes.into());
                let size = Self::calculate_entry_size(key, &new_sv) as u64;
                self.current_memory.fetch_add(size, Ordering::Relaxed);
                self.key_count.fetch_add(1, Ordering::Relaxed);
                vac.insert(crate::storage::item::Entry::new(new_sv));
                self.bump_key_version(key);
                Ok(delta)
            }
        }
    }

    /// Atomically append a value to a key's string value.
    /// If the key doesn't exist, it is created with the appended value.
    /// Returns the length of the string after append.
    pub fn atomic_append(&self, key: &[u8], append_value: &[u8]) -> StorageResult<usize> {
        match self.active_db().entry(key.to_vec()) {
            ShardEntry::Occupied(mut occ) => {
                if occ.get().is_expired() {
                    let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                    let new_val = append_value.to_vec();
                    let new_len = new_val.len();
                    let new_sv = StoreValue::Str(new_val.into());
                    let new_size = Self::calculate_entry_size(occ.key(), &new_sv) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                    self.current_memory.fetch_add(new_size, Ordering::Relaxed);
                    let mut new_item = crate::storage::item::Entry::new(new_sv);
                    new_item.expires_at = None;
                    occ.insert(new_item);
                    self.bump_key_version(key);
                    return Ok(new_len);
                }
                if occ.get().data_type() != RedisDataType::String {
                    return Err(StorageError::WrongType);
                }
                let old_val_size = occ.get().value.as_str().map(|b| b.len()).unwrap_or(0);
                let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                if let Some(bytes) = occ.get_mut().value.as_compact_str_mut() {
                    bytes.append(append_value);
                }
                let new_len = occ.get().value.as_str().map(|b| b.len()).unwrap_or(0);
                let new_size = old_size + (new_len - old_val_size) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                self.current_memory.fetch_add(new_size, Ordering::Relaxed);
                occ.get_mut().touch();
                self.bump_key_version(key);
                Ok(new_len)
            }
            ShardEntry::Vacant(vac) => {
                let new_val = append_value.to_vec();
                let new_len = new_val.len();
                let new_sv = StoreValue::Str(new_val.into());
                let size = Self::calculate_entry_size(key, &new_sv) as u64;
                self.current_memory.fetch_add(size, Ordering::Relaxed);
                self.key_count.fetch_add(1, Ordering::Relaxed);
                vac.insert(crate::storage::item::Entry::new(new_sv));
                self.bump_key_version(key);
                Ok(new_len)
            }
        }
    }

    /// Generic atomic read-modify-write for collection types.
    /// The closure receives `Option<&mut Vec<u8>>` (None if key doesn't exist or is expired)
    /// and returns `Option<Vec<u8>>` (None to delete the key, Some to set the new value).
    /// The closure runs while holding the shard entry lock, ensuring atomicity.
    /// Returns the closure's return value.
    pub fn atomic_modify<F, R>(
        &self,
        key: &[u8],
        data_type: RedisDataType,
        f: F,
    ) -> StorageResult<R>
    where
        F: FnOnce(Option<&mut StoreValue>) -> Result<(ModifyResult, R), StorageError>,
    {
        match self.active_db().entry(key.to_vec()) {
            ShardEntry::Occupied(mut occ) => {
                let expired = occ.get().is_expired();
                if !expired && occ.get().data_type() != data_type {
                    return Err(StorageError::WrongType);
                }
                // O(1) old_size: use cached_mem_size + key.len() instead of O(n) calculate_entry_size.
                // Falls back to full calculation when cached_mem_size is 0 (pre-cache entries from RDB/AOF restore).
                let cached = occ.get().cached_mem_size;
                let old_size = if cached > 0 {
                    occ.key().len() as u64 + cached
                } else {
                    Self::calculate_entry_size(occ.key(), &occ.get().value) as u64
                };

                let (action, result) = if expired {
                    f(None)?
                } else {
                    f(Some(&mut occ.get_mut().value))?
                };

                let is_unchanged = matches!(&action, ModifyResult::KeepUnchanged);

                match action {
                    ModifyResult::KeepUnchanged => {
                        if expired {
                            // Expired key, closure got None — clean up
                            self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                            occ.remove();
                            self.key_count.fetch_sub(1, Ordering::Relaxed);
                            // Physical removal of an expired entry is a state change
                            // that WATCH must detect — bump the key version
                            self.bump_key_version(key);
                        }
                        // Non-expired: true no-op — no touch, no memory update
                    }
                    ModifyResult::Keep(delta) => {
                        if expired {
                            // Expired key, closure got None but returned Keep = no-op
                            // Clean up the expired entry
                            self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                            occ.remove();
                            self.key_count.fetch_sub(1, Ordering::Relaxed);
                        } else {
                            // O(1) memory update using delta instead of recalculating entry size
                            if delta != 0 {
                                let entry = occ.get_mut();
                                // Lazy-init cached_mem_size for pre-cache entries (serde default 0)
                                if entry.cached_mem_size == 0 {
                                    entry.cached_mem_size = entry.value.mem_size() as u64;
                                }
                                entry.cached_mem_size =
                                    (entry.cached_mem_size as i64 + delta) as u64;
                                if delta > 0 {
                                    self.current_memory
                                        .fetch_add(delta as u64, Ordering::Relaxed);
                                } else {
                                    self.current_memory
                                        .fetch_sub((-delta) as u64, Ordering::Relaxed);
                                }
                            }
                            occ.get_mut().touch();
                        }
                    }
                    ModifyResult::Set(val) => {
                        let new_val_mem = val.mem_size() as u64;
                        let new_size = key.len() as u64 + new_val_mem;
                        self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                        self.current_memory.fetch_add(new_size, Ordering::Relaxed);
                        if expired {
                            occ.insert(Entry::new(val));
                        } else {
                            let entry = occ.get_mut();
                            entry.value = val;
                            entry.cached_mem_size = new_val_mem;
                            entry.touch();
                        }
                    }
                    ModifyResult::Delete => {
                        self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                        occ.remove();
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
                    }
                }
                if !is_unchanged {
                    self.bump_key_version(key);
                    self.increment_write_counters_sync();
                }
                Ok(result)
            }
            ShardEntry::Vacant(vac) => {
                let (action, result) = f(None)?;
                match action {
                    ModifyResult::Set(val) => {
                        let size = Self::calculate_entry_size(key, &val) as u64;
                        self.current_memory.fetch_add(size, Ordering::Relaxed);
                        self.key_count.fetch_add(1, Ordering::Relaxed);
                        vac.insert(Entry::new(val));
                        self.bump_key_version(key);
                        self.increment_write_counters_sync();
                    }
                    ModifyResult::Keep(_) | ModifyResult::KeepUnchanged | ModifyResult::Delete => {
                        // Nothing to keep or delete for vacant entry
                    }
                }
                Ok(result)
            }
        }
    }

    /// Atomic read access to a key with LRU/LFU tracking. Uses an exclusive shard lock
    /// (ShardedStore `get_mut()`) to update last-accessed time for correct eviction behavior.
    /// Does NOT bump key version or write data back. Returns `(result, was_expired)` where
    /// `was_expired` indicates the caller should trigger lazy expiration cleanup.
    pub fn atomic_read<F, R>(&self, key: &[u8], data_type: RedisDataType, f: F) -> StorageResult<R>
    where
        F: FnOnce(Option<&StoreValue>) -> Result<R, StorageError>,
    {
        match self.active_db().get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    f(None)
                } else if entry.data_type() != data_type {
                    Err(StorageError::WrongType)
                } else {
                    entry.touch();
                    f(Some(&entry.value))
                }
            }
            None => f(None),
        }
    }

    /// Atomically get and delete a key. Returns the old value if it existed.
    /// Returns Err(WrongType) if the key exists but is not a string type.
    pub fn atomic_getdel(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        match self.active_db().entry(key.to_vec()) {
            ShardEntry::Occupied(occ) => {
                if occ.get().is_expired() {
                    let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                    self.key_count.fetch_sub(1, Ordering::Relaxed);
                    occ.remove();
                    self.bump_key_version(key);
                    Ok(None)
                } else if occ.get().data_type() != RedisDataType::String {
                    Err(StorageError::WrongType)
                } else {
                    let old_value = occ.get().value.as_str().map(|b| b.to_vec());
                    let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                    self.key_count.fetch_sub(1, Ordering::Relaxed);
                    occ.remove();
                    self.bump_key_version(key);
                    Ok(old_value)
                }
            }
            ShardEntry::Vacant(_) => Ok(None),
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
        // Phase 1: Synchronous shard access (holds shard lock)
        let phase1_result: Result<_, StorageError> = {
            if let Some(mut entry) = self.active_db().get_mut(key) {
                let item = &mut *entry;

                if item.is_expired() {
                    Ok((None, None, false))
                } else if item.data_type() != RedisDataType::String {
                    Err(StorageError::WrongType)
                } else {
                    let value = item.value.as_str().map(|b| b.to_vec());

                    let (queue_action, changed) = match new_expiry {
                        None => (None, false),
                        Some(None) => {
                            item.remove_expiry();
                            (None, true)
                        }
                        Some(Some(duration)) => {
                            item.expire(duration);
                            let expires_at = cached_now() + duration;
                            (Some(expires_at), true)
                        }
                    };

                    item.touch();
                    Ok((value, queue_action, changed))
                }
            } else {
                Ok((None, None, false))
            }
            // Shard lock released here
        };
        let (value, queue_action, expiry_changed) = phase1_result?;

        // Phase 2: Async expiration queue work (no shard lock held)
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

        let store_value = StoreValue::Str(value.into());
        let required_size = Self::calculate_entry_size(&key, &store_value);
        self.maybe_evict(required_size).await?;

        let old_value = match self.active_db().entry(key.clone()) {
            ShardEntry::Occupied(mut occ) => {
                let expired = occ.get().is_expired();
                if !expired && occ.get().data_type() != RedisDataType::String {
                    return Err(StorageError::WrongType);
                }
                let old: Option<Vec<u8>> = if expired {
                    None
                } else {
                    occ.get().value.as_str().map(|b| b.to_vec())
                };
                let old_size = Self::calculate_entry_size(occ.key(), &occ.get().value) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::Relaxed);
                let item = crate::storage::item::Entry::new(store_value);
                occ.insert(item);
                // Bump version while OccupiedEntry still holds the shard lock
                self.bump_key_version(&key);
                old
            }
            ShardEntry::Vacant(vac) => {
                self.current_memory
                    .fetch_add(required_size as u64, Ordering::Relaxed);
                self.key_count.fetch_add(1, Ordering::Relaxed);
                vac.insert(crate::storage::item::Entry::new(store_value));
                self.bump_key_version(&key);
                None
            }
        };

        self.increment_write_counters().await;
        Ok(old_value)
    }

    // ========== Hash Primitives ==========
    // Hash data is stored natively as NativeHashMap (hashbrown + FxHash).

    /// Set one or more fields in a hash. Returns the number of NEW fields added.
    pub fn hash_set(&self, key: &[u8], fields: &[(&[u8], &[u8])]) -> StorageResult<i64> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            match existing {
                Some(StoreValue::Hash(map)) => {
                    let mut new_count = 0i64;
                    let mut delta: i64 = 0;
                    for &(field, value) in fields {
                        match map.insert(field.to_vec(), value.to_vec()) {
                            None => {
                                new_count += 1;
                                delta += field.len() as i64 + value.len() as i64 + 64;
                            }
                            Some(old_val) => {
                                // Field existed: delta is difference in value size
                                delta += value.len() as i64 - old_val.len() as i64;
                            }
                        }
                    }
                    Ok((ModifyResult::Keep(delta), new_count))
                }
                None => {
                    let mut map = NativeHashMap::default();
                    let mut new_count = 0i64;
                    for &(field, value) in fields {
                        if map.insert(field.to_vec(), value.to_vec()).is_none() {
                            new_count += 1;
                        }
                    }
                    Ok((ModifyResult::Set(StoreValue::Hash(map)), new_count))
                }
                _ => unreachable!(),
            }
        })
    }

    /// Set a field only if it does NOT exist. Returns true if the field was set.
    pub fn hash_set_nx(&self, key: &[u8], field: &[u8], value: &[u8]) -> StorageResult<bool> {
        let field_exists = self.atomic_read(key, RedisDataType::Hash, |existing| {
            match existing {
                Some(StoreValue::Hash(map)) => Ok(map.contains_key(field)),
                None => Ok(false),
                _ => unreachable!(),
            }
        })?;
        if field_exists {
            return Ok(false);
        }
        self.atomic_modify(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => {
                if map.contains_key(field) {
                    Ok((ModifyResult::Keep(0), false))
                } else {
                    let delta = field.len() as i64 + value.len() as i64 + 64;
                    map.insert(field.to_vec(), value.to_vec());
                    Ok((ModifyResult::Keep(delta), true))
                }
            }
            None => {
                let mut map = NativeHashMap::default();
                map.insert(field.to_vec(), value.to_vec());
                Ok((ModifyResult::Set(StoreValue::Hash(map)), true))
            }
            _ => unreachable!(),
        })
    }

    /// Get a single field from a hash.
    pub fn hash_get(&self, key: &[u8], field: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => Ok(map.get(field).cloned()),
            None => Ok(None),
            _ => unreachable!(),
        })
    }

    /// Get multiple fields from a hash.
    pub fn hash_mget(&self, key: &[u8], fields: &[&[u8]]) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => {
                Ok(fields.iter().map(|f| map.get(*f).cloned()).collect())
            }
            None => Ok(fields.iter().map(|_| None).collect()),
            _ => unreachable!(),
        })
    }

    /// Get all field-value pairs from a hash.
    pub fn hash_getall(&self, key: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => {
                Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            }
            None => Ok(Vec::new()),
            _ => unreachable!(),
        })
    }

    /// Delete one or more fields from a hash.
    pub fn hash_del(&self, key: &[u8], fields: &[&[u8]]) -> StorageResult<i64> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => {
                let mut removed = 0i64;
                let mut delta: i64 = 0;
                for &field in fields {
                    if let Some(old_val) = map.remove(field) {
                        removed += 1;
                        delta -= field.len() as i64 + old_val.len() as i64 + 64;
                    }
                }
                if map.is_empty() && removed > 0 {
                    Ok((ModifyResult::Delete, removed))
                } else if removed > 0 {
                    Ok((ModifyResult::Keep(delta), removed))
                } else {
                    Ok((ModifyResult::KeepUnchanged, 0))
                }
            }
            None => Ok((ModifyResult::KeepUnchanged, 0)),
            _ => unreachable!(),
        })
    }

    /// Check if a field exists in a hash.
    pub fn hash_exists(&self, key: &[u8], field: &[u8]) -> StorageResult<bool> {
        self.atomic_read(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => Ok(map.contains_key(field)),
            None => Ok(false),
            _ => unreachable!(),
        })
    }

    /// Get the number of fields in a hash.
    pub fn hash_len(&self, key: &[u8]) -> StorageResult<i64> {
        self.atomic_read(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => Ok(map.len() as i64),
            None => Ok(0),
            _ => unreachable!(),
        })
    }

    /// Get all field names in a hash.
    pub fn hash_keys(&self, key: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => Ok(map.keys().cloned().collect()),
            None => Ok(Vec::new()),
            _ => unreachable!(),
        })
    }

    /// Get all values in a hash.
    pub fn hash_vals(&self, key: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => Ok(map.values().cloned().collect()),
            None => Ok(Vec::new()),
            _ => unreachable!(),
        })
    }

    /// Increment a hash field's integer value by delta.
    pub fn hash_incr_by(&self, key: &[u8], field: &[u8], delta: i64) -> StorageResult<i64> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let (map, _is_new) = match existing {
                Some(StoreValue::Hash(map)) => (map, false),
                None => {
                    // Will create new hash below
                    let mut map = NativeHashMap::default();
                    let new_val = delta; // 0 + delta
                    map.insert(field.to_vec(), new_val.to_string().into_bytes());
                    return Ok((ModifyResult::Set(StoreValue::Hash(map)), new_val));
                }
                _ => unreachable!(),
            };
            let (current, old_val_len) = match map.get(field) {
                Some(v) => {
                    let old_len = v.len();
                    let s = std::str::from_utf8(v).map_err(|_| StorageError::NotANumber)?;
                    (s.parse::<i64>().map_err(|_| StorageError::NotANumber)?, old_len as i64)
                }
                None => (0, -(field.len() as i64 + 64)), // new field: account for key+overhead
            };
            let new_val = current.checked_add(delta).ok_or(StorageError::NotANumber)?;
            let new_bytes = new_val.to_string().into_bytes();
            let mem_delta = new_bytes.len() as i64 - old_val_len;
            map.insert(field.to_vec(), new_bytes);
            Ok((ModifyResult::Keep(mem_delta), new_val))
        })
    }

    /// Increment a hash field's float value by delta.
    pub fn hash_incr_by_float(
        &self,
        key: &[u8],
        field: &[u8],
        delta: f64,
    ) -> StorageResult<String> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let (map, _is_new) = match existing {
                Some(StoreValue::Hash(map)) => (map, false),
                None => {
                    let new_val = delta;
                    if !new_val.is_finite() {
                        return Err(StorageError::NotAFloat);
                    }
                    let mut map = NativeHashMap::default();
                    let new_val_str = format_float(new_val);
                    map.insert(field.to_vec(), new_val_str.as_bytes().to_vec());
                    return Ok((ModifyResult::Set(StoreValue::Hash(map)), new_val_str));
                }
                _ => unreachable!(),
            };
            let (current, old_val_len) = match map.get(field) {
                Some(v) => {
                    let old_len = v.len();
                    let s = std::str::from_utf8(v).map_err(|_| StorageError::NotAFloat)?;
                    let f: f64 = s.parse().map_err(|_| StorageError::NotAFloat)?;
                    if !f.is_finite() {
                        return Err(StorageError::NotAFloat);
                    }
                    (f, old_len as i64)
                }
                None => (0.0, -(field.len() as i64 + 64)), // new field: account for key+overhead
            };
            let new_val = current + delta;
            if !new_val.is_finite() {
                return Err(StorageError::NotAFloat);
            }
            let new_val_str = format_float(new_val);
            let new_bytes = new_val_str.as_bytes().to_vec();
            let mem_delta = new_bytes.len() as i64 - old_val_len;
            map.insert(field.to_vec(), new_bytes);
            Ok((ModifyResult::Keep(mem_delta), new_val_str))
        })
    }

    /// Get the string length of a hash field's value.
    pub fn hash_strlen(&self, key: &[u8], field: &[u8]) -> StorageResult<i64> {
        self.atomic_read(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => {
                Ok(map.get(field).map(|v| v.len() as i64).unwrap_or(0))
            }
            None => Ok(0),
            _ => unreachable!(),
        })
    }

    /// Get random fields from a hash.
    pub fn hash_randfield(&self, key: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.atomic_read(key, RedisDataType::Hash, |existing| match existing {
            Some(StoreValue::Hash(map)) => {
                Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            }
            None => Ok(Vec::new()),
            _ => unreachable!(),
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
                StoreValue::List(std::collections::VecDeque::from(vec![b"data".to_vec()])),
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
                        let deque = std::collections::VecDeque::from(vec![b"item1".to_vec()]);
                        Ok((ModifyResult::Set(StoreValue::List(deque)), 1))
                    }
                    Some(_) => unreachable!(),
                }
            })
            .unwrap();
        assert_eq!(result, 1);

        // Modify the list to add another element
        let result: usize = storage
            .atomic_modify(b"my_list", RedisDataType::List, |existing| match existing {
                Some(StoreValue::List(deque)) => {
                    deque.push_back(b"item2".to_vec());
                    let new_len = deque.len();
                    Ok((ModifyResult::Keep(0), new_len))
                }
                _ => unreachable!(),
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
            Ok((ModifyResult::Keep(0), ()))
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
                StoreValue::Str(b"original_value".to_vec().into()),
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
                StoreValue::Str(b"new_value".to_vec().into()),
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
                StoreValue::Str(b"original".to_vec().into()),
                None,
            )
            .await
            .unwrap();

        // Overwrite with preserve_ttl - should have no TTL since original had none
        storage
            .set_with_type_preserve_ttl(
                b"no_ttl_key".to_vec(),
                StoreValue::Str(b"updated".to_vec().into()),
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
                StoreValue::Str(b"value".to_vec().into()),
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
                StoreValue::Str(b"initial".to_vec().into()),
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
                    StoreValue::Str(value.into()),
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
    async fn test_shard_based_locking() {
        let config = StorageConfig::default();
        let storage = Arc::new(StorageEngine::new(config));

        // Create a key and acquire its shard lock
        let key = b"held_key".to_vec();
        storage
            .set(key.clone(), b"value".to_vec(), None)
            .await
            .unwrap();

        // Acquire shard lock
        let guard = storage.lock_keys(&[key.clone()]).await;

        // Can still read/write the key (data operations use separate locks)
        let val = storage.get(&key).await.unwrap();
        assert_eq!(val, Some(b"value".to_vec()));

        // Drop the guard
        drop(guard);

        // Key should still be accessible
        let val = storage.get(&key).await.unwrap();
        assert_eq!(val, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_concurrent_reads_use_read_lock() {
        // Verify that concurrent GET operations don't block each other
        // (read locks allow multiple concurrent readers)
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set up test data
        for i in 0..100 {
            let key = format!("read_key:{}", i);
            let val = format!("val:{}", i);
            storage.set(key.into_bytes(), val.into_bytes(), None).await.unwrap();
        }

        // Spawn many concurrent read tasks
        let mut handles = Vec::new();
        for _ in 0..16 {
            let storage = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                for i in 0..100 {
                    let key = format!("read_key:{}", i);
                    let val = storage.get(key.as_bytes()).await.unwrap();
                    assert!(val.is_some(), "Key {} should exist", key);
                    let expected = format!("val:{}", i);
                    assert_eq!(val.unwrap(), expected.as_bytes());
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_concurrent_exists_ttl_type_read_lock() {
        // Verify EXISTS, TTL, TYPE all use read locks and work concurrently
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        for i in 0..50 {
            let key = format!("rlock_key:{}", i);
            storage.set(key.into_bytes(), b"value".to_vec(), None).await.unwrap();
        }

        let mut handles = Vec::new();
        for _ in 0..8 {
            let storage = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                for i in 0..50 {
                    let key = format!("rlock_key:{}", i);
                    // All these should use read locks
                    assert!(storage.exists(key.as_bytes()).await.unwrap());
                    let _ttl = storage.ttl(key.as_bytes()).await.unwrap();
                    let dtype = storage.get_type(key.as_bytes()).await.unwrap();
                    assert_eq!(dtype, "string");
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_incr_atomicity_under_contention() {
        // Verify INCR is atomic: N threads each incrementing the same key M times
        // should result in N*M as the final value
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Initialize key to 0
        storage.set(b"counter".to_vec(), b"0".to_vec(), None).await.unwrap();

        let num_tasks = 16;
        let increments_per_task = 1000;

        let mut handles = Vec::new();
        for _ in 0..num_tasks {
            let storage = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                for _ in 0..increments_per_task {
                    storage.atomic_incr(b"counter", 1).unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let val = storage.get(b"counter").await.unwrap().unwrap();
        let final_value: i64 = std::str::from_utf8(&val).unwrap().parse().unwrap();
        assert_eq!(final_value, num_tasks * increments_per_task);
    }

    #[tokio::test]
    async fn test_decr_atomicity_under_contention() {
        // Same test but with DECR (negative delta)
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        let num_tasks: i64 = 8;
        let decrements: i64 = 500;
        let start_value = num_tasks * decrements;

        storage.set(
            b"dcounter".to_vec(),
            start_value.to_string().into_bytes(),
            None,
        ).await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..num_tasks {
            let storage = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                for _ in 0..decrements {
                    storage.atomic_incr(b"dcounter", -1).unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let val = storage.get(b"dcounter").await.unwrap().unwrap();
        let final_value: i64 = std::str::from_utf8(&val).unwrap().parse().unwrap();
        assert_eq!(final_value, 0);
    }

    #[tokio::test]
    async fn test_relaxed_ordering_counter_accuracy() {
        // Verify that relaxed atomic orderings still give correct final counts
        // after all concurrent operations complete
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        let num_tasks = 8;
        let ops_per_task = 100;

        let mut handles = Vec::new();
        for t in 0..num_tasks {
            let storage = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                for i in 0..ops_per_task {
                    let key = format!("relax_key:{}:{}", t, i);
                    storage.set(key.into_bytes(), b"v".to_vec(), None).await.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // After all tasks complete, key_count should reflect all insertions
        let count = storage.get_key_count();
        assert_eq!(count, (num_tasks * ops_per_task) as u64);

        // Memory tracking should be non-zero
        let mem = storage.get_used_memory();
        assert!(mem > 0, "Memory usage should be tracked");
    }

    #[tokio::test]
    async fn test_update_prefix_indices_batch_for_db() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let db_idx = 0;

        // Batch insert multiple keys with indexed prefixes
        let updates: Vec<(&[u8], bool)> = vec![
            (b"user:alice", true),
            (b"user:bob", true),
            (b"cache:page1", true),
            (b"nonindexed:key", true), // should be ignored
        ];
        storage
            .update_prefix_indices_batch_for_db(&updates, db_idx)
            .await;

        // Verify prefix index state
        let index = storage.prefix_index.read().await;
        let user_keys = index.get(&(db_idx, "user:".to_string())).unwrap();
        assert_eq!(user_keys.len(), 2);
        assert!(user_keys.contains(&b"user:alice".to_vec()));
        assert!(user_keys.contains(&b"user:bob".to_vec()));

        let cache_keys = index.get(&(db_idx, "cache:".to_string())).unwrap();
        assert_eq!(cache_keys.len(), 1);
        assert!(cache_keys.contains(&b"cache:page1".to_vec()));

        // Non-indexed prefix should not appear
        assert!(index.get(&(db_idx, "nonindexed:".to_string())).is_none());
        drop(index);

        // Batch delete one key and insert another
        let updates2: Vec<(&[u8], bool)> = vec![
            (b"user:alice", false), // delete
            (b"user:charlie", true), // insert
        ];
        storage
            .update_prefix_indices_batch_for_db(&updates2, db_idx)
            .await;

        let index = storage.prefix_index.read().await;
        let user_keys = index.get(&(db_idx, "user:".to_string())).unwrap();
        assert_eq!(user_keys.len(), 2);
        assert!(!user_keys.contains(&b"user:alice".to_vec()));
        assert!(user_keys.contains(&b"user:bob".to_vec()));
        assert!(user_keys.contains(&b"user:charlie".to_vec()));
        drop(index);

        // Delete all keys from a prefix -- entry should be cleaned up
        let updates3: Vec<(&[u8], bool)> = vec![
            (b"cache:page1", false),
        ];
        storage
            .update_prefix_indices_batch_for_db(&updates3, db_idx)
            .await;

        let index = storage.prefix_index.read().await;
        assert!(index.get(&(db_idx, "cache:".to_string())).is_none());
        drop(index);

        // Empty batch should be a no-op
        storage
            .update_prefix_indices_batch_for_db(&[], db_idx)
            .await;
    }

    #[tokio::test]
    async fn test_key_versions_cleanup_after_expiry() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set keys with short TTL
        for i in 0..5 {
            let key = format!("expiring_key_{}", i);
            storage
                .set(key.as_bytes().to_vec(), b"val".to_vec(), Some(Duration::from_millis(50)))
                .await
                .unwrap();
            // Bump version to populate key_versions map
            storage.bump_key_version(key.as_bytes());
        }

        // Also set a persistent key
        storage
            .set(b"persistent_key".to_vec(), b"val".to_vec(), None)
            .await
            .unwrap();
        storage.bump_key_version(b"persistent_key");

        // key_versions should have entries for all 6 keys
        assert_eq!(storage.key_versions_len(), 6);

        // Wait for expiration
        sleep(Duration::from_millis(100)).await;

        // Access expired keys to trigger lazy expiration
        for i in 0..5 {
            let key = format!("expiring_key_{}", i);
            let _ = storage.get(key.as_bytes()).await;
        }

        // Run cleanup
        storage.cleanup_stale_key_versions();

        // Only the persistent key's version should remain
        assert_eq!(storage.key_versions_len(), 1);

        // Verify the persistent key's version is still there
        let version = storage.get_key_version(b"persistent_key");
        assert!(version > 0);
    }

    #[tokio::test]
    async fn test_active_watch_count() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        assert_eq!(storage.active_watch_count(), 0);

        storage.increment_watch_count();
        assert_eq!(storage.active_watch_count(), 1);

        storage.increment_watch_count();
        assert_eq!(storage.active_watch_count(), 2);

        storage.decrement_watch_count();
        assert_eq!(storage.active_watch_count(), 1);

        storage.decrement_watch_count();
        assert_eq!(storage.active_watch_count(), 0);
    }

    #[tokio::test]
    async fn test_key_exists_in_any_db() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        // Set a key in DB 0
        storage
            .set(b"mykey".to_vec(), b"val".to_vec(), None)
            .await
            .unwrap();

        // Create a scoped key for DB 0
        let scoped = StorageEngine::db_scoped_key(0, b"mykey");
        assert!(storage.key_exists_in_any_db(&scoped));

        // Non-existent key should not exist
        let scoped_missing = StorageEngine::db_scoped_key(0, b"missing");
        assert!(!storage.key_exists_in_any_db(&scoped_missing));

        // Invalid scoped key (too short) should return false
        assert!(!storage.key_exists_in_any_db(&[1, 2, 3]));
    }

    #[tokio::test]
    async fn test_lpush_lpop_memory_returns_to_baseline() {
        // Test: LPUSH N items then LPOP N items - verify used_memory returns to baseline
        let storage = StorageEngine::new(StorageConfig::default());

        let baseline = storage.current_memory.load(Ordering::Relaxed);

        // LPUSH 100 items
        let n = 100;
        for i in 0..n {
            let val = format!("value_{:04}", i).into_bytes();
            storage
                .atomic_modify(b"testlist", RedisDataType::List, {
                    let val = val.clone();
                    move |current| match current {
                        Some(StoreValue::List(deque)) => {
                            let delta = val.len() as i64 + 24;
                            deque.push_front(val);
                            Ok((ModifyResult::Keep(delta), ()))
                        }
                        None => {
                            let deque = std::collections::VecDeque::from(vec![val]);
                            Ok((ModifyResult::Set(StoreValue::List(deque)), ()))
                        }
                        _ => unreachable!(),
                    }
                })
                .unwrap();
        }

        let after_push = storage.current_memory.load(Ordering::Relaxed);
        assert!(after_push > baseline, "Memory should increase after LPUSH");

        // LPOP all 100 items
        for _ in 0..n {
            let result: Option<Vec<u8>> = storage
                .atomic_modify(b"testlist", RedisDataType::List, |current| match current {
                    Some(StoreValue::List(deque)) => {
                        if let Some(e) = deque.pop_front() {
                            let delta = -(e.len() as i64 + 24);
                            if deque.is_empty() {
                                Ok((ModifyResult::Delete, Some(e)))
                            } else {
                                Ok((ModifyResult::Keep(delta), Some(e)))
                            }
                        } else {
                            Ok((ModifyResult::Keep(0), None))
                        }
                    }
                    None => Ok((ModifyResult::Keep(0), None)),
                    _ => unreachable!(),
                })
                .unwrap();
            assert!(result.is_some());
        }

        let after_pop = storage.current_memory.load(Ordering::Relaxed);
        assert_eq!(
            after_pop, baseline,
            "Memory should return to baseline after popping all items"
        );
    }

    #[tokio::test]
    async fn test_cached_mem_size_matches_calculate_entry_size() {
        use crate::storage::value::NativeHashSet;
        let storage = StorageEngine::new(StorageConfig::default());

        // Test List: push some items
        for i in 0..10 {
            let val = format!("item_{}", i).into_bytes();
            storage
                .atomic_modify(b"list_key", RedisDataType::List, {
                    let val = val.clone();
                    move |current| match current {
                        Some(StoreValue::List(deque)) => {
                            let delta = val.len() as i64 + 24;
                            deque.push_back(val);
                            Ok((ModifyResult::Keep(delta), ()))
                        }
                        None => {
                            let deque = std::collections::VecDeque::from(vec![val]);
                            Ok((ModifyResult::Set(StoreValue::List(deque)), ()))
                        }
                        _ => unreachable!(),
                    }
                })
                .unwrap();
        }
        // Verify cached_mem_size matches value.mem_size() for list
        storage.active_db().get_mut(b"list_key").map(|entry| {
            let actual = entry.value.mem_size() as u64;
            assert_eq!(
                entry.cached_mem_size, actual,
                "List: cached_mem_size {} != actual {}",
                entry.cached_mem_size, actual
            );
        });

        // Test Set: add some members
        for i in 0..10 {
            let member = format!("member_{}", i).into_bytes();
            storage
                .atomic_modify(b"set_key", RedisDataType::Set, {
                    let member = member.clone();
                    move |current| match current {
                        Some(StoreValue::Set(set)) => {
                            let delta = if set.insert(member.clone()) {
                                member.len() as i64 + 32
                            } else {
                                0
                            };
                            Ok((ModifyResult::Keep(delta), ()))
                        }
                        None => {
                            let mut set = NativeHashSet::default();
                            set.insert(member);
                            Ok((ModifyResult::Set(StoreValue::Set(set)), ()))
                        }
                        _ => unreachable!(),
                    }
                })
                .unwrap();
        }
        // Verify cached_mem_size matches value.mem_size() for set
        storage.active_db().get_mut(b"set_key").map(|entry| {
            let actual = entry.value.mem_size() as u64;
            assert_eq!(
                entry.cached_mem_size, actual,
                "Set: cached_mem_size {} != actual {}",
                entry.cached_mem_size, actual
            );
        });

        // Test Hash: set some fields
        let fields: Vec<(&[u8], &[u8])> = vec![
            (b"f1", b"val1"),
            (b"f2", b"val2"),
            (b"f3", b"val3"),
        ];
        storage.hash_set(b"hash_key", &fields).unwrap();
        storage.active_db().get_mut(b"hash_key").map(|entry| {
            let actual = entry.value.mem_size() as u64;
            assert_eq!(
                entry.cached_mem_size, actual,
                "Hash: cached_mem_size {} != actual {}",
                entry.cached_mem_size, actual
            );
        });

        // Test String: set via regular path
        storage
            .set(b"str_key".to_vec(), b"hello_world".to_vec(), None)
            .await
            .unwrap();
        storage.active_db().get_mut(b"str_key").map(|entry| {
            let actual = entry.value.mem_size() as u64;
            assert_eq!(
                entry.cached_mem_size, actual,
                "String: cached_mem_size {} != actual {}",
                entry.cached_mem_size, actual
            );
        });
    }
}
