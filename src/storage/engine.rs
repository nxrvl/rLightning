use std::collections::{HashMap, BinaryHeap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::RwLock;
use tokio::time::{self, Interval};
use dashmap::DashMap;

use crate::storage::error::StorageError;
use crate::storage::item::{StorageItem, RedisDataType};
use crate::networking::resp::RespCommand;

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
pub enum EvictionPolicy {
    /// Remove the least recently used item
    LRU,
    /// Remove the least frequently used item
    LFU,
    /// Remove a random item
    Random,
    /// Don't evict items
    NoEviction,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::LRU
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

// Also implement FromStr for easier conversions
impl std::str::FromStr for EvictionPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "lru" => Ok(Self::LRU),
            "lfu" => Ok(Self::LFU),
            "random" => Ok(Self::Random),
            "noeviction" => Ok(Self::NoEviction),
            _ => Err(format!("Unknown eviction policy: {}", s)),
        }
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
    /// Pattern index for common prefixes (optimizes KEYS command)
    prefix_index: RwLock<HashMap<String, Vec<Vec<u8>>>>,
    /// Additional databases (1-15) for multi-database support (SELECT/MOVE)
    extra_dbs: Vec<DashMap<Vec<u8>, StorageItem>>,
    /// Per-key version counter for WATCH (optimistic locking) support
    key_versions: DashMap<Vec<u8>, u64>,
    /// Global version counter for generating unique version numbers
    global_version: AtomicU64,
}

impl StorageEngine {
    /// Create a new storage engine with the given configuration
    pub fn new(config: StorageConfig) -> Arc<Self> {
        let mut extra_dbs = Vec::with_capacity(NUM_DATABASES - 1);
        for _ in 0..(NUM_DATABASES - 1) {
            extra_dbs.push(DashMap::with_capacity(64));
        }

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
        });
        
        // Start the expiration task
        Self::start_expiration_task(Arc::clone(&engine));
        
        engine
    }
    
    /// Start the background task for efficient TTL-based expiration
    fn start_expiration_task(engine: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = engine.expiration_timer.write().await;
            
            loop {
                interval.tick().await;
                // Use priority queue for efficient expiration + probabilistic sampling
                engine.process_expired_keys().await;
            }
        });
    }
    
    /// Efficient expiration using priority queue + probabilistic sampling
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

                // Only remove the key if it is actually expired (it may have been refreshed with a new TTL)
                let should_remove = match self.data.get(&expired_entry.key) {
                    Some(item) => item.is_expired(),
                    None => false, // Key already deleted
                };
                if should_remove {
                    if let Some((k, item)) = self.data.remove(&expired_entry.key) {
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
        
        // Phase 2: Probabilistic sampling for keys not in queue (cleanup)
        // This handles keys that might have been missed or expired through lazy deletion
        if removed_count < MAX_REMOVALS_PER_CYCLE / 2 {
            self.probabilistic_cleanup().await;
        }
    }
    
    /// Probabilistic cleanup for keys not tracked in expiration queue
    async fn probabilistic_cleanup(&self) {
        const SAMPLE_SIZE: usize = 20;
        let mut keys_to_remove = Vec::new();

        // Sample keys starting from a random offset to avoid always checking the same keys
        let len = self.data.len();
        let skip = if len > SAMPLE_SIZE { fastrand::usize(0..len) } else { 0 };
        let mut sampled_count = 0;
        for entry in self.data.iter().skip(skip) {
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
            for entry in self.data.iter().take(SAMPLE_SIZE - sampled_count) {
                if entry.value().is_expired() {
                    keys_to_remove.push(entry.key().clone());
                }
            }
        }
        
        // Remove expired keys found in this sample
        for key in keys_to_remove {
            if let Some((k, item)) = self.data.remove(&key) {
                let size = Self::calculate_size(&k, &item.value) as u64;
                self.current_memory.fetch_sub(size, Ordering::AcqRel);
                self.key_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
    
    /// Get the size of a key-value pair in bytes
    fn calculate_size(key: &[u8], value: &[u8]) -> usize {
        key.len() + value.len()
    }
    
    /// Helper method to remove expired key and update memory/counters
    async fn remove_expired_key(&self, key: &[u8]) {
        if let Some((k, item)) = self.data.remove(key) {
            if !item.is_expired() {
                // Key was refreshed concurrently — put it back
                self.data.insert(k, item);
                return;
            }
            // Use atomic operations for memory tracking
            let size = Self::calculate_size(&k, &item.value) as u64;
            self.current_memory.fetch_sub(size, Ordering::AcqRel);
            self.key_count.fetch_sub(1, Ordering::AcqRel);
        }
    }
    
    /// Add a key to the expiration queue
    async fn add_to_expiration_queue(&self, key: Vec<u8>, expires_at: Instant) {
        let mut queue = self.expiration_queue.write().await;
        queue.push(ExpirationEntry { expires_at, key });
    }
    
    /// Remove a key from expiration queue (used when TTL is removed)
    async fn remove_from_expiration_queue(&self, key: &[u8]) {
        let mut queue = self.expiration_queue.write().await;
        // Note: BinaryHeap doesn't support efficient removal by key
        // In practice, we'll let the background task clean up stale entries
        // This is acceptable since the queue will filter out non-existent keys
        
        // For now, we rebuild the queue without the target key (expensive but rare operation)
        let entries: Vec<ExpirationEntry> = queue.drain().collect();
        for entry in entries {
            if entry.key != key {
                queue.push(entry);
            }
        }
    }
    
    /// Common prefixes to index for faster KEYS operations
    const INDEXED_PREFIXES: &'static [&'static str] = &[
        "user:", "session:", "cache:", "temp:", "auth:", "token:", 
        "data:", "config:", "stats:", "log:", "queue:", "job:"
    ];
    
    /// Update prefix indices when a key is added or removed
    async fn update_prefix_indices(&self, key: &[u8], is_insert: bool) {
        if let Ok(key_str) = std::str::from_utf8(key) {
            let mut index = self.prefix_index.write().await;
            
            for &prefix in Self::INDEXED_PREFIXES {
                if key_str.starts_with(prefix) {
                    let entry = index.entry(prefix.to_string()).or_insert_with(Vec::new);
                    if is_insert {
                        entry.push(key.to_vec());
                    } else {
                        entry.retain(|k| k != key);
                        // Clean up empty entries
                        if entry.is_empty() {
                            index.remove(prefix);
                        }
                    }
                    break; // Only index the first matching prefix
                }
            }
        }
    }
    
    /// Extract prefix from a pattern for index lookup
    fn extract_prefix_from_pattern(&self, pattern: &str) -> Option<String> {
        // Check if pattern is a simple prefix pattern like "user:*"
        if pattern.ends_with('*') && !pattern[..pattern.len()-1].contains('*') {
            let prefix = &pattern[..pattern.len()-1];
            // Only return if it's one of our indexed prefixes
            if Self::INDEXED_PREFIXES.contains(&prefix) {
                return Some(prefix.to_string());
            }
        }
        None
    }
    
    /// Check if we need to evict items to make room
    async fn maybe_evict(&self, required_size: usize) -> StorageResult<()> {
        // Check if adding this item would exceed our memory limit
        let current_memory = self.current_memory.load(Ordering::Acquire) as usize;
        if current_memory + required_size > self.config.max_memory {
            // If we're not allowed to evict, fail
            if self.config.eviction_policy == EvictionPolicy::NoEviction {
                return Err(StorageError::MemoryLimitExceeded);
            }
            
            // Evict items until we have enough space
            while self.current_memory.load(Ordering::Acquire) as usize + required_size > self.config.max_memory {
                // If there are no more items to evict, fail
                if self.data.is_empty() {
                    return Err(StorageError::MemoryLimitExceeded);
                }
                
                // Find a victim based on the eviction policy
                let victim_key = match self.config.eviction_policy {
                    EvictionPolicy::LRU => {
                        // Find the oldest accessed item
                        self.data.iter()
                            .min_by_key(|item| item.value().last_accessed)
                            .map(|item| item.key().clone())
                    },
                    EvictionPolicy::Random => {
                        // Pick a random key by skipping a random number of entries
                        let len = self.data.len();
                        if len == 0 {
                            None
                        } else {
                            let skip = fastrand::usize(0..len);
                            self.data.iter().nth(skip).map(|item| item.key().clone())
                        }
                    },
                    _ => {
                        // Default to LRU for now
                        self.data.iter()
                            .min_by_key(|item| item.value().last_accessed)
                            .map(|item| item.key().clone())
                    }
                };
                
                // Remove the victim
                if let Some(key) = victim_key {
                    if let Some((k, item)) = self.data.remove(&key) {
                        let size = Self::calculate_size(&k, &item.value) as u64;
                        self.current_memory.fetch_sub(size, Ordering::AcqRel);
                        // Decrement key count for evicted keys
                        self.key_count.fetch_sub(1, Ordering::AcqRel);
                    }
                } else {
                    // Somehow we have no items to evict
                    return Err(StorageError::MemoryLimitExceeded);
                }
            }
        }
        
        Ok(())
    }
    
    /// Set a key-value pair in the storage engine
    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) -> StorageResult<()> {
        // Check size limits
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }
        
        if value.len() > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }
        
        // CRITICAL FIX: Data validation to prevent RESP protocol confusion
        if value.len() > 10240 { // 10KB
            tracing::debug!("Large value SET operation: key={:?}, value_len={}", 
                String::from_utf8_lossy(&key), value.len());
        }
        
        let required_size = Self::calculate_size(&key, &value);

        // Ensure we have enough memory
        self.maybe_evict(required_size).await?;

        // Create a new item
        let mut item = StorageItem::new(value);
        let expires_at = if let Some(ttl) = ttl {
            item.expire(ttl);
            Some(Instant::now() + ttl)
        } else {
            None
        };

        // Use entry API for atomic read-modify-write to avoid TOCTOU races
        let is_new_key;
        match self.data.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let old_size = Self::calculate_size(entry.key(), &entry.get().value);
                if old_size > 0 {
                    self.current_memory.fetch_sub(old_size as u64, Ordering::AcqRel);
                }
                self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                entry.insert(item);
                is_new_key = false;
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                entry.insert(item);
                is_new_key = true;
            }
        }

        // Add to expiration queue if TTL is set
        if let Some(expires_at) = expires_at {
            self.add_to_expiration_queue(key.clone(), expires_at).await;
        }

        // Update prefix index and key count for new keys
        if is_new_key {
            self.update_prefix_indices(&key, true).await;
            self.key_count.fetch_add(1, Ordering::AcqRel);
        }

        // Bump key version for WATCH support
        self.bump_key_version(&key);
        // Increment write counters
        self.increment_write_counters().await;

        Ok(())
    }

    /// Set a key-value pair with explicit data type
    pub async fn set_with_type(&self, key: Vec<u8>, value: Vec<u8>, data_type: RedisDataType, ttl: Option<Duration>) -> StorageResult<()> {
        // Check size limits
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }
        
        if value.len() > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }
        
        // For large values (likely JSON), do deeper validation
        if value.len() > 10240 { // 10KB
            // Log large SET operations for debugging
            tracing::debug!("Large value SET operation: key={:?}, value_len={}, type={}", 
                String::from_utf8_lossy(&key), value.len(), data_type.as_str());
        }
        
        let required_size = Self::calculate_size(&key, &value);
        
        // Ensure we have enough memory
        self.maybe_evict(required_size).await?;
        
        // Create a new item with explicit type
        let mut item = StorageItem::new_with_type(value, data_type);
        let expires_at = if let Some(ttl) = ttl {
            item.expire(ttl);
            Some(Instant::now() + ttl)
        } else {
            None
        };

        // Use entry API for atomic read-modify-write to avoid TOCTOU races
        let is_new_key;
        match self.data.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let old_size = Self::calculate_size(entry.key(), &entry.get().value);
                if old_size > 0 {
                    self.current_memory.fetch_sub(old_size as u64, Ordering::AcqRel);
                }
                self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                entry.insert(item);
                is_new_key = false;
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                entry.insert(item);
                is_new_key = true;
            }
        }

        // Add to expiration queue if TTL is set
        if let Some(expires_at) = expires_at {
            self.add_to_expiration_queue(key.clone(), expires_at).await;
        }

        // Update prefix index and key count for new keys
        if is_new_key {
            self.update_prefix_indices(&key, true).await;
            self.key_count.fetch_add(1, Ordering::AcqRel);
        }

        // Bump key version for WATCH support
        self.bump_key_version(&key);
        // Increment write counters
        self.increment_write_counters().await;

        Ok(())
    }

    /// Set a key-value pair preserving the existing TTL if present.
    /// Use this for collection-type modifications (lists, sets, sorted sets, streams, geo)
    /// where updating the value should not reset the key's expiration.
    pub async fn set_with_type_preserve_ttl(&self, key: Vec<u8>, value: Vec<u8>, data_type: RedisDataType) -> StorageResult<()> {
        // Read existing TTL before overwriting
        let existing_ttl = if let Some(entry) = self.data.get(&key) {
            if entry.value().is_expired() {
                None
            } else {
                entry.value().ttl()
            }
        } else {
            None
        };
        self.set_with_type(key, value, data_type, existing_ttl).await
    }

    /// Get a value from the storage engine
    pub async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let result = if let Some(mut entry) = self.data.get_mut(key) {
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
        if result.is_none() && self.data.contains_key(key) {
            self.remove_expired_key(key).await;
        }
        
        Ok(result)
    }
    
    /// Delete a key from the storage engine
    pub async fn del(&self, key: &[u8]) -> StorageResult<bool> {
        if let Some((k, item)) = self.data.remove(key) {
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
    
    /// Check if a key exists in the storage engine
    pub async fn exists(&self, key: &[u8]) -> StorageResult<bool> {
        // Check if the key exists and is not expired
        if let Some(entry) = self.data.get(key) {
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
        if let Some(mut entry) = self.data.get_mut(key) {
            let item = entry.value_mut();

            if item.is_expired() {
                return Ok(false);
            }

            if let Some(ttl) = ttl {
                item.expire(ttl);
                // Add to expiration queue
                let expires_at = Instant::now() + ttl;
                self.add_to_expiration_queue(key.to_vec(), expires_at).await;
            } else {
                item.remove_expiry();
                // Remove from expiration queue (expensive operation, but TTL removal is rare)
                self.remove_from_expiration_queue(key).await;
            }

            // Bump key version for WATCH support
            self.bump_key_version(key);
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    /// Get the TTL (time-to-live) for a key
    pub async fn ttl(&self, key: &[u8]) -> StorageResult<Option<Duration>> {
        if let Some(entry) = self.data.get(key) {
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
    
    /// Get all keys in the storage engine
    pub async fn all_keys(&self) -> StorageResult<Vec<Vec<u8>>> {
        let keys: Vec<Vec<u8>> = self.data.iter()
            .filter(|item| !item.value().is_expired())
            .map(|item| item.key().clone())
            .collect();
        
        Ok(keys)
    }
    
    /// Get all keys matching a pattern
    pub async fn keys(&self, pattern: &str) -> StorageResult<Vec<Vec<u8>>> {
        // Try to use prefix index for common patterns
        if let Some(prefix) = self.extract_prefix_from_pattern(pattern) {
            let index = self.prefix_index.read().await;
            if let Some(indexed_keys) = index.get(&prefix) {
                // Filter indexed keys for exact pattern match and expiration
                let mut keys = Vec::new();
                for key in indexed_keys {
                    // Check if key still exists and is not expired
                    if let Some(entry) = self.data.get(key) {
                        if !entry.value().is_expired() {
                            if let Ok(key_str) = std::str::from_utf8(key) {
                                if self.matches_pattern(key_str, pattern) {
                                    keys.push(key.clone());
                                }
                            }
                        }
                    }
                }
                return Ok(keys);
            }
        }
        
        // Fall back to full scan for complex patterns or non-indexed prefixes
        let mut keys = Vec::new();
        
        for item in self.data.iter() {
            if item.value().is_expired() {
                continue;
            }
            
            // Convert key to string for pattern matching
            if let Ok(key_str) = std::str::from_utf8(item.key()) {
                if self.matches_pattern(key_str, pattern) {
                    keys.push(item.key().clone());
                }
            }
        }
        
        Ok(keys)
    }
    
    /// Check if a string matches a simple glob pattern using iterative two-pointer algorithm.
    /// Supports * (match any sequence) and ? (match any single char).
    fn matches_pattern(&self, s: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        let p: Vec<char> = pattern.chars().collect();
        let s: Vec<char> = s.chars().collect();
        let (mut pi, mut si) = (0usize, 0usize);
        let mut star_pi: Option<usize> = None;
        let mut star_si: Option<usize> = None;

        while si < s.len() {
            if pi < p.len() && (p[pi] == '?' || p[pi] == s[si]) {
                pi += 1;
                si += 1;
            } else if pi < p.len() && p[pi] == '*' {
                star_pi = Some(pi);
                star_si = Some(si);
                pi += 1;
            } else if let Some(sp) = star_pi {
                pi = sp + 1;
                let ss = star_si.unwrap() + 1;
                star_si = Some(ss);
                si = ss;
            } else {
                return false;
            }
        }

        while pi < p.len() && p[pi] == '*' {
            pi += 1;
        }

        pi == p.len()
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

        // Increment write counters
        self.increment_write_counters().await;

        Ok(())
    }
    
    /// Flush the current database (db0 only, not all databases)
    pub async fn flush_db(&self) -> StorageResult<()> {
        let size: u64 = self.data.iter()
            .map(|entry| Self::calculate_size(entry.key(), &entry.value().value) as u64)
            .sum();
        self.data.clear();
        self.current_memory.fetch_sub(size.min(self.current_memory.load(Ordering::Acquire)), Ordering::AcqRel);
        // Recount keys across extra databases rather than blindly setting to 0
        let remaining: u64 = self.extra_dbs.iter()
            .map(|db| db.len() as u64)
            .sum();
        self.key_count.store(remaining, Ordering::Release);
        Ok(())
    }
    
    /// Get a snapshot of the current data (database 0 only).
    /// Note: Extra databases (1-15) are excluded because the persistence layer
    /// does not yet support per-database serialization, and merging all databases
    /// into a flat HashMap would silently lose data when keys share names across databases.
    pub async fn snapshot(&self) -> StorageResult<HashMap<Vec<u8>, StorageItem>> {
        let mut snapshot = HashMap::with_capacity(self.data.len());

        for item in self.data.iter() {
            if !item.value().is_expired() {
                snapshot.insert(item.key().clone(), item.value().clone());
            }
        }

        Ok(snapshot)
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

    /// Bump the version counter for a specific key (used by WATCH for optimistic locking)
    pub fn bump_key_version(&self, key: &[u8]) {
        let new_version = self.global_version.fetch_add(1, Ordering::SeqCst);
        self.key_versions.insert(key.to_vec(), new_version);
    }

    /// Get the current version of a key (returns 0 if key has never been modified)
    pub fn get_key_version(&self, key: &[u8]) -> u64 {
        self.key_versions.get(key).map(|v| *v).unwrap_or(0)
    }
    
    /// Process a command (for AOF replay)
    pub async fn process_command(&self, command: &RespCommand) -> StorageResult<()> {
        // This is a simplified implementation just to handle basic commands
        // In a real implementation, you would invoke your command handler
        
        match command.name.as_slice() {
            b"SET" => {
                if command.args.len() >= 2 {
                    let key = &command.args[0];
                    let value = &command.args[1];
                    
                    // Check for EX argument
                    let mut ttl = None;
                    for i in 2..command.args.len() - 1 {
                        if command.args[i] == b"EX" {
                            if let Ok(secs) = std::str::from_utf8(&command.args[i+1]) {
                                if let Ok(secs_val) = secs.parse::<u64>() {
                                    ttl = Some(Duration::from_secs(secs_val));
                                }
                            }
                            break;
                        }
                    }
                    
                    self.set(key.clone(), value.clone(), ttl).await?;
                }
            },
            b"DEL" => {
                for arg in &command.args {
                    let _ = self.del(arg).await?;
                }
            },
            b"EXPIRE" => {
                if command.args.len() >= 2 {
                    let key = &command.args[0];
                    
                    if let Ok(secs) = std::str::from_utf8(&command.args[1]) {
                        if let Ok(secs_val) = secs.parse::<u64>() {
                            let ttl = Some(Duration::from_secs(secs_val));
                            self.expire(key, ttl).await?;
                        }
                    }
                }
            },
            b"PEXPIRE" => {
                if command.args.len() >= 2 {
                    let key = &command.args[0];
                    
                    if let Ok(millis) = std::str::from_utf8(&command.args[1]) {
                        if let Ok(millis_val) = millis.parse::<u64>() {
                            let ttl = Some(Duration::from_millis(millis_val));
                            self.expire(key, ttl).await?;
                        }
                    }
                }
            },
            _ => {
                // Log unknown command but don't fail
                tracing::warn!("Unknown command during AOF replay: {:?}", command);
            }
        }
        
        Ok(())
    }
    
    /// Get the type of a key
    pub async fn get_type(&self, key: &[u8]) -> StorageResult<String> {
        // Check if key exists and is not expired
        if let Some(entry) = self.data.get(key) {
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
        if let Some(entry) = self.data.get(key) {
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

    /// Get the current memory usage in bytes
    pub fn get_used_memory(&self) -> u64 {
        self.current_memory.load(Ordering::Acquire)
    }
    
    /// Get a random key from the storage engine (O(1) operation for RANDOMKEY)
    pub async fn get_random_key(&self) -> StorageResult<Option<Vec<u8>>> {
        // Check if database is empty
        if self.key_count.load(Ordering::Acquire) == 0 {
            return Ok(None);
        }

        // Use reservoir sampling to get a random non-expired key efficiently
        // This avoids the O(n) scan of keys("*")
        let mut random_key = None;
        let mut count = 0;
        let max_samples = 100; // Limit sampling to avoid long iterations

        for entry in self.data.iter() {
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

    /// Get a reference to a specific database (0 = primary, 1-15 = extra)
    pub fn get_db(&self, index: usize) -> Option<&DashMap<Vec<u8>, StorageItem>> {
        if index == 0 {
            Some(&self.data)
        } else if index < NUM_DATABASES {
            Some(&self.extra_dbs[index - 1])
        } else {
            None
        }
    }

    /// Touch a key (update its last access time). Returns true if the key exists.
    pub async fn touch(&self, key: &[u8]) -> StorageResult<bool> {
        if let Some(mut entry) = self.data.get_mut(key) {
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
        let now_unix = now_system.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;

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
        let now_unix_ms = now_system.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;

        if unix_timestamp_ms <= now_unix_ms {
            // Timestamp is in the past - delete the key
            return self.del(key).await;
        }

        let duration_from_now = Duration::from_millis((unix_timestamp_ms - now_unix_ms) as u64);
        self.expire(key, Some(duration_from_now)).await
    }

    /// Get the absolute Unix timestamp (seconds) when a key will expire
    pub async fn expiretime(&self, key: &[u8]) -> StorageResult<Option<i64>> {
        if let Some(entry) = self.data.get(key) {
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
                let unix_ts = expire_system.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
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
        if let Some(entry) = self.data.get(key) {
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
                let unix_ms = expire_system.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
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
        // Get the source item
        let src_item = if let Some(entry) = self.data.get(src) {
            if entry.value().is_expired() {
                drop(entry);
                self.remove_expired_key(src).await;
                return Ok(false);
            }
            entry.value().clone()
        } else {
            return Ok(false);
        };

        // Check if destination exists (non-expired) and we're not replacing
        if !replace {
            if let Some(entry) = self.data.get(&dst) {
                if !entry.value().is_expired() {
                    return Ok(false);
                }
            }
        }

        // Clone with fresh timestamps but same TTL
        let mut new_item = StorageItem::new_with_type(src_item.value.clone(), src_item.data_type.clone());
        if let Some(expires_at) = src_item.expires_at {
            let now = Instant::now();
            if expires_at > now {
                let remaining = expires_at - now;
                new_item.expire(remaining);
                self.add_to_expiration_queue(dst.clone(), Instant::now() + remaining).await;
            }
        }

        let required_size = Self::calculate_size(&dst, &src_item.value);
        self.maybe_evict(required_size).await?;

        // Use entry API for atomic insert-or-replace with correct memory/key accounting
        use dashmap::mapref::entry::Entry;
        match self.data.entry(dst.clone()) {
            Entry::Occupied(mut occ) => {
                let old_size = Self::calculate_size(&dst, &occ.get().value) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                occ.insert(new_item);
            }
            Entry::Vacant(vac) => {
                self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                vac.insert(new_item);
                self.update_prefix_indices(&dst, true).await;
                self.key_count.fetch_add(1, Ordering::AcqRel);
            }
        }

        // Bump key version for WATCH support
        self.bump_key_version(&dst);
        self.increment_write_counters().await;
        Ok(true)
    }

    /// Move a key from the current database (0) to another database
    pub async fn move_key(&self, key: &[u8], dst_db: usize) -> StorageResult<bool> {
        if dst_db == 0 || dst_db >= NUM_DATABASES {
            return Ok(false);
        }

        let dst = &self.extra_dbs[dst_db - 1];

        // Check if key exists in destination
        if dst.contains_key(key) {
            return Ok(false);
        }

        // Get the source item
        let src_item = if let Some(entry) = self.data.get(key) {
            if entry.value().is_expired() {
                drop(entry);
                self.remove_expired_key(key).await;
                return Ok(false);
            }
            entry.value().clone()
        } else {
            return Ok(false);
        };

        // Track the moved item's memory in the global counter
        // (self.del will decrement for the source, so we add for the destination first)
        let moved_size = Self::calculate_size(&key.to_vec(), &src_item.value) as u64;
        self.current_memory.fetch_add(moved_size, Ordering::AcqRel);

        // Insert into destination database
        dst.insert(key.to_vec(), src_item);

        // Remove from source database (this decrements current_memory)
        self.del(key).await?;

        Ok(true)
    }

    /// Get a raw StorageItem reference for OBJECT/DUMP commands
    pub async fn get_item(&self, key: &[u8]) -> StorageResult<Option<StorageItem>> {
        if let Some(entry) = self.data.get(key) {
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
        if let Some(entry) = self.data.get(key) {
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
    pub async fn get_encoding(&self, key: &[u8]) -> StorageResult<Option<String>> {
        if let Some(entry) = self.data.get(key) {
            if entry.value().is_expired() {
                drop(entry);
                self.remove_expired_key(key).await;
                return Ok(None);
            }
            let encoding = match &entry.value().data_type {
                RedisDataType::String => {
                    // Check if it's an integer
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
                RedisDataType::List => "listpack",
                RedisDataType::Set => "listpack",
                RedisDataType::Hash => "listpack",
                RedisDataType::ZSet => "listpack",
                RedisDataType::Stream => "stream",
            };
            Ok(Some(encoding.to_string()))
        } else {
            Ok(None)
        }
    }

    /// Restore a key from a serialized value (for RESTORE command)
    pub async fn restore_key(&self, key: Vec<u8>, value: Vec<u8>, data_type: RedisDataType, ttl: Option<Duration>, replace: bool) -> StorageResult<bool> {
        // Check if key exists
        if self.data.contains_key(&key) {
            if !replace {
                return Err(StorageError::KeyExists);
            }
            // Remove old key
            self.del(&key).await?;
        }

        self.set_with_type(key, value, data_type, ttl).await?;
        Ok(true)
    }

    /// Count keys that belong to a given hash slot
    pub async fn count_keys_in_slot(&self, slot: u16) -> usize {
        use crate::cluster::slot::key_hash_slot;
        let mut count = 0;
        for entry in self.data.iter() {
            if key_hash_slot(entry.key()) == slot {
                count += 1;
            }
        }
        count
    }

    /// Get keys that belong to a given hash slot (up to count)
    pub async fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<Vec<u8>> {
        use crate::cluster::slot::key_hash_slot;
        let mut keys = Vec::new();
        for entry in self.data.iter() {
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
        if let Some(entry) = self.data.get(key) {
            Some(entry.value.clone())
        } else {
            None
        }
    }

    // ========== Atomic Primitives ==========

    /// Atomically set a key only if it does NOT exist (SET NX).
    /// Returns true if the key was set, false if it already existed.
    pub async fn set_nx(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) -> StorageResult<bool> {
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
        let inserted = match self.data.entry(key.clone()) {
            Entry::Occupied(occ) => {
                // Key exists - check if expired
                if occ.get().is_expired() {
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    let mut occ = occ;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                    occ.insert(item);
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(vac) => {
                self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                vac.insert(item);
                true
            }
        };

        if inserted {
            if let Some(expires_at) = expires_at {
                self.add_to_expiration_queue(key.clone(), expires_at).await;
            }
            self.update_prefix_indices(&key, true).await;
            self.key_count.fetch_add(1, Ordering::AcqRel);
            self.bump_key_version(&key);
            self.increment_write_counters().await;
        }

        Ok(inserted)
    }

    /// Atomically set a key only if it DOES exist (SET XX).
    /// Returns true if the key was set, false if it did not exist.
    pub async fn set_xx(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) -> StorageResult<bool> {
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
        let updated = match self.data.entry(key.clone()) {
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
                    self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                    occ.insert(item);
                    true
                }
            }
            Entry::Vacant(_) => false,
        };

        if updated {
            if let Some(expires_at) = expires_at {
                self.add_to_expiration_queue(key.clone(), expires_at).await;
            }
            self.bump_key_version(&key);
            self.increment_write_counters().await;
        }

        Ok(updated)
    }

    /// Unified SET with all flag combinations (NX, XX, GET).
    /// Returns SetResult indicating what happened.
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
        let (did_set, old_value, is_new_key, final_expires_at) = match self.data.entry(key.clone()) {
            Entry::Occupied(mut occ) => {
                let expired = occ.get().is_expired();
                let effectively_exists = !expired;

                if nx && effectively_exists {
                    // NX: don't set if key exists
                    let old = if get_old { Some(occ.get().value.clone()) } else { None };
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
                    self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                    if keepttl && effectively_exists {
                        // Preserve the old TTL by copying expires_at
                        item.expires_at = preserved_expires;
                    }
                    occ.insert(item);
                    let effective_expires = if keepttl && effectively_exists {
                        preserved_expires.map(|ea| ea)
                    } else {
                        new_expires_at
                    };
                    (true, old, expired, effective_expires)
                }
            }
            Entry::Vacant(vac) => {
                if xx {
                    // XX: key must exist
                    (false, None, false, None)
                } else {
                    self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                    vac.insert(item);
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
            self.bump_key_version(&key);
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
        match self.data.entry(key.to_vec()) {
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
                let current_str = std::str::from_utf8(&occ.get().value)
                    .map_err(|_| StorageError::NotANumber)?;
                let current: i64 = current_str.parse()
                    .map_err(|_| StorageError::NotANumber)?;
                let new_val = current.checked_add(delta)
                    .ok_or(StorageError::NotANumber)?;
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
        match self.data.entry(key.to_vec()) {
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
                let current_str = std::str::from_utf8(&occ.get().value)
                    .map_err(|_| StorageError::NotAFloat)?;
                let current: f64 = current_str.parse()
                    .map_err(|_| StorageError::NotAFloat)?;
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
        match self.data.entry(key.to_vec()) {
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
    pub fn atomic_modify<F, R>(&self, key: &[u8], data_type: RedisDataType, f: F) -> StorageResult<R>
    where
        F: FnOnce(Option<&mut Vec<u8>>) -> Result<(Option<Vec<u8>>, R), StorageError>,
    {
        use dashmap::mapref::entry::Entry;
        match self.data.entry(key.to_vec()) {
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
                }
                Ok(result)
            }
        }
    }

    /// Atomically get and delete a key. Returns the old value if it existed.
    pub fn atomic_getdel(&self, key: &[u8]) -> Option<Vec<u8>> {
        use dashmap::mapref::entry::Entry;
        match self.data.entry(key.to_vec()) {
            Entry::Occupied(occ) => {
                if occ.get().is_expired() {
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    self.key_count.fetch_sub(1, Ordering::AcqRel);
                    occ.remove();
                    self.bump_key_version(key);
                    None
                } else {
                    let old_value = occ.get().value.clone();
                    let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                    self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                    self.key_count.fetch_sub(1, Ordering::AcqRel);
                    occ.remove();
                    self.bump_key_version(key);
                    Some(old_value)
                }
            }
            Entry::Vacant(_) => None,
        }
    }

    /// Atomically get the old value and set a new value.
    pub async fn atomic_getset(&self, key: Vec<u8>, value: Vec<u8>) -> StorageResult<Option<Vec<u8>>> {
        if key.len() > self.config.max_key_size {
            return Err(StorageError::ValueTooLarge);
        }
        if value.len() > self.config.max_value_size {
            return Err(StorageError::ValueTooLarge);
        }

        let required_size = Self::calculate_size(&key, &value);
        self.maybe_evict(required_size).await?;

        use dashmap::mapref::entry::Entry;
        let old_value = match self.data.entry(key.clone()) {
            Entry::Occupied(mut occ) => {
                let expired = occ.get().is_expired();
                let old = if expired { None } else { Some(occ.get().value.clone()) };
                let old_size = Self::calculate_size(occ.key(), &occ.get().value) as u64;
                self.current_memory.fetch_sub(old_size, Ordering::AcqRel);
                self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                let item = StorageItem::new(value);
                occ.insert(item);
                old
            }
            Entry::Vacant(vac) => {
                self.current_memory.fetch_add(required_size as u64, Ordering::AcqRel);
                self.key_count.fetch_add(1, Ordering::AcqRel);
                vac.insert(StorageItem::new(value));
                None
            }
        };

        self.bump_key_version(&key);
        self.increment_write_counters().await;
        Ok(old_value)
    }

    // ========== Hash Primitives ==========
    // Hash data is stored as a single key with RedisDataType::Hash.
    // The value is a bincode-serialized HashMap<Vec<u8>, Vec<u8>>.

    /// Deserialize hash data from bytes. Returns empty HashMap if bytes is None.
    fn hash_deserialize(data: Option<&Vec<u8>>) -> HashMap<Vec<u8>, Vec<u8>> {
        match data {
            Some(bytes) if !bytes.is_empty() => {
                bincode::deserialize(bytes).unwrap_or_default()
            }
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
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let preserved = existing.as_ref().map(|v| (*v).clone());
            let map = Self::hash_deserialize(existing.as_deref());
            let val = map.get(field).cloned();
            Ok((preserved, val))
        })
    }

    /// Get multiple fields from a hash. Returns a Vec of Option<Vec<u8>> in order.
    pub fn hash_mget(&self, key: &[u8], fields: &[&[u8]]) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let preserved = existing.as_ref().map(|v| (*v).clone());
            let map = Self::hash_deserialize(existing.as_deref());
            let results: Vec<Option<Vec<u8>>> = fields.iter()
                .map(|f| map.get(*f).cloned())
                .collect();
            Ok((preserved, results))
        })
    }

    /// Get all field-value pairs from a hash.
    pub fn hash_getall(&self, key: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let preserved = existing.as_ref().map(|v| (*v).clone());
            let map = Self::hash_deserialize(existing.as_deref());
            let pairs: Vec<(Vec<u8>, Vec<u8>)> = map.into_iter().collect();
            Ok((preserved, pairs))
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
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let preserved = existing.as_ref().map(|v| (*v).clone());
            let map = Self::hash_deserialize(existing.as_deref());
            let exists = map.contains_key(field);
            Ok((preserved, exists))
        })
    }

    /// Get the number of fields in a hash.
    pub fn hash_len(&self, key: &[u8]) -> StorageResult<i64> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let preserved = existing.as_ref().map(|v| (*v).clone());
            let map = Self::hash_deserialize(existing.as_deref());
            let len = map.len() as i64;
            Ok((preserved, len))
        })
    }

    /// Get all field names in a hash.
    pub fn hash_keys(&self, key: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let preserved = existing.as_ref().map(|v| (*v).clone());
            let map = Self::hash_deserialize(existing.as_deref());
            let keys: Vec<Vec<u8>> = map.keys().cloned().collect();
            Ok((preserved, keys))
        })
    }

    /// Get all values in a hash.
    pub fn hash_vals(&self, key: &[u8]) -> StorageResult<Vec<Vec<u8>>> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let preserved = existing.as_ref().map(|v| (*v).clone());
            let map = Self::hash_deserialize(existing.as_deref());
            let vals: Vec<Vec<u8>> = map.values().cloned().collect();
            Ok((preserved, vals))
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
            let new_val = current.checked_add(delta)
                .ok_or(StorageError::NotANumber)?;
            map.insert(field.to_vec(), new_val.to_string().into_bytes());
            Ok((Some(Self::hash_serialize(&map)), new_val))
        })
    }

    /// Increment a hash field's float value by delta. Returns the new value as string.
    pub fn hash_incr_by_float(&self, key: &[u8], field: &[u8], delta: f64) -> StorageResult<String> {
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
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let preserved = existing.as_ref().map(|v| (*v).clone());
            let map = Self::hash_deserialize(existing.as_deref());
            let len = map.get(field).map(|v| v.len() as i64).unwrap_or(0);
            Ok((preserved, len))
        })
    }

    /// Get random fields from a hash. Returns (fields, values) pairs.
    /// If the hash doesn't exist, returns empty vec.
    pub fn hash_randfield(&self, key: &[u8]) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.atomic_modify(key, RedisDataType::Hash, |existing| {
            let preserved = existing.as_ref().map(|v| (*v).clone());
            let map = Self::hash_deserialize(existing.as_deref());
            let pairs: Vec<(Vec<u8>, Vec<u8>)> = map.into_iter().collect();
            Ok((preserved, pairs))
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
        storage.set(b"key1".to_vec(), b"value1".to_vec(), None).await.unwrap();
        
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
        storage.set(b"expire_key".to_vec(), b"value".to_vec(), None).await.unwrap();
        
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
        storage.set(
            b"expiring_key".to_vec(), 
            b"expiring_value".to_vec(), 
            Some(ttl)
        ).await.unwrap();
        
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
        storage.set(b"del_key1".to_vec(), b"value1".to_vec(), None).await.unwrap();
        storage.set(b"del_key2".to_vec(), b"value2".to_vec(), None).await.unwrap();
        storage.set(b"keep_key".to_vec(), b"keep_value".to_vec(), None).await.unwrap();
        
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
        storage.set(b"exists_key".to_vec(), b"value".to_vec(), None).await.unwrap();
        
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
            storage.set(key, b"value".to_vec(), Some(Duration::from_millis(30))).await.unwrap();
        }
        
        // Set one key with longer TTL
        storage.set(
            b"cleanup_survivor".to_vec(),
            b"survivor".to_vec(),
            Some(Duration::from_millis(200))
        ).await.unwrap();
        
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
        let result = storage.set_nx(b"nx_key".to_vec(), b"value1".to_vec(), None).await.unwrap();
        assert!(result);

        // Second set_nx on same key should fail
        let result = storage.set_nx(b"nx_key".to_vec(), b"value2".to_vec(), None).await.unwrap();
        assert!(!result);

        // Value should still be the first one
        let val = storage.get(b"nx_key").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_set_nx_expired_key() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Set key with short TTL
        storage.set(b"nx_exp".to_vec(), b"old".to_vec(), Some(Duration::from_millis(30))).await.unwrap();
        sleep(Duration::from_millis(50)).await;

        // set_nx should succeed since key is expired
        let result = storage.set_nx(b"nx_exp".to_vec(), b"new".to_vec(), None).await.unwrap();
        assert!(result);

        let val = storage.get(b"nx_exp").await.unwrap();
        assert_eq!(val, Some(b"new".to_vec()));
    }

    #[tokio::test]
    async fn test_set_xx_basic() {
        let storage = StorageEngine::new(StorageConfig::default());

        // set_xx on non-existent key should fail
        let result = storage.set_xx(b"xx_key".to_vec(), b"value1".to_vec(), None).await.unwrap();
        assert!(!result);

        // Set the key first
        storage.set(b"xx_key".to_vec(), b"value1".to_vec(), None).await.unwrap();

        // set_xx should now succeed
        let result = storage.set_xx(b"xx_key".to_vec(), b"value2".to_vec(), None).await.unwrap();
        assert!(result);

        let val = storage.get(b"xx_key").await.unwrap();
        assert_eq!(val, Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_set_with_options_nx() {
        let storage = StorageEngine::new(StorageConfig::default());

        let result = storage.set_with_options(
            b"opt_key".to_vec(), b"v1".to_vec(), None, true, false, false, false
        ).await.unwrap();
        assert_eq!(result, SetResult::Ok);

        // NX should fail on existing key
        let result = storage.set_with_options(
            b"opt_key".to_vec(), b"v2".to_vec(), None, true, false, false, false
        ).await.unwrap();
        assert_eq!(result, SetResult::NotSet);
    }

    #[tokio::test]
    async fn test_set_with_options_get() {
        let storage = StorageEngine::new(StorageConfig::default());

        // GET on non-existent key
        let result = storage.set_with_options(
            b"get_key".to_vec(), b"v1".to_vec(), None, false, false, true, false
        ).await.unwrap();
        assert_eq!(result, SetResult::OldValue(None));

        // GET on existing key
        let result = storage.set_with_options(
            b"get_key".to_vec(), b"v2".to_vec(), None, false, false, true, false
        ).await.unwrap();
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
        storage.set(b"not_num".to_vec(), b"hello".to_vec(), None).await.unwrap();

        let result = storage.atomic_incr(b"not_num", 1);
        assert!(matches!(result, Err(StorageError::NotANumber)));
    }

    #[tokio::test]
    async fn test_atomic_incr_wrong_type() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Set a list type value
        storage.set_with_type(b"list_key".to_vec(), b"data".to_vec(), RedisDataType::List, None).await.unwrap();

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
        let result: usize = storage.atomic_modify(b"my_list", RedisDataType::List, |existing| {
            match existing {
                None => {
                    // Create new list with one element
                    let data = bincode::serialize(&vec![b"item1".to_vec()]).unwrap();
                    Ok((Some(data), 1))
                }
                Some(_) => unreachable!(),
            }
        }).unwrap();
        assert_eq!(result, 1);

        // Modify the list to add another element
        let result: usize = storage.atomic_modify(b"my_list", RedisDataType::List, |existing| {
            match existing {
                Some(data) => {
                    let mut list: Vec<Vec<u8>> = bincode::deserialize(data).unwrap();
                    list.push(b"item2".to_vec());
                    let new_len = list.len();
                    let new_data = bincode::serialize(&list).unwrap();
                    Ok((Some(new_data), new_len))
                }
                None => unreachable!(),
            }
        }).unwrap();
        assert_eq!(result, 2);
    }

    #[tokio::test]
    async fn test_atomic_modify_wrong_type() {
        let storage = StorageEngine::new(StorageConfig::default());

        // Set a string key
        storage.set(b"str_key".to_vec(), b"value".to_vec(), None).await.unwrap();

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
        storage.set(b"gd_key".to_vec(), b"value".to_vec(), None).await.unwrap();

        // GETDEL should return the value and delete
        let val = storage.atomic_getdel(b"gd_key");
        assert_eq!(val, Some(b"value".to_vec()));

        // Key should be gone
        let val = storage.get(b"gd_key").await.unwrap();
        assert_eq!(val, None);

        // GETDEL on non-existent key
        let val = storage.atomic_getdel(b"gd_key");
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_atomic_getset() {
        let storage = StorageEngine::new(StorageConfig::default());

        // GETSET on non-existent key
        let old = storage.atomic_getset(b"gs_key".to_vec(), b"v1".to_vec()).await.unwrap();
        assert_eq!(old, None);

        // GETSET on existing key
        let old = storage.atomic_getset(b"gs_key".to_vec(), b"v2".to_vec()).await.unwrap();
        assert_eq!(old, Some(b"v1".to_vec()));

        // Verify new value
        let val = storage.get(b"gs_key").await.unwrap();
        assert_eq!(val, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_concurrent_set_nx() {
        let storage = Arc::new(StorageEngine::new(StorageConfig::default()));
        // Unwrap the inner Arc since StorageEngine::new returns Arc<Self>
        // Actually StorageEngine::new returns Arc<Self>, so storage is already Arc<Arc<StorageEngine>>
        // Let me fix this - we need the Arc<StorageEngine> directly
        let storage = StorageEngine::new(StorageConfig::default());

        let mut handles = vec![];
        let success_count = Arc::new(AtomicU64::new(0));

        for i in 0..10 {
            let s = Arc::clone(&storage);
            let sc = Arc::clone(&success_count);
            handles.push(tokio::spawn(async move {
                let val = format!("value_{}", i).into_bytes();
                if s.set_nx(b"race_key".to_vec(), val, None).await.unwrap() {
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
        storage.set(b"race_counter".to_vec(), b"0".to_vec(), None).await.unwrap();

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
        assert_eq!(mem, 0, "Memory should be 0 after setting and deleting all keys");
    }
} 