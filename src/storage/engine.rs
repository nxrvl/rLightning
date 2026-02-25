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
                        self.current_memory.fetch_sub(size, Ordering::Relaxed);
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
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
        
        // Sample random keys to check for expiration
        for (sampled_count, entry) in self.data.iter().enumerate() {
            if sampled_count >= SAMPLE_SIZE {
                break;
            }
            
            if entry.value().is_expired() {
                keys_to_remove.push(entry.key().clone());
            }
        }
        
        // Remove expired keys found in this sample
        for key in keys_to_remove {
            if let Some((k, item)) = self.data.remove(&key) {
                let size = Self::calculate_size(&k, &item.value) as u64;
                self.current_memory.fetch_sub(size, Ordering::Relaxed);
                self.key_count.fetch_sub(1, Ordering::Relaxed);
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
            self.current_memory.fetch_sub(size, Ordering::Relaxed);
            self.key_count.fetch_sub(1, Ordering::Relaxed);
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
        let current_memory = self.current_memory.load(Ordering::Relaxed) as usize;
        if current_memory + required_size > self.config.max_memory {
            // If we're not allowed to evict, fail
            if self.config.eviction_policy == EvictionPolicy::NoEviction {
                return Err(StorageError::MemoryLimitExceeded);
            }
            
            // Evict items until we have enough space
            while self.current_memory.load(Ordering::Relaxed) as usize + required_size > self.config.max_memory {
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
                        // Pick a random key - this could be improved with better randomization
                        self.data.iter().next().map(|item| item.key().clone())
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
                        self.current_memory.fetch_sub(size, Ordering::Relaxed);
                        // Decrement key count for evicted keys
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
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
        
        // Validate that data doesn't start with RESP command indicators
        // This prevents stored data from being misinterpreted as commands
        if !value.is_empty() {
            let first_byte = value[0];
            let looks_like_resp_command = matches!(first_byte, b'+' | b'-' | b':' | b'$' | b'*');
            
            if looks_like_resp_command {
                // If data starts with RESP indicators, it needs special handling
                tracing::warn!("SET data starts with RESP command byte: {:02x} ({})", 
                    first_byte, first_byte as char);
                
                // For safety, we could prefix such data, but for now just log it
                // This helps identify problematic data patterns
                let preview = if value.len() > 50 { 
                    String::from_utf8_lossy(&value[0..50]) 
                } else { 
                    String::from_utf8_lossy(&value) 
                };
                tracing::debug!("RESP-like data preview: {}", preview);
            }
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
                    self.current_memory.fetch_sub(old_size as u64, Ordering::Relaxed);
                }
                self.current_memory.fetch_add(required_size as u64, Ordering::Relaxed);
                entry.insert(item);
                is_new_key = false;
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                self.current_memory.fetch_add(required_size as u64, Ordering::Relaxed);
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
            self.key_count.fetch_add(1, Ordering::Relaxed);
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
                    self.current_memory.fetch_sub(old_size as u64, Ordering::Relaxed);
                }
                self.current_memory.fetch_add(required_size as u64, Ordering::Relaxed);
                entry.insert(item);
                is_new_key = false;
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                self.current_memory.fetch_add(required_size as u64, Ordering::Relaxed);
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
            self.key_count.fetch_add(1, Ordering::Relaxed);
        }

        // Bump key version for WATCH support
        self.bump_key_version(&key);
        // Increment write counters
        self.increment_write_counters().await;

        Ok(())
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
        self.current_memory.store(0, Ordering::Relaxed);
        self.key_count.store(0, Ordering::Relaxed);

        // Clear prefix index
        let mut prefix_index = self.prefix_index.write().await;
        prefix_index.clear();

        // Increment write counters
        self.increment_write_counters().await;

        Ok(())
    }
    
    /// Flush the current database
    pub async fn flush_db(&self) -> StorageResult<()> {
        self.flush_all().await
    }
    
    /// Get a snapshot of the current data (all databases)
    pub async fn snapshot(&self) -> StorageResult<HashMap<Vec<u8>, StorageItem>> {
        let mut snapshot = HashMap::with_capacity(self.data.len());

        // Snapshot database 0
        for item in self.data.iter() {
            if !item.value().is_expired() {
                snapshot.insert(item.key().clone(), item.value().clone());
            }
        }

        // Snapshot extra databases (1-15)
        for db in &self.extra_dbs {
            for item in db.iter() {
                if !item.value().is_expired() {
                    snapshot.insert(item.key().clone(), item.value().clone());
                }
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
        self.key_count.load(Ordering::Relaxed)
    }

    /// Get the current memory usage in bytes
    pub fn get_used_memory(&self) -> u64 {
        self.current_memory.load(Ordering::Relaxed)
    }
    
    /// Get a random key from the storage engine (O(1) operation for RANDOMKEY)
    pub async fn get_random_key(&self) -> StorageResult<Option<Vec<u8>>> {
        // Check if database is empty
        if self.key_count.load(Ordering::Relaxed) == 0 {
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

        // Check if destination exists
        if !replace && self.data.contains_key(&dst) {
            if let Some(entry) = self.data.get(&dst) {
                if !entry.value().is_expired() {
                    return Ok(false);
                }
            }
        }

        // Store the copy
        let is_new_key = !self.data.contains_key(&dst);
        let old_size = if let Some(entry) = self.data.get(&dst) {
            Self::calculate_size(&dst, &entry.value)
        } else {
            0
        };

        let required_size = Self::calculate_size(&dst, &src_item.value);
        self.maybe_evict(required_size).await?;

        if old_size > 0 {
            self.current_memory.fetch_sub(old_size as u64, Ordering::Relaxed);
        }
        self.current_memory.fetch_add(required_size as u64, Ordering::Relaxed);

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

        self.data.insert(dst.clone(), new_item);

        if is_new_key {
            self.update_prefix_indices(&dst, true).await;
            self.key_count.fetch_add(1, Ordering::Relaxed);
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

        // Insert into destination database
        dst.insert(key.to_vec(), src_item);

        // Remove from source database
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
} 