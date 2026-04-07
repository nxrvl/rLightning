//! Sharded storage engine using parking_lot::RwLock per shard.
//! Replaces DashMap for better performance and control over locking granularity.
//!
//! Each shard contains its own expiration min-heap, eliminating the global
//! expiration queue lock contention from the previous design.

use arrayvec::ArrayVec;
use hashbrown::HashMap as HBHashMap;
use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use rustc_hash::FxBuildHasher;
use std::collections::BinaryHeap;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use crate::storage::item::Entry;

/// The underlying HashMap type used within each shard.
pub type ShardMap = HBHashMap<Vec<u8>, Entry, FxBuildHasher>;

/// Entry in a per-shard expiration min-heap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardExpirationEntry {
    pub expires_at: Instant,
    pub key: Vec<u8>,
}

impl PartialOrd for ShardExpirationEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ShardExpirationEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering so earliest expiration comes first (min-heap)
        other.expires_at.cmp(&self.expires_at)
    }
}

/// Maximum number of eviction candidates sampled per shard.
pub const EVICTION_SAMPLE_SIZE: usize = 16;

/// A candidate for eviction, holding enough info to pick a victim without
/// re-acquiring the shard lock.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct EvictionCandidate {
    pub key: Vec<u8>,
    pub lru_clock: u32,
    pub access_count: u16,
    pub has_expiry: bool,
    pub mem_size: usize,
}

/// A single shard with cache-line alignment to prevent false sharing.
#[repr(align(128))]
struct Shard {
    /// Data storage with parking_lot RwLock for fast synchronous locking.
    inner: RwLock<ShardMap>,
    /// Per-shard expiration min-heap. Uses a separate parking_lot Mutex
    /// so expiration processing doesn't hold the data RwLock.
    expiration_heap: Mutex<BinaryHeap<ShardExpirationEntry>>,
    /// Transaction-level lock for MULTI/EXEC isolation (async-compatible).
    tx_lock: Arc<tokio::sync::Mutex<()>>,
    /// Per-shard memory usage counter (bytes).
    used_memory: AtomicU64,
    /// Per-shard key count.
    key_count: AtomicU32,
}

/// High-performance sharded concurrent hash map.
/// Uses parking_lot::RwLock per shard with FxHash for shard selection.
pub struct ShardedStore {
    shards: Box<[Shard]>,
    shard_mask: usize,
}

// --- Entry API types ---

/// Occupied entry in a shard (key exists). Holds the shard write lock.
pub struct OccupiedShardEntry<'a> {
    guard: RwLockWriteGuard<'a, ShardMap>,
    key: Vec<u8>,
}

/// Vacant entry in a shard (key does not exist). Holds the shard write lock.
pub struct VacantShardEntry<'a> {
    guard: RwLockWriteGuard<'a, ShardMap>,
    key: Vec<u8>,
}

/// Entry in a shard, either occupied or vacant.
pub enum ShardEntry<'a> {
    Occupied(OccupiedShardEntry<'a>),
    Vacant(VacantShardEntry<'a>),
}

impl<'a> OccupiedShardEntry<'a> {
    /// Get a reference to the key.
    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    /// Get a reference to the value.
    pub fn get(&self) -> &Entry {
        self.guard.get(&self.key).unwrap()
    }

    /// Get a mutable reference to the value.
    pub fn get_mut(&mut self) -> &mut Entry {
        self.guard.get_mut(&self.key).unwrap()
    }

    /// Replace the value, returning the old value.
    pub fn insert(&mut self, value: Entry) -> Entry {
        self.guard.insert(self.key.clone(), value).unwrap()
    }

    /// Remove the entry and return the value.
    pub fn remove(mut self) -> Entry {
        self.guard.remove(&self.key).unwrap()
    }

    /// Remove the entry and return both key and value.
    pub fn remove_entry(mut self) -> (Vec<u8>, Entry) {
        let value = self.guard.remove(&self.key).unwrap();
        (self.key, value)
    }
}

impl<'a> VacantShardEntry<'a> {
    /// Get a reference to the key.
    #[allow(dead_code)]
    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    /// Insert a value, consuming the vacant entry.
    pub fn insert(mut self, value: Entry) {
        self.guard.insert(self.key, value);
    }
}

impl Default for ShardedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedStore {
    /// Create a new ShardedStore with shard count based on CPU count.
    /// Shard count = (num_cpus * 16).next_power_of_two().clamp(16, 1024)
    #[allow(dead_code)]
    pub fn new() -> Self {
        let shard_count = (num_cpus::get() * 16).next_power_of_two().clamp(16, 1024);
        Self::with_shard_count(shard_count)
    }

    /// Create a new ShardedStore with a specific initial capacity per shard.
    pub fn with_capacity(capacity: usize) -> Self {
        let shard_count = (num_cpus::get() * 16).next_power_of_two().clamp(16, 1024);
        Self::with_shard_count_and_capacity(shard_count, capacity)
    }

    pub(crate) fn with_shard_count(shard_count: usize) -> Self {
        Self::with_shard_count_and_capacity(shard_count, 0)
    }

    fn with_shard_count_and_capacity(shard_count: usize, capacity: usize) -> Self {
        assert!(shard_count.is_power_of_two());
        let per_shard_capacity = if capacity > 0 {
            capacity / shard_count + 1
        } else {
            0
        };

        let shards: Vec<Shard> = (0..shard_count)
            .map(|_| Shard {
                inner: RwLock::new(if per_shard_capacity > 0 {
                    ShardMap::with_capacity_and_hasher(per_shard_capacity, FxBuildHasher)
                } else {
                    ShardMap::with_hasher(FxBuildHasher)
                }),
                expiration_heap: Mutex::new(BinaryHeap::new()),
                tx_lock: Arc::new(tokio::sync::Mutex::new(())),
                used_memory: AtomicU64::new(0),
                key_count: AtomicU32::new(0),
            })
            .collect();

        ShardedStore {
            shards: shards.into_boxed_slice(),
            shard_mask: shard_count - 1,
        }
    }

    /// Compute the shard index for a key using FxHash.
    #[inline]
    pub fn shard_index(&self, key: &[u8]) -> usize {
        let mut hasher = FxBuildHasher.build_hasher();
        hasher.write(key);
        hasher.finish() as usize & self.shard_mask
    }

    /// Get the number of shards.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Get a read reference to an entry by key.
    /// Returns a mapped read guard that derefs to &Entry.
    pub fn get(&self, key: &[u8]) -> Option<MappedRwLockReadGuard<'_, Entry>> {
        let shard_idx = self.shard_index(key);
        let guard = self.shards[shard_idx].inner.read();
        RwLockReadGuard::try_map(guard, |shard| shard.get(key)).ok()
    }

    /// Get a write reference to an entry by key.
    /// Returns a mapped write guard that derefs to &mut Entry.
    pub fn get_mut(&self, key: &[u8]) -> Option<MappedRwLockWriteGuard<'_, Entry>> {
        let shard_idx = self.shard_index(key);
        let guard = self.shards[shard_idx].inner.write();
        RwLockWriteGuard::try_map(guard, |shard| shard.get_mut(key)).ok()
    }

    /// Get an entry for a key (occupied or vacant), holding the shard write lock.
    pub fn entry(&self, key: Vec<u8>) -> ShardEntry<'_> {
        let shard_idx = self.shard_index(&key);
        let guard = self.shards[shard_idx].inner.write();
        let exists = guard.contains_key(&*key);
        if exists {
            ShardEntry::Occupied(OccupiedShardEntry { guard, key })
        } else {
            ShardEntry::Vacant(VacantShardEntry { guard, key })
        }
    }

    /// Insert a key-value pair. Returns the old value if the key existed.
    pub fn insert(&self, key: Vec<u8>, value: Entry) -> Option<Entry> {
        let shard_idx = self.shard_index(&key);
        let mut guard = self.shards[shard_idx].inner.write();
        guard.insert(key, value)
    }

    /// Remove a key. Returns (key, value) if the key existed.
    pub fn remove(&self, key: &[u8]) -> Option<(Vec<u8>, Entry)> {
        let shard_idx = self.shard_index(key);
        let mut guard = self.shards[shard_idx].inner.write();
        guard.remove_entry(key)
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let shard_idx = self.shard_index(key);
        let guard = self.shards[shard_idx].inner.read();
        guard.contains_key(key)
    }

    /// Get the total number of entries across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.inner.read().len()).sum()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.inner.read().is_empty())
    }

    /// Remove all entries from all shards.
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            let mut guard = shard.inner.write();
            guard.clear();
            shard.expiration_heap.lock().clear();
            shard.used_memory.store(0, Ordering::Relaxed);
            shard.key_count.store(0, Ordering::Relaxed);
        }
    }

    /// Iterate over all entries, calling the closure for each (key, value) pair.
    /// Acquires a read lock on each shard sequentially.
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&Vec<u8>, &Entry),
    {
        for shard in self.shards.iter() {
            let guard = shard.inner.read();
            for (key, entry) in guard.iter() {
                f(key, entry);
            }
        }
    }

    /// Iterate over all entries mutably.
    pub fn for_each_mut<F>(&self, mut f: F)
    where
        F: FnMut(&Vec<u8>, &mut Entry),
    {
        for shard in self.shards.iter() {
            let mut guard = shard.inner.write();
            for (key, entry) in guard.iter_mut() {
                f(key, entry);
            }
        }
    }

    /// Iterate and collect results. Returns items where f returns Some.
    pub fn filter_map<F, R>(&self, mut f: F) -> Vec<R>
    where
        F: FnMut(&Vec<u8>, &Entry) -> Option<R>,
    {
        let mut results = Vec::new();
        for shard in self.shards.iter() {
            let guard = shard.inner.read();
            for (key, entry) in guard.iter() {
                if let Some(r) = f(key, entry) {
                    results.push(r);
                }
            }
        }
        results
    }

    /// Iterate starting from a random offset, collecting up to `count` items.
    /// Used for probabilistic sampling (expiration, eviction).
    pub fn sample<F, R>(&self, count: usize, mut f: F) -> Vec<R>
    where
        F: FnMut(&Vec<u8>, &Entry) -> Option<R>,
    {
        if count == 0 {
            return Vec::new();
        }
        let num_shards = self.shards.len();
        let start_shard = fastrand::usize(0..num_shards);
        let mut results = Vec::with_capacity(count);

        for i in 0..num_shards {
            let shard_idx = (start_shard + i) % num_shards;
            let guard = self.shards[shard_idx].inner.read();
            for (key, entry) in guard.iter() {
                if results.len() >= count {
                    return results;
                }
                if let Some(r) = f(key, entry) {
                    results.push(r);
                }
            }
            if results.len() >= count {
                break;
            }
        }
        results
    }

    /// Conditionally remove an entry. Removes if the predicate returns true.
    pub fn remove_if<F>(&self, key: &[u8], f: F) -> bool
    where
        F: FnOnce(&Vec<u8>, &Entry) -> bool,
    {
        let shard_idx = self.shard_index(key);
        let mut guard = self.shards[shard_idx].inner.write();
        if let Some((k, v)) = guard.get_key_value(key)
            && f(k, v)
        {
            guard.remove(key);
            return true;
        }
        false
    }

    // --- Per-shard memory tracking (test helpers) ---

    /// Add to the memory counter for the shard containing `key`.
    #[cfg(test)]
    pub fn add_memory(&self, key: &[u8], amount: u64) {
        let shard_idx = self.shard_index(key);
        self.shards[shard_idx]
            .used_memory
            .fetch_add(amount, Ordering::Relaxed);
    }

    /// Increment the key count for the shard containing `key`.
    #[cfg(test)]
    pub fn inc_key_count(&self, key: &[u8]) {
        let shard_idx = self.shard_index(key);
        self.shards[shard_idx]
            .key_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Get total memory usage across all shards.
    #[cfg(test)]
    pub fn total_used_memory(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.used_memory.load(Ordering::Relaxed))
            .sum()
    }

    /// Get total key count across all shards.
    #[cfg(test)]
    pub fn total_key_count(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.key_count.load(Ordering::Relaxed) as u64)
            .sum()
    }

    // --- Transaction locking ---

    /// Get the transaction lock for a specific shard (for MULTI/EXEC isolation).
    pub fn shard_tx_lock(&self, shard_idx: usize) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.shards[shard_idx].tx_lock)
    }

    /// Compute unique sorted shard indices for a set of keys.
    pub fn unique_shard_indices(&self, keys: &[Vec<u8>]) -> Vec<usize> {
        let mut indices: Vec<usize> = keys.iter().map(|k| self.shard_index(k)).collect();
        indices.sort_unstable();
        indices.dedup();
        indices
    }

    // --- Batch execution methods ---

    /// Execute a read-only function with direct access to a shard's HashMap.
    /// Acquires a read lock for the duration of the closure. Used by pipeline
    /// batching to execute multiple read commands under a single lock acquisition.
    #[inline]
    pub fn execute_on_shard_ref<F, R>(&self, shard_idx: usize, f: F) -> R
    where
        F: FnOnce(&ShardMap) -> R,
    {
        let guard = self.shards[shard_idx].inner.read();
        f(&guard)
    }

    /// Execute a mutating function with direct access to a shard's HashMap.
    /// Acquires a write lock for the duration of the closure. Used by pipeline
    /// batching to execute multiple write commands under a single lock acquisition.
    #[inline]
    pub fn execute_on_shard_mut<F, R>(&self, shard_idx: usize, f: F) -> R
    where
        F: FnOnce(&mut ShardMap) -> R,
    {
        let mut guard = self.shards[shard_idx].inner.write();
        f(&mut guard)
    }

    // --- Shard-indexed counter methods (for batch processing) ---

    /// Add to the memory counter for a specific shard by index.
    pub fn add_memory_by_shard(&self, shard_idx: usize, amount: u64) {
        self.shards[shard_idx]
            .used_memory
            .fetch_add(amount, Ordering::Relaxed);
    }

    /// Subtract from the memory counter for a specific shard by index.
    pub fn sub_memory_by_shard(&self, shard_idx: usize, amount: u64) {
        let _ = self.shards[shard_idx].used_memory.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |cur| Some(cur.saturating_sub(amount)),
        );
    }

    /// Increment the key count for a specific shard by index.
    pub fn inc_key_count_by_shard(&self, shard_idx: usize, count: u32) {
        self.shards[shard_idx]
            .key_count
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Decrement the key count for a specific shard by index.
    pub fn dec_key_count_by_shard(&self, shard_idx: usize, count: u32) {
        let _ = self.shards[shard_idx].key_count.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |cur| Some(cur.saturating_sub(count)),
        );
    }

    // --- Per-shard expiration methods ---

    /// Add a key to the per-shard expiration heap.
    pub fn add_expiration(&self, key: &[u8], expires_at: Instant) {
        let shard_idx = self.shard_index(key);
        self.add_expiration_by_shard(shard_idx, key.to_vec(), expires_at);
    }

    /// Add an expiration entry to a specific shard's heap by index.
    pub fn add_expiration_by_shard(&self, shard_idx: usize, key: Vec<u8>, expires_at: Instant) {
        let mut heap = self.shards[shard_idx].expiration_heap.lock();
        heap.push(ShardExpirationEntry { expires_at, key });
    }

    /// Remove a key from the per-shard expiration heap.
    /// O(n) filter + heapify via `retain`, only called on TTL removal (PERSIST, DEL).
    pub fn remove_expiration(&self, key: &[u8]) {
        let shard_idx = self.shard_index(key);
        let mut heap = self.shards[shard_idx].expiration_heap.lock();
        heap.retain(|entry| entry.key != key);
    }

    /// Process expired keys from a single shard's expiration heap.
    /// Returns a list of (key, Entry) pairs that were expired and removed.
    /// `max_removals`: maximum keys to expire in this call.
    /// `now`: the current (possibly cached) instant.
    pub fn expire_shard(
        &self,
        shard_idx: usize,
        now: Instant,
        max_removals: usize,
    ) -> Vec<(Vec<u8>, Entry)> {
        let mut removed = Vec::new();

        // Phase 1: Collect expired keys from the heap (lock heap only)
        let mut expired_keys = Vec::new();
        {
            let mut heap = self.shards[shard_idx].expiration_heap.lock();
            while expired_keys.len() < max_removals {
                match heap.peek() {
                    Some(entry) if entry.expires_at <= now => {
                        let entry = heap.pop().unwrap();
                        expired_keys.push(entry.key);
                    }
                    _ => break,
                }
            }
        }

        if expired_keys.is_empty() {
            return removed;
        }

        // Phase 2: Remove expired keys from data map (lock data)
        let mut guard = self.shards[shard_idx].inner.write();
        for key in expired_keys {
            if let Some(entry) = guard.get(&key) {
                // Verify the key is actually expired (may have been refreshed)
                if let Some(exp) = entry.expires_at
                    && exp <= now
                    && let Some(entry) = guard.remove(&key)
                {
                    removed.push((key, entry));
                }
                // else: key has no expiry anymore (PERSIST was called), skip
            }
            // else: key was already deleted, skip
        }

        removed
    }

    /// Sample eviction candidates from a specific shard.
    /// Returns up to EVICTION_SAMPLE_SIZE candidates without removing them.
    pub fn sample_eviction_candidates(
        &self,
        shard_idx: usize,
        volatile_only: bool,
    ) -> ArrayVec<EvictionCandidate, EVICTION_SAMPLE_SIZE> {
        let mut candidates = ArrayVec::new();
        let guard = self.shards[shard_idx].inner.read();

        for (key, entry) in guard.iter() {
            if candidates.is_full() {
                break;
            }
            if volatile_only && entry.expires_at.is_none() {
                continue;
            }
            candidates.push(EvictionCandidate {
                key: key.clone(),
                lru_clock: entry.lru_clock,
                access_count: entry.access_count,
                has_expiry: entry.expires_at.is_some(),
                mem_size: key.len() + entry.value.mem_size(),
            });
        }

        candidates
    }

    /// COW snapshot: clone each shard's HashMap under a short-lived read lock.
    /// Returns a Vec of (key, Entry) pairs for all non-expired entries.
    /// Each shard lock is held only for the duration of the clone, then released
    /// before moving to the next shard. This allows concurrent writes to proceed
    /// on other shards while the snapshot is being built.
    pub fn snapshot_cow(&self) -> Vec<(Vec<u8>, Entry)> {
        let mut result = Vec::new();
        for shard in self.shards.iter() {
            let guard = shard.inner.read();
            for (key, entry) in guard.iter() {
                if !entry.is_expired() {
                    result.push((key.clone(), entry.clone()));
                }
            }
            // guard drops here, releasing the read lock before next shard
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::item::Entry;
    use crate::storage::value::StoreValue;

    fn make_entry(val: &[u8]) -> Entry {
        Entry::new(StoreValue::Str(val.to_vec().into()))
    }

    #[test]
    fn test_shard_distribution() {
        let store = ShardedStore::new();
        let num_shards = store.num_shards();
        assert!(num_shards >= 16);
        assert!(num_shards.is_power_of_two());

        // Insert many keys and verify distribution
        let mut shard_counts = vec![0usize; num_shards];
        for i in 0..10000 {
            let key = format!("key:{}", i);
            let idx = store.shard_index(key.as_bytes());
            assert!(idx < num_shards);
            shard_counts[idx] += 1;
        }

        // Verify no shard is empty and distribution is reasonable
        let min_count = *shard_counts.iter().min().unwrap();
        let max_count = *shard_counts.iter().max().unwrap();
        // With 10000 keys across N shards, expect ~10000/N per shard
        // Allow 10x variance for hash distribution
        let expected = 10000 / num_shards;
        assert!(min_count > 0, "No shard should be empty");
        assert!(
            max_count < expected * 10,
            "max_count={} too high (expected ~{})",
            max_count,
            expected
        );
    }

    #[test]
    fn test_basic_operations() {
        let store = ShardedStore::new();

        // Insert
        assert!(
            store
                .insert(b"key1".to_vec(), make_entry(b"val1"))
                .is_none()
        );
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());

        // Get
        let entry = store.get(b"key1").unwrap();
        assert_eq!(entry.value.as_str().unwrap(), b"val1".as_slice());
        drop(entry);

        // Get mut
        {
            let mut entry = store.get_mut(b"key1").unwrap();
            entry.value = StoreValue::Str(b"val2".to_vec().into());
        }
        let entry = store.get(b"key1").unwrap();
        assert_eq!(entry.value.as_str().unwrap(), b"val2".as_slice());
        drop(entry);

        // Contains
        assert!(store.contains_key(b"key1"));
        assert!(!store.contains_key(b"key2"));

        // Remove
        let (k, v) = store.remove(b"key1").unwrap();
        assert_eq!(k, b"key1".to_vec());
        assert_eq!(v.value.as_str().unwrap(), b"val2".as_slice());
        assert!(store.is_empty());
        assert!(store.remove(b"key1").is_none());
    }

    #[test]
    fn test_entry_api_occupied() {
        let store = ShardedStore::new();
        store.insert(b"key1".to_vec(), make_entry(b"val1"));

        match store.entry(b"key1".to_vec()) {
            ShardEntry::Occupied(mut occ) => {
                assert_eq!(occ.key(), &b"key1".to_vec());
                assert_eq!(occ.get().value.as_str().unwrap(), b"val1".as_slice());

                // Modify through entry
                occ.get_mut().value = StoreValue::Str(b"val2".to_vec().into());
                assert_eq!(occ.get().value.as_str().unwrap(), b"val2".as_slice());

                // Insert replacement
                let old = occ.insert(make_entry(b"val3"));
                assert_eq!(old.value.as_str().unwrap(), b"val2".as_slice());
            }
            ShardEntry::Vacant(_) => panic!("Expected occupied"),
        }

        // Test remove
        match store.entry(b"key1".to_vec()) {
            ShardEntry::Occupied(occ) => {
                let (k, v) = occ.remove_entry();
                assert_eq!(k, b"key1".to_vec());
                assert_eq!(v.value.as_str().unwrap(), b"val3".as_slice());
            }
            _ => panic!("Expected occupied"),
        }
        assert!(store.is_empty());
    }

    #[test]
    fn test_entry_api_vacant() {
        let store = ShardedStore::new();

        match store.entry(b"key1".to_vec()) {
            ShardEntry::Vacant(vac) => {
                assert_eq!(vac.key(), &b"key1".to_vec());
                vac.insert(make_entry(b"val1"));
            }
            ShardEntry::Occupied(_) => panic!("Expected vacant"),
        }

        assert_eq!(store.len(), 1);
        assert!(store.contains_key(b"key1"));
    }

    #[test]
    fn test_concurrent_read_write() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(ShardedStore::new());
        let num_threads = 8;
        let ops_per_thread = 1000;

        let mut handles = Vec::new();

        // Spawn writer threads
        for t in 0..num_threads {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("t{}:k{}", t, i);
                    let val = format!("v{}", i);
                    store.insert(key.into_bytes(), make_entry(val.as_bytes()));
                }
            }));
        }

        // Spawn reader threads
        for t in 0..num_threads {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("t{}:k{}", t, i);
                    let _ = store.get(key.as_bytes());
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(store.len(), num_threads * ops_per_thread);
    }

    #[test]
    fn test_for_each_and_filter_map() {
        let store = ShardedStore::new();
        for i in 0..100 {
            let key = format!("key:{}", i);
            store.insert(key.into_bytes(), make_entry(b"val"));
        }

        let mut count = 0;
        store.for_each(|_key, _entry| {
            count += 1;
        });
        assert_eq!(count, 100);

        let keys: Vec<Vec<u8>> = store.filter_map(|key, _entry| Some(key.clone()));
        assert_eq!(keys.len(), 100);
    }

    #[test]
    fn test_per_shard_counters() {
        let store = ShardedStore::new();
        store.insert(b"key1".to_vec(), make_entry(b"val1"));
        store.add_memory(b"key1", 100);
        store.inc_key_count(b"key1");

        assert!(store.total_used_memory() >= 100);
        assert!(store.total_key_count() >= 1);
    }

    #[test]
    fn test_clear() {
        let store = ShardedStore::new();
        for i in 0..100 {
            store.insert(format!("key:{}", i).into_bytes(), make_entry(b"val"));
        }
        assert_eq!(store.len(), 100);

        store.clear();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn test_sample() {
        let store = ShardedStore::new();
        for i in 0..1000 {
            store.insert(format!("key:{}", i).into_bytes(), make_entry(b"val"));
        }

        let sampled: Vec<Vec<u8>> = store.sample(20, |key, _| Some(key.clone()));
        assert_eq!(sampled.len(), 20);
    }

    #[test]
    fn test_remove_if() {
        let store = ShardedStore::new();
        store.insert(b"keep".to_vec(), make_entry(b"val"));
        store.insert(b"remove".to_vec(), make_entry(b"val"));

        // Should not remove when predicate is false
        assert!(!store.remove_if(b"keep", |_, _| false));
        assert!(store.contains_key(b"keep"));

        // Should remove when predicate is true
        assert!(store.remove_if(b"remove", |_, _| true));
        assert!(!store.contains_key(b"remove"));

        // Should return false for non-existent key
        assert!(!store.remove_if(b"nonexistent", |_, _| true));
    }

    #[tokio::test]
    async fn test_shard_tx_locks() {
        let store = ShardedStore::new();
        let shard_idx = store.shard_index(b"key1");

        // Should be able to acquire and release tx lock
        let lock = store.shard_tx_lock(shard_idx);
        let _guard = lock.lock().await;
        // Lock is held, data operations should still work (separate locks)
        store.insert(b"key1".to_vec(), make_entry(b"val1"));
        assert!(store.contains_key(b"key1"));
    }

    #[test]
    fn test_unique_shard_indices() {
        let store = ShardedStore::new();
        let keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"a".to_vec()];
        let indices = store.unique_shard_indices(&keys);
        // Should be sorted and deduplicated
        for i in 1..indices.len() {
            assert!(indices[i] > indices[i - 1]);
        }
    }

    #[test]
    fn test_cache_line_alignment() {
        // Verify Shard is 128-byte aligned
        assert_eq!(std::mem::align_of::<Shard>(), 128);
    }

    #[test]
    fn test_concurrent_reads_dont_block() {
        // Verify that many concurrent reads via get() (read lock) don't block each other
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(ShardedStore::new());

        // Populate
        for i in 0..1000 {
            store.insert(format!("key:{}", i).into_bytes(), make_entry(b"value"));
        }

        // Spawn many reader threads simultaneously
        let mut handles = Vec::new();
        for _ in 0..16 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let key = format!("key:{}", i);
                    let entry = store.get(key.as_bytes());
                    assert!(entry.is_some(), "Key should exist");
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_relaxed_ordering_counters() {
        // Verify that relaxed atomics for memory/key counters converge to correct values
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(ShardedStore::new());
        let num_threads = 8;
        let ops_per_thread = 500;

        let mut handles = Vec::new();
        for t in 0..num_threads {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("t{}:k{}", t, i);
                    store.insert(key.into_bytes(), make_entry(b"val"));
                    store.add_memory(format!("t{}:k{}", t, i).as_bytes(), 10);
                    store.inc_key_count(format!("t{}:k{}", t, i).as_bytes());
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // After all threads complete, counters should be correct
        let total_keys = store.total_key_count();
        assert_eq!(total_keys, (num_threads * ops_per_thread) as u64);

        let total_memory = store.total_used_memory();
        assert_eq!(total_memory, (num_threads * ops_per_thread * 10) as u64);
    }

    #[test]
    fn test_execute_on_shard_ref_concurrent() {
        // Verify execute_on_shard_ref uses read locks allowing concurrent access
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(ShardedStore::new());
        for i in 0..100 {
            store.insert(format!("key:{}", i).into_bytes(), make_entry(b"value"));
        }

        let mut handles = Vec::new();
        let num_shards = store.num_shards();

        for _ in 0..8 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let mut found = 0;
                for shard_idx in 0..num_shards {
                    found += store.execute_on_shard_ref(shard_idx, |map| map.len());
                }
                assert_eq!(found, 100);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_per_shard_expiration_basic() {
        use std::time::Duration;

        let store = ShardedStore::with_shard_count(16);
        let key = b"expkey".to_vec();
        let now = Instant::now();
        let expires_at = now + Duration::from_millis(50);

        // Insert key with expiration
        let mut entry = make_entry(b"val");
        entry.expires_at = Some(expires_at);
        store.insert(key.clone(), entry);

        // Add to expiration heap
        store.add_expiration(&key, expires_at);

        // Before expiration: expire_shard should not remove
        let removed = store.expire_shard(store.shard_index(&key), now, 10);
        assert!(removed.is_empty(), "Should not expire before deadline");
        assert!(store.contains_key(&key));

        // After expiration: expire_shard should remove
        let future = now + Duration::from_millis(100);
        let removed = store.expire_shard(store.shard_index(&key), future, 10);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0, key);
        assert!(!store.contains_key(&key));
    }

    #[test]
    fn test_per_shard_expiration_refreshed_key() {
        use std::time::Duration;

        let store = ShardedStore::with_shard_count(16);
        let key = b"refreshed".to_vec();
        let now = Instant::now();
        let old_expires = now + Duration::from_millis(50);

        // Insert with short TTL
        let mut entry = make_entry(b"val");
        entry.expires_at = Some(old_expires);
        store.insert(key.clone(), entry);
        store.add_expiration(&key, old_expires);

        // Refresh the TTL to much later
        let new_expires = now + Duration::from_secs(60);
        if let Some(mut e) = store.get_mut(&key) {
            e.expires_at = Some(new_expires);
        }

        // Try to expire at old time - key should NOT be removed
        // because the entry's expires_at was refreshed
        let removed = store.expire_shard(
            store.shard_index(&key),
            now + Duration::from_millis(100),
            10,
        );
        assert!(removed.is_empty(), "Refreshed key should not be expired");
        assert!(store.contains_key(&key));
    }

    #[test]
    fn test_per_shard_expiration_max_removals() {
        use std::time::Duration;

        let store = ShardedStore::with_shard_count(16);
        let now = Instant::now();
        let expires = now + Duration::from_millis(10);

        // Insert multiple keys that all hash to the same shard
        // by finding keys that land on shard 0
        let mut shard0_keys = Vec::new();
        for i in 0u32..10000 {
            let key = format!("exp:{}", i).into_bytes();
            if store.shard_index(&key) == 0 {
                let mut entry = make_entry(b"val");
                entry.expires_at = Some(expires);
                store.insert(key.clone(), entry);
                store.add_expiration_by_shard(0, key.clone(), expires);
                shard0_keys.push(key);
                if shard0_keys.len() >= 10 {
                    break;
                }
            }
        }
        assert!(shard0_keys.len() >= 5, "Need enough keys on shard 0");

        // Expire with max_removals = 3
        let future = now + Duration::from_millis(100);
        let removed = store.expire_shard(0, future, 3);
        assert_eq!(removed.len(), 3, "Should respect max_removals limit");
    }

    #[test]
    fn test_remove_expiration() {
        use std::time::Duration;

        let store = ShardedStore::with_shard_count(16);
        let key = b"persist_me".to_vec();
        let now = Instant::now();
        let expires = now + Duration::from_millis(50);

        // Add expiration
        let mut entry = make_entry(b"val");
        entry.expires_at = Some(expires);
        store.insert(key.clone(), entry);
        store.add_expiration(&key, expires);

        // Remove expiration (simulates PERSIST)
        store.remove_expiration(&key);

        // expire_shard should find nothing in the heap
        let future = now + Duration::from_millis(100);
        let removed = store.expire_shard(store.shard_index(&key), future, 10);
        assert!(removed.is_empty());
    }

    #[test]
    fn test_eviction_candidate_sampling() {
        let store = ShardedStore::with_shard_count(16);

        // Insert keys with varying LRU clocks
        for i in 0..100 {
            let key = format!("evict:{}", i).into_bytes();
            let mut entry = make_entry(b"val");
            entry.lru_clock = i as u32;
            entry.access_count = (100 - i) as u16;
            if i % 2 == 0 {
                entry.expires_at = Some(Instant::now() + std::time::Duration::from_secs(60));
            }
            store.insert(key, entry);
        }

        // Sample from each shard (all keys)
        let mut total_candidates = 0;
        for shard_idx in 0..store.num_shards() {
            let candidates = store.sample_eviction_candidates(shard_idx, false);
            total_candidates += candidates.len();
            // Each candidate should have valid data
            for c in &candidates {
                assert!(!c.key.is_empty());
                assert!(c.mem_size > 0);
            }
        }
        assert!(total_candidates > 0, "Should find some candidates");

        // Sample volatile-only (keys with expiry)
        let mut volatile_candidates = 0;
        for shard_idx in 0..store.num_shards() {
            let candidates = store.sample_eviction_candidates(shard_idx, true);
            for c in &candidates {
                assert!(
                    c.has_expiry,
                    "Volatile-only should only return keys with expiry"
                );
            }
            volatile_candidates += candidates.len();
        }
        assert!(volatile_candidates > 0 && volatile_candidates < total_candidates);
    }

    #[test]
    fn test_clear_also_clears_expiration_heaps() {
        use std::time::Duration;

        let store = ShardedStore::with_shard_count(16);
        let now = Instant::now();

        // Add keys with expirations
        for i in 0..50 {
            let key = format!("clearme:{}", i).into_bytes();
            let expires = now + Duration::from_millis(100);
            let mut entry = make_entry(b"val");
            entry.expires_at = Some(expires);
            store.insert(key.clone(), entry);
            store.add_expiration(&key, expires);
        }

        // Clear the store
        store.clear();

        // Expiration heaps should also be empty
        let future = now + Duration::from_secs(1);
        for shard_idx in 0..store.num_shards() {
            let removed = store.expire_shard(shard_idx, future, 100);
            assert!(removed.is_empty(), "Expiration heaps should be cleared");
        }
    }
}
