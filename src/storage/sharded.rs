//! Sharded storage engine using parking_lot::RwLock per shard.
//! Replaces DashMap for better performance and control over locking granularity.

use hashbrown::HashMap as HBHashMap;
use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use rustc_hash::FxBuildHasher;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use crate::storage::item::Entry;

type ShardMap = HBHashMap<Vec<u8>, Entry, FxBuildHasher>;

/// Internal data for a single shard.
struct ShardInner {
    map: ShardMap,
}

/// A single shard with cache-line alignment to prevent false sharing.
#[repr(align(128))]
struct Shard {
    /// Data storage with parking_lot RwLock for fast synchronous locking.
    inner: RwLock<ShardInner>,
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
    guard: RwLockWriteGuard<'a, ShardInner>,
    key: Vec<u8>,
}

/// Vacant entry in a shard (key does not exist). Holds the shard write lock.
pub struct VacantShardEntry<'a> {
    guard: RwLockWriteGuard<'a, ShardInner>,
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
        self.guard.map.get(&self.key).unwrap()
    }

    /// Get a mutable reference to the value.
    pub fn get_mut(&mut self) -> &mut Entry {
        self.guard.map.get_mut(&self.key).unwrap()
    }

    /// Replace the value, returning the old value.
    pub fn insert(&mut self, value: Entry) -> Entry {
        self.guard.map.insert(self.key.clone(), value).unwrap()
    }

    /// Remove the entry and return the value.
    pub fn remove(mut self) -> Entry {
        self.guard.map.remove(&self.key).unwrap()
    }

    /// Remove the entry and return both key and value.
    pub fn remove_entry(mut self) -> (Vec<u8>, Entry) {
        let value = self.guard.map.remove(&self.key).unwrap();
        (self.key, value)
    }
}

impl<'a> VacantShardEntry<'a> {
    /// Get a reference to the key.
    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    /// Insert a value, consuming the vacant entry.
    pub fn insert(mut self, value: Entry) {
        self.guard.map.insert(self.key, value);
    }
}

impl ShardedStore {
    /// Create a new ShardedStore with shard count based on CPU count.
    /// Shard count = (num_cpus * 16).next_power_of_two().clamp(16, 1024)
    pub fn new() -> Self {
        let shard_count = (num_cpus::get() * 16)
            .next_power_of_two()
            .clamp(16, 1024);
        Self::with_shard_count(shard_count)
    }

    /// Create a new ShardedStore with a specific initial capacity per shard.
    pub fn with_capacity(capacity: usize) -> Self {
        let shard_count = (num_cpus::get() * 16)
            .next_power_of_two()
            .clamp(16, 1024);
        Self::with_shard_count_and_capacity(shard_count, capacity)
    }

    fn with_shard_count(shard_count: usize) -> Self {
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
                inner: RwLock::new(ShardInner {
                    map: if per_shard_capacity > 0 {
                        ShardMap::with_capacity_and_hasher(per_shard_capacity, FxBuildHasher)
                    } else {
                        ShardMap::with_hasher(FxBuildHasher)
                    },
                }),
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
        RwLockReadGuard::try_map(guard, |shard| shard.map.get(key)).ok()
    }

    /// Get a write reference to an entry by key.
    /// Returns a mapped write guard that derefs to &mut Entry.
    pub fn get_mut(&self, key: &[u8]) -> Option<MappedRwLockWriteGuard<'_, Entry>> {
        let shard_idx = self.shard_index(key);
        let guard = self.shards[shard_idx].inner.write();
        RwLockWriteGuard::try_map(guard, |shard| shard.map.get_mut(key)).ok()
    }

    /// Get an entry for a key (occupied or vacant), holding the shard write lock.
    pub fn entry(&self, key: Vec<u8>) -> ShardEntry<'_> {
        let shard_idx = self.shard_index(&key);
        let guard = self.shards[shard_idx].inner.write();
        let exists = guard.map.contains_key(&*key);
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
        guard.map.insert(key, value)
    }

    /// Remove a key. Returns (key, value) if the key existed.
    pub fn remove(&self, key: &[u8]) -> Option<(Vec<u8>, Entry)> {
        let shard_idx = self.shard_index(key);
        let mut guard = self.shards[shard_idx].inner.write();
        guard
            .map
            .remove_entry(key)
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let shard_idx = self.shard_index(key);
        let guard = self.shards[shard_idx].inner.read();
        guard.map.contains_key(key)
    }

    /// Get the total number of entries across all shards.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.inner.read().map.len())
            .sum()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.inner.read().map.is_empty())
    }

    /// Remove all entries from all shards.
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            let mut guard = shard.inner.write();
            guard.map.clear();
            shard.used_memory.store(0, Ordering::Release);
            shard.key_count.store(0, Ordering::Release);
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
            for (key, entry) in guard.map.iter() {
                f(key, entry);
            }
        }
    }

    /// Iterate over all entries with mutable access.
    /// Acquires a write lock on each shard sequentially.
    #[allow(dead_code)]
    pub fn for_each_mut<F>(&self, mut f: F)
    where
        F: FnMut(&Vec<u8>, &mut Entry),
    {
        for shard in self.shards.iter() {
            let mut guard = shard.inner.write();
            for (key, entry) in guard.map.iter_mut() {
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
            for (key, entry) in guard.map.iter() {
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
            for (key, entry) in guard.map.iter() {
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
        if let Some((k, v)) = guard.map.get_key_value(key) {
            if f(k, v) {
                guard.map.remove(key);
                return true;
            }
        }
        false
    }

    // --- Per-shard memory tracking ---

    /// Add to the memory counter for the shard containing `key`.
    pub fn add_memory(&self, key: &[u8], amount: u64) {
        let shard_idx = self.shard_index(key);
        self.shards[shard_idx]
            .used_memory
            .fetch_add(amount, Ordering::AcqRel);
    }

    /// Subtract from the memory counter for the shard containing `key`.
    pub fn sub_memory(&self, key: &[u8], amount: u64) {
        let shard_idx = self.shard_index(key);
        self.shards[shard_idx]
            .used_memory
            .fetch_sub(amount, Ordering::AcqRel);
    }

    /// Increment the key count for the shard containing `key`.
    pub fn inc_key_count(&self, key: &[u8]) {
        let shard_idx = self.shard_index(key);
        self.shards[shard_idx]
            .key_count
            .fetch_add(1, Ordering::AcqRel);
    }

    /// Decrement the key count for the shard containing `key`.
    pub fn dec_key_count(&self, key: &[u8]) {
        let shard_idx = self.shard_index(key);
        self.shards[shard_idx]
            .key_count
            .fetch_sub(1, Ordering::AcqRel);
    }

    /// Get total memory usage across all shards.
    pub fn total_used_memory(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.used_memory.load(Ordering::Relaxed))
            .sum()
    }

    /// Get total key count across all shards.
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

    /// Compute unique sorted shard indices from byte slice keys.
    pub fn unique_shard_indices_from_slices(&self, keys: &[&[u8]]) -> Vec<usize> {
        let mut indices: Vec<usize> = keys.iter().map(|k| self.shard_index(k)).collect();
        indices.sort_unstable();
        indices.dedup();
        indices
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::item::Entry;
    use crate::storage::value::StoreValue;

    fn make_entry(val: &[u8]) -> Entry {
        Entry::new(StoreValue::Str(val.to_vec()))
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
        assert!(store.insert(b"key1".to_vec(), make_entry(b"val1")).is_none());
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());

        // Get
        let entry = store.get(b"key1").unwrap();
        assert_eq!(entry.value.as_str().unwrap(), &b"val1".to_vec());
        drop(entry);

        // Get mut
        {
            let mut entry = store.get_mut(b"key1").unwrap();
            entry.value = StoreValue::Str(b"val2".to_vec());
        }
        let entry = store.get(b"key1").unwrap();
        assert_eq!(entry.value.as_str().unwrap(), &b"val2".to_vec());
        drop(entry);

        // Contains
        assert!(store.contains_key(b"key1"));
        assert!(!store.contains_key(b"key2"));

        // Remove
        let (k, v) = store.remove(b"key1").unwrap();
        assert_eq!(k, b"key1".to_vec());
        assert_eq!(v.value.as_str().unwrap(), &b"val2".to_vec());
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
                assert_eq!(occ.get().value.as_str().unwrap(), &b"val1".to_vec());

                // Modify through entry
                occ.get_mut().value = StoreValue::Str(b"val2".to_vec());
                assert_eq!(occ.get().value.as_str().unwrap(), &b"val2".to_vec());

                // Insert replacement
                let old = occ.insert(make_entry(b"val3"));
                assert_eq!(old.value.as_str().unwrap(), &b"val2".to_vec());
            }
            ShardEntry::Vacant(_) => panic!("Expected occupied"),
        }

        // Test remove
        match store.entry(b"key1".to_vec()) {
            ShardEntry::Occupied(occ) => {
                let (k, v) = occ.remove_entry();
                assert_eq!(k, b"key1".to_vec());
                assert_eq!(v.value.as_str().unwrap(), &b"val3".to_vec());
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
}
