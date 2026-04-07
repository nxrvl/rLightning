use std::sync::LazyLock;
use std::time::{Duration, Instant};

use serde::{Serialize, Deserialize};
use crate::storage::clock::cached_now;
use crate::storage::value::StoreValue;

/// Redis data type
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RedisDataType {
    String,
    List,
    Set,
    Hash,
    ZSet,
    Stream,
}

impl RedisDataType {
    pub fn as_str(&self) -> &'static str {
        match self {
            RedisDataType::String => "string",
            RedisDataType::List => "list",
            RedisDataType::Set => "set",
            RedisDataType::Hash => "hash",
            RedisDataType::ZSet => "zset",
            RedisDataType::Stream => "stream",
        }
    }
}

/// Epoch for LRU clock calculation.
static LRU_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Get current LRU clock value (seconds since engine start).
pub fn lru_now() -> u32 {
    LRU_EPOCH.elapsed().as_secs() as u32
}

/// A single entry in the storage engine.
/// Replaces the old `StorageItem` with native `StoreValue` types
/// instead of bincode-serialized `Vec<u8>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// The native data value
    pub value: StoreValue,
    /// When this item will expire (if set)
    #[serde(skip)]
    pub expires_at: Option<Instant>,
    /// LRU clock (seconds since engine start)
    pub lru_clock: u32,
    /// Access counter for LFU eviction (u16 is sufficient)
    pub access_count: u16,
    /// Cached value.mem_size() for O(1) memory tracking in atomic_modify.
    /// Updated via deltas on mutation; recalculated on RDB/AOF restore.
    #[serde(skip, default)]
    pub cached_mem_size: u64,
}

impl Entry {
    /// Create a new entry with a native value.
    pub fn new(value: StoreValue) -> Self {
        let cached_mem_size = value.mem_size() as u64;
        Entry {
            value,
            expires_at: None,
            lru_clock: lru_now(),
            access_count: 5,
            cached_mem_size,
        }
    }

    /// Create a new string entry (convenience for the most common case).
    pub fn new_string(value: Vec<u8>) -> Self {
        Self::new(StoreValue::Str(value.into()))
    }

    /// Recalculate cached_mem_size from the current value.
    /// Called after RDB/AOF restore where cached_mem_size defaults to 0.
    pub fn refresh_cached_mem_size(&mut self) {
        self.cached_mem_size = self.value.mem_size() as u64;
    }

    /// Check if this entry has expired.
    /// Uses cached clock (~1ms resolution) to avoid syscall overhead.
    /// Uses `>=` to match Redis semantics and stay consistent with
    /// expire_shard / lazy_expire which use `exp <= now`.
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            cached_now() >= expires_at
        } else {
            false
        }
    }

    /// Calculate the remaining TTL (time-to-live).
    /// Uses cached clock for consistency with is_expired().
    pub fn ttl(&self) -> Option<Duration> {
        self.expires_at.map(|expires_at| {
            let now = cached_now();
            if now >= expires_at {
                Duration::from_secs(0)
            } else {
                expires_at - now
            }
        })
    }

    /// Update the entry's access time and increment access counter.
    pub fn touch(&mut self) {
        self.lru_clock = lru_now();
        self.access_count = self.access_count.saturating_add(1);
    }

    /// Set a new expiration time.
    /// Uses cached clock for consistency. Returns the computed expires_at
    /// so callers can reuse it for the expiration heap without a second
    /// `cached_now()` call.
    pub fn expire(&mut self, ttl: Duration) -> Instant {
        let expires_at = cached_now() + ttl;
        self.expires_at = Some(expires_at);
        expires_at
    }

    /// Remove the expiration time.
    pub fn remove_expiry(&mut self) {
        self.expires_at = None;
    }

    /// Get the Redis data type of this entry's value.
    pub fn data_type(&self) -> RedisDataType {
        self.value.data_type()
    }
}

/// Backward-compatible type alias.
pub type StorageItem = Entry;
