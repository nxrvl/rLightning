use std::sync::LazyLock;
use std::time::{Duration, Instant};

use serde::{Serialize, Deserialize};
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
}

impl Entry {
    /// Create a new entry with a native value.
    pub fn new(value: StoreValue) -> Self {
        Entry {
            value,
            expires_at: None,
            lru_clock: lru_now(),
            access_count: 0,
        }
    }

    /// Create a new string entry (convenience for the most common case).
    pub fn new_string(value: Vec<u8>) -> Self {
        Self::new(StoreValue::Str(value.into()))
    }

    /// Check if this entry has expired.
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Instant::now() > expires_at
        } else {
            false
        }
    }

    /// Calculate the remaining TTL (time-to-live).
    pub fn ttl(&self) -> Option<Duration> {
        self.expires_at.map(|expires_at| {
            let now = Instant::now();
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
    pub fn expire(&mut self, ttl: Duration) {
        self.expires_at = Some(Instant::now() + ttl);
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
