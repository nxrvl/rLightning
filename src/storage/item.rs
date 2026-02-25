use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};

/// Redis data type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Represents a single key-value pair in the storage engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageItem {
    /// The actual data value
    pub value: Vec<u8>,
    /// The Redis data type for this item
    pub data_type: RedisDataType,
    /// When this item was created
    #[allow(dead_code)]
    #[serde(skip, default = "Instant::now")]
    pub created_at: Instant,
    /// When this item was last accessed
    #[serde(skip, default = "Instant::now")]
    pub last_accessed: Instant,
    /// When this item will expire (if set)
    #[serde(skip)]
    pub expires_at: Option<Instant>,
}

impl StorageItem {
    /// Create a new storage item with string type (for backwards compatibility)
    pub fn new(value: Vec<u8>) -> Self {
        let now = Instant::now();
        
        StorageItem {
            value,
            data_type: RedisDataType::String,
            created_at: now,
            last_accessed: now,
            expires_at: None,
        }
    }
    
    /// Create a new storage item with explicit type
    pub fn new_with_type(value: Vec<u8>, data_type: RedisDataType) -> Self {
        let now = Instant::now();
        
        StorageItem {
            value,
            data_type,
            created_at: now,
            last_accessed: now,
            expires_at: None,
        }
    }
    
    /// Check if this item has expired
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Instant::now() > expires_at
        } else {
            false
        }
    }
    
    /// Calculate the remaining TTL (time-to-live)
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
    
    /// Update the item's access time
    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }
    
    /// Set a new expiration time
    pub fn expire(&mut self, ttl: Duration) {
        self.expires_at = Some(Instant::now() + ttl);
    }
    
    /// Remove the expiration time
    pub fn remove_expiry(&mut self) {
        self.expires_at = None;
    }
} 