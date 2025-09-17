use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use serde_json::Value;
use lru::{LruCache, KeyRef};
use tracing::{debug, warn, info};

use crate::command::types::json_document::JsonDocument;
use crate::command::types::json_config::JsonConfig;

/// JSON storage with memory optimization
pub struct JsonStorage {
    /// The global configuration
    config: JsonConfig,
    
    /// Document cache using LRU eviction
    documents: RwLock<LruCache<String, Arc<JsonDocument>>>,
    
    /// Fragment storage for very large documents
    fragments: RwLock<HashMap<String, Vec<Arc<JsonFragment>>>>,
    
    /// Memory usage statistics
    memory_stats: Mutex<MemoryStats>,
    
    /// Last cleanup time
    last_cleanup: Mutex<Instant>,
}

impl JsonStorage {
    /// Create a new JSON storage with the given configuration
    pub fn new(config: JsonConfig, capacity: usize) -> Self {
        JsonStorage {
            config,
            documents: RwLock::new(LruCache::new(capacity)),
            fragments: RwLock::new(HashMap::new()),
            memory_stats: Mutex::new(MemoryStats::default()),
            last_cleanup: Mutex::new(Instant::now()),
        }
    }
    
    /// Get a JSON document by key
    pub fn get(&self, key: &str) -> Option<Value> {
        // Check if it's a whole document
        if let Some(doc) = self.get_document(key) {
            return Some(doc.to_value());
        }
        
        // Check if it's a fragmented document
        if let Some(value) = self.get_fragmented(key) {
            return Some(value);
        }
        
        None
    }
    
    /// Get a value at a specific path in a document
    pub fn get_path(&self, key: &str, path: &str) -> Option<Value> {
        // Check if it's a whole document
        if let Some(doc) = self.get_document(key) {
            return doc.get(path);
        }
        
        // Check if it's a fragmented document
        if let Some(value) = self.get_fragmented(key) {
            // Convert to document and get path
            let doc = JsonDocument::new(value);
            return doc.get(path);
        }
        
        None
    }
    
    /// Set a JSON document by key
    pub fn set(&self, key: &str, value: Value, ttl: Option<Duration>) -> bool {
        // Check the document size
        let size = self.estimate_size(&value);
        
        // Update memory stats
        {
            let mut stats = self.memory_stats.lock().unwrap();
            stats.update_set(size);
        }
        
        // Choose storage strategy based on size
        if size > self.config.max_document_size_bytes / 2 {
            // Store as fragments for very large documents
            debug!("Storing large JSON document as fragments: {} bytes", size);
            self.store_fragmented(key, value, ttl)
        } else {
            // Store as single document
            debug!("Storing JSON document: {} bytes", size);
            self.store_document(key, value, ttl)
        }
    }
    
    /// Set a value at a specific path in a document
    pub fn set_path(&self, key: &str, path: &str, value: Value, ttl: Option<Duration>) -> bool {
        // Get the current document
        let current = self.get(key);
        
        match current {
            Some(mut doc_value) => {
                // Convert to document and set path
                let mut doc = JsonDocument::new(doc_value);
                if doc.set(path, value) {
                    // Store the updated document
                    return self.set(key, doc.to_value(), ttl);
                }
                false
            }
            None => {
                // Create a new document if the key doesn't exist
                if path == "." {
                    // Just set the whole document
                    self.set(key, value, ttl)
                } else {
                    // Create a document with the path
                    let mut doc = JsonDocument::new(Value::Object(serde_json::Map::new()));
                    if doc.set(path, value) {
                        self.set(key, doc.to_value(), ttl)
                    } else {
                        false
                    }
                }
            }
        }
    }
    
    /// Delete a JSON document
    pub fn delete(&self, key: &str) -> bool {
        let mut deleted = false;
        
        // Try to delete from documents
        {
            let mut docs = self.documents.write().unwrap();
            if docs.contains(&key.to_string()) {
                docs.pop(&key.to_string());
                deleted = true;
            }
        }
        
        // Try to delete from fragments
        {
            let mut frags = self.fragments.write().unwrap();
            if frags.contains_key(key) {
                frags.remove(key);
                deleted = true;
            }
        }
        
        // Update memory stats if deleted
        if deleted {
            let mut stats = self.memory_stats.lock().unwrap();
            stats.update_delete();
        }
        
        deleted
    }
    
    /// Get memory usage statistics
    pub fn memory_stats(&self) -> MemoryStats {
        let stats = self.memory_stats.lock().unwrap();
        stats.clone()
    }
    
    /// Run periodic cleanup and maintenance
    pub fn cleanup(&self) {
        let mut last_cleanup = self.last_cleanup.lock().unwrap();
        let now = Instant::now();
        
        // Only run cleanup every 60 seconds
        if now.duration_since(*last_cleanup) < Duration::from_secs(60) {
            return;
        }
        
        *last_cleanup = now;
        
        // Log memory usage
        let stats = self.memory_stats();
        info!("JSON memory usage: {} bytes, {} documents, {} fragments",
              stats.total_bytes, stats.document_count, stats.fragment_count);
        
        // Clean up expired fragments
        self.cleanup_fragments();
    }
    
    /// Get a document from the cache
    fn get_document(&self, key: &str) -> Option<Arc<JsonDocument>> {
        let mut docs = self.documents.write().unwrap();
        if let Some(doc) = docs.get(&key.to_string()) {
            // Update memory stats
            let mut stats = self.memory_stats.lock().unwrap();
            stats.update_get(doc.size_bytes());
            
            return Some(doc.clone());
        }
        None
    }
    
    /// Store a document in the cache
    fn store_document(&self, key: &str, value: Value, _ttl: Option<Duration>) -> bool {
        let doc = JsonDocument::new(value);
        let size = doc.size_bytes();
        let doc_arc = Arc::new(doc);
        
        let mut docs = self.documents.write().unwrap();
        docs.put(key.to_string(), doc_arc);
        
        true
    }
    
    /// Get a fragmented document
    fn get_fragmented(&self, key: &str) -> Option<Value> {
        let frags = self.fragments.read().unwrap();
        if let Some(fragments) = frags.get(key) {
            // Combine fragments into a single value
            let mut result_str = String::new();
            for fragment in fragments {
                result_str.push_str(&fragment.data);
            }
            
            // Parse the combined JSON
            match serde_json::from_str(&result_str) {
                Ok(value) => {
                    // Update memory stats
                    let mut stats = self.memory_stats.lock().unwrap();
                    stats.update_get(result_str.len());
                    
                    return Some(value);
                }
                Err(e) => {
                    warn!("Failed to parse fragmented JSON: {}", e);
                    return None;
                }
            }
        }
        None
    }
    
    /// Store a document as fragments
    fn store_fragmented(&self, key: &str, value: Value, ttl: Option<Duration>) -> bool {
        // Convert to string
        let json_str = match serde_json::to_string(&value) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to serialize JSON for fragmentation: {}", e);
                return false;
            }
        };
        
        // Calculate fragment size (max 1MB per fragment)
        let fragment_size = 1024 * 1024;
        let fragments_needed = (json_str.len() + fragment_size - 1) / fragment_size;
        
        debug!("Fragmenting JSON document: {} bytes into {} fragments",
               json_str.len(), fragments_needed);
        
        // Create fragments
        let mut fragments = Vec::with_capacity(fragments_needed);
        for i in 0..fragments_needed {
            let start = i * fragment_size;
            let end = (start + fragment_size).min(json_str.len());
            let data = json_str[start..end].to_string();
            
            let fragment = JsonFragment {
                index: i,
                data,
                created: Instant::now(),
                ttl,
            };
            
            fragments.push(Arc::new(fragment));
        }
        
        // Store fragments
        let mut frags = self.fragments.write().unwrap();
        frags.insert(key.to_string(), fragments);
        
        true
    }
    
    /// Clean up expired fragments
    fn cleanup_fragments(&self) {
        let now = Instant::now();
        let mut frags = self.fragments.write().unwrap();
        
        // Find keys with expired fragments
        let expired_keys: Vec<String> = frags.iter()
            .filter_map(|(key, fragments)| {
                for fragment in fragments {
                    if let Some(ttl) = fragment.ttl {
                        if now.duration_since(fragment.created) > ttl {
                            return Some(key.clone());
                        }
                    }
                }
                None
            })
            .collect();
        
        // Remove expired keys
        for key in expired_keys {
            frags.remove(&key);
        }
    }
    
    /// Estimate the size of a JSON value
    fn estimate_size(&self, value: &Value) -> usize {
        match value {
            Value::Null => 4, // "null"
            Value::Bool(b) => if *b { 4 } else { 5 }, // "true" or "false"
            Value::Number(n) => n.to_string().len(),
            Value::String(s) => s.len() + 2, // quotes
            Value::Array(arr) => {
                // "[" + items + commas + "]"
                let mut size = 2; // brackets
                size += arr.len().saturating_sub(1); // commas
                
                // Add size of each item
                for item in arr {
                    size += self.estimate_size(item);
                }
                
                size
            },
            Value::Object(obj) => {
                // "{" + keys + values + colons + commas + "}"
                let mut size = 2; // braces
                size += obj.len().saturating_sub(1); // commas
                size += obj.len(); // colons
                
                // Add size of each key-value pair
                for (key, value) in obj {
                    size += key.len() + 2; // key + quotes
                    size += self.estimate_size(value);
                }
                
                size
            }
        }
    }
}

/// Fragment of a large JSON document
struct JsonFragment {
    /// Fragment index
    index: usize,
    
    /// Fragment data
    data: String,
    
    /// Creation time
    created: Instant,
    
    /// Time to live (optional)
    ttl: Option<Duration>,
}

/// Memory usage statistics
#[derive(Debug, Clone, Default)]
pub struct MemoryStats {
    /// Total bytes used
    pub total_bytes: usize,
    
    /// Number of documents
    pub document_count: usize,
    
    /// Number of fragments
    pub fragment_count: usize,
    
    /// Number of get operations
    pub get_count: usize,
    
    /// Number of set operations
    pub set_count: usize,
    
    /// Number of delete operations
    pub delete_count: usize,
}

impl MemoryStats {
    /// Update stats for a get operation
    fn update_get(&mut self, size: usize) {
        self.get_count += 1;
    }
    
    /// Update stats for a set operation
    fn update_set(&mut self, size: usize) {
        self.set_count += 1;
        self.total_bytes += size;
    }
    
    /// Update stats for a delete operation
    fn update_delete(&mut self) {
        self.delete_count += 1;
    }
}