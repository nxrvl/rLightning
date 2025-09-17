use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Configuration for JSON data handling in Redis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonConfig {
    /// Maximum size for JSON documents in bytes (default: 512MB)
    pub max_document_size_bytes: usize,
    
    /// Maximum depth for nested JSON objects (default: 128)
    pub max_nesting_depth: usize,
    
    /// Maximum number of fields in a JSON object (default: 100,000)
    pub max_object_fields: usize,
    
    /// Whether to validate UTF-8 in JSON strings (default: true)
    pub validate_utf8: bool,
    
    /// Whether to sanitize control characters (default: true)
    pub sanitize_control_chars: bool,
    
    /// Whether to use path optimization for common queries (default: true)
    pub optimize_path_queries: bool,
    
    /// Whether to fragment large JSON documents for memory efficiency (default: true)
    pub use_fragmentation: bool,
    
    /// Fragmentation threshold in bytes (default: 1MB)
    pub fragmentation_threshold: usize,
    
    /// Maximum number of cached paths per document (default: 100)
    pub max_path_cache_size: usize,
    
    /// Cache size for documents in memory (default: 1000)
    pub document_cache_size: usize,
}

impl Default for JsonConfig {
    fn default() -> Self {
        JsonConfig {
            // 512MB - same as Redis default max string size
            max_document_size_bytes: 512 * 1024 * 1024,
            
            // Reasonable limits to prevent stack overflows
            max_nesting_depth: 128,
            max_object_fields: 100_000,
            
            // Safety features enabled by default
            validate_utf8: true,
            sanitize_control_chars: true,
            
            // Performance optimizations
            optimize_path_queries: true,
            
            // Memory management
            use_fragmentation: true,
            fragmentation_threshold: 1024 * 1024, // 1MB
            max_path_cache_size: 100,
            document_cache_size: 1000,
        }
    }
}

impl JsonConfig {
    /// Apply configuration from a JSON object
    pub fn apply_from_json(&mut self, config_json: &serde_json::Value) -> Result<(), String> {
        if let Some(size) = config_json.get("max_document_size_mb") {
            if let Some(size) = size.as_u64() {
                // Convert MB to bytes
                self.max_document_size_bytes = (size as usize) * 1024 * 1024;
                debug!("Set max_document_size_bytes to {}", self.max_document_size_bytes);
            } else {
                return Err("max_document_size_mb must be a positive integer".to_string());
            }
        }

        if let Some(depth) = config_json.get("max_nesting_depth") {
            if let Some(depth) = depth.as_u64() {
                self.max_nesting_depth = depth as usize;
                debug!("Set max_nesting_depth to {}", self.max_nesting_depth);
            } else {
                return Err("max_nesting_depth must be a positive integer".to_string());
            }
        }

        if let Some(fields) = config_json.get("max_object_fields") {
            if let Some(fields) = fields.as_u64() {
                self.max_object_fields = fields as usize;
                debug!("Set max_object_fields to {}", self.max_object_fields);
            } else {
                return Err("max_object_fields must be a positive integer".to_string());
            }
        }

        if let Some(validate) = config_json.get("validate_utf8") {
            if let Some(validate) = validate.as_bool() {
                self.validate_utf8 = validate;
                debug!("Set validate_utf8 to {}", self.validate_utf8);
            } else {
                return Err("validate_utf8 must be a boolean".to_string());
            }
        }

        if let Some(sanitize) = config_json.get("sanitize_control_chars") {
            if let Some(sanitize) = sanitize.as_bool() {
                self.sanitize_control_chars = sanitize;
                debug!("Set sanitize_control_chars to {}", self.sanitize_control_chars);
            } else {
                return Err("sanitize_control_chars must be a boolean".to_string());
            }
        }

        if let Some(optimize) = config_json.get("optimize_path_queries") {
            if let Some(optimize) = optimize.as_bool() {
                self.optimize_path_queries = optimize;
                debug!("Set optimize_path_queries to {}", self.optimize_path_queries);
            } else {
                return Err("optimize_path_queries must be a boolean".to_string());
            }
        }

        if let Some(fragmentation) = config_json.get("use_fragmentation") {
            if let Some(fragmentation) = fragmentation.as_bool() {
                self.use_fragmentation = fragmentation;
                debug!("Set use_fragmentation to {}", self.use_fragmentation);
            } else {
                return Err("use_fragmentation must be a boolean".to_string());
            }
        }

        if let Some(threshold) = config_json.get("fragmentation_threshold_kb") {
            if let Some(threshold) = threshold.as_u64() {
                // Convert KB to bytes
                self.fragmentation_threshold = (threshold as usize) * 1024;
                debug!("Set fragmentation_threshold to {}", self.fragmentation_threshold);
            } else {
                return Err("fragmentation_threshold_kb must be a positive integer".to_string());
            }
        }

        if let Some(cache_size) = config_json.get("max_path_cache_size") {
            if let Some(cache_size) = cache_size.as_u64() {
                self.max_path_cache_size = cache_size as usize;
                debug!("Set max_path_cache_size to {}", self.max_path_cache_size);
            } else {
                return Err("max_path_cache_size must be a positive integer".to_string());
            }
        }
        
        if let Some(doc_cache) = config_json.get("document_cache_size") {
            if let Some(doc_cache) = doc_cache.as_u64() {
                self.document_cache_size = doc_cache as usize;
                debug!("Set document_cache_size to {}", self.document_cache_size);
            } else {
                return Err("document_cache_size must be a positive integer".to_string());
            }
        }

        Ok(())
    }
}

/// Global JSON configuration with thread-safe access
#[derive(Debug, Clone)]
pub struct GlobalJsonConfig {
    inner: Arc<RwLock<JsonConfig>>,
}

impl GlobalJsonConfig {
    /// Create a new default JSON configuration
    pub fn new() -> Self {
        GlobalJsonConfig {
            inner: Arc::new(RwLock::new(JsonConfig::default())),
        }
    }
    
    /// Update the configuration
    pub async fn update(&self, config: JsonConfig) {
        let mut write_guard = self.inner.write().await;
        *write_guard = config;
    }
    
    /// Get a read-only view of the current configuration
    pub async fn get(&self) -> JsonConfig {
        let read_guard = self.inner.read().await;
        read_guard.clone()
    }
}

impl Default for GlobalJsonConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// JSON path query optimization
#[derive(Debug, Clone)]
pub struct PathOptimization {
    /// Cache for frequently used paths
    pub path_cache: Vec<String>,
    
    /// Whether path optimization is enabled
    pub enabled: bool,
}

impl PathOptimization {
    /// Create a new path optimization with default settings
    pub fn new(enabled: bool) -> Self {
        PathOptimization {
            path_cache: Vec::new(),
            enabled,
        }
    }
    
    /// Add a path to the cache if it doesn't exist
    pub fn add_path(&mut self, path: &str) {
        if self.enabled && !self.path_cache.contains(&path.to_string()) {
            self.path_cache.push(path.to_string());
        }
    }
    
    /// Check if a path is in the cache
    pub fn has_path(&self, path: &str) -> bool {
        self.path_cache.contains(&path.to_string())
    }
}