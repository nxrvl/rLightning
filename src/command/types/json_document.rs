use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::cell::RefCell;
use std::borrow::Cow;
use tracing::{debug, warn};

/// Optimized JSON document structure for efficient path operations
#[derive(Debug, Clone)]
pub struct JsonDocument {
    /// Root node of the document
    root: Arc<JsonNode>,
    
    /// Path cache for frequently used paths
    path_cache: RefCell<HashMap<String, Weak<JsonNode>>>,
    
    /// Size of the document in bytes (estimated)
    size_bytes: usize,
    
    /// Original JSON value
    original: Value,
}

impl JsonDocument {
    /// Create a new JSON document from a JSON value
    pub fn new(value: Value) -> Self {
        // Estimate the size of the document
        let size_bytes = estimate_json_size(&value);
        
        // Create the optimized structure
        let root = Arc::new(JsonNode::from_value(&value, None));
        
        JsonDocument {
            root,
            path_cache: RefCell::new(HashMap::new()),
            size_bytes,
            original: value,
        }
    }
    
    /// Get a value at a specific path
    pub fn get(&self, path: &str) -> Option<Value> {
        // Check cache first
        let mut cache = self.path_cache.borrow_mut();
        if let Some(node_weak) = cache.get(path) {
            if let Some(node) = node_weak.upgrade() {
                return Some(node.to_value());
            }
        }
        
        // Parse the path
        let path_parts = parse_path(path);
        if path_parts.is_empty() {
            return Some(self.original.clone());
        }
        
        // Navigate to the target node
        let mut current = self.root.clone();
        for part in &path_parts {
            match &*current {
                JsonNode::Object(map) => {
                    if let Some(next) = map.get(part) {
                        current = next.clone();
                    } else {
                        return None;
                    }
                }
                JsonNode::Array(items) => {
                    if let Ok(index) = part.parse::<usize>() {
                        if index < items.len() {
                            current = items[index].clone();
                        } else {
                            return None;
                        }
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        
        // Cache the result if it's an object or array (worth caching)
        match &*current {
            JsonNode::Object(_) | JsonNode::Array(_) => {
                cache.insert(path.to_string(), Arc::downgrade(&current));
            }
            _ => {}
        }
        
        // Convert node back to JSON value
        Some(current.to_value())
    }
    
    /// Set a value at a specific path
    pub fn set(&mut self, path: &str, value: Value) -> bool {
        // If path is root, replace the entire document
        if path == "." {
            self.root = Arc::new(JsonNode::from_value(&value, None));
            self.original = value;
            self.path_cache = RefCell::new(HashMap::new());
            return true;
        }
        
        // Parse the path
        let path_parts = parse_path(path);
        if path_parts.is_empty() {
            return false;
        }
        
        // Clone the document to modify it
        let mut new_root = self.root.as_ref().clone();
        
        // Navigate to the parent node
        let mut current = &mut new_root;
        for (i, part) in path_parts.iter().enumerate().take(path_parts.len() - 1) {
            match current {
                JsonNode::Object(map) => {
                    if !map.contains_key(part) {
                        // Create intermediate objects as needed
                        map.insert(part.clone(), Arc::new(JsonNode::Object(HashMap::new())));
                    }
                    let next = map.get_mut(part).unwrap();
                    let next_node = Arc::as_ref(next).clone();
                    current = Arc::get_mut(next).unwrap();
                }
                JsonNode::Array(items) => {
                    if let Ok(index) = part.parse::<usize>() {
                        if index < items.len() {
                            let next = &mut items[index];
                            current = Arc::get_mut(next).unwrap();
                        } else {
                            return false; // Index out of bounds
                        }
                    } else {
                        return false; // Invalid array index
                    }
                }
                _ => return false, // Not an object or array
            }
        }
        
        // Set the value at the final path segment
        let last_part = &path_parts[path_parts.len() - 1];
        match current {
            JsonNode::Object(map) => {
                map.insert(last_part.clone(), Arc::new(JsonNode::from_value(&value, None)));
            }
            JsonNode::Array(items) => {
                if let Ok(index) = last_part.parse::<usize>() {
                    if index < items.len() {
                        items[index] = Arc::new(JsonNode::from_value(&value, None));
                    } else {
                        return false; // Index out of bounds
                    }
                } else {
                    return false; // Invalid array index
                }
            }
            _ => return false, // Not an object or array
        }
        
        // Update the document
        self.root = Arc::new(new_root);
        
        // Clear the path cache
        self.path_cache = RefCell::new(HashMap::new());
        
        // Update the original value
        self.original = self.root.to_value();
        
        true
    }
    
    /// Get the size of the document in bytes
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }
    
    /// Get all paths in the document
    pub fn paths(&self) -> Vec<String> {
        let mut result = Vec::new();
        self.collect_paths(&mut result, self.root.clone(), "".to_string());
        result
    }
    
    /// Helper to collect all paths in the document
    fn collect_paths(&self, result: &mut Vec<String>, node: Arc<JsonNode>, prefix: String) {
        match &*node {
            JsonNode::Object(map) => {
                for (key, value) in map {
                    let new_prefix = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", prefix, key)
                    };
                    result.push(new_prefix.clone());
                    self.collect_paths(result, value.clone(), new_prefix);
                }
            }
            JsonNode::Array(items) => {
                for (i, item) in items.iter().enumerate() {
                    let new_prefix = if prefix.is_empty() {
                        i.to_string()
                    } else {
                        format!("{}.{}", prefix, i)
                    };
                    result.push(new_prefix.clone());
                    self.collect_paths(result, item.clone(), new_prefix);
                }
            }
            _ => {}
        }
    }
    
    /// Convert the document back to a JSON value
    pub fn to_value(&self) -> Value {
        self.original.clone()
    }
}

/// Optimized node structure for the JSON document
#[derive(Debug, Clone)]
enum JsonNode {
    /// Null value
    Null,
    /// Boolean value
    Boolean(bool),
    /// Number value
    Number(f64),
    /// String value
    String(String),
    /// Object with named fields
    Object(HashMap<String, Arc<JsonNode>>),
    /// Array of items
    Array(Vec<Arc<JsonNode>>),
}

impl JsonNode {
    /// Create a node from a JSON value
    fn from_value(value: &Value, _parent: Option<Weak<JsonNode>>) -> Self {
        match value {
            Value::Null => JsonNode::Null,
            Value::Bool(b) => JsonNode::Boolean(*b),
            Value::Number(n) => {
                if let Some(v) = n.as_f64() {
                    JsonNode::Number(v)
                } else {
                    // Fallback for large integers
                    JsonNode::Number(n.as_i64().unwrap_or(0) as f64)
                }
            }
            Value::String(s) => JsonNode::String(s.clone()),
            Value::Array(arr) => {
                let items: Vec<Arc<JsonNode>> = arr.iter()
                    .map(|v| Arc::new(JsonNode::from_value(v, None)))
                    .collect();
                JsonNode::Array(items)
            }
            Value::Object(obj) => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k.clone(), Arc::new(JsonNode::from_value(v, None)));
                }
                JsonNode::Object(map)
            }
        }
    }
    
    /// Convert a node back to a JSON value
    fn to_value(&self) -> Value {
        match self {
            JsonNode::Null => Value::Null,
            JsonNode::Boolean(b) => Value::Bool(*b),
            JsonNode::Number(n) => {
                // Try to convert to integer if it's a whole number
                if n.fract() == 0.0 && *n <= i64::MAX as f64 && *n >= i64::MIN as f64 {
                    Value::Number(serde_json::Number::from(*n as i64))
                } else {
                    Value::Number(serde_json::Number::from_f64(*n).unwrap_or_default())
                }
            }
            JsonNode::String(s) => Value::String(s.clone()),
            JsonNode::Array(arr) => {
                let items: Vec<Value> = arr.iter()
                    .map(|node| node.to_value())
                    .collect();
                Value::Array(items)
            }
            JsonNode::Object(obj) => {
                let mut map = Map::new();
                for (k, v) in obj {
                    map.insert(k.clone(), v.to_value());
                }
                Value::Object(map)
            }
        }
    }
}

/// Parse a JSON path into individual segments
fn parse_path(path: &str) -> Vec<String> {
    if path == "." {
        return Vec::new();
    }
    
    path.split('.')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// Estimate the size of a JSON value in bytes
fn estimate_json_size(value: &Value) -> usize {
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
                size += estimate_json_size(item);
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
                size += estimate_json_size(value);
            }
            
            size
        }
    }
}