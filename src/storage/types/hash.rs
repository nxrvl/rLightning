use std::collections::HashMap;

/// Represents a Redis hash data structure
#[derive(Debug, Clone)]
pub struct HashItem {
    /// The hash map containing field-value pairs
    pub fields: HashMap<Vec<u8>, Vec<u8>>,
}

impl HashItem {
    /// Create a new empty hash
    pub fn new() -> Self {
        HashItem {
            fields: HashMap::new(),
        }
    }
    
    /// Set a field in the hash
    pub fn set_field(&mut self, field: Vec<u8>, value: Vec<u8>) -> bool {
        // Returns true if this is a new field
        self.fields.insert(field, value).is_none()
    }
    
    /// Get a field value from the hash
    pub fn get_field(&self, field: &[u8]) -> Option<&Vec<u8>> {
        self.fields.get(field)
    }
    
    /// Delete a field from the hash
    pub fn delete_field(&mut self, field: &[u8]) -> bool {
        // Returns true if the field was present
        self.fields.remove(field).is_some()
    }
    
    /// Check if a field exists in the hash
    pub fn field_exists(&self, field: &[u8]) -> bool {
        self.fields.contains_key(field)
    }
    
    /// Get all fields and values in the hash
    pub fn get_all(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.fields.iter()
    }
    
    /// Get the number of fields in the hash
    pub fn len(&self) -> usize {
        self.fields.len()
    }
    
    /// Check if the hash is empty
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
    
    /// Calculate the total memory size of the hash
    pub fn memory_usage(&self) -> usize {
        // Base size of the struct itself - estimation
        let mut size = std::mem::size_of::<Self>();
        
        // Add size of all keys and values
        for (field, value) in &self.fields {
            size += field.len() + value.len();
        }
        
        size
    }
}
