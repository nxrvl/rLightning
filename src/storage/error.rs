
use thiserror::Error;

use crate::command;

/// Errors that can occur during storage operations
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Key not found")]
    #[allow(dead_code)]
    KeyNotFound,
    #[error("Key already exists")]
    #[allow(dead_code)]
    KeyExists,
    #[error("Wrong value type")]
    #[allow(dead_code)]
    WrongType,
    #[error("Value too large")]
    ValueTooLarge,
    #[error("Memory limit exceeded")]
    MemoryLimitExceeded,
    #[error("Internal error: {0}")]
    #[allow(dead_code)]
    InternalError(String),
}

impl From<StorageError> for crate::command::error::CommandError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::KeyNotFound => 
                command::CommandError::InvalidArgument("Key not found".to_string()),
            StorageError::WrongType => command::CommandError::WrongType,
            StorageError::ValueTooLarge => 
                command::CommandError::InvalidArgument("Value too large".to_string()),
            StorageError::MemoryLimitExceeded => 
                command::CommandError::InternalError("Memory limit exceeded".to_string()),
            StorageError::KeyExists => 
                command::CommandError::InvalidArgument("Key already exists".to_string()),
            StorageError::InternalError(msg) => command::CommandError::InternalError(msg),
        }
    }
} 