use std::io;
use thiserror::Error;

/// Errors that can occur during persistence operations
#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum PersistenceError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Error during serialization or deserialization
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// File is corrupted
    #[error("Corrupted file: {0}")]
    CorruptedFile(String),

    /// CRC validation failed
    #[error("CRC validation failed: {0}")]
    CrcValidationFailed(String),

    /// Directory creation failed
    #[error("Failed to create directory: {0}")]
    DirectoryCreationFailed(String),

    /// Atomic rename failed
    #[error("Atomic rename failed: {0}")]
    AtomicRenameFailed(String),

    /// Background task failed
    #[error("Background task failed: {0}")]
    BackgroundTaskFailed(String),

    /// AOF rewriting failed
    #[error("AOF rewriting failed: {0}")]
    AofRewriteFailed(String),
    
    /// RDB snapshot creation failed
    #[error("RDB snapshot creation failed: {0}")]
    RdbSnapshotFailed(String),
    
    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
    
    /// Other error
    #[error("Persistence error: {0}")]
    Other(String),
} 