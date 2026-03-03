use std::fmt;
use std::io;

/// Error type for replication operations
#[derive(Debug)]
#[allow(dead_code)]
pub enum ReplicationError {
    /// I/O error
    Io(io::Error),
    /// Connection error
    Connection(String),
    /// Authentication error
    Authentication(String),
    /// Synchronization error
    Sync(String),
    /// Protocol error
    Protocol(String),
    /// Internal error
    Internal(String),
}

impl fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::Connection(msg) => write!(f, "Connection error: {}", msg),
            Self::Authentication(msg) => write!(f, "Authentication error: {}", msg),
            Self::Sync(msg) => write!(f, "Synchronization error: {}", msg),
            Self::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            Self::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for ReplicationError {}

impl From<io::Error> for ReplicationError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<crate::networking::error::NetworkError> for ReplicationError {
    fn from(err: crate::networking::error::NetworkError) -> Self {
        match err {
            crate::networking::error::NetworkError::Io(e) => Self::Io(e),
            crate::networking::error::NetworkError::Resp(e) => Self::Protocol(e.to_string()),
            crate::networking::error::NetworkError::Command(e) => Self::Protocol(e.to_string()),
            crate::networking::error::NetworkError::Serialization(msg) => Self::Protocol(msg),
            crate::networking::error::NetworkError::ClientDisconnected => {
                Self::Connection("Client disconnected unexpectedly".to_string())
            }
            crate::networking::error::NetworkError::ConnectionClosed => {
                Self::Connection("Connection closed".to_string())
            }
            crate::networking::error::NetworkError::AddressResolution(msg) => Self::Connection(msg),
            crate::networking::error::NetworkError::Internal(msg) => Self::Internal(msg),
            crate::networking::error::NetworkError::Persistence(e) => {
                Self::Internal(format!("Persistence error: {}", e))
            }
        }
    }
}
