use std::io;
use thiserror::Error;

use crate::command::CommandError;
use crate::networking::resp::RespError;

/// Errors that can occur during networking operations.
#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum NetworkError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("RESP protocol error: {0}")]
    Resp(#[from] RespError),

    #[error("Command processing error: {0}")]
    Command(#[from] CommandError),

    #[error("Persistence error: {0}")]
    Persistence(#[from] crate::persistence::error::PersistenceError),

    #[error("Failed to serialize response: {0}")]
    Serialization(String),

    #[error("Client disconnected unexpectedly")]
    ClientDisconnected,

    #[error("Connection closed by request")]
    ConnectionClosed,

    #[error("Could not resolve address: {0}")]
    AddressResolution(String),

    #[error("Internal network error: {0}")]
    Internal(String),
}
