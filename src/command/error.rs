use thiserror::Error;

/// Errors that can occur when executing commands
#[derive(Debug, Error, PartialEq)]
#[allow(dead_code)]
pub enum CommandError {
    /// Not enough or too many arguments supplied
    WrongNumberOfArguments,
    /// An operation was performed on the wrong type of value
    WrongType,
    /// An invalid argument was provided
    InvalidArgument(String),
    /// An unknown command was received
    UnknownCommand(String),
    /// Tried to compute an arithmetic on a non-numeric value
    NotANumber,
    /// An integer overflow occurred
    IntegerOverflow,
    /// A key was not found
    KeyNotFound,
    /// An internal error occurred
    InternalError(String),
    /// A storage engine error occurred
    StorageError(String),
    /// Permission denied by ACL
    PermissionDenied(String),
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandError::WrongNumberOfArguments => {
                write!(f, "ERR wrong number of arguments")
            }
            CommandError::WrongType => {
                write!(f, "WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            CommandError::InvalidArgument(msg) => {
                write!(f, "ERR {}", msg)
            }
            CommandError::UnknownCommand(cmd) => {
                write!(f, "ERR unknown command '{}'", cmd.to_lowercase())
            }
            CommandError::NotANumber => {
                write!(f, "ERR value is not an integer or out of range")
            }
            CommandError::IntegerOverflow => {
                write!(f, "ERR integer overflow")
            }
            CommandError::KeyNotFound => {
                write!(f, "ERR no such key")
            }
            CommandError::InternalError(msg) => {
                write!(f, "ERR {}", msg)
            }
            CommandError::StorageError(msg) => {
                write!(f, "ERR {}", msg)
            }
            CommandError::PermissionDenied(msg) => {
                write!(f, "NOPERM {}", msg)
            }
        }
    }
}

/// Result type for command execution
pub type CommandResult = Result<crate::networking::resp::RespValue, CommandError>; 