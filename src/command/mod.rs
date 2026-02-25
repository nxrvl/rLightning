pub mod commands;
pub mod error;
pub mod handler;
pub mod parser;
pub mod transaction;
pub mod types;
pub mod utils;

// Re-export the error types for use in other modules
pub use self::error::{CommandError, CommandResult};

/// Represents a parsed Redis command
#[derive(Debug, Clone)]
pub struct Command {
    /// The command name
    pub name: String,
    /// The command arguments
    pub args: Vec<Vec<u8>>,
}