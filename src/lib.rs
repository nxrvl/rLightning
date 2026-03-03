/// rLightning - Redis-compatible in-memory key-value store
///
/// This crate provides a high-performance, Redis-compatible in-memory database
/// focused on session management and caching use cases.
// Re-export modules for testing
pub mod cluster;
pub mod command;
pub mod networking;
pub mod persistence;
pub mod pubsub;
pub mod replication;
pub mod scripting;
pub mod security;
pub mod sentinel;
pub mod storage;
pub mod utils;

// Import types commonly used throughout the codebase
pub use command::error::{CommandError, CommandResult};
pub use networking::resp::RespValue;
