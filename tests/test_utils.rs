use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};
use rlightning::security::{SecurityConfig, SecurityManager};
use rlightning::command::handler::CommandHandler;
// Removed unused import: rlightning::persistence::config::AofSyncPolicy
use std::sync::Arc;

pub const DEFAULT_TEST_PORT: u16 = 17000;

/// Sets up a test server with the specified port offset
/// Returns the server address
pub async fn setup_test_server(port_offset: u16) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    setup_test_server_with_optional_security(port_offset, StorageConfig::default(), None).await
}

/// Creates a test server with custom config
pub async fn setup_test_server_with_config(
    port_offset: u16,
    config: StorageConfig,
) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    setup_test_server_with_optional_security(port_offset, config, None).await
}

/// Sets up a test server with specified port offset and security configuration
/// Returns the server address
pub async fn setup_test_server_with_security(
    port_offset: u16,
    storage_config: StorageConfig,
    security_config: SecurityConfig,
) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    setup_test_server_with_optional_security(port_offset, storage_config, Some(security_config)).await
}

/// Helper to set up a test server with optional security
pub async fn setup_test_server_with_optional_security(
    port_offset: u16,
    storage_config: StorageConfig,
    security_config: Option<SecurityConfig>,
) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let port = DEFAULT_TEST_PORT + port_offset;
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    let storage = Arc::new(StorageEngine::new(storage_config));
    let _command_handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

    let mut server_builder = Server::new(addr, Arc::clone(&storage))
        .with_connection_limit(100)
        .with_buffer_size(1024 * 1024); // Use 1MB buffer size for tests (up from 1KB)

    if let Some(sec_conf) = security_config {
        let security_manager = Arc::new(SecurityManager::new(sec_conf));
        server_builder = server_builder.with_security(security_manager);
    }
    
    let server = server_builder;

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    sleep(Duration::from_millis(200)).await;

    Ok(addr)
}

/// Creates a new Redis client connected to the specified address
pub async fn create_client(addr: SocketAddr) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    let client = match Client::connect(addr).await {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Failed to connect to server: {}", e);
            sleep(Duration::from_millis(500)).await;
            Client::connect(addr).await?
        }
    };
    Ok(client)
}

/// Waits for the specified duration
pub async fn wait_ms(ms: u64) {
    sleep(Duration::from_millis(ms)).await;
} 