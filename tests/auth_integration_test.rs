pub mod auth_integration_tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    use rlightning::security::{SecurityConfig, SecurityManager};
    use rlightning::storage::engine::{StorageConfig, StorageEngine};
    use rlightning::networking::client::Client;
    use rlightning::networking::server::Server;
    use rlightning::networking::resp::RespValue;

    const TEST_PASSWORD: &str = "testpassword123";
    const DEFAULT_TEST_PORT: u16 = 17000;

    fn create_auth_security_config(require_auth: bool) -> SecurityConfig {
        SecurityConfig {
            require_auth,
            password: TEST_PASSWORD.to_string(),
            acl_file: None,
        }
    }
    
    /// Sets up a test server with the specified port offset and security configuration
    async fn setup_test_server_with_security(
        port_offset: u16,
        storage_config: StorageConfig,
        security_config: SecurityConfig,
    ) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
        setup_test_server_with_optional_security(port_offset, storage_config, Some(security_config)).await
    }
    
    /// Helper to set up a test server with optional security
    async fn setup_test_server_with_optional_security(
        port_offset: u16,
        storage_config: StorageConfig,
        security_config: Option<SecurityConfig>,
    ) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
        let port = DEFAULT_TEST_PORT + port_offset;
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
        let storage = Arc::new(StorageEngine::new(storage_config));
        
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
    async fn create_client(addr: SocketAddr) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
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

    #[tokio::test]
    async fn test_auth_required_correct_password() {
        let security_config = create_auth_security_config(true);
        let storage_config = StorageConfig::default();
        let addr = setup_test_server_with_security(0, storage_config, security_config)
            .await
            .expect("Failed to start server");

        let mut client = create_client(addr).await.expect("Failed to create client");

        // Send AUTH command
        let auth_response = client
            .send_command_str("AUTH", &[TEST_PASSWORD])
            .await
            .expect("Failed to send AUTH command");
        assert_eq!(auth_response, RespValue::SimpleString("OK".to_string()));

        // Send PING command
        let ping_response = client
            .send_command_str("PING", &[])
            .await
            .expect("Failed to send PING command");
        assert_eq!(ping_response, RespValue::SimpleString("PONG".to_string()));
    }

    #[tokio::test]
    async fn test_auth_required_incorrect_password() {
        let security_config = create_auth_security_config(true);
        let storage_config = StorageConfig::default();
        let addr = setup_test_server_with_security(1, storage_config, security_config) // Use different port offset
            .await
            .expect("Failed to start server");

        let mut client = create_client(addr).await.expect("Failed to create client");

        // Send AUTH command with incorrect password
        let auth_response = client
            .send_command_str("AUTH", &["wrongpassword"])
            .await
            .expect("Failed to send AUTH command");
        // Redis typically returns an error for wrong password, but our AUTH command itself returns OK.
        // The check happens when SecurityManager::authenticate is called.
        // The current server logic might return OK for AUTH, but then deny subsequent commands.
        // Let's assume for now AUTH itself might return an error or OK, but subsequent commands will fail.
        // Based on src/networking/server.rs, the AUTH command handler itself returns OK.
        // The actual authentication happens in SecurityManager.authenticate.
        // If authenticate fails, the client is not marked as authenticated.
        assert_eq!(auth_response, RespValue::SimpleString("OK".to_string()));


        // Send PING command - should fail
        let ping_response = client
            .send_command_str("PING", &[])
            .await
            .expect("Failed to send PING command");
        
        // Expect "NOAUTH Authentication required."
        // The error message comes from server.rs: Self::send_error(&mut socket, error_msg.to_string(), &client_addr_str).await?;
        // Where error_msg = "NOAUTH Authentication required.";
        // This is wrapped in RespValue::Error.
        assert_eq!(
            ping_response,
            RespValue::Error("ERR NOAUTH Authentication required.".to_string())
        );
    }

    #[tokio::test]
    async fn test_auth_required_no_auth_attempt() {
        let security_config = create_auth_security_config(true);
        let storage_config = StorageConfig::default();
        let addr = setup_test_server_with_security(2, storage_config, security_config) // Use different port offset
            .await
            .expect("Failed to start server");

        let mut client = create_client(addr).await.expect("Failed to create client");

        // Send PING command without authenticating
        let ping_response = client
            .send_command_str("PING", &[])
            .await
            .expect("Failed to send PING command");
        assert_eq!(
            ping_response,
            RespValue::Error("ERR NOAUTH Authentication required.".to_string())
        );
    }

    #[tokio::test]
    async fn test_auth_not_required() {
        // Use the more general setup function with optional security
        let storage_config = StorageConfig::default();
        let addr = setup_test_server_with_optional_security(3, storage_config, None) // No security config
            .await
            .expect("Failed to start server");

        let mut client = create_client(addr).await.expect("Failed to create client");

        // Send PING command - should succeed
        let ping_response = client
            .send_command_str("PING", &[])
            .await
            .expect("Failed to send PING command");
        assert_eq!(ping_response, RespValue::SimpleString("PONG".to_string()));

        // Send AUTH command - should still return OK (as per current auth command logic)
        let auth_response = client
            .send_command_str("AUTH", &["anypassword"])
            .await
            .expect("Failed to send AUTH command");
        assert_eq!(auth_response, RespValue::SimpleString("OK".to_string()));

        // Send PING again to ensure state is fine
        let ping_response_after_auth = client
            .send_command_str("PING", &[])
            .await
            .expect("Failed to send PING command");
        assert_eq!(ping_response_after_auth, RespValue::SimpleString("PONG".to_string()));
    }
    
    #[tokio::test]
    async fn test_auth_wrong_number_of_arguments() {
        let storage_config = StorageConfig::default();
        // Server started without security for simplicity, as arg check is before auth logic
        let addr = setup_test_server_with_optional_security(4, storage_config, None)
            .await
            .expect("Failed to start server");

        let mut client = create_client(addr).await.expect("Failed to create client");

        // AUTH with no arguments
        let response_no_args = client
            .send_command_str("AUTH", &[])
            .await
            .expect("Failed to send AUTH command with no args");
        assert_eq!(
            response_no_args,
            RespValue::Error("ERR wrong number of arguments".to_string())
        );

        // AUTH with too many arguments
        let response_too_many_args = client
            .send_command_str("AUTH", &["pass1", "pass2"])
            .await
            .expect("Failed to send AUTH command with too many args");
        assert_eq!(
            response_too_many_args,
            RespValue::Error("ERR wrong number of arguments".to_string())
        );
    }
}

// Need to add this to tests/lib.rs
// pub mod auth_integration_test; 