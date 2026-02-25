pub mod acl;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tracing::{debug, info, warn};

pub use acl::AclManager;

/// Configuration for the security module
#[derive(Debug, Clone, Default)]
pub struct SecurityConfig {
    /// Whether authentication is required
    pub require_auth: bool,
    /// Password for authentication
    pub password: String,
    /// Path to ACL file (optional)
    #[allow(dead_code)]
    pub acl_file: Option<PathBuf>,
}

/// Security manager that handles authentication and ACL
pub struct SecurityManager {
    config: SecurityConfig,
    /// Map of client addresses to authentication status (legacy, kept for backward compat)
    authenticated_clients: Arc<RwLock<HashMap<String, bool>>>,
    /// ACL manager for fine-grained access control
    acl: AclManager,
}

impl SecurityManager {
    /// Create a new security manager with the given configuration
    pub fn new(config: SecurityConfig) -> Self {
        info!(
            require_auth = config.require_auth,
            "Initializing security manager"
        );

        let password = if config.password.is_empty() {
            None
        } else {
            Some(config.password.as_str())
        };
        let acl = AclManager::new(config.require_auth, password);

        Self {
            config,
            authenticated_clients: Arc::new(RwLock::new(HashMap::new())),
            acl,
        }
    }

    /// Check if authentication is required
    pub fn require_auth(&self) -> bool {
        self.config.require_auth
    }

    /// Get a reference to the ACL manager
    pub fn acl(&self) -> &AclManager {
        &self.acl
    }

    /// Authenticate a client with password only (default user)
    pub fn authenticate(&self, client_addr: &str, password: &[u8]) -> bool {
        if !self.require_auth() {
            return true;
        }

        let password_str = match std::str::from_utf8(password) {
            Ok(s) => s,
            Err(_) => {
                warn!(client_addr = %client_addr, "Invalid UTF-8 in password");
                return false;
            }
        };

        // Use ACL manager to authenticate as default user
        match self.acl.authenticate(client_addr, "default", password_str) {
            Ok(_) => {
                if let Ok(mut clients) = self.authenticated_clients.write() {
                    clients.insert(client_addr.to_string(), true);
                }
                true
            }
            Err(_) => {
                warn!(client_addr = %client_addr, "Authentication failed");
                false
            }
        }
    }

    /// Authenticate a client with username and password
    pub fn authenticate_with_username(
        &self,
        client_addr: &str,
        username: &str,
        password: &str,
    ) -> Result<String, String> {
        let result = self.acl.authenticate(client_addr, username, password)?;
        if let Ok(mut clients) = self.authenticated_clients.write() {
            clients.insert(client_addr.to_string(), true);
        }
        Ok(result)
    }

    /// Check if a client is authenticated
    pub fn is_authenticated(&self, client_addr: &str) -> bool {
        if !self.require_auth() {
            return true;
        }
        self.acl.is_authenticated(client_addr)
    }

    /// Remove a client from the authenticated list (e.g., when connection is closed)
    pub fn remove_client(&self, client_addr: &str) {
        if let Ok(mut clients) = self.authenticated_clients.write() {
            clients.remove(client_addr);
            debug!(client_addr = %client_addr, "Client removed from authentication list");
        }
        self.acl.remove_client(client_addr);
    }

    /// Check if a command is allowed for the given client (ACL check)
    pub fn check_command_permission(&self, client_addr: &str, cmd: &str) -> bool {
        self.acl.check_command_permission(client_addr, cmd)
    }

    /// Check if a key access is allowed for the given client (ACL check)
    pub fn check_key_permission(&self, client_addr: &str, key: &[u8]) -> bool {
        self.acl.check_key_permission(client_addr, key)
    }

    /// Check if a channel access is allowed for the given client (ACL check)
    pub fn check_channel_permission(&self, client_addr: &str, channel: &[u8]) -> bool {
        self.acl.check_channel_permission(client_addr, channel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authentication() {
        let config = SecurityConfig {
            require_auth: true,
            password: "test_password".to_string(),
            acl_file: None,
        };

        let security = SecurityManager::new(config);

        // Test valid authentication
        assert!(security.authenticate("127.0.0.1:1234", b"test_password"));
        assert!(security.is_authenticated("127.0.0.1:1234"));

        // Test invalid authentication
        assert!(!security.authenticate("127.0.0.1:5678", b"wrong_password"));
        assert!(!security.is_authenticated("127.0.0.1:5678"));

        // Test client removal
        security.remove_client("127.0.0.1:1234");
        assert!(!security.is_authenticated("127.0.0.1:1234"));
    }

    #[test]
    fn test_auth_not_required() {
        let config = SecurityConfig {
            require_auth: false,
            password: "test_password".to_string(),
            acl_file: None,
        };

        let security = SecurityManager::new(config);

        // When auth is not required, all clients should be considered authenticated
        assert!(security.is_authenticated("127.0.0.1:1234"));
        assert!(security.authenticate("127.0.0.1:5678", b"wrong_password"));
        assert!(security.is_authenticated("127.0.0.1:5678"));
    }

    #[test]
    fn test_authenticate_with_username() {
        let config = SecurityConfig {
            require_auth: true,
            password: "defaultpass".to_string(),
            acl_file: None,
        };

        let security = SecurityManager::new(config);

        // Create a user via ACL
        security
            .acl()
            .handle_setuser(&[
                b"alice".to_vec(),
                b"on".to_vec(),
                b">alicepass".to_vec(),
                b"+@all".to_vec(),
                b"~*".to_vec(),
                b"&*".to_vec(),
            ])
            .unwrap();

        // Auth with username
        let result = security.authenticate_with_username("c1", "alice", "alicepass");
        assert!(result.is_ok());
        assert!(security.is_authenticated("c1"));

        // Auth with wrong password
        let result = security.authenticate_with_username("c2", "alice", "wrong");
        assert!(result.is_err());
        assert!(!security.is_authenticated("c2"));

        // Auth default user with password
        let result = security.authenticate_with_username("c3", "default", "defaultpass");
        assert!(result.is_ok());
    }

    #[test]
    fn test_acl_command_permission() {
        let config = SecurityConfig {
            require_auth: true,
            password: "pass".to_string(),
            acl_file: None,
        };

        let security = SecurityManager::new(config);

        // Create restricted user
        security
            .acl()
            .handle_setuser(&[
                b"reader".to_vec(),
                b"on".to_vec(),
                b">readerpass".to_vec(),
                b"+@read".to_vec(),
                b"~*".to_vec(),
            ])
            .unwrap();

        // Auth as reader
        security
            .authenticate_with_username("c1", "reader", "readerpass")
            .unwrap();

        // Can read
        assert!(security.check_command_permission("c1", "get"));
        // Cannot write
        assert!(!security.check_command_permission("c1", "set"));
    }

    #[test]
    fn test_acl_key_permission() {
        let config = SecurityConfig {
            require_auth: true,
            password: "pass".to_string(),
            acl_file: None,
        };

        let security = SecurityManager::new(config);

        // Create user with limited key access
        security
            .acl()
            .handle_setuser(&[
                b"app".to_vec(),
                b"on".to_vec(),
                b">apppass".to_vec(),
                b"+@all".to_vec(),
                b"~app:*".to_vec(),
            ])
            .unwrap();

        security
            .authenticate_with_username("c1", "app", "apppass")
            .unwrap();

        assert!(security.check_key_permission("c1", b"app:data"));
        assert!(!security.check_key_permission("c1", b"system:data"));
    }
}
