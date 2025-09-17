use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tracing::{debug, info, warn};

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

/// Security manager that handles authentication
pub struct SecurityManager {
    config: SecurityConfig,
    /// Map of client addresses to authentication status
    authenticated_clients: Arc<RwLock<HashMap<String, bool>>>,
}

impl SecurityManager {
    /// Create a new security manager with the given configuration
    pub fn new(config: SecurityConfig) -> Self {
        info!(
            require_auth = config.require_auth,
            "Initializing security manager"
        );
        Self {
            config,
            authenticated_clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if authentication is required
    pub fn require_auth(&self) -> bool {
        self.config.require_auth
    }

    /// Authenticate a client with the given password
    pub fn authenticate(&self, client_addr: &str, password: &[u8]) -> bool {
        if !self.require_auth() {
            // Authentication not required, always return true
            return true;
        }

        let password_str = match std::str::from_utf8(password) {
            Ok(s) => s,
            Err(_) => {
                warn!(client_addr = %client_addr, "Invalid UTF-8 in password");
                return false;
            }
        };

        let is_valid = password_str == self.config.password;
        
        if is_valid {
            // Update authentication status
            if let Ok(mut clients) = self.authenticated_clients.write() {
                clients.insert(client_addr.to_string(), true);
                debug!(client_addr = %client_addr, "Client authenticated");
            }
        } else {
            warn!(client_addr = %client_addr, "Authentication failed");
        }

        is_valid
    }

    /// Check if a client is authenticated
    pub fn is_authenticated(&self, client_addr: &str) -> bool {
        if !self.require_auth() {
            // Authentication not required, always return true
            return true;
        }

        // Check if client is in the authenticated list
        if let Ok(clients) = self.authenticated_clients.read() {
            clients.get(client_addr).copied().unwrap_or(false)
        } else {
            false
        }
    }

    /// Remove a client from the authenticated list (e.g., when connection is closed)
    pub fn remove_client(&self, client_addr: &str) {
        if let Ok(mut clients) = self.authenticated_clients.write() {
            clients.remove(client_addr);
            debug!(client_addr = %client_addr, "Client removed from authentication list");
        }
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
} 