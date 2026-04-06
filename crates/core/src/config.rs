use serde::{Serialize, Deserialize};

/// Default maximum number of simultaneous client connections.
/// Connections beyond this limit are rejected immediately after accept.
const DEFAULT_MAX_CONNECTIONS: usize = 256;

// Server configuration for Adolap
//
// This module defines the `ServerConfig` struct, which holds configuration
// settings for the Adolap server, such as the listening address, data storage
// path, and the maximum number of allowed simultaneous connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub data_path: String,
    /// Maximum number of simultaneous client connections.
    /// New connections are rejected when the limit is reached.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

fn default_max_connections() -> usize {
    DEFAULT_MAX_CONNECTIONS
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            listen_addr: String::from("0.0.0.0:5999"),
            data_path: String::from("./data"),
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_server_config() {
        let config = ServerConfig::default();
        assert_eq!(config.listen_addr, "0.0.0.0:5999");
        assert_eq!(config.data_path, "./data");
        assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
    }

    #[test]
    fn deserializes_legacy_config_without_max_connections() {
        let json = r#"{"listen_addr": "0.0.0.0:5999", "data_path": "./data"}"#;
        let config: ServerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
    }

    #[test]
    fn deserializes_config_with_custom_max_connections() {
        let json = r#"{"listen_addr": "127.0.0.1:6000", "data_path": "/var/db", "max_connections": 50}"#;
        let config: ServerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_connections, 50);
    }
}