use serde::{Serialize, Deserialize};

// Server configuration for Adolap
// 
// This module defines the `ServerConfig` struct, which holds configuration settings for the Adolap server, such as the listening address and data storage path. 
// It also provides a default implementation and unit tests to ensure the default values are correct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
  pub listen_addr: String,
  pub data_path: String,
}
impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig { 
          listen_addr: String::from("0.0.0.0:5999"), 
          data_path: String::from("./data") 
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
    }
}