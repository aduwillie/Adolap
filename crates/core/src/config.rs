use serde::{Serialize, Deserialize};

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
