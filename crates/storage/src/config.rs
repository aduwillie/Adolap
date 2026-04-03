use serde::{Serialize, Deserialize};
use core::error::AdolapError;
use std::path::Path;
use tokio::fs;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStorageConfig {
  pub row_group_size: usize,
  pub compression: CompressionType,
  pub enable_bloom_filter: bool,
  pub enable_dictionary_encoding: bool,
}

impl Default for TableStorageConfig {
    fn default() -> Self {
        Self {
            row_group_size: 16_384,             // 16kb
            compression: CompressionType::Lz4,
            enable_bloom_filter: true,
            enable_dictionary_encoding: true,
        }
    }
}

impl TableStorageConfig {
    pub async fn load(path: &Path) -> Result<Self, AdolapError> {
        let bytes = fs::read(path).await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| AdolapError::Serialization(format!("Table config error: {}", e)))
    }

    pub async fn save(&self, path: &Path) -> Result<(), AdolapError> {
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| AdolapError::Serialization(format!("Table config error: {}", e)))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(path, bytes).await?;
        Ok(())
    }
}
