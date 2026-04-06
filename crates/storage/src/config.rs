use serde::{Serialize, Deserialize};
use core::error::AdolapError;
use std::path::Path;
use tokio::fs;

const DEFAULT_ROW_GROUP_SIZE: usize = 16_384;
const DEFAULT_COMPACTION_SEGMENT_THRESHOLD: usize = 8;
const DEFAULT_COMPACTION_ROW_GROUP_THRESHOLD: usize = 8;
const DEFAULT_BACKGROUND_COMPACTION_INTERVAL_SECONDS: u64 = 60;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TableStorageConfig {
  pub row_group_size: usize,
  pub compression: CompressionType,
  pub enable_bloom_filter: bool,
  pub enable_dictionary_encoding: bool,
  pub compaction_segment_threshold: usize,
  pub compaction_row_group_threshold: usize,
  pub enable_background_compaction: bool,
  pub background_compaction_interval_seconds: u64,
}

impl Default for TableStorageConfig {
    fn default() -> Self {
        Self {
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            compression: CompressionType::Lz4,
            enable_bloom_filter: true,
            enable_dictionary_encoding: true,
            compaction_segment_threshold: DEFAULT_COMPACTION_SEGMENT_THRESHOLD,
            compaction_row_group_threshold: DEFAULT_COMPACTION_ROW_GROUP_THRESHOLD,
            enable_background_compaction: true,
            background_compaction_interval_seconds: DEFAULT_BACKGROUND_COMPACTION_INTERVAL_SECONDS,
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

#[cfg(test)]
mod tests {
    use super::{CompressionType, TableStorageConfig};

    #[test]
    fn default_table_storage_config_enables_compaction_defaults() {
        let config = TableStorageConfig::default();
        assert_eq!(config.row_group_size, 16_384);
        assert!(matches!(config.compression, CompressionType::Lz4));
        assert_eq!(config.compaction_segment_threshold, 8);
        assert_eq!(config.compaction_row_group_threshold, 8);
        assert!(config.enable_background_compaction);
        assert_eq!(config.background_compaction_interval_seconds, 60);
    }

    #[test]
    fn deserializing_legacy_config_falls_back_to_compaction_defaults() {
        let legacy = r#"{
  "row_group_size": 1024,
  "compression": "None",
  "enable_bloom_filter": false,
  "enable_dictionary_encoding": false
}"#;

        let config: TableStorageConfig = serde_json::from_str(legacy).unwrap();
        assert_eq!(config.row_group_size, 1024);
        assert!(matches!(config.compression, CompressionType::None));
        assert_eq!(config.compaction_segment_threshold, 8);
        assert_eq!(config.compaction_row_group_threshold, 8);
        assert!(config.enable_background_compaction);
        assert_eq!(config.background_compaction_interval_seconds, 60);
    }
}
