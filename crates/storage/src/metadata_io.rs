use core::error::AdolapError;
use std::path::Path;
use crate::{metadata::SegmentMetadata, naming::{SEGMENT_META_JSON_FILE_NAME, SEGMENT_METADATA_FILE_NAME}};
use tokio::fs;
use postcard::from_bytes;

/// Write the segment metadata to disk in both binary and JSON formats (for debugging and testing purposes).
/// 
/// Directory layout:
/// segment_<id>/
/// |-- segment.meta (binary metadata file)
/// |-- segment.meta.json (JSON metadata file)
pub async fn write_segment_metdata(
  segment_dir: &Path,
  metadata: &SegmentMetadata,
  write_json_debug: bool,
) -> Result<(), AdolapError> {
  // Ensure all segment directories exist
  fs::create_dir_all(segment_dir).await?;

  // 1. Write binary metadata file
  let binary_metadata_path = segment_dir.join(SEGMENT_METADATA_FILE_NAME);
  let binary_bytes = postcard::to_allocvec(metadata)
    .map_err(|e| AdolapError::Serialization(format!("Cannot serialize metadata: {}", e)))?;

  fs::write(&binary_metadata_path, binary_bytes).await?;

  // 2. Optionally write JSON metadata file for debugging
  if write_json_debug {
    let json_metadata_path = segment_dir.join(SEGMENT_META_JSON_FILE_NAME);
    let json_string = serde_json::to_string_pretty(metadata)
      .map_err(|e| AdolapError::Serialization(format!("Cannot serialize metadata to JSON: {}", e)))?;

    fs::write(&json_metadata_path, json_string).await?;
  }

  Ok(())
}

/// Read the segment metadata from disk.
/// This will read the binary metadata file and deserialize it into a SegmentMetadata struct.
/// Results are cached because segment metadata is immutable once written.
pub async fn read_segment_metadata(segment_dir: &Path) -> Result<SegmentMetadata, AdolapError> {
  let binary_metadata_path = segment_dir.join(SEGMENT_METADATA_FILE_NAME);

  let cache = crate::read_cache::global_cache();
  if let Some(cached) = cache.get_segment_metadata(&binary_metadata_path) {
      return Ok(cached);
  }

  let binary_bytes = fs::read(&binary_metadata_path).await
    .map_err(|e| AdolapError::Serialization(format!("Cannot read metadata file: {}", e)))?;

  let metadata: SegmentMetadata = from_bytes(&binary_bytes)
    .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize metadata: {}", e)))?;

  cache.put_segment_metadata(binary_metadata_path, metadata.clone());
  Ok(metadata)
}

#[cfg(test)]
mod tests {
  use super::{read_segment_metadata, write_segment_metdata};
  use crate::{config::TableStorageConfig, metadata::SegmentMetadata, naming::{SEGMENT_META_JSON_FILE_NAME, SEGMENT_METADATA_FILE_NAME}};
  use core::id::TableId;
  use tokio::runtime::Runtime;

  fn run_async_test<F, T>(future: F) -> T
  where
      F: std::future::Future<Output = T>,
  {
      Runtime::new().unwrap().block_on(future)
  }

  #[test]
  fn writes_and_reads_segment_metadata_with_optional_json() {
      run_async_test(async {
          let temp_dir = tempfile::tempdir().unwrap();
          let metadata = SegmentMetadata::new(TableId { value: 99 }, TableStorageConfig::default());

          write_segment_metdata(temp_dir.path(), &metadata, true).await.unwrap();
          let read_back = read_segment_metadata(temp_dir.path()).await.unwrap();

          assert_eq!(read_back.table_id.value, 99);
          assert!(temp_dir.path().join(SEGMENT_METADATA_FILE_NAME).exists());
          assert!(temp_dir.path().join(SEGMENT_META_JSON_FILE_NAME).exists());
      });
  }
}