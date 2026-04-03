use core::{error::AdolapError, id::TableId};
use std::path::Path;
use tokio::fs;

use crate::{
  column::ColumnInput, config::TableStorageConfig, metadata::SegmentMetadata, naming::{row_group_dir_name, SEGMENT_META_JSON_FILE_NAME, SEGMENT_METADATA_FILE_NAME}, row_group_writer::RowGroupWriter, schema::TableSchema
};

pub struct SegmentWriter<'a> {
  pub table_id: TableId,
  pub storage_config: &'a TableStorageConfig,
  pub schema: &'a TableSchema,
}

impl<'a> SegmentWriter<'a> {
  pub fn new(table_id: TableId, storage_config: &'a TableStorageConfig, schema: &'a TableSchema) -> Self {
    Self {
      table_id,
      storage_config,
      schema,
    }
  }

  pub async fn write_segment(
    &self, 
    segment_dir: &Path, 
    row_groups: Vec<Vec<ColumnInput<'_>>>) -> Result<SegmentMetadata, AdolapError> {
      // Ensure that segment directory exists
      fs::create_dir_all(segment_dir).await
        .map_err(|e| AdolapError::StorageError(format!("Cannot create segment directory: {}", e)))?;

      let mut segment_metadata = SegmentMetadata::new(self.table_id, self.storage_config.clone());

      for (row_group_index, row_group) in row_groups.into_iter().enumerate() {
        let row_group_dir = segment_dir.join(row_group_dir_name(row_group_index as u64));
        let row_group_writer = RowGroupWriter::new(self.storage_config);

        let row_group_metadata = row_group_writer.write_row_group(&row_group_dir, self.schema, row_group).await?;

        segment_metadata.total_rows += row_group_metadata.row_count;
        segment_metadata.total_size_bytes += row_group_metadata.total_compressed_size_bytes;
        segment_metadata.row_groups.push(row_group_metadata);
      }

      // Write segment metadata to disk
      let metadata_file_path = segment_dir.join(SEGMENT_METADATA_FILE_NAME);
      let metadata_bytes = postcard::to_allocvec(&segment_metadata)
        .map_err(|e| AdolapError::Serialization(format!("Cannot serialize segment metadata: {}", e)))?;

      fs::write(&metadata_file_path, metadata_bytes)
        .await
        .map_err(|e| AdolapError::StorageError(format!("Cannot write segment metadata: {}", e)))?;
    
    // Also write JSON metadata for debugging and testing purposes
      let metadata_json_file_path = segment_dir.join(SEGMENT_META_JSON_FILE_NAME);
      let metadata_json_bytes = serde_json::to_vec_pretty(&segment_metadata)
        .map_err(|e| AdolapError::Serialization(format!("Cannot serialize segment metadata to JSON: {}", e)))?;

      fs::write(&metadata_json_file_path, metadata_json_bytes)
        .await
        .map_err(|e| AdolapError::StorageError(format!("Cannot write segment metadata (JSON): {}", e)))?;

      Ok(segment_metadata)
    }
}
