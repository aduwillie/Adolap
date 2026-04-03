use core::{error::AdolapError, types::{ByteCount, RowCount}};
use std::path::Path;
use tokio::fs;

use crate::{
  column::{ColumnInput, ColumnValues}, 
  column_writer::ColumnChunkWriter, 
  config::TableStorageConfig, 
  naming::ROW_GROUP_METADATA_FILE_NAME, 
  row_group::RowGroupMetadata, 
  schema::TableSchema
};

pub struct RowGroupWriter<'a> {
  pub storage_config: &'a TableStorageConfig,
}

impl<'a> RowGroupWriter<'a> {
  pub fn new(storage_config: &'a TableStorageConfig) -> Self {
    Self { storage_config }
  }

  pub async fn write_row_group(
    &self, 
    row_group_dir: &Path, 
    schema: &TableSchema,
    columns: Vec<ColumnInput<'_>>) -> Result<RowGroupMetadata, AdolapError> {
    // Validate columns length matches schema
    if columns.len() != schema.columns.len() {
      return Err(AdolapError::StorageError(format!(
        "Number of columns ({}) does not match schema ({})", 
        columns.len(), 
        schema.columns.len()
      )));
    }

    // Ensure directories exists
    fs::create_dir_all(row_group_dir).await
      .map_err(|e| AdolapError::StorageError(format!("Cannot create row group directory: {}", e)))?;

    let mut column_descriptors = Vec::with_capacity(columns.len());
    for (column_index, column) in columns.iter().enumerate() {
      let column_schema = &schema.columns[column_index];
      let column_writer = ColumnChunkWriter::new(column_index, self.storage_config, column_schema.column_type.clone());
      let column_descriptor = match column.values {
        ColumnValues::Utf8(data) => column_writer.write_utf8_column(row_group_dir, data, column.validity).await?,
        ColumnValues::I32(data) => column_writer.write_i32_column(row_group_dir, data, column.validity).await?,
        ColumnValues::U32(data) => column_writer.write_u32_column(row_group_dir, data, column.validity).await?,
        ColumnValues::Bool(data) => column_writer.write_bool_column(row_group_dir, data, column.validity).await?,
      };
      column_descriptors.push(column_descriptor);
    }

     let row_count = columns.first().map(|col| match &col.values {
      ColumnValues::Utf8(data) => data.len(),
      ColumnValues::I32(data) => data.len(),
      ColumnValues::U32(data) => data.len(),
      ColumnValues::Bool(data) => data.len(),
    }).unwrap_or(0) as RowCount;

    let total_compressed_size_bytes: ByteCount = column_descriptors.iter().map(|col| col.compressed_size).sum();
    let total_uncompressed_size_bytes: ByteCount = column_descriptors.iter().map(|col| col.uncompressed_size).sum();
    let metadata = RowGroupMetadata { 
      row_count, 
      columns: column_descriptors,
      total_compressed_size_bytes,
      total_uncompressed_size_bytes,
    };

    let metadata_bytes = postcard::to_allocvec(&metadata)
      .map_err(|e| AdolapError::Serialization(format!("Cannot serialize row group metadata: {}", e)))?;

    // Write the row group metadata to disk (e.g. row_group.meta)
    let metadata_path = row_group_dir.join(ROW_GROUP_METADATA_FILE_NAME);
    fs::write(&metadata_path, metadata_bytes).await?;

    Ok(metadata)
  }
}
