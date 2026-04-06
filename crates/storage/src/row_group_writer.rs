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

  #[cfg(test)]
  mod tests {
    use super::RowGroupWriter;
    use crate::{
      column::{ColumnInput, ColumnValues},
      config::{CompressionType, TableStorageConfig},
      schema::{ColumnSchema, ColumnType, TableSchema},
    };
    use core::error::AdolapError;
    use tokio::runtime::Runtime;

    fn run_async_test<F, T>(future: F) -> T
    where
      F: std::future::Future<Output = T>,
    {
      Runtime::new().unwrap().block_on(future)
    }

    fn sample_schema() -> TableSchema {
      TableSchema {
        columns: vec![
          ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false },
          ColumnSchema { name: "active".into(), column_type: ColumnType::Bool, nullable: false },
        ],
      }
    }

    #[test]
    fn rejects_mismatched_column_count() {
      run_async_test(async {
        let temp_dir = tempfile::tempdir().unwrap();
        let schema = sample_schema();
        let config = TableStorageConfig::default();
        let ids = [1u32, 2u32];

        match RowGroupWriter::new(&config)
          .write_row_group(
            temp_dir.path(),
            &schema,
            vec![ColumnInput { values: ColumnValues::U32(&ids), validity: None }],
          )
          .await
          .unwrap_err()
        {
          AdolapError::StorageError(message) => assert!(message.contains("does not match schema")),
          other => panic!("expected storage error, got {:?}", other),
        }
      });
    }

    #[test]
    fn writes_metadata_for_valid_row_group() {
      run_async_test(async {
        let temp_dir = tempfile::tempdir().unwrap();
        let schema = sample_schema();
        let config = TableStorageConfig {
          compression: CompressionType::None,
          enable_bloom_filter: false,
          enable_dictionary_encoding: false,
          ..TableStorageConfig::default()
        };
        let ids = [1u32, 2u32];
        let flags = [true, false];

        let metadata = RowGroupWriter::new(&config)
          .write_row_group(
            temp_dir.path(),
            &schema,
            vec![
              ColumnInput { values: ColumnValues::U32(&ids), validity: None },
              ColumnInput { values: ColumnValues::Bool(&flags), validity: None },
            ],
          )
          .await
          .unwrap();

        assert_eq!(metadata.row_count, 2);
        assert_eq!(metadata.columns.len(), 2);
        assert!(temp_dir.path().join("row_group.meta").exists());
      });
    }
  }
