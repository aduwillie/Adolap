use core::error::AdolapError;
use std::path::Path;
use tokio::fs;

use crate::{
    column_reader::ColumnChunkReader, naming::ROW_GROUP_METADATA_FILE_NAME, record_batch::RecordBatch, row_group::RowGroupMetadata, schema::TableSchema
};

pub struct RowGroupReader<'a> {
    pub schema: &'a TableSchema,
    pub projected_indices: Option<Vec<usize>>,
}

impl<'a> RowGroupReader<'a> {
    pub fn new(schema: &'a TableSchema, projected_indices: Option<Vec<usize>>) -> Self {
        Self { schema, projected_indices }
    }

    pub async fn read_row_group(
        &self,
        row_group_dir: &Path,
    ) -> Result<RecordBatch, AdolapError> {
        // 1. Load row_group.meta
        let meta_path = row_group_dir.join(ROW_GROUP_METADATA_FILE_NAME);
        let meta_bytes = fs::read(&meta_path).await?;
        let metadata: RowGroupMetadata = postcard::from_bytes(&meta_bytes)
            .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize row group metadata: {}", e)))?;

        let selected_indices = self
            .projected_indices
            .clone()
            .unwrap_or_else(|| (0..self.schema.columns.len()).collect());
        let mut columns = Vec::with_capacity(selected_indices.len());
        let reader = ColumnChunkReader::new();

        // 2. Read each column chunk
        for i in selected_indices.iter().copied() {
            let col_schema = &self.schema.columns[i];
            let descriptor = &metadata.columns[i];

            let values = reader
                .read_column_chunk(row_group_dir, descriptor, col_schema.column_type.clone())
                .await?;

            columns.push(values);
        }

        // 3. Build RecordBatch
        Ok(RecordBatch {
            schema: TableSchema {
                columns: selected_indices
                    .into_iter()
                    .map(|index| self.schema.columns[index].clone())
                    .collect(),
            },
            row_count: metadata.row_count as usize,
            columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RowGroupReader;
    use crate::{
        column::{ColumnInput, ColumnValue, ColumnValues},
        config::{CompressionType, TableStorageConfig},
        row_group_writer::RowGroupWriter,
        schema::{ColumnSchema, ColumnType, TableSchema},
    };
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
                ColumnSchema { name: "name".into(), column_type: ColumnType::Utf8, nullable: false },
            ],
        }
    }

    #[test]
    fn reads_row_group_with_projection() {
        run_async_test(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let row_group_dir = temp_dir.path().join("row_group_000");
            let schema = sample_schema();
            let config = TableStorageConfig {
                compression: CompressionType::None,
                enable_bloom_filter: false,
                enable_dictionary_encoding: false,
                ..TableStorageConfig::default()
            };
            let ids = [1u32, 2u32];
            let names = ["a".to_string(), "b".to_string()];

            RowGroupWriter::new(&config)
                .write_row_group(
                    &row_group_dir,
                    &schema,
                    vec![
                        ColumnInput { values: ColumnValues::U32(&ids), validity: None },
                        ColumnInput { values: ColumnValues::Utf8(&names), validity: None },
                    ],
                )
                .await
                .unwrap();

            let batch = RowGroupReader::new(&schema, Some(vec![1]))
                .read_row_group(&row_group_dir)
                .await
                .unwrap();

            assert_eq!(batch.schema.columns.len(), 1);
            assert_eq!(batch.schema.columns[0].name, "name");
            assert_eq!(batch.to_rows().unwrap(), vec![vec![Some(ColumnValue::Utf8("a".into()))], vec![Some(ColumnValue::Utf8("b".into()))]]);
        });
    }
}
