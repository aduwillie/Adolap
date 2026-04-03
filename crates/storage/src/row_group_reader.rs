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
