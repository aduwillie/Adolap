use core::error::AdolapError;

use storage::{record_batch::RecordBatch, schema::TableSchema};

pub fn project(record_batch: &RecordBatch, columns: &[&str]) -> Result<RecordBatch, AdolapError> {
  let mut projected_columns = Vec::with_capacity(columns.len());
  let mut projected_schema_columns = Vec::with_capacity(columns.len());

  for &name in columns {
    let index = record_batch.column_index(name)
      .ok_or_else(|| AdolapError::StorageError(format!("Column '{}' not found in record batch schema", name)))?;

    projected_columns.push(record_batch.columns[index].clone());
    projected_schema_columns.push(record_batch.schema.columns[index].clone());
  }

  let projected_schema = TableSchema {
    columns: projected_schema_columns,
  };

  Ok(RecordBatch {
    schema: projected_schema,
    columns: projected_columns,
    row_count: record_batch.row_count,
  })
}
