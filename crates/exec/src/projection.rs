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

  #[cfg(test)]
  mod tests {
    use super::project;
    use storage::{
      column::ColumnValue,
      record_batch::RecordBatch,
      schema::{ColumnSchema, ColumnType, TableSchema},
    };

    #[test]
    fn projects_requested_columns_only() {
      let batch = RecordBatch::from_rows(
        TableSchema {
          columns: vec![
            ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false },
            ColumnSchema { name: "name".into(), column_type: ColumnType::Utf8, nullable: false },
          ],
        },
        &[vec![Some(ColumnValue::U32(1)), Some(ColumnValue::Utf8("alpha".into()))]],
      )
      .unwrap();

      let projected = project(&batch, &["name"]).unwrap();

      assert_eq!(projected.schema.columns.len(), 1);
      assert_eq!(projected.schema.columns[0].name, "name");
      assert_eq!(projected.to_rows().unwrap(), vec![vec![Some(ColumnValue::Utf8("alpha".into()))]]);
    }
  }
