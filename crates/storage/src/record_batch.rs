//! Row-oriented materialization helpers for columnar storage.

use crate::{
  column::{ColumnInputOwned, ColumnValue, ColumnValuesOwned},
  null::is_null,
  schema::{ColumnType, TableSchema},
};
use core::error::AdolapError;

/// In-memory batch of rows backed by typed column vectors.
pub struct RecordBatch {
  pub schema: TableSchema,
  pub columns: Vec<ColumnInputOwned>,
  pub row_count: usize,
}

impl RecordBatch {
  /// Resolve a column name to its index in the schema.
  pub fn column_index(&self, name: &str) -> Option<usize> {
    self.schema.columns.iter().position(|col| col.name == name)
  }

  /// Materialize the batch as nullable row values.
  pub fn to_rows(&self) -> Result<Vec<Vec<Option<ColumnValue>>>, AdolapError> {
    let mut rows = vec![Vec::with_capacity(self.columns.len()); self.row_count];

    for column in &self.columns {
      for row_index in 0..self.row_count {
        let value = if is_null(column.validity.as_deref(), row_index) {
          None
        } else {
          Some(match &column.values {
            ColumnValuesOwned::Utf8(values) => ColumnValue::Utf8(values[row_index].clone()),
            ColumnValuesOwned::I32(values) => ColumnValue::I32(values[row_index]),
            ColumnValuesOwned::U32(values) => ColumnValue::U32(values[row_index]),
            ColumnValuesOwned::Bool(values) => ColumnValue::Bool(values[row_index]),
          })
        };
        rows[row_index].push(value);
      }
    }

    Ok(rows)
  }

  /// Build a batch from row-oriented values validated against a schema.
  ///
  /// Example:
  /// ```text
  /// let batch = RecordBatch::from_rows(schema, &rows)?;
  /// ```
  pub fn from_rows(schema: TableSchema, rows: &[Vec<Option<ColumnValue>>]) -> Result<Self, AdolapError> {
    schema.validate_rows(rows)?;

    let row_count = rows.len();
    let mut columns = Vec::with_capacity(schema.columns.len());
    for column in &schema.columns {
      columns.push(column_buffer(&column.column_type, row_count));
    }

    for (row_index, row) in rows.iter().enumerate() {
      for (column_index, value) in row.iter().enumerate() {
        let column_schema = &schema.columns[column_index];
        push_value(&mut columns[column_index], value.as_ref(), column_schema, row_index)?;
      }
    }

    Ok(Self {
      schema,
      columns,
      row_count,
    })
  }
}

fn column_buffer(column_type: &ColumnType, row_count: usize) -> ColumnInputOwned {
  let validity = vec![0xFF; row_count.div_ceil(8)];
  match column_type {
    ColumnType::Utf8 => ColumnInputOwned { values: ColumnValuesOwned::Utf8(Vec::with_capacity(row_count)), validity: Some(validity) },
    ColumnType::I32 => ColumnInputOwned { values: ColumnValuesOwned::I32(Vec::with_capacity(row_count)), validity: Some(validity) },
    ColumnType::U32 => ColumnInputOwned { values: ColumnValuesOwned::U32(Vec::with_capacity(row_count)), validity: Some(validity) },
    ColumnType::Bool => ColumnInputOwned { values: ColumnValuesOwned::Bool(Vec::with_capacity(row_count)), validity: Some(validity) },
  }
}

fn push_value(
  column: &mut ColumnInputOwned,
  value: Option<&ColumnValue>,
  schema: &crate::schema::ColumnSchema,
  row_index: usize,
) -> Result<(), AdolapError> {
  match (&mut column.values, value) {
    (ColumnValuesOwned::Utf8(values), Some(ColumnValue::Utf8(value))) => values.push(value.clone()),
    (ColumnValuesOwned::Utf8(values), Some(ColumnValue::I32(value))) => values.push(value.to_string()),
    (ColumnValuesOwned::Utf8(values), Some(ColumnValue::U32(value))) => values.push(value.to_string()),
    (ColumnValuesOwned::Utf8(values), Some(ColumnValue::Bool(value))) => values.push(value.to_string()),
    (ColumnValuesOwned::Utf8(values), None) => {
      mark_null(&mut column.validity, row_index);
      values.push(String::new());
    }

    (ColumnValuesOwned::I32(values), Some(ColumnValue::I32(value))) => values.push(*value),
    (ColumnValuesOwned::I32(values), Some(ColumnValue::U32(value))) => values.push(i32::try_from(*value).map_err(|_| {
      AdolapError::StorageError(format!("Value {} does not fit in I32 for column {}", value, schema.name))
    })?),
    (ColumnValuesOwned::I32(values), Some(ColumnValue::Utf8(value))) => values.push(value.parse::<i32>().map_err(|_| {
      AdolapError::StorageError(format!("Value '{}' cannot be parsed as I32 for column {}", value, schema.name))
    })?),
    (ColumnValuesOwned::I32(_), Some(ColumnValue::Bool(_))) => {
      return Err(AdolapError::StorageError(format!("Column {} expects I32 values", schema.name)));
    }
    (ColumnValuesOwned::I32(values), None) => {
      mark_null(&mut column.validity, row_index);
      values.push(0);
    }

    (ColumnValuesOwned::U32(values), Some(ColumnValue::U32(value))) => values.push(*value),
    (ColumnValuesOwned::U32(values), Some(ColumnValue::I32(value))) => values.push(u32::try_from(*value).map_err(|_| {
      AdolapError::StorageError(format!("Value {} does not fit in U32 for column {}", value, schema.name))
    })?),
    (ColumnValuesOwned::U32(values), Some(ColumnValue::Utf8(value))) => values.push(value.parse::<u32>().map_err(|_| {
      AdolapError::StorageError(format!("Value '{}' cannot be parsed as U32 for column {}", value, schema.name))
    })?),
    (ColumnValuesOwned::U32(_), Some(ColumnValue::Bool(_))) => {
      return Err(AdolapError::StorageError(format!("Column {} expects U32 values", schema.name)));
    }
    (ColumnValuesOwned::U32(values), None) => {
      mark_null(&mut column.validity, row_index);
      values.push(0);
    }

    (ColumnValuesOwned::Bool(values), Some(ColumnValue::Bool(value))) => values.push(*value),
    (ColumnValuesOwned::Bool(values), Some(ColumnValue::Utf8(value))) => values.push(parse_bool(value, &schema.name)?),
    (ColumnValuesOwned::Bool(values), Some(ColumnValue::I32(value))) => values.push(match value { 0 => false, 1 => true, _ => return Err(AdolapError::StorageError(format!("Column {} expects Bool values", schema.name))) }),
    (ColumnValuesOwned::Bool(values), Some(ColumnValue::U32(value))) => values.push(match value { 0 => false, 1 => true, _ => return Err(AdolapError::StorageError(format!("Column {} expects Bool values", schema.name))) }),
    (ColumnValuesOwned::Bool(values), None) => {
      mark_null(&mut column.validity, row_index);
      values.push(false);
    }
  }

  if !schema.nullable && value.is_none() {
    return Err(AdolapError::StorageError(format!("Column {} is not nullable", schema.name)));
  }

  clear_redundant_validity(column);

  Ok(())
}

fn mark_null(validity: &mut Option<Vec<u8>>, row_index: usize) {
  if let Some(bits) = validity.as_mut() {
    let byte_index = row_index / 8;
    bits[byte_index] &= !(1 << (row_index % 8));
  }
}

fn parse_bool(value: &str, column_name: &str) -> Result<bool, AdolapError> {
  match value.to_ascii_lowercase().as_str() {
    "true" | "1" => Ok(true),
    "false" | "0" => Ok(false),
    _ => Err(AdolapError::StorageError(format!("Value '{}' cannot be parsed as Bool for column {}", value, column_name))),
  }
}

fn clear_redundant_validity(column: &mut ColumnInputOwned) {
  if column.validity.as_ref().is_some_and(|bits| bits.iter().all(|byte| *byte == 0xFF)) {
    column.validity = None;
  }
}
