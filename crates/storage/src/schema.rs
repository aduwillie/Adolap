use core::error::AdolapError;
use std::path::Path;
use serde_json;
use tokio::fs;

use serde::{Deserialize, Serialize};

use crate::column::ColumnValue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ColumnType {
  Utf8,
  I32,
  U32,
  Bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
  pub name: String,
  pub column_type: ColumnType,
  pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
  pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub async fn load(path: &Path) -> Result<Self, AdolapError> {
        let bytes = tokio::fs::read(path).await?;
        let schema: TableSchema = serde_json::from_slice(&bytes)
            .map_err(|e| AdolapError::Serialization(format!("Schema error: {}", e)))?;
        Ok(schema)
    }

  pub fn validate_rows(&self, rows: &[Vec<Option<ColumnValue>>]) -> Result<(), AdolapError> {
    for row in rows {
      self.validate_row(row)?;
    }
    Ok(())
  }

  pub fn validate_row(&self, row: &[Option<ColumnValue>]) -> Result<(), AdolapError> {
    if row.len() != self.columns.len() {
      return Err(AdolapError::StorageError(format!(
        "Row has {} values but schema requires {}",
        row.len(),
        self.columns.len()
      )));
    }

    for (value, column) in row.iter().zip(&self.columns) {
      column.validate_value(value.as_ref())?;
    }

    Ok(())
  }

  pub async fn save(&self, path: &Path) -> Result<(), AdolapError> {
    let bytes = serde_json::to_vec_pretty(self)
      .map_err(|e| AdolapError::Serialization(format!("Schema error: {}", e)))?;
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent).await?;
    }
    fs::write(path, bytes).await?;
    Ok(())
  }
}

impl ColumnSchema {
  pub fn validate_value(&self, value: Option<&ColumnValue>) -> Result<(), AdolapError> {
    match value {
      None => {
        if self.nullable {
          Ok(())
        } else {
          Err(AdolapError::StorageError(format!(
            "Column {} is not nullable",
            self.name
          )))
        }
      }
      Some(value) => self.column_type.validate_value(value, &self.name),
    }
  }
}

impl ColumnType {
  fn validate_value(&self, value: &ColumnValue, column_name: &str) -> Result<(), AdolapError> {
    match self {
      ColumnType::Utf8 => Ok(()),
      ColumnType::I32 => match value {
        ColumnValue::I32(_) => Ok(()),
        ColumnValue::U32(value) => i32::try_from(*value).map(|_| ()).map_err(|_| {
          AdolapError::StorageError(format!(
            "Value {} does not fit in I32 for column {}",
            value, column_name
          ))
        }),
        ColumnValue::Utf8(value) => value.parse::<i32>().map(|_| ()).map_err(|_| {
          AdolapError::StorageError(format!(
            "Value '{}' cannot be parsed as I32 for column {}",
            value, column_name
          ))
        }),
        ColumnValue::Bool(_) => Err(type_error(column_name, "I32")),
      },
      ColumnType::U32 => match value {
        ColumnValue::U32(_) => Ok(()),
        ColumnValue::I32(value) => u32::try_from(*value).map(|_| ()).map_err(|_| {
          AdolapError::StorageError(format!(
            "Value {} does not fit in U32 for column {}",
            value, column_name
          ))
        }),
        ColumnValue::Utf8(value) => value.parse::<u32>().map(|_| ()).map_err(|_| {
          AdolapError::StorageError(format!(
            "Value '{}' cannot be parsed as U32 for column {}",
            value, column_name
          ))
        }),
        ColumnValue::Bool(_) => Err(type_error(column_name, "U32")),
      },
      ColumnType::Bool => match value {
        ColumnValue::Bool(_) => Ok(()),
        ColumnValue::Utf8(value) => parse_bool(value, column_name).map(|_| ()),
        ColumnValue::I32(value) => match value {
          0 | 1 => Ok(()),
          _ => Err(type_error(column_name, "Bool")),
        },
        ColumnValue::U32(value) => match value {
          0 | 1 => Ok(()),
          _ => Err(type_error(column_name, "Bool")),
        },
      },
    }
  }
}

fn parse_bool(value: &str, column_name: &str) -> Result<bool, AdolapError> {
  match value.to_ascii_lowercase().as_str() {
    "true" | "1" => Ok(true),
    "false" | "0" => Ok(false),
    _ => Err(AdolapError::StorageError(format!(
      "Value '{}' cannot be parsed as Bool for column {}",
      value, column_name
    ))),
  }
}

fn type_error(column_name: &str, expected: &str) -> AdolapError {
  AdolapError::StorageError(format!(
    "Column {} expects {} values",
    column_name, expected
  ))
}

