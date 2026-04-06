use core::{error::AdolapError, types::{ByteCount, ColumnIndex}};
use crate::{config::CompressionType, stats::ColumnStats};
use serde::{Serialize, Deserialize};

/// Logic description of a column chunk inside a row group.
/// The actual on-disk data will live in a .data file
/// This struct describes how to interpret the data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnChunkDescriptor {
  // This is the position of the column in the table schema, not the position in the row group
  // Eg if the table has columns [id, name, age], then column_id 0 is "id", column_id 1 is "name", and column_id 2 is "age"
  pub column_id: ColumnIndex,
  pub compression: CompressionType,
  pub uses_dictionary_encoding: bool,
  pub has_bloom_filter: bool,

  pub dictionary_file: Option<String>,
  pub data_file: String,
  pub bloom_filter_file: Option<String>,
  pub validity_file: Option<String>,

  pub uncompressed_size: ByteCount,
  pub compressed_size: ByteCount,

  pub stats: ColumnStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnValue {
    Utf8(String),
    I32(i32),
    U32(u32),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ColumnValues<'a> {
  Utf8(&'a [String]),
  I32(&'a [i32]),
  U32(&'a [u32]),
  Bool(&'a [bool]),
}

impl<'a> ColumnValues<'a> {
  pub fn len(&self) -> usize {
    match self {
      ColumnValues::Utf8(data) => data.len(),
      ColumnValues::I32(data) => data.len(),
      ColumnValues::U32(data) => data.len(),
      ColumnValues::Bool(data) => data.len(),
    }
  }
}

impl<'a> ColumnValues<'a> {
  pub fn as_utf8(&self) -> Result<&'a [String], AdolapError> {
    match self {
      ColumnValues::Utf8(data) => Ok(data),
      _ => Err(AdolapError::StorageError("Column is not of type UTF-8".into())),
    }
  }

  pub fn as_i32(&self) -> Result<&'a [i32], AdolapError> {
    match self {
      ColumnValues::I32(data) => Ok(data),
      _ => Err(AdolapError::StorageError("Column is not of type I32".into())),
    }
  }

  pub fn as_u32(&self) -> Result<&'a [u32], AdolapError> {
    match self {
      ColumnValues::U32(data) => Ok(data),
      _ => Err(AdolapError::StorageError("Column is not of type U32".into())),
    }
  }

  pub fn as_bool(&self) -> Result<&'a [bool], AdolapError> {
    match self {
      ColumnValues::Bool(data) => Ok(data),
      _ => Err(AdolapError::StorageError("Column is not of type Bool".into())),
    }
  }
}


#[derive(Debug, Clone)]
pub struct ColumnInput<'a> {
  pub values: ColumnValues<'a>,
  pub validity: Option<&'a [u8]>,
}

#[derive(Debug, Clone)]
pub struct ColumnInputOwned {
  pub values: ColumnValuesOwned,
  pub validity: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum ColumnValuesOwned {
  Utf8(Vec<String>),
  I32(Vec<i32>),
  U32(Vec<u32>),
  Bool(Vec<bool>),
}

impl ColumnValuesOwned {
  pub fn as_borrowed(&self) -> ColumnValues<'_> {
    match self {
      ColumnValuesOwned::Utf8(values) => ColumnValues::Utf8(values),
      ColumnValuesOwned::I32(values) => ColumnValues::I32(values),
      ColumnValuesOwned::U32(values) => ColumnValues::U32(values),
      ColumnValuesOwned::Bool(values) => ColumnValues::Bool(values),
    }
  }
}

impl ColumnInputOwned {
  pub fn as_borrowed(&self) -> ColumnInput<'_> {
    ColumnInput {
      values: self.values.as_borrowed(),
      validity: self.validity.as_deref(),
    }
  }
}

#[cfg(test)]
mod tests {
    use super::{ColumnInputOwned, ColumnValue, ColumnValues, ColumnValuesOwned};
    use core::error::AdolapError;

    #[test]
    fn column_values_len_and_accessors_match_variant() {
        let utf8 = vec!["a".to_string(), "b".to_string()];
        let values = ColumnValues::Utf8(&utf8);
        assert_eq!(values.len(), 2);
        assert_eq!(values.as_utf8().unwrap(), utf8.as_slice());

        match values.as_i32().unwrap_err() {
            AdolapError::StorageError(message) => assert!(message.contains("not of type I32")),
            other => panic!("expected storage error, got {:?}", other),
        }
    }

    #[test]
    fn owned_column_values_borrow_without_losing_validity() {
        let owned = ColumnInputOwned {
            values: ColumnValuesOwned::I32(vec![1, 2, 3]),
            validity: Some(vec![0b0000_0101]),
        };

        let borrowed = owned.as_borrowed();
        assert_eq!(borrowed.validity, Some(&[0b0000_0101][..]));
        assert_eq!(borrowed.values.as_i32().unwrap(), &[1, 2, 3]);

        let scalar = ColumnValue::Bool(true);
        assert!(matches!(scalar, ColumnValue::Bool(true)));
    }
}
