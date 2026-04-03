use core::error::AdolapError;
use std::path::Path;
use tokio::fs;

use crate::{
    column::{ColumnChunkDescriptor, ColumnInputOwned, ColumnValuesOwned},
    compression::decompress_buffer,
    null::is_null,
    schema::ColumnType,
};

pub struct ColumnChunkReader;

impl ColumnChunkReader {
    pub fn new() -> Self {
        Self
    }

    pub async fn read_column_chunk(
        &self,
        row_group_dir: &Path,
        descriptor: &ColumnChunkDescriptor,
        column_type: ColumnType,
    ) -> Result<ColumnInputOwned, AdolapError> {
        // 1. Read and decompress .data
        let data_path = row_group_dir.join(&descriptor.data_file);
        let compressed = fs::read(&data_path)
            .await
            .map_err(|e| AdolapError::StorageError(format!("Cannot read column data: {}", e)))?;

        let decompressed =
            decompress_buffer(&compressed, descriptor.compression)
                .map_err(|e| AdolapError::StorageError(format!("Decompression failed: {}", e)))?;

        // 2. Read validity bitmap (if present)
        let validity = if let Some(validity_file) = &descriptor.validity_file {
            let path = row_group_dir.join(validity_file);
            Some(fs::read(&path).await?)
        } else {
            None
        };

        // 3. Dispatch based on type + dictionary flag
        let values = match column_type {
            ColumnType::Utf8 => {
                if descriptor.uses_dictionary_encoding {
                    self.read_utf8_dictionary(row_group_dir, descriptor, &decompressed, validity.as_deref()).await
                } else {
                    self.read_utf8_plain(&decompressed, validity.as_deref())
                }
            }

            ColumnType::I32 => self.read_i32(&decompressed, validity.as_deref()),
            ColumnType::U32 => self.read_u32(&decompressed, validity.as_deref()),
            ColumnType::Bool => self.read_bool(&decompressed, validity.as_deref()),
        }?;

        Ok(ColumnInputOwned { values, validity })
    }

    // ---------------- UTF-8 (Plain) ----------------

    fn read_utf8_plain(
        &self,
        decompressed: &[u8],
        validity: Option<&[u8]>,
    ) -> Result<ColumnValuesOwned, AdolapError> {
        let mut values: Vec<String> = postcard::from_bytes(decompressed)
            .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize UTF-8 values: {}", e)))?;

        if let Some(bits) = validity {
            for i in 0..values.len() {
                if is_null(Some(bits), i) {
                    values[i] = String::new(); // or keep Option<String> if you prefer
                }
            }
        }

        Ok(ColumnValuesOwned::Utf8(values))
    }

    // ---------------- UTF-8 (Dictionary) ----------------

    async fn read_utf8_dictionary(
        &self,
        row_group_dir: &Path,
        descriptor: &ColumnChunkDescriptor,
        decompressed: &[u8],
        validity: Option<&[u8]>,
    ) -> Result<ColumnValuesOwned, AdolapError> {
        // 1. Read dictionary
        let dict_file = descriptor.dictionary_file.as_ref().ok_or_else(|| {
            AdolapError::StorageError("Dictionary encoding enabled but no dictionary file found".into())
        })?;

        let dict_path = row_group_dir.join(dict_file);
        let dict_bytes = fs::read(&dict_path).await?;
        let dict: Vec<String> = postcard::from_bytes(&dict_bytes)
            .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize dictionary: {}", e)))?;

        // 2. Read encoded indices
        let encoded: Vec<u32> = postcard::from_bytes(decompressed)
            .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize encoded dictionary indices: {}", e)))?;

        // 3. Decode
        let mut values = Vec::with_capacity(encoded.len());
        for (i, idx) in encoded.iter().enumerate() {
            if is_null(validity, i) || *idx == u32::MAX {
                values.push(String::new());
            } else {
                values.push(dict[*idx as usize].clone());
            }
        }

        Ok(ColumnValuesOwned::Utf8(values))
    }

    // ---------------- I32 ----------------

    fn read_i32(
        &self,
        decompressed: &[u8],
        validity: Option<&[u8]>,
    ) -> Result<ColumnValuesOwned, AdolapError> {
        let mut values: Vec<i32> = postcard::from_bytes(decompressed)
            .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize i32 values: {}", e)))?;

        if let Some(bits) = validity {
            for i in 0..values.len() {
                if is_null(Some(bits), i) {
                    values[i] = 0; // or sentinel
                }
            }
        }

        Ok(ColumnValuesOwned::I32(values))
    }

    // ---------------- U32 ----------------

    fn read_u32(
        &self,
        decompressed: &[u8],
        validity: Option<&[u8]>,
    ) -> Result<ColumnValuesOwned, AdolapError> {
        let mut values: Vec<u32> = postcard::from_bytes(decompressed)
            .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize u32 values: {}", e)))?;

        if let Some(bits) = validity {
            for i in 0..values.len() {
                if is_null(Some(bits), i) {
                    values[i] = 0;
                }
            }
        }

        Ok(ColumnValuesOwned::U32(values))
    }

    // ---------------- Bool ----------------

    fn read_bool(
        &self,
        decompressed: &[u8],
        validity: Option<&[u8]>,
    ) -> Result<ColumnValuesOwned, AdolapError> {
        let mut values: Vec<bool> = postcard::from_bytes(decompressed)
            .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize bool values: {}", e)))?;

        if let Some(bits) = validity {
            for i in 0..values.len() {
                if is_null(Some(bits), i) {
                    values[i] = false;
                }
            }
        }

        Ok(ColumnValuesOwned::Bool(values))
    }
}
