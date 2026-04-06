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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        column::{ColumnChunkDescriptor, ColumnValues},
        column_writer::ColumnChunkWriter,
        config::{CompressionType, TableStorageConfig},
        stats::ColumnStats,
    };
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn run_async_test<F, T>(future: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        Runtime::new().unwrap().block_on(future)
    }

    fn temp_row_group_dir() -> (TempDir, std::path::PathBuf) {
        let temp_dir = tempfile::tempdir().unwrap();
        let row_group_dir = temp_dir.path().join("row_group_0");
        std::fs::create_dir_all(&row_group_dir).unwrap();
        (temp_dir, row_group_dir)
    }

    fn storage_config(
        compression: CompressionType,
        enable_dictionary_encoding: bool,
        enable_bloom_filter: bool,
    ) -> TableStorageConfig {
        TableStorageConfig {
            row_group_size: 1024,
            compression,
            enable_bloom_filter,
            enable_dictionary_encoding,
            compaction_segment_threshold: 8,
            compaction_row_group_threshold: 8,
            enable_background_compaction: false,
            background_compaction_interval_seconds: 60,
        }
    }

    fn empty_stats() -> ColumnStats {
        ColumnStats::empty()
    }

    #[test]
    fn reads_utf8_plain_round_trip_with_nulls() {
        run_async_test(async {
            let (_temp_dir, row_group_dir) = temp_row_group_dir();
            let reader = ColumnChunkReader::new();
            let config = storage_config(CompressionType::Zstd, false, true);
            let writer = ColumnChunkWriter::new(0, &config, ColumnType::Utf8);
            let values = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
            let validity = [0b0000_0101];

            let descriptor = writer
                .write_column(&row_group_dir, ColumnValues::Utf8(&values), Some(&validity))
                .await
                .unwrap();

            assert!(!descriptor.uses_dictionary_encoding);
            assert!(descriptor.dictionary_file.is_none());
            assert!(descriptor.validity_file.is_some());

            let column = reader
                .read_column_chunk(&row_group_dir, &descriptor, ColumnType::Utf8)
                .await
                .unwrap();

            match column.values {
                ColumnValuesOwned::Utf8(decoded) => {
                    assert_eq!(decoded, vec!["alpha", "", "gamma"]);
                }
                other => panic!("expected utf8 values, got {:?}", other),
            }
            assert_eq!(column.validity, Some(validity.to_vec()));
        });
    }

    #[test]
    fn reads_utf8_dictionary_round_trip_with_nulls() {
        run_async_test(async {
            let (_temp_dir, row_group_dir) = temp_row_group_dir();
            let reader = ColumnChunkReader::new();
            let config = storage_config(CompressionType::Lz4, true, true);
            let writer = ColumnChunkWriter::new(1, &config, ColumnType::Utf8);
            let values = vec![
                "us".to_string(),
                "ca".to_string(),
                "us".to_string(),
                "mx".to_string(),
            ];
            let validity = [0b0000_1101];

            let descriptor = writer
                .write_utf8_column(&row_group_dir, &values, Some(&validity))
                .await
                .unwrap();

            assert!(descriptor.uses_dictionary_encoding);
            assert!(descriptor.dictionary_file.is_some());

            let column = reader
                .read_column_chunk(&row_group_dir, &descriptor, ColumnType::Utf8)
                .await
                .unwrap();

            match column.values {
                ColumnValuesOwned::Utf8(decoded) => {
                    assert_eq!(decoded, vec!["us", "", "us", "mx"]);
                }
                other => panic!("expected utf8 values, got {:?}", other),
            }
            assert_eq!(column.validity, Some(validity.to_vec()));
        });
    }

    #[test]
    fn reads_i32_round_trip_with_nulls() {
        run_async_test(async {
            let (_temp_dir, row_group_dir) = temp_row_group_dir();
            let reader = ColumnChunkReader::new();
            let config = storage_config(CompressionType::None, false, true);
            let writer = ColumnChunkWriter::new(2, &config, ColumnType::I32);
            let values = vec![-5, 10, 15];
            let validity = [0b0000_0101];

            let descriptor = writer
                .write_i32_column(&row_group_dir, &values, Some(&validity))
                .await
                .unwrap();

            let column = reader
                .read_column_chunk(&row_group_dir, &descriptor, ColumnType::I32)
                .await
                .unwrap();

            match column.values {
                ColumnValuesOwned::I32(decoded) => assert_eq!(decoded, vec![-5, 0, 15]),
                other => panic!("expected i32 values, got {:?}", other),
            }
            assert_eq!(column.validity, Some(validity.to_vec()));
        });
    }

    #[test]
    fn reads_u32_round_trip_with_nulls() {
        run_async_test(async {
            let (_temp_dir, row_group_dir) = temp_row_group_dir();
            let reader = ColumnChunkReader::new();
            let config = storage_config(CompressionType::Lz4, false, true);
            let writer = ColumnChunkWriter::new(3, &config, ColumnType::U32);
            let values = vec![1u32, 2, 3, 4];
            let validity = [0b0000_1011];

            let descriptor = writer
                .write_u32_column(&row_group_dir, &values, Some(&validity))
                .await
                .unwrap();

            let column = reader
                .read_column_chunk(&row_group_dir, &descriptor, ColumnType::U32)
                .await
                .unwrap();

            match column.values {
                ColumnValuesOwned::U32(decoded) => assert_eq!(decoded, vec![1, 2, 0, 4]),
                other => panic!("expected u32 values, got {:?}", other),
            }
            assert_eq!(column.validity, Some(validity.to_vec()));
        });
    }

    #[test]
    fn reads_bool_round_trip_with_nulls() {
        run_async_test(async {
            let (_temp_dir, row_group_dir) = temp_row_group_dir();
            let reader = ColumnChunkReader::new();
            let config = storage_config(CompressionType::Zstd, false, false);
            let writer = ColumnChunkWriter::new(4, &config, ColumnType::Bool);
            let values = vec![true, true, false];
            let validity = [0b0000_0101];

            let descriptor = writer
                .write_bool_column(&row_group_dir, &values, Some(&validity))
                .await
                .unwrap();

            assert!(!descriptor.has_bloom_filter);

            let column = reader
                .read_column_chunk(&row_group_dir, &descriptor, ColumnType::Bool)
                .await
                .unwrap();

            match column.values {
                ColumnValuesOwned::Bool(decoded) => assert_eq!(decoded, vec![true, false, false]),
                other => panic!("expected bool values, got {:?}", other),
            }
            assert_eq!(column.validity, Some(validity.to_vec()));
        });
    }

    #[test]
    fn errors_when_dictionary_encoding_descriptor_is_missing_dictionary_file() {
        run_async_test(async {
            let (_temp_dir, row_group_dir) = temp_row_group_dir();
            let reader = ColumnChunkReader::new();
            let encoded = postcard::to_allocvec(&vec![0u32, 1u32]).unwrap();
            let data_path = row_group_dir.join("column_0.data");
            fs::write(&data_path, &encoded).await.unwrap();

            let descriptor = ColumnChunkDescriptor {
                column_id: 0,
                compression: CompressionType::None,
                uses_dictionary_encoding: true,
                has_bloom_filter: false,
                dictionary_file: None,
                data_file: "column_0.data".to_string(),
                bloom_filter_file: None,
                validity_file: None,
                uncompressed_size: encoded.len() as u64,
                compressed_size: encoded.len() as u64,
                stats: empty_stats(),
            };

            match reader
                .read_column_chunk(&row_group_dir, &descriptor, ColumnType::Utf8)
                .await
                .unwrap_err()
            {
                AdolapError::StorageError(message) => {
                    assert!(message.contains("no dictionary file found"));
                }
                other => panic!("expected storage error, got {:?}", other),
            }
        });
    }

    #[test]
    fn errors_when_compressed_payload_cannot_be_decompressed() {
        run_async_test(async {
            let (_temp_dir, row_group_dir) = temp_row_group_dir();
            let reader = ColumnChunkReader::new();
            let data_path = row_group_dir.join("column_0.data");
            fs::write(&data_path, b"not-zstd").await.unwrap();

            let descriptor = ColumnChunkDescriptor {
                column_id: 0,
                compression: CompressionType::Zstd,
                uses_dictionary_encoding: false,
                has_bloom_filter: false,
                dictionary_file: None,
                data_file: "column_0.data".to_string(),
                bloom_filter_file: None,
                validity_file: None,
                uncompressed_size: 8,
                compressed_size: 8,
                stats: empty_stats(),
            };

            match reader
                .read_column_chunk(&row_group_dir, &descriptor, ColumnType::U32)
                .await
                .unwrap_err()
            {
                AdolapError::StorageError(message) => {
                    assert!(message.contains("Decompression failed"));
                }
                other => panic!("expected storage error, got {:?}", other),
            }
        });
    }
}