use core::{error::AdolapError, types::ByteCount};
use std::{collections::HashMap, path::Path};
use tokio::fs;

use crate::{
    bloom,
    column::{ColumnChunkDescriptor, ColumnValues}, 
    compression::compress_buffer, 
    config::TableStorageConfig, 
    null::is_null, 
    schema::ColumnType, 
    stats::{ColumnStats, compute_bool_column_stats, compute_i32_column_stats, compute_u32_column_stats, compute_utf8_column_stats}
};

pub struct ColumnChunkWriter<'a> {
    pub column_index: usize,
    pub storage_config: &'a TableStorageConfig,
    pub column_type: ColumnType,
}

impl<'a> ColumnChunkWriter<'a> {
    pub fn new(column_index: usize, storage_config: &'a TableStorageConfig, column_type: ColumnType) -> Self {
        Self {
            column_index,
            storage_config,
            column_type,
        }
    }
}

impl<'a> ColumnChunkWriter<'a> {
    pub async fn write_column(
        &self,
        row_group_dir: &Path,
        values: ColumnValues<'_>,
        validity: Option<&[u8]>,
    ) -> Result<ColumnChunkDescriptor, AdolapError> {
        let validity_file = self.write_validity_bitmap(row_group_dir, validity).await?;

        let (final_buffer, dictionary_file, uses_dictionary, stats) = match self.column_type {
            ColumnType::Utf8 => self.write_utf8(row_group_dir, values.as_utf8()?, validity).await?,
            ColumnType::I32 => self.write_i32(values.as_i32()?, validity)?,
            ColumnType::U32 => self.write_u32(values.as_u32()?, validity)?,
            ColumnType::Bool => self.write_bool(values.as_bool()?, validity)?,
        };

        let bloom_filter_file = self.write_bloom_filter(row_group_dir, &values, validity).await?;

        self.finalize_column_write(
            row_group_dir,
            final_buffer,
            dictionary_file,
            uses_dictionary,
            bloom_filter_file,
            validity_file,
            stats,
        )
        .await
    }

    pub async fn write_utf8_column(
        &self,
        row_group_dir: &Path,
        values: &[String],
        validity: Option<&[u8]>,
    ) -> Result<ColumnChunkDescriptor, AdolapError> {
        let validity_file = self.write_validity_bitmap(row_group_dir, validity).await?;
        let (final_buffer, dictionary_file, uses_dictionary, stats) = self.write_utf8(row_group_dir, values, validity).await?;
        let bloom_filter_file = bloom::write_bloom_file(row_group_dir, &self.column_index, values, validity).await?;

        self.finalize_column_write(
            row_group_dir,
            final_buffer,
            dictionary_file,
            uses_dictionary,
            bloom_filter_file,
            validity_file,
            stats,
        )
        .await
    }

    pub async fn write_i32_column(
        &self,
        row_group_dir: &Path,
        values: &[i32],
        validity: Option<&[u8]>,
    ) -> Result<ColumnChunkDescriptor, AdolapError> {
        let validity_file = self.write_validity_bitmap(row_group_dir, validity).await?;
        let (final_buffer, dictionary_file, uses_dictionary, stats) = self.write_i32(values, validity)?;
        let bloom_filter_file = bloom::write_bloom_file(row_group_dir, &self.column_index, values, validity).await?;

        self.finalize_column_write(
            row_group_dir,
            final_buffer,
            dictionary_file,
            uses_dictionary,
            bloom_filter_file,
            validity_file,
            stats,
        )
        .await
    }

    pub async fn write_u32_column(
        &self,
        row_group_dir: &Path,
        values: &[u32],
        validity: Option<&[u8]>,
    ) -> Result<ColumnChunkDescriptor, AdolapError> {
        let validity_file = self.write_validity_bitmap(row_group_dir, validity).await?;
        let (final_buffer, dictionary_file, uses_dictionary, stats) = self.write_u32(values, validity)?;
        let bloom_filter_file = bloom::write_bloom_file(row_group_dir, &self.column_index, values, validity).await?;

        self.finalize_column_write(
            row_group_dir,
            final_buffer,
            dictionary_file,
            uses_dictionary,
            bloom_filter_file,
            validity_file,
            stats,
        )
        .await
    }

    pub async fn write_bool_column(
        &self,
        row_group_dir: &Path,
        values: &[bool],
        validity: Option<&[u8]>,
    ) -> Result<ColumnChunkDescriptor, AdolapError> {
        let validity_file = self.write_validity_bitmap(row_group_dir, validity).await?;
        let (final_buffer, dictionary_file, uses_dictionary, stats) = self.write_bool(values, validity)?;

        self.finalize_column_write(
            row_group_dir,
            final_buffer,
            dictionary_file,
            uses_dictionary,
            None,
            validity_file,
            stats,
        )
        .await
    }

    async fn finalize_column_write(
        &self,
        row_group_dir: &Path,
        final_buffer: Vec<u8>,
        dictionary_file: Option<String>,
        uses_dictionary: bool,
        bloom_filter_file: Option<String>,
        validity_file: Option<String>,
        stats: ColumnStats,
    ) -> Result<ColumnChunkDescriptor, AdolapError> {
        let compression = self.storage_config.compression;
        let uncompressed_size = final_buffer.len() as ByteCount;
        let compressed_buffer = compress_buffer(&final_buffer, compression)
            .map_err(|e| AdolapError::StorageError(format!("Compression failed: {}", e)))?;
        let compressed_size = compressed_buffer.len() as ByteCount;

        let data_file_name = format!("column_{}.data", self.column_index);
        let data_file_path = row_group_dir.join(&data_file_name);

        fs::write(&data_file_path, compressed_buffer)
            .await
            .map_err(|e| AdolapError::StorageError(format!("Cannot write column data: {}", e)))?;

        Ok(ColumnChunkDescriptor {
            column_id: self.column_index,
            compression,
            uses_dictionary_encoding: uses_dictionary,
            has_bloom_filter: bloom_filter_file.is_some(),

            data_file: data_file_name,
            dictionary_file,
            bloom_filter_file,
            validity_file,

            uncompressed_size,
            compressed_size,

            stats,
        })
    }

    async fn write_validity_bitmap(
        &self,
        row_group_dir: &Path,
        validity: Option<&[u8]>,
    ) -> Result<Option<String>, AdolapError> {
        if let Some(null_bits) = validity {
            let validity_file_name = format!("column_{}.valid", self.column_index);
            let validity_file_path = row_group_dir.join(&validity_file_name);

            fs::write(&validity_file_path, null_bits)
                .await
                .map_err(|e| AdolapError::StorageError(format!("Cannot write validity bitmap: {}", e)))?;

            Ok(Some(validity_file_name))
        } else {
            Ok(None)
        }
    }

    async fn write_utf8(
        &self,
        row_group_dir: &Path,
        values: &[String],
        validity: Option<&[u8]>,
    ) -> Result<(Vec<u8>, Option<String>, bool, ColumnStats), AdolapError> {
        let (final_buffer, dictionary_file, uses_dictionary) = if self.storage_config.enable_dictionary_encoding {
            let (encoded_buffer, dict_file_name) = self.encode_utf8_dictionary(row_group_dir, values, validity).await?;
            (encoded_buffer, Some(dict_file_name), true)
        } else {
            let raw_buffer = postcard::to_allocvec(values)
                .map_err(|e| AdolapError::Serialization(format!("Cannot serialize UTF-8 values: {}", e)))?;
            (raw_buffer, None, false)
        };

        let stats = compute_utf8_column_stats(values, validity)?;
        Ok((final_buffer, dictionary_file, uses_dictionary, stats))
    }

    async fn encode_utf8_dictionary(
        &self,
        row_group_dir: &Path,
        values: &[String],
        validity: Option<&[u8]>,
    ) -> Result<(Vec<u8>, String), AdolapError> {
        let mut dict: Vec<String> = Vec::new();
        let mut index_map: HashMap<&str, u32> = HashMap::new();
        let mut encoded_values: Vec<u32> = Vec::with_capacity(values.len());

        for (i, value) in values.iter().enumerate() {
            if is_null(validity, i) {
                encoded_values.push(u32::MAX);
                continue;
            }

            if let Some(&idx) = index_map.get(value.as_str()) {
                encoded_values.push(idx);
            } else {
                let idx = dict.len() as u32;
                dict.push(value.clone());
                index_map.insert(value.as_str(), idx);
                encoded_values.push(idx);
            }
        }

        let dict_buffer = postcard::to_allocvec(&dict)
            .map_err(|e| AdolapError::Serialization(format!("Cannot serialize dictionary: {}", e)))?;

        let dict_file_name = format!("column_{}.dict", self.column_index);
        let dict_file_path = row_group_dir.join(&dict_file_name);

        fs::write(&dict_file_path, dict_buffer)
            .await
            .map_err(|e| AdolapError::StorageError(format!("Cannot write dictionary: {}", e)))?;

        let encoded_buffer = postcard::to_allocvec(&encoded_values)
            .map_err(|e| AdolapError::Serialization(format!("Cannot serialize encoded values: {}", e)))?;

        Ok((encoded_buffer, dict_file_name))
    }

    async fn write_bloom_filter(
        &self,
        row_group_dir: &Path,
        values: &ColumnValues<'_>,
        validity: Option<&[u8]>,
    ) -> Result<Option<String>, AdolapError> {
        if !self.storage_config.enable_bloom_filter {
            return Ok(None);
        }

        let file_name = match self.column_type {
            ColumnType::Utf8 => bloom::write_bloom_file(row_group_dir, &self.column_index, values.as_utf8()?, validity).await?,
            ColumnType::I32 => bloom::write_bloom_file(row_group_dir, &self.column_index, values.as_i32()?, validity).await?,
            ColumnType::U32 => bloom::write_bloom_file(row_group_dir, &self.column_index, values.as_u32()?, validity).await?,
            ColumnType::Bool => None,
        };

        Ok(file_name)
    }

    fn write_i32(
        &self,
        values: &[i32],
        validity: Option<&[u8]>,
    ) -> Result<(Vec<u8>, Option<String>, bool, ColumnStats), AdolapError> {
        let final_buffer = postcard::to_allocvec(values)
            .map_err(|e| AdolapError::Serialization(format!("Cannot serialize i32 values: {}", e)))?;
        let stats = compute_i32_column_stats(values, validity)?;
        Ok((final_buffer, None, false, stats))
    }

    fn write_u32(
        &self,
        values: &[u32],
        validity: Option<&[u8]>,
    ) -> Result<(Vec<u8>, Option<String>, bool, ColumnStats), AdolapError> {
        let final_buffer = postcard::to_allocvec(values)
            .map_err(|e| AdolapError::Serialization(format!("Cannot serialize u32 values: {}", e)))?;
        let stats = compute_u32_column_stats(values, validity)?;
        Ok((final_buffer, None, false, stats))
    }

    fn write_bool(
        &self,
        values: &[bool],
        validity: Option<&[u8]>,
    ) -> Result<(Vec<u8>, Option<String>, bool, ColumnStats), AdolapError> {
        let final_buffer = postcard::to_allocvec(values)
            .map_err(|e| AdolapError::Serialization(format!("Cannot serialize bool values: {}", e)))?;
        let stats = compute_bool_column_stats(values, validity)?;
        Ok((final_buffer, None, false, stats))
    }
}
