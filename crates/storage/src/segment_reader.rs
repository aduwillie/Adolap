//! Segment-level reading and predicate pruning.

use core::error::AdolapError;
use std::path::Path;
use tokio::fs;

use crate::{
    column::ColumnValue, 
    metadata::SegmentMetadata, 
    naming::{SEGMENT_METADATA_FILE_NAME, row_group_dir_name}, 
    record_batch::RecordBatch, 
    row_group_reader::RowGroupReader, 
    schema::TableSchema
};

/// Segment pruning predicates evaluated against per-row-group metadata.
#[derive(Debug, Clone)]
pub enum Predicate {
    Equals(String, ColumnValue),
    GreaterThan(String, ColumnValue),
    LessThan(String, ColumnValue),
    And(Vec<Predicate>),
}

/// Reads record batches from a single segment directory.
pub struct SegmentReader<'a> {
    pub schema: &'a TableSchema,
    pub projected_columns: Option<Vec<String>>,
}

impl<'a> SegmentReader<'a> {
    /// Create a segment reader with an optional projection list.
    pub fn new(schema: &'a TableSchema, projected_columns: Option<Vec<String>>) -> Self {
        Self { schema, projected_columns }
    }

    /// Read a segment and prune row groups when statistics or bloom filters allow it.
    pub async fn read_segment(
        &self,
        segment_dir: &Path,
        predicate: Option<&Predicate>,
    ) -> Result<Vec<RecordBatch>, AdolapError> {
        // 1. Load segment.meta
        let meta_path = segment_dir.join(SEGMENT_METADATA_FILE_NAME);
        let meta_bytes = fs::read(&meta_path).await?;
        let segment_meta: SegmentMetadata = postcard::from_bytes(&meta_bytes)
            .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize segment metadata: {}", e)))?;

        let mut batches = Vec::new();
        let projected_indices = self.projected_indices()?;
        let rg_reader = RowGroupReader::new(self.schema, projected_indices.clone());

        if let Some(pred) = predicate {
            if !segment_meta
                .row_groups
                .iter()
                .enumerate()
                .any(|(i, rg_meta)| {
                    let rg_dir = segment_dir.join(row_group_dir_name(i as u64));
                    self.row_group_matches_predicate(rg_meta, pred, &rg_dir).unwrap_or(true)
                })
            {
                return Ok(Vec::new());
            }
        }

        // 2. For each row group, apply pruning
        for (i, rg_meta) in segment_meta.row_groups.iter().enumerate() {
            let rg_dir = segment_dir.join(row_group_dir_name(i as u64));

            if let Some(pred) = predicate {
                if !self.row_group_matches_predicate(rg_meta, pred, &rg_dir)? {
                    continue; // prune
                }
            }

            // 3. Read row group
            let batch = rg_reader.read_row_group(&rg_dir).await?;
            batches.push(batch);
        }

        Ok(batches)
    }

    fn row_group_matches_predicate(
        &self,
        rg: &crate::row_group::RowGroupMetadata,
        predicate: &Predicate,
        rg_dir: &Path,
    ) -> Result<bool, AdolapError> {
        match predicate {
            Predicate::Equals(col_name, value) => {
                self.prune_equals(rg, col_name, value, rg_dir)
            }
            Predicate::GreaterThan(col_name, value) => {
                self.prune_gt(rg, col_name, value)
            }
            Predicate::LessThan(col_name, value) => {
                self.prune_lt(rg, col_name, value)
            }
            Predicate::And(predicates) => {
                for predicate in predicates {
                    if !self.row_group_matches_predicate(rg, predicate, rg_dir)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
        }
    }

    fn prune_equals(
        &self,
        rg: &crate::row_group::RowGroupMetadata,
        col_name: &str,
        value: &ColumnValue,
        rg_dir: &Path,
    ) -> Result<bool, AdolapError> {
        let col_index = self.column_index(col_name)?;
        let chunk = &rg.columns[col_index];

        // 1. Bloom filter check
        if chunk.has_bloom_filter {
            if let Some(_bloom_file) = &chunk.bloom_filter_file {
                // If bloom filter says "definitely not present", prune
                if !self.bloom_might_contain(chunk, rg_dir, value)? {
                    return Ok(false);
                }
            }
        }

        // 2. Min/max check
        if chunk.stats.min.is_some() {
            if !self.value_within_min_max(value, &chunk.stats)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn prune_gt(
        &self,
        rg: &crate::row_group::RowGroupMetadata,
        col_name: &str,
        value: &ColumnValue,
    ) -> Result<bool, AdolapError> {
        let col_index = self.column_index(col_name)?;
        let chunk = &rg.columns[col_index];

        if let Some(_max) = &chunk.stats.max {
            if !self.value_less_than_max(value, &chunk.stats)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn prune_lt(
        &self,
        rg: &crate::row_group::RowGroupMetadata,
        col_name: &str,
        value: &ColumnValue,
    ) -> Result<bool, AdolapError> {
        let col_index = self.column_index(col_name)?;
        let chunk = &rg.columns[col_index];

        if let Some(_min) = &chunk.stats.min {
            if !self.value_greater_than_min(value, &chunk.stats)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn bloom_might_contain(
        &self,
        chunk: &crate::column::ColumnChunkDescriptor,
        rg_dir: &Path,
        value: &ColumnValue,
    ) -> Result<bool, AdolapError> {
        let Some(bloom_file) = chunk.bloom_filter_file.as_ref() else {
            return Ok(true);
        };

        let bloom_path = rg_dir.join(bloom_file);
        let bloom_bytes = std::fs::read(&bloom_path)?;
        let bloom: crate::bloom::BloomFilter = postcard::from_bytes(&bloom_bytes)
            .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize bloom filter: {}", e)))?;

        Ok(match value {
            ColumnValue::Utf8(s) => bloom.might_contain(s),
            ColumnValue::I32(v) => bloom.might_contain(v),
            ColumnValue::U32(v) => bloom.might_contain(v),
            ColumnValue::Bool(v) => bloom.might_contain(v),
        })
    }

    fn column_index(&self, col_name: &str) -> Result<usize, AdolapError> {
        self.schema.columns
            .iter()
            .position(|col| col.name == col_name)
            .ok_or_else(|| AdolapError::StorageError(format!("Unknown column name: {}", col_name)))
    }

    fn value_within_min_max(
        &self,
        value: &ColumnValue,
        stats: &crate::stats::ColumnStats,
    ) -> Result<bool, AdolapError> {
        Ok(self.value_greater_than_min(value, stats)? && self.value_less_than_max(value, stats)?)
    }

    fn value_less_than_max(
        &self,
        value: &ColumnValue,
        stats: &crate::stats::ColumnStats,
    ) -> Result<bool, AdolapError> {
        match value {
            ColumnValue::Utf8(value) => compare_max::<String>(value, stats),
            ColumnValue::I32(value) => compare_max::<i32>(value, stats),
            ColumnValue::U32(value) => compare_max::<u32>(value, stats),
            ColumnValue::Bool(value) => compare_max::<bool>(value, stats),
        }
    }

    fn value_greater_than_min(
        &self,
        value: &ColumnValue,
        stats: &crate::stats::ColumnStats,
    ) -> Result<bool, AdolapError> {
        match value {
            ColumnValue::Utf8(value) => compare_min::<String>(value, stats),
            ColumnValue::I32(value) => compare_min::<i32>(value, stats),
            ColumnValue::U32(value) => compare_min::<u32>(value, stats),
            ColumnValue::Bool(value) => compare_min::<bool>(value, stats),
        }
    }

    fn projected_indices(&self) -> Result<Option<Vec<usize>>, AdolapError> {
        let Some(columns) = &self.projected_columns else {
            return Ok(None);
        };

        let indices = columns
            .iter()
            .map(|name| self.column_index(name))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(indices))
    }
}

fn compare_min<T>(value: &T, stats: &crate::stats::ColumnStats) -> Result<bool, AdolapError>
where
    T: serde::de::DeserializeOwned + PartialOrd,
{
    let Some(min) = &stats.min else {
        return Ok(true);
    };
    let min_value: T = postcard::from_bytes(min)
        .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize min stats value: {}", e)))?;
    Ok(value >= &min_value)
}

fn compare_max<T>(value: &T, stats: &crate::stats::ColumnStats) -> Result<bool, AdolapError>
where
    T: serde::de::DeserializeOwned + PartialOrd,
{
    let Some(max) = &stats.max else {
        return Ok(true);
    };
    let max_value: T = postcard::from_bytes(max)
        .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize max stats value: {}", e)))?;
    Ok(value <= &max_value)
}
