use core::{id::TableId, time::now_ms, types::{ByteCount, RowCount, TimestampMs}};
use serde::{Serialize, Deserialize};
use crate::{config::TableStorageConfig, row_group::RowGroupMetadata};

/// Metadata for a single segment of a table.
/// A segment is a collection of row groups that are stored together on disk.
/// This is loaded before reading any data files and is used for pruning, planning, and optimizing reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
  pub table_id: TableId,
  pub total_rows: RowCount,
  pub total_size_bytes: ByteCount,
  pub row_groups: Vec<RowGroupMetadata>,
  pub storage_config: TableStorageConfig,
  pub created_at: TimestampMs,
}

impl SegmentMetadata {
    pub fn new(table_id: TableId, storage_config: TableStorageConfig) -> Self {
        Self {
            table_id,
            total_rows: 0,
            total_size_bytes: 0,
            row_groups: Vec::new(),
            storage_config,
            created_at: now_ms(),
        }
    }
}
