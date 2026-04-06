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

#[cfg(test)]
mod tests {
    use super::SegmentMetadata;
    use crate::config::TableStorageConfig;
    use core::{id::TableId, time::now_ms};

    #[test]
    fn segment_metadata_new_starts_empty() {
        let before = now_ms();
        let metadata = SegmentMetadata::new(TableId { value: 7 }, TableStorageConfig::default());
        let after = now_ms();

        assert_eq!(metadata.table_id.value, 7);
        assert_eq!(metadata.total_rows, 0);
        assert_eq!(metadata.total_size_bytes, 0);
        assert!(metadata.row_groups.is_empty());
        assert!(metadata.created_at >= before);
        assert!(metadata.created_at <= after);
    }
}
