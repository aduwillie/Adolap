use core::types::{ByteCount, RowCount};
use serde::{Serialize, Deserialize};
use crate::column::ColumnChunkDescriptor;

/// Metadata for a single row group within a segment.
/// A row group is a collection of rows that are stored together on disk.
/// This is the smallest unit of pruning and scanning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowGroupMetadata {
  pub row_count: RowCount,
  pub columns: Vec<ColumnChunkDescriptor>,
  
  pub total_uncompressed_size_bytes: ByteCount,
  pub total_compressed_size_bytes: ByteCount,
}

#[cfg(test)]
mod tests {
  use super::RowGroupMetadata;
  use crate::{column::ColumnChunkDescriptor, config::CompressionType, stats::ColumnStats};

  #[test]
  fn row_group_metadata_round_trips_through_postcard() {
    let metadata = RowGroupMetadata {
      row_count: 3,
      columns: vec![ColumnChunkDescriptor {
        column_id: 0,
        compression: CompressionType::None,
        uses_dictionary_encoding: false,
        has_bloom_filter: false,
        dictionary_file: None,
        data_file: "column_0.data".into(),
        bloom_filter_file: None,
        validity_file: None,
        uncompressed_size: 12,
        compressed_size: 12,
        stats: ColumnStats::empty(),
      }],
      total_uncompressed_size_bytes: 12,
      total_compressed_size_bytes: 12,
    };

    let bytes = postcard::to_allocvec(&metadata).unwrap();
    let decoded: RowGroupMetadata = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(decoded.row_count, 3);
    assert_eq!(decoded.columns.len(), 1);
    assert_eq!(decoded.total_compressed_size_bytes, 12);
  }
}
