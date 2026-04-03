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
