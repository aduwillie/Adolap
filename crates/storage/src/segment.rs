use std::path::PathBuf;
use crate::metadata::SegmentMetadata;

/// Represents a single segment of a table, which is a collection of row groups stored together on disk.
/// This struct knows:
/// - where the segment is located on disk (path)
/// - how to load its metadata
/// - how to access its row groups and column chunks for reading data
#[derive(Debug, Clone)]
pub struct Segment {
  pub path: PathBuf,
  pub metadata: SegmentMetadata,
}

impl Segment {
  pub fn new(path: PathBuf, metadata: SegmentMetadata) -> Self {
    Self { path, metadata }
  }

  pub fn row_groups_count(&self) -> usize {
    self.metadata.row_groups.len()
  }
}

#[cfg(test)]
mod tests {
    use super::Segment;
    use crate::{config::TableStorageConfig, metadata::SegmentMetadata, row_group::RowGroupMetadata};
    use core::id::TableId;
    use std::path::PathBuf;

    #[test]
    fn segment_wraps_path_and_counts_row_groups() {
        let mut metadata = SegmentMetadata::new(TableId { value: 11 }, TableStorageConfig::default());
        metadata.row_groups.push(RowGroupMetadata {
            row_count: 2,
            columns: Vec::new(),
            total_uncompressed_size_bytes: 10,
            total_compressed_size_bytes: 4,
        });

        let path = PathBuf::from("segment_1");
        let segment = Segment::new(path.clone(), metadata);

        assert_eq!(segment.path, path);
        assert_eq!(segment.row_groups_count(), 1);
    }
}
