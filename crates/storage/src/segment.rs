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
