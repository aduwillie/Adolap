use core::error::AdolapError;
use std::path::{Path, PathBuf};
use tokio::fs;
use crate::{
    metadata::SegmentMetadata, naming::SEGMENT_METADATA_FILE_NAME, record_batch::RecordBatch, schema::TableSchema, segment_reader::{Predicate, SegmentReader}
};

pub struct TableReader<'a> {
    pub table_dir: &'a Path,
    pub schema: &'a TableSchema,
}

impl<'a> TableReader<'a> {
    pub fn new(table_dir: &'a Path, schema: &'a TableSchema) -> Self {
        Self { table_dir, schema }
    }

    pub async fn read_table(
        &self,
        predicate: Option<&Predicate>,
        projected_columns: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>, AdolapError> {
        let mut batches = Vec::new();
        let segment_dirs = self.discover_segments().await?;
        let segment_reader = SegmentReader::new(self.schema, projected_columns);

        for segment_dir in segment_dirs {
            // Load segment metadata (for future use: stats, debugging, etc.)
            let meta_path = segment_dir.join(SEGMENT_METADATA_FILE_NAME);
            let meta_bytes = fs::read(&meta_path).await?;
            let _segment_meta: SegmentMetadata = postcard::from_bytes(&meta_bytes)
                .map_err(|e| AdolapError::Serialization(format!("Cannot deserialize segment metadata: {}", e)))?;

            // Delegate pruning + reading to SegmentReader
            let mut segment_batches = segment_reader
                .read_segment(&segment_dir, predicate)
                .await?;

            batches.append(&mut segment_batches);
        }

        Ok(batches)
    }

    async fn discover_segments(&self) -> Result<Vec<PathBuf>, AdolapError> {
        let mut dirs = Vec::new();
        let mut entries = fs::read_dir(self.table_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("segment_") {
                        dirs.push(path);
                    }
                }
            }
        }

        dirs.sort();
        Ok(dirs)
    }
}
