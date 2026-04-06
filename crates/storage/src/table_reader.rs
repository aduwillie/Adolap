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

#[cfg(test)]
mod tests {
    use super::TableReader;
    use crate::{
        column::ColumnValue,
        schema::{ColumnSchema, ColumnType, TableSchema},
        table_writer::TableWriter,
    };
    use tokio::runtime::Runtime;

    fn run_async_test<F, T>(future: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        Runtime::new().unwrap().block_on(future)
    }

    fn sample_schema() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false },
                ColumnSchema { name: "name".into(), column_type: ColumnType::Utf8, nullable: false },
            ],
        }
    }

    #[test]
    fn reads_table_batches_from_discovered_segments() {
        run_async_test(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let schema = sample_schema();
            let writer = TableWriter::create_table(temp_dir.path(), "default", "events", &schema, &Default::default())
                .await
                .unwrap();

            writer
                .insert_rows(&[
                    vec![Some(ColumnValue::U32(1)), Some(ColumnValue::Utf8("alpha".into()))],
                    vec![Some(ColumnValue::U32(2)), Some(ColumnValue::Utf8("beta".into()))],
                ])
                .await
                .unwrap();

            let batches = TableReader::new(&writer.table_dir, &schema)
                .read_table(None, Some(vec!["name".into()]))
                .await
                .unwrap();

            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].schema.columns[0].name, "name");
            assert_eq!(batches[0].row_count, 2);
        });
    }
}
