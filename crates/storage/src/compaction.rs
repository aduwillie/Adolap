use crate::{
    metadata::SegmentMetadata,
    metadata_io::read_segment_metadata,
    naming::{segment_dir_name, SEGMENT_METADATA_FILE_NAME},
    record_batch::RecordBatch,
    segment_reader::SegmentReader,
    segment_writer::SegmentWriter,
    table_writer::TableWriter,
};
use core::{error::AdolapError, time::now_ms};
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::{info, warn};

const COMPACTION_LOCK_FILE_NAME: &str = ".compaction.lock";
const COMPACTION_TEMP_PREFIX: &str = ".compacting_";
const COMPACTION_BACKUP_PREFIX: &str = ".compacted_old_";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowGroupCompactionReport {
    pub segment_name: String,
    pub input_row_groups: usize,
    pub output_row_groups: usize,
    pub rows_rewritten: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub skipped: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentCompactionReport {
    pub input_segments: usize,
    pub output_segments: usize,
    pub input_row_groups: usize,
    pub output_row_groups: usize,
    pub rows_rewritten: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub skipped: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VacuumReport {
    pub removed_entries: usize,
    pub removed_paths: Vec<String>,
}

pub struct RowGroupCompactor<'a> {
    table: &'a TableWriter,
}

pub struct SegmentCompactor<'a> {
    table: &'a TableWriter,
}

impl<'a> RowGroupCompactor<'a> {
    pub fn new(table: &'a TableWriter) -> Self {
        Self { table }
    }

    pub async fn compact_segment(&self, segment_dir: &Path) -> Result<RowGroupCompactionReport, AdolapError> {
        let _lock = TableCompactionLock::acquire(&self.table.table_dir).await?;
        self.compact_segment_locked(segment_dir).await
    }

    async fn compact_segment_locked(&self, segment_dir: &Path) -> Result<RowGroupCompactionReport, AdolapError> {
        let metadata = read_segment_metadata(segment_dir).await?;
        let segment_name = file_name_text(segment_dir)?;
        let rows = read_segment_rows(self.table, segment_dir).await?;
        let output_row_groups = optimal_row_group_count(self.table, rows.len());
        let input_row_groups = metadata.row_groups.len();

        if rows.is_empty() || output_row_groups >= input_row_groups {
            return Ok(RowGroupCompactionReport {
                segment_name,
                input_row_groups,
                output_row_groups: input_row_groups,
                rows_rewritten: rows.len(),
                bytes_before: metadata.total_size_bytes,
                bytes_after: metadata.total_size_bytes,
                skipped: true,
            });
        }

        let temp_segment_dir = segment_dir
            .parent()
            .unwrap_or(self.table.table_dir.as_path())
            .join(format!("{}{}", COMPACTION_TEMP_PREFIX, now_ms()));

        let rewritten_metadata = write_rows_to_segment(self.table, &temp_segment_dir, &rows).await?;
        replace_directory(segment_dir, &temp_segment_dir).await?;

        info!(
            table = %self.table.table_dir.display(),
            segment = %segment_name,
            input_row_groups,
            output_row_groups = rewritten_metadata.row_groups.len(),
            rows_rewritten = rows.len(),
            "compacted segment row groups"
        );

        Ok(RowGroupCompactionReport {
            segment_name,
            input_row_groups,
            output_row_groups: rewritten_metadata.row_groups.len(),
            rows_rewritten: rows.len(),
            bytes_before: metadata.total_size_bytes,
            bytes_after: rewritten_metadata.total_size_bytes,
            skipped: false,
        })
    }
}

impl<'a> SegmentCompactor<'a> {
    pub fn new(table: &'a TableWriter) -> Self {
        Self { table }
    }

    pub async fn needs_compaction(&self) -> Result<bool, AdolapError> {
        let segments = discover_segments(&self.table.table_dir).await?;
        if segments.len() >= self.table.storage_config.compaction_segment_threshold.max(1) {
            return Ok(true);
        }

        for (_, metadata) in segments {
            if metadata.row_groups.len() > self.table.storage_config.compaction_row_group_threshold.max(1) {
                return Ok(true);
            }
            let optimal = optimal_row_group_count(self.table, metadata.total_rows as usize);
            if optimal < metadata.row_groups.len() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub async fn maybe_compact(&self) -> Result<Option<SegmentCompactionReport>, AdolapError> {
        if !self.needs_compaction().await? {
            return Ok(None);
        }

        Ok(Some(self.optimize().await?))
    }

    pub async fn optimize(&self) -> Result<SegmentCompactionReport, AdolapError> {
        let _lock = TableCompactionLock::acquire(&self.table.table_dir).await?;
        let segments = discover_segments(&self.table.table_dir).await?;
        if segments.is_empty() {
            return Ok(SegmentCompactionReport {
                skipped: true,
                ..SegmentCompactionReport::default()
            });
        }

        let input_segments = segments.len();
        let input_row_groups = segments.iter().map(|(_, metadata)| metadata.row_groups.len()).sum();
        let bytes_before = segments.iter().map(|(_, metadata)| metadata.total_size_bytes).sum();
        let rows = read_table_rows(self.table, &segments).await?;
        let rows_per_segment = rows_per_compacted_segment(self.table);
        let output_segments = if rows.is_empty() { 0 } else { rows.len().div_ceil(rows_per_segment) };

        let current_optimal_row_groups: usize = segments
            .iter()
            .map(|(_, metadata)| optimal_row_group_count(self.table, metadata.total_rows as usize))
            .sum();
        if input_segments == output_segments && current_optimal_row_groups == input_row_groups {
            return Ok(SegmentCompactionReport {
                input_segments,
                output_segments,
                input_row_groups,
                output_row_groups: input_row_groups,
                rows_rewritten: rows.len(),
                bytes_before,
                bytes_after: bytes_before,
                skipped: true,
            });
        }

        let temp_root = self
            .table
            .table_dir
            .join(format!("{}{}", COMPACTION_TEMP_PREFIX, now_ms()));
        fs::create_dir_all(&temp_root).await?;

        let mut bytes_after = 0u64;
        let mut output_row_groups = 0usize;
        for (index, segment_rows) in rows.chunks(rows_per_segment.max(1)).enumerate() {
            let segment_dir = temp_root.join(segment_dir_name(index as u64));
            let metadata = write_rows_to_segment(self.table, &segment_dir, segment_rows).await?;
            bytes_after += metadata.total_size_bytes;
            output_row_groups += metadata.row_groups.len();
        }

        replace_table_segments(
            &self.table.table_dir,
            segments.into_iter().map(|(path, _)| path).collect(),
            &temp_root,
        )
        .await?;

        info!(
            table = %self.table.table_dir.display(),
            input_segments,
            output_segments,
            rows_rewritten = rows.len(),
            "compacted table segments"
        );

        Ok(SegmentCompactionReport {
            input_segments,
            output_segments,
            input_row_groups,
            output_row_groups,
            rows_rewritten: rows.len(),
            bytes_before,
            bytes_after,
            skipped: false,
        })
    }

    pub async fn vacuum(&self) -> Result<VacuumReport, AdolapError> {
        let _lock = TableCompactionLock::acquire(&self.table.table_dir).await?;
        let mut entries = match fs::read_dir(&self.table.table_dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(VacuumReport::default()),
            Err(err) => return Err(err.into()),
        };

        let mut removed_paths = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };

            let should_remove = name.starts_with(COMPACTION_TEMP_PREFIX)
                || name.starts_with(COMPACTION_BACKUP_PREFIX)
                || (path.is_dir() && name.starts_with("segment_") && fs::metadata(path.join(SEGMENT_METADATA_FILE_NAME)).await.is_err());

            if !should_remove {
                continue;
            }

            if path.is_dir() {
                fs::remove_dir_all(&path).await?;
            } else {
                fs::remove_file(&path).await?;
            }
            removed_paths.push(name.to_string());
        }

        if !removed_paths.is_empty() {
            info!(table = %self.table.table_dir.display(), removed_entries = removed_paths.len(), "vacuumed table compaction leftovers");
        }

        Ok(VacuumReport {
            removed_entries: removed_paths.len(),
            removed_paths,
        })
    }
}

struct TableCompactionLock {
    path: PathBuf,
}

impl TableCompactionLock {
    async fn acquire(table_dir: &Path) -> Result<Self, AdolapError> {
        let path = table_dir.join(COMPACTION_LOCK_FILE_NAME);
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(_) => Ok(Self { path }),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Err(AdolapError::StorageError(
                "Compaction already in progress for table".into(),
            )),
            Err(err) => Err(err.into()),
        }
    }
}

impl Drop for TableCompactionLock {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(path = %self.path.display(), error = %error, "failed to remove compaction lock file");
            }
        }
    }
}

pub(crate) async fn discover_segments(table_dir: &Path) -> Result<Vec<(PathBuf, SegmentMetadata)>, AdolapError> {
    let mut segments = Vec::new();
    let mut entries = match fs::read_dir(table_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };

        let Some(id_text) = name.strip_prefix("segment_") else {
            continue;
        };
        let Ok(segment_id) = id_text.parse::<u64>() else {
            continue;
        };
        if fs::metadata(path.join(SEGMENT_METADATA_FILE_NAME)).await.is_err() {
            continue;
        }

        segments.push((segment_id, path.clone(), read_segment_metadata(&path).await?));
    }

    segments.sort_by_key(|(segment_id, _, _)| *segment_id);
    Ok(segments
        .into_iter()
        .map(|(_, path, metadata)| (path, metadata))
        .collect())
}

fn optimal_row_group_count(table: &TableWriter, row_count: usize) -> usize {
    if row_count == 0 {
        0
    } else {
        row_count.div_ceil(table.storage_config.row_group_size.max(1))
    }
}

fn rows_per_compacted_segment(table: &TableWriter) -> usize {
    table.storage_config.row_group_size.max(1)
        * table.storage_config.compaction_row_group_threshold.max(1)
}

async fn read_segment_rows(table: &TableWriter, segment_dir: &Path) -> Result<Vec<Vec<Option<crate::column::ColumnValue>>>, AdolapError> {
    let reader = SegmentReader::new(&table.schema, None);
    let batches = reader.read_segment(segment_dir, None).await?;
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(batch.to_rows()?);
    }
    Ok(rows)
}

async fn read_table_rows(
    table: &TableWriter,
    segments: &[(PathBuf, SegmentMetadata)],
) -> Result<Vec<Vec<Option<crate::column::ColumnValue>>>, AdolapError> {
    let mut rows = Vec::new();
    for (segment_dir, _) in segments {
        rows.extend(read_segment_rows(table, segment_dir).await?);
    }
    Ok(rows)
}

async fn write_rows_to_segment(
    table: &TableWriter,
    segment_dir: &Path,
    rows: &[Vec<Option<crate::column::ColumnValue>>],
) -> Result<SegmentMetadata, AdolapError> {
    let mut batches = Vec::new();
    for chunk in rows.chunks(table.storage_config.row_group_size.max(1)) {
        batches.push(RecordBatch::from_rows(table.schema.clone(), chunk)?);
    }

    let borrowed_row_groups = batches
        .iter()
        .map(|batch| batch.columns.iter().map(crate::column::ColumnInputOwned::as_borrowed).collect())
        .collect();
    let writer = SegmentWriter::new(table.table_id, &table.storage_config, &table.schema);
    writer.write_segment(segment_dir, borrowed_row_groups).await
}

async fn replace_directory(target_dir: &Path, temp_dir: &Path) -> Result<(), AdolapError> {
    let parent = target_dir
        .parent()
        .ok_or_else(|| AdolapError::StorageError("Segment directory has no parent".into()))?;
    let backup_dir = parent.join(format!("{}{}", COMPACTION_BACKUP_PREFIX, now_ms()));

    if fs::metadata(&backup_dir).await.is_ok() {
        fs::remove_dir_all(&backup_dir).await?;
    }

    fs::rename(target_dir, &backup_dir).await?;
    if let Err(error) = fs::rename(temp_dir, target_dir).await {
        let _ = fs::rename(&backup_dir, target_dir).await;
        return Err(error.into());
    }

    fs::remove_dir_all(&backup_dir).await?;
    Ok(())
}

async fn replace_table_segments(
    table_dir: &Path,
    old_segments: Vec<PathBuf>,
    temp_root: &Path,
) -> Result<(), AdolapError> {
    let backup_root = table_dir.join(format!("{}{}", COMPACTION_BACKUP_PREFIX, now_ms()));
    fs::create_dir_all(&backup_root).await?;

    for segment in &old_segments {
        let name = file_name_text(segment)?;
        fs::rename(segment, backup_root.join(name)).await?;
    }

    let mut new_entries = fs::read_dir(temp_root).await?;
    while let Some(entry) = new_entries.next_entry().await? {
        let path = entry.path();
        let name = file_name_text(&path)?;
        fs::rename(&path, table_dir.join(name)).await?;
    }

    if fs::metadata(temp_root).await.is_ok() {
        fs::remove_dir_all(temp_root).await?;
    }
    fs::remove_dir_all(backup_root).await?;
    Ok(())
}

fn file_name_text(path: &Path) -> Result<String, AdolapError> {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(|value| value.to_string())
        .ok_or_else(|| AdolapError::StorageError(format!("Invalid path name: {}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::{discover_segments, RowGroupCompactor, SegmentCompactor};
    use crate::{
        column::{ColumnInputOwned, ColumnValue},
        config::{CompressionType, TableStorageConfig},
        naming::segment_dir_name,
        record_batch::RecordBatch,
        schema::{ColumnSchema, ColumnType, TableSchema},
        segment_writer::SegmentWriter,
        table_writer::TableWriter,
    };
    use tempfile::TempDir;
    use tokio::fs;
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
                ColumnSchema {
                    name: "id".to_string(),
                    column_type: ColumnType::U32,
                    nullable: false,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    column_type: ColumnType::Utf8,
                    nullable: false,
                },
            ],
        }
    }

    fn sample_rows() -> Vec<Vec<Option<ColumnValue>>> {
        vec![
            vec![Some(ColumnValue::U32(1)), Some(ColumnValue::Utf8("alpha".into()))],
            vec![Some(ColumnValue::U32(2)), Some(ColumnValue::Utf8("beta".into()))],
            vec![Some(ColumnValue::U32(3)), Some(ColumnValue::Utf8("gamma".into()))],
        ]
    }

    async fn create_table_writer(temp_dir: &TempDir, storage_config: &TableStorageConfig) -> TableWriter {
        TableWriter::create_table(
            temp_dir.path(),
            "default",
            "events",
            &sample_schema(),
            storage_config,
        )
        .await
        .unwrap()
    }

    async fn write_custom_segment(
        writer: &TableWriter,
        segment_id: u64,
        row_groups: Vec<Vec<Vec<Option<ColumnValue>>>>,
    ) {
        let mut batches = Vec::new();
        for rows in row_groups {
            batches.push(RecordBatch::from_rows(writer.schema.clone(), &rows).unwrap());
        }

        let borrowed = batches
            .iter()
            .map(|batch| batch.columns.iter().map(ColumnInputOwned::as_borrowed).collect())
            .collect();

        SegmentWriter::new(writer.table_id, &writer.storage_config, &writer.schema)
            .write_segment(&writer.table_dir.join(segment_dir_name(segment_id)), borrowed)
            .await
            .unwrap();
    }

    #[test]
    fn row_group_compactor_rewrites_underfilled_segments() {
        run_async_test(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = TableStorageConfig {
                row_group_size: 4,
                compression: CompressionType::None,
                enable_bloom_filter: false,
                enable_dictionary_encoding: false,
                compaction_segment_threshold: 8,
                compaction_row_group_threshold: 4,
                enable_background_compaction: false,
                background_compaction_interval_seconds: 60,
            };
            let writer = create_table_writer(&temp_dir, &config).await;
            let rows = sample_rows();
            write_custom_segment(
                &writer,
                0,
                vec![vec![rows[0].clone()], vec![rows[1].clone()], vec![rows[2].clone()]],
            )
            .await;

            let report = RowGroupCompactor::new(&writer)
                .compact_segment(&writer.table_dir.join(segment_dir_name(0)))
                .await
                .unwrap();

            assert_eq!(report.input_row_groups, 3);
            assert_eq!(report.output_row_groups, 1);
            assert!(!report.skipped);

            let segments = discover_segments(&writer.table_dir).await.unwrap();
            assert_eq!(segments.len(), 1);
            assert_eq!(segments[0].1.row_groups.len(), 1);
            assert_eq!(segments[0].1.total_rows, 3);
        });
    }

    #[test]
    fn segment_compactor_merges_segments_and_preserves_rows() {
        run_async_test(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = TableStorageConfig {
                row_group_size: 2,
                compression: CompressionType::None,
                enable_bloom_filter: false,
                enable_dictionary_encoding: false,
                compaction_segment_threshold: 2,
                compaction_row_group_threshold: 4,
                enable_background_compaction: false,
                background_compaction_interval_seconds: 60,
            };
            let writer = create_table_writer(&temp_dir, &config).await;

            for row in sample_rows() {
                writer.insert_rows(&[row]).await.unwrap();
            }

            let report = SegmentCompactor::new(&writer).optimize().await.unwrap();
            assert_eq!(report.input_segments, 3);
            assert_eq!(report.output_segments, 1);
            assert_eq!(report.rows_rewritten, 3);
            assert!(!report.skipped);

            let segments = discover_segments(&writer.table_dir).await.unwrap();
            assert_eq!(segments.len(), 1);

            let rows = super::read_table_rows(&writer, &segments).await.unwrap();
            assert_eq!(rows, sample_rows());
        });
    }

    #[test]
    fn vacuum_removes_stale_compaction_directories_and_invalid_segments() {
        run_async_test(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let writer = create_table_writer(&temp_dir, &TableStorageConfig::default()).await;

            fs::create_dir_all(writer.table_dir.join(".compacting_stale")).await.unwrap();
            fs::create_dir_all(writer.table_dir.join(".compacted_old_stale")).await.unwrap();
            fs::create_dir_all(writer.table_dir.join("segment_99")).await.unwrap();

            let report = SegmentCompactor::new(&writer).vacuum().await.unwrap();
            assert_eq!(report.removed_entries, 3);
            assert!(report.removed_paths.iter().any(|path| path == ".compacting_stale"));
            assert!(report.removed_paths.iter().any(|path| path == ".compacted_old_stale"));
            assert!(report.removed_paths.iter().any(|path| path == "segment_99"));
        });
    }
}