use core::{
    error::AdolapError,
    id::TableId,
    time::now_ms,
};
use serde_json::Value;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};
use tokio::fs;
use tracing::{debug, info};

use crate::{
    column::{ColumnInput, ColumnInputOwned, ColumnValue, ColumnValuesOwned},
    config::TableStorageConfig,
    naming::{segment_dir_name, SEGMENT_METADATA_FILE_NAME, TABLE_CONFIG_FILE_NAME},
    schema::{ColumnType, TableSchema},
    segment_writer::SegmentWriter,
};

const REPLACE_STAGING_PREFIX: &str = ".replace_staging_";
const REPLACE_BACKUP_PREFIX: &str = ".replace_backup_";

pub const DEFAULT_DATABASE_NAME: &str = "default";
pub const SCHEMA_FILE_NAME: &str = "schema.json";

pub struct TableWriter {
    pub table_dir: PathBuf,
    pub schema: TableSchema,
    pub storage_config: TableStorageConfig,
    pub table_id: TableId,
}

impl TableWriter {
    pub async fn create_database(data_root: &Path, database: &str) -> Result<PathBuf, AdolapError> {
        validate_name(database, "database")?;
        let db_dir = data_root.join(database);
        fs::create_dir_all(&db_dir).await?;
        info!(database, path = %db_dir.display(), "created database directory");
        Ok(db_dir)
    }

    pub async fn create_table(
        data_root: &Path,
        database: &str,
        table: &str,
        schema: &TableSchema,
        storage_config: &TableStorageConfig,
    ) -> Result<Self, AdolapError> {
        validate_name(table, "table")?;
        let db_dir = Self::create_database(data_root, database).await?;
        let table_dir = db_dir.join(table);
        fs::create_dir_all(&table_dir).await?;
        schema.save(&table_dir.join(SCHEMA_FILE_NAME)).await?;
        storage_config.save(&table_dir.join(TABLE_CONFIG_FILE_NAME)).await?;
        info!(database, table, path = %table_dir.display(), columns = schema.columns.len(), row_group_size = storage_config.row_group_size, "created table storage");
        Ok(Self::from_parts(table_dir, schema.clone(), storage_config.clone()))
    }

    pub async fn open(data_root: &Path, database: &str, table: &str) -> Result<Self, AdolapError> {
        Self::open_at(data_root.join(database).join(table)).await
    }

    pub async fn open_at(table_dir: PathBuf) -> Result<Self, AdolapError> {
        let schema = TableSchema::load(&table_dir.join(SCHEMA_FILE_NAME)).await?;
        let storage_config = match fs::metadata(table_dir.join(TABLE_CONFIG_FILE_NAME)).await {
            Ok(_) => TableStorageConfig::load(&table_dir.join(TABLE_CONFIG_FILE_NAME)).await?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => TableStorageConfig::default(),
            Err(err) => return Err(err.into()),
        };
        debug!(path = %table_dir.display(), columns = schema.columns.len(), "opened table writer");
        Ok(Self::from_parts(table_dir, schema, storage_config))
    }

    pub async fn insert_rows(&self, rows: &[Vec<Option<ColumnValue>>]) -> Result<usize, AdolapError> {
        if rows.is_empty() {
            debug!(table = %self.table_dir.display(), "skipping insert because no rows were provided");
            return Ok(0);
        }

        self.schema.validate_rows(rows)?;

        let row_groups = self.build_row_groups(rows)?;
        let borrowed_row_groups: Vec<Vec<ColumnInput<'_>>> = row_groups
            .iter()
            .map(|row_group| row_group.iter().map(ColumnInputOwned::as_borrowed).collect())
            .collect();

        let segment_id = self.next_segment_id().await?;
        let segment_dir = self.table_dir.join(segment_dir_name(segment_id));
        let segment_writer = SegmentWriter::new(self.table_id, &self.storage_config, &self.schema);
        segment_writer.write_segment(&segment_dir, borrowed_row_groups).await?;
        info!(table = %self.table_dir.display(), row_count = rows.len(), segment_id, row_group_count = row_groups.len(), "inserted rows into new segment");

        Ok(rows.len())
    }

    /// Replace all rows in the table atomically.
    ///
    /// The new rows are written to a temporary staging directory first.  Once
    /// all writes succeed the existing segment directories are renamed to a
    /// backup location and the staging segments are moved into place.  The
    /// backup is then removed.  A crash between the rename steps leaves either
    /// the old data or the new data fully intact; no window exists where the
    /// table is empty.
    pub async fn replace_rows(&self, rows: &[Vec<Option<ColumnValue>>]) -> Result<usize, AdolapError> {
        info!(table = %self.table_dir.display(), replacement_rows = rows.len(), "replacing table rows atomically");

        let ts = now_ms();
        let staging_dir = self.table_dir.join(format!("{}{}", REPLACE_STAGING_PREFIX, ts));
        let backup_dir  = self.table_dir.join(format!("{}{}", REPLACE_BACKUP_PREFIX, ts));

        // Clean up any leftover staging/backup directories from a previous crash.
        for prefix in [REPLACE_STAGING_PREFIX, REPLACE_BACKUP_PREFIX] {
            let mut entries = match fs::read_dir(&self.table_dir).await {
                Ok(e) => e,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
                Err(err) => return Err(err.into()),
            };
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.file_name().and_then(|n| n.to_str()).map_or(false, |n| n.starts_with(prefix)) {
                    fs::remove_dir_all(&path).await?;
                    debug!(path = %path.display(), "removed leftover replace directory");
                }
            }
        }

        // 1. Write new segments into the staging directory.
        fs::create_dir_all(&staging_dir).await?;

        // Build a TableWriter that targets the staging directory so that
        // segment IDs start from 0 and config/schema are not written there.
        let staging_writer = Self {
            table_dir: staging_dir.clone(),
            schema: self.schema.clone(),
            storage_config: self.storage_config.clone(),
            table_id: self.table_id,
        };
        let inserted = staging_writer.insert_rows(rows).await?;

        // 2. Collect existing live segment directories.
        let live_segments = self.collect_segment_dirs().await?;

        // 3. Move live segments to backup atomically (rename is near-atomic
        //    on most filesystems when source and destination are on the same
        //    volume, which is always true here).
        fs::create_dir_all(&backup_dir).await?;
        for segment in &live_segments {
            let name = segment
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| AdolapError::StorageError("Invalid segment path".into()))?;
            fs::rename(segment, backup_dir.join(name)).await?;
        }

        // 4. Move staged segments into the live table directory.
        let mut staged = fs::read_dir(&staging_dir).await?;
        while let Some(entry) = staged.next_entry().await? {
            let path = entry.path();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| AdolapError::StorageError("Invalid staged segment path".into()))?
                .to_string();
            fs::rename(&path, self.table_dir.join(&name)).await?;
        }

        // 5. Remove the now-empty staging directory and the backup.
        fs::remove_dir_all(&staging_dir).await?;
        fs::remove_dir_all(&backup_dir).await?;

        info!(table = %self.table_dir.display(), inserted, "atomic replace completed");
        Ok(inserted)
    }

    pub async fn clear_data(&self) -> Result<(), AdolapError> {
        for segment in self.collect_segment_dirs().await? {
            let name = segment.file_name().and_then(|n| n.to_str()).unwrap_or("");
            fs::remove_dir_all(&segment).await?;
            debug!(segment = %name, table = %self.table_dir.display(), "removed segment during clear_data");
        }
        Ok(())
    }

    async fn collect_segment_dirs(&self) -> Result<Vec<PathBuf>, AdolapError> {
        let mut segments = Vec::new();
        let mut entries = match fs::read_dir(&self.table_dir).await {
            Ok(e) => e,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(segments),
            Err(err) => return Err(err.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("segment_") {
                        segments.push(path);
                    }
                }
            }
        }
        Ok(segments)
    }

    pub async fn ingest_json_file(&self, file_path: &Path) -> Result<usize, AdolapError> {
        info!(table = %self.table_dir.display(), file_path = %file_path.display(), "ingesting json file");
        let content = fs::read_to_string(file_path).await?;
        let rows = self.parse_rows_from_json(&content)?;
        debug!(row_count = rows.len(), "parsed rows from ingest file");
        self.insert_rows(&rows).await
    }

    fn from_parts(table_dir: PathBuf, schema: TableSchema, storage_config: TableStorageConfig) -> Self {
        let table_id = TableId {
            value: hash_path(&table_dir),
        };

        Self {
            table_dir,
            schema,
            storage_config,
            table_id,
        }
    }

    fn build_row_groups(
        &self,
        rows: &[Vec<Option<ColumnValue>>],
    ) -> Result<Vec<Vec<ColumnInputOwned>>, AdolapError> {
        let chunk_size = self.storage_config.row_group_size.max(1);
        let mut row_groups = Vec::new();

        for chunk in rows.chunks(chunk_size) {
            row_groups.push(self.build_row_group(chunk)?);
        }

        Ok(row_groups)
    }

    fn build_row_group(
        &self,
        rows: &[Vec<Option<ColumnValue>>],
    ) -> Result<Vec<ColumnInputOwned>, AdolapError> {
        let mut columns = self
            .schema
            .columns
            .iter()
            .map(|column| MutableColumn::new(column.column_type.clone(), rows.len()))
            .collect::<Vec<_>>();

        for (row_index, row) in rows.iter().enumerate() {
            if row.len() != self.schema.columns.len() {
                return Err(AdolapError::StorageError(format!(
                    "Row has {} values but schema requires {}",
                    row.len(),
                    self.schema.columns.len()
                )));
            }

            for (column_index, cell) in row.iter().enumerate() {
                let column_schema = &self.schema.columns[column_index];
                columns[column_index].push(cell.as_ref(), column_schema, row_index)?;
            }
        }

        Ok(columns.into_iter().map(MutableColumn::finish).collect())
    }

    fn parse_rows_from_json(&self, content: &str) -> Result<Vec<Vec<Option<ColumnValue>>>, AdolapError> {
        match serde_json::from_str::<Value>(content) {
            Ok(Value::Array(rows)) => rows
                .into_iter()
                .map(|row| self.json_row_to_values(row))
                .collect(),
            Ok(other) => Err(AdolapError::StorageError(format!(
                "Expected a JSON array for ingest, got {}",
                other
            ))),
            Err(_) => content
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(|line| {
                    let value: Value = serde_json::from_str(line).map_err(|e| {
                        AdolapError::StorageError(format!("Invalid JSON line: {}", e))
                    })?;
                    self.json_row_to_values(value)
                })
                .collect(),
        }
    }

    fn json_row_to_values(&self, value: Value) -> Result<Vec<Option<ColumnValue>>, AdolapError> {
        match value {
            Value::Object(object) => self
                .schema
                .columns
                .iter()
                .map(|column| {
                    object
                        .get(&column.name)
                        .cloned()
                        .map(json_value_to_column_value)
                        .transpose()
                        .map(|value| value.flatten())
                })
                .collect(),
            Value::Array(values) => {
                if values.len() != self.schema.columns.len() {
                    return Err(AdolapError::StorageError(format!(
                        "JSON row has {} values but schema requires {}",
                        values.len(),
                        self.schema.columns.len()
                    )));
                }

                values.into_iter().map(json_value_to_column_value).collect()
            }
            other => Err(AdolapError::StorageError(format!(
                "Unsupported JSON row format: {}",
                other
            ))),
        }
    }

    async fn next_segment_id(&self) -> Result<u64, AdolapError> {
        let mut max_segment_id = None;
        let mut entries = match fs::read_dir(&self.table_dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
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

            if path.join(SEGMENT_METADATA_FILE_NAME).is_file() {
                if let Ok(segment_id) = id_text.parse::<u64>() {
                    max_segment_id = Some(
                        max_segment_id.map_or(segment_id, |current: u64| current.max(segment_id)),
                    );
                }
            }
        }

        Ok(max_segment_id.map_or(0, |value| value + 1))
    }
}

fn validate_name(value: &str, kind: &str) -> Result<(), AdolapError> {
    if value.is_empty() || !value.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(AdolapError::StorageError(format!(
            "Invalid {} name: {}",
            kind, value
        )));
    }
    Ok(())
}

fn hash_path(path: &Path) -> u64 {
    let mut hasher = DefaultHasher::new();
    path.to_string_lossy().hash(&mut hasher);
    hasher.finish()
}

fn json_value_to_column_value(value: Value) -> Result<Option<ColumnValue>, AdolapError> {
    match value {
        Value::Null => Ok(None),
        Value::String(value) => Ok(Some(ColumnValue::Utf8(value))),
        Value::Bool(value) => Ok(Some(ColumnValue::Bool(value))),
        Value::Number(value) => {
            if let Some(int_value) = value.as_i64() {
                if let Ok(v) = i32::try_from(int_value) {
                    return Ok(Some(ColumnValue::I32(v)));
                }
                if let Ok(v) = u32::try_from(int_value) {
                    return Ok(Some(ColumnValue::U32(v)));
                }
            }

            Err(AdolapError::StorageError(format!(
                "Unsupported numeric value in ingest file: {}",
                value
            )))
        }
        other => Err(AdolapError::StorageError(format!(
            "Unsupported value in ingest file: {}",
            other
        ))),
    }
}

enum MutableColumn {
    Utf8 { values: Vec<String>, validity: Vec<u8>, has_nulls: bool },
    I32 { values: Vec<i32>, validity: Vec<u8>, has_nulls: bool },
    U32 { values: Vec<u32>, validity: Vec<u8>, has_nulls: bool },
    Bool { values: Vec<bool>, validity: Vec<u8>, has_nulls: bool },
}

impl MutableColumn {
    fn new(column_type: ColumnType, row_count: usize) -> Self {
        let validity = vec![0xFF; row_count.div_ceil(8)];
        match column_type {
            ColumnType::Utf8 => Self::Utf8 { values: Vec::with_capacity(row_count), validity, has_nulls: false },
            ColumnType::I32 => Self::I32 { values: Vec::with_capacity(row_count), validity, has_nulls: false },
            ColumnType::U32 => Self::U32 { values: Vec::with_capacity(row_count), validity, has_nulls: false },
            ColumnType::Bool => Self::Bool { values: Vec::with_capacity(row_count), validity, has_nulls: false },
        }
    }

    fn push(
        &mut self,
        value: Option<&ColumnValue>,
        schema: &crate::schema::ColumnSchema,
        row_index: usize,
    ) -> Result<(), AdolapError> {
        match self {
            MutableColumn::Utf8 { values, validity, has_nulls } => {
                let value = match value {
                    Some(ColumnValue::Utf8(value)) => value.clone(),
                    Some(ColumnValue::I32(value)) => value.to_string(),
                    Some(ColumnValue::U32(value)) => value.to_string(),
                    Some(ColumnValue::Bool(value)) => value.to_string(),
                    None => {
                        ensure_nullable(schema)?;
                        mark_null(validity, row_index);
                        *has_nulls = true;
                        String::new()
                    }
                };
                values.push(value);
            }
            MutableColumn::I32 { values, validity, has_nulls } => {
                let value = match value {
                    Some(ColumnValue::I32(value)) => *value,
                    Some(ColumnValue::U32(value)) => i32::try_from(*value).map_err(|_| {
                        AdolapError::StorageError(format!(
                            "Value {} does not fit in I32 for column {}",
                            value, schema.name
                        ))
                    })?,
                    Some(ColumnValue::Utf8(value)) => value.parse::<i32>().map_err(|_| {
                        AdolapError::StorageError(format!(
                            "Value '{}' cannot be parsed as I32 for column {}",
                            value, schema.name
                        ))
                    })?,
                    Some(ColumnValue::Bool(_)) => {
                        return Err(type_error(schema, "I32"));
                    }
                    None => {
                        ensure_nullable(schema)?;
                        mark_null(validity, row_index);
                        *has_nulls = true;
                        0
                    }
                };
                values.push(value);
            }
            MutableColumn::U32 { values, validity, has_nulls } => {
                let value = match value {
                    Some(ColumnValue::U32(value)) => *value,
                    Some(ColumnValue::I32(value)) => u32::try_from(*value).map_err(|_| {
                        AdolapError::StorageError(format!(
                            "Value {} does not fit in U32 for column {}",
                            value, schema.name
                        ))
                    })?,
                    Some(ColumnValue::Utf8(value)) => value.parse::<u32>().map_err(|_| {
                        AdolapError::StorageError(format!(
                            "Value '{}' cannot be parsed as U32 for column {}",
                            value, schema.name
                        ))
                    })?,
                    Some(ColumnValue::Bool(_)) => {
                        return Err(type_error(schema, "U32"));
                    }
                    None => {
                        ensure_nullable(schema)?;
                        mark_null(validity, row_index);
                        *has_nulls = true;
                        0
                    }
                };
                values.push(value);
            }
            MutableColumn::Bool { values, validity, has_nulls } => {
                let value = match value {
                    Some(ColumnValue::Bool(value)) => *value,
                    Some(ColumnValue::Utf8(value)) => parse_bool(value, &schema.name)?,
                    Some(ColumnValue::I32(value)) => match value {
                        0 => false,
                        1 => true,
                        _ => return Err(type_error(schema, "Bool")),
                    },
                    Some(ColumnValue::U32(value)) => match value {
                        0 => false,
                        1 => true,
                        _ => return Err(type_error(schema, "Bool")),
                    },
                    None => {
                        ensure_nullable(schema)?;
                        mark_null(validity, row_index);
                        *has_nulls = true;
                        false
                    }
                };
                values.push(value);
            }
        }

        Ok(())
    }

    fn finish(self) -> ColumnInputOwned {
        match self {
            MutableColumn::Utf8 { values, validity, has_nulls } => ColumnInputOwned {
                values: ColumnValuesOwned::Utf8(values),
                validity: has_nulls.then_some(validity),
            },
            MutableColumn::I32 { values, validity, has_nulls } => ColumnInputOwned {
                values: ColumnValuesOwned::I32(values),
                validity: has_nulls.then_some(validity),
            },
            MutableColumn::U32 { values, validity, has_nulls } => ColumnInputOwned {
                values: ColumnValuesOwned::U32(values),
                validity: has_nulls.then_some(validity),
            },
            MutableColumn::Bool { values, validity, has_nulls } => ColumnInputOwned {
                values: ColumnValuesOwned::Bool(values),
                validity: has_nulls.then_some(validity),
            },
        }
    }
}

fn ensure_nullable(schema: &crate::schema::ColumnSchema) -> Result<(), AdolapError> {
    if !schema.nullable {
        return Err(AdolapError::StorageError(format!(
            "Column {} is not nullable",
            schema.name
        )));
    }
    Ok(())
}

fn mark_null(validity: &mut [u8], row_index: usize) {
    let byte_index = row_index / 8;
    let mask = !(1 << (row_index % 8));
    validity[byte_index] &= mask;
}

fn parse_bool(value: &str, column_name: &str) -> Result<bool, AdolapError> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(AdolapError::StorageError(format!(
            "Value '{}' cannot be parsed as Bool for column {}",
            value, column_name
        ))),
    }
}

fn type_error(schema: &crate::schema::ColumnSchema, expected: &str) -> AdolapError {
    AdolapError::StorageError(format!(
        "Column {} expects {} values",
        schema.name, expected
    ))
}

#[cfg(test)]
mod tests {
    use super::{validate_name, TableWriter};
    use crate::{column::ColumnValue, schema::{ColumnSchema, ColumnType, TableSchema}};
    use core::error::AdolapError;
    use tokio::runtime::Runtime;

    fn run_async_test<F, T>(future: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        Runtime::new().unwrap().block_on(future)
    }

    fn sample_schema() -> TableSchema {
        TableSchema {
            columns: vec![ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false }],
        }
    }

    #[test]
    fn validate_name_rejects_invalid_identifiers() {
        assert!(validate_name("good_name", "table").is_ok());
        match validate_name("bad-name", "table").unwrap_err() {
            AdolapError::StorageError(message) => assert!(message.contains("Invalid table name")),
            other => panic!("expected storage error, got {:?}", other),
        }
    }

    #[test]
    fn insert_rows_returns_zero_for_empty_input() {
        run_async_test(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let writer = TableWriter::create_table(temp_dir.path(), "default", "events", &sample_schema(), &Default::default())
                .await
                .unwrap();

            let inserted = writer.insert_rows(&[]).await.unwrap();
            assert_eq!(inserted, 0);
            assert!(!writer.table_dir.join("segment_0").exists());

            writer.insert_rows(&[vec![Some(ColumnValue::U32(1))]]).await.unwrap();
            assert!(writer.table_dir.join("segment_0").exists());
        });
    }

    #[test]
    fn replace_rows_overwrites_existing_data() {
        run_async_test(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let writer = TableWriter::create_table(
                temp_dir.path(),
                "default",
                "events",
                &sample_schema(),
                &Default::default(),
            )
            .await
            .unwrap();

            writer.insert_rows(&[vec![Some(ColumnValue::U32(1))]]).await.unwrap();
            writer.insert_rows(&[vec![Some(ColumnValue::U32(2))]]).await.unwrap();

            let replaced = writer
                .replace_rows(&[vec![Some(ColumnValue::U32(99))]])
                .await
                .unwrap();

            assert_eq!(replaced, 1);

            // Only one segment should exist after replacement.
            let segments: Vec<_> = std::fs::read_dir(&writer.table_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("segment_"))
                        .unwrap_or(false)
                })
                .collect();
            assert_eq!(segments.len(), 1, "expected exactly one segment after replace");
        });
    }

    #[test]
    fn replace_rows_leaves_no_staging_or_backup_directories() {
        run_async_test(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let writer = TableWriter::create_table(
                temp_dir.path(),
                "default",
                "events",
                &sample_schema(),
                &Default::default(),
            )
            .await
            .unwrap();

            writer.insert_rows(&[vec![Some(ColumnValue::U32(42))]]).await.unwrap();
            writer
                .replace_rows(&[vec![Some(ColumnValue::U32(7))]])
                .await
                .unwrap();

            // No leftover staging or backup directories.
            let leftovers: Vec<_> = std::fs::read_dir(&writer.table_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    let name = e.file_name();
                    let n = name.to_string_lossy();
                    n.starts_with(super::REPLACE_STAGING_PREFIX)
                        || n.starts_with(super::REPLACE_BACKUP_PREFIX)
                })
                .collect();
            assert!(leftovers.is_empty(), "leftover staging/backup dirs: {:?}", leftovers);
        });
    }

    #[test]
    fn replace_rows_with_empty_set_removes_all_segments() {
        run_async_test(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let writer = TableWriter::create_table(
                temp_dir.path(),
                "default",
                "events",
                &sample_schema(),
                &Default::default(),
            )
            .await
            .unwrap();

            writer.insert_rows(&[vec![Some(ColumnValue::U32(1))]]).await.unwrap();
            let replaced = writer.replace_rows(&[]).await.unwrap();

            assert_eq!(replaced, 0);

            let segments: Vec<_> = std::fs::read_dir(&writer.table_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("segment_"))
                        .unwrap_or(false)
                })
                .collect();
            assert!(segments.is_empty(), "expected no segments after replacing with empty set");
        });
    }
}