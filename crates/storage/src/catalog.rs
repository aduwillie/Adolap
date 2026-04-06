use crate::{
    config::TableStorageConfig,
    naming::TABLE_CONFIG_FILE_NAME,
    schema::TableSchema,
    table_writer::{DEFAULT_DATABASE_NAME, SCHEMA_FILE_NAME, TableWriter},
};
use core::error::AdolapError;
use std::{collections::BTreeMap, path::{Path, PathBuf}};
use tokio::fs;

/// Metadata describing a logical database directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseMetadata {
    pub name: String,
    pub path: PathBuf,
}

/// Metadata required to open or describe a table.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub database: String,
    pub name: String,
    pub path: PathBuf,
    pub schema: TableSchema,
    pub storage_config: TableStorageConfig,
}

impl TableMetadata {
    /// Return the fully-qualified table name as `database.table`.
    pub fn fqn(&self) -> String {
        format!("{}.{}", self.database, self.name)
    }
}

/// Filesystem catalog rooted at the workspace data directory.
#[derive(Debug, Clone)]
pub struct Catalog {
    data_root: PathBuf,
}

impl Catalog {
    /// Create a catalog rooted at a data directory.
    pub fn new(data_root: PathBuf) -> Self {
        Self {
            data_root,
        }
    }

    /// Return the catalog data root.
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    /// List known databases, including a synthetic default database for legacy layouts.
    pub async fn list_databases(&self) -> Result<Vec<DatabaseMetadata>, AdolapError> {
        let mut databases = BTreeMap::new();

        let mut entries = match fs::read_dir(&self.data_root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };

        let mut has_legacy_default_tables = false;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };

            if path.join(SCHEMA_FILE_NAME).is_file() {
                has_legacy_default_tables = true;
                continue;
            }

            tracing::debug!("Discovered database '{}' at {}", name, &path.display());

            databases.insert(
                name.to_string(),
                DatabaseMetadata {
                    name: name.to_string(),
                    path,
                },
            );
        }

        if has_legacy_default_tables && !databases.contains_key(DEFAULT_DATABASE_NAME) {
            databases.insert(
                DEFAULT_DATABASE_NAME.to_string(),
                DatabaseMetadata {
                    name: DEFAULT_DATABASE_NAME.to_string(),
                    path: self.data_root.join(DEFAULT_DATABASE_NAME),
                },
            );
        }

        Ok(databases.into_values().collect())
    }

    /// List tables discovered under canonical and legacy storage layouts.
    pub async fn list_tables(&self) -> Result<Vec<TableMetadata>, AdolapError> {
        let mut tables = BTreeMap::new();

        let mut root_entries = match fs::read_dir(&self.data_root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };

        while let Some(entry) = root_entries.next_entry().await? {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };

            if path.join(SCHEMA_FILE_NAME).is_file() {
                continue;
            }

            let mut table_entries = match fs::read_dir(&path).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };

            while let Some(table_entry) = table_entries.next_entry().await? {
                let table_path = table_entry.path();
                if !table_path.is_dir() || !table_path.join(SCHEMA_FILE_NAME).is_file() {
                    continue;
                }

                let Some(table_name) = table_path.file_name().and_then(|value| value.to_str()) else {
                    continue;
                };

                let metadata = self
                    .load_table_metadata(name.to_string(), table_name.to_string(), table_path)
                    .await?;
                tables.insert(metadata.fqn(), metadata);
            }
        }

        let mut legacy_entries = match fs::read_dir(&self.data_root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(tables.into_values().collect()),
            Err(err) => return Err(err.into()),
        };

        while let Some(entry) = legacy_entries.next_entry().await? {
            let path = entry.path();
            if !path.is_dir() || !path.join(SCHEMA_FILE_NAME).is_file() {
                continue;
            }

            let Some(table_name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };

            let metadata = self
                .load_table_metadata(
                    DEFAULT_DATABASE_NAME.to_string(),
                    table_name.to_string(),
                    path,
                )
                .await?;
            tables.entry(metadata.fqn()).or_insert(metadata);
        }

        Ok(tables.into_values().collect())
    }

    /// Check whether a database exists.
    pub async fn database_exists(&self, database: &str) -> Result<bool, AdolapError> {
        validate_identifier(database, "database")?;
        if database == DEFAULT_DATABASE_NAME {
            if fs::metadata(self.data_root.join(DEFAULT_DATABASE_NAME)).await.is_ok() {
                return Ok(true);
            }

            let mut entries = match fs::read_dir(&self.data_root).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(err) => return Err(err.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.is_dir() && path.join(SCHEMA_FILE_NAME).is_file() {
                    return Ok(true);
                }
            }
            return Ok(false);
        }

        Ok(fs::metadata(self.data_root.join(database)).await.is_ok())
    }

    /// Check whether a table exists.
    pub async fn table_exists(&self, table_ref: &str) -> Result<bool, AdolapError> {
        Ok(self.try_resolve_table(table_ref).await?.is_some())
    }

    /// Resolve a table reference or return a descriptive error.
    pub async fn resolve_table(&self, table_ref: &str) -> Result<TableMetadata, AdolapError> {
        self.try_resolve_table(table_ref).await?.ok_or_else(|| {
            AdolapError::ExecutionError(format!("Table '{}' does not exist", normalize_fqn(table_ref)))
        })
    }

    /// Resolve the target table for row-oriented inserts.
    pub async fn resolve_insert_target(
        &self,
        table_ref: Option<&str>,
    ) -> Result<TableMetadata, AdolapError> {
        match table_ref {
            Some(table_ref) if !table_ref.trim().is_empty() => self.resolve_table(table_ref).await,
            _ => {
                let tables = self.list_tables().await?;
                match tables.as_slice() {
                    [table] => Ok(table.clone()),
                    [] => Err(AdolapError::ExecutionError(
                        "INSERT INTO ROWS requires a table name because no tables were found".into(),
                    )),
                    _ => Err(AdolapError::ExecutionError(
                        "INSERT INTO ROWS requires a table name when more than one table exists"
                            .into(),
                    )),
                }
            }
        }
    }

    /// Create a database directory.
    pub async fn create_database(&self, database: &str) -> Result<DatabaseMetadata, AdolapError> {
        let path = TableWriter::create_database(&self.data_root, database).await?;
        Ok(DatabaseMetadata {
            name: database.to_string(),
            path,
        })
    }

    /// Create a table and return its metadata.
    pub async fn create_table(
        &self,
        table_ref: &str,
        schema: &TableSchema,
        storage_config: &TableStorageConfig,
    ) -> Result<TableMetadata, AdolapError> {
        let (database, table) = split_table_reference(table_ref)?;
        TableWriter::create_table(&self.data_root, &database, &table, schema, storage_config).await?;
        self.resolve_table(&format!("{}.{}", database, table)).await
    }

    /// Open a table writer for an existing table.
    pub async fn open_table_writer(&self, table_ref: &str) -> Result<TableWriter, AdolapError> {
        let table = self.resolve_table(table_ref).await?;
        TableWriter::open_at(table.path).await
    }

    /// Drop a single table.
    pub async fn drop_table(&self, table_ref: &str) -> Result<TableMetadata, AdolapError> {
        let table = self.resolve_table(table_ref).await?;
        fs::remove_dir_all(&table.path).await?;
        Ok(table)
    }

    /// Drop a database and all of its tables.
    pub async fn drop_database(&self, database: &str) -> Result<usize, AdolapError> {
        validate_identifier(database, "database")?;

        let tables = self
            .list_tables()
            .await?
            .into_iter()
            .filter(|table| table.database == database)
            .collect::<Vec<_>>();

        if tables.is_empty() && !self.database_exists(database).await? {
            return Err(AdolapError::ExecutionError(format!(
                "Database '{}' does not exist",
                database
            )));
        }

        if database == DEFAULT_DATABASE_NAME {
            let default_dir = self.data_root.join(DEFAULT_DATABASE_NAME);
            if fs::metadata(&default_dir).await.is_ok() {
                fs::remove_dir_all(default_dir).await?;
            }

            for table in &tables {
                if table.path.parent() == Some(self.data_root.as_path())
                    && fs::metadata(&table.path).await.is_ok()
                {
                    fs::remove_dir_all(&table.path).await?;
                }
            }
        } else {
            let database_dir = self.data_root.join(database);
            if fs::metadata(&database_dir).await.is_ok() {
                fs::remove_dir_all(database_dir).await?;
            }
        }

        Ok(tables.len())
    }

    async fn try_resolve_table(
        &self,
        table_ref: &str,
    ) -> Result<Option<TableMetadata>, AdolapError> {
        let (database, table) = split_table_reference(table_ref)?;
        let canonical_path = self.data_root.join(&database).join(&table);
        if canonical_path.join(SCHEMA_FILE_NAME).is_file() {
            return self
                .load_table_metadata(database, table, canonical_path)
                .await
                .map(Some);
        }

        if database == DEFAULT_DATABASE_NAME {
            let legacy_path = self.data_root.join(&table);
            if legacy_path.join(SCHEMA_FILE_NAME).is_file() {
                return self
                    .load_table_metadata(database, table, legacy_path)
                    .await
                    .map(Some);
            }
        }

        Ok(None)
    }

    async fn load_table_metadata(
        &self,
        database: String,
        name: String,
        path: PathBuf,
    ) -> Result<TableMetadata, AdolapError> {
        let schema = TableSchema::load(&path.join(SCHEMA_FILE_NAME)).await?;
        let storage_config = match fs::metadata(path.join(TABLE_CONFIG_FILE_NAME)).await {
            Ok(_) => TableStorageConfig::load(&path.join(TABLE_CONFIG_FILE_NAME)).await?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => TableStorageConfig::default(),
            Err(err) => return Err(err.into()),
        };
        Ok(TableMetadata {
            database,
            name,
            path,
            schema,
            storage_config,
        })
    }
}

/// Normalize a potentially short table reference into `database.table` form.
pub fn normalize_fqn(table_ref: &str) -> String {
    match split_table_reference(table_ref) {
        Ok((database, table)) => format!("{}.{}", database, table),
        Err(_) => table_ref.trim().to_string(),
    }
}

/// Split a table reference into database and table components.
pub fn split_table_reference(table_ref: &str) -> Result<(String, String), AdolapError> {
    let trimmed = table_ref.trim();
    if trimmed.is_empty() {
        return Err(AdolapError::ExecutionError(
            "Table reference cannot be empty".into(),
        ));
    }

    let (database, table) = match trimmed.split_once('.') {
        Some((database, table)) => (database.trim(), table.trim()),
        None => (DEFAULT_DATABASE_NAME, trimmed),
    };

    validate_identifier(database, "database")?;
    validate_identifier(table, "table")?;
    Ok((database.to_string(), table.to_string()))
}

fn validate_identifier(value: &str, kind: &str) -> Result<(), AdolapError> {
    if value.is_empty() || !value.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(AdolapError::ExecutionError(format!(
            "Invalid {} name: {}",
            kind, value
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, ColumnType};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn temp_catalog() -> (TempDir, Catalog) {
        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = Catalog::new(temp_dir.path().to_path_buf());
        (temp_dir, catalog)
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
                    nullable: true,
                },
            ],
        }
    }

    async fn create_legacy_table(
        data_root: &Path,
        table: &str,
        schema: &TableSchema,
        storage_config: Option<&TableStorageConfig>,
    ) -> PathBuf {
        let table_path = data_root.join(table);
        fs::create_dir_all(&table_path).await.unwrap();
        schema.save(&table_path.join(SCHEMA_FILE_NAME)).await.unwrap();
        if let Some(storage_config) = storage_config {
            storage_config
                .save(&table_path.join(TABLE_CONFIG_FILE_NAME))
                .await
                .unwrap();
        }
        table_path
    }

    fn run_async_test<F>(future: F)
    where
        F: std::future::Future<Output = ()>,
    {
        Runtime::new().unwrap().block_on(future);
    }

    #[test]
    fn test_split_table_reference() {
        assert_eq!(
            split_table_reference("db.table").unwrap(),
            ("db".to_string(), "table".to_string())
        );
        assert_eq!(
            split_table_reference("  db  .  table  ").unwrap(),
            ("db".to_string(), "table".to_string())
        );
        assert_eq!(
            split_table_reference("table").unwrap(),
            (DEFAULT_DATABASE_NAME.to_string(), "table".to_string())
        );
        assert!(split_table_reference("").is_err());
        assert!(split_table_reference("invalid-table").is_err());
        assert!(split_table_reference("invalid.table.name").is_err());
    }

    #[test]
    fn list_databases_includes_canonical_and_synthetic_default() {
        run_async_test(async {
            let (_temp_dir, catalog) = temp_catalog();
            let schema = sample_schema();
            let storage_config = TableStorageConfig::default();

            catalog.create_database("warehouse").await.unwrap();
            catalog
                .create_table("analytics.events", &schema, &storage_config)
                .await
                .unwrap();
            create_legacy_table(catalog.data_root(), "visits", &schema, None).await;

            let databases = catalog.list_databases().await.unwrap();
            let names = databases
                .iter()
                .map(|database| database.name.as_str())
                .collect::<Vec<_>>();

            assert_eq!(names, vec!["analytics", "default", "warehouse"]);
            assert_eq!(
                databases
                    .iter()
                    .find(|database| database.name == DEFAULT_DATABASE_NAME)
                    .unwrap()
                    .path,
                catalog.data_root().join(DEFAULT_DATABASE_NAME)
            );
        });
    }

    #[test]
    fn list_tables_discovers_canonical_and_legacy_tables() {
        run_async_test(async {
            let (_temp_dir, catalog) = temp_catalog();
            let schema = sample_schema();
            let storage_config = TableStorageConfig::default();

            catalog
                .create_table("analytics.events", &schema, &storage_config)
                .await
                .unwrap();
            create_legacy_table(
                catalog.data_root(),
                "visits",
                &schema,
                Some(&storage_config),
            )
            .await;

            let tables = catalog.list_tables().await.unwrap();
            let names = tables.iter().map(TableMetadata::fqn).collect::<Vec<_>>();

            assert_eq!(names, vec!["analytics.events", "default.visits"]);
            assert_eq!(tables[0].schema.columns.len(), 2);
            assert_eq!(tables[1].storage_config.row_group_size, storage_config.row_group_size);
        });
    }

    #[test]
    fn database_exists_treats_default_as_virtual_for_legacy_tables() {
        run_async_test(async {
            let (_temp_dir, catalog) = temp_catalog();
            let schema = sample_schema();

            create_legacy_table(catalog.data_root(), "visits", &schema, None).await;
            catalog.create_database("analytics").await.unwrap();

            assert!(catalog.database_exists(DEFAULT_DATABASE_NAME).await.unwrap());
            assert!(catalog.database_exists("analytics").await.unwrap());
            assert!(!catalog.database_exists("missing").await.unwrap());
        });
    }

    #[test]
    fn resolve_table_prefers_canonical_default_path_over_legacy_fallback() {
        run_async_test(async {
            let (_temp_dir, catalog) = temp_catalog();
            let schema = sample_schema();
            let storage_config = TableStorageConfig::default();

            let legacy_path = create_legacy_table(catalog.data_root(), "orders", &schema, None).await;
            let canonical = catalog
                .create_table("default.orders", &schema, &storage_config)
                .await
                .unwrap();

            let resolved = catalog.resolve_table("orders").await.unwrap();

            assert_eq!(resolved.path, canonical.path);
            assert_ne!(resolved.path, legacy_path);
            assert_eq!(resolved.fqn(), "default.orders");
        });
    }

    #[test]
    fn resolve_insert_target_requires_name_when_zero_or_multiple_tables_exist() {
        run_async_test(async {
            let (_temp_dir, catalog) = temp_catalog();

            match catalog.resolve_insert_target(None).await.unwrap_err() {
                AdolapError::ExecutionError(message) => {
                    assert!(message.contains("no tables were found"));
                }
                other => panic!("expected execution error, got {:?}", other),
            }

            let schema = sample_schema();
            let storage_config = TableStorageConfig::default();
            let only_table = catalog
                .create_table("analytics.events", &schema, &storage_config)
                .await
                .unwrap();

            assert_eq!(
                catalog.resolve_insert_target(None).await.unwrap().fqn(),
                only_table.fqn()
            );

            catalog
                .create_table("analytics.users", &schema, &storage_config)
                .await
                .unwrap();

            match catalog.resolve_insert_target(None).await.unwrap_err() {
                AdolapError::ExecutionError(message) => {
                    assert!(message.contains("more than one table exists"));
                }
                other => panic!("expected execution error, got {:?}", other),
            }
        });
    }

    #[test]
    fn drop_default_database_removes_canonical_and_legacy_tables_only() {
        run_async_test(async {
            let (_temp_dir, catalog) = temp_catalog();
            let schema = sample_schema();
            let storage_config = TableStorageConfig::default();

            let canonical_default = catalog
                .create_table("default.orders", &schema, &storage_config)
                .await
                .unwrap();
            let legacy_default = create_legacy_table(catalog.data_root(), "visits", &schema, None).await;
            let analytics_table = catalog
                .create_table("analytics.events", &schema, &storage_config)
                .await
                .unwrap();

            let dropped_count = catalog.drop_database(DEFAULT_DATABASE_NAME).await.unwrap();

            assert_eq!(dropped_count, 2);
            assert!(fs::metadata(&canonical_default.path).await.is_err());
            assert!(fs::metadata(&legacy_default).await.is_err());
            assert!(fs::metadata(&analytics_table.path).await.is_ok());

            let remaining_tables = catalog
                .list_tables()
                .await
                .unwrap()
                .into_iter()
                .map(|table| table.fqn())
                .collect::<Vec<_>>();
            assert_eq!(remaining_tables, vec!["analytics.events"]);
        });
    }
}
