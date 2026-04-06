use crate::{catalog::Catalog, compaction::SegmentCompactor, table_writer::TableWriter};
use core::error::AdolapError;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::{
    sync::Mutex,
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};
use tracing::{debug, info, warn};

const SCHEDULER_POLL_INTERVAL_SECONDS: u64 = 5;

#[derive(Clone)]
pub struct BackgroundCompactionScheduler {
    data_root: PathBuf,
    last_attempts: Arc<Mutex<HashMap<PathBuf, Instant>>>,
}

impl BackgroundCompactionScheduler {
    pub fn new(data_root: PathBuf) -> Self {
        Self {
            data_root,
            last_attempts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(error) = self.run_once().await {
                    warn!(error = %error, data_root = %self.data_root.display(), "background compaction pass failed");
                }
                sleep(Duration::from_secs(SCHEDULER_POLL_INTERVAL_SECONDS)).await;
            }
        })
    }

    pub async fn run_once(&self) -> Result<usize, AdolapError> {
        let catalog = Catalog::new(self.data_root.clone());
        let tables = catalog.list_tables().await?;
        let mut compacted_tables = 0usize;

        for table in tables {
            if !table.storage_config.enable_background_compaction {
                continue;
            }

            let interval = Duration::from_secs(table.storage_config.background_compaction_interval_seconds.max(1));
            let mut last_attempts = self.last_attempts.lock().await;
            if let Some(last_attempt) = last_attempts.get(&table.path) {
                if last_attempt.elapsed() < interval {
                    continue;
                }
            }
            last_attempts.insert(table.path.clone(), Instant::now());
            drop(last_attempts);

            let writer = TableWriter::open_at(table.path.clone()).await?;
            let compactor = SegmentCompactor::new(&writer);
            if let Some(report) = compactor.maybe_compact().await? {
                compacted_tables += 1;
                info!(
                    table = %writer.table_dir.display(),
                    input_segments = report.input_segments,
                    output_segments = report.output_segments,
                    "background compaction completed"
                );
            } else {
                debug!(table = %writer.table_dir.display(), "background compaction skipped; thresholds not met");
            }
        }

        Ok(compacted_tables)
    }
}

#[cfg(test)]
mod tests {
    use super::BackgroundCompactionScheduler;
    use crate::{
        column::ColumnValue,
        compaction::discover_segments,
        config::{CompressionType, TableStorageConfig},
        schema::{ColumnSchema, ColumnType, TableSchema},
        table_writer::TableWriter,
    };
    use tempfile::TempDir;
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
                    name: "id".into(),
                    column_type: ColumnType::U32,
                    nullable: false,
                },
                ColumnSchema {
                    name: "name".into(),
                    column_type: ColumnType::Utf8,
                    nullable: false,
                },
            ],
        }
    }

    async fn create_table(temp_dir: &TempDir) -> TableWriter {
        let config = TableStorageConfig {
            row_group_size: 2,
            compression: CompressionType::None,
            enable_bloom_filter: false,
            enable_dictionary_encoding: false,
            compaction_segment_threshold: 2,
            compaction_row_group_threshold: 4,
            enable_background_compaction: true,
            background_compaction_interval_seconds: 1,
        };
        TableWriter::create_table(temp_dir.path(), "default", "events", &sample_schema(), &config)
            .await
            .unwrap()
    }

    #[test]
    fn background_scheduler_compacts_tables_when_thresholds_are_met() {
        run_async_test(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let writer = create_table(&temp_dir).await;
            for index in 0..3 {
                writer
                    .insert_rows(&[vec![
                        Some(ColumnValue::U32(index + 1)),
                        Some(ColumnValue::Utf8(format!("row-{}", index + 1))),
                    ]])
                    .await
                    .unwrap();
            }

            let scheduler = BackgroundCompactionScheduler::new(temp_dir.path().to_path_buf());
            let compacted = scheduler.run_once().await.unwrap();

            assert_eq!(compacted, 1);
            assert_eq!(discover_segments(&writer.table_dir).await.unwrap().len(), 1);
        });
    }
}