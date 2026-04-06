//! Request dispatcher for binary protocol messages.
//!
//! This module stays focused on translating wire messages into execution-layer
//! operations. Query execution and metadata inspection are delegated to helper
//! modules so the top-level request flow stays readable.

use crate::meta::handle_meta_command;
use protocol::{
    ClientMessage, ColumnDefinition, ColumnType as ProtocolColumnType, QueryResult,
    ResultRow, ResultSet, ScalarValue, ServerMessage,
};
use storage::record_batch::RecordBatch;
use storage::schema::{ColumnSchema, ColumnType, TableSchema};
use storage::{
    catalog::{Catalog, normalize_fqn},
    null::is_null,
    table_writer::TableWriter,
};
use adolap_core::error::AdolapError;
use std::path::{Path, PathBuf};

use exec::logical_plan::LogicalPlan;
use exec::parser::{parse_statement, Statement};
use exec::optimizer;
use exec::planner;
use exec::executor::Executor;
use tracing::{debug, info};

const DATA_ROOT: &str = "data";

pub async fn handle_message(msg: ClientMessage) -> Result<ServerMessage, AdolapError> {
    let catalog = Catalog::new(PathBuf::from(DATA_ROOT));
    match msg {
        ClientMessage::Ping => {
                debug!("handling ping request");
                Ok(ServerMessage::Pong)
        },

    ClientMessage::QueryText(text) => {
            info!(query_len = text.len(), query_lines = text.lines().count(), "handling text query");
      match parse_statement(&text)? {
                Statement::Query(plan) => execute_query(&catalog, plan).await,
        Statement::CreateDatabase { name } => {
                                        info!(database = %name, "creating database from text query");
                    catalog.create_database(&name).await?;
                    ok_message(format!("database '{}' created", name))
        }
                Statement::DropDatabase { name } => {
                                        info!(database = %name, "dropping database from text query");
                    let dropped_tables = catalog.drop_database(&name).await?;
                                        ok_message(format!("database '{}' dropped ({} table(s) removed)", name, dropped_tables))
                }
        Statement::CreateTable { table, schema, storage_config } => {
                                info!(table = %table, columns = schema.columns.len(), row_group_size = storage_config.row_group_size, "creating table from text query");
                let table = catalog.create_table(&table, &schema, &storage_config).await?;
                                        ok_message(format!("table '{}' created", table.fqn()))
                }
                Statement::DropTable { table } => {
                                        info!(table = %table, "dropping table from text query");
                    let dropped = catalog.drop_table(&table).await?;
                                        ok_message(format!("table '{}' dropped", dropped.fqn()))
        }
        Statement::InsertRows { table, rows } => {
                                        info!(table = ?table, row_count = rows.len(), "inserting rows from text query");
                    let table = catalog.resolve_insert_target(table.as_deref()).await?;
                    table.schema.validate_rows(&rows)?;
                    let writer = TableWriter::open_at(table.path.clone()).await?;
          let inserted = writer.insert_rows(&rows).await?;
                                        ok_message(format!("inserted {} row(s) into {}", inserted, table.fqn()))
                }
                Statement::DeleteRows { table, predicate } => {
                    info!(table = %table, has_predicate = predicate.is_some(), "deleting rows from text query");
                    let deleted = delete_rows(&catalog, &table, predicate.as_ref()).await?;
                                        ok_message(format!("deleted {} row(s) from {}", deleted, normalize_fqn(&table)))
        }
        Statement::IngestInto { table, file_path } => {
                    info!(table = %table, file_path = %file_path, "ingesting rows from text query");
                    let writer = catalog.open_table_writer(&table).await?;
          let inserted = writer.ingest_json_file(Path::new(&file_path)).await?;
                                        ok_message(format!("ingested {} row(s) into {} from {}", inserted, normalize_fqn(&table), file_path))
        }
      }
    }
        ClientMessage::CreateDatabase { name } => {
                        info!(database = %name, "creating database from protocol command");
                        catalog.create_database(&name).await?;
            ok_message(format!("database '{}' created", name))
        }
        ClientMessage::CreateTable { table, columns } => {
            info!(table = %table, columns = columns.len(), "creating table from protocol command");
            let schema = protocol_columns_to_schema(columns)?;
                        let table = catalog.create_table(&table, &schema, &storage::config::TableStorageConfig::default()).await?;
                        ok_message(format!("table '{}' created", table.fqn()))
        }
        ClientMessage::InsertRows { table, rows } => {
                        info!(table = ?table, row_count = rows.len(), "inserting rows from protocol command");
                        let rows = protocol_rows_to_storage_rows(rows);
                        let table = catalog.resolve_insert_target(table.as_deref()).await?;
                        table.schema.validate_rows(&rows)?;
                        let writer = TableWriter::open_at(table.path.clone()).await?;
                        let inserted = writer.insert_rows(&rows).await?;
                        ok_message(format!("inserted {} row(s) into {}", inserted, table.fqn()))
        }
        ClientMessage::IngestInto { table, file_path } => {
                        info!(table = %table, file_path = %file_path, "ingesting rows from protocol command");
                        let writer = catalog.open_table_writer(&table).await?;
            let inserted = writer.ingest_json_file(Path::new(&file_path)).await?;
                        ok_message(format!("ingested {} row(s) into {} from {}", inserted, normalize_fqn(&table), file_path))
        }
                ClientMessage::MetaCommand(command) => {
                    info!(command = %command, "handling meta command");
                    handle_meta_command(&catalog, &command).await
                },
  }
}

async fn execute_query(catalog: &Catalog, plan: LogicalPlan) -> Result<ServerMessage, AdolapError> {
        debug!(?plan, "optimizing logical plan");
        let optimized = optimizer::optimize(plan);
        let bound = planner::bind_plan(catalog, optimized).await?;
        let physical = planner::create_physical_plan(&bound)?;
        let logical_plan = format!("{:#?}", bound);
        let physical_plan = format!("{:#?}", physical);

    let executor = Executor::new();
    let batches = executor.execute(&physical).await?;
    info!(batch_count = batches.len(), row_count = batches.iter().map(|batch| batch.row_count).sum::<usize>(), "query execution completed");
    Ok(ServerMessage::QueryResult(QueryResult::new(
        convert_batches_to_resultset(batches)?,
        logical_plan,
        physical_plan,
    )))
}

fn ok_message(message: String) -> Result<ServerMessage, AdolapError> {
    Ok(ServerMessage::Ok(message))
}

fn protocol_columns_to_schema(columns: Vec<ColumnDefinition>) -> Result<TableSchema, AdolapError> {
    if columns.is_empty() {
        return Err(AdolapError::ExecutionError(
            "CREATE TABLE requires at least one column".into(),
        ));
    }

    Ok(TableSchema {
        columns: columns
            .into_iter()
            .map(|column| ColumnSchema {
                name: column.name,
                column_type: match column.column_type {
                    ProtocolColumnType::Utf8 => ColumnType::Utf8,
                    ProtocolColumnType::I32 => ColumnType::I32,
                    ProtocolColumnType::U32 => ColumnType::U32,
                    ProtocolColumnType::Bool => ColumnType::Bool,
                },
                nullable: column.nullable,
            })
            .collect(),
    })
}

fn protocol_rows_to_storage_rows(rows: Vec<Vec<ScalarValue>>) -> Vec<Vec<Option<storage::column::ColumnValue>>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    ScalarValue::Null => None,
                    ScalarValue::Utf8(value) => Some(storage::column::ColumnValue::Utf8(value)),
                    ScalarValue::I32(value) => Some(storage::column::ColumnValue::I32(value)),
                    ScalarValue::U32(value) => Some(storage::column::ColumnValue::U32(value)),
                    ScalarValue::Bool(value) => Some(storage::column::ColumnValue::Bool(value)),
                })
                .collect()
        })
        .collect()
}

fn convert_batches_to_resultset(
    batches: Vec<RecordBatch>,
) -> Result<ResultSet, AdolapError> {
    if batches.is_empty() {
        return Ok(ResultSet::new(vec![], vec![]));
    }

    let batch = &batches[0];

    let columns = batch
        .schema
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect::<Vec<_>>();

    let mut rows = Vec::new();

    for row_idx in 0..batch.row_count {
        let mut values = Vec::new();

        for col in &batch.columns {
            let v = if is_null(col.validity.as_deref(), row_idx) {
                "NULL".to_string()
            } else {
                match &col.values {
                    storage::column::ColumnValuesOwned::I32(vs) => vs[row_idx].to_string(),
                    storage::column::ColumnValuesOwned::U32(vs) => vs[row_idx].to_string(),
                    storage::column::ColumnValuesOwned::Bool(vs) => vs[row_idx].to_string(),
                    storage::column::ColumnValuesOwned::Utf8(vs) => vs[row_idx].clone(),
                }
            };
            values.push(v);
        }

        rows.push(ResultRow::new(values));
    }

    Ok(ResultSet::new(columns, rows))
}

async fn delete_rows(
    catalog: &Catalog,
    table_ref: &str,
    predicate: Option<&exec::predicate::Expr>,
) -> Result<usize, AdolapError> {
    let table = catalog.resolve_table(table_ref).await?;
    let writer = TableWriter::open_at(table.path.clone()).await?;
    let table_reader = storage::table_reader::TableReader::new(&table.path, &table.schema);
    let batches = table_reader.read_table(None, None).await?;

    let mut survivors = Vec::new();
    let mut deleted = 0usize;

    for batch in batches {
        let delete_mask = match predicate {
            Some(predicate) => predicate.evaluate_to_bool_mask(&batch)?,
            None => vec![true; batch.row_count],
        };

        for (row, should_delete) in batch.to_rows()?.into_iter().zip(delete_mask) {
            if should_delete {
                deleted += 1;
            } else {
                survivors.push(row);
            }
        }
    }

    writer.replace_rows(&survivors).await?;
    info!(table = %table_ref, deleted, survivors = survivors.len(), "delete completed");
    Ok(deleted)
}
