//! Request dispatcher for binary protocol messages.
//!
//! This module stays focused on translating wire messages into execution-layer
//! operations. Query execution and metadata inspection are delegated to helper
//! modules so the top-level request flow stays readable.

use crate::meta::handle_meta_command;
use adolap_core::error::AdolapError;
use exec::aggregate::AggFunc;
use exec::executor::Executor;
use exec::logical_plan::{LogicalPlan, OrderBy, OrderDirection};
use exec::optimizer;
use exec::parser::{parse_statement, Statement};
use exec::physical_plan::PhysicalPlan;
use exec::planner;
use protocol::{
    ClientMessage, ColumnDefinition, ColumnType as ProtocolColumnType, QueryResult, QuerySummary,
    ResultRow, ResultSet, ScalarValue, ServerMessage,
};
use std::path::{Path, PathBuf};
use std::time::Instant;
use storage::record_batch::RecordBatch;
use storage::schema::{ColumnSchema, ColumnType, TableSchema};
use storage::{
    catalog::{Catalog, normalize_fqn},
    null::is_null,
    table_writer::TableWriter,
};
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
    let started = Instant::now();
    debug!(?plan, "optimizing logical plan");
    let optimized = optimizer::optimize(plan);
    let bound = planner::bind_plan(catalog, optimized).await?;
    let physical = planner::create_physical_plan(&bound)?;
    let logical_plan = format_logical_plan(&bound);
    let physical_plan = format_physical_plan(&physical);

    let executor = Executor::new();
    let batches = executor.execute(&physical).await?;
    let batch_count = batches.len();
    let row_count = batches.iter().map(|batch| batch.row_count).sum::<usize>();
    let result_set = convert_batches_to_resultset(batches)?;
    let summary = QuerySummary::new(
        row_count,
        result_set.columns.len(),
        batch_count,
        started.elapsed().as_secs_f64() * 1000.0,
    );
    info!(batch_count, row_count, execution_ms = summary.execution_time_ms, "query execution completed");
    Ok(ServerMessage::QueryResult(QueryResult::new(
        result_set,
        logical_plan,
        physical_plan,
        summary,
    )))
}

pub(crate) fn format_logical_plan(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::Scan { table_ref, .. } => format!("Scan({})", table_ref),
        LogicalPlan::Filter { input, predicate } => {
            format!("{} -> Filter({})", format_logical_plan(input), format_expr(predicate))
        }
        LogicalPlan::Project { input, columns } => {
            format!("{} -> Project({})", format_logical_plan(input), format_list(columns))
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            agg_column,
            agg_func,
        } => format!(
            "{} -> Aggregate(keys=[{}], {}({}))",
            format_logical_plan(input),
            format_list(group_keys),
            format_agg_func(agg_func),
            agg_column
        ),
        LogicalPlan::GroupFilter { input, predicate } => format!(
            "{} -> GroupFilter({})",
            format_logical_plan(input),
            format_expr(predicate)
        ),
        LogicalPlan::Join {
            left,
            right,
            left_on,
            right_on,
            ..
        } => format!(
            "HashJoin(on={} = {}, left=[{}], right=[{}])",
            left_on,
            right_on,
            format_logical_plan(left),
            format_logical_plan(right)
        ),
        LogicalPlan::Sort { input, order_by } => format!(
            "{} -> Sort({})",
            format_logical_plan(input),
            format_order_by(order_by)
        ),
        LogicalPlan::Limit { input, limit, offset } => format!(
            "{} -> Limit(limit={}, offset={})",
            format_logical_plan(input),
            limit.map(|value| value.to_string()).unwrap_or_else(|| "none".into()),
            offset
        ),
    }
}

pub(crate) fn format_physical_plan(plan: &PhysicalPlan<'_>) -> String {
    match plan {
        PhysicalPlan::Scan {
            table,
            projected_columns,
            predicate,
        } => {
            let mut attributes = vec![format!("table={}", table.fqn())];
            if let Some(columns) = projected_columns {
                attributes.push(format!("projection=[{}]", format_list(columns)));
            }
            if let Some(predicate) = predicate {
                attributes.push(format!("predicate={}", format_pushdown_predicate(predicate)));
            }
            format!("Scan({})", attributes.join(", "))
        }
        PhysicalPlan::Filter { input, predicate } => {
            format!("{} -> Filter({})", format_physical_plan(input), format_expr(predicate))
        }
        PhysicalPlan::Project { input, columns } => {
            format!("{} -> Project({})", format_physical_plan(input), format_list(columns))
        }
        PhysicalPlan::HashAggregate {
            input,
            group_keys,
            agg_column,
            agg_func,
        } => format!(
            "{} -> HashAggregate(keys=[{}], {}({}))",
            format_physical_plan(input),
            format_list(group_keys),
            format_agg_func(agg_func),
            agg_column
        ),
        PhysicalPlan::GroupFilter { input, predicate } => format!(
            "{} -> GroupFilter({})",
            format_physical_plan(input),
            format_expr(predicate)
        ),
        PhysicalPlan::HashJoin {
            left,
            right,
            left_on,
            right_on,
            ..
        } => format!(
            "HashJoin(on={} = {}, left=[{}], right=[{}])",
            left_on,
            right_on,
            format_physical_plan(left),
            format_physical_plan(right)
        ),
        PhysicalPlan::Sort { input, order_by } => format!(
            "{} -> Sort({})",
            format_physical_plan(input),
            format_order_by(order_by)
        ),
        PhysicalPlan::Limit { input, limit, offset } => format!(
            "{} -> Limit(limit={}, offset={})",
            format_physical_plan(input),
            limit.map(|value| value.to_string()).unwrap_or_else(|| "none".into()),
            offset
        ),
    }
}

fn format_list(values: &[String]) -> String {
    if values.is_empty() {
        "*".to_string()
    } else {
        values.join(", ")
    }
}

fn format_order_by(order_by: &[OrderBy]) -> String {
    order_by
        .iter()
        .map(|order| format!("{} {}", format_expr(&order.expr), format_order_direction(order.direction)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_order_direction(direction: OrderDirection) -> &'static str {
    match direction {
        OrderDirection::Asc => "ASC",
        OrderDirection::Desc => "DESC",
    }
}

fn format_expr(expr: &exec::predicate::Expr) -> String {
    match expr {
        exec::predicate::Expr::Column(name) => name.clone(),
        exec::predicate::Expr::Aggregate { func, column } => {
            format!("{}({})", format_agg_func(func), column)
        }
        exec::predicate::Expr::Literal(literal) => format_literal(literal),
        exec::predicate::Expr::Eq(lhs, rhs) => format!("{} = {}", format_expr(lhs), format_expr(rhs)),
        exec::predicate::Expr::Gt(lhs, rhs) => format!("{} > {}", format_expr(lhs), format_expr(rhs)),
        exec::predicate::Expr::Lt(lhs, rhs) => format!("{} < {}", format_expr(lhs), format_expr(rhs)),
        exec::predicate::Expr::And(lhs, rhs) => format!("({} AND {})", format_expr(lhs), format_expr(rhs)),
        exec::predicate::Expr::Or(lhs, rhs) => format!("({} OR {})", format_expr(lhs), format_expr(rhs)),
    }
}

fn format_literal(literal: &exec::predicate::Literal) -> String {
    match literal {
        exec::predicate::Literal::Utf8(value) => format!("\"{}\"", value),
        exec::predicate::Literal::I32(value) => value.to_string(),
        exec::predicate::Literal::U32(value) => value.to_string(),
        exec::predicate::Literal::Bool(value) => value.to_string(),
    }
}

fn format_pushdown_predicate(predicate: &storage::segment_reader::Predicate) -> String {
    match predicate {
        storage::segment_reader::Predicate::Equals(column, value) => {
            format!("{} = {}", column, format_column_value(value))
        }
        storage::segment_reader::Predicate::GreaterThan(column, value) => {
            format!("{} > {}", column, format_column_value(value))
        }
        storage::segment_reader::Predicate::LessThan(column, value) => {
            format!("{} < {}", column, format_column_value(value))
        }
        storage::segment_reader::Predicate::And(predicates) => predicates
            .iter()
            .map(format_pushdown_predicate)
            .collect::<Vec<_>>()
            .join(" AND "),
    }
}

fn format_column_value(value: &storage::column::ColumnValue) -> String {
    match value {
        storage::column::ColumnValue::Utf8(value) => format!("\"{}\"", value),
        storage::column::ColumnValue::I32(value) => value.to_string(),
        storage::column::ColumnValue::U32(value) => value.to_string(),
        storage::column::ColumnValue::Bool(value) => value.to_string(),
    }
}

fn format_agg_func(func: &AggFunc) -> &'static str {
    match func {
        AggFunc::Count => "COUNT",
        AggFunc::Sum => "SUM",
        AggFunc::Avg => "AVG",
        AggFunc::Min => "MIN",
        AggFunc::Max => "MAX",
    }
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

#[cfg(test)]
mod tests {
    use super::{convert_batches_to_resultset, format_logical_plan, format_physical_plan, handle_message, protocol_rows_to_storage_rows};
    use exec::{aggregate::AggFunc, logical_plan::LogicalPlan, physical_plan::PhysicalPlan, predicate::{col, lit_i32}};
    use protocol::{ClientMessage, ScalarValue, ServerMessage};
    use storage::{
        catalog::TableMetadata,
        column::ColumnValue,
        config::TableStorageConfig,
        record_batch::RecordBatch,
        segment_reader::Predicate,
        schema::{ColumnSchema, ColumnType, TableSchema},
    };
    use std::path::PathBuf;
    use tokio::runtime::Runtime;

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
                    nullable: true,
                },
            ],
        }
    }

    #[test]
    fn ping_returns_pong() {
        Runtime::new().unwrap().block_on(async {
            match handle_message(ClientMessage::Ping).await.unwrap() {
                ServerMessage::Pong => {}
                other => panic!("unexpected response: {:?}", other),
            }
        });
    }

    #[test]
    fn converts_protocol_rows_to_storage_rows() {
        let rows = protocol_rows_to_storage_rows(vec![vec![
            ScalarValue::U32(7),
            ScalarValue::Utf8("alpha".into()),
            ScalarValue::Null,
        ]]);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some(ColumnValue::U32(7)));
        assert_eq!(rows[0][1], Some(ColumnValue::Utf8("alpha".into())));
        assert_eq!(rows[0][2], None);
    }

    #[test]
    fn converts_batches_to_resultset_and_renders_nulls() {
        let batch = RecordBatch::from_rows(
            sample_schema(),
            &[
                vec![Some(ColumnValue::U32(1)), Some(ColumnValue::Utf8("alpha".into()))],
                vec![Some(ColumnValue::U32(2)), None],
            ],
        )
        .unwrap();

        let result_set = convert_batches_to_resultset(vec![batch]).unwrap();

        assert_eq!(result_set.columns, vec!["id", "name"]);
        assert_eq!(result_set.rows.len(), 2);
        assert_eq!(result_set.rows[1].values, vec!["2", "NULL"]);
    }

    #[test]
    fn formats_logical_plans_as_compact_arrows() {
        let plan = LogicalPlan::scan("analytics.events")
            .filter(col("revenue").gt(lit_i32(10)))
            .project(vec!["country", "revenue"])
            .aggregate(vec!["country"], "revenue", AggFunc::Sum);

        let rendered = format_logical_plan(&plan);

        assert!(rendered.contains("Scan(analytics.events) -> Filter(revenue > 10)"));
        assert!(rendered.contains("-> Project(country, revenue)"));
        assert!(rendered.contains("-> Aggregate(keys=[country], SUM(revenue))"));
        assert!(!rendered.contains('{'));
        assert!(!rendered.contains('}'));
    }

    #[test]
    fn formats_physical_plans_as_compact_arrows() {
        let table = TableMetadata {
            database: "analytics".into(),
            name: "events".into(),
            path: PathBuf::from("data/analytics/events"),
            schema: sample_schema(),
            storage_config: TableStorageConfig::default(),
        };
        let plan = PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::Scan {
                table: &table,
                projected_columns: Some(vec!["country".into()]),
                predicate: Some(Predicate::GreaterThan("revenue".into(), ColumnValue::I32(10))),
            }),
            columns: vec!["country".into()],
        };

        let rendered = format_physical_plan(&plan);

        assert!(rendered.contains("Scan(table=analytics.events, projection=[country], predicate=revenue > 10)"));
        assert!(rendered.ends_with("-> Project(country)"));
        assert!(!rendered.contains('{'));
        assert!(!rendered.contains('}'));
    }
}
