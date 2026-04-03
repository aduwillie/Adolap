//! Server-side implementation of CLI meta commands.
//!
//! The CLI forwards commands such as `\\databases`, `\\schema db.table`, and
//! `\\explain ...` to the server so metadata is always inspected relative to the
//! active server process rather than the client's local filesystem.

use adolap_core::error::AdolapError;
use exec::optimizer;
use exec::parser::{Statement, parse_statement};
use exec::planner;
use protocol::{MetaResult, ServerMessage};
use std::cmp::Ordering;
use std::fmt::Write as _;
use std::path::Path;
use storage::catalog::{Catalog, TableMetadata};
use storage::column::ColumnValue;
use storage::metadata::SegmentMetadata;
use storage::metadata_io::read_segment_metadata;
use storage::schema::ColumnType;
use sysinfo::{ProcessesToUpdate, System};
use tokio::fs;
use tracing::{debug, info};

/// Execute a server-side meta command and return its formatted response.
pub async fn handle_meta_command(catalog: &Catalog, command: &str) -> Result<ServerMessage, AdolapError> {
    let trimmed = command.trim();
    let mut parts = trimmed.split_whitespace();
    let verb = parts.next().unwrap_or_default().to_ascii_lowercase();
    let args = parts.collect::<Vec<_>>();
    debug!(verb = %verb, arg_count = args.len(), "executing server meta command");

    match verb.as_str() {
        "databases" => meta_message("Databases", format_databases(&catalog.list_databases().await?)),
        "tables" => meta_message("Tables", format_tables(&catalog.list_tables().await?, args.first().copied())),
        "schema" => {
            let table_ref = required_table_arg(&args, "schema")?;
            meta_message("Schema", format_schema(&catalog.resolve_table(table_ref).await?))
        }
        "segments" => {
            let table_ref = required_table_arg(&args, "segments")?;
            let table = catalog.resolve_table(table_ref).await?;
            let segments = read_all_segment_metadata(&table.path).await?;
            meta_message("Segments", format_segments(&table, &segments))
        }
        "storage" => {
            let table_ref = required_table_arg(&args, "storage")?;
            meta_message("Storage", format_storage(&catalog.resolve_table(table_ref).await?))
        }
        "stats" => {
            let table_ref = required_table_arg(&args, "stats")?;
            let table = catalog.resolve_table(table_ref).await?;
            let segments = read_all_segment_metadata(&table.path).await?;
            meta_message("Table Stats", format_stats(&table, &segments)?)
        }
        "explain" => {
            let query = trimmed[verb.len()..].trim();
            if query.is_empty() {
                return Err(AdolapError::ExecutionError("EXPLAIN requires an inline query".into()));
            }
            info!(query_len = query.len(), "building explain output");
            meta_message("Explain", explain_query(catalog, query).await?)
        }
        other => Err(AdolapError::ExecutionError(format!("Unknown meta command: {}", other))),
    }
}

fn meta_message(title: impl Into<String>, content: impl Into<String>) -> Result<ServerMessage, AdolapError> {
    Ok(ServerMessage::MetaResult(MetaResult::new(title.into(), content.into())))
}

fn required_table_arg<'a>(args: &'a [&str], command: &str) -> Result<&'a str, AdolapError> {
    args.first()
        .copied()
        .ok_or_else(|| AdolapError::ExecutionError(format!("{} requires a table reference", command)))
}

fn format_databases(databases: &[storage::catalog::DatabaseMetadata]) -> String {
    if databases.is_empty() {
        return "No databases found".to_string();
    }

    databases
        .iter()
        .map(|database| database.name.clone())
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_tables(tables: &[TableMetadata], database: Option<&str>) -> String {
    let filtered = tables
        .iter()
        .filter(|table| database.map(|database| table.database == database).unwrap_or(true))
        .map(|table| {
            format!(
                "{}\tcolumns={}\tpath={}",
                table.fqn(),
                table.schema.columns.len(),
                table.path.display()
            )
        })
        .collect::<Vec<_>>();

    if filtered.is_empty() {
        "No tables found".to_string()
    } else {
        filtered.join("\n")
    }
}

fn format_schema(table: &TableMetadata) -> String {
    let mut output = String::new();
    let _ = writeln!(&mut output, "table: {}", table.fqn());
    let _ = writeln!(&mut output, "path: {}", table.path.display());
    let _ = writeln!(&mut output, "columns:");
    for column in &table.schema.columns {
        let _ = writeln!(
            &mut output,
            "  - {}: {:?}{}",
            column.name,
            column.column_type,
            if column.nullable { " nullable" } else { "" }
        );
    }
    output.trim_end().to_string()
}

fn format_storage(table: &TableMetadata) -> String {
    format!(
        "table: {}\ncompression: {:?}\nrow_group_size: {}\ndictionary_encoding: {}\nbloom_filter: {}",
        table.fqn(),
        table.storage_config.compression,
        table.storage_config.row_group_size,
        table.storage_config.enable_dictionary_encoding,
        table.storage_config.enable_bloom_filter,
    )
}

fn format_segments(table: &TableMetadata, segments: &[SegmentMetadata]) -> String {
    if segments.is_empty() {
        return format!("table: {}\nNo segments found", table.fqn());
    }

    let mut output = String::new();
    let _ = writeln!(&mut output, "table: {}", table.fqn());
    for (index, segment) in segments.iter().enumerate() {
        let _ = writeln!(
            &mut output,
            "segment_{}\trows={}\trow_groups={}\tsize_bytes={}\tcreated_at={}",
            index,
            segment.total_rows,
            segment.row_groups.len(),
            segment.total_size_bytes,
            segment.created_at,
        );
    }
    output.trim_end().to_string()
}

fn format_stats(table: &TableMetadata, segments: &[SegmentMetadata]) -> Result<String, AdolapError> {
    let total_rows = segments.iter().map(|segment| segment.total_rows as u64).sum::<u64>();
    let total_size = segments.iter().map(|segment| segment.total_size_bytes as u64).sum::<u64>();
    let total_row_groups = segments.iter().map(|segment| segment.row_groups.len()).sum::<usize>();

    let mut output = String::new();
    let _ = writeln!(&mut output, "table: {}", table.fqn());
    let _ = writeln!(&mut output, "segments: {}", segments.len());
    let _ = writeln!(&mut output, "row_groups: {}", total_row_groups);
    let _ = writeln!(&mut output, "rows: {}", total_rows);
    let _ = writeln!(&mut output, "size_bytes: {}", total_size);
    let _ = writeln!(&mut output, "column_stats:");

    for (column_index, column) in table.schema.columns.iter().enumerate() {
        let mut null_count = 0u64;
        let mut distinct_sum = 0u64;
        let mut min = None;
        let mut max = None;

        for segment in segments {
            for row_group in &segment.row_groups {
                let Some(chunk) = row_group.columns.get(column_index) else {
                    continue;
                };
                null_count += chunk.stats.null_count as u64;
                distinct_sum += chunk.stats.distinct_count as u64;
                merge_stats(&mut min, &mut max, &column.column_type, &chunk.stats.min, &chunk.stats.max)?;
            }
        }

        let _ = writeln!(
            &mut output,
            "  - {}: min={} max={} null_count={} row_group_distinct_sum={}",
            column.name,
            min.unwrap_or_else(|| "n/a".to_string()),
            max.unwrap_or_else(|| "n/a".to_string()),
            null_count,
            distinct_sum,
        );
    }

    Ok(output.trim_end().to_string())
}

fn merge_stats(
    current_min: &mut Option<String>,
    current_max: &mut Option<String>,
    column_type: &ColumnType,
    new_min: &Option<Vec<u8>>,
    new_max: &Option<Vec<u8>>,
) -> Result<(), AdolapError> {
    if let Some(value) = decode_stat_value(column_type, new_min)? {
        if current_min
            .as_ref()
            .map(|current| compare_stat_strings(column_type, &value, current) == Ordering::Less)
            .unwrap_or(true)
        {
            *current_min = Some(value);
        }
    }

    if let Some(value) = decode_stat_value(column_type, new_max)? {
        if current_max
            .as_ref()
            .map(|current| compare_stat_strings(column_type, &value, current) == Ordering::Greater)
            .unwrap_or(true)
        {
            *current_max = Some(value);
        }
    }

    Ok(())
}

fn compare_stat_strings(column_type: &ColumnType, left: &str, right: &str) -> Ordering {
    match column_type {
        ColumnType::Utf8 => left.cmp(right),
        ColumnType::I32 => left
            .parse::<i32>()
            .unwrap_or_default()
            .cmp(&right.parse::<i32>().unwrap_or_default()),
        ColumnType::U32 => left
            .parse::<u32>()
            .unwrap_or_default()
            .cmp(&right.parse::<u32>().unwrap_or_default()),
        ColumnType::Bool => left.cmp(right),
    }
}

fn decode_stat_value(column_type: &ColumnType, bytes: &Option<Vec<u8>>) -> Result<Option<String>, AdolapError> {
    let Some(bytes) = bytes else {
        return Ok(None);
    };

    let value = match column_type {
        ColumnType::Utf8 => postcard::from_bytes::<String>(bytes)
            .map(ColumnValue::Utf8)
            .map_err(|error| AdolapError::Serialization(format!("Cannot decode Utf8 stats value: {}", error)))?,
        ColumnType::I32 => postcard::from_bytes::<i32>(bytes)
            .map(ColumnValue::I32)
            .map_err(|error| AdolapError::Serialization(format!("Cannot decode I32 stats value: {}", error)))?,
        ColumnType::U32 => postcard::from_bytes::<u32>(bytes)
            .map(ColumnValue::U32)
            .map_err(|error| AdolapError::Serialization(format!("Cannot decode U32 stats value: {}", error)))?,
        ColumnType::Bool => postcard::from_bytes::<bool>(bytes)
            .map(ColumnValue::Bool)
            .map_err(|error| AdolapError::Serialization(format!("Cannot decode Bool stats value: {}", error)))?,
    };

    Ok(Some(match value {
        ColumnValue::Utf8(value) => value,
        ColumnValue::I32(value) => value.to_string(),
        ColumnValue::U32(value) => value.to_string(),
        ColumnValue::Bool(value) => value.to_string(),
    }))
}

async fn read_all_segment_metadata(table_path: &Path) -> Result<Vec<SegmentMetadata>, AdolapError> {
    let mut segments = Vec::new();
    let mut entries = match fs::read_dir(table_path).await {
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

        if !name.starts_with("segment_") {
            continue;
        }

        segments.push(read_segment_metadata(&path).await?);
    }

    segments.sort_by_key(|segment| segment.created_at);
    Ok(segments)
}

async fn explain_query(catalog: &Catalog, query: &str) -> Result<String, AdolapError> {
    let plan = match parse_statement(query)? {
        Statement::Query(plan) => plan,
        _ => {
            return Err(AdolapError::ExecutionError(
                "EXPLAIN currently only supports query statements".into(),
            ))
        }
    };

    let optimized = optimizer::optimize(plan);
    let bound = planner::bind_plan(catalog, optimized).await?;
    let physical = planner::create_physical_plan(&bound)?;

    let mut system = System::new_all();
    let pid = sysinfo::get_current_pid()
        .map_err(|error| AdolapError::ExecutionError(format!("Failed to inspect server pid: {}", error)))?;
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    let memory = system.process(pid).map(|process| process.memory()).unwrap_or_default();

    Ok(format!(
        "Logical Plan\n{:#?}\n\nPhysical Plan\n{:#?}\n\nServer Memory\n  rss_bytes: {}",
        bound,
        physical,
        memory,
    ))
}