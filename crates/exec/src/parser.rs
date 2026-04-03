/*!
    AQL Parser (Adolap Query Language)
    ----------------------------------

    This module implements the text-based DSL for Adolap. The language is
    intentionally SQL‑like, but simplified and line‑oriented to make it easy
    to parse, reason about, and extend.

    Example AQL query:

        FROM analytics.events
        SELECT country, device, revenue
        FILTER revenue > 10
        FILTER country = "US"
        GROUP BY country, device
        AGG SUM(revenue)

    The parser reads the input line‑by‑line and constructs an `AQLBuilder`,
    which ultimately produces a `LogicalPlan` via `builder.build()`.

    ------------------------------------------------------------------------
    FILTER Expressions
    ------------------------------------------------------------------------

    FILTER clauses accept boolean expressions using:

        - comparison operators: =, >, <
        - logical operators: AND, OR
        - parentheses for grouping
        - identifiers (column names)
        - string literals: "US"
        - integer literals: 10

    Example:

        FILTER (country = "US" AND revenue > 10) OR device = "mobile"

    Multiple FILTER clauses are allowed. They are automatically combined
    using logical AND by the DSL builder:

        FILTER A
        FILTER B
        FILTER C

    becomes:

        (A AND B AND C)

    ------------------------------------------------------------------------
    Parsing Strategy
    ------------------------------------------------------------------------

    This file uses a *top‑down recursive descent parser* for expressions.
    The grammar is simple and unambiguous, so recursive descent is ideal.

    The parser is structured by operator precedence:

        OR          lowest precedence
        AND
        comparison  (=, >, <)
        primary     identifiers, literals, parenthesized expressions

    The call hierarchy is:

        parse_expr
            -> parse_or
                -> parse_and
                    -> parse_comparison
                        -> parse_primary

    This ensures correct precedence without needing a lexer generator or
    bottom‑up parsing machinery.

    ------------------------------------------------------------------------
    Output
    ------------------------------------------------------------------------

    The parser produces a `LogicalPlan` that the execution engine can run.
    All FILTER expressions are converted into `Expr` trees, which the exec
    layer evaluates vectorized over RecordBatches.

    This design keeps the language simple, predictable, and easy to extend
    with new clauses (ORDER BY, LIMIT, HAVING, etc.) in the future.
*/


use crate::aggregate::AggFunc;
use crate::dsl::aql;
use crate::logical_plan::{LogicalPlan, OrderBy, OrderDirection};
use crate::predicate::{Expr, Literal};
use core::error::AdolapError;
use storage::config::{CompressionType, TableStorageConfig};
use storage::column::ColumnValue;
use storage::schema::{ColumnSchema, ColumnType, TableSchema};

#[derive(Debug, Clone)]
pub enum Statement {
    Query(LogicalPlan),
    CreateDatabase { name: String },
    DropDatabase { name: String },
    CreateTable { table: String, schema: TableSchema, storage_config: TableStorageConfig },
    DropTable { table: String },
    InsertRows { table: Option<String>, rows: Vec<Vec<Option<ColumnValue>>> },
    DeleteRows { table: String, predicate: Option<Expr> },
    IngestInto { table: String, file_path: String },
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    StringLit(String),
    IntLit(i32),
    Eq,
    Gt,
    Lt,
    And,
    Or,
    LParen,
    RParen,
}

pub fn parse_statement(input: &str) -> Result<Statement, AdolapError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(AdolapError::ExecutionError("Empty AQL input".into()));
    }

    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("CREATE DATABASE ") {
        let name = trimmed["CREATE DATABASE ".len()..].trim();
        if name.is_empty() {
            return Err(AdolapError::ExecutionError(
                "CREATE DATABASE requires a database name".into(),
            ));
        }
        return Ok(Statement::CreateDatabase {
            name: name.to_string(),
        });
    }

    if upper.starts_with("CREATE TABLE ") {
        return parse_create_table(trimmed);
    }

    if upper.starts_with("DROP DATABASE ") {
        return parse_drop_database(trimmed);
    }

    if upper.starts_with("DROP TABLE ") {
        return parse_drop_table(trimmed);
    }

    if upper.starts_with("INSERT INTO ") {
        return parse_insert(trimmed);
    }

    if upper.starts_with("DELETE FROM ") {
        return parse_delete(trimmed);
    }

    if upper.starts_with("INGEST INTO ") {
        return parse_ingest(trimmed);
    }

    Ok(Statement::Query(parse_query_aql(trimmed)?))
}

pub fn parse_aql(input: &str) -> Result<LogicalPlan, AdolapError> {
    match parse_statement(input)? {
        Statement::Query(plan) => Ok(plan),
        _ => Err(AdolapError::ExecutionError(
            "Expected a query statement".into(),
        )),
    }
}

fn parse_query_aql(input: &str) -> Result<LogicalPlan, AdolapError> {
    let mut builder = aql();
    let mut seen_group_by = false;
    let mut seen_from = false;

    for line in input.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with("FROM ") {
            let table_ref = line[5..].trim();
            validate_qualified_table_name(table_ref, "FROM")?;
            builder = builder.from(table_ref);
            seen_from = true;
        } else if line.starts_with("JOIN ") {
            if !seen_from {
                return Err(AdolapError::ExecutionError(
                    "JOIN requires a preceding FROM clause".into(),
                ));
            }
            let (table_ref, left_on, right_on) = parse_join_clause(&line[5..])?;
            builder = builder.join(&table_ref, &left_on, &right_on);
        } else if line.starts_with("SELECT ") {
            let cols = parse_list(&line[7..]);
            builder = builder.select(cols);
        } else if line.starts_with("FILTER ") {
            let expr_str = line[7..].trim();
            let tokens = tokenize(expr_str)?;
            let (expr, _) = parse_expr(&tokens, 0)?;
            builder = builder.filter(expr);
        } else if line.starts_with("GROUP BY ") {
            let keys = parse_list(&line[9..]);
            builder = builder.group_by(keys);
            seen_group_by = true;
        } else if line.starts_with("GROUP FILTER ") {
            if !seen_group_by {
                return Err(AdolapError::ExecutionError(
                    "GROUP FILTER can only appear after GROUP BY".into(),
                ));
            }
            let expr_str = line[13..].trim();
            builder = builder.group_filter(parse_predicate(expr_str)?);
        } else if line.starts_with("AGG ") {
            let (func, col) = parse_agg(&line[4..])?;
            builder = builder.agg(func, &col);
        } else if line.starts_with("ORDER BY ") {
            builder = builder.order_by(parse_order_by_clause(&line[9..])?);
        } else if line.starts_with("LIMIT ") {
            builder = builder.limit(parse_usize_clause("LIMIT", &line[6..])?);
        } else if line.starts_with("OFFSET ") {
            builder = builder.offset(parse_usize_clause("OFFSET", &line[7..])?);
        } else if line.starts_with("SKIP ") {
            builder = builder.offset(parse_usize_clause("SKIP", &line[5..])?);
        } else {
            return Err(AdolapError::ExecutionError(format!(
                "Unknown AQL clause: {}",
                line
            )));
        }
    }

    Ok(builder.build())
}

fn parse_join_clause(input: &str) -> Result<(String, String, String), AdolapError> {
    let upper = input.to_ascii_uppercase();
    let on_index = upper.find(" ON ").ok_or_else(|| {
        AdolapError::ExecutionError(
            "JOIN must use the syntax 'JOIN <table> ON <left_column> = <right_column>'"
                .into(),
        )
    })?;

    let table_ref = input[..on_index].trim();
    validate_qualified_table_name(table_ref, "JOIN")?;

    let condition = input[on_index + 4..].trim();
    let Some((left_on, right_on)) = condition.split_once('=') else {
        return Err(AdolapError::ExecutionError(
            "JOIN ON currently only supports equality predicates".into(),
        ));
    };

    Ok((
        table_ref.to_string(),
        left_on.trim().to_string(),
        right_on.trim().to_string(),
    ))
}

fn parse_create_table(input: &str) -> Result<Statement, AdolapError> {
    let rest = input["CREATE TABLE ".len()..].trim();
    let open = rest.find('(').ok_or_else(|| {
        AdolapError::ExecutionError("CREATE TABLE requires a column list".into())
    })?;
    let close = find_matching_paren(rest, open).ok_or_else(|| {
        AdolapError::ExecutionError("CREATE TABLE is missing a closing ')'".into())
    })?;
    if close <= open {
        return Err(AdolapError::ExecutionError(
            "CREATE TABLE column list is malformed".into(),
        ));
    }

    let table = rest[..open].trim();
    if table.is_empty() {
        return Err(AdolapError::ExecutionError(
            "CREATE TABLE requires a table name".into(),
        ));
    }

    let inner = &rest[open + 1..close];
    let remainder = rest[close + 1..].trim();
    let mut columns = Vec::new();
    for raw_column in inner.split(',') {
        let column_def = raw_column.trim();
        if column_def.is_empty() {
            continue;
        }

        let parts: Vec<&str> = column_def.split_whitespace().collect();
        if parts.len() < 2 {
            return Err(AdolapError::ExecutionError(format!(
                "Invalid column definition: {}",
                column_def
            )));
        }

        let nullable = parts
            .iter()
            .skip(2)
            .any(|part| part.eq_ignore_ascii_case("NULL") || part.eq_ignore_ascii_case("NULLABLE"));

        columns.push(ColumnSchema {
            name: parts[0].to_string(),
            column_type: parse_column_type(parts[1])?,
            nullable,
        });
    }

    if columns.is_empty() {
        return Err(AdolapError::ExecutionError(
            "CREATE TABLE requires at least one column".into(),
        ));
    }

    let storage_config = if remainder.is_empty() {
        TableStorageConfig::default()
    } else {
        parse_table_storage_config(remainder)?
    };

    Ok(Statement::CreateTable {
        table: table.to_string(),
        schema: TableSchema { columns },
        storage_config,
    })
}

fn parse_table_storage_config(input: &str) -> Result<TableStorageConfig, AdolapError> {
    if !input.to_ascii_uppercase().starts_with("USING CONFIG") {
        return Err(AdolapError::ExecutionError(
            "CREATE TABLE only supports 'USING CONFIG (...)' after the schema".into(),
        ));
    }

    let rest = input["USING CONFIG".len()..].trim();
    if !rest.starts_with('(') {
        return Err(AdolapError::ExecutionError(
            "USING CONFIG requires a parenthesized config list".into(),
        ));
    }

    let close = find_matching_paren(rest, 0).ok_or_else(|| {
        AdolapError::ExecutionError("USING CONFIG is missing a closing ')'".into())
    })?;
    if !rest[close + 1..].trim().is_empty() {
        return Err(AdolapError::ExecutionError(
            "Unexpected tokens after USING CONFIG clause".into(),
        ));
    }

    let mut config = TableStorageConfig::default();
    for entry in split_top_level(&rest[1..close], ',') {
        if entry.is_empty() {
            continue;
        }

        let Some((key, raw_value)) = entry.split_once('=') else {
            return Err(AdolapError::ExecutionError(format!(
                "Invalid config entry: {}",
                entry
            )));
        };

        let key = key.trim().to_ascii_lowercase();
        let raw_value = raw_value.trim();
        match key.as_str() {
            "compression" => {
                config.compression = parse_compression_setting(raw_value)?;
            }
            "row_group_size" => {
                config.row_group_size = raw_value.parse::<usize>().map_err(|_| {
                    AdolapError::ExecutionError(format!(
                        "Invalid row_group_size value: {}",
                        raw_value
                    ))
                })?;
            }
            "bloom_filter" | "enable_bloom_filter" => {
                config.enable_bloom_filter = parse_bool_setting(raw_value)?;
            }
            "dictionary_encoding" | "enable_dictionary_encoding" => {
                config.enable_dictionary_encoding = parse_bool_setting(raw_value)?;
            }
            other => {
                return Err(AdolapError::ExecutionError(format!(
                    "Unsupported table config option: {}",
                    other
                )));
            }
        }
    }

    Ok(config)
}

fn parse_compression_setting(input: &str) -> Result<CompressionType, AdolapError> {
    let value = parse_quoted_string(input)?;
    match value.to_ascii_lowercase().as_str() {
        "none" => Ok(CompressionType::None),
        "lz4" => Ok(CompressionType::Lz4),
        "zstd" => Ok(CompressionType::Zstd),
        other => Err(AdolapError::ExecutionError(format!(
            "Unsupported compression type: {}",
            other
        ))),
    }
}

fn parse_bool_setting(input: &str) -> Result<bool, AdolapError> {
    match input.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(AdolapError::ExecutionError(format!(
            "Invalid boolean config value: {}",
            other
        ))),
    }
}

fn find_matching_paren(input: &str, open_index: usize) -> Option<usize> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut depth = 0usize;
    let mut in_string = false;

    for (index, ch) in chars.iter().enumerate().skip(open_index) {
        match ch {
            '"' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }

    None
}

fn parse_drop_database(input: &str) -> Result<Statement, AdolapError> {
    let name = input["DROP DATABASE ".len()..].trim();
    if name.is_empty() {
        return Err(AdolapError::ExecutionError(
            "DROP DATABASE requires a database name".into(),
        ));
    }

    Ok(Statement::DropDatabase {
        name: name.to_string(),
    })
}

fn parse_drop_table(input: &str) -> Result<Statement, AdolapError> {
    let table = input["DROP TABLE ".len()..].trim();
    if table.is_empty() {
        return Err(AdolapError::ExecutionError(
            "DROP TABLE requires a table name".into(),
        ));
    }

    Ok(Statement::DropTable {
        table: table.to_string(),
    })
}

fn parse_insert(input: &str) -> Result<Statement, AdolapError> {
    let rest = input["INSERT INTO ".len()..].trim();
    let upper = rest.to_ascii_uppercase();

    let (table, rows_part) = if upper.starts_with("ROWS") {
        (None, rest["ROWS".len()..].trim())
    } else {
        let rows_index = upper.find(" ROWS").ok_or_else(|| {
            AdolapError::ExecutionError(
                "INSERT INTO must use the syntax 'INSERT INTO <table> ROWS (...)'".into(),
            )
        })?;
        (
            Some(rest[..rows_index].trim().to_string()),
            rest[rows_index + " ROWS".len()..].trim(),
        )
    };

    let rows = parse_rows_clause(rows_part)?;
    if rows.is_empty() {
        return Err(AdolapError::ExecutionError(
            "INSERT INTO must include at least one row".into(),
        ));
    }

    Ok(Statement::InsertRows { table, rows })
}

fn parse_delete(input: &str) -> Result<Statement, AdolapError> {
    let rest = input["DELETE FROM ".len()..].trim();
    if rest.is_empty() {
        return Err(AdolapError::ExecutionError(
            "DELETE FROM requires a table name".into(),
        ));
    }

    if let Some((table, expr)) = split_delete_predicate(rest, " FILTER ") {
        return Ok(Statement::DeleteRows {
            table: table.to_string(),
            predicate: Some(parse_predicate(expr)?),
        });
    }

    if let Some((table, expr)) = split_delete_predicate(rest, " WHERE ") {
        return Ok(Statement::DeleteRows {
            table: table.to_string(),
            predicate: Some(parse_predicate(expr)?),
        });
    }

    Ok(Statement::DeleteRows {
        table: rest.to_string(),
        predicate: None,
    })
}

fn parse_ingest(input: &str) -> Result<Statement, AdolapError> {
    let rest = input["INGEST INTO ".len()..].trim();
    let upper = rest.to_ascii_uppercase();
    let from_index = upper.find(" FROM ").ok_or_else(|| {
        AdolapError::ExecutionError(
            "INGEST INTO must use the syntax 'INGEST INTO <table> FROM \"file.json\"'".into(),
        )
    })?;

    let table = rest[..from_index].trim();
    let file_path = rest[from_index + " FROM ".len()..].trim();
    if table.is_empty() || file_path.is_empty() {
        return Err(AdolapError::ExecutionError(
            "INGEST INTO requires both a table name and a file path".into(),
        ));
    }

    Ok(Statement::IngestInto {
        table: table.to_string(),
        file_path: parse_quoted_string(file_path)?,
    })
}

fn parse_rows_clause(input: &str) -> Result<Vec<Vec<Option<ColumnValue>>>, AdolapError> {
    let mut rows = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut pos = 0;

    while pos < chars.len() {
        while pos < chars.len() && (chars[pos].is_whitespace() || chars[pos] == ',') {
            pos += 1;
        }

        if pos >= chars.len() {
            break;
        }

        if chars[pos] != '(' {
            return Err(AdolapError::ExecutionError(
                "INSERT rows must be wrapped in parentheses".into(),
            ));
        }

        let start = pos + 1;
        let mut depth = 1usize;
        let mut in_string = false;
        pos += 1;

        while pos < chars.len() {
            match chars[pos] {
                '"' => in_string = !in_string,
                '(' if !in_string => depth += 1,
                ')' if !in_string => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            pos += 1;
        }

        if pos >= chars.len() || chars[pos] != ')' {
            return Err(AdolapError::ExecutionError(
                "Unterminated INSERT row literal".into(),
            ));
        }

        let row_text: String = chars[start..pos].iter().collect();
        let json_text = format!("[{}]", row_text);
        let json_values: Vec<serde_json::Value> = serde_json::from_str(&json_text).map_err(|e| {
            AdolapError::ExecutionError(format!("Invalid row literal: {}", e))
        })?;
        let row = json_values
            .into_iter()
            .map(json_value_to_column_value)
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(row);
        pos += 1;
    }

    Ok(rows)
}

fn json_value_to_column_value(value: serde_json::Value) -> Result<Option<ColumnValue>, AdolapError> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(value) => Ok(Some(ColumnValue::Utf8(value))),
        serde_json::Value::Bool(value) => Ok(Some(ColumnValue::Bool(value))),
        serde_json::Value::Number(value) => {
            if let Some(int_value) = value.as_i64() {
                if let Ok(v) = i32::try_from(int_value) {
                    return Ok(Some(ColumnValue::I32(v)));
                }
                if let Ok(v) = u32::try_from(int_value) {
                    return Ok(Some(ColumnValue::U32(v)));
                }
            }

            Err(AdolapError::ExecutionError(format!(
                "Unsupported numeric literal: {}",
                value
            )))
        }
        other => Err(AdolapError::ExecutionError(format!(
            "Unsupported row literal value: {}",
            other
        ))),
    }
}

fn parse_column_type(input: &str) -> Result<ColumnType, AdolapError> {
    match input.to_ascii_uppercase().as_str() {
        "UTF8" | "STRING" => Ok(ColumnType::Utf8),
        "I32" | "INT" | "INT32" => Ok(ColumnType::I32),
        "U32" | "UINT" | "UINT32" => Ok(ColumnType::U32),
        "BOOL" | "BOOLEAN" => Ok(ColumnType::Bool),
        other => Err(AdolapError::ExecutionError(format!(
            "Unsupported column type: {}",
            other
        ))),
    }
}

fn parse_quoted_string(input: &str) -> Result<String, AdolapError> {
    let trimmed = input.trim();
    if trimmed.len() < 2 || !trimmed.starts_with('"') || !trimmed.ends_with('"') {
        return Err(AdolapError::ExecutionError(format!(
            "Expected a quoted string, got: {}",
            input
        )));
    }
    Ok(trimmed[1..trimmed.len() - 1].to_string())
}

fn split_delete_predicate<'a>(input: &'a str, separator: &str) -> Option<(&'a str, &'a str)> {
    let upper = input.to_ascii_uppercase();
    let index = upper.find(separator)?;
    let table = input[..index].trim();
    let predicate = input[index + separator.len()..].trim();
    if table.is_empty() || predicate.is_empty() {
        return None;
    }
    Some((table, predicate))
}

fn parse_list(input: &str) -> Vec<String> {
    input
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn parse_predicate(input: &str) -> Result<Expr, AdolapError> {
    let tokens = tokenize(input)?;
    let (expr, consumed) = parse_expr(&tokens, 0)?;
    if consumed != tokens.len() {
        return Err(AdolapError::ExecutionError(
            "Unexpected token at end of predicate".into(),
        ));
    }
    Ok(expr)
}

fn parse_order_by_clause(input: &str) -> Result<Vec<OrderBy>, AdolapError> {
    let mut order_by = Vec::new();
    for raw_term in split_top_level(input, ',') {
        let term = raw_term.trim();
        if term.is_empty() {
            continue;
        }

        let (expr_text, direction) = if let Some(value) = term.strip_suffix(" DESC") {
            (value.trim(), OrderDirection::Desc)
        } else if let Some(value) = term.strip_suffix(" ASC") {
            (value.trim(), OrderDirection::Asc)
        } else {
            (term, OrderDirection::Asc)
        };

        order_by.push(OrderBy {
            expr: parse_order_expr(expr_text)?,
            direction,
        });
    }

    if order_by.is_empty() {
        return Err(AdolapError::ExecutionError(
            "ORDER BY requires at least one expression".into(),
        ));
    }

    Ok(order_by)
}

fn parse_order_expr(input: &str) -> Result<Expr, AdolapError> {
    let tokens = tokenize(input)?;
    let (expr, consumed) = parse_primary(&tokens, 0)?;
    if consumed != tokens.len() {
        return Err(AdolapError::ExecutionError(
            "ORDER BY expressions must be a column name or aggregate reference".into(),
        ));
    }
    Ok(expr)
}

fn parse_usize_clause(clause: &str, input: &str) -> Result<usize, AdolapError> {
    input.trim().parse::<usize>().map_err(|error| {
        AdolapError::ExecutionError(format!(
            "{} requires a non-negative integer: {}",
            clause, error
        ))
    })
}

fn split_top_level(input: &str, separator: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut current = String::new();

    for ch in input.chars() {
        match ch {
            '"' => {
                in_string = !in_string;
                current.push(ch);
            }
            '(' if !in_string => {
                depth += 1;
                current.push(ch);
            }
            ')' if !in_string => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            value if value == separator && !in_string && depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }

    parts
}

fn parse_agg(input: &str) -> Result<(AggFunc, String), AdolapError> {
    let input = input.trim();
    // e.g. "SUM(revenue)"
    if let Some(rest) = input.strip_prefix("SUM(") {
        let col = rest.trim_end_matches(')').trim().to_string();
        Ok((AggFunc::Sum, col))
    } else if let Some(rest) = input.strip_prefix("AVG(") {
        let col = rest.trim_end_matches(')').trim().to_string();
        Ok((AggFunc::Avg, col))
    } else if let Some(rest) = input.strip_prefix("MIN(") {
        let col = rest.trim_end_matches(')').trim().to_string();
        Ok((AggFunc::Min, col))
    } else if let Some(rest) = input.strip_prefix("MAX(") {
        let col = rest.trim_end_matches(')').trim().to_string();
        Ok((AggFunc::Max, col))
    } else if let Some(rest) = input.strip_prefix("COUNT(") {
        let col = rest.trim_end_matches(')').trim().to_string();
        Ok((AggFunc::Count, col))
    } else {
        Err(AdolapError::ExecutionError(format!(
            "Invalid AGG clause: {}",
            input
        )))
    }
}

fn validate_qualified_table_name(table_ref: &str, context: &str) -> Result<(), AdolapError> {
    match table_ref.split_once('.') {
        Some((database, table))
            if is_valid_identifier(database) && is_valid_identifier(table) => Ok(()),
        _ => Err(AdolapError::ExecutionError(format!(
            "{} requires a fully qualified table name in the form <database_name>.<table_name>",
            context
        ))),
    }
}

fn is_valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn tokenize(input: &str) -> Result<Vec<Token>, AdolapError> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' => {
                chars.next();
            }
            '(' => {
                tokens.push(Token::LParen);
                chars.next();
            }
            ')' => {
                tokens.push(Token::RParen);
                chars.next();
            }
            '=' => {
                tokens.push(Token::Eq);
                chars.next();
            }
            '>' => {
                tokens.push(Token::Gt);
                chars.next();
            }
            '<' => {
                tokens.push(Token::Lt);
                chars.next();
            }
            '"' => {
                chars.next(); // consume "
                let mut s = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '"' {
                        break;
                    }
                    s.push(c);
                    chars.next();
                }
                if chars.peek() == Some(&'"') {
                    chars.next(); // closing "
                    tokens.push(Token::StringLit(s));
                } else {
                    return Err(AdolapError::ExecutionError(
                        "Unterminated string literal".into(),
                    ));
                }
            }
            c if c.is_ascii_digit() => {
                let mut num = String::new();
                while let Some(&d) = chars.peek() {
                    if d.is_ascii_digit() {
                        num.push(d);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let v: i32 = num.parse().map_err(|e| {
                    AdolapError::ExecutionError(format!("Invalid integer literal: {}", e))
                })?;
                tokens.push(Token::IntLit(v));
            }
            _ => {
                // identifier or keyword
                let mut ident = String::new();
                while let Some(&c2) = chars.peek() {
                    if c2.is_alphanumeric() || c2 == '_' || c2 == '.' {
                        ident.push(c2);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let upper = ident.to_uppercase();
                match upper.as_str() {
                    "AND" => tokens.push(Token::And),
                    "OR" => tokens.push(Token::Or),
                    _ => tokens.push(Token::Ident(ident)),
                }
            }
        }
    }

    Ok(tokens)
}

// Recursive descent with precedence:
// OR  (lowest)
// AND
// comparison (=, >, <) (highest)

fn parse_expr(tokens: &[Token], pos: usize) -> Result<(Expr, usize), AdolapError> {
    parse_or(tokens, pos)
}

fn parse_or(tokens: &[Token], mut pos: usize) -> Result<(Expr, usize), AdolapError> {
    let (mut left, p) = parse_and(tokens, pos)?;
    pos = p;

    while pos < tokens.len() {
        if tokens[pos] == Token::Or {
            let (right, p2) = parse_and(tokens, pos + 1)?;
            left = Expr::Or(Box::new(left), Box::new(right));
            pos = p2;
        } else {
            break;
        }
    }

    Ok((left, pos))
}

fn parse_and(tokens: &[Token], mut pos: usize) -> Result<(Expr, usize), AdolapError> {
    let (mut left, p) = parse_comparison(tokens, pos)?;
    pos = p;

    while pos < tokens.len() {
        if tokens[pos] == Token::And {
            let (right, p2) = parse_comparison(tokens, pos + 1)?;
            left = Expr::And(Box::new(left), Box::new(right));
            pos = p2;
        } else {
            break;
        }
    }

    Ok((left, pos))
}

fn parse_comparison(tokens: &[Token], pos: usize) -> Result<(Expr, usize), AdolapError> {
    let (left, pos) = parse_primary(tokens, pos)?;

    if pos >= tokens.len() {
        return Ok((left, pos));
    }

    match tokens[pos] {
        Token::Eq => {
            let (right, p2) = parse_primary(tokens, pos + 1)?;
            Ok((Expr::Eq(Box::new(left), Box::new(right)), p2))
        }
        Token::Gt => {
            let (right, p2) = parse_primary(tokens, pos + 1)?;
            Ok((Expr::Gt(Box::new(left), Box::new(right)), p2))
        }
        Token::Lt => {
            let (right, p2) = parse_primary(tokens, pos + 1)?;
            Ok((Expr::Lt(Box::new(left), Box::new(right)), p2))
        }
        _ => Ok((left, pos)),
    }
}

fn parse_primary(tokens: &[Token], pos: usize) -> Result<(Expr, usize), AdolapError> {
    if pos >= tokens.len() {
        return Err(AdolapError::ExecutionError(
            "Unexpected end of expression".into(),
        ));
    }

    match &tokens[pos] {
        Token::Ident(name) => {
            if pos + 3 < tokens.len() && tokens[pos + 1] == Token::LParen {
                if let Token::Ident(column) = &tokens[pos + 2] {
                    if tokens[pos + 3] == Token::RParen {
                        return Ok((
                            Expr::Aggregate {
                                func: parse_agg_func_name(name)?,
                                column: column.clone(),
                            },
                            pos + 4,
                        ));
                    }
                }
            }

            Ok((Expr::Column(name.clone()), pos + 1))
        }
        Token::StringLit(s) => Ok((Expr::Literal(Literal::Utf8(s.clone())), pos + 1)),
        Token::IntLit(v) => Ok((Expr::Literal(Literal::I32(*v)), pos + 1)),
        Token::LParen => {
            let (expr, p2) = parse_expr(tokens, pos + 1)?;
            if p2 >= tokens.len() || tokens[p2] != Token::RParen {
                return Err(AdolapError::ExecutionError(
                    "Expected closing ')'".into(),
                ));
            }
            Ok((expr, p2 + 1))
        }
        _ => Err(AdolapError::ExecutionError(
            "Unexpected token in expression".into(),
        )),
    }
}

fn parse_agg_func_name(input: &str) -> Result<AggFunc, AdolapError> {
    match input.to_ascii_uppercase().as_str() {
        "SUM" => Ok(AggFunc::Sum),
        "AVG" => Ok(AggFunc::Avg),
        "MIN" => Ok(AggFunc::Min),
        "MAX" => Ok(AggFunc::Max),
        "COUNT" => Ok(AggFunc::Count),
        other => Err(AdolapError::ExecutionError(format!(
            "Unsupported aggregate function: {}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_aql, parse_statement, Statement};
    use storage::column::ColumnValue;
    use storage::config::CompressionType;
    use storage::schema::ColumnType;

    #[test]
    fn parses_create_database() {
        let statement = parse_statement("CREATE DATABASE mydb").unwrap();
        match statement {
            Statement::CreateDatabase { name } => assert_eq!(name, "mydb"),
            other => panic!("unexpected statement: {:?}", other),
        }
    }

    #[test]
    fn parses_create_table() {
        let statement = parse_statement(
            "CREATE TABLE mydb.events (\n  country Utf8,\n  device Utf8,\n  revenue I32\n)",
        )
        .unwrap();

        match statement {
            Statement::CreateTable { table, schema, storage_config } => {
                assert_eq!(table, "mydb.events");
                assert_eq!(schema.columns.len(), 3);
                assert_eq!(schema.columns[0].column_type, ColumnType::Utf8);
                assert_eq!(schema.columns[2].column_type, ColumnType::I32);
                assert_eq!(storage_config.row_group_size, 16_384);
            }
            other => panic!("unexpected statement: {:?}", other),
        }
    }

    #[test]
    fn parses_create_table_with_using_config() {
        let statement = parse_statement(
            "CREATE TABLE mydb.events (country Utf8, revenue I32) USING CONFIG (compression = \"lz4\", row_group_size = 8192, bloom_filter = true)",
        )
        .unwrap();

        match statement {
            Statement::CreateTable { storage_config, .. } => {
                assert_eq!(storage_config.row_group_size, 8192);
                assert!(storage_config.enable_bloom_filter);
                assert!(matches!(storage_config.compression, CompressionType::Lz4));
            }
            other => panic!("unexpected statement: {:?}", other),
        }
    }

    #[test]
    fn parses_drop_database() {
        let statement = parse_statement("DROP DATABASE mydb").unwrap();
        match statement {
            Statement::DropDatabase { name } => assert_eq!(name, "mydb"),
            other => panic!("unexpected statement: {:?}", other),
        }
    }

    #[test]
    fn parses_drop_table() {
        let statement = parse_statement("DROP TABLE mydb.events").unwrap();
        match statement {
            Statement::DropTable { table } => assert_eq!(table, "mydb.events"),
            other => panic!("unexpected statement: {:?}", other),
        }
    }

    #[test]
    fn parses_insert_rows() {
        let statement = parse_statement(
            "INSERT INTO mydb.events ROWS (\"US\", \"mobile\", \"25\"), (\"CA\", \"web\", 30)",
        )
        .unwrap();

        match statement {
            Statement::InsertRows { table, rows } => {
                assert_eq!(table.as_deref(), Some("mydb.events"));
                assert_eq!(rows.len(), 2);
                assert!(matches!(rows[0][0], Some(ColumnValue::Utf8(_))));
                assert!(matches!(rows[1][2], Some(ColumnValue::I32(30))));
            }
            other => panic!("unexpected statement: {:?}", other),
        }
    }

    #[test]
    fn parses_delete_rows_with_filter() {
        let statement = parse_statement("DELETE FROM mydb.events FILTER revenue > 10").unwrap();
        match statement {
            Statement::DeleteRows { table, predicate } => {
                assert_eq!(table, "mydb.events");
                assert!(predicate.is_some());
            }
            other => panic!("unexpected statement: {:?}", other),
        }
    }

    #[test]
    fn parses_group_filter_and_order_by() {
        let plan = parse_aql(
            "FROM analytics.events\nGROUP BY country\nAGG SUM(revenue)\nGROUP FILTER SUM(revenue) > 10\nSELECT country, sum_revenue\nORDER BY sum_revenue DESC, country",
        )
        .unwrap();

        match plan {
            crate::logical_plan::LogicalPlan::Sort { order_by, .. } => {
                assert_eq!(order_by.len(), 2);
                assert!(matches!(order_by[0].direction, crate::logical_plan::OrderDirection::Desc));
            }
            other => panic!("unexpected plan: {:?}", other),
        }
    }

    #[test]
    fn parses_limit_and_offset() {
        let plan = parse_aql(
            "FROM analytics.events\nSELECT country\nORDER BY country\nLIMIT 10\nOFFSET 5",
        )
        .unwrap();

        match plan {
            crate::logical_plan::LogicalPlan::Limit { limit, offset, input } => {
                assert_eq!(limit, Some(10));
                assert_eq!(offset, 5);
                assert!(matches!(*input, crate::logical_plan::LogicalPlan::Sort { .. }));
            }
            other => panic!("unexpected plan: {:?}", other),
        }
    }

    #[test]
    fn parses_skip_without_limit() {
        let plan = parse_aql(
            "FROM analytics.events\nSELECT country\nSKIP 3",
        )
        .unwrap();

        match plan {
            crate::logical_plan::LogicalPlan::Limit { limit, offset, .. } => {
                assert_eq!(limit, None);
                assert_eq!(offset, 3);
            }
            other => panic!("unexpected plan: {:?}", other),
        }
    }

    #[test]
    fn parses_join_query() {
        let plan = parse_aql(
            "FROM analytics.events\nJOIN analytics.devices ON analytics.events.device = analytics.devices.id\nSELECT analytics.events.country, analytics.devices.category",
        )
        .unwrap();

        match plan {
            crate::logical_plan::LogicalPlan::Project { input, .. } => match *input {
                crate::logical_plan::LogicalPlan::Join { left_on, right_on, .. } => {
                    assert_eq!(left_on, "analytics.events.device");
                    assert_eq!(right_on, "analytics.devices.id");
                }
                other => panic!("unexpected inner plan: {:?}", other),
            },
            other => panic!("unexpected plan: {:?}", other),
        }
    }

    #[test]
    fn parses_query_with_qualified_from() {
        let plan = parse_aql("FROM analytics.events\nSELECT country").unwrap();
        match plan {
            crate::logical_plan::LogicalPlan::Project { input, .. } => match *input {
                crate::logical_plan::LogicalPlan::Scan { table_ref, .. } => {
                    assert_eq!(table_ref, "analytics.events")
                }
                other => panic!("unexpected inner plan: {:?}", other),
            },
            other => panic!("unexpected plan: {:?}", other),
        }
    }

    #[test]
    fn rejects_query_with_unqualified_from() {
        let err = parse_aql("FROM events\nSELECT country").unwrap_err();
        assert!(err
            .to_string()
            .contains("fully qualified table name"));
    }
}
