use core::error::AdolapError;

/// Wire-level representation of a tabular result.
/// For now, everything is stringified; you can later add typed values.
#[derive(Debug, Clone)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<ResultRow>,
}

#[derive(Debug, Clone)]
pub struct ResultRow {
    pub values: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub result_set: ResultSet,
    pub logical_plan: String,
    pub physical_plan: String,
}

#[derive(Debug, Clone)]
pub struct MetaResult {
    pub title: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Utf8,
    I32,
    U32,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarValue {
    Null,
    Utf8(String),
    I32(i32),
    U32(u32),
    Bool(bool),
}

#[derive(Debug, Clone)]
pub enum ClientMessage {
    /// AQL text query, e.g.:
    /// FROM analytics.events
    /// SELECT country
    /// FILTER revenue > 10
    QueryText(String),

    /// Create a database under the server data root.
    CreateDatabase {
        name: String,
    },

    /// Create a table with an explicit schema.
    CreateTable {
        table: String,
        columns: Vec<ColumnDefinition>,
    },

    /// Insert one or more rows into a table.
    InsertRows {
        table: Option<String>,
        rows: Vec<Vec<ScalarValue>>,
    },

    /// Ingest rows from a file into a table.
    IngestInto {
        table: String,
        file_path: String,
    },

    /// Meta command issued by the interactive CLI.
    MetaCommand(String),

    /// Simple liveness check.
    Ping,
}

#[derive(Debug, Clone)]
pub enum ServerMessage {
    /// Successful query result.
    QueryResult(QueryResult),

    /// Successful command acknowledgement for non-query operations.
    Ok(String),

    /// Successful meta command result.
    MetaResult(MetaResult),

    /// Error message (human-readable for now).
    Error(String),

    /// Response to Ping.
    Pong,
}

impl ResultSet {
    pub fn new(columns: Vec<String>, rows: Vec<ResultRow>) -> Self {
        Self { columns, rows }
    }
}

impl ResultRow {
    pub fn new(values: Vec<String>) -> Self {
        Self { values }
    }
}

impl QueryResult {
    pub fn new(result_set: ResultSet, logical_plan: String, physical_plan: String) -> Self {
        Self {
            result_set,
            logical_plan,
            physical_plan,
        }
    }
}

impl MetaResult {
    pub fn new(title: String, content: String) -> Self {
        Self { title, content }
    }
}

pub type ProtocolResult<T> = Result<T, AdolapError>;
