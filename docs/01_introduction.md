# Chapter 1: Introduction

## 1. What Adolap Is

Adolap is a compact database system written in Rust. It is not just a parser or a storage library — it is a complete local system with distinct, cooperating layers:

- A text query language (AQL) for queries and data definition.
- A planner and executor that transforms text into typed record batches.
- A storage engine that persists data on disk using segments and row groups.
- A binary framed protocol used over TCP.
- A server process that accepts requests and executes them.
- An interactive CLI for local development and inspection.

## 2. The Seven-Step Mental Model

Every query goes through all seven steps in order.

### 2.1 The CLI sends a message over TCP

The CLI encodes an AQL query string or meta command as a `ClientMessage` and writes it as a length-prefixed frame:

```rust
// crates/cli/src/client.rs
pub async fn send_query(&mut self, query: &str) -> Result<ClientResponse, AdolapError> {
    self.send_message(ClientMessage::QueryText(query.to_string())).await
}
```

`ClientResponse` includes the raw server message, wall-clock elapsed duration, and response byte count — used for timing and profiling output.

### 2.2 The server reads and routes the frame

The handler is the top-level dispatch point for every request type:

```rust
// crates/server/src/handler.rs (representative structure)
match msg {
    ClientMessage::QueryText(text) => {
        match parse_statement(&text)? {
            Statement::Query(plan) => execute_query(&catalog, plan).await,
            Statement::CreateDatabase { name } => { ... }
            Statement::CreateTable { table, schema, storage_config } => { ... }
            Statement::InsertRows { table, rows } => { ... }
            Statement::DeleteRows { table, predicate } => { ... }
            Statement::IngestInto { table, file_path } => { ... }
        }
    }
    ClientMessage::MetaCommand(command) => handle_meta_command(&catalog, &command).await,
    ...
}
```

### 2.3 Query text becomes a logical plan

The parser reads AQL text and produces a `LogicalPlan` tree using a fluent builder API:

```rust
// crates/exec/src/logical_plan.rs
let plan = LogicalPlan::scan("analytics.events")
    .filter(col("revenue").gt(50))
    .project(vec!["country", "revenue"])
    .sort(vec![OrderBy { expr: col("revenue"), direction: OrderDirection::Desc }])
    .limit(Some(10), 0);
```

### 2.4 The optimizer rewrites the plan

Before binding, the optimizer applies **filter-below-project pushdown**: when a `Project` wraps a `Filter`, it inverts them so the filter runs closer to the scan, reducing rows the projection sees:

```rust
// crates/exec/src/optimizer.rs
LogicalPlan::Project { input, columns } => {
    match *input {
        LogicalPlan::Filter { input: inner, predicate } => {
            // Project(Filter(Scan)) -> Filter(Project(Scan))
            let projected = LogicalPlan::Project { input: inner, columns };
            LogicalPlan::Filter { input: Box::new(projected), predicate }
        }
        other => LogicalPlan::Project {
            input: Box::new(optimize(other)),
            columns,
        },
    }
}
```

### 2.5 The planner binds the plan and creates a physical plan

The planner resolves each `Scan` node against the `Catalog`, attaches `TableMetadata` (path, schema, storage config), and converts the logical tree into a `PhysicalPlan` whose nodes map directly to executor operations.

### 2.6 The executor produces `Vec<RecordBatch>`

`Executor` is stateless. Its `execute` method takes a `&PhysicalPlan` and returns a `Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, AdolapError>>>>`. Every plan node produces one or more batches collected and passed up the tree:

```rust
// crates/exec/src/executor.rs
pub fn execute<'b>(
    &'b self,
    plan: &'b PhysicalPlan<'b>,
) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, AdolapError>> + Send + 'b>> {
    Box::pin(async move {
        match plan {
            PhysicalPlan::Scan { table, projected_columns, predicate } => {
                let table_reader = TableReader::new(&table.path, &table.schema);
                table_reader.read_table(predicate, projected_columns.clone()).await
            }
            PhysicalPlan::Filter { input, predicate } => {
                let batches = self.execute(input).await?;
                filter_batches(&batches, predicate)
            }
            PhysicalPlan::Project { input, columns } => {
                let batches = self.execute(input).await?;
                let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                project_batches(&batches, &cols)
            }
            // ... additional plan nodes
        }
    })
}
```

The server collects all batches from the returned `Vec<RecordBatch>`, calls `to_rows` on each, and serializes them into the `ResultSet` sent to the CLI.

### 2.7 The CLI renders the result

The server packages the `ResultSet`, logical plan text, physical plan text, and a `QuerySummary` into a `QueryResult` message. The CLI printer renders it in labeled sections:

```
=== Query Summary ===
rows: 2
columns: 2
batches: 1
server_execution_ms: 0.812

=== Logical Plan ===
Scan(analytics.events) -> Filter(revenue > 50) -> Project(country, revenue)

=== Physical Plan ===
Scan(table=analytics.events) -> Filter(revenue > 50) -> Project(country, revenue)

=== Query Results ===
┌─────────┬─────────┐
│ country │ revenue │
├─────────┼─────────┤
│ US      │ 120     │
│ US      │ 80      │
└─────────┴─────────┘
(2 rows)
```

## 3. Workspace Structure

```text
Adolap/
  Cargo.toml
  Readme.md
  docs/
  crates/
    cli/       — interactive shell
    core/      — shared errors, config, and utilities
    exec/      — parser, planner, optimizer, executor
    protocol/  — wire message types and framing
    server/    — TCP listener, handler, meta commands
    storage/   — catalog, schema, record batches, segments
```

### 3.1 `core`

Provides shared building blocks: `AdolapError`, logging helpers, and config loading utilities. Every other crate depends on `core`.

### 3.2 `storage`

Owns the physical data model: table schemas, record batches, filesystem catalog, table reading and writing, segment and row-group metadata, stats, bloom filters, and compression configuration.

### 3.3 `exec`

Contains the query engine: AQL parser, logical and physical plan types, optimizer, planner, predicate evaluation, filtering, projection, aggregation, and the executor.

### 3.4 `protocol`

Defines `ClientMessage` and `ServerMessage` enums, serialization/deserialization (using `postcard`), and length-prefixed framing helpers for TCP.

### 3.5 `server`

Listens on a TCP socket, manages a connection counter, spawns per-connection tasks, runs queries, and handles meta commands against the live filesystem catalog.

### 3.6 `cli`

The human-facing interactive shell: multiline query buffering, history persistence to `~/.adolap_history`, meta command dispatch, output formatting (unicode, ascii, csv, tsv), and profiling toggles.

## 4. Core Type Reference

### 4.1 `LogicalPlan` — the query tree

Defined in `crates/exec/src/logical_plan.rs`. Every variant is an explicit Rust enum case:

```rust
pub enum LogicalPlan {
    Scan {
        table_ref: String,
        table: Option<TableMetadata>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_keys: Vec<String>,
        agg_column: String,
        agg_func: AggFunc,
    },
    GroupFilter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_on: String,
        right_on: String,
        output_schema: Option<TableSchema>,
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderBy>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<usize>,
        offset: usize,
    },
}
```

`Scan` is always a leaf. All other variants wrap `input: Box<LogicalPlan>`, forming a tree the optimizer and planner traverse recursively. `table` in `Scan` starts as `None` after parsing and is filled by the planner when it resolves the name against the catalog. `limit: None` in `Limit` means no upper bound. `offset` defaults to `0`.

Supporting types:

```rust
pub enum OrderDirection { Asc, Desc }

pub struct OrderBy {
    pub expr: Expr,
    pub direction: OrderDirection,
}
```

### 4.2 `TableSchema` and `ColumnSchema` — the schema model

Defined in `crates/storage/src/schema.rs`. These structs describe every table's column structure and are serialized to JSON on disk:

```rust
pub enum ColumnType {
    Utf8,
    I32,
    U32,
    Bool,
}

pub struct ColumnSchema {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
}

pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
}
```

`ColumnType` is the complete set of supported value types — exactly four. `nullable: false` is enforced at write time; inserting `None` into a non-nullable column produces `AdolapError::StorageError`. `TableSchema` provides `validate_rows` (validates before writing) and `save`/`load` (JSON serialization). Type coercions are allowed at write time: a `U32` value can be stored in an `I32` column if it fits; a string can be stored in an `I32` column if it parses as a valid integer.

### 4.3 `RecordBatch` — the execution unit

Defined in `crates/storage/src/record_batch.rs`. The central data structure passed between executor nodes and returned from every scan:

```rust
pub struct RecordBatch {
    pub schema: TableSchema,
    pub columns: Vec<ColumnInputOwned>,
    pub row_count: usize,
}
```

- `schema` — describes the columns in this batch. Projection and aggregation produce batches with schemas different from the source table.
- `columns` — one `ColumnInputOwned` per column. Each holds a typed value vector (`ColumnValuesOwned::Utf8`, `::I32`, `::U32`, or `::Bool`) and an optional `validity` bitmap. The bitmap uses one bit per row; a cleared bit marks that row as null. When all bits are set (no nulls anywhere), `validity` is `None` to avoid redundant allocation.
- `row_count` — the number of rows in this batch.

Key methods:

```rust
// Materialize as nullable row values — used by the server to build a ResultSet
pub fn to_rows(&self) -> Result<Vec<Vec<Option<ColumnValue>>>, AdolapError>

// Build a batch from row-oriented values, validated against the schema
pub fn from_rows(schema: TableSchema, rows: &[Vec<Option<ColumnValue>>]) -> Result<Self, AdolapError>
```

The executor returns `Vec<RecordBatch>`. The server collects all batches, calls `to_rows` on each, flattens the results, and serializes them into the `ResultSet` the CLI renders.

### 4.4 `Catalog` — filesystem-backed table discovery

Defined in `crates/storage/src/catalog.rs`. The catalog is rooted at the `data/` directory of the server process and resolves table names to metadata by reading the filesystem:

```rust
pub struct Catalog {
    data_root: PathBuf,
}

pub struct DatabaseMetadata {
    pub name: String,
    pub path: PathBuf,
}

pub struct TableMetadata {
    pub database: String,
    pub name: String,
    pub path: PathBuf,
    pub schema: TableSchema,
    pub storage_config: TableStorageConfig,
}

impl TableMetadata {
    pub fn fqn(&self) -> String {
        format!("{}.{}", self.database, self.name)
    }
}
```

Resolution reads `data/<database>/<table>/schema.json` and `data/<database>/<table>/table_config.json`. There is no in-memory catalog state — every `resolve_table` call reads the filesystem, so the server is stateless across restarts.

### 4.5 `AdolapError` — unified error handling

Defined in `crates/core/src/error.rs`. All fallible operations return `Result<T, AdolapError>`:

```rust
#[derive(Debug, Error)]
pub enum AdolapError {
    #[error("Core error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Server error: {0}")]
    ServerError(String),

    #[error("CLI error: {0}")]
    CliError(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Unknown error: {0}")]
    UnknownError(String),
}
```

`IO` uses `#[from]` so any `std::io::Error` converts automatically. All other variants carry a `String` message. The `thiserror` crate generates `Display` from the `#[error]` attributes, giving every variant a consistent human-readable prefix (e.g., `"Storage error: Column x is not nullable"`).

## 5. The Optimizer

The optimizer lives in `crates/exec/src/optimizer.rs` and runs between parsing and physical plan creation. It applies one main rule: **filter-below-project pushdown**.

When the optimizer encounters `Project(Filter(X))`, it rewrites it to `Filter(Project(X))` so the filter runs closer to the scan, reducing the number of rows the projection copies. The rule is applied recursively, so it works at any depth in the tree. All other variants — `Aggregate`, `GroupFilter`, `Join`, `Sort`, `Limit`, `Scan` — are passed through with their children recursively optimized but not structurally reordered.

## 6. Design Principles Visible in the Code

### 6.1 Small crates with clear boundaries

Separate crates prevent protocol, execution, storage, and UI concerns from collapsing into one binary. Adding a new executor rule touches only `exec`; changing the wire format touches only `protocol` and its consumers.

### 6.2 Text-first query language

The query language is line-oriented and SQL-like but intentionally smaller. `FROM` precedes `SELECT`, making the parse tree straightforward to build left-to-right.

### 6.3 Record batches as the execution unit

Every operator — scan, filter, project, aggregate, join, sort, limit — both consumes and produces `Vec<RecordBatch>`. This keeps memory allocation predictable and preserves column-oriented data layout through the full execution pipeline.

### 6.4 Filesystem-backed catalog

Tables are resolved from on-disk metadata without a separate catalog service. Copy a `data/` directory to a new machine and the new server sees the same tables immediately.

### 6.5 Observable query execution

The CLI displays logical and physical plans for every query by default (`\plan on`). The server exposes schema, storage config, segment metadata, and column stats via meta commands.

## 7. A First Concrete Example

```text
CREATE DATABASE analytics;

CREATE TABLE analytics.events (
  event_id U32,
  country Utf8,
  revenue I32,
  is_paying Bool
);

INSERT INTO analytics.events ROWS
(1, "US", 120, true),
(2, "DE", 0, false),
(3, "US", 80, true);

FROM analytics.events
SELECT country, revenue
FILTER revenue > 50
ORDER BY revenue DESC;
```

That sequence exercises: parser DDL/DML support, catalog-backed table resolution (`analytics.events` → `data/analytics/events/`), storage writes into segments, logical planning (`Scan -> Filter -> Project -> Sort`), optimizer pushdown, physical plan execution returning `Vec<RecordBatch>`, server batch-to-ResultSet conversion, and CLI rendering.

## 8. Code Anchors Worth Reading Early

- `crates/server/src/handler.rs` — top-level request routing.
- `crates/exec/src/parser.rs` — AQL syntax and statement parsing.
- `crates/exec/src/logical_plan.rs` — full `LogicalPlan` enum and builder methods.
- `crates/exec/src/optimizer.rs` — filter-below-project pushdown.
- `crates/exec/src/planner.rs` — binding and physical plan creation.
- `crates/exec/src/executor.rs` — recursive physical plan execution.
- `crates/storage/src/catalog.rs` — table discovery and name resolution.
- `crates/storage/src/record_batch.rs` — `RecordBatch` construction and row materialization.
- `crates/storage/src/table_writer.rs` — persistence path for inserted or ingested data.
- `crates/cli/src/repl.rs` — interactive shell and multiline buffering.
- `crates/cli/src/meta.rs` — all CLI meta commands and `ReplSettings`.

## 9. How to Read the Rest of the Book

1. Use Chapter 2 for setup, build, run, and local inspection workflow.
2. Use Chapter 3 for storage and persistence internals.
3. Use Chapter 4 for the query language, binding, and physical planning path.
4. Use Chapter 5 for networking, server dispatch, and CLI behavior.
5. Use Chapter 6 for an end-to-end walkthrough that reconnects all of the above.
