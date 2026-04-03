# Chapter 1: Introduction

## What Adolap is

Adolap is a compact database system written in Rust. It is not just a parser or a storage library. It is a complete local system with the following layers working together:

- A text language for queries and data definition.
- A planner and executor that transform text into record batches.
- A storage engine that persists data on disk using segments and row groups.
- A binary protocol used over TCP.
- A server process that accepts requests and executes them.
- An interactive CLI for local development and inspection.

The project is useful in two modes:

- As a learning codebase for database internals.
- As a working local system you can run, query, and extend.

## The main mental model

The shortest accurate explanation of Adolap is this:

1. The CLI sends either AQL text or meta commands over a binary TCP protocol.
2. The server parses the request and routes it.
3. Query text becomes a logical plan.
4. The logical plan is optimized, bound against catalog metadata, and converted into a physical plan.
5. The executor reads table data from storage and produces one or more `RecordBatch` values.
6. The server converts those batches into protocol result rows and returns them to the CLI.
7. The CLI renders the result, optionally including logical and physical plans.

## Workspace structure

The repository is a Cargo workspace with focused crates:

```text
Adolap/
  Cargo.toml
  Readme.md
  docs/
  crates/
    cli/
    core/
    exec/
    protocol/
    server/
    storage/
```

Each crate has a narrow responsibility.

### `core`

The `core` crate provides shared building blocks such as the common `AdolapError` type, ids, logging helpers, config helpers, and utilities. The rest of the workspace depends on it to avoid redefining shared concepts.

### `storage`

The `storage` crate owns the physical data model:

- table schemas
- record batches
- filesystem catalog discovery
- table writing and reading
- segment and row-group metadata
- stats and bloom filters
- compression-related configuration

### `exec`

The `exec` crate contains the query engine proper:

- parser and DSL
- logical and physical plan structures
- optimizer rules
- planner and binding logic
- predicate evaluation
- execution helpers

### `protocol`

The `protocol` crate defines what goes over the wire. It includes:

- message types
- serialization and deserialization
- framing helpers for length-prefixed TCP communication

### `server`

The `server` crate listens on a socket, receives framed messages, dispatches commands, runs queries, and exposes server-side metadata inspection commands.

### `cli`

The `cli` crate is the human-facing shell. It manages:

- interactive input
- multiline query buffering
- command history
- meta commands
- output formatting
- planner visibility and profiling toggles

## Core design principles visible in the code

Several design choices repeat throughout the repository:

### 1. Small crates with clear boundaries

The workspace uses separate crates so protocol, execution, storage, and UI concerns do not collapse into one binary crate.

### 2. Text-first query language

The main user-facing language is line-oriented and SQL-like, but intentionally smaller and easier to parse than full SQL.

### 3. Record batches as the execution unit

Execution and storage exchange `RecordBatch` values instead of individual rows. This keeps the code simple while preserving a column-oriented model.

### 4. Filesystem-backed catalog

Tables are resolved from on-disk metadata rather than an external catalog service.

### 5. Observable query execution

The CLI can display logical and physical plans, and the server exposes table, storage, segment, and stats metadata via meta commands.

## A first concrete example

This short session shows the whole vertical slice from query text to persisted data:

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

That sequence exercises:

- parser support for DDL and DML
- catalog-backed table resolution
- storage writes into one or more segments
- logical and physical planning for a scan/filter/project/sort query
- record-batch materialization
- server-to-CLI result transport

## Code anchors worth reading early

These files are the best entry points for understanding the full system:

- `crates/server/src/handler.rs`: top-level request routing.
- `crates/exec/src/parser.rs`: AQL syntax and statement parsing.
- `crates/exec/src/logical_plan.rs`: the logical model of a query.
- `crates/exec/src/planner.rs`: binding and physical plan creation.
- `crates/storage/src/catalog.rs`: table discovery and name resolution.
- `crates/storage/src/table_writer.rs`: persistence path for inserted or ingested data.
- `crates/cli/src/repl.rs`: interactive shell behavior.

## Example code snippets

### The server is the main orchestration point

The handler shows the high-level flow clearly:

```rust
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

That is a useful orientation point because it touches execution, storage, and protocol boundaries in one place.

### The logical plan is intentionally readable

The logical plan is represented as explicit enum variants:

```rust
pub enum LogicalPlan {
    Scan { table_ref: String, table: Option<TableMetadata> },
    Filter { input: Box<LogicalPlan>, predicate: Expr },
    Project { input: Box<LogicalPlan>, columns: Vec<String> },
    Aggregate { ... },
    GroupFilter { ... },
    Join { ... },
    Sort { ... },
    Limit { ... },
}
```

This is one of the strongest “database book” qualities in the project: the central abstractions are explicit and easy to read.

## How to read the rest of the book

The most effective reading order is:

1. Read `docs/architecture.md` next for the system-wide component map and the main request flows.
2. Use Chapter 2 for setup, build, run, and local inspection workflow.
3. Use Chapter 3 for storage and persistence internals.
4. Use Chapter 4 for the query language, binding, and physical planning path.
5. Use Chapter 5 for networking, server dispatch, and CLI behavior.
6. Use Chapter 6 for an end-to-end walkthrough that reconnects all of the above.