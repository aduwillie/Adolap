# Architecture Overview

## 1. Why this document exists

The chapter docs explain individual subsystems, but Adolap also benefits from a system-level map. This document introduces that map. It focuses on three questions:

1. What are the major runtime components?
2. Why are they separated the way they are?
3. How do those decisions show up in this repository today?

The goal is not to describe an abstract ideal database. The goal is to describe the concrete architecture that exists in this repo.

## 2. One-sentence architecture summary

Adolap is a local analytical database implemented as a Rust workspace where a CLI speaks a binary TCP protocol to a server, the server owns query parsing and execution, the execution layer reads and transforms `RecordBatch` values, and the storage layer persists tables as filesystem-backed segments and row groups.

## 3. The top-level system model

Adolap is easiest to understand as six cooperating layers:

1. A human types AQL or meta commands into the CLI.
2. The CLI serializes requests into protocol messages and sends them over TCP.
3. The server decodes those messages and routes them to query execution or metadata handlers.
4. The execution engine parses text, builds a logical plan, binds it against catalog metadata, and produces a physical plan.
5. The executor reads `RecordBatch` values from storage and applies relational operators.
6. The server converts results into protocol messages and sends them back to the CLI for rendering.

That separation is visible directly in the workspace layout:

| Area | Crate | Main responsibility |
| --- | --- | --- |
| Shared primitives | `crates/core` | Errors, ids, time, logging, and basic shared types. |
| Persistence | `crates/storage` | Catalog resolution, schemas, record batches, segments, row groups, column IO, stats, bloom filters. |
| Query engine | `crates/exec` | Parsing, logical plans, optimization, binding, physical plans, and execution. |
| Wire protocol | `crates/protocol` | Message model, binary codecs, and length-prefixed framing. |
| Runtime server | `crates/server` | TCP listener, request dispatch, query execution orchestration, server-side meta commands. |
| Interactive client | `crates/cli` | REPL, TCP client, shell settings, formatting, history, and developer-oriented ergonomics. |

## 4. Architectural principles

Several architectural decisions are repeated throughout the codebase.

### 4.1 The server is authoritative

Even though Adolap is a local developer-oriented system, the CLI is intentionally thin. It does not inspect the filesystem itself, does not parse the storage layout locally, and does not try to reconstruct planner behavior on the client side.

That decision shows up in two concrete ways:

- The CLI sends text or meta commands to the server instead of executing them locally.
- Metadata commands such as `\schema`, `\segments`, `\storage`, `\stats`, and `\explain` are handled on the server in `crates/server/src/meta.rs`.

Why this is a good decision here:

- The server owns the actual `data/` root.
- Query planning depends on catalog state known to the server.
- CLI introspection stays consistent with the process executing queries.

### 4.2 AQL and transport are separate concerns

Users type a text language, but processes communicate using a binary protocol. That is deliberate.

Theory:

- A language describes intent.
- A transport describes delivery.

If those are collapsed together, the CLI, server, and protocol logic become harder to evolve independently.

Implementation in this repo:

- `crates/exec/src/parser.rs` handles AQL text.
- `crates/protocol/src/message.rs` defines the wire messages.
- `crates/protocol/src/codec.rs` serializes and deserializes them.
- `crates/protocol/src/framing.rs` handles `[u32 length][payload]` framing and knows nothing about query semantics.

This is a strong separation for a small codebase because it keeps the networking layer simple and keeps AQL changes from forcing transport changes unless the external message contract actually changes.

### 4.3 `RecordBatch` is the cross-layer data unit

Adolap is not row-at-a-time internally. It also is not a deeply vectorized Arrow-style engine. It uses a middle-ground abstraction: a `RecordBatch` containing a schema, typed column vectors, validity bitmaps, and a row count.

Why this matters:

- Storage can stay column-oriented.
- Execution operators can work on grouped data instead of individual rows.
- The system remains understandable without a large execution framework.

Implementation in this repo:

- `crates/storage/src/record_batch.rs` defines the in-memory batch shape.
- `RowGroupReader` materializes a row group into a `RecordBatch`.
- The executor consumes vectors of `RecordBatch` values.
- The server converts query output batches into protocol result rows.

`Executor::execute` returns `Vec<RecordBatch>`. The handler's `convert_batches_to_resultset`
iterates **all** batches in that vector to build the wire `ResultSet`, so tables spanning
multiple row groups return complete results:

```rust
// crates/server/src/handler.rs
// Iterate every batch so that tables spanning multiple row groups return
// complete results instead of only the first batch.
for batch in &batches {
    for row_idx in 0..batch.row_count {
        // ... serialize every row from every batch
    }
}
```

### 4.4 The filesystem is the catalog and storage authority

Adolap does not have a separate metadata service. Table existence, schema files, segment metadata, and row-group metadata on disk are the source of truth.

Implementation in this repo:

- `Catalog` in `crates/storage/src/catalog.rs` resolves logical table names to filesystem locations and loaded metadata.
- `TableWriter` persists `schema.json` and `table.config.json` into table directories.
- Segment and row-group metadata are stored alongside the data they describe.

This is an intentional learning-oriented design. It keeps all core state inspectable on disk and makes the system easy to reason about locally.

### 4.5 The crate graph mirrors the runtime graph

The workspace split is not cosmetic. It roughly matches the runtime execution path:

```
cli -> protocol -> server -> exec + storage -> protocol -> cli
```

That matters because the repository is trying to teach system boundaries, not just package files differently.

### 4.6 Async runtime and observability

The server runs on **Tokio**, the standard asynchronous Rust runtime. Every I/O call — TCP framing, filesystem reads, and compaction — is `async` and scheduled cooperatively.

Structured logging throughout the server uses the **`tracing`** crate. Spans and events carry typed key-value fields instead of format strings, making logs machine-parseable:

```rust
// crates/server/src/tcp.rs
info!(%addr, max_connections, "server listening");
info!(%peer, connections = current + 1, "client connected");
warn!(%peer, current_connections = current, max_connections,
      "connection limit reached; dropping new connection");
```

You can control log verbosity with the `RUST_LOG` environment variable, for example `RUST_LOG=adolap_server=debug`.

### 4.7 Error propagation with `AdolapError`

All fallible operations in the server, storage, and exec crates return `Result<T, AdolapError>`. The enum is defined in `crates/core/src/error.rs` and variants include:

- `AdolapError::ExecutionError(String)` — runtime failures during query execution.
- `AdolapError::StorageError(String)` — filesystem or serialization problems.
- `AdolapError::Serialization(String)` — codec failures (postcard / serde_json).
- `AdolapError::Io(std::io::Error)` — low-level I/O via the `From<io::Error>` impl.

This single error type propagates with `?` across crate boundaries so that every handler function has a uniform signature:

```rust
pub async fn handle_message(msg: ClientMessage) -> Result<ServerMessage, AdolapError>
```

## 5. TCP server internals

### 5.1 Entry point

`crates/server/src/tcp.rs` provides two public functions:

```rust
pub async fn start_server(addr: &str) -> Result<(), AdolapError>
pub async fn start_server_with_limit(addr: &str, max_connections: usize) -> Result<(), AdolapError>
```

`start_server` delegates to `start_server_with_limit` using the constant
`DEFAULT_MAX_CONNECTIONS = 256`. The limit is also exposed via `ServerConfig`
in `crates/core/src/config.rs`:

```rust
pub struct ServerConfig {
    pub listen_addr: String,    // default "0.0.0.0:5999"
    pub data_path: String,      // default "./data"
    pub max_connections: usize, // default 256
}
```

`ServerConfig` is JSON-serializable with `serde`, and missing fields fall back to defaults, so a config file written before `max_connections` was added continues to work.

### 5.2 Connection limiting

The accept loop tracks live connections with a shared `AtomicUsize`. This counter is lock-free and accurate across all spawned tasks:

```rust
let connection_count = Arc::new(AtomicUsize::new(0));

loop {
    let (socket, peer) = listener.accept().await?;

    let current = connection_count.load(Ordering::Relaxed);
    if current >= max_connections {
        warn!(%peer, current_connections = current, max_connections,
              "connection limit reached; dropping new connection");
        drop(socket);
        continue;
    }

    let count = Arc::clone(&connection_count);
    count.fetch_add(1, Ordering::Relaxed);

    task::spawn(async move {
        if let Err(e) = handle_connection(socket).await {
            error!(error = %e, "connection handler failed");
        }
        count.fetch_sub(1, Ordering::Relaxed); // always decremented on disconnect
    });
}
```

A connection that is dropped at the limit does not increment the counter, so a brief surge of connections cannot permanently inflate it.

### 5.3 Per-connection loop

Each accepted connection runs `handle_connection` on its own Tokio task:

```rust
async fn handle_connection(mut socket: TcpStream) -> Result<(), AdolapError> {
    loop {
        let frame = read_frame(&mut socket).await?;
        let msg = decode_client_message(&frame)?;
        let response = handle_message(msg).await?;
        let bytes = encode_server_message(&response);
        write_frame(&mut socket, &bytes).await?;
    }
}
```

The loop exits when `read_frame` returns an error, which is the expected path for a client that closes its TCP connection.

### 5.4 Background compaction at startup

Immediately after binding the listener, `tcp.rs` spawns a `BackgroundCompactionScheduler`:

```rust
let _background_compaction =
    BackgroundCompactionScheduler::new(PathBuf::from("data")).spawn();
```

The scheduler runs as a detached Tokio task. It wakes every 5 seconds
(`SCHEDULER_POLL_INTERVAL_SECONDS`), lists all tables via the catalog, and for
each table whose per-table compaction interval has elapsed, runs
`SegmentCompactor::maybe_compact`. Tables opt in via their `table.config.json`:

```json
{
  "enable_background_compaction": true,
  "background_compaction_interval_seconds": 60
}
```

See section 9 for the full compaction story.

## 6. Request dispatch

### 6.1 Handler overview

`crates/server/src/handler.rs` is the single point where protocol messages become storage and execution operations. It creates a `Catalog` pointed at `"data/"` and then matches on `ClientMessage`:

```rust
pub async fn handle_message(msg: ClientMessage) -> Result<ServerMessage, AdolapError> {
    let catalog = Catalog::new(PathBuf::from(DATA_ROOT));
    match msg {
        ClientMessage::Ping               => Ok(ServerMessage::Pong),
        ClientMessage::QueryText(text)    => { /* parse → optimize → bind → execute */ }
        ClientMessage::CreateDatabase     => { /* catalog.create_database */ }
        ClientMessage::CreateTable        => { /* catalog.create_table */ }
        ClientMessage::InsertRows         => { /* catalog.resolve_insert_target → TableWriter */ }
        ClientMessage::IngestInto         => { /* catalog.open_table_writer → ingest_json_file */ }
        ClientMessage::MetaCommand(cmd)   => handle_meta_command(&catalog, &cmd).await,
    }
}
```

### 6.2 Query execution pipeline inside the handler

For `ClientMessage::QueryText`, the handler runs the full five-stage pipeline:

```
parse_statement → optimizer::optimize → planner::bind → planner::to_physical → Executor::execute
```

Timing is measured around the executor call so query summaries can report wall-clock milliseconds. The result batches are then converted to the wire `ResultSet` by iterating every batch:

```rust
async fn execute_query(catalog: &Catalog, plan: LogicalPlan) -> Result<ServerMessage, AdolapError> {
    let started = Instant::now();
    // ... optimize, bind, plan
    let batches = Executor::new().execute(&physical_plan).await?;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let result_set = convert_batches_to_resultset(batches)?;
    // wrap in ServerMessage::QueryResult { result_set, summary, plan_text }
}
```

## 7. The two main request flows

Adolap has two dominant request categories.

### 7.1 Flow A: query and data-manipulation path

This path covers `FROM ... SELECT ...`, `CREATE TABLE`, `INSERT INTO`, `DELETE FROM`, and `INGEST INTO`.

Detailed path:

1. The REPL in `crates/cli/src/repl.rs` collects input until it sees a terminating `;`.
2. The client in `crates/cli/src/client.rs` encodes a `ClientMessage`.
3. `crates/protocol/src/framing.rs` writes a length-prefixed frame (`[u32 length][payload]`).
4. The server in `crates/server/src/tcp.rs` reads the frame and decodes the message.
5. `crates/server/src/handler.rs` matches on the `ClientMessage` variant.
6. For text queries, `parse_statement` in `crates/exec/src/parser.rs` returns a `Statement`.
7. Query statements are optimized, bound with catalog metadata, converted to a physical plan, and executed.
8. Storage readers load batches from disk as needed; each segment is visited in sorted order.
9. The server packages all result batches and plan strings into a `ServerMessage::QueryResult` or returns `ServerMessage::Ok` for non-query statements.
10. The CLI decodes the response and renders it.

### 7.2 Flow B: metadata and introspection path

This path covers commands such as `\databases`, `\tables analytics`, `\schema analytics.events`, and `\explain ...`.

Detailed path:

1. The CLI sees an input line starting with `\` while no multiline query is active.
2. It forwards the command to the server as `ClientMessage::MetaCommand`.
3. `crates/server/src/meta.rs` resolves the command.
4. The server uses the catalog, segment metadata, stats decoding, and planner helpers to produce a human-readable response.
5. The CLI renders that response without needing direct filesystem knowledge.

This separation is one of the most important architectural choices in the repo. Introspection is implemented as a first-class server feature, not as a debugging shortcut in the client.

## 8. Storage architecture in the large

### 8.1 Hierarchy

The storage layer is organized as:

```
database → table → segment → row group → column files
```

In the canonical layout, tables live under:

```text
data/<database>/<table>/
```

The catalog also preserves compatibility with a legacy layout for default-database tables stored directly under:

```text
data/<table>/
```

That fallback is not just documented intent. `Catalog::list_tables` and table resolution explicitly support it when `schema.json` is present.

### 8.2 Catalog resolution

`Catalog` (`crates/storage/src/catalog.rs`) is the gateway between logical table names and filesystem paths:

```rust
pub struct Catalog {
    data_root: PathBuf,
}

impl Catalog {
    pub fn new(data_root: PathBuf) -> Self { ... }

    // Resolve "db.table" or "table" to a TableMetadata with its path and schema.
    pub async fn resolve_table(&self, table_ref: &str) -> Result<TableMetadata, AdolapError>;

    // List all databases under data_root.
    pub async fn list_databases(&self) -> Result<Vec<DatabaseMetadata>, AdolapError>;

    // List every table across all databases plus the default root.
    pub async fn list_tables(&self) -> Result<Vec<TableMetadata>, AdolapError>;

    // Create a new table directory with schema.json and table.config.json.
    pub async fn create_table(
        &self, fqn: &str,
        schema: &TableSchema,
        config: &TableStorageConfig,
    ) -> Result<TableMetadata, AdolapError>;
}
```

`TableMetadata` carries the resolved `path`, loaded `schema`, and `storage_config` so callers never need to parse JSON themselves.

### 8.3 Immutable segment writes

Adolap's storage is designed around immutable segment creation. New inserts create a new `segment_<N>` directory rather than appending into an existing segment in place. Deletes are implemented as read-filter-rewrite: the handler reads all batches, removes matching rows, and calls `replace_rows` to write the survivors back as a fresh set of segments while clearing the old ones.

### 8.4 Column storage and bloom filters

Each row group directory contains one file per column:

```text
segment_0/
  row_group_0/
    column_0.bin        ← compressed column data
    column_0.bloom      ← optional bloom filter (variable size Vec<u8>)
    column_0.stats      ← min/max statistics
    column_0.dict       ← optional dictionary
    row_group.meta      ← row count, null bitmaps, etc.
```

Bloom filters are covered in depth in `docs/07_bloom_filters.md`.

## 9. Compaction

### 9.1 Why compaction matters

Each `INSERT` or `INGEST` call produces a new segment. Without compaction, a table receiving many small inserts accumulates many small segment directories. More segments mean more file handles opened per scan and higher per-row-group overhead from metadata reads.

### 9.2 `TableStorageConfig` controls compaction behavior

```rust
// crates/storage/src/config.rs
pub struct TableStorageConfig {
    pub row_group_size: usize,                          // default 16_384
    pub compression: CompressionType,                   // default Lz4
    pub enable_bloom_filter: bool,                      // default true
    pub enable_dictionary_encoding: bool,               // default true
    pub compaction_segment_threshold: usize,            // default 8
    pub compaction_row_group_threshold: usize,          // default 8
    pub enable_background_compaction: bool,             // default true
    pub background_compaction_interval_seconds: u64,    // default 60
}
```

`compaction_segment_threshold` is the minimum segment count that triggers a compaction pass. `compaction_row_group_threshold` applies the same gate to row groups within a segment. Both thresholds must be exceeded for `SegmentCompactor::maybe_compact` to actually rewrite anything.

### 9.3 `BackgroundCompactionScheduler`

```rust
// crates/storage/src/background_compaction.rs
pub struct BackgroundCompactionScheduler {
    data_root: PathBuf,
    last_attempts: Arc<Mutex<HashMap<PathBuf, Instant>>>,
}

impl BackgroundCompactionScheduler {
    pub fn new(data_root: PathBuf) -> Self { ... }

    // Spawn a long-running Tokio task. Returns a JoinHandle.
    pub fn spawn(self) -> JoinHandle<()>;

    // Run one pass over all eligible tables.
    pub async fn run_once(&self) -> Result<usize, AdolapError>;
}
```

The scheduler wakes every `SCHEDULER_POLL_INTERVAL_SECONDS` (5 seconds), then for each table:

1. Skips the table if `enable_background_compaction` is `false`.
2. Checks whether enough time has elapsed since the last attempt (`background_compaction_interval_seconds`).
3. Calls `SegmentCompactor::maybe_compact`. If it produced a `CompactionReport`, logs the result.

The `last_attempts` map stores a per-table `Instant` so the scheduler does not hammer a table that does not need compaction on every 5-second tick.

## 10. Query engine architecture in the large

The query engine follows a classic staged database pipeline.

### 10.1 Parse

`crates/exec/src/parser.rs` turns text into a `Statement`. A `Statement` is either a `Query(LogicalPlan)` or one of the DML variants (`CreateTable`, `InsertRows`, `DeleteRows`, `IngestInto`, etc.).

### 10.2 Optimize

`crates/exec/src/optimizer.rs` currently applies a small recursive rule set, most notably filter/project reordering where possible.

### 10.3 Bind

`crates/exec/src/planner.rs` resolves table references through the catalog, validates column names, canonicalizes grouped expressions, and derives output schemas.

### 10.4 Plan physically

The same planner module turns bound logical plans into physical plans while tracking:

- required columns for projection pushdown
- scan predicates for filter pushdown
- join-side requirements

### 10.5 Execute

`crates/exec/src/executor.rs` runs the physical plan recursively and returns all batches:

```rust
pub struct Executor;

impl Executor {
    pub fn execute<'b>(
        &'b self,
        plan: &'b PhysicalPlan<'b>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, AdolapError>> + Send + 'b>> {
        Box::pin(async move {
            match plan {
                PhysicalPlan::Scan { table, projected_columns, predicate } => {
                    TableReader::new(&table.path, &table.schema)
                        .read_table(predicate, projected_columns.clone())
                        .await
                }
                PhysicalPlan::Filter { input, predicate } => {
                    let batches = self.execute(input).await?;
                    filter_batches(&batches, predicate)
                }
                PhysicalPlan::HashJoin { left, right, left_on, right_on, output_schema } => {
                    let left_batches = self.execute(left).await?;
                    let right_batches = self.execute(right).await?;
                    hash_join_batches(&left_batches, &right_batches, left_on, right_on, output_schema)
                }
                // ... Project, HashAggregate, Sort, Limit
            }
        })
    }
}
```

This staged design is important because it keeps responsibilities explicit. The parser does not need schema knowledge, the binder does not need wire protocol knowledge, and the executor does not need to infer user syntax.

## 11. Server and CLI architecture in the large

The CLI is deliberately developer-friendly. It supports:

- multiline statements
- history persisted to `.adolap_history`
- `Ctrl+C` behavior that clears the active buffer before exiting an idle session
- local toggles for plan, timing, profile, and output format

The server is deliberately thin in another sense: it orchestrates, but it does not duplicate execution logic that belongs in `exec` or persistence logic that belongs in `storage`.

That division makes the main server dispatch in `crates/server/src/handler.rs` a good architecture entry point. It is where transport-level requests become execution- and storage-level operations.

## 12. Why this architecture is a good fit for this repository

Adolap is not trying to be a production-distributed warehouse. The current architecture fits the project because it optimizes for:

- readability over maximal performance
- inspectability over abstraction hiding
- clear crate boundaries over monolithic convenience
- explicit metadata over magic behavior
- local iteration over deployment complexity

Those choices make the repo useful as both a working toy database and a serious systems-learning project.

## 13. Known limitations that shape the architecture today

Accurate documentation should also name the current edges of the design.

- The catalog is process-local and filesystem-backed; there is no transactional metadata layer.
- Deletes are implemented as full-table rewrite, not as tombstones or in-place mutation.
- Result transport is presently string-oriented at the wire level.
- The optimizer is intentionally small and rule-based rather than cost-based.

These are not contradictions of the architecture. They are the current stage of the architecture.

## 14. Where to go next

Read the remaining docs in this order if you want to understand the codebase deeply:

1. `docs/01_introduction.md` for project framing.
2. This architecture overview for system boundaries.
3. `docs/02_setup_and_local_development.md` for how to run and inspect the system.
4. `docs/03_storage_engine.md` for the physical data model and storage implementation.
5. `docs/04_query_language_and_planning.md` for the logical and physical query pipeline.
6. `docs/05_protocol_server_and_cli.md` for operational behavior.
7. `docs/06_end_to_end_walkthrough.md` for a concrete end-to-end exercise.
8. `docs/07_bloom_filters.md` for the variable-size bloom filter implementation.
