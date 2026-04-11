# Chapter 2: Setup and Local Development

## 1. Goal of This Chapter

This chapter explains how to get Adolap running locally in a repeatable way. It covers:

- building the workspace
- starting the server
- connecting with the CLI
- creating tables and loading data
- running tests
- inspecting plans and metadata
- all meta commands and their outputs
- storage configuration and background compaction
- on-disk directory layout after writes

## 2. Prerequisites

You need:

- Rust stable toolchain
- Cargo
- Two terminals (one for the server, one for the CLI)

Check your toolchain:

```bash
rustc --version
cargo --version
```

## 3. Build the Workspace

From the repository root:

```bash
cargo build
```

To build a single crate:

```bash
cargo build -p server
cargo build -p cli
cargo build -p exec
cargo build -p storage
```

## 4. Run the Full Test Suite

```bash
cargo test
```

For targeted crate iteration:

```bash
cargo test -p protocol
cargo test -p cli
cargo test -p exec
cargo test -p storage
```

## 5. Clean Local State

The server persists table data under `data/` relative to its working directory. To start fresh:

```bash
rm -rf data/
```

## 6. Server Configuration

### 6.1 `ServerConfig`

Defined in `crates/core/src/config.rs`. Controls the listen address, data path, and maximum simultaneous connections:

```rust
pub struct ServerConfig {
    pub listen_addr: String,   // default: "0.0.0.0:5999"
    pub data_path: String,     // default: "./data"
    pub max_connections: usize, // default: 256
}
```

The default values are:

| Field             | Default         | Description                              |
|-------------------|-----------------|------------------------------------------|
| `listen_addr`     | `0.0.0.0:5999`  | TCP bind address                         |
| `data_path`       | `./data`        | Root directory for all persisted tables  |
| `max_connections` | `256`           | Maximum simultaneous client connections  |

### 6.2 Connection Limit Behavior

When the active connection count reaches `max_connections`, new connections are dropped immediately after `accept()`. The accepted socket is dropped without writing any response — the client will see the connection close without a protocol message:

```rust
// crates/server/src/tcp.rs
let current = connection_count.load(Ordering::Relaxed);
if current >= max_connections {
    warn!(%peer, current_connections = current, max_connections,
        "connection limit reached; dropping new connection");
    drop(socket);
    continue;
}
```

The limit is enforced with an `AtomicUsize` counter. Connections decrement the counter when their handler task completes.

## 7. Table Storage Configuration

### 7.1 `TableStorageConfig`

Defined in `crates/storage/src/config.rs`. All eight fields use `#[serde(default)]`, so legacy config files missing the newer fields fall back to their defaults:

```rust
pub struct TableStorageConfig {
    pub row_group_size: usize,
    pub compression: CompressionType,
    pub enable_bloom_filter: bool,
    pub enable_dictionary_encoding: bool,
    pub compaction_segment_threshold: usize,
    pub compaction_row_group_threshold: usize,
    pub enable_background_compaction: bool,
    pub background_compaction_interval_seconds: u64,
}
```

| Field                                    | Default  | Description                                                        |
|------------------------------------------|----------|--------------------------------------------------------------------|
| `row_group_size`                         | `16384`  | Maximum number of rows per row group within a segment              |
| `compression`                            | `Lz4`    | Compression codec: `None`, `Lz4`, or `Zstd`                       |
| `enable_bloom_filter`                    | `true`   | Build per-row-group bloom filters for point-lookup pruning         |
| `enable_dictionary_encoding`             | `true`   | Use dictionary encoding for low-cardinality string columns         |
| `compaction_segment_threshold`           | `8`      | Compact when this many small segments accumulate                   |
| `compaction_row_group_threshold`         | `8`      | Compact when a segment has this many row groups                    |
| `enable_background_compaction`           | `true`   | Run periodic background compaction automatically                   |
| `background_compaction_interval_seconds` | `60`     | Interval in seconds between background compaction sweeps           |

### 7.2 Background Compaction

Background compaction is enabled by default and runs every 60 seconds. It scans all tables under `data/` and merges small segments when either `compaction_segment_threshold` or `compaction_row_group_threshold` is reached (default: 8 for both). The `BackgroundCompactionScheduler` is started when the server launches:

```rust
// crates/server/src/tcp.rs
let _background_compaction = BackgroundCompactionScheduler::new(PathBuf::from("data")).spawn();
```

You can also trigger compaction manually with `\optimize <db.table>` or reclaim stale files with `\vacuum <db.table>`.

### 7.3 Specifying Config at Table Creation

```text
CREATE TABLE analytics.events (
  event_id U32,
  country Utf8,
  revenue I32,
  is_paying Bool
) USING CONFIG (
  compression = "lz4",
  row_group_size = 2,
  bloom_filter = true,
  dictionary_encoding = true
);
```

Setting `row_group_size = 2` with small data is useful for development — it creates multiple row groups from a small insert, making internal storage structure visible in `\segments` and `\stats`.

## 8. Start the Server

```bash
cargo run -p server
```

The server binds to `0.0.0.0:5999` and prints:

```text
Adolap server listening on 0.0.0.0:5999
```

## 9. Start the CLI

Open a second terminal:

```bash
cargo run -p cli
```

The CLI resolves its target address in this order:

1. First command-line argument
2. `ADOLAP_ADDR` environment variable
3. Default `127.0.0.1:5999`

Examples:

```bash
cargo run -p cli -- 127.0.0.1:5999
ADOLAP_ADDR=127.0.0.1:5999 cargo run -p cli
```

Connection logic from `crates/cli/src/client.rs`:

```rust
pub async fn connect() -> Result<(Self, String), AdolapError> {
    let addr = resolve_addr();
    let stream = TcpStream::connect(&addr).await?;
    Ok((Self { stream }, addr))
}
```

## 10. Basic Local Workflow

### 10.1 Create a database

```text
CREATE DATABASE analytics;
```

### 10.2 Create a table

```text
CREATE TABLE analytics.events (
  event_id U32,
  country Utf8,
  device Utf8,
  revenue I32,
  is_paying Bool
) USING CONFIG (
  compression = "lz4",
  row_group_size = 2,
  bloom_filter = true,
  dictionary_encoding = true
);
```

### 10.3 Insert rows

```text
INSERT INTO analytics.events ROWS
(1, "US", "mobile", 120, true),
(2, "US", "desktop", 75, true),
(3, "DE", "mobile", 0, false),
(4, "FR", "tablet", 35, false);
```

### 10.4 Run a query

```text
FROM analytics.events
SELECT country, device, revenue
FILTER revenue > 50
ORDER BY revenue DESC;
```

## 11. Loading Data from Files

### 11.1 NDJSON format

Create a file `events.ndjson` with one JSON object per line:

```json
{"event_id": 100, "country": "US", "device": "mobile", "revenue": 130, "is_paying": true}
{"event_id": 101, "country": "CA", "device": "desktop", "revenue": 25, "is_paying": false}
{"event_id": 102, "country": "US", "device": "tablet", "revenue": 90, "is_paying": true}
```

### 11.2 JSON array format

The same data can be provided as a JSON array in a file `events.json`:

```json
[
  {"event_id": 100, "country": "US", "device": "mobile", "revenue": 130, "is_paying": true},
  {"event_id": 101, "country": "CA", "device": "desktop", "revenue": 25, "is_paying": false},
  {"event_id": 102, "country": "US", "device": "tablet", "revenue": 90, "is_paying": true}
]
```

`TableWriter::ingest_json_file` accepts both formats automatically — it detects whether the top-level value is a JSON array or a sequence of newline-delimited objects.

### 11.3 Ingest command

```text
INGEST INTO analytics.events FROM "events.ndjson";
INGEST INTO analytics.events FROM "events.json";
```

## 12. On-Disk Directory Layout

After creating a database and inserting rows, the `data/` directory looks like this:

```text
data/
  analytics/
    events/
      schema.json          ← TableSchema (column names, types, nullability)
      table_config.json    ← TableStorageConfig (all 8 fields)
      segment_<uuid>/
        metadata.json      ← SegmentMetadata (row counts, row group stats)
        row_group_0/
          event_id.col     ← compressed column data
          country.col
          device.col
          revenue.col
          is_paying.col
        row_group_1/
          ...
      segment_<uuid>/      ← created by a second insert or after compaction
        ...
```

Each `INSERT` or `INGEST` creates a new segment directory. Background compaction merges small segments according to `compaction_segment_threshold` and `compaction_row_group_threshold`. After `\optimize`, merged segments replace the originals. After `\vacuum`, stale compaction artifacts are removed.

## 13. All Meta Commands

Meta commands start with `\`. They are dispatched by the CLI to the server (for catalog and storage commands) or handled locally (for setting toggles).

| Command                              | What It Does / What It Shows                                                    |
|--------------------------------------|---------------------------------------------------------------------------------|
| `\q`, `\quit`                        | Exit the CLI session                                                            |
| `\l`, `\databases`                   | Table of all databases: name, table count, total rows, has_data flag            |
| `\dt`, `\tables [database]`          | Table of all (or filtered) tables: database, table, columns, rows, has_data    |
| `\d <db.table>`, `\schema <db.table>`| Table path and column listing: name, type, nullable                             |
| `\segments <db.table>`               | One line per segment: index, rows, row_groups, size_bytes, created_at           |
| `\storage <db.table>`                | All 8 `TableStorageConfig` fields for the table                                 |
| `\stats <db.table>`                  | Aggregate stats: segments, row_groups, rows, size_bytes, per-column min/max/nulls|
| `\optimize <db.table>`               | Compact segments; reports input/output counts and bytes before/after            |
| `\vacuum <db.table>`                 | Remove stale compaction leftovers; reports removed paths                        |
| `\explain <query>`                   | Build logical and physical plans without executing; also shows server RSS bytes |
| `\plan [on\|off]`                    | Toggle logical/physical plan display in query output (default: on)              |
| `\timing [on\|off]`                  | Toggle `client_round_trip_ms` in query summary (default: off)                   |
| `\profile [on\|off]`                 | Toggle `client_round_trip_ms` + `response_bytes` in query summary (default: off)|
| `\format [unicode\|ascii\|csv\|tsv]` | Set result table rendering format (default: unicode)                            |
| `\memory`                            | Show CLI process RSS and virtual memory bytes                                   |
| `\version`                           | Print CLI version from `CARGO_PKG_VERSION`                                      |
| `\about`                             | Print short description of the CLI                                              |
| `\help`, `\?`, `\commands`           | Print all meta commands                                                         |

### 13.1 What `\plan on` outputs

When `\plan on` is active (the default), every query response includes two extra sections between the Query Summary and the Query Results:

```
=== Logical Plan ===
Scan(analytics.events) -> Filter(revenue > 50) -> Project(country, revenue)

=== Physical Plan ===
Scan(table=analytics.events, projected=[country, revenue], predicate=revenue > 50) -> Project(country, revenue)
```

Plans are rendered in compact arrow notation: each node is separated by ` -> `, listing the operation name and its key parameters. Use `\plan off` to suppress these sections.

### 13.2 What `\timing on` outputs

When `\timing on` is active, the Query Summary includes an additional line:

```
=== Query Summary ===
rows: 2
columns: 2
batches: 1
server_execution_ms: 0.812
client_round_trip_ms: 3.441     ← added by \timing on
```

`server_execution_ms` is measured by the server from plan start to batch completion. `client_round_trip_ms` is the wall-clock duration measured by the CLI from sending the request frame to receiving the response frame, in milliseconds with three decimal places.

### 13.3 What `\profile on` outputs

`\profile on` is a superset of `\timing on`. It adds `response_bytes` to the summary:

```
=== Query Summary ===
rows: 2
columns: 2
batches: 1
server_execution_ms: 0.812
client_round_trip_ms: 3.441
response_bytes: 512
```

### 13.4 What `\explain` outputs

`\explain` sends the query to the server for plan construction without executing it. The server returns:

```
=== Logical Plan ===
Scan(analytics.events) -> Filter(revenue > 50) -> Project(country, revenue) -> Sort(revenue DESC)

=== Physical Plan ===
Scan(table=analytics.events, ...) -> ...

=== Server Summary ===
server_rss_bytes: 18432000
```

## 14. CLI Ergonomics

- Multiline queries are supported; a query is complete when it ends with `;`.
- The primary prompt is `adolap> `; the continuation prompt is `.....> `.
- History is saved to `.adolap_history` in the user home directory.
- `Ctrl+C` on a non-empty buffer clears it; `Ctrl+C` on an empty buffer exits.
- Output formats: `unicode` (default, box-drawing characters), `ascii` (+/- borders), `csv`, `tsv`.

## 15. Common Local Scenarios

### 15.1 Validate a parser or planner change

Run representative queries with `\plan on` and compare logical plan, physical plan, and result output before and after your change.

```text
\explain FROM analytics.events
SELECT country, revenue
FILTER revenue > 50
ORDER BY revenue DESC
LIMIT 3
```

### 15.2 Validate storage writes and pruning

Use a small `row_group_size` (e.g. `2`) so small inserts create multiple row groups. Then inspect:

```text
\segments analytics.events
\stats analytics.events
```

### 15.3 Validate protocol changes

Whenever you change a request or response shape:

```bash
cargo test -p protocol -p cli -p server
```

This checks codec round trips, CLI behavior, and server compilation together.

## 16. Troubleshooting

### 16.1 The CLI cannot connect

Check that the server is running and bound to the expected address. The CLI default is `127.0.0.1:5999`.

### 16.2 The server starts but queries do not see old data

Data is relative to the server process working directory. If you launch the server from a different directory, the effective `data/` root changes.

### 16.3 You want a clean tutorial run

Delete `data/` and restart the server.

### 16.4 Many crates rebuild after changing shared types

Expected in a multi-crate workspace when shared `protocol` or `storage` types change.

## 17. Chapter Takeaway

After this chapter you should be able to:

- Build the workspace confidently.
- Run the server and CLI locally.
- Understand all `TableStorageConfig` fields and their defaults.
- Know that connections beyond 256 are dropped immediately.
- Create and populate tables with direct `INSERT` or file `INGEST` (NDJSON or JSON array).
- Query and inspect data with meta commands.
- Use `\plan`, `\timing`, `\profile`, and `\explain` as debugging surfaces while making code changes.
