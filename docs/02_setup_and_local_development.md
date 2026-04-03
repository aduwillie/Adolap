# Chapter 2: Setup and Local Development

## Goal of this chapter

This chapter explains how to get Adolap running locally in a repeatable way. It covers the practical workflow you will use most often:

- building the workspace
- starting the server
- connecting with the CLI
- creating tables
- loading sample data
- running tests
- inspecting plans and metadata

## Prerequisites

You need:

- Rust stable
- Cargo
- two terminals

Check your toolchain:

```powershell
rustc --version
cargo --version
```

## Build the workspace

From the repository root:

```powershell
cargo build
```

This compiles all crates in the workspace. If you only want to iterate on one area, build the relevant crate:

```powershell
cargo build -p server
cargo build -p cli
cargo build -p exec
cargo build -p storage
```

## Run the full test suite

Use the full workspace test suite first, especially after changing shared types or storage behavior:

```powershell
cargo test
```

When iterating faster, use targeted crate tests:

```powershell
cargo test -p protocol
cargo test -p cli
cargo test -p exec
cargo test -p storage
```

## Clean local state

The server persists table data under `data/` relative to the server process working directory. If you want a clean environment for examples in this chapter:

```powershell
Remove-Item -Recurse -Force .\data -ErrorAction SilentlyContinue
```

## Start the server

Run the TCP server:

```powershell
cargo run -p server
```

The current server entrypoint binds to `0.0.0.0:5999`.

You should see:

```text
Adolap server listening on 0.0.0.0:5999
```

The relevant entrypoint is simple by design:

```rust
#[tokio::main]
async fn main() {
    if let Err(e) = start_server("0.0.0.0:5999").await {
        eprintln!("Server error: {}", e);
    }
}
```

This keeps startup logic obvious and pushes runtime behavior into the server crate.

## Start the CLI

Open a second terminal and run:

```powershell
cargo run -p cli
```

The CLI resolves its address in this order:

1. first command-line argument
2. `ADOLAP_ADDR` environment variable
3. default `127.0.0.1:5999`

Examples:

```powershell
cargo run -p cli -- 127.0.0.1:5999
```

```powershell
$env:ADOLAP_ADDR = "127.0.0.1:5999"
cargo run -p cli
```

The connection logic is small and easy to reason about:

```rust
pub async fn connect() -> Result<(Self, String), AdolapError> {
    let addr = resolve_addr();
    let stream = TcpStream::connect(&addr).await?;
    Ok((Self { stream }, addr))
}
```

## Basic local workflow

### Step 1: create a database

```text
CREATE DATABASE analytics;
```

### Step 2: create a table

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

This exercise is useful because it proves:

- parser support for `CREATE TABLE`
- schema persistence
- storage config persistence
- catalog discovery under `data/analytics/events`

### Step 3: insert a few rows directly

```text
INSERT INTO analytics.events ROWS
(1, "US", "mobile", 120, true),
(2, "US", "desktop", 75, true),
(3, "DE", "mobile", 0, false),
(4, "FR", "tablet", 35, false);
```

### Step 4: run a query

```text
FROM analytics.events
SELECT country, device, revenue
FILTER revenue > 50
ORDER BY revenue DESC;
```

### Step 5: inspect plans and metadata

```text
\plan on
\timing on
\profile on
\schema analytics.events
\storage analytics.events
\segments analytics.events
\stats analytics.events
```

## Loading sample data from files

One of the simplest realistic workflows is ingesting JSON or NDJSON files.

Create a file named `events.ndjson`:

```json
{"event_id": 100, "country": "US", "device": "mobile", "revenue": 130, "is_paying": true}
{"event_id": 101, "country": "CA", "device": "desktop", "revenue": 25, "is_paying": false}
{"event_id": 102, "country": "US", "device": "tablet", "revenue": 90, "is_paying": true}
{"event_id": 103, "country": "DE", "device": "mobile", "revenue": 10, "is_paying": false}
```

Then ingest it:

```text
INGEST INTO analytics.events FROM "events.ndjson";
```

This goes through `TableWriter::ingest_json_file`, which accepts either:

- a JSON array of rows
- newline-delimited JSON objects

That makes local experimentation easy because you do not need a custom import tool.

## CLI ergonomics

The CLI is intentionally helpful for development:

- multiline queries are supported
- commands are complete when they end with `;`
- history is saved to `.adolap_history` in the user home directory
- Ctrl+C clears the in-progress query buffer before exiting an idle session
- output formats include `unicode`, `ascii`, `csv`, and `tsv`

Example toggles:

```text
\format unicode
\plan on
\timing on
\profile on
```

## Common local scenarios

### Scenario A: validate a parser or planner change

Run the server and CLI, then execute several representative queries and compare:

- result output
- logical plan
- physical plan
- `\explain` output

Suggested command:

```text
\explain FROM analytics.events
SELECT country, revenue
FILTER revenue > 50
ORDER BY revenue DESC
LIMIT 3
```

### Scenario B: validate storage writes and pruning

Use a very small `row_group_size` such as `2` so a small data set creates multiple row groups. Then inspect:

```text
\segments analytics.events
\stats analytics.events
```

This is a useful trick because it makes internal storage behavior visible with small data.

### Scenario C: validate protocol changes

Whenever you change a request or response shape, run:

```powershell
cargo test -p protocol -p cli -p server
```

That checks codec round trips, CLI behavior, and server compilation together.

## Troubleshooting

### The CLI cannot connect

Check that the server is running and bound to the expected address. The CLI default is `127.0.0.1:5999`.

### The server starts but queries do not see old data

Remember that data is relative to the server process working directory. If you launch the server from another directory, the effective `data/` root changes.

### You want a clean tutorial run

Delete `data/` and restart the server.

### You changed shared types and now many crates rebuild

That is expected in a multi-crate workspace when shared protocol or storage types move.

## Chapter takeaway

After this chapter you should be able to:

- build the workspace confidently
- run the server and CLI locally
- create and populate tables
- query and inspect data
- use the CLI as a debugging surface while making code changes