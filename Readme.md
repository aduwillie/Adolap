# Adolap

Adolap is a small Rust database project that combines a custom text query language, a simple storage engine, a binary TCP protocol, a server process, and an interactive CLI. The codebase is organized as a workspace so each major concern stays in its own crate while still composing into a single local database system.

This repository is best read in two ways:

1. As an executable project you can build, run, and query locally.
2. As a database engineering notebook where each chapter explains one layer of the system.

## What the project does

Adolap currently supports:

- Databases and tables under a filesystem-backed catalog.
- AQL text queries with `FROM`, `SELECT`, `FILTER`, `GROUP BY`, `GROUP FILTER`, `AGG`, `JOIN`, `ORDER BY`, `LIMIT`, `OFFSET`, and `SKIP`.
- `CREATE DATABASE`, `DROP DATABASE`, `CREATE TABLE`, `DROP TABLE`, `INSERT INTO`, `DELETE FROM`, and `INGEST INTO` statements.
- A binary request/response protocol used by the CLI and server.
- An interactive shell with multiline input, history, meta commands, planner visibility, and multiple output formats.
- Segment, row-group, bloom-filter, and stats-aware storage reads.

## Workspace layout

The workspace is split into focused crates:

| Crate | Purpose |
| --- | --- |
| `crates/core` | Shared errors, ids, logging, config, and utility helpers. |
| `crates/storage` | Schema, record batches, catalog, segment and row-group IO, stats, compression, bloom filters. |
| `crates/exec` | Parser, DSL, logical plans, optimizer, planner, predicates, and executor. |
| `crates/protocol` | Binary message model, codecs, and TCP framing helpers. |
| `crates/server` | TCP listener, request dispatch, query execution, and server-side meta commands. |
| `crates/cli` | Interactive shell that speaks the binary protocol over TCP. |

## Prerequisites

You only need a standard Rust toolchain to build and run the project locally.

- Rust stable from `rustup`
- Cargo
- A terminal with TCP loopback access

Recommended commands for checking your environment:

```powershell
rustc --version
cargo --version
```

## Setup

Clone the repository and build the whole workspace:

```powershell
git clone <your-repo-url>
cd Adolap
cargo build
```

Run the full test suite:

```powershell
cargo test
```

Adolap stores table data under a local `data/` directory relative to where the server process runs. If you want a clean local run, remove that directory before starting again:

```powershell
Remove-Item -Recurse -Force .\data -ErrorAction SilentlyContinue
```

## Local run scenarios

### Scenario 1: Start the server with the default address

The server binds to `0.0.0.0:5999` by default.

```powershell
cargo run -p server
```

Expected startup message:

```text
Adolap server listening on 0.0.0.0:5999
```

### Scenario 2: Connect with the interactive CLI

In a second terminal, start the CLI:

```powershell
cargo run -p cli
```

Expected connection message:

```text
Connected to Adolap at 127.0.0.1:5999
```

The CLI also accepts a custom address as the first argument or via `ADOLAP_ADDR`:

```powershell
cargo run -p cli -- 127.0.0.1:5999
```

```powershell
$env:ADOLAP_ADDR = "127.0.0.1:5999"
cargo run -p cli
```

### Scenario 3: Create a database and table

Inside the CLI, terminate commands with `;`.

```text
CREATE DATABASE analytics;

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

### Scenario 4: Insert rows directly

```text
INSERT INTO analytics.events ROWS
(1, "US", "mobile", 120, true),
(2, "US", "desktop", 75, true),
(3, "DE", "mobile", 0, false),
(4, "FR", "tablet", 35, false);
```

### Scenario 5: Ingest JSON or NDJSON data

Create a sample file such as `events.ndjson`:

```json
{"event_id": 10, "country": "US", "device": "mobile", "revenue": 140, "is_paying": true}
{"event_id": 11, "country": "CA", "device": "desktop", "revenue": 20, "is_paying": false}
{"event_id": 12, "country": "US", "device": "tablet", "revenue": 95, "is_paying": true}
```

Then ingest it:

```text
INGEST INTO analytics.events FROM "events.ndjson";
```

### Scenario 6: Query the data

Example point lookup and projection:

```text
FROM analytics.events
SELECT country, device, revenue
FILTER revenue > 50
ORDER BY revenue DESC
LIMIT 5;
```

Example grouped query:

```text
FROM analytics.events
SELECT country
GROUP BY country
AGG SUM(revenue)
GROUP FILTER SUM(revenue) > 100
ORDER BY SUM(revenue) DESC;
```

### Scenario 7: Inspect metadata from the CLI

The interactive shell supports server-backed meta commands:

```text
\databases
	ables analytics
\schema analytics.events
\storage analytics.events
\segments analytics.events
\stats analytics.events
\explain FROM analytics.events
SELECT country, revenue
FILTER revenue > 50
ORDER BY revenue DESC
LIMIT 5
```

Useful local shell toggles:

```text
\plan on
	iming on
\profile on
\format unicode
\memory
```

## Data and storage notes

- The server uses `data/` as its storage root.
- The preferred table layout is `data/<db>/<table>`.
- The catalog still recognizes legacy default-database tables stored as `data/<table>` when a `schema.json` file exists there.
- Segment and row-group metadata support pruning and inspection.

## Development workflow

Build everything:

```powershell
cargo build
```

Run all tests:

```powershell
cargo test
```

Run individual crates when iterating:

```powershell
cargo test -p exec
cargo test -p storage
cargo test -p protocol
cargo test -p cli
cargo test -p server
```

## Documentation guide

The `docs/` directory is organized like a short database book. Start with the introduction and continue in order.

| Chapter | Summary |
| --- | --- |
| [docs/01_introduction.md](docs/01_introduction.md) | Project goals, mental model, workspace overview, and how the crates fit together. |
| [docs/02_setup_and_local_development.md](docs/02_setup_and_local_development.md) | Detailed local setup, build, run, test, and common day-to-day development scenarios. |
| [docs/03_storage_engine.md](docs/03_storage_engine.md) | Catalog, schemas, record batches, table writing, segment layout, stats, and pruning. |
| [docs/04_query_language_and_planning.md](docs/04_query_language_and_planning.md) | AQL syntax, parsing, logical plans, optimization, binding, and physical planning. |
| [docs/05_protocol_server_and_cli.md](docs/05_protocol_server_and_cli.md) | Binary protocol, TCP framing, request lifecycle, server dispatch, and CLI behavior. |
| [docs/06_end_to_end_walkthrough.md](docs/06_end_to_end_walkthrough.md) | A tutorial-style chapter with sample data, queries, inspection commands, and architecture mapping. |

## Recommended reading order

If you want to understand the codebase deeply, use this sequence:

1. Read [docs/01_introduction.md](docs/01_introduction.md).
2. Set up the project with [docs/02_setup_and_local_development.md](docs/02_setup_and_local_development.md).
3. Study persistence in [docs/03_storage_engine.md](docs/03_storage_engine.md).
4. Study planning in [docs/04_query_language_and_planning.md](docs/04_query_language_and_planning.md).
5. Study networking and tooling in [docs/05_protocol_server_and_cli.md](docs/05_protocol_server_and_cli.md).
6. Run the tutorial in [docs/06_end_to_end_walkthrough.md](docs/06_end_to_end_walkthrough.md).

## Current status

Adolap is a serious learning project rather than a production database. The code is intentionally readable and modular, and the documentation is meant to make the internal tradeoffs visible. That makes it a good codebase for experimenting with storage formats, planning rules, new operators, protocol changes, and CLI ergonomics.
