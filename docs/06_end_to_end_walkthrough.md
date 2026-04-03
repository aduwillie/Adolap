# Chapter 6: End-to-End Walkthrough

## Goal of this chapter

This chapter is a guided lab. It ties together the previous chapters by walking through a realistic local session and mapping each step back to the architecture.

If you only want one document to read while running the code locally, read this one after the introduction.

## Scenario

We will build a tiny analytics dataset with two tables:

- `analytics.events`
- `analytics.users`

Then we will:

- ingest and insert data
- run filters and sorts
- run grouped aggregation
- run a join
- inspect schema, segments, storage config, stats, and plans

## Step 0: clean state and start processes

From the repository root:

```powershell
Remove-Item -Recurse -Force .\data -ErrorAction SilentlyContinue
cargo run -p server
```

In a second terminal:

```powershell
cargo run -p cli
```

## Step 1: create the database

```text
CREATE DATABASE analytics;
```

What this exercises:

- parser support for `CREATE DATABASE`
- server handler dispatch
- catalog-backed database creation

## Step 2: create the tables

Create `events` with a deliberately small row group size so internal structure is easier to observe:

```text
CREATE TABLE analytics.events (
  event_id U32,
  user_id U32,
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

Create `users`:

```text
CREATE TABLE analytics.users (
  id U32,
  email Utf8,
  plan Utf8,
  active Bool
);
```

Architecture notes:

- `CREATE TABLE` is parsed into a `Statement::CreateTable`.
- The server resolves it through the catalog.
- `TableWriter::create_table` persists `schema.json` and the table config.

## Step 3: insert a small dimension table

```text
INSERT INTO analytics.users ROWS
(1, "a@example.com", "pro", true),
(2, "b@example.com", "free", true),
(3, "c@example.com", "pro", false);
```

This is the cleanest way to validate the row-oriented insert path.

## Step 4: ingest an NDJSON fact table

Create `events.ndjson` with this content:

```json
{"event_id": 100, "user_id": 1, "country": "US", "device": "mobile", "revenue": 130, "is_paying": true}
{"event_id": 101, "user_id": 1, "country": "US", "device": "desktop", "revenue": 75, "is_paying": true}
{"event_id": 102, "user_id": 2, "country": "CA", "device": "desktop", "revenue": 0, "is_paying": false}
{"event_id": 103, "user_id": 2, "country": "CA", "device": "mobile", "revenue": 20, "is_paying": false}
{"event_id": 104, "user_id": 3, "country": "DE", "device": "mobile", "revenue": 90, "is_paying": true}
{"event_id": 105, "user_id": 3, "country": "FR", "device": "tablet", "revenue": 15, "is_paying": false}
```

Ingest it:

```text
INGEST INTO analytics.events FROM "events.ndjson";
```

Why this is a good example:

- it creates enough rows to demonstrate row-group chunking
- it contains different countries and devices
- it includes zero and non-zero revenue values
- it supports grouped aggregation and joins later in the walkthrough

## Step 5: run a focused query

```text
FROM analytics.events
SELECT country, device, revenue
FILTER revenue > 50
ORDER BY revenue DESC;
```

Concepts exercised:

- scan
- filter
- projection
- sort

Expected intuitive result shape:

```text
country | device  | revenue
US      | mobile  | 130
DE      | mobile  | 90
US      | desktop | 75
```

The exact table border style depends on your current `\format` setting.

## Step 6: look at the plans

Enable plan display:

```text
\plan on
```

Then rerun the query or ask for an explain-only view:

```text
\explain FROM analytics.events
SELECT country, device, revenue
FILTER revenue > 50
ORDER BY revenue DESC
LIMIT 3
```

What to look for:

- the logical tree should show the intended operators
- the physical tree should show how execution will actually happen
- the explain output also includes server memory information

## Step 7: run a grouped aggregate

```text
FROM analytics.events
SELECT country
GROUP BY country
AGG SUM(revenue)
GROUP FILTER SUM(revenue) > 50
ORDER BY SUM(revenue) DESC;
```

Why this query matters:

- it demonstrates aggregate binding
- it validates grouped filter support
- it validates grouped ordering over the aggregate output column

This is one of the richer examples in the current language.

## Step 8: run a join

```text
FROM analytics.events
JOIN analytics.users ON user_id = id
SELECT country, revenue, email, plan
FILTER revenue > 10
ORDER BY revenue DESC;
```

This validates:

- qualified table binding
- join column resolution
- join output schema derivation
- hash join execution

## Step 9: inspect storage internals

Use CLI meta commands:

```text
\schema analytics.events
\storage analytics.events
\segments analytics.events
\stats analytics.events
```

What each one tells you:

- `\schema`: logical table definition and nullability
- `\storage`: persisted table config such as row group size and bloom filter settings
- `\segments`: how many segment directories exist and their row counts and sizes
- `\stats`: segment totals and per-column min/max/null/distinct summaries

Because this walkthrough uses a small row group size, these commands are especially educational.

## Step 10: use output modes for different jobs

For human inspection:

```text
\format unicode
```

For quick copy/paste into spreadsheets or scripts:

```text
\format csv
```

For plain terminals:

```text
\format ascii
```

For lightweight profiling while iterating:

```text
\timing on
\profile on
```

## Map the walkthrough back to the code

Here is the same walkthrough mapped to the main source files:

| Action | Main code path |
| --- | --- |
| Type a query or statement in the CLI | `crates/cli/src/repl.rs` |
| Convert the request to protocol bytes | `crates/cli/src/client.rs`, `crates/protocol/src/codec.rs` |
| Read and write framed TCP payloads | `crates/protocol/src/framing.rs` |
| Accept socket connections | `crates/server/src/tcp.rs` |
| Route the request | `crates/server/src/handler.rs` |
| Parse AQL | `crates/exec/src/parser.rs` |
| Build and bind plans | `crates/exec/src/logical_plan.rs`, `crates/exec/src/planner.rs` |
| Optimize and execute | `crates/exec/src/optimizer.rs`, `crates/exec/src/executor.rs` |
| Read and write persisted data | `crates/storage/src/table_writer.rs`, `crates/storage/src/table_reader.rs`, `crates/storage/src/segment_reader.rs` |
| Inspect metadata via meta commands | `crates/server/src/meta.rs` |

## What to pay attention to as a developer

During this walkthrough, keep an eye on these design qualities:

- The crates are separate, but the execution path is still easy to trace.
- Storage concerns are mostly isolated to `storage`.
- Query semantics are mostly isolated to `exec`.
- The CLI is thin and mostly delegates to the server.
- The protocol is simple enough that message shape changes remain manageable.

## Suggested exercises after the walkthrough

If you want to keep learning from the codebase, these are good next experiments:

1. Add a new predicate combination and trace it through parser, binder, and executor.
2. Add a new meta command and route it through CLI, protocol, and server.
3. Add a new storage statistic and expose it through `\stats`.
4. Change `row_group_size` and observe how `\segments` and `\stats` change.
5. Add a new output mode in the CLI printer.

## Chapter takeaway

This walkthrough shows that Adolap is already more than a set of disconnected subsystems. It behaves like a coherent local database project. That is the most important architectural result of the repository as it exists today.