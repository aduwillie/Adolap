# Architecture Overview

## Why this document exists

The chapter docs explain individual subsystems, but Adolap also benefits from a system-level map. This document introduces that map. It focuses on three questions:

1. What are the major runtime components?
2. Why are they separated the way they are?
3. How do those decisions show up in this repository today?

The goal is not to describe an abstract ideal database. The goal is to describe the concrete architecture that exists in this repo.

## One-sentence architecture summary

Adolap is a local analytical database implemented as a Rust workspace where a CLI speaks a binary TCP protocol to a server, the server owns query parsing and execution, the execution layer reads and transforms `RecordBatch` values, and the storage layer persists tables as filesystem-backed segments and row groups.

## The top-level system model

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

## Architectural principles

Several architectural decisions are repeated throughout the codebase.

### 1. The server is authoritative

Even though Adolap is a local developer-oriented system, the CLI is intentionally thin. It does not inspect the filesystem itself, does not parse the storage layout locally, and does not try to reconstruct planner behavior on the client side.

That decision shows up in two concrete ways:

- The CLI sends text or meta commands to the server instead of executing them locally.
- Metadata commands such as `\schema`, `\segments`, `\storage`, `\stats`, and `\explain` are handled on the server in `crates/server/src/meta.rs`.

Why this is a good decision here:

- The server owns the actual `data/` root.
- Query planning depends on catalog state known to the server.
- CLI introspection stays consistent with the process executing queries.

### 2. AQL and transport are separate concerns

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

### 3. `RecordBatch` is the cross-layer data unit

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

There is one current implementation detail worth knowing: `Executor::execute` returns `Vec<RecordBatch>`, but `convert_batches_to_resultset` in `crates/server/src/handler.rs` currently converts only the first batch into the wire `ResultSet`. The architecture supports multi-batch execution; the current response conversion is narrower than that architecture.

### 4. The filesystem is the catalog and storage authority

Adolap does not have a separate metadata service. Table existence, schema files, segment metadata, and row-group metadata on disk are the source of truth.

Implementation in this repo:

- `Catalog` in `crates/storage/src/catalog.rs` resolves logical table names to filesystem locations and loaded metadata.
- `TableWriter` persists `schema.json` and `table.config.json` into table directories.
- Segment and row-group metadata are stored alongside the data they describe.

This is an intentional learning-oriented design. It keeps all core state inspectable on disk and makes the system easy to reason about locally.

### 5. The crate graph mirrors the runtime graph

The workspace split is not cosmetic. It roughly matches the runtime execution path:

`cli -> protocol -> server -> exec + storage -> protocol -> cli`

That matters because the repository is trying to teach system boundaries, not just package files differently.

## The two main request flows

Adolap has two dominant request categories.

### Flow A: query and data-manipulation path

This path covers `FROM ... SELECT ...`, `CREATE TABLE`, `INSERT INTO`, `DELETE FROM`, and `INGEST INTO`.

Detailed path:

1. The REPL in `crates/cli/src/repl.rs` collects input until it sees a terminating `;`.
2. The client in `crates/cli/src/client.rs` encodes a `ClientMessage`.
3. `crates/protocol/src/framing.rs` writes a length-prefixed frame.
4. The server in `crates/server/src/tcp.rs` reads the frame and decodes the message.
5. `crates/server/src/handler.rs` matches on the `ClientMessage`.
6. For text queries, `parse_statement` in `crates/exec/src/parser.rs` returns a `Statement`.
7. Query statements are optimized, bound with catalog metadata, converted to a physical plan, and executed.
8. Storage readers load batches from disk as needed.
9. The server packages the result and plan strings into a `ServerMessage::QueryResult` or returns `ServerMessage::Ok` for non-query statements.
10. The CLI decodes the response and renders it.

### Flow B: metadata and introspection path

This path covers commands such as `\databases`, `\tables analytics`, `\schema analytics.events`, and `\explain ...`.

Detailed path:

1. The CLI sees an input line starting with `\` while no multiline query is active.
2. It forwards the command to the server as `ClientMessage::MetaCommand`.
3. `crates/server/src/meta.rs` resolves the command.
4. The server uses the catalog, segment metadata, stats decoding, and planner helpers to produce a human-readable response.
5. The CLI renders that response without needing direct filesystem knowledge.

This separation is one of the most important architectural choices in the repo. Introspection is implemented as a first-class server feature, not as a debugging shortcut in the client.

## Storage architecture in the large

The storage layer is organized as:

`database -> table -> segment -> row group -> column files`

In the canonical layout, tables live under:

```text
data/<database>/<table>
```

The catalog also preserves compatibility with a legacy layout for default-database tables stored directly under:

```text
data/<table>
```

That fallback is not just documented intent. `Catalog::list_tables` and table resolution explicitly support it when `schema.json` is present.

Adolap’s storage is designed around immutable segment creation. New inserts create a new segment directory rather than appending into an existing segment in place. Deletes are implemented as read-filter-rewrite using `replace_rows`, which clears segment directories and writes survivors back as fresh segments.

## Query engine architecture in the large

The query engine follows a classic staged database pipeline.

### Parse

`crates/exec/src/parser.rs` turns text into a `Statement` or query plan shape.

### Optimize

`crates/exec/src/optimizer.rs` currently applies a small recursive rule set, most notably filter/project reordering where possible.

### Bind

`crates/exec/src/planner.rs` resolves table references through the catalog, validates column names, canonicalizes grouped expressions, and derives output schemas.

### Plan physically

The same planner module turns bound logical plans into physical plans while tracking:

- required columns for projection pushdown
- scan predicates for filter pushdown
- join-side requirements

### Execute

`crates/exec/src/executor.rs` runs the physical plan and returns batches.

This staged design is important because it keeps responsibilities explicit. The parser does not need schema knowledge, the binder does not need wire protocol knowledge, and the executor does not need to infer user syntax.

## Server and CLI architecture in the large

The CLI is deliberately developer-friendly. It supports:

- multiline statements
- history persisted to `.adolap_history`
- `Ctrl+C` behavior that clears the active buffer before exiting an idle session
- local toggles for plan, timing, profile, and output format

The server is deliberately thin in another sense: it orchestrates, but it does not duplicate execution logic that belongs in `exec` or persistence logic that belongs in `storage`.

That division makes the main server dispatch in `crates/server/src/handler.rs` a good architecture entry point. It is where transport-level requests become execution- and storage-level operations.

## Why this architecture is a good fit for this repository

Adolap is not trying to be a production-distributed warehouse. The current architecture fits the project because it optimizes for:

- readability over maximal performance
- inspectability over abstraction hiding
- clear crate boundaries over monolithic convenience
- explicit metadata over magic behavior
- local iteration over deployment complexity

Those choices make the repo useful as both a working toy database and a serious systems-learning project.

## Known limitations that shape the architecture today

Accurate documentation should also name the current edges of the design.

- The catalog is process-local and filesystem-backed; there is no transactional metadata layer.
- Deletes are implemented as full-table rewrite, not as tombstones or in-place mutation.
- Result transport is presently string-oriented at the wire level.
- The server response builder currently materializes the first result batch into `ResultSet` output.
- The optimizer is intentionally small and rule-based rather than cost-based.

These are not contradictions of the architecture. They are the current stage of the architecture.

## Where to go next

Read the remaining docs in this order if you want to understand the codebase deeply:

1. `docs/01_introduction.md` for project framing.
2. This architecture overview for system boundaries.
3. `docs/02_setup_and_local_development.md` for how to run and inspect the system.
4. `docs/03_storage_engine.md` for the physical data model and storage implementation.
5. `docs/04_query_language_and_planning.md` for the logical and physical query pipeline.
6. `docs/05_protocol_server_and_cli.md` for operational behavior.
7. `docs/06_end_to_end_walkthrough.md` for a concrete end-to-end exercise.