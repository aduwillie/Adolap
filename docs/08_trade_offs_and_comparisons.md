# Chapter 8: Trade-offs and Comparisons

## Goal of this chapter

This chapter explains the intentional design trade-offs that shape Adolap's architecture, compares Adolap to several production analytical databases, and names the risks that would need to be addressed before any real-world workload could rely on the system.

The analysis here is grounded in the actual code. File references point at the specific locations where each trade-off becomes visible.

---

## Section 1: Key design trade-offs

### 1.1 Filesystem-backed catalog instead of a metadata service

**Decision** (`crates/storage/src/catalog.rs`)

Adolap resolves table names by walking the `data/` directory tree. Schema files (`schema.json`) and storage configs (`table.config.json`) are written alongside the data they describe. There is no separate metadata service, no transaction log over catalog changes, and no distributed coordination.

**Benefit:** The entire catalog state is human-readable on disk. Any terminal can inspect it without special tooling. The catalog is never inconsistent with the data it describes because both live in the same directory.

**Cost:** Creating and dropping tables is not atomic relative to concurrent reads. If two processes try to create a table with the same name simultaneously, both may attempt to write `schema.json` with no mutual exclusion. There is no rollback if a `CREATE TABLE` succeeds but a subsequent `INSERT INTO` fails before any rows land.

### 1.2 Immutable segment creation over in-place mutation

**Decision** (`crates/storage/src/table_writer.rs:73-93`)

Every `INSERT INTO` statement writes a brand-new segment directory. Existing segments are never modified. Deletions are implemented as read-filter-rewrite: all surviving rows are rewritten into a fresh segment after the old ones are removed.

**Benefit:** Write logic is straightforward. Segment directories are either complete (all files written, metadata present) or non-existent. There is no partial-write state to recover from during a normal insert.

**Cost:** Delete performance is O(table_size). Deleting a single row from a table with one million rows requires reading every row, filtering, and writing back 999,999 rows. The `clear_data` → `insert_rows` sequence in `replace_rows` is also not atomic: a crash between the two calls leaves the table empty.

### 1.3 Row-group granularity for pruning

**Decision** (`crates/storage/src/segment_reader.rs`, `crates/storage/src/stats.rs`)

The smallest unit the engine can skip during a scan is a row group (default: 16,384 rows). Bloom filters and min/max statistics are computed and stored per row group, per column. The engine cannot skip individual rows within a row group.

**Benefit:** Row-group metadata is small and cheap to read. Stats and bloom filters for an entire table fit in memory easily. The pruning logic is simple: a single comparison against serialized min/max values, or three hash lookups for a bloom check.

**Cost:** For selective equality queries on high-cardinality columns, the engine may still read thousands of rows it will immediately discard once the actual column data is loaded. A secondary index (B-tree, inverted) would allow row-level skipping but adds significant write-path complexity.

### 1.4 Fixed-size bloom filters

**Decision** (`crates/storage/src/bloom.rs:11`)

Every bloom filter is exactly 2,048 bits (256 bytes) regardless of how many distinct values the column contains. With three hash functions, the theoretical false-positive rate for `n` inserted values is approximately:

```
fpp ≈ (1 - e^(-3n/2048))^3
```

For a row group of 16,384 rows with a column that has 1,000 distinct values, the false-positive rate is about 1%. For 10,000 distinct values it exceeds 95%, at which point the bloom filter provides almost no benefit and just wastes the I/O required to read and deserialize it.

**Cost:** Bloom filters are unhelpful for high-cardinality columns at the default row group size.

### 1.5 Result set limited to the first record batch

**Decision** (`crates/server/src/handler.rs:403`)

`convert_batches_to_resultset` currently materializes only `batches[0]` into the wire `ResultSet`. If the executor returns multiple batches (which it does when a table spans multiple row groups), the rows from all batches beyond the first are silently discarded.

This is the most impactful correctness gap in the current implementation. It is acknowledged in `docs/architecture.md:95`.

**Cost:** Queries against tables with more than one row group return incomplete results with no error or warning.

### 1.6 No WAL, no crash recovery

**Decision** (no file; an explicit absence)

Adolap writes segment files directly to disk with no write-ahead log. If the process is killed mid-write, the partially written segment directory is left on disk. `next_segment_id` (`crates/storage/src/table_writer.rs:248-280`) only recognises directories that contain a `segment.meta` file, so a partial segment is skipped on the next read. However, the rows from that partial write are silently lost.

The `replace_rows` path (`table_writer.rs:96-100`) is more dangerous: it calls `clear_data` to remove all segment directories and then calls `insert_rows` to write new ones. A crash between the two calls leaves the table empty.

**Cost:** Data loss is possible under any abnormal process termination.

### 1.7 In-memory aggregation with no spill

**Decision** (`crates/exec/src/aggregate.rs`, `crates/exec/src/group_by.rs`)

`HashAggregate` accumulates all groups in a `HashMap<String, AggState>` in process memory. There is no spill-to-disk path. A `GROUP BY` query on a column with millions of distinct values will exhaust available memory and crash the process.

### 1.8 Minimal type system

**Decision** (`crates/storage/src/schema.rs:11-16`)

Adolap supports four column types: `Utf8`, `I32`, `U32`, and `Bool`. There are no floating-point types, no date or timestamp types, no decimal types, and no nested types. This limits the schemas that can be expressed and the queries that can return meaningful results.

---

## Section 2: Comparisons to other analytical databases

### 2.1 DuckDB

DuckDB is an in-process analytical database targeting a similar single-machine use case.

| Dimension | Adolap | DuckDB |
|---|---|---|
| **Storage** | Directory tree of per-column files; custom postcard binary | Single `.duckdb` file; columnar with RLE, bitpacking, FSST string compression |
| **Execution** | Batch-oriented; no SIMD | Vectorised with 2,048-value vectors; SIMD where available |
| **Concurrency** | None | MVCC; multiple readers and a single writer |
| **SQL coverage** | AQL subset (~10 clauses, 4 types) | Full SQL: window functions, CTEs, lateral joins, 50+ types |
| **Durability** | No WAL; crash loses last write | WAL and periodic checkpointing |
| **Embedding** | TCP server only | In-process C API; Python, R, Node, Java bindings |
| **Bloom filters** | Fixed 2,048-bit per column per row group | Per-segment zone maps and min/max only; bloom is optional per column |
| **Adolap advantage** | Every layer is visible and readable in ~6,000 lines of Rust | — |
| **DuckDB advantage** | Correctness, completeness, performance, ecosystem | — |

### 2.2 ClickHouse

ClickHouse is a production columnar database optimised for high-throughput analytical inserts and queries.

| Dimension | Adolap | ClickHouse |
|---|---|---|
| **Write model** | Append-only new segment per insert | Append-only parts with continuous background merge (MergeTree) |
| **Compaction** | Background merge every 60 s; threshold-based | Continuous adaptive background merge; configurable TTL policies |
| **Compression** | LZ4 or Zstd per column | LZ4 or Zstd plus additional codec pipeline: Delta, DoubleDelta, Gorilla, T64 |
| **Encoding** | Dictionary for UTF-8 only | 15+ codecs; `LowCardinality` type; `FixedString` |
| **Indexing** | Bloom + min/max per row group | Sparse primary index (granules); inverted, bloom, set, range secondary indexes |
| **Transactions** | None | No full ACID, but part-level atomicity; lightweight deletes via marks |
| **Replication** | None | Async or synchronous replica sets via ZooKeeper or ClickHouse Keeper |
| **Adolap advantage** | The segment → row-group → column-file model directly mirrors ClickHouse's part → granule → column-file model and is easier to understand | — |
| **ClickHouse advantage** | All performance, reliability, and operational dimensions | — |

### 2.3 Apache Druid

Apache Druid is a real-time analytical database targeting low-latency queries on time-series event data.

| Dimension | Adolap | Druid |
|---|---|---|
| **Ingestion** | Batch NDJSON or row literals; no streaming | Kafka, Kinesis, batch Hadoop/Spark; real-time rollup at ingest |
| **Pre-aggregation** | None | Configurable rollup reduces storage 10–100× for metrics workloads |
| **Indexing** | Bloom + min/max | Roaring Bitmap inverted indexes per dimension; per-column statistics |
| **Scalability** | Single process | Historicals, Brokers, Coordinators, Overlords; auto-sharding by time |
| **Query language** | AQL | Druid SQL over Apache Calcite; native JSON API |
| **Adolap advantage** | Trivial to set up; no ZooKeeper or deep storage dependency | — |
| **Druid advantage** | Streaming ingest, pre-aggregation, horizontal scale, production SLA | — |

### 2.4 Apache Pinot

Pinot targets very low-latency (sub-100 ms P99) real-time OLAP at large scale.

| Dimension | Adolap | Pinot |
|---|---|---|
| **Indexing** | Bloom + min/max | Bloom, sorted, inverted, range, text, FST, JSON, geospatial, Star-Tree |
| **Star-Tree index** | None | Pre-rolled dimension trees; sub-millisecond aggregations |
| **Type system** | 4 types | 20+ types including `TIMESTAMP`, `JSON`, `MAP`, `LIST` |
| **Upserts** | None (delete is full rewrite) | Primary-key upsert tables with MVCC |
| **Adolap advantage** | Understandable codebase | — |
| **Pinot advantage** | Real-time upserts, low-latency, Kubernetes-native operations | — |

### 2.5 Snowflake and BigQuery

Both are cloud-managed analytical warehouses.

| Dimension | Adolap | Snowflake / BigQuery |
|---|---|---|
| **Storage** | Local filesystem | Managed object storage (S3 / GCS) |
| **Scale** | Single machine | Elastic; petabyte-scale |
| **ACID** | None | Full ACID with time-travel (Snowflake); snapshot isolation (BigQuery) |
| **Connectivity** | Custom binary TCP protocol | JDBC, ODBC, HTTP REST, Arrow IPC, native client libraries |
| **Cost** | Free; runs locally | Pay-per-query and pay-per-storage |
| **Adolap advantage** | Free, offline, no cloud dependency, transparent internals | — |
| **Cloud warehouse advantage** | No operational burden, global scale, full SQL, SLA | — |

### 2.6 Trino (Presto)

Trino is a distributed query engine that federates over many data sources.

| Dimension | Adolap | Trino |
|---|---|---|
| **Architecture** | Embedded single-process | Distributed coordinator + worker pool |
| **Joins** | In-memory hash join; no spill | Broadcast, partitioned hash, dynamic filtering, spill-to-disk |
| **File formats** | Custom binary only | Parquet, ORC, Avro, JSON, Delta Lake, Iceberg, Hudi |
| **Optimizer** | Single rule: filter-below-project | Cost-based with statistics; adaptive re-planning |
| **Adolap advantage** | Zero infrastructure | — |
| **Trino advantage** | Federation, pushdown to data sources, distributed sort and aggregation | — |

---

## Section 3: Missing pieces and production-readiness risks

The risks below are ordered by impact. Each entry names the relevant file, describes the gap, and suggests a concrete fix.

### Risk 1 — Multi-batch result truncation (correctness, high impact)

**File:** `crates/server/src/handler.rs:403`

`convert_batches_to_resultset` processes only `batches[0]`. Any table that spans more than one row group silently returns partial results.

**Fix:** Iterate all batches in the vector; collect column names from the first batch; collect rows from every batch.

### Risk 2 — Non-atomic `replace_rows` (data loss, high impact)

**File:** `crates/storage/src/table_writer.rs:96-100`

`replace_rows` calls `clear_data()` and then `insert_rows()`. A crash between the two calls leaves the table empty. There is no backup or recovery path.

**Fix:** Write new segments to a temporary directory under the table root; on success, rename old segment directories to a backup path and rename the temp directories into place; remove the backup. The rename operations should be as close to atomic as the OS permits.

### Risk 3 — No write-ahead log (data loss, high impact)

**File:** absent; the `segment_writer.rs` write path has no counterpart WAL.

A crash during any write leaves a partial segment directory. `next_segment_id` silently skips partial directories. The writes in that segment are permanently lost.

**Fix:** Before writing a segment, append a WAL entry recording the intent. On startup, scan the WAL for incomplete entries and either replay or roll back the incomplete segment directory.

### Risk 4 — Fixed bloom filter size at high cardinality (correctness, medium impact)

**File:** `crates/storage/src/bloom.rs:11`

At 16,384 rows per row group with 10,000 or more distinct values, the 2,048-bit bloom filter has a false-positive rate above 95%. The filter then burns I/O reading and deserializing a useless structure.

**Fix:** Size the filter using the formula `m = -(n * ln(fpp)) / (ln(2))^2` where `n` is the expected number of distinct values (available from `distinct_count` in `ColumnStats`) and `fpp` is a configurable target false-positive probability. Store the actual bit count in the bloom file header so the reader can reconstruct the filter correctly.

### Risk 5 — In-memory aggregation with no spill (availability, medium impact)

**File:** `crates/exec/src/aggregate.rs`, `crates/exec/src/group_by.rs`

A `GROUP BY` on a high-cardinality column accumulates one `AggState` entry per distinct value in process memory with no bound.

**Fix:** Track total memory used by the aggregate hash map. When usage exceeds a configurable threshold, sort and spill the current partial aggregates to a temporary file on disk; merge spilled files during finalization.

### Risk 6 — Minimal type system (functional coverage, medium impact)

**File:** `crates/storage/src/schema.rs:11-16`

The absence of `F64`, `TimestampMs`, and `Decimal` types prevents storing most real-world analytics schemas.

**Fix:** Add `F64` first (touches schema, column, stats, predicate, executor, codec, and printer). Add `TimestampMs` as a newtype over `i64` second. Each new type requires changes across the full stack but follows a clear pattern set by the existing four types.

### Risk 7 — No authentication or transport security (security, medium impact)

**File:** `crates/server/src/tcp.rs`

The server binds a plain TCP socket with no authentication, no TLS, and no access control. Any local or network client can read and modify all data.

**Fix:** Wrap `TcpStream` in `tokio-rustls` with a configurable certificate and key. Add a simple challenge-response authentication step before the first `ClientMessage` is accepted.

### Risk 8 — No connection limit (availability, low impact)

**File:** `crates/server/src/tcp.rs:20`

The server spawns one Tokio task per accepted connection with no upper bound. A client that opens thousands of connections exhausts the OS file descriptor table and available memory.

**Fix:** Track the current connection count with an `AtomicUsize`. Reject new connections when the count exceeds a configurable `max_connections` value in `ServerConfig`.

---

## Section 4: Suggested next steps

If the goal is to make Adolap more than a learning project, the following work items address the highest-priority gaps in order:

1. Fix the multi-batch result truncation (one function change, no new dependencies).
2. Make `replace_rows` atomic using temp-dir and rename.
3. Add a write-ahead log before each segment write.
4. Size bloom filters dynamically from column stats.
5. Add `F64` and `TimestampMs` to the type system.
6. Add a spill path to the hash aggregate operator.
7. Add TLS and basic authentication to the server.
8. Add connection limits to the TCP listener.

Steps 1 and 2 are correctness fixes with very small implementation footprints. Steps 3 through 8 each require more design work but follow patterns already established in the codebase.
