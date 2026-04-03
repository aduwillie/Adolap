# Chapter 3: Storage Engine

## Goal of this chapter

This chapter explains the storage engine in enough detail that you can read the code with confidence, debug on-disk behavior, and reason about why the current design looks the way it does.

It covers three things for each major area:

1. The storage theory Adolap is borrowing from.
2. The design decisions the project makes.
3. The concrete implementation in this repository.

Adolap's storage layer is intentionally compact, but it is not trivial. It already includes a real catalog, immutable segments, row-group chunking, per-column metadata, compression, dictionary encoding for strings, bloom filters, null bitmaps, and metadata-driven pruning.

## The storage problem Adolap is solving

Any database storage layer needs to answer the same basic questions:

- Where does a logical table live physically?
- How is schema persisted?
- How are rows written and later found again?
- What metadata is cheap to read before touching the full data?
- How much of the data can be skipped for a selective query?

Adolap answers those questions with a file-backed, segment-oriented, row-grouped columnar design.

That design is a good fit for this repo because it keeps the data model inspectable on disk while still demonstrating real analytical database techniques.

## Core storage model

At the highest level, storage is organized like this:

```text
database
  -> table
    -> segment
      -> row group
        -> column chunk files + metadata
```

The canonical table path is:

```text
data/<database>/<table>
```

There is also a legacy compatibility path:

```text
data/<table>
```

That fallback is not just mentioned in docs. `Catalog` actively resolves both layouts, treating the legacy form as the implicit `default` database when `schema.json` exists at the table root.

### Why this hierarchy exists

Theory:

- A database groups tables under a namespace.
- A segment gives the system an immutable write unit.
- A row group gives the system a practical pruning and scan unit.
- A column chunk keeps values of one column physically together for better analytical reads.

Decision in Adolap:

- Keep the hierarchy explicit on disk instead of hiding it behind a binary super-file.
- Prefer immutable new-segment writes over in-place mutation.
- Make row groups the smallest practical read/prune unit.

Implementation in this repo:

- `crates/storage/src/catalog.rs` resolves database and table directories.
- `crates/storage/src/table_writer.rs` allocates new segments.
- `crates/storage/src/segment_writer.rs` writes a full segment.
- `crates/storage/src/row_group_writer.rs` writes each row group.
- `crates/storage/src/column_writer.rs` writes per-column chunk files and metadata.

## Catalog theory, decisions, and implementation

### Theory

A catalog is the component that maps logical names such as `analytics.events` to physical metadata. In larger systems the catalog may be transactional and service-backed. In Adolap it is filesystem-backed.

### Decision

Adolap keeps the catalog simple and local:

- the filesystem is the metadata authority
- schema and storage config live alongside table data
- server and planner code should go through the catalog instead of constructing table paths manually

This is the right choice for this codebase because it keeps table resolution observable and avoids smuggling path logic into higher layers.

### Implementation in this repo

`Catalog` in `crates/storage/src/catalog.rs` is the storage entry point for name resolution and table lifecycle.

Key responsibilities:

- list databases
- list tables
- resolve `database.table`
- support implicit `default.table`
- create and drop databases and tables
- open a `TableWriter` for an existing table
- preserve compatibility with the legacy `data/<table>` layout

Important public methods:

```rust
pub async fn resolve_table(&self, table_ref: &str) -> Result<TableMetadata, AdolapError>
pub async fn create_table(&self, table_ref: &str, schema: &TableSchema, storage_config: &TableStorageConfig) -> Result<TableMetadata, AdolapError>
pub async fn open_table_writer(&self, table_ref: &str) -> Result<TableWriter, AdolapError>
pub async fn drop_table(&self, table_ref: &str) -> Result<TableMetadata, AdolapError>
pub async fn drop_database(&self, database: &str) -> Result<usize, AdolapError>
```

The repo note about catalog usage matters here: planner and server code should resolve tables through `Catalog`, not by hand-building `data/<db>/<table>` paths.

## Table metadata persistence

Each table directory persists at least two pieces of durable metadata:

- `schema.json`
- `table.config.json`

### Theory

Schemas and storage settings need to survive process restarts and must be discoverable before any segment is opened.

### Decision

Adolap persists schema and table storage configuration at table creation time and keeps them as human-readable JSON files.

That is a strong choice for a learning project because it keeps the physical contract legible without extra tooling.

### Implementation in this repo

`TableWriter::create_table` does the following:

1. creates the database directory if necessary
2. creates the table directory
3. writes `schema.json`
4. writes `table.config.json`
5. returns a ready-to-use `TableWriter`

The current storage configuration fields are:

- `row_group_size`
- `compression`
- `enable_bloom_filter`
- `enable_dictionary_encoding`

The default configuration is:

```rust
impl Default for TableStorageConfig {
    fn default() -> Self {
        Self {
            row_group_size: 16_384,
            compression: CompressionType::Lz4,
            enable_bloom_filter: true,
            enable_dictionary_encoding: true,
        }
    }
}
```

One accuracy detail matters here: `row_group_size` is a row count, not a byte size. The code chunks incoming rows into groups of at most that many rows.

## Write path: from rows to immutable segments

`TableWriter` is the main write abstraction.

It supports:

- database creation
- table creation
- opening an existing table
- inserting rows
- ingesting JSON and NDJSON
- clearing table data
- replacing all rows

### Theory

Analytical storage systems often separate ingest input format from physical layout. Adolap follows that pattern.

- Input arrives row-oriented.
- Storage is written column-oriented.
- New writes are appended as new immutable segment directories.

### Decision

Adolap chooses immutable segment creation rather than mutating existing segments. That keeps write logic simpler and makes metadata generation straightforward.

### Implementation in this repo

The insert path in `TableWriter::insert_rows` is structurally:

1. validate input rows against the schema
2. split rows into row groups using `row_group_size.max(1)`
3. transpose each row group into per-column buffers
4. allocate the next segment id by scanning existing `segment_*` directories
5. write a new segment directory through `SegmentWriter`

That is the core conversion from row-oriented user input into Adolap's physical layout.

Representative code path:

```rust
pub async fn insert_rows(&self, rows: &[Vec<Option<ColumnValue>>]) -> Result<usize, AdolapError> {
    self.schema.validate_rows(rows)?;
    let row_groups = self.build_row_groups(rows)?;
    let segment_id = self.next_segment_id().await?;
    let segment_dir = self.table_dir.join(segment_dir_name(segment_id));
    let segment_writer = SegmentWriter::new(self.table_id, &self.storage_config, &self.schema);
    segment_writer.write_segment(&segment_dir, borrowed_row_groups).await?;
    Ok(rows.len())
}
```

### Delete behavior is rewrite-based

This is an important repo-specific behavior that should be documented explicitly.

Adolap does not currently delete rows in place. In `crates/server/src/handler.rs`, `delete_rows`:

1. reads the full table through `TableReader`
2. computes a delete mask
3. materializes surviving rows
4. calls `TableWriter::replace_rows`

`replace_rows` clears existing segment directories and writes survivors back as fresh segments.

That means deletes are logically correct but operationally full-table rewrites.

## Segment design

### Theory

A segment is a durable write unit containing multiple row groups and segment-level metadata. In larger systems, segments are often the unit of compaction, movement, and retention. Adolap uses them mainly as immutable append units.

### Decision

Adolap stores each segment in its own directory and writes two forms of metadata:

- a binary `segment.meta` file used by the engine
- a JSON `segment.meta.json` file used for debugging and inspection

The JSON copy is a deliberate teaching and testing aid.

### Implementation in this repo

`SegmentWriter::write_segment`:

1. creates the segment directory
2. writes each row group under `row_group_000`, `row_group_001`, and so on
3. accumulates total rows and compressed size
4. serializes `SegmentMetadata` with `postcard`
5. writes a pretty JSON mirror of the same metadata

Naming helpers live in `crates/storage/src/naming.rs`:

- `segment_<id>` for segment directories
- `row_group_<nnn>` for row-group directories
- `segment.meta` for segment metadata
- `segment.meta.json` for debug/test readability

`SegmentMetadata` stores:

- `table_id`
- `total_rows`
- `total_size_bytes`
- `row_groups`
- `storage_config`
- `created_at`

The inclusion of `storage_config` inside segment metadata is useful because it lets a segment carry the configuration context that was active when it was written.

## Row-group design

### Theory

A row group is a chunk of rows stored together, with a separate chunk per column. It is a useful compromise:

- smaller than a full table scan
- larger than single-row storage
- large enough to amortize metadata cost
- small enough to prune selectively

### Decision

Adolap makes row groups the smallest pruning unit and writes row-group metadata in a dedicated file.

### Implementation in this repo

`RowGroupWriter::write_row_group`:

1. validates that the number of provided columns matches the schema
2. creates the row-group directory
3. writes each column chunk through `ColumnChunkWriter`
4. computes total compressed and uncompressed sizes
5. writes `row_group.meta`

`RowGroupMetadata` stores:

- `row_count`
- column descriptors for every column
- `total_uncompressed_size_bytes`
- `total_compressed_size_bytes`

That metadata becomes the base input for later pruning and storage inspection.

## Column chunk design

Column chunks are where most of the physical storage decisions become visible.

### Theory

Columnar storage keeps values from the same column together so analytical scans can:

- read only needed columns
- compress similar values well
- compute statistics naturally
- prune more intelligently

### Decision

Adolap stores one data file per column chunk plus optional sidecar files for dictionary encoding, bloom filters, and validity bitmaps.

This is deliberately explicit. Rather than inventing a single opaque file format, the repo favors decomposed files that expose how the storage features work.

### Implementation in this repo

`ColumnChunkWriter` writes:

- `column_<index>.data` for the compressed payload
- `column_<index>.dict` for UTF-8 dictionary data when enabled
- `column_<index>.bloom` through the bloom helper when enabled and supported
- `column_<index>.valid` when a validity bitmap is needed

Each `ColumnChunkDescriptor` records:

- the compression type
- whether dictionary encoding was used
- whether a bloom filter exists
- data, dictionary, bloom, and validity file names
- compressed and uncompressed sizes
- column statistics

### Compression

Theory:

- Column-oriented buffers are strong compression candidates.

Decision:

- Adolap compresses the final serialized column buffer using the table's configured compression type.

Implementation:

- `finalize_column_write` calls `compress_buffer` before writing `column_<index>.data`.
- `ColumnChunkReader` later calls `decompress_buffer` before decoding values.

### Dictionary encoding

Theory:

- Repeated strings often compress well with dictionary encoding because values are replaced with integer ids.

Decision:

- Adolap applies dictionary encoding only to UTF-8 columns and only when `enable_dictionary_encoding` is true.

Implementation:

- `write_utf8` chooses between plain string serialization and `encode_utf8_dictionary`.
- Null string entries are represented as `u32::MAX` in the encoded index stream.
- The actual dictionary is stored in `column_<index>.dict`.

### Bloom filters

Theory:

- Bloom filters are useful for fast negative membership checks, especially for equality predicates.

Decision:

- Adolap writes bloom filters only when enabled in table config.
- The generic writer path supports UTF-8, `i32`, and `u32` values.
- The dedicated bool writer path does not emit bloom filters.

Implementation:

- `ColumnChunkWriter::write_bloom_filter` creates sidecar files for supported types.
- `SegmentReader::prune_equals` uses the bloom filter before consulting min/max stats.

### Validity bitmaps

Theory:

- Nullable columns need a compact way to say whether each row position is null.

Decision:

- Adolap uses packed validity bitmaps and omits them entirely when a column has no nulls.

Implementation:

- Row-building paths allocate a bitmap initialized to all `1` bits.
- Null positions clear a bit.
- `clear_redundant_validity` removes the bitmap when every bit is still `1`.
- `null::is_null` treats `None` and `Some([])` as no-null cases.

That last rule is an important correctness fix in this repo because callers may pass missing or empty validity information during stats and materialization.

## Record batches as the in-memory boundary

`RecordBatch` is the bridge between storage and execution.

It contains:

- a `TableSchema`
- typed column buffers
- optional per-column validity bitmaps
- a row count

Why this matters theoretically:

- it keeps storage column-oriented
- it avoids row-at-a-time execution
- it keeps the in-memory API simple enough to understand

Implementation in this repo:

- `RowGroupReader` materializes each row group into a `RecordBatch`
- `RecordBatch::to_rows` is used when row-oriented reconstruction is needed, such as delete-rewrite
- `RecordBatch::from_rows` can rebuild a batch from validated row values

This repo-specific detail also matters: the validity bitmaps must survive round-trips correctly. Dropping null metadata breaks delete-rewrite and any path that materializes rows back to storage.

## Statistics and pruning

### Theory

Metadata-driven pruning is one of the defining ideas of analytical storage. If a query predicate can prove a chunk cannot match, the engine should skip that chunk before reading the data pages.

### Decision

Adolap computes per-column stats for every row group and uses them, plus bloom filters, during segment reads.

Current statistics include:

- `min`
- `max`
- `null_count`
- `distinct_count`

The current pruning predicates are intentionally limited but useful:

- equality
- greater-than
- less-than
- conjunctions via `And`

### Implementation in this repo

Stats are computed in `crates/storage/src/stats.rs` when column chunks are written.

Type-specific helpers exist for:

- UTF-8
- `i32`
- `u32`
- `bool`

Min and max values are serialized with `postcard`, and null counts rely on the packed validity bitmap helpers.

During reads, `SegmentReader`:

1. loads `segment.meta`
2. resolves projected column indices
3. checks whether any row group might match the predicate
4. evaluates bloom filters and min/max tests row group by row group
5. reads only the surviving row groups via `RowGroupReader`

The actual pruning methods are:

- `prune_equals`
- `prune_gt`
- `prune_lt`

This is not a full statistics engine, but it is a real metadata-driven scan avoidance mechanism.

## Read path: table -> segment -> row group -> column chunk

The read side is organized as a layered pipeline.

### TableReader

`TableReader` discovers `segment_*` directories, sorts them, and delegates each one to `SegmentReader`.

Its job is orchestration across segments, not detailed pruning.

### SegmentReader

`SegmentReader` owns:

- predicate pruning
- projection index resolution
- row-group iteration within a segment

It makes pruning decisions before touching the actual column chunk payloads.

### RowGroupReader

`RowGroupReader` loads `row_group.meta`, selects projected columns, and uses `ColumnChunkReader` to materialize each selected column.

### ColumnChunkReader

`ColumnChunkReader`:

1. reads and decompresses `column_<index>.data`
2. loads the validity bitmap if present
3. decodes plain or dictionary-encoded UTF-8 values
4. decodes typed numeric or boolean vectors
5. returns a `ColumnInputOwned`

The read pipeline is deliberately layered because it keeps the responsibilities visible and makes it easier to debug where a storage bug lives.

## Example physical layout

Suppose you create:

```text
CREATE TABLE analytics.events (
  event_id U32,
  country Utf8,
  revenue I32,
  is_paying Bool
) USING CONFIG (
  row_group_size = 2,
  compression = "lz4",
  bloom_filter = true,
  dictionary_encoding = true
);
```

Then insert six rows.

One plausible layout is:

```text
data/
  analytics/
    events/
      schema.json
      table.config.json
      segment_0/
        segment.meta
        segment.meta.json
        row_group_000/
          row_group.meta
          column_0.data
          column_1.data
          column_1.dict
          column_1.bloom
          column_2.data
          column_2.bloom
          column_3.data
        row_group_001/
          ...
        row_group_002/
          ...
```

With `row_group_size = 2`, six rows become three row groups. That is why `\segments analytics.events` and `\stats analytics.events` are especially informative when testing with small datasets.

## Why these choices make sense for Adolap

The storage layer is a good match for the repository because it optimizes for:

- inspectability
- correctness visibility
- straightforward layering
- realistic analytical techniques without excessive machinery

It demonstrates real storage-engine ideas while still being small enough to understand end to end.

## Future directions suggested by the current design

The current storage shape leaves clear room for growth:

- compaction or segment merge operations
- richer projection pushdown
- more predicate forms in pruning
- more advanced stats
- transactional metadata changes
- better multi-segment maintenance policies

The important point is that the existing structure already has the right seams for those experiments.

## Chapter takeaway

Adolap's storage engine is not just a persistence detail. It is a deliberate teaching surface. Logical tables are resolved through a real catalog, rows are converted into immutable segmented columnar storage, metadata is written in a way that supports inspection and pruning, and the read path turns those physical structures back into `RecordBatch` values for execution.