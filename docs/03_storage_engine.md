# Chapter 3: Storage Engine

## Goal of this chapter

This chapter explains the storage engine in enough detail that you can read the code with confidence, debug on-disk behavior, and reason about why the current design looks the way it does.

It covers three things for each major area:

1. The storage theory Adolap is borrowing from.
2. The design decisions the project makes.
3. The concrete implementation in this repository.

Adolap's storage layer is intentionally compact but not trivial. It includes a real catalog, immutable segments, row-group chunking, per-column metadata, compression, dictionary encoding for strings, bloom filters, null bitmaps, metadata-driven pruning, and background compaction.

For a focused walkthrough of the Bloom filter implementation, see [Chapter 7: Bloom Filters](07_bloom_filters.md).

---

## 1. Storage hierarchy

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

### 1.1 Why this hierarchy exists

- A database groups tables under a namespace.
- A segment is an immutable write unit.
- A row group is the smallest pruning and scan unit.
- A column chunk keeps values of one column physically together for better analytical reads.

### 1.2 Key source files

| Responsibility | File |
|---|---|
| Name resolution and table lifecycle | `crates/storage/src/catalog.rs` |
| Write orchestration | `crates/storage/src/table_writer.rs` |
| Segment writing | `crates/storage/src/segment_writer.rs` |
| Row group writing | `crates/storage/src/row_group_writer.rs` |
| Column chunk writing | `crates/storage/src/column_writer.rs` |
| Compaction | `crates/storage/src/compaction.rs` |
| Background compaction scheduling | `crates/storage/src/background_compaction.rs` |
| Naming helpers | `crates/storage/src/naming.rs` |
| Null bitmap helpers | `crates/storage/src/null.rs` |
| Column statistics | `crates/storage/src/stats.rs` |
| Storage configuration | `crates/storage/src/config.rs` |
| Segment metadata I/O | `crates/storage/src/metadata_io.rs` |

---

## 2. Table storage configuration

`TableStorageConfig` in `crates/storage/src/config.rs` controls every physical storage decision for a table. It is persisted at table creation time as `table.config.json` alongside `schema.json`.

### 2.1 All fields, types, and defaults

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
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

impl Default for TableStorageConfig {
    fn default() -> Self {
        Self {
            row_group_size: 16_384,
            compression: CompressionType::Lz4,
            enable_bloom_filter: true,
            enable_dictionary_encoding: true,
            compaction_segment_threshold: 8,
            compaction_row_group_threshold: 8,
            enable_background_compaction: true,
            background_compaction_interval_seconds: 60,
        }
    }
}
```

Field descriptions:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `row_group_size` | `usize` | `16_384` | Maximum number of rows per row group. This is a **row count**, not a byte limit. |
| `compression` | `CompressionType` | `Lz4` | Compression applied to each column chunk data file. Options: `None`, `Lz4`, `Zstd`. |
| `enable_bloom_filter` | `bool` | `true` | Whether to write per-column bloom filter sidecar files for supported types. |
| `enable_dictionary_encoding` | `bool` | `true` | Whether to dictionary-encode UTF-8 columns. |
| `compaction_segment_threshold` | `usize` | `8` | Compact when there are at least this many segments in the table. |
| `compaction_row_group_threshold` | `usize` | `8` | Compact when any segment has more than this many row groups. |
| `enable_background_compaction` | `bool` | `true` | Whether the background compaction scheduler should process this table. |
| `background_compaction_interval_seconds` | `u64` | `60` | Minimum seconds between background compaction passes for this table. |

The `#[serde(default)]` attribute means that older `table.config.json` files written before the compaction fields were added will automatically get the default values on deserialization. This is a backwards-compatibility guarantee.

---

## 3. Naming helpers (`naming.rs`)

`crates/storage/src/naming.rs` centralizes all file and directory name generation so the rest of the codebase never builds paths by hand.

```rust
/// segment_0, segment_1, ...
pub fn segment_dir_name(segment_id: u64) -> String {
    format!("segment_{}", segment_id)
}

/// row_group_000, row_group_001, ...
pub fn row_group_dir_name(index: u64) -> String {
    format!("row_group_{:03}", index)
}

/// col_0.data, col_1.data, ...
pub fn column_chunk_file_name(column_id: u64) -> String {
    format!("col_{}.data", column_id)
}

pub const SEGMENT_METADATA_FILE_NAME:  &str = "segment.meta";
pub const SEGMENT_META_JSON_FILE_NAME: &str = "segment.meta.json";
pub const ROW_GROUP_METADATA_FILE_NAME: &str = "row_group.meta";
pub const TABLE_CONFIG_FILE_NAME:       &str = "table.config.json";
```

Key design note: `row_group_dir_name` zero-pads to three digits so directory listings sort numerically. `segment_dir_name` does not pad because segment IDs are assigned monotonically and sorting lexicographically by prefix already works.

---

## 4. Metadata persistence (`metadata_io.rs`)

`crates/storage/src/metadata_io.rs` provides async helpers for reading and writing the two key metadata types:

- `SegmentMetadata` — written to `segment.meta` (binary, `postcard`) and `segment.meta.json` (human-readable JSON mirror).
- `RowGroupMetadata` — written to `row_group.meta`.

The JSON copy of segment metadata is a deliberate teaching and debugging aid. It has no role in the read path; the engine always deserializes from the binary `postcard` file.

### 4.1 `SegmentMetadata` struct

```rust
pub struct SegmentMetadata {
    pub table_id: TableId,
    pub total_rows: RowCount,
    pub total_size_bytes: ByteCount,
    pub row_groups: Vec<RowGroupMetadata>,
    pub storage_config: TableStorageConfig,
    pub created_at: TimestampMs,
}
```

The inclusion of `storage_config` inside segment metadata lets any reader know the exact configuration that was active when the segment was written, regardless of what the current `table.config.json` says.

### 4.2 `RowGroupMetadata` struct

`RowGroupMetadata` stores:

- `row_count`
- a per-column `ColumnChunkDescriptor` for every column in the schema
- `total_uncompressed_size_bytes`
- `total_compressed_size_bytes`

`ColumnChunkDescriptor` records whether dictionary encoding and bloom filters were used, the names of all sidecar files, compressed and uncompressed sizes, and the `ColumnStats` for that column.

---

## 5. Write path: rows → immutable segments

### 5.1 `TableWriter`

`TableWriter` is the main write abstraction. It supports:

- database and table creation
- opening an existing table
- inserting rows
- ingesting JSON and NDJSON
- clearing table data
- replacing all rows atomically

The insert path in `TableWriter::insert_rows` works as follows:

1. Validate input rows against the schema.
2. Split rows into chunks of at most `row_group_size` rows.
3. For each chunk, transpose rows into per-column buffers using `MutableColumn`.
4. Allocate the next segment ID by scanning existing `segment_*` directories.
5. Write a new immutable segment directory through `SegmentWriter`.

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

### 5.2 `MutableColumn`: row-to-column transposition

`MutableColumn` is an internal enum used during `build_row_groups`. It accumulates values and a packed validity bitmap for one column as rows are pushed in one at a time, then converts to a final `ColumnInputOwned` for the writer.

```rust
enum MutableColumn {
    Utf8 { values: Vec<String>, validity: Vec<u8>, has_nulls: bool },
    I32  { values: Vec<i32>,    validity: Vec<u8>, has_nulls: bool },
    U32  { values: Vec<u32>,    validity: Vec<u8>, has_nulls: bool },
    Bool { values: Vec<bool>,   validity: Vec<u8>, has_nulls: bool },
}
```

Construction initializes `validity` to `vec![0xFF; row_count.div_ceil(8)]` — all bits set to 1 (not null). When a `None` value is pushed, `mark_null` clears the bit at the row's position and sets `has_nulls = true`.

`MutableColumn::finish` converts to `ColumnInputOwned`. If `has_nulls` is false, the validity bitmap is dropped (`validity: has_nulls.then_some(validity)`), saving space.

This is the critical transposition point: input arrives row-oriented, `MutableColumn` accumulates column-oriented buffers, and the writer receives clean `ColumnInputOwned` values.

### 5.3 `replace_rows`: atomic full-table rewrite

`replace_rows` is used by delete operations and by callers that need to overwrite a table's entire content safely. It uses a staging-and-backup pattern to avoid leaving the table in a broken state if the process crashes mid-write.

The constants that name the staging directories:

```rust
const REPLACE_STAGING_PREFIX: &str = ".replace_staging_";
const REPLACE_BACKUP_PREFIX:  &str = ".replace_backup_";
```

The sequence of steps:

1. Write new segments into a `.replace_staging_<timestamp>` directory inside the table root.
2. Rename each existing `segment_*` directory to `.replace_backup_<timestamp>_<id>`.
3. Move the staging segments into the live table directory.
4. Delete the backup directories.

If the process crashes between steps 2 and 3, the backup directories remain and can be recovered. On the next call, stale staging and backup directories from previous attempts are cleaned up first.

Tests verify that no staging or backup directories survive a successful `replace_rows` call.

---

## 6. Segment design

### 6.1 `SegmentWriter`

`SegmentWriter::write_segment`:

1. Creates the segment directory.
2. Writes each row group under `row_group_000`, `row_group_001`, etc., via `RowGroupWriter`.
3. Accumulates `total_rows` and `total_size_bytes`.
4. Serializes `SegmentMetadata` with `postcard` to `segment.meta`.
5. Writes a pretty-printed JSON mirror to `segment.meta.json`.

---

## 7. Null bitmap representation (`null.rs`)

Adolap uses packed validity bitmaps to track nullability. Each byte holds 8 row positions. A bit value of `1` means the row is **not null**; `0` means null.

```rust
/// Return whether a row is null for a validity bitmap.
pub fn is_null(validity: Option<&[u8]>, index: usize) -> bool {
    match validity {
        None => false,
        Some([]) => false,
        Some(bits) => {
            let Some(byte_index) = bits.get(index / 8) else {
                return false;
            };
            let mask = 1 << (index % 8);
            (byte_index & mask) == 0
        }
    }
}
```

Important correctness rule: both `None` and `Some([])` (empty slice) are treated as "no nulls present." This handles the case where callers pass missing or empty validity metadata during stats computation and materialization.

`count_nulls(bits, len)` counts null positions by iterating over `0..len` and calling `is_null`.

Validity bitmaps are omitted entirely when `has_nulls` is false — `clear_redundant_validity` (in the column writer path) removes bitmaps that are all `1` bits.

---

## 8. Column statistics (`stats.rs`)

### 8.1 `ColumnStats` struct

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
    pub null_count: u32,
    pub distinct_count: u32,
}
```

`min` and `max` are stored as raw bytes serialized with `postcard`. This is type-agnostic — the reader must know the column type to deserialize them correctly.

### 8.2 `compute_*` functions

Type-specific helpers compute stats over a value slice and an optional validity bitmap:

- `compute_utf8_column_stats(values: &[String], validity: Option<&[u8]>)`
- `compute_i32_column_stats(values: &[i32], validity: Option<&[u8]>)`
- `compute_u32_column_stats(values: &[u32], validity: Option<&[u8]>)`
- `compute_bool_column_stats(values: &[bool], validity: Option<&[u8]>)`

Each function:
1. Skips null positions (checked via `is_null`).
2. Tracks `min` and `max` over non-null values.
3. Counts distinct non-null values using a `HashSet`.
4. Counts nulls via `count_nulls`.
5. Returns `ColumnStats::empty()` if all values are null or the slice is empty.

Min and max are serialized with `postcard::to_allocvec` before being stored in the struct.

---

## 9. Compaction

Adolap writes one new immutable segment per `insert_rows` call. Without compaction, a table with many small inserts accumulates many tiny segments, which degrades read performance. The compaction subsystem merges segments and consolidates row groups.

### 9.1 `SegmentCompactor::needs_compaction`

A table needs compaction when either of the following is true:

- The number of `segment_*` directories is ≥ `compaction_segment_threshold` (default 8).
- Any segment has more than `compaction_row_group_threshold` (default 8) row groups.

```rust
pub async fn needs_compaction(&self) -> Result<bool, AdolapError> {
    let segments = discover_segments(&self.table.table_dir).await?;
    if segments.len() >= self.table.storage_config.compaction_segment_threshold.max(1) {
        return Ok(true);
    }
    for (_, metadata) in segments {
        if metadata.row_groups.len() > self.table.storage_config.compaction_row_group_threshold.max(1) {
            return Ok(true);
        }
        ...
    }
    Ok(false)
}
```

### 9.2 `SegmentCompactor::optimize`: the compaction algorithm

When compaction is triggered, `optimize`:

1. Reads all rows from all segments.
2. Calculates an optimal number of output segments based on total row count and `row_group_size`.
3. Writes rows into new segment directories under `.compacting_<timestamp>` prefixes.
4. Atomically replaces the old segment directories with the new ones.
5. Deletes old segment directories.

The result is fewer, larger segments each with a smaller number of row groups.

### 9.3 `RowGroupCompactor`: intra-segment compaction

`RowGroupCompactor::compact_segment` compacts the row groups within a single segment without merging across segments. It re-reads the segment's rows and rewrites them with the optimal row-group size. It is skipped if the current row-group count is already optimal.

### 9.4 `BackgroundCompactionScheduler`: autonomous compaction

`BackgroundCompactionScheduler` in `crates/storage/src/background_compaction.rs` runs as a Tokio background task.

```rust
pub struct BackgroundCompactionScheduler {
    data_root: PathBuf,
    last_attempts: Arc<Mutex<HashMap<PathBuf, Instant>>>,
}
```

`spawn()` starts an infinite loop that:

1. Calls `run_once()` every `SCHEDULER_POLL_INTERVAL_SECONDS` (5 seconds).
2. `run_once` enumerates all tables via the `Catalog`.
3. Skips tables where `enable_background_compaction` is false.
4. Skips tables whose last compaction attempt was within `background_compaction_interval_seconds` (default 60s).
5. Calls `SegmentCompactor::maybe_compact` for eligible tables.

The `last_attempts` map is keyed by table path and tracks when each table was last considered for compaction. This prevents hammering a table with compaction on every poll tick.

---

## 10. Statistics and pruning

### 10.1 Theory

Metadata-driven pruning lets the engine skip row groups and segments entirely when a query predicate cannot match any value in that chunk.

### 10.2 Decision

Adolap computes per-column stats for every row group and uses them, plus bloom filters, during segment reads.

Current statistics: `min`, `max`, `null_count`, `distinct_count`.

Current pruning predicates: equality, greater-than, less-than, and conjunctions via `And`.

### 10.3 Implementation

During reads, `SegmentReader`:

1. Loads `segment.meta`.
2. Resolves projected column indices.
3. Checks row groups against the predicate using bloom filters and min/max stats.
4. Reads only surviving row groups via `RowGroupReader`.

Pruning methods: `prune_equals`, `prune_gt`, `prune_lt`.

---

## 11. Example physical layout

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
          col_0.data
          col_1.data
          col_1.bloom
          col_2.data
          col_2.bloom
          col_3.data
        row_group_001/
          ...
```

With `row_group_size = 2`, six inserted rows produce three row groups inside a single segment. After enough inserts push the segment count past `compaction_segment_threshold`, the background compactor merges them back into one.

---

## 12. Read path summary

| Layer | File | Responsibility |
|---|---|---|
| `TableReader` | `table_reader.rs` | Discovers `segment_*` directories, delegates to `SegmentReader` |
| `SegmentReader` | `segment_reader.rs` | Predicate pruning, projection index resolution, row-group iteration |
| `RowGroupReader` | `row_group_reader.rs` | Loads `row_group.meta`, selects projected columns |
| `ColumnChunkReader` | `column_writer.rs` | Reads, decompresses, decodes column data, returns `ColumnInputOwned` |

---

## 13. Chapter takeaway

Adolap's storage engine is a deliberate teaching surface. Logical tables are resolved through a real catalog, rows are converted into immutable segmented columnar storage, metadata is written in a way that supports inspection and pruning, a background compaction scheduler keeps the segment count under control, and the read path turns physical structures back into `RecordBatch` values for execution. Every layer is visible on disk.
