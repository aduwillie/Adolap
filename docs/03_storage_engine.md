# Chapter 3: Storage Engine

## Goal of this chapter

This chapter explains how Adolap stores and reads table data. The storage design is intentionally simple enough to understand in one sitting, but rich enough to demonstrate several real database ideas:

- catalog-backed table discovery
- schema persistence
- row-grouped columnar storage
- segment metadata
- per-column statistics
- bloom-filter-assisted pruning
- null bitmap handling

## Storage hierarchy

The main physical hierarchy is:

```text
database
  -> table
    -> segment
      -> row group
        -> column files and metadata
```

In the preferred layout, a table lives under:

```text
data/<database>/<table>
```

The catalog still supports a legacy default-database fallback layout:

```text
data/<table>
```

That compatibility behavior matters because the codebase already preserves older storage layouts when a `schema.json` file exists in the legacy location.

## The catalog is the storage entry point

The catalog resolves logical table names to on-disk metadata. The important type is `Catalog` in `crates/storage/src/catalog.rs`.

Key responsibilities:

- list databases
- list tables
- resolve `database.table`
- support implicit `default.table`
- create and drop databases and tables
- open a `TableWriter` for an existing table

The public surface is intentionally direct:

```rust
pub async fn resolve_table(&self, table_ref: &str) -> Result<TableMetadata, AdolapError>
pub async fn create_table(&self, table_ref: &str, schema: &TableSchema, storage_config: &TableStorageConfig) -> Result<TableMetadata, AdolapError>
pub async fn open_table_writer(&self, table_ref: &str) -> Result<TableWriter, AdolapError>
```

That makes the catalog the correct boundary for server and planner code. They do not need to know where table directories live on disk.

## Table schema and config persistence

Every table stores a schema and may store a table configuration file.

Important files:

- `schema.json`
- table config file referenced by `TABLE_CONFIG_FILE_NAME`

The storage config currently tracks:

- `row_group_size`
- `compression`
- `enable_bloom_filter`
- `enable_dictionary_encoding`

The default config in `crates/storage/src/config.rs` is:

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

This is one of the clearest examples of the project favoring explicit configuration over hidden behavior.

## Writing data with `TableWriter`

`TableWriter` is the main write path.

It supports:

- creating databases
- creating tables
- opening an existing table
- inserting row-oriented data
- replacing all rows
- clearing segment data
- ingesting JSON and NDJSON

The write path is conceptually:

1. validate rows against the schema
2. split rows into row groups according to `row_group_size`
3. build column-oriented structures for each row group
4. allocate a new segment id
5. write a segment directory and metadata

The key insertion path is compact and readable:

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

That is a good summary of how the storage layer thinks: row-oriented input becomes grouped columnar storage under a new immutable segment directory.

## Record batches as the bridge between storage and execution

`RecordBatch` is one of the central types in the whole codebase. It sits between storage and execution.

It contains:

- a `TableSchema`
- typed column vectors
- a row count

The project uses record batches for:

- reading table data from storage
- applying predicates and projections
- converting back to row-oriented structures when needed
- converting results into protocol rows

The essential shape is simple:

```rust
pub struct RecordBatch {
  pub schema: TableSchema,
  pub columns: Vec<ColumnInputOwned>,
  pub row_count: usize,
}
```

### Why record batches matter

This gives the project a column-oriented execution model without requiring a very sophisticated vectorized engine. That is a strong middle ground for a learning database.

## Null handling and validity bitmaps

Adolap stores per-column validity information using packed bitmaps.

The null helper in `crates/storage/src/null.rs` now deliberately treats:

- `None`
- empty bitmaps

as “no nulls”. That detail matters because statistics and row materialization can safely call into null helpers without risking an out-of-bounds panic on an empty slice.

The key rule is:

```rust
match validity {
    None => false,
    Some([]) => false,
    Some(bits) => { ... }
}
```

This is one of those small pieces of storage correctness that looks minor but prevents real bugs.

## Statistics and pruning

The storage layer computes per-column stats such as:

- min
- max
- null count
- distinct count

Those stats are then used to prune row groups before reading full data. This is handled in `SegmentReader`.

The pruning predicates currently include:

- equality
- greater-than
- less-than
- conjunctions via `And`

The read path looks like this conceptually:

1. load segment metadata
2. decide which row groups might match the predicate
3. use bloom filters and min/max stats where possible
4. read only the remaining row groups

The core method expresses that clearly:

```rust
pub async fn read_segment(
    &self,
    segment_dir: &Path,
    predicate: Option<&Predicate>,
) -> Result<Vec<RecordBatch>, AdolapError>
```

### Bloom filters

For equality predicates, `SegmentReader` checks whether the row group’s bloom filter can prove a value is absent. If it can, the row group is pruned without reading the actual column data.

This is not a full cost-based engine, but it demonstrates a real storage optimization pattern in a compact codebase.

## Table reads

`TableReader` orchestrates reads across segments:

```rust
pub async fn read_table(
    &self,
    predicate: Option<&Predicate>,
    projected_columns: Option<Vec<String>>,
) -> Result<Vec<RecordBatch>, AdolapError>
```

It:

1. discovers segment directories
2. creates a `SegmentReader`
3. reads each segment
4. appends batches into one vector

This design means storage can stay segmented while execution still sees a regular sequence of batches.

## Example storage scenario

Suppose you create this table:

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

With a row group size of `2`, a single segment would likely contain three row groups. That makes `\segments analytics.events` and `\stats analytics.events` especially informative during local development.

## Why this storage layer is good for learning

The implementation demonstrates real storage topics without burying them under framework complexity:

- name resolution through a catalog
- persisted schemas and config
- immutable segment generation
- row-group chunking
- null bitmap correctness
- stats-driven pruning
- file-based ingestion

That makes it a practical foundation for future experiments such as:

- better compression handling
- more selective projection pushdown
- richer statistics
- compaction or merge operations
- transaction experiments

## Chapter takeaway

After this chapter, the most important thing to remember is that Adolap stores data in a way that deliberately exposes core database concepts. The storage code is not an opaque persistence layer. It is part of the teaching surface of the repository.