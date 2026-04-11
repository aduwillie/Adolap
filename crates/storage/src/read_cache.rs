//! In-process read cache for immutable storage artifacts.
//!
//! Segments, row-group metadata, bloom filters, schemas, and table configs
//! are immutable once written. This module caches their deserialized forms
//! so repeated reads within the same process avoid redundant I/O and
//! deserialization.
//!
//! The cache is stored behind a global [`OnceLock`] and uses interior
//! mutability via [`std::sync::Mutex`] to allow concurrent readers to
//! populate entries on first access.  Because each stored artifact is
//! small (metadata structs, 256-byte bloom filters, short JSON schemas)
//! memory pressure is negligible for typical workloads.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::bloom::BloomFilter;
use crate::config::TableStorageConfig;
use crate::metadata::SegmentMetadata;
use crate::schema::TableSchema;

/// A process-global cache for immutable read artifacts.
///
/// Every entry is keyed by the canonical file path so that distinct tables,
/// segments, and columns never collide.
#[derive(Debug)]
pub struct ReadCache {
    schemas: Mutex<HashMap<PathBuf, TableSchema>>,
    configs: Mutex<HashMap<PathBuf, TableStorageConfig>>,
    segment_metadata: Mutex<HashMap<PathBuf, SegmentMetadata>>,
    bloom_filters: Mutex<HashMap<PathBuf, BloomFilter>>,
}

/// Obtain (or lazily create) the process-global [`ReadCache`].
pub fn global_cache() -> &'static ReadCache {
    static INSTANCE: OnceLock<ReadCache> = OnceLock::new();
    INSTANCE.get_or_init(ReadCache::new)
}

impl ReadCache {
    fn new() -> Self {
        Self {
            schemas: Mutex::new(HashMap::new()),
            configs: Mutex::new(HashMap::new()),
            segment_metadata: Mutex::new(HashMap::new()),
            bloom_filters: Mutex::new(HashMap::new()),
        }
    }

    // ------------------------------------------------------------------
    // Schema helpers
    // ------------------------------------------------------------------

    /// Return a cached schema for `path`, or `None` on a miss.
    pub fn get_schema(&self, path: &Path) -> Option<TableSchema> {
        self.schemas.lock().unwrap().get(path).cloned()
    }

    /// Insert a schema into the cache.
    pub fn put_schema(&self, path: PathBuf, schema: TableSchema) {
        self.schemas.lock().unwrap().insert(path, schema);
    }

    /// Remove a cached schema (e.g. after a table is dropped or recreated).
    pub fn invalidate_schema(&self, path: &Path) {
        self.schemas.lock().unwrap().remove(path);
    }

    // ------------------------------------------------------------------
    // Table storage config helpers
    // ------------------------------------------------------------------

    /// Return a cached table storage config for `path`, or `None` on a miss.
    pub fn get_config(&self, path: &Path) -> Option<TableStorageConfig> {
        self.configs.lock().unwrap().get(path).cloned()
    }

    /// Insert a table storage config into the cache.
    pub fn put_config(&self, path: PathBuf, config: TableStorageConfig) {
        self.configs.lock().unwrap().insert(path, config);
    }

    /// Remove a cached config (e.g. after a table is dropped or recreated).
    pub fn invalidate_config(&self, path: &Path) {
        self.configs.lock().unwrap().remove(path);
    }

    // ------------------------------------------------------------------
    // Segment metadata helpers
    // ------------------------------------------------------------------

    /// Return cached segment metadata for `path`, or `None` on a miss.
    pub fn get_segment_metadata(&self, path: &Path) -> Option<SegmentMetadata> {
        self.segment_metadata.lock().unwrap().get(path).cloned()
    }

    /// Insert segment metadata into the cache.
    pub fn put_segment_metadata(&self, path: PathBuf, metadata: SegmentMetadata) {
        self.segment_metadata.lock().unwrap().insert(path, metadata);
    }

    // ------------------------------------------------------------------
    // Bloom filter helpers
    // ------------------------------------------------------------------

    /// Return a cached bloom filter for `path`, or `None` on a miss.
    pub fn get_bloom_filter(&self, path: &Path) -> Option<BloomFilter> {
        self.bloom_filters.lock().unwrap().get(path).cloned()
    }

    /// Insert a bloom filter into the cache.
    pub fn put_bloom_filter(&self, path: PathBuf, filter: BloomFilter) {
        self.bloom_filters.lock().unwrap().insert(path, filter);
    }

    // ------------------------------------------------------------------
    // Bulk invalidation
    // ------------------------------------------------------------------

    /// Remove all cached entries whose path starts with `prefix`.
    ///
    /// This is used when a table is dropped or its segments are rewritten
    /// (e.g. during compaction or `replace_rows`).
    pub fn invalidate_prefix(&self, prefix: &Path) {
        self.schemas.lock().unwrap().retain(|k, _| !k.starts_with(prefix));
        self.configs.lock().unwrap().retain(|k, _| !k.starts_with(prefix));
        self.segment_metadata.lock().unwrap().retain(|k, _| !k.starts_with(prefix));
        self.bloom_filters.lock().unwrap().retain(|k, _| !k.starts_with(prefix));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionType;
    use crate::schema::{ColumnSchema, ColumnType};

    fn sample_schema() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false },
            ],
        }
    }

    #[test]
    fn schema_cache_hit_after_put() {
        let cache = ReadCache::new();
        let path = PathBuf::from("/tmp/test/schema.json");
        assert!(cache.get_schema(&path).is_none());

        cache.put_schema(path.clone(), sample_schema());
        let hit = cache.get_schema(&path);
        assert!(hit.is_some());
        assert_eq!(hit.unwrap().columns.len(), 1);
    }

    #[test]
    fn invalidate_schema_removes_entry() {
        let cache = ReadCache::new();
        let path = PathBuf::from("/tmp/test/schema.json");
        cache.put_schema(path.clone(), sample_schema());
        cache.invalidate_schema(&path);
        assert!(cache.get_schema(&path).is_none());
    }

    #[test]
    fn config_cache_hit_after_put() {
        let cache = ReadCache::new();
        let path = PathBuf::from("/tmp/test/table.config.json");
        let config = TableStorageConfig::default();
        cache.put_config(path.clone(), config.clone());
        let hit = cache.get_config(&path).unwrap();
        assert!(matches!(hit.compression, CompressionType::Lz4));
    }

    #[test]
    fn segment_metadata_cache_round_trip() {
        let cache = ReadCache::new();
        let path = PathBuf::from("/tmp/test/segment_0/segment.meta");
        let meta = SegmentMetadata::new(
            core::id::TableId { value: 1 },
            TableStorageConfig::default(),
        );
        cache.put_segment_metadata(path.clone(), meta);
        assert!(cache.get_segment_metadata(&path).is_some());
    }

    #[test]
    fn bloom_filter_cache_round_trip() {
        let cache = ReadCache::new();
        let path = PathBuf::from("/tmp/test/column_0.bloom");
        let bloom = BloomFilter::new();
        cache.put_bloom_filter(path.clone(), bloom);
        assert!(cache.get_bloom_filter(&path).is_some());
    }

    #[test]
    fn invalidate_prefix_removes_matching_entries() {
        let cache = ReadCache::new();
        let prefix = PathBuf::from("/tmp/test");
        cache.put_schema(prefix.join("schema.json"), sample_schema());
        cache.put_config(prefix.join("config.json"), TableStorageConfig::default());
        cache.put_bloom_filter(prefix.join("bloom"), BloomFilter::new());

        let other = PathBuf::from("/tmp/other/schema.json");
        cache.put_schema(other.clone(), sample_schema());

        cache.invalidate_prefix(&prefix);

        assert!(cache.get_schema(&prefix.join("schema.json")).is_none());
        assert!(cache.get_config(&prefix.join("config.json")).is_none());
        assert!(cache.get_bloom_filter(&prefix.join("bloom")).is_none());
        // entry under /tmp/other should survive
        assert!(cache.get_schema(&other).is_some());
    }
}
