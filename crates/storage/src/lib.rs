//! Storage engine primitives for Adolap.
//!
//! The storage crate owns on-disk naming, schema management, segment and row
//! group IO, statistics, and filesystem catalog discovery.

pub mod config;
pub mod catalog;
pub mod segment;
pub mod row_group;
pub mod column;
pub mod metadata;
pub mod naming;

pub mod metadata_io;

pub mod column_writer;
pub mod column_reader;
pub mod row_group_writer;
pub mod row_group_reader;
pub mod segment_writer;
pub mod segment_reader;
pub mod table_reader;
pub mod compaction;
pub mod background_compaction;

pub mod bloom;
pub mod compression;

pub mod stats;
pub mod null;

pub mod schema;
pub mod record_batch;
pub mod table_writer;

#[cfg(test)]
mod tests {
	use crate::{config::TableStorageConfig, naming::segment_dir_name};

	#[test]
	fn storage_crate_exports_core_modules() {
		let config = TableStorageConfig::default();
		assert!(config.enable_bloom_filter);
		assert_eq!(segment_dir_name(3), "segment_3");
	}
}
