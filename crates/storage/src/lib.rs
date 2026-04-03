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

pub mod bloom;
pub mod compression;

pub mod stats;
pub mod null;

pub mod schema;
pub mod record_batch;
pub mod table_writer;
