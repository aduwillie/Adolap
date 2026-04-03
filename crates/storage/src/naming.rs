/// Utility functions for getting name of a segment directory
/// Eg segment_0, segment_1, etc
pub fn segment_dir_name(segment_id: u64) -> String {
    format!("segment_{}", segment_id)
}

/// Utility functions for getting name of a row group directory
/// Eg row_group_000, row_group_001, etc
pub fn row_group_dir_name(index: u64) -> String {
    format!("row_group_{:03}", index)
}

/// Utility functions for getting name of a column chunk file
/// Eg col_0.data, col_1.data, etc
pub fn column_chunk_file_name(column_id: u64) -> String {
    format!("col_{}.data", column_id)
}

/// Name of the metadata file for a segment
pub const SEGMENT_METADATA_FILE_NAME: &str = "segment.meta";

/// Name of the JSON metadata file for a segment, used for debugging and testing purposes. 
/// This is not used in production.
pub const SEGMENT_META_JSON_FILE_NAME: &str = "segment.meta.json";

/// Name of the metadata file for a row group
pub const ROW_GROUP_METADATA_FILE_NAME: &str = "row_group.meta";

/// Name of the per-table storage configuration file.
pub const TABLE_CONFIG_FILE_NAME: &str = "table.config.json";
