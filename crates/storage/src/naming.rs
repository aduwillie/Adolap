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

#[cfg(test)]
mod tests {
    use super::{
        column_chunk_file_name, row_group_dir_name, segment_dir_name, ROW_GROUP_METADATA_FILE_NAME,
        SEGMENT_META_JSON_FILE_NAME, SEGMENT_METADATA_FILE_NAME, TABLE_CONFIG_FILE_NAME,
    };

    #[test]
    fn formats_storage_names_consistently() {
        assert_eq!(segment_dir_name(0), "segment_0");
        assert_eq!(segment_dir_name(42), "segment_42");
        assert_eq!(row_group_dir_name(0), "row_group_000");
        assert_eq!(row_group_dir_name(12), "row_group_012");
        assert_eq!(column_chunk_file_name(7), "col_7.data");
        assert_eq!(SEGMENT_METADATA_FILE_NAME, "segment.meta");
        assert_eq!(SEGMENT_META_JSON_FILE_NAME, "segment.meta.json");
        assert_eq!(ROW_GROUP_METADATA_FILE_NAME, "row_group.meta");
        assert_eq!(TABLE_CONFIG_FILE_NAME, "table.config.json");
    }
}
