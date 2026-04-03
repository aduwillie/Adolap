// Number of rows in a table
pub type RowCount = u64;

// Number of bytes (file sizes, memory usage, etc.)
pub type ByteCount = u64;

// Timestamp in milliseconds since the Unix epoch
pub type TimestampMs = i64;

// A byte offset within a file (used for indexing and seeking)
pub type FileOffset = u64;

// Index of a column within a table schema
pub type ColumnIndex = usize;
