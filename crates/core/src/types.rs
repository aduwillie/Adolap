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

#[cfg(test)]
mod tests {
	use super::{ByteCount, ColumnIndex, FileOffset, RowCount, TimestampMs};

	#[test]
	fn aliases_hold_expected_primitive_values() {
		let rows: RowCount = 42;
		let bytes: ByteCount = 1024;
		let timestamp: TimestampMs = -5;
		let offset: FileOffset = 128;
		let column_index: ColumnIndex = 3;

		assert_eq!(rows, 42);
		assert_eq!(bytes, 1024);
		assert_eq!(timestamp, -5);
		assert_eq!(offset, 128);
		assert_eq!(column_index, 3);
	}
}
