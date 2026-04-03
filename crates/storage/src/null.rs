//! Helpers for packed column validity bitmaps.

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

/// Count null rows in a packed validity bitmap.
pub fn count_nulls(bits: &[u8], len: usize) -> usize {
  (0..len)
    .filter(|&i| is_null(Some(bits), i))
    .count()
}
