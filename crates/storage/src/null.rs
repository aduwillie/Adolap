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

#[cfg(test)]
mod tests {
    use super::{count_nulls, is_null};

    #[test]
    fn interprets_validity_bitmaps_and_empty_inputs() {
        let validity = [0b0000_1101];

        assert!(!is_null(None, 0));
        assert!(!is_null(Some(&[]), 3));
        assert!(!is_null(Some(&validity), 0));
        assert!(is_null(Some(&validity), 1));
        assert!(!is_null(Some(&validity), 2));
        assert!(!is_null(Some(&validity), 3));
        assert!(!is_null(Some(&validity), 8));
        assert_eq!(count_nulls(&validity, 4), 1);
    }
}
