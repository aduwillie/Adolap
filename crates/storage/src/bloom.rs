use ahash::RandomState;
use serde::{Deserialize, Serialize};
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::Path;
use serde::Serialize as SerdeSerialize;
use core::error::AdolapError;
use crate::null::is_null;

/// Minimum bloom filter size: 64 bytes (512 bits).
const MIN_BLOOM_BYTES: usize = 64;

/// Maximum bloom filter size: 4096 bytes (32_768 bits).
/// Larger sizes are not useful for single row-group column chunks.
const MAX_BLOOM_BYTES: usize = 4096;

/// Default target false-positive probability used by `new()`.
const DEFAULT_FPP: f64 = 0.01;

/// Number of hash functions. Three is a well-known sweet spot for bloom filters.
const NUM_HASH_FUNCTIONS: usize = 3;

/// A variable-size bloom filter.
///
/// The bit array is sized at construction time using the formula
/// `m = -(n * ln(fpp)) / (ln(2))^2` where `n` is the expected number of
/// distinct values and `fpp` is the target false-positive probability.
///
/// The actual byte count is stored in the serialised form so that readers
/// can reconstruct the filter correctly regardless of what size was chosen at
/// write time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Packed bit array.  Length is always a multiple of 8.
    bits: Vec<u8>,
}

impl BloomFilter {
    /// Create a filter sized for approximately `expected_distinct` values at
    /// a 1% false-positive rate.
    pub fn with_capacity(expected_distinct: usize) -> Self {
        let byte_count = optimal_byte_count(expected_distinct, DEFAULT_FPP);
        Self { bits: vec![0u8; byte_count] }
    }

    /// Create a filter with a legacy fixed 2 048-bit (256-byte) array.
    /// Kept for backward compatibility and as the default for callers that do
    /// not know the expected cardinality.
    pub fn new() -> Self {
        Self { bits: vec![0u8; 256] }
    }

    /// Number of bits in the filter.
    pub fn bit_count(&self) -> usize {
        self.bits.len() * 8
    }

    /// Add a value to the bloom filter.
    pub fn add<T: Hash>(&mut self, value: &T) {
        let m = self.bit_count() as u64;
        for seed in 0..NUM_HASH_FUNCTIONS as u64 {
            let hash = bloom_hash(value, seed + 1);
            let bit_index = (hash % m) as usize;
            self.bits[bit_index / 8] |= 1 << (bit_index % 8);
        }
    }

    /// Return `false` only when the value is definitely absent.
    /// A `true` result means the value *might* be present.
    pub fn might_contain<T: Hash>(&self, value: &T) -> bool {
        let m = self.bit_count() as u64;
        for seed in 0..NUM_HASH_FUNCTIONS as u64 {
            let hash = bloom_hash(value, seed + 1);
            let bit_index = (hash % m) as usize;
            if (self.bits[bit_index / 8] & (1 << (bit_index % 8))) == 0 {
                return false;
            }
        }
        true
    }
}

/// Compute the optimal byte count for a bloom filter that should hold
/// `n` items with a target false-positive probability of `fpp`.
///
/// Formula: `m = -(n * ln(fpp)) / (ln(2))^2`
/// Result is clamped to [`MIN_BLOOM_BYTES`, `MAX_BLOOM_BYTES`] and rounded
/// up to the nearest byte.
pub fn optimal_byte_count(n: usize, fpp: f64) -> usize {
    if n == 0 {
        return MIN_BLOOM_BYTES;
    }
    let fpp = fpp.clamp(1e-10, 1.0 - 1e-10);
    let m_bits = (-(n as f64) * fpp.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
    let byte_count = m_bits.div_ceil(8);
    byte_count.clamp(MIN_BLOOM_BYTES, MAX_BLOOM_BYTES)
}

fn bloom_hash<T: Hash>(value: &T, seed: u64) -> u64 {
    let state = RandomState::with_seeds(seed, seed.wrapping_mul(7919), 0, 0);
    let mut hasher = state.build_hasher();
    value.hash(&mut hasher);
    hasher.finish()
}

pub trait BloomHashable: Hash {}

impl BloomHashable for String {}
impl BloomHashable for &str {}
impl BloomHashable for u32 {}
impl BloomHashable for u64 {}
impl BloomHashable for i32 {}
impl BloomHashable for i64 {}

pub async fn write_bloom_file<T>(
    row_group_dir: &Path,
    column_index: &usize,
    values: &[T],
    validity: Option<&[u8]>,
) -> Result<Option<String>, AdolapError>
where
    T: BloomHashable + SerdeSerialize,
{
    // Count non-null values to size the filter appropriately.
    let non_null_count = values
        .iter()
        .enumerate()
        .filter(|(i, _)| !is_null(validity, *i))
        .count();

    let mut bloom = BloomFilter::with_capacity(non_null_count);
    for (i, value) in values.iter().enumerate() {
        if !is_null(validity, i) {
            bloom.add(value);
            tracing::debug!("Added value at index {} to bloom filter for column {}", i, *column_index);
        } else {
            tracing::debug!("Skipping null value at index {} for bloom filter", i);
        }
    }

    let bloom_bytes = postcard::to_allocvec(&bloom)
        .map_err(|e| AdolapError::Serialization(format!("Cannot serialize bloom filter: {}", e)))?;

    let bloom_file_name = format!("column_{}.bloom", column_index);
    let bloom_file_path = row_group_dir.join(&bloom_file_name);

    tokio::fs::write(&bloom_file_path, &bloom_bytes)
        .await
        .map_err(|e| AdolapError::StorageError(format!("Cannot write bloom filter: {}", e)))?;

    tracing::info!(
        path = %bloom_file_path.display(),
        bit_count = bloom.bit_count(),
        non_null_values = non_null_count,
        "bloom filter written"
    );

    Ok(Some(bloom_file_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_legacy_256_byte_filter() {
        let bloom = BloomFilter::new();
        assert_eq!(bloom.bit_count(), 2048);
    }

    #[test]
    fn with_capacity_zero_uses_minimum_size() {
        let bloom = BloomFilter::with_capacity(0);
        assert_eq!(bloom.bit_count(), MIN_BLOOM_BYTES * 8);
    }

    #[test]
    fn with_capacity_scales_up_for_large_cardinality() {
        // 10 000 distinct values at 1% fpp should produce a filter
        // meaningfully larger than the 256-byte legacy default.
        let bloom = BloomFilter::with_capacity(10_000);
        assert!(bloom.bit_count() > 2048, "expected larger filter for high cardinality");
        assert!(bloom.bit_count() <= MAX_BLOOM_BYTES * 8);
    }

    #[test]
    fn optimal_byte_count_respects_bounds() {
        // 0 items → minimum
        assert_eq!(optimal_byte_count(0, 0.01), MIN_BLOOM_BYTES);
        // 1 000 000 items → capped at maximum
        assert_eq!(optimal_byte_count(1_000_000, 0.01), MAX_BLOOM_BYTES);
    }

    #[test]
    fn filter_with_strings_no_false_negatives() {
        let mut bloom = BloomFilter::with_capacity(4);
        bloom.add(&"hello");
        bloom.add(&"world");

        assert!(bloom.might_contain(&"hello"));
        assert!(bloom.might_contain(&"world"));
        assert!(!bloom.might_contain(&"foo"));
        assert!(!bloom.might_contain(&"bar"));
    }

    #[test]
    fn filter_with_integers_no_false_negatives() {
        let mut bloom = BloomFilter::with_capacity(2);
        bloom.add(&42i32);
        bloom.add(&100i32);

        assert!(bloom.might_contain(&42i32));
        assert!(bloom.might_contain(&100i32));
        assert!(!bloom.might_contain(&7i32));
        assert!(!bloom.might_contain(&99i32));
    }

    #[test]
    fn filter_contains_all_inserted_values_at_scale() {
        let n = 1_000usize;
        let mut bloom = BloomFilter::with_capacity(n);
        for i in 0..n {
            bloom.add(&(i as i32));
        }
        for i in 0..n {
            assert!(bloom.might_contain(&(i as i32)), "false negative at {i}");
        }
        // Spot-check false-positive rate stays sane — not a strict guarantee
        let fp = (n..n + 100).filter(|i| bloom.might_contain(&(*i as i32))).count();
        assert!(fp < 50, "false-positive rate too high: {fp}/100");
    }

    #[test]
    fn bloom_filter_roundtrips_through_postcard() {
        let mut bloom = BloomFilter::with_capacity(20);
        for i in 0..20i32 {
            bloom.add(&i);
        }
        let bytes = postcard::to_allocvec(&bloom).unwrap();
        let restored: BloomFilter = postcard::from_bytes(&bytes).unwrap();

        for i in 0..20i32 {
            assert!(restored.might_contain(&i), "false negative after roundtrip at {i}");
        }
    }

    #[test]
    fn write_bloom_file_creates_readable_filter() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let values = vec!["hello", "world", "foo", "bar"];

            let file_name = write_bloom_file(temp_dir.path(), &0usize, &values, None)
                .await
                .expect("write failed")
                .expect("expected a file name");

            let bytes = tokio::fs::read(temp_dir.path().join(&file_name)).await.unwrap();
            let bloom: BloomFilter = postcard::from_bytes(&bytes).unwrap();

            for v in &values {
                assert!(bloom.might_contain(v), "false negative for '{v}'");
            }
        });
    }

    #[test]
    fn write_bloom_file_skips_null_values() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::tempdir().unwrap();
            // validity byte: bits 0 and 2 are 0 (null), bits 1 and 3 are 1 (non-null)
            // bit 0 = value[0] null, bit 1 = value[1] non-null, etc.
            let validity: [u8; 1] = [0b00001010]; // indices 1, 3 non-null
            let values = vec![1i32, 2, 3, 4];

            let file_name = write_bloom_file(temp_dir.path(), &0usize, &values, Some(&validity))
                .await
                .unwrap()
                .unwrap();

            let bytes = tokio::fs::read(temp_dir.path().join(&file_name)).await.unwrap();
            let bloom: BloomFilter = postcard::from_bytes(&bytes).unwrap();

            // Non-null values must be present.
            assert!(bloom.might_contain(&2i32));
            assert!(bloom.might_contain(&4i32));
            // Null values (1 and 3) may or may not be present (false positives are ok)
            // but the point is the filter was built without them, not with them.
        });
    }
}
