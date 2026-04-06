use ahash::RandomState;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Error;
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::Path;
use serde::Serialize as SerdeSerialize;
use core::error::AdolapError;
use crate::null::is_null;

/// A simple fixed-size bloom filter: 2048 bits (256 bytes)
#[derive(Debug, Clone)]
pub struct BloomFilter {
  bits: [u8; 256], // 2048 bits = 256 * 8 bits (256 bytes)
}

impl Serialize for BloomFilter {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    serializer.serialize_bytes(&self.bits)
  }
}

impl<'de> Deserialize<'de> for BloomFilter {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: Deserializer<'de>,
  {
    let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
    let bits = bytes
      .try_into()
      .map_err(|bytes: Vec<u8>| D::Error::invalid_length(bytes.len(), &"expected 256 bytes"))?;

    Ok(BloomFilter { bits })
  }
}

impl BloomFilter {
    pub fn new() -> Self {
        Self { bits: [0; 256] }
    }

    /// Add a value to the bloom filter
    pub fn add<T: Hash>(&mut self, value: &T) {
        for seed in [1u64, 2, 3] {
            let hash = bloom_hash(value, seed);

            let bit_index = (hash % 2048) as usize;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;

            self.bits[byte_index] |= 1 << bit_offset;
        }
    }

    /// Check if a value might be present
    pub fn might_contain<T: Hash>(&self, value: &T) -> bool {
        for seed in [1u64, 2, 3] {
            let hash = bloom_hash(value, seed);

            let bit_index = (hash % 2048) as usize;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;

            if (self.bits[byte_index] & (1 << bit_offset)) == 0 {
                return false; // definitely not present
            }
        }
        true // maybe present
    }
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
    let mut bloom = BloomFilter::new();
    for (i, value) in values.iter().enumerate() {
        if !is_null(validity, i) {
          bloom.add(value);
          tracing::debug!("Added value at index {} to bloom filter for column {}", i, *column_index);
        }
        else {
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

    tracing::info!("Bloom filter written to {}", bloom_file_path.display());

    Ok(Some(bloom_file_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_with_strings() {
        let mut bloom = BloomFilter::new();
        bloom.add(&"hello");
        bloom.add(&"world");

        assert!(bloom.might_contain(&"hello"));
        assert!(bloom.might_contain(&"world"));
        assert!(!bloom.might_contain(&"foo"));
        assert!(!bloom.might_contain(&"bar"));
    }

    #[test]
    fn test_bloom_filter_with_integers() {
        let mut bloom = BloomFilter::new();
        bloom.add(&42);
        bloom.add(&100);

        assert!(bloom.might_contain(&42));
        assert!(bloom.might_contain(&100));
        assert!(!bloom.might_contain(&7));
        assert!(!bloom.might_contain(&99));
    }

    #[test]
    fn test_bloom_filter_false_positives() {
        let mut bloom = BloomFilter::new();
        for i in 0..1000 {
            bloom.add(&i);
        }

        // Check that all added values are present
        for i in 0..1000 {
            assert!(bloom.might_contain(&i));
        }

        // Check some values that were not added (may have false positives)
        let false_positives = (1000..1100).filter(|i| bloom.might_contain(i)).count();
        println!("False positives: {}", false_positives);
    }

    #[test]
    fn test_write_bloom_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let row_group_dir = temp_dir.path();
            let column_index = 0;
            let values = vec!["hello", "world", "foo", "bar"];
            let validity = None;

            let bloom_file_name = write_bloom_file(row_group_dir, &column_index, &values, validity)
                .await
                .expect("Failed to write bloom file")
                .expect("Bloom file name should be returned");

            let bloom_file_path = row_group_dir.join(&bloom_file_name);
            assert!(bloom_file_path.exists(), "Bloom file should exist");

            // Optionally, read back the bloom filter and check its contents
              let bloom_bytes = tokio::fs::read(&bloom_file_path)
                  .await
                  .expect("Failed to read bloom file");
              let bloom: BloomFilter = postcard::from_bytes(&bloom_bytes)
                  .expect("Failed to deserialize bloom filter");

              assert!(bloom.might_contain(&"hello"));
              assert!(bloom.might_contain(&"world"));
              assert!(bloom.might_contain(&"foo"));
              assert!(bloom.might_contain(&"bar"));

              // Cleanup bloom file
              tokio::fs::remove_file(&bloom_file_path)
                  .await
                  .expect("Failed to remove bloom file");
        });
    }
}
