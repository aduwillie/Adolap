use ahash::AHasher;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Error;
use std::hash::{Hash, Hasher};
use std::path::Path;
use tokio::fs;
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
            let mut hasher = AHasher::new_with_keys(seed as u128, seed as u128 * 7919);
            value.hash(&mut hasher);
            let hash = hasher.finish();

            let bit_index = (hash % 2048) as usize;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;

            self.bits[byte_index] |= 1 << bit_offset;
        }
    }

    /// Check if a value might be present
    pub fn might_contain<T: Hash>(&self, value: &T) -> bool {
        for seed in [1u64, 2, 3] {
            let mut hasher = AHasher::new_with_keys(seed as u128, seed as u128 * 7919);
            value.hash(&mut hasher);
            let hash = hasher.finish();

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

pub trait BloomHashable: Hash {}

impl BloomHashable for String {}
impl BloomHashable for &str {}
impl BloomHashable for u32 {}
impl BloomHashable for u64 {}
impl BloomHashable for i32 {}
impl BloomHashable for i64 {}

pub async fn write_bloom_file<T>(
    row_group_dir: &Path,
    column_index: usize,
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
        }
    }

    let bloom_bytes = postcard::to_allocvec(&bloom)
        .map_err(|e| AdolapError::Serialization(format!("Cannot serialize bloom filter: {}", e)))?;

    let bloom_file_name = format!("column_{}.bloom", column_index);
    let bloom_file_path = row_group_dir.join(&bloom_file_name);

    fs::write(&bloom_file_path, bloom_bytes)
        .await
        .map_err(|e| AdolapError::StorageError(format!("Cannot write bloom filter: {}", e)))?;

    Ok(Some(bloom_file_name))
}

