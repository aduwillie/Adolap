# Chapter 7: Bloom Filters

## 1. Goal of this chapter

This chapter explains Adolap's Bloom filter implementation in enough detail that you can:

1. Understand what the filter does and what guarantees it provides.
2. Read the implementation in `crates/storage/src/bloom.rs` confidently.
3. Build intuition for the bit math, hash usage, and optimal sizing tradeoffs.
4. Understand the bit packing technique used by the persisted representation.
5. Understand which columns get filters and which do not.

## 2. What problem the Bloom filter solves

The Bloom filter exists to answer one narrow but valuable question cheaply:

- Could this row group contain a particular value?

It is used as a **negative filter**, not as proof that a value exists.

That distinction matters.

- If the filter says `false`, the value is **definitely not present**.
- If the filter says `true`, the value **might** be present.

This is useful for storage pruning. If a query asks for `name = "alice"` and the Bloom filter for a row group returns `false`, the engine can skip opening and scanning that row group for that predicate.

## 3. What Adolap stores

### 3.1 The `BloomFilter` struct

```rust
// crates/storage/src/bloom.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Packed bit array. Length is always a multiple of 8.
    bits: Vec<u8>,
}
```

The filter is **variable size**: the byte vector is allocated at construction time based on the expected number of distinct values. This is a change from an earlier fixed 256-byte (2048-bit) representation — the current implementation sizes the filter optimally per column chunk.

### 3.2 Key constants

```rust
const MIN_BLOOM_BYTES: usize = 64;     // 512 bits minimum
const MAX_BLOOM_BYTES: usize = 4096;   // 32 768 bits maximum
const DEFAULT_FPP: f64 = 0.01;        // 1% false-positive probability target
const NUM_HASH_FUNCTIONS: usize = 3;   // k = 3 hash probes per value
```

### 3.3 Construction

Two constructors are available:

```rust
// Optimal sizing: uses the formula below to pick the right bit count.
pub fn with_capacity(expected_distinct: usize) -> Self

// Legacy fixed 256-byte (2 048-bit) filter.
// Used for backward compatibility and as a default when cardinality is unknown.
pub fn new() -> Self
```

The write path always uses `with_capacity`, passing the count of non-null values in the column chunk as the `expected_distinct` hint.

## 4. The optimal sizing formula

### 4.1 The math

Given:

- `n` = expected number of distinct values
- `fpp` = desired false-positive probability (default `0.01` = 1%)

The optimal number of **bits** is:

```
m = -(n * ln(fpp)) / (ln(2))²
```

The byte count is `ceil(m / 8)`, clamped to `[MIN_BLOOM_BYTES, MAX_BLOOM_BYTES]`.

The Rust implementation:

```rust
pub fn optimal_byte_count(n: usize, fpp: f64) -> usize {
    if n == 0 {
        return MIN_BLOOM_BYTES;
    }
    let fpp = fpp.clamp(1e-10, 1.0 - 1e-10);
    let m_bits = (-(n as f64) * fpp.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
    let byte_count = m_bits.div_ceil(8);
    byte_count.clamp(MIN_BLOOM_BYTES, MAX_BLOOM_BYTES)
}
```

### 4.2 Worked examples

**Example 1: n = 100, fpp = 0.01**

```
ln(0.01) = -4.6052
ln(2)² = 0.4805
m = -(100 × -4.6052) / 0.4805
m = 460.52 / 0.4805 ≈ 958.5 bits
bytes = ceil(958.5 / 8) = 120 bytes
clamped to [64, 4096] → 120 bytes
```

**Example 2: n = 1 000, fpp = 0.01**

```
m = -(1000 × -4.6052) / 0.4805 ≈ 9 585 bits
bytes = ceil(9585 / 8) = 1199 bytes
clamped to [64, 4096] → 1 199 bytes
```

**Example 3: n = 5 000, fpp = 0.01**

```
m ≈ 47 926 bits → 5 991 bytes
clamped to MAX_BLOOM_BYTES → 4 096 bytes
```

So for very large row groups the filter is capped at 4 096 bytes, which slightly increases the false-positive rate beyond 1% but keeps storage bounded.

**Example 4: n = 0 (all nulls)**

```
→ returns MIN_BLOOM_BYTES = 64 bytes
```

### 4.3 Why fix both a minimum and a maximum?

- The minimum (64 bytes = 512 bits) ensures that even a one-row column chunk produces a functional filter and that the file is not so small as to cause alignment issues.
- The maximum (4 096 bytes = 32 768 bits) keeps bloom filter overhead predictable and prevents pathological cases where a single large column chunk balloons the storage.

## 5. The core idea

A Bloom filter is a bitset plus multiple hash functions.

For each inserted value:

1. Hash the value several times using different seeds.
2. Map each hash to a bit position.
3. Set those bits to `1`.

For each lookup:

1. Hash the value the same way.
2. Recompute the same bit positions.
3. Check whether those bits are all set.

If any required bit is `0`, the value was never inserted.

If all required bits are `1`, the value may have been inserted, or it may collide with bits set by other values.

This gives the standard Bloom filter behavior:

- No false negatives.
- Possible false positives.

## 6. Bit packing technique

This implementation uses bit packing.

Bit packing means storing multiple boolean flags inside one byte instead of dedicating a whole byte to each flag.

Since one byte has 8 bits, one `u8` can store 8 yes/no flags.

Without bit packing:

- 2 048 logical bits would cost 2 048 bytes if stored as one byte per flag.

With bit packing:

- 2 048 logical bits cost only 256 bytes.

That is why the implementation converts a global bit position into:

- `byte_index = bit_index / 8`
- `bit_offset = bit_index % 8`

`byte_index` tells you which byte contains the bit.

`bit_offset` tells you which slot inside that byte to read or write.

The two key operations are:

```rust
self.bits[byte_index] |= 1 << bit_offset;  // set a bit
```

and:

```rust
(self.bits[byte_index] & (1 << bit_offset)) == 0  // test bit absent
```

## 7. Hash-to-bit mapping

For each of the three hash seeds, the code hashes the input value and maps the hash into the filter range:

```rust
pub fn add<T: Hash>(&mut self, value: &T) {
    let m = self.bit_count() as u64;
    for seed in 0..NUM_HASH_FUNCTIONS as u64 {
        let hash = bloom_hash(value, seed + 1);
        let bit_index = (hash % m) as usize;
        self.bits[bit_index / 8] |= 1 << (bit_index % 8);
    }
}

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
```

`bloom_hash` uses `AHasher` seeded differently for each of the three probes:

```rust
fn bloom_hash<T: Hash>(value: &T, seed: u64) -> u64 {
    let state = RandomState::with_seeds(seed, seed.wrapping_mul(7919), 0, 0);
    let mut hasher = state.build_hasher();
    value.hash(&mut hasher);
    hasher.finish()
}
```

`AHasher` is practical because it is fast, deterministic per seed, and gives good bit dispersion.

## 8. Bit position examples

For any filter of `m` bits:

- bit `0` → byte `0`, offset `0`
- bit `7` → byte `0`, offset `7`
- bit `8` → byte `1`, offset `0`
- bit `13` → byte `1`, offset `5`
- bit `300` → byte `37`, offset `4`

For a filter of 1 000 bytes (8 000 bits), a hash value of `12345` maps to:

```
bit_index = 12345 % 8000 = 4345
byte_index = 4345 / 8 = 543
bit_offset = 4345 % 8 = 1
mask = 1 << 1 = 0b0000_0010
```

## 9. Worked insertion example

Assume a 64-byte (512-bit) filter starts empty — every byte is `0x00`.

Insert the string `"alice"`. The implementation hashes it with seeds `1`, `2`, and `3`.

Assume those three hashes map (after `% 512`) to bit positions `13`, `300`, and `409`.

**Seed 1 → bit 13:**

```
byte_index = 13 / 8 = 1
bit_offset = 13 % 8 = 5
mask = 1 << 5 = 0b0010_0000

bits[1] before: 00000000
bits[1] after:  00100000
```

**Seed 2 → bit 300:**

```
byte_index = 300 / 8 = 37
bit_offset = 300 % 8 = 4
mask = 1 << 4 = 0b0001_0000

bits[37] before: 00000000
bits[37] after:  00010000
```

**Seed 3 → bit 409:**

```
byte_index = 409 / 8 = 51
bit_offset = 409 % 8 = 1
mask = 1 << 1 = 0b0000_0010

bits[51] before: 00000000
bits[51] after:  00000010
```

After inserting `"alice"`, the filter has evidence of that value at three bit positions. It does not store the string itself.

## 10. Worked lookup example

Now ask whether the filter might contain `"alice"`.

The code reuses the same 3 seeds and recomputes the same 3 bit positions: `13`, `300`, `409`.

It then checks those three bit locations.

- All of them are set → `might_contain` returns `true`.
- If even one is clear → returns `false` immediately.

That is why Bloom filters are excellent at cheap negative checks.

## 11. Why false positives happen

Suppose you later insert `"alicia"`, and its 3 hashes land on bits `13`, `888`, and `1600`.

The first bit overlaps with `"alice"`.

That overlap is normal. Multiple values share the same finite bitset.

If enough unrelated values happen to set all the bits needed by some query value, the filter says `true` even though that exact value was never inserted.

What cannot happen is a **false negative**: a value was inserted but one of its bits is somehow unset. Bloom filters are designed to avoid that.

## 12. Which columns get bloom filters

### 12.1 Bool columns are excluded

`Bool` columns never receive a bloom filter. In `crates/storage/src/column_writer.rs`, the `write_bloom_filter` dispatch explicitly returns `None` for booleans:

```rust
// crates/storage/src/column_writer.rs
async fn write_bloom_filter(...) -> Result<Option<String>, AdolapError> {
    if !self.storage_config.enable_bloom_filter {
        return Ok(None);
    }
    match column_type {
        ColumnType::Utf8 => bloom::write_bloom_file(...).await?,
        ColumnType::I32  => bloom::write_bloom_file(...).await?,
        ColumnType::U32  => bloom::write_bloom_file(...).await?,
        ColumnType::Bool => None,  // ← Bool columns always skip bloom filters
    }
}
```

This makes sense: a boolean column has at most two distinct values (`true` and `false`), so a bloom filter would be useless for pruning. Any row group containing at least one `true` and one `false` would pass every lookup.

### 12.2 Null values are skipped

The write path counts only **non-null** values to size the filter, and only inserts non-null values:

```rust
// crates/storage/src/bloom.rs
pub async fn write_bloom_file<T>(
    row_group_dir: &Path,
    column_index: &usize,
    values: &[T],
    validity: Option<&[u8]>,
) -> Result<Option<String>, AdolapError> {
    let non_null_count = values
        .iter()
        .enumerate()
        .filter(|(i, _)| !is_null(validity, *i))
        .count();

    let mut bloom = BloomFilter::with_capacity(non_null_count);
    for (i, value) in values.iter().enumerate() {
        if !is_null(validity, i) {
            bloom.add(value);
        }
        // nulls are silently skipped
    }
    // ... serialize and write
}
```

The `is_null` helper reads a packed validity bitmap (`crates/storage/src/null.rs`):

```rust
pub fn is_null(validity: Option<&[u8]>, index: usize) -> bool {
    match validity {
        None => false,
        Some([]) => false,
        Some(bits) => {
            let byte_index = bits.get(index / 8)?;
            let mask = 1 << (index % 8);
            (byte_index & mask) == 0
        }
    }
}
```

A `None` validity slice means every row is non-null (the common case for non-nullable columns).

### 12.3 Per-table opt-in

The `TableStorageConfig` field `enable_bloom_filter` (default `true`) gates the entire mechanism. Setting it to `false` in `table.config.json` causes `write_bloom_filter` to return `None` for all columns, and the row group metadata records `has_bloom_filter: false`.

## 13. Serialization

### 13.1 Format

`BloomFilter` derives `serde::Serialize` and `serde::Deserialize`. The bloom file is serialized with **postcard**, a compact binary codec:

```rust
let bloom_bytes = postcard::to_allocvec(&bloom)
    .map_err(|e| AdolapError::Serialization(...))?;
```

Because `bits` is a `Vec<u8>`, postcard encodes its length as a varint prefix before the raw bytes. This means the reader reconstructs the correct bit count automatically — there is no need to store `m` separately.

### 13.2 File naming

Each column's bloom filter is written to:

```
<row_group_dir>/column_<index>.bloom
```

For example, column 0 in `segment_0/row_group_0/` writes `column_0.bloom`.

### 13.3 Roundtrip example

```rust
let mut bloom = BloomFilter::with_capacity(20);
for i in 0..20i32 { bloom.add(&i); }

let bytes = postcard::to_allocvec(&bloom).unwrap();
let restored: BloomFilter = postcard::from_bytes(&bytes).unwrap();

for i in 0..20i32 {
    assert!(restored.might_contain(&i)); // no false negatives after roundtrip
}
```

## 14. False-positive rate approximation

The standard approximation for the actual false-positive rate given filter size `m` bits, `k` hash functions, and `n` inserted values:

```
p ≈ (1 - e^(-k·n/m))^k
```

With `k = 3` and the optimal `m = -(n · ln(0.01)) / ln(2)²`:

| n (non-null values) | filter size | approx fpp |
| --- | --- | --- |
| 50 | 64 bytes (clamped min) | ~1.0% |
| 100 | 120 bytes | ~1.0% |
| 500 | 598 bytes | ~1.0% |
| 1 000 | 1 199 bytes | ~1.0% |
| 3 000 | 3 583 bytes | ~1.0% |
| 5 000 | 4 096 bytes (clamped max) | ~2.5% |
| 10 000 | 4 096 bytes (clamped max) | ~11% |

Large row groups that exceed the cap accept a higher false-positive rate. That is a reasonable tradeoff: the filter still prunes many cases, and the per-column storage cost stays bounded.

## 15. Practical mental model

The simplest correct mental model is this:

- The filter is a compact fingerprint field, sized to the column chunk's cardinality.
- Each inserted non-null value leaves three bit marks behind.
- Lookup checks whether those three marks are all present.
- `false` means **impossible**.
- `true` means **possible** (might be a false positive).

That is all a Bloom filter needs to do to be useful in a storage engine.
