# Chapter 7: Bloom Filters

## Goal of this chapter

This chapter explains Adolap's Bloom filter implementation in enough detail that you can:

1. Understand what the filter does and what guarantees it provides.
2. Read the implementation in `crates/storage/src/bloom.rs` confidently.
3. Build intuition for the bit math, hash usage, and sizing tradeoffs.
4. Understand the bit packing technique used by the persisted representation.

## What problem the Bloom filter solves

The Bloom filter exists to answer one narrow but valuable question cheaply:

- Could this row group contain a particular value?

It is used as a negative filter, not as proof that a value exists.

That distinction matters.

- If the filter says `false`, the value is definitely not present.
- If the filter says `true`, the value might be present.

This is useful for storage pruning. If a query asks for `name = "alice"` and the Bloom filter for a row group returns `false`, the engine can skip opening and scanning that row group for that predicate.

## The core idea

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

## What Adolap stores

The current Bloom filter implementation in `crates/storage/src/bloom.rs` uses:

- `m = 2048` bits of total filter space
- `k = 3` hash functions, implemented as the same hasher with 3 different seeds
- a packed byte array of `256` bytes because `256 * 8 = 2048`

The filter is stored as:

```rust
pub struct BloomFilter {
    bits: [u8; 256],
}
```

That means the logical filter has 2048 individual bit positions, but the physical representation compresses them into 256 bytes.

## Bit packing technique

This implementation uses bit packing.

Bit packing means storing multiple boolean flags inside one byte instead of dedicating a whole byte to each flag.

Since one byte has 8 bits, one `u8` can store 8 yes/no flags.

Without bit packing:

- 2048 logical bits would cost 2048 bytes if stored as one byte per flag.

With bit packing:

- 2048 logical bits cost only 256 bytes.

That is why the implementation converts a global bit position into:

- `byte_index = bit_index / 8`
- `bit_offset = bit_index % 8`

`byte_index` tells you which byte contains the bit.

`bit_offset` tells you which slot inside that byte to read or write.

The two key operations are:

```rust
self.bits[byte_index] |= 1 << bit_offset;
```

which sets a bit, and:

```rust
(self.bits[byte_index] & (1 << bit_offset)) == 0
```

which tests whether the bit is absent.

## The hash-to-bit mapping

For each seed, the code hashes the input value and maps the hash into the filter range with:

```rust
let bit_index = (hash % 2048) as usize;
```

So no matter how large the hash output is, it becomes a valid index in the filter.

Once `bit_index` is known, the code derives:

```rust
let byte_index = bit_index / 8;
let bit_offset = bit_index % 8;
```

This is just the usual conversion from a global bit number to a byte slot and an intra-byte position.

Examples:

- bit `0` -> byte `0`, offset `0`
- bit `7` -> byte `0`, offset `7`
- bit `8` -> byte `1`, offset `0`
- bit `13` -> byte `1`, offset `5`
- bit `300` -> byte `37`, offset `4`

## Worked insertion example

Assume the filter starts empty.

That means every byte is:

```text
00000000
```

Now insert the string `"alice"`.

The implementation hashes it three times using seeds `1`, `2`, and `3`.

Assume those three hashes map to these bit positions:

- seed `1` -> bit `13`
- seed `2` -> bit `300`
- seed `3` -> bit `1025`

These are illustrative numbers, but the logic is exactly what the real code does.

### First hash result: bit 13

Compute location:

- `byte_index = 13 / 8 = 1`
- `bit_offset = 13 % 8 = 5`

Construct the mask:

- `1 << 5 = 0b0010_0000`

Set the bit:

```text
bits[1] before: 00000000
mask:           00100000
result:         00100000
```

### Second hash result: bit 300

Compute location:

- `byte_index = 300 / 8 = 37`
- `bit_offset = 300 % 8 = 4`

Construct the mask:

- `1 << 4 = 0b0001_0000`

Set the bit:

```text
bits[37] before: 00000000
mask:             00010000
result:           00010000
```

### Third hash result: bit 1025

Compute location:

- `byte_index = 1025 / 8 = 128`
- `bit_offset = 1025 % 8 = 1`

Construct the mask:

- `1 << 1 = 0b0000_0010`

Set the bit:

```text
bits[128] before: 00000000
mask:              00000010
result:            00000010
```

After inserting `"alice"`, the filter has evidence of that value at three bit positions. It does not store the string itself.

## Worked lookup example

Now ask whether the filter might contain `"alice"`.

The code reuses the same 3 seeds and recomputes the same 3 bit positions:

- `13`
- `300`
- `1025`

It then checks those three bit locations.

If all of them are still set, lookup returns `true`.

If even one of them is clear, lookup returns `false` immediately.

That is why Bloom filters are excellent at cheap negative checks.

## Why false positives happen

Suppose you later insert `"alicia"`, and its 3 hashes land on:

- bit `13`
- bit `888`
- bit `1600`

The first bit overlaps with `"alice"`.

That overlap is normal. Multiple values share the same finite bitset.

If enough unrelated values happen to set all the bits needed by some query value, the filter says `true` even though that exact value was never inserted.

That is a false positive.

What cannot happen is this:

- a value was inserted
- one of its bits somehow remains unset
- lookup returns `false`

That would be a false negative, and Bloom filters are designed to avoid that.

## Why 2048 bits?

`2048` is a design choice, not a fundamental constant.

Bloom filter sizing usually starts from:

- `n`: expected number of inserted values
- `p`: desired false-positive rate

The standard sizing formulas are:

```text
m = -(n * ln(p)) / (ln(2)^2)
k = (m / n) * ln(2)
```

Where:

- `m` is the number of bits
- `k` is the number of hash functions

Adolap fixes:

- `m = 2048`
- `k = 3`

That is a pragmatic storage-engine choice:

- small enough to store cheaply per column chunk
- fixed-size and easy to serialize
- good enough as a coarse pruning filter for moderate row-group cardinalities

For the current implementation, the approximate false-positive rate is:

```text
p ≈ (1 - e^(-kn/m))^k
```

with `m = 2048` and `k = 3`.

Some rough examples:

- `n = 100` -> about `0.25%`
- `n = 200` -> about `1.6%`
- `n = 300` -> about `4.5%`
- `n = 400` -> about `8.7%`

So `2048` bits is reasonable when the expected number of distinct inserted values per filter is in the low hundreds and the Bloom filter is only being used as a pruning hint.

## Why Adolap uses a concrete hasher

The implementation uses `AHasher` as the concrete hash algorithm and varies the seed to simulate multiple hash functions.

That choice is practical because:

- it is fast
- it is easy to seed
- it gives a simple way to derive multiple hash projections of the same input

The important distinction is:

- `Hash` is the trait for values that can be fed into a hasher
- `Hasher` is the trait describing a hash state machine
- `AHasher` is the concrete implementation actually performing the hash computation

You cannot instantiate the `Hasher` trait by itself. You need a real hasher type.

## How this fits the write path

`write_bloom_file` builds one filter for the column values in a row group.

It:

1. creates an empty filter
2. iterates through values
3. skips nulls using the validity bitmap
4. inserts non-null values into the filter
5. serializes the packed bytes
6. writes `column_<index>.bloom`

That means the filter becomes one more piece of row-group metadata that the engine can consult before touching more expensive data.

## Practical mental model

The simplest correct mental model is this:

- the filter is a compact fingerprint field
- each inserted value leaves several bit marks behind
- lookup checks whether those marks are all present
- `false` means impossible
- `true` means possible

That is all a Bloom filter needs to do to be useful in a storage engine.