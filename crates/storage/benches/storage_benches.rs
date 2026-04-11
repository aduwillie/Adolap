//! Criterion benchmarks for the storage crate.
//!
//! Run with: `cargo bench -p storage`
//!
//! Benchmarks cover:
//!  - bloom filter `add` and `might_contain`
//!  - column statistics computation
//!  - compression round-trips (LZ4 and Zstd)
//!  - full insert-then-read round-trip through `TableWriter`/`TableReader`

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use storage::{
    bloom::BloomFilter,
    compression::{compress_buffer, decompress_buffer},
    config::{CompressionType, TableStorageConfig},
    schema::{ColumnSchema, ColumnType, TableSchema},
    stats::{compute_i32_column_stats, compute_utf8_column_stats},
};

// ---------------------------------------------------------------------------
// Bloom filter benchmarks
// ---------------------------------------------------------------------------

fn bench_bloom_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom/add");
    for n in [100usize, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut bloom = BloomFilter::with_capacity(n);
                for i in 0..n {
                    bloom.add(&(i as i32));
                }
                bloom
            });
        });
    }
    group.finish();
}

fn bench_bloom_might_contain_positive(c: &mut Criterion) {
    let n = 1_000usize;
    let mut bloom = BloomFilter::with_capacity(n);
    for i in 0..n {
        bloom.add(&(i as i32));
    }

    c.bench_function("bloom/might_contain/positive", |b| {
        b.iter(|| {
            let mut found = 0usize;
            for i in 0..n {
                if bloom.might_contain(&(i as i32)) {
                    found += 1;
                }
            }
            found
        });
    });
}

fn bench_bloom_might_contain_negative(c: &mut Criterion) {
    let n = 1_000usize;
    let mut bloom = BloomFilter::with_capacity(n);
    for i in 0..n {
        bloom.add(&(i as i32));
    }

    c.bench_function("bloom/might_contain/negative", |b| {
        b.iter(|| {
            let mut found = 0usize;
            // Query values that were never inserted — should all return false
            // (with high probability for a well-sized filter).
            let start = n as i32;
            let end = start * 2;
            for i in start..end {
                if bloom.might_contain(&i) {
                    found += 1;
                }
            }
            found
        });
    });
}

// ---------------------------------------------------------------------------
// Column statistics benchmarks
// ---------------------------------------------------------------------------

fn bench_compute_i32_stats(c: &mut Criterion) {
    let mut group = c.benchmark_group("stats/compute_i32");
    for n in [1_000usize, 16_384, 65_536] {
        let values: Vec<i32> = (0..n as i32).collect();
        group.bench_with_input(BenchmarkId::from_parameter(n), &values, |b, v| {
            b.iter(|| compute_i32_column_stats(v, None).unwrap());
        });
    }
    group.finish();
}

fn bench_compute_utf8_stats(c: &mut Criterion) {
    let mut group = c.benchmark_group("stats/compute_utf8");
    for n in [1_000usize, 16_384] {
        let values: Vec<String> = (0..n).map(|i| format!("value_{}", i)).collect();
        group.bench_with_input(BenchmarkId::from_parameter(n), &values, |b, v| {
            b.iter(|| compute_utf8_column_stats(v, None).unwrap());
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Compression round-trip benchmarks
// ---------------------------------------------------------------------------

fn bench_compression_roundtrip(c: &mut Criterion) {
    // Simulate a 16 384-element i32 column payload (~64 KB uncompressed).
    let values: Vec<i32> = (0..16_384).collect();
    let payload = postcard::to_allocvec(&values).unwrap();

    let mut group = c.benchmark_group("compression/roundtrip");

    for codec in [CompressionType::Lz4, CompressionType::Zstd, CompressionType::None] {
        let label = format!("{:?}", codec);
        group.bench_with_input(BenchmarkId::from_parameter(&label), &payload, |b, p| {
            b.iter(|| {
                let compressed = compress_buffer(p, codec).unwrap();
                let decompressed = decompress_buffer(&compressed, codec).unwrap();
                decompressed
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Full insert / read round-trip
// ---------------------------------------------------------------------------

fn bench_insert_and_read(c: &mut Criterion) {
    use storage::{
        column::ColumnValue,
        table_reader::TableReader,
        table_writer::TableWriter,
    };
    use tokio::runtime::Runtime;

    let rt = Runtime::new().unwrap();
    let schema = TableSchema {
        columns: vec![
            ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false },
            ColumnSchema { name: "value".into(), column_type: ColumnType::I32, nullable: false },
        ],
    };
    let config = TableStorageConfig {
        row_group_size: 1_000,
        ..TableStorageConfig::default()
    };

    // Build 5 000 rows once and reuse them across iterations.
    let rows: Vec<Vec<Option<ColumnValue>>> = (0u32..5_000)
        .map(|i| vec![Some(ColumnValue::U32(i)), Some(ColumnValue::I32(i as i32 * -1))])
        .collect();

    c.bench_function("insert_read/5000_rows", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp_dir = tempfile::tempdir().unwrap();
                let writer = TableWriter::create_table(
                    temp_dir.path(),
                    "default",
                    "bench",
                    &schema,
                    &config,
                )
                .await
                .unwrap();
                writer.insert_rows(&rows).await.unwrap();

                let reader = TableReader::new(&writer.table_dir, &schema);
                let batches = reader.read_table(None, None).await.unwrap();
                batches.into_iter().map(|b| b.row_count).sum::<usize>()
            })
        });
    });
}

criterion_group!(
    benches,
    bench_bloom_add,
    bench_bloom_might_contain_positive,
    bench_bloom_might_contain_negative,
    bench_compute_i32_stats,
    bench_compute_utf8_stats,
    bench_compression_roundtrip,
    bench_insert_and_read,
);
criterion_main!(benches);
