//! Criterion benchmarks for the exec crate.
//!
//! Run with: `cargo bench -p exec`
//!
//! Benchmarks cover:
//!  - predicate evaluation on a `RecordBatch`
//!  - AQL parsing of common statement types
//!  - logical plan construction and optimization
//!  - physical plan creation and full query execution

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use exec::{
    optimizer,
    parser::parse_statement,
    predicate::{col, lit_i32},
};
use storage::{
    column::ColumnValue,
    record_batch::RecordBatch,
    schema::{ColumnSchema, ColumnType, TableSchema},
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false },
            ColumnSchema { name: "revenue".into(), column_type: ColumnType::I32, nullable: false },
        ],
    }
}

fn make_batch(row_count: usize) -> RecordBatch {
    let rows: Vec<Vec<Option<ColumnValue>>> = (0..row_count)
        .map(|i| {
            vec![
                Some(ColumnValue::U32(i as u32)),
                Some(ColumnValue::I32(i as i32 - (row_count as i32 / 2))),
            ]
        })
        .collect();
    RecordBatch::from_rows(make_schema(), &rows).unwrap()
}

// ---------------------------------------------------------------------------
// Predicate evaluation
// ---------------------------------------------------------------------------

fn bench_predicate_eval(c: &mut Criterion) {
    let mut group = c.benchmark_group("predicate/eval");
    for n in [1_000usize, 16_384, 65_536] {
        let batch = make_batch(n);
        let predicate = col("revenue").gt(lit_i32(0));

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| predicate.evaluate_to_bool_mask(&batch).unwrap());
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// AQL parsing
// ---------------------------------------------------------------------------

fn bench_parse_simple_select(c: &mut Criterion) {
    let query = "FROM analytics.events SELECT id, revenue FILTER revenue > 100;";
    c.bench_function("parse/simple_select", |b| {
        b.iter(|| parse_statement(query).unwrap());
    });
}

fn bench_parse_grouped_aggregate(c: &mut Criterion) {
    let query = "FROM analytics.events \
                 SELECT country \
                 FILTER revenue > 0 \
                 GROUP BY country \
                 AGG SUM(revenue) \
                 ORDER BY SUM(revenue) DESC \
                 LIMIT 10;";
    c.bench_function("parse/grouped_aggregate", |b| {
        b.iter(|| parse_statement(query).unwrap());
    });
}

// ---------------------------------------------------------------------------
// Optimizer
// ---------------------------------------------------------------------------

fn bench_optimizer(c: &mut Criterion) {
    use exec::{logical_plan::LogicalPlan, predicate::lit_i32};
    // Build a plan that the optimizer can improve: Filter above Project.
    let plan = LogicalPlan::scan("analytics.events")
        .project(vec!["id", "revenue"])
        .filter(col("revenue").gt(lit_i32(0)));

    c.bench_function("optimizer/filter_pushdown", |b| {
        b.iter(|| optimizer::optimize(plan.clone()));
    });
}

criterion_group!(
    benches,
    bench_predicate_eval,
    bench_parse_simple_select,
    bench_parse_grouped_aggregate,
    bench_optimizer,
);
criterion_main!(benches);
