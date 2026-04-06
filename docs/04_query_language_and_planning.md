# Chapter 4: Query Language and Planning

## Goal of this chapter

This chapter explains how Adolap turns text into executable work. It covers:

1. The AQL language and its statement families.
2. Statement parsing and the recursive-descent expression parser.
3. Logical plans.
4. Optimization.
5. Binding against catalog metadata.
6. Physical plan creation and pushdown.
7. Execution.

---

## 1. AQL: design philosophy

AQL (Adolap Query Language) is SQL-like but deliberately simplified and line-oriented. Each clause occupies its own line. This keeps the language easy to parse, easy to read, and easy to extend without a grammar generator.

Example:

```text
FROM analytics.events
SELECT country, device, revenue
FILTER revenue > 10
FILTER country = "US"
GROUP BY country, device
AGG SUM(revenue)
```

---

## 2. The `Statement` enum

The parser entry point `parse_statement` produces a `Statement`. The full enum from `crates/exec/src/parser.rs`:

```rust
pub enum Statement {
    Query(LogicalPlan),
    CreateDatabase { name: String },
    DropDatabase { name: String },
    CreateTable { table: String, schema: TableSchema, storage_config: TableStorageConfig },
    DropTable { table: String },
    InsertRows { table: Option<String>, rows: Vec<Vec<Option<ColumnValue>>> },
    DeleteRows { table: String, predicate: Option<Expr> },
    IngestInto { table: String, file_path: String },
}
```

`Query` wraps a `LogicalPlan`. Everything else is a DDL or DML operation. The same text protocol handles both analytical queries and table lifecycle management.

---

## 3. Query clauses

The query parser (`parse_query_aql`) recognizes these clauses:

| Clause | Description |
|---|---|
| `FROM <table>` | Required. Names the base table. |
| `JOIN <table> ON <col> = <col>` | Inner join on a single equality. |
| `SELECT <col>, ...` | Projection. |
| `FILTER <expr>` | Row filter. Multiple `FILTER` lines are ANDed together. |
| `GROUP BY <col>, ...` | Grouping keys for aggregation. |
| `AGG <func>(<col>)` | Aggregation function applied after grouping. |
| `GROUP FILTER <agg_expr>` | Post-aggregation predicate (like SQL `HAVING`). |
| `ORDER BY <expr> [ASC\|DESC]` | Sort specification. |
| `LIMIT <n>` | Maximum output rows. |
| `OFFSET <n>` / `SKIP <n>` | Number of leading rows to skip. |

### 3.1 OFFSET and SKIP are identical

Both `OFFSET` and `SKIP` call the same builder method:

```rust
} else if line.starts_with("OFFSET ") {
    builder = builder.offset(parse_usize_clause("OFFSET", &line[7..])?);
} else if line.starts_with("SKIP ") {
    builder = builder.offset(parse_usize_clause("SKIP", &line[5..])?);
}
```

They produce the same `offset` field in `LogicalPlan::Limit`. There is no semantic difference.

---

## 4. Recursive-descent expression parser

Filter expressions are parsed by a hand-written recursive-descent parser with the following precedence chain (lowest to highest):

```
parse_expr
  └─ parse_or
       └─ parse_and
            └─ parse_comparison   (=, >, <)
                 └─ parse_primary  (column, literal, agg ref, parenthesized expr)
```

Key functions from `parser.rs`:

```rust
fn parse_expr(tokens: &[Token], pos: usize) -> Result<(Expr, usize), AdolapError>
fn parse_or(tokens: &[Token], mut pos: usize) -> Result<(Expr, usize), AdolapError>
fn parse_and(tokens: &[Token], mut pos: usize) -> Result<(Expr, usize), AdolapError>
fn parse_comparison(tokens: &[Token], pos: usize) -> Result<(Expr, usize), AdolapError>
fn parse_primary(tokens: &[Token], pos: usize) -> Result<(Expr, usize), AdolapError>
```

`parse_primary` emits:
- `Expr::Column(name)` for identifiers.
- `Expr::Literal(Literal::Utf8(s))` for string literals.
- `Expr::Literal(Literal::I32(v))` for integer literals.
- `Expr::Aggregate { func, column }` for `SUM(col)`, `COUNT(col)`, etc.
- A recursively parsed grouped expression for `(...)`.

---

## 5. The `Expr` enum (predicate model)

From `crates/exec/src/predicate.rs`:

```rust
pub enum Literal {
    Utf8(String),
    I32(i32),
    U32(u32),
    Bool(bool),
}

pub enum Expr {
    Column(String),
    Aggregate { func: AggFunc, column: String },
    Literal(Literal),
    Eq(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}
```

`Expr::evaluate_to_bool_mask` produces a `Vec<bool>` mask over a `RecordBatch`. The dispatch is:

```rust
Expr::Eq(lhs, rhs)  => compare_eq(lhs, rhs, batch),
Expr::Gt(lhs, rhs)  => compare_gt(lhs, rhs, batch),
Expr::Lt(lhs, rhs)  => compare_lt(lhs, rhs, batch),
Expr::And(lhs, rhs) => and_mask(lhs, rhs, batch),
Expr::Or(lhs, rhs)  => or_mask(lhs, rhs, batch),
_ => Err(...)  // Column, Aggregate, Literal cannot be top-level bool predicates
```

### 5.1 NULL comparison semantics

Adolap does not have a dedicated `IS NULL` / `IS NOT NULL` predicate in `Expr`. The current comparison functions operate on concrete typed values. A null row position has its validity bit cleared; comparison functions that inspect the underlying value buffer at a null position will use the placeholder value (e.g., `0` for integers, empty string for UTF-8). This means null rows are not automatically excluded from `=`, `>`, or `<` comparisons at the execution layer — callers should be aware that nulls are not treated with three-valued logic in the current implementation.

---

## 6. Supported aggregation functions (`AggFunc`)

From `crates/exec/src/aggregate.rs`:

```rust
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}
```

AQL syntax and output column naming:

| AQL syntax | `AggFunc` | Output column name |
|---|---|---|
| `COUNT(col)` | `Count` | `count(col)` |
| `SUM(col)` | `Sum` | `sum(col)` |
| `AVG(col)` | `Avg` | `avg(col)` |
| `MIN(col)` | `Min` | `min(col)` |
| `MAX(col)` | `Max` | `max(col)` |

---

## 7. The `LogicalPlan` enum

From `crates/exec/src/logical_plan.rs`:

```rust
pub enum LogicalPlan {
    Scan {
        table_ref: String,
        table: Option<TableMetadata>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_keys: Vec<String>,
        agg_column: String,
        agg_func: AggFunc,
    },
    GroupFilter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_on: String,
        right_on: String,
        output_schema: Option<TableSchema>,
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderBy>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<usize>,
        offset: usize,
    },
}
```

`Scan.table` is `None` immediately after parsing; it is filled in during `bind_plan`. This is how the logical plan distinguishes an unbound from a bound scan.

---

## 8. Optimization

The optimizer (`crates/exec/src/optimizer.rs`) performs a single recursive pass. Its current rule pushes a `Filter` below a `Project` when possible.

```rust
pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, columns } => {
            match optimize(*input) {
                LogicalPlan::Filter { input: inner, predicate } => {
                    let projected = LogicalPlan::Project { input: inner, columns };
                    LogicalPlan::Filter {
                        input: Box::new(optimize(projected)),
                        predicate,
                    }
                }
                other => LogicalPlan::Project {
                    input: Box::new(other),
                    columns,
                },
            }
        }
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(optimize(*input)),
            predicate,
        },
        LogicalPlan::Aggregate { input, group_keys, agg_column, agg_func } => {
            LogicalPlan::Aggregate {
                input: Box::new(optimize(*input)),
                group_keys, agg_column, agg_func,
            }
        }
        // ... Sort, Limit, GroupFilter, Join all recurse similarly
        scan @ LogicalPlan::Scan { .. } => scan,
    }
}
```

The optimizer does not do cost estimation. It applies a single structural rule and recurses. New rules can be added as additional `match` arms.

---

## 9. Binding against catalog metadata (`bind_plan`)

Binding is where symbolic query text becomes schema-aware. It is the phase that fills `Scan.table` with real `TableMetadata` and validates all column name references.

### 9.1 Function signatures

```rust
pub async fn bind_plan(
    catalog: &Catalog,
    plan: LogicalPlan,
) -> Result<LogicalPlan, AdolapError>

pub fn create_physical_plan<'a>(
    plan: &'a LogicalPlan,
) -> Result<PhysicalPlan<'a>, AdolapError>
```

### 9.2 What binding does

`bind_plan` works in two phases:

**Phase 1 — `bind_scans`**: walks the tree and resolves each `Scan.table_ref` against the catalog:

```rust
LogicalPlan::Scan { table_ref, .. } => {
    let table = catalog.resolve_table(&table_ref).await?;
    Ok(LogicalPlan::Scan { table_ref, table: Some(table) })
}
```

**Phase 2 — `bind_plan_node`**: walks the now-catalog-resolved tree and:

- Validates every `Expr::Column` reference against the schema (`canonicalize_column_name`, `require_column`).
- Resolves `Expr::Aggregate` nodes in `GROUP FILTER` and grouped `ORDER BY` expressions into plain `Expr::Column` references pointing at the aggregate output column.
- Builds projected schemas for `Project` nodes (`project_schema`).
- Builds aggregate output schemas for `Aggregate` nodes (`aggregate_schema`), adding an output column named `sum(col)`, `count(col)`, etc.
- Derives the `output_schema` for `Join` nodes by concatenating both sides' schemas.

Column name resolution uses `canonicalize_column_name`, which performs a case-insensitive match against the schema and returns the canonical (schema-declared) spelling. This is how `REVENUE` in a query matches `revenue` in the schema.

---

## 10. Physical plan creation and pushdown

`create_physical_plan` calls the internal helper `create_physical_plan_with_pushdown`, passing an initially empty set of required columns and an empty filter list.

```rust
fn create_physical_plan_with_pushdown<'a>(
    plan: &'a LogicalPlan,
    required_columns: &BTreeSet<String>,
    pushdown_filters: &[Expr],
) -> Result<PhysicalPlan<'a>, AdolapError>
```

### 10.1 Projection pushdown

At each `Project` node, the projected column names are added to `required_columns`. When a `Scan` is finally reached, `derive_scan_projection` computes the minimal column set the scan needs to materialize:

```rust
let projected_columns = derive_scan_projection(table_ref, &table.schema, required_columns);
```

If the projected set equals the full schema, `projected_columns` is `None` (no pruning needed).

### 10.2 Filter pushdown

At each `Filter` node, the predicate is split into conjuncts and added to `pushdown_filters`. When a `Scan` is reached, `build_pushdown_predicate` converts compatible `Expr` nodes into storage-layer `Predicate` values:

```rust
fn expr_to_pushdown_predicate(expr: &Expr) -> Option<Predicate> {
    match expr {
        Expr::Eq(lhs, rhs) => column_literal_predicate(lhs, rhs, Predicate::Equals),
        Expr::Gt(lhs, rhs) => column_literal_predicate(lhs, rhs, Predicate::GreaterThan),
        Expr::Lt(lhs, rhs) => column_literal_predicate(lhs, rhs, Predicate::LessThan),
        ...
    }
}
```

Multiple predicates are combined with `Predicate::And`. Predicates that cannot be pushed (e.g., `OR` expressions) are left as `PhysicalPlan::Filter` nodes above the scan.

### 10.3 `PhysicalPlan` enum

From `crates/exec/src/physical_plan.rs`:

```rust
pub enum PhysicalPlan<'a> {
    Scan {
        table: &'a TableMetadata,
        projected_columns: Option<Vec<String>>,
        predicate: Option<Predicate>,
    },
    Filter {
        input: Box<PhysicalPlan<'a>>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan<'a>>,
        columns: Vec<String>,
    },
    HashAggregate {
        input: Box<PhysicalPlan<'a>>,
        group_keys: Vec<String>,
        agg_column: String,
        agg_func: AggFunc,
    },
    GroupFilter {
        input: Box<PhysicalPlan<'a>>,
        predicate: Expr,
    },
    HashJoin {
        left: Box<PhysicalPlan<'a>>,
        right: Box<PhysicalPlan<'a>>,
        left_on: String,
        right_on: String,
        output_schema: TableSchema,
    },
    Sort {
        input: Box<PhysicalPlan<'a>>,
        order_by: Vec<OrderBy>,
    },
    Limit {
        input: Box<PhysicalPlan<'a>>,
        limit: Option<usize>,
        offset: usize,
    },
}
```

`Scan.predicate` carries the pushed-down storage predicate, which `SegmentReader` uses for bloom-filter and min/max pruning before touching column data.

---

## 11. Execution

The executor (`crates/exec/src/executor.rs`) walks the `PhysicalPlan` recursively and returns `Vec<RecordBatch>`.

```rust
pub fn execute<'b>(
    &'b self,
    plan: &'b PhysicalPlan<'b>,
) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, AdolapError>> + Send + 'b>>
```

The boxed async return type handles recursive async execution without stack overflow.

### 11.1 Operator behaviors

| Operator | Behavior |
|---|---|
| `Scan` | Opens table via `TableReader` with pushed projection and predicate. Returns storage batches. |
| `Filter` | Calls `Expr::evaluate_to_bool_mask` on each batch, keeps matching rows. |
| `Project` | Retains only requested columns from each batch. |
| `HashAggregate` | Materializes all rows, groups by keys, computes aggregate, returns one output batch. |
| `GroupFilter` | Applies post-aggregation predicate (like `HAVING`). |
| `HashJoin` | Materializes both sides, builds a hash map on the right side, emits joined rows. |
| `Sort` | Materializes all rows, sorts in memory, rebuilds a batch. |
| `Limit` | Materializes rows, applies `offset` then `limit`, rebuilds a batch. |

---

## 12. End-to-end pipeline

The server handler shows the complete flow:

```rust
let optimized = optimizer::optimize(plan);
let bound     = planner::bind_plan(catalog, optimized).await?;
let physical  = planner::create_physical_plan(&bound)?;

let executor = Executor::new();
let batches  = executor.execute(&physical).await?;
```

1. Parse text → `Statement::Query(LogicalPlan)`.
2. Optimize the logical plan (filter-below-project).
3. Bind scans to catalog metadata, validate column references.
4. Create a `PhysicalPlan` with projection and filter pushdown.
5. Execute into `Vec<RecordBatch>`.
6. Serialize into a wire `ResultSet` with compact plan strings and execution summary.

---

## 13. Example: tracing a query

```text
FROM analytics.events
SELECT country, revenue
FILTER revenue > 50
ORDER BY revenue DESC
LIMIT 3;
```

1. **Parse**: `Scan("analytics.events") → Filter(revenue > 50) → Project([country, revenue]) → Sort(revenue DESC) → Limit(3, 0)`.
2. **Optimize**: `Filter` is already below `Project`; no reordering needed.
3. **Bind**: `analytics.events` resolved to `TableMetadata`; `revenue` and `country` validated against schema.
4. **Physical plan**: `Scan(projection=[country,revenue], predicate=revenue>50) → Sort → Limit`.
5. **Execute**: storage skips row groups where `max(revenue) ≤ 50`; surviving batches sorted and truncated.

---

## 14. Chapter takeaway

Adolap's planning pipeline is intentionally explicit. Text becomes a logical plan, the logical plan becomes a schema-aware bound plan, the bound plan becomes a physical plan with pushdown, and the executor turns that into `RecordBatch` output. Each phase is a distinct, testable layer that can be extended independently.
