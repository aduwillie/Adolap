# Chapter 4: Query Language and Planning

## Goal of this chapter

This chapter explains how Adolap turns text into executable work. It covers:

- the AQL language
- statement parsing
- logical plans
- optimization
- binding against catalog metadata
- physical plan creation
- execution

## AQL is intentionally small and line-oriented

The parser documentation describes AQL as SQL-like but simplified and line-oriented. That is an important design choice. It keeps the language easy to parse and easy to extend.

An example query from the parser module captures the style well:

```text
FROM analytics.events
SELECT country, device, revenue
FILTER revenue > 10
FILTER country = "US"
GROUP BY country, device
AGG SUM(revenue)
```

Instead of trying to mimic every part of SQL, AQL emphasizes a predictable clause-per-line structure.

## Supported statement families

The parser currently handles more than just queries. The `Statement` enum includes:

- `Query`
- `CreateDatabase`
- `DropDatabase`
- `CreateTable`
- `DropTable`
- `InsertRows`
- `DeleteRows`
- `IngestInto`

That means the same text protocol supports both analytical queries and operational table management.

## Query clauses

The query parser recognizes these clauses:

- `FROM`
- `JOIN`
- `SELECT`
- `FILTER`
- `GROUP BY`
- `GROUP FILTER`
- `AGG`
- `ORDER BY`
- `LIMIT`
- `OFFSET`
- `SKIP`

### Example: projection and filtering

```text
FROM analytics.events
SELECT country, revenue
FILTER revenue > 50;
```

### Example: join

```text
FROM analytics.events
JOIN analytics.users ON user_id = id
SELECT country, revenue;
```

### Example: grouped aggregate with post-aggregate filtering

```text
FROM analytics.events
SELECT country
GROUP BY country
AGG SUM(revenue)
GROUP FILTER SUM(revenue) > 100
ORDER BY SUM(revenue) DESC;
```

This is one of the more interesting parts of the language because `GROUP FILTER` behaves like a grouped post-aggregation predicate.

## Expression parsing

Filter expressions support:

- `=`
- `>`
- `<`
- `AND`
- `OR`
- parentheses
- identifiers
- string literals
- integer literals

The parser uses recursive descent with operator precedence:

1. `OR`
2. `AND`
3. comparison
4. primary expressions

That gives you predictable boolean behavior without a parser generator.

## The logical plan

Once text is parsed, Adolap builds a `LogicalPlan` tree. The logical plan is one of the best places to understand the engine because it names the key operations explicitly.

Representative shape:

```rust
pub enum LogicalPlan {
    Scan { table_ref: String, table: Option<TableMetadata> },
    Filter { input: Box<LogicalPlan>, predicate: Expr },
    Project { input: Box<LogicalPlan>, columns: Vec<String> },
    Aggregate { ... },
    GroupFilter { ... },
    Join { ... },
    Sort { ... },
    Limit { ... },
}
```

The logical plan is not yet executable. It still needs binding and physical planning.

## Optimization

The optimizer is intentionally simple today, which is a good fit for a learning project. The current rule pushes a `Filter` below a `Project` when possible.

The code says this directly:

```rust
// For now, just a simple recursive pass that:
// - pushes Filter below Project when possible
// - leaves everything else as-is
```

This is worth noting because it shows a realistic optimization idea with a small implementation footprint. The engine does not pretend to be a fully cost-based optimizer.

## Binding against catalog metadata

Binding is where symbolic query text becomes schema-aware.

The planner binds scans through the catalog:

```rust
pub async fn bind_plan(catalog: &Catalog, plan: LogicalPlan) -> Result<LogicalPlan, AdolapError>
```

Important things binding does:

- resolve table references to actual `TableMetadata`
- validate column references against schemas
- canonicalize column names
- derive aggregate output columns
- resolve grouped `ORDER BY` and `GROUP FILTER`
- construct join output schemas

The bind step is where a lot of correctness lives. It prevents later layers from having to guess what a user meant.

## Physical plan creation and pushdown

After binding, the planner creates a `PhysicalPlan`. This is where execution details such as scan projection and pushed predicates are chosen.

The planner entrypoint is:

```rust
pub fn create_physical_plan<'a>(plan: &'a LogicalPlan) -> Result<PhysicalPlan<'a>, AdolapError>
```

The internal helper `create_physical_plan_with_pushdown` shows an important design direction: the planner tracks required columns and pushdown filters so scans can avoid unnecessary work.

That means even though Adolap is compact, it is already thinking in terms of:

- projection pushdown
- filter pushdown
- schema-aware scans

## Execution

The `Executor` walks the physical plan and returns batches. It handles:

- scan
- filter
- project
- aggregate
- group filter
- hash join
- sort
- limit/offset

The main entrypoint is:

```rust
pub fn execute<'b>(
    &'b self,
    plan: &'b PhysicalPlan<'b>,
) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, AdolapError>> + Send + 'b>>
```

This may look slightly heavy because of boxed async recursion, but it keeps the executor implementation direct and readable.

## Predicate evaluation

The current predicate system supports a useful but intentionally bounded set of boolean expressions. The `Expr` enum contains column references, literals, aggregate references, comparison nodes, and boolean conjunctions.

Representative shape:

```rust
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

The current evaluation path focuses on concrete combinations that are already needed by the engine. That keeps complexity under control while still enabling realistic queries.

## End-to-end planning path in one place

The server handler shows the whole planning flow clearly:

```rust
let optimized = optimizer::optimize(plan);
let bound = planner::bind_plan(catalog, optimized).await?;
let physical = planner::create_physical_plan(&bound)?;

let executor = Executor::new();
let batches = executor.execute(&physical).await?;
```

That is the central path to keep in mind when reading the engine.

## Example query and what happens

Take this query:

```text
FROM analytics.events
SELECT country, revenue
FILTER revenue > 50
ORDER BY revenue DESC
LIMIT 3;
```

The engine does the following:

1. parse each clause into an internal plan description
2. create a logical `Scan -> Filter -> Project -> Sort -> Limit` structure
3. bind the `analytics.events` scan to actual table metadata from the catalog
4. validate the `country` and `revenue` references against the schema
5. derive a physical plan with pushed scan information where possible
6. execute the plan against storage batches
7. stringify the first result batch into a protocol result set for the CLI

## Why the planning layer matters

The planning layer is the bridge between “nice user language” and “correct work over storage.” If you want to extend Adolap with new clauses or operators, this is where the architectural consequences become visible.

Typical future extensions would likely touch:

- parser
- logical plan
- binder
- physical plan
- executor

That layered structure is a strong sign that the codebase has a sound shape for growth.

## Chapter takeaway

The key lesson is that Adolap’s planning pipeline is intentionally explicit. Text becomes a logical plan, the logical plan becomes a schema-aware bound plan, the bound plan becomes a physical plan, and the executor turns that into batches. Each phase is visible in the code and teachable on its own.