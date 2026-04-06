use crate::aggregate::{agg_output_name, AggFunc};
use crate::logical_plan::{LogicalPlan, OrderBy};
use crate::physical_plan::PhysicalPlan;
use crate::predicate::Expr;
use core::error::AdolapError;
use std::collections::BTreeSet;
use storage::{
    catalog::Catalog,
    segment_reader::Predicate,
    schema::{ColumnSchema, TableSchema},
};
use tracing::debug;

pub async fn bind_plan(catalog: &Catalog, plan: LogicalPlan) -> Result<LogicalPlan, AdolapError> {
    debug!(?plan, "binding logical plan");
    let plan = bind_scans(plan, catalog).await?;
    let (bound, _) = bind_plan_node(plan, &TableSchema { columns: Vec::new() }, None)?;
    debug!(?bound, "bound logical plan");
    Ok(bound)
}

pub fn create_physical_plan<'a>(plan: &'a LogicalPlan) -> Result<PhysicalPlan<'a>, AdolapError> {
    debug!(?plan, "creating physical plan");
    create_physical_plan_with_pushdown(plan, &BTreeSet::new(), &[])
}
fn bind_scans<'a>(
    plan: LogicalPlan,
    catalog: &'a Catalog,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<LogicalPlan, AdolapError>> + Send + 'a>> {
    Box::pin(async move {
        match plan {
            LogicalPlan::Scan { table_ref, .. } => {
                let table = catalog.resolve_table(&table_ref).await?;
                debug!(table = %table_ref, path = %table.path.display(), "resolved scan table metadata");
                Ok(LogicalPlan::Scan {
                    table_ref,
                    table: Some(table),
                })
            }
            LogicalPlan::Filter { input, predicate } => Ok(LogicalPlan::Filter {
                input: Box::new(bind_scans(*input, catalog).await?),
                predicate,
            }),
            LogicalPlan::Project { input, columns } => Ok(LogicalPlan::Project {
                input: Box::new(bind_scans(*input, catalog).await?),
                columns,
            }),
            LogicalPlan::Aggregate { input, group_keys, agg_column, agg_func } => {
                Ok(LogicalPlan::Aggregate {
                    input: Box::new(bind_scans(*input, catalog).await?),
                    group_keys,
                    agg_column,
                    agg_func,
                })
            }
            LogicalPlan::GroupFilter { input, predicate } => Ok(LogicalPlan::GroupFilter {
                input: Box::new(bind_scans(*input, catalog).await?),
                predicate,
            }),
            LogicalPlan::Join { left, right, left_on, right_on, output_schema } => {
                Ok(LogicalPlan::Join {
                    left: Box::new(bind_scans(*left, catalog).await?),
                    right: Box::new(bind_scans(*right, catalog).await?),
                    left_on,
                    right_on,
                    output_schema,
                })
            }
            LogicalPlan::Sort { input, order_by } => Ok(LogicalPlan::Sort {
                input: Box::new(bind_scans(*input, catalog).await?),
                order_by,
            }),
            LogicalPlan::Limit { input, limit, offset } => Ok(LogicalPlan::Limit {
                input: Box::new(bind_scans(*input, catalog).await?),
                limit,
                offset,
            }),
        }
    })
}

#[derive(Clone)]
struct AggregateBinding {
    func: AggFunc,
    input_column: String,
    output_column: String,
}

fn bind_plan_node(
    plan: LogicalPlan,
    input_schema: &TableSchema,
    aggregate: Option<AggregateBinding>,
) -> Result<(LogicalPlan, TableSchema), AdolapError> {
    match plan {
        LogicalPlan::Scan { table_ref, table } => {
            let table = table.ok_or_else(|| AdolapError::ExecutionError("Query plan has not been bound to catalog metadata".into()))?;
            Ok((
                LogicalPlan::Scan { table_ref, table: Some(table.clone()) },
                table.schema.clone(),
            ))
        }
        LogicalPlan::Filter { input, predicate } => {
            let (input, schema) = bind_plan_node(*input, input_schema, aggregate)?;
            let predicate = bind_expr(predicate, None, &schema)?;
            validate_expr(&predicate, &schema)?;
            Ok((
                LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate,
                },
                schema,
            ))
        }
        LogicalPlan::Project { input, columns } => {
            let (input, schema) = bind_plan_node(*input, input_schema, aggregate)?;
            let projected = project_schema(&schema, &columns)?;
            Ok((
                LogicalPlan::Project {
                    input: Box::new(input),
                    columns,
                },
                projected,
            ))
        }
        LogicalPlan::Aggregate {
            input,
            group_keys,
            agg_column,
            agg_func,
        } => {
            let (input, schema) = bind_plan_node(*input, input_schema, None)?;
            let group_keys = group_keys
                .into_iter()
                .map(|key| canonicalize_column_name(&schema, &key))
                .collect::<Result<Vec<_>, _>>()?;
            let agg_column = canonicalize_column_name(&schema, &agg_column)?;

            let output_schema = aggregate_schema(&schema, &group_keys, &agg_column, &agg_func)?;
            Ok((
                LogicalPlan::Aggregate {
                    input: Box::new(input),
                    group_keys,
                    agg_column,
                    agg_func,
                },
                output_schema,
            ))
        }
        LogicalPlan::GroupFilter { input, predicate } => {
            let (input, schema) = bind_plan_node(*input, input_schema, aggregate.clone())?;
            let Some(aggregate) = find_aggregate_binding(&input) else {
                return Err(AdolapError::ExecutionError(
                    "GROUP FILTER can only be used after GROUP BY".into(),
                ));
            };
            let predicate = bind_expr(predicate, Some(&aggregate), &schema)?;
            validate_expr(&predicate, &schema)?;
            Ok((
                LogicalPlan::GroupFilter {
                    input: Box::new(input),
                    predicate,
                },
                schema,
            ))
        }
        LogicalPlan::Join { left, right, left_on, right_on, .. } => {
            let (left, left_schema) = bind_plan_node(*left, input_schema, None)?;
            let (right, right_schema) = bind_plan_node(*right, input_schema, None)?;
            let left_prefix = plan_prefix(&left)?;
            let right_prefix = plan_prefix(&right)?;
            let left_on = resolve_join_column(&left_schema, &left_prefix, &left_on)?;
            let right_on = resolve_join_column(&right_schema, &right_prefix, &right_on)?;
            let output_schema = join_output_schema(&left, &left_schema, &right, &right_schema)?;
            Ok((
                LogicalPlan::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    left_on,
                    right_on,
                    output_schema: Some(output_schema.clone()),
                },
                output_schema,
            ))
        }
        LogicalPlan::Sort { input, order_by } => {
            let (input, schema) = bind_plan_node(*input, input_schema, aggregate.clone())?;
            let aggregate = find_aggregate_binding(&input);
            let order_by = bind_order_by(order_by, aggregate.as_ref(), &schema)?;
            Ok((
                LogicalPlan::Sort {
                    input: Box::new(input),
                    order_by,
                },
                schema,
            ))
        }
        LogicalPlan::Limit { input, limit, offset } => {
            let (input, schema) = bind_plan_node(*input, input_schema, aggregate)?;
            Ok((
                LogicalPlan::Limit {
                    input: Box::new(input),
                    limit,
                    offset,
                },
                schema,
            ))
        }
    }
}

fn validate_expr(expr: &Expr, schema: &TableSchema) -> Result<(), AdolapError> {
    match expr {
        Expr::Column(name) => {
            require_column(schema, name)?;
            Ok(())
        }
        Expr::Aggregate { .. } => Err(AdolapError::ExecutionError(
            "Aggregate references must be bound before execution".into(),
        )),
        Expr::Literal(_) => Ok(()),
        Expr::Eq(lhs, rhs)
        | Expr::Gt(lhs, rhs)
        | Expr::Lt(lhs, rhs)
        | Expr::And(lhs, rhs)
        | Expr::Or(lhs, rhs) => {
            validate_expr(lhs, schema)?;
            validate_expr(rhs, schema)
        }
    }
}

fn project_schema(schema: &TableSchema, columns: &[String]) -> Result<TableSchema, AdolapError> {
    let projected_columns = columns
        .iter()
        .map(|column| require_column(schema, column).cloned())
        .collect::<Result<Vec<ColumnSchema>, AdolapError>>()?;
    Ok(TableSchema {
        columns: projected_columns,
    })
}

fn aggregate_schema(
    input_schema: &TableSchema,
    group_keys: &[String],
    agg_column: &str,
    agg_func: &AggFunc,
) -> Result<TableSchema, AdolapError> {
    let mut columns = group_keys
        .iter()
        .map(|key| require_column(input_schema, key).cloned())
        .collect::<Result<Vec<ColumnSchema>, AdolapError>>()?;

    columns.push(ColumnSchema {
        name: agg_output_name(agg_func, agg_column),
        column_type: storage::schema::ColumnType::I32,
        nullable: false,
    });

    Ok(TableSchema { columns })
}

fn bind_expr(expr: Expr, aggregate: Option<&AggregateBinding>, schema: &TableSchema) -> Result<Expr, AdolapError> {
    match expr {
        Expr::Aggregate { func, column } => {
            let aggregate = aggregate.ok_or_else(|| {
                AdolapError::ExecutionError(
                    "Aggregate references are only valid in GROUP FILTER or grouped ORDER BY"
                        .into(),
                )
            })?;

            if aggregate.func != func || aggregate.input_column != column {
                return Err(AdolapError::ExecutionError(format!(
                    "Aggregate reference {:?}({}) does not match the active AGG clause",
                    func, column
                )));
            }

            Ok(Expr::Column(aggregate.output_column.clone()))
        }
        Expr::Column(name) => Ok(Expr::Column(canonicalize_column_name(schema, &name).unwrap_or(name))),
        Expr::Literal(literal) => Ok(Expr::Literal(literal)),
        Expr::Eq(lhs, rhs) => Ok(Expr::Eq(
            Box::new(bind_expr(*lhs, aggregate, schema)?),
            Box::new(bind_expr(*rhs, aggregate, schema)?),
        )),
        Expr::Gt(lhs, rhs) => Ok(Expr::Gt(
            Box::new(bind_expr(*lhs, aggregate, schema)?),
            Box::new(bind_expr(*rhs, aggregate, schema)?),
        )),
        Expr::Lt(lhs, rhs) => Ok(Expr::Lt(
            Box::new(bind_expr(*lhs, aggregate, schema)?),
            Box::new(bind_expr(*rhs, aggregate, schema)?),
        )),
        Expr::And(lhs, rhs) => Ok(Expr::And(
            Box::new(bind_expr(*lhs, aggregate, schema)?),
            Box::new(bind_expr(*rhs, aggregate, schema)?),
        )),
        Expr::Or(lhs, rhs) => Ok(Expr::Or(
            Box::new(bind_expr(*lhs, aggregate, schema)?),
            Box::new(bind_expr(*rhs, aggregate, schema)?),
        )),
    }
}

fn bind_order_by(
    order_by: Vec<OrderBy>,
    aggregate: Option<&AggregateBinding>,
    schema: &TableSchema,
) -> Result<Vec<OrderBy>, AdolapError> {
    order_by
        .into_iter()
        .map(|order| {
            let expr = bind_expr(order.expr, aggregate, schema)?;
            match &expr {
                Expr::Column(name) => {
                    let name = canonicalize_column_name(schema, name)?;
                    require_column(schema, &name)?;
                    Ok(OrderBy {
                        expr: Expr::Column(name),
                        direction: order.direction,
                    })
                }
                _ => Err(AdolapError::ExecutionError(
                    "ORDER BY only supports columns and aggregate references".into(),
                )),
            }
        })
        .collect()
}

fn find_aggregate_binding(plan: &LogicalPlan) -> Option<AggregateBinding> {
    match plan {
        LogicalPlan::Aggregate {
            input: _,
            agg_column,
            agg_func,
            ..
        } => Some(AggregateBinding {
            func: agg_func.clone(),
            input_column: agg_column.clone(),
            output_column: agg_output_name(agg_func, agg_column),
        }),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::GroupFilter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => find_aggregate_binding(input),
        LogicalPlan::Join { .. } => None,
        LogicalPlan::Scan { .. } => None,
    }
}

fn create_physical_plan_with_pushdown<'a>(
    plan: &'a LogicalPlan,
    required_columns: &BTreeSet<String>,
    pushdown_filters: &[Expr],
) -> Result<PhysicalPlan<'a>, AdolapError> {
    match plan {
        LogicalPlan::Scan { table: Some(table), table_ref } => {
            let projected_columns = derive_scan_projection(table_ref, &table.schema, required_columns);
            let predicate = build_pushdown_predicate(pushdown_filters, table_ref, &table.schema);
            Ok(PhysicalPlan::Scan {
                table,
                projected_columns,
                predicate,
            })
        }
        LogicalPlan::Scan { .. } => Err(AdolapError::ExecutionError(
            "Query plan has not been bound to catalog metadata".into(),
        )),
        LogicalPlan::Filter { input, predicate } => {
            let mut child_required = required_columns.clone();
            collect_expr_columns(predicate, &mut child_required);
            let mut child_filters = pushdown_filters.to_vec();
            child_filters.extend(split_conjuncts(predicate.clone()));
            Ok(PhysicalPlan::Filter {
                input: Box::new(create_physical_plan_with_pushdown(input, &child_required, &child_filters)?),
                predicate: predicate.clone(),
            })
        }
        LogicalPlan::Project { input, columns } => {
            let child_required = columns.iter().cloned().collect::<BTreeSet<_>>();
            Ok(PhysicalPlan::Project {
                input: Box::new(create_physical_plan_with_pushdown(input, &child_required, pushdown_filters)?),
                columns: columns.clone(),
            })
        }
        LogicalPlan::Aggregate { input, group_keys, agg_column, agg_func } => {
            let mut child_required = group_keys.iter().cloned().collect::<BTreeSet<_>>();
            child_required.insert(agg_column.clone());
            Ok(PhysicalPlan::HashAggregate {
                input: Box::new(create_physical_plan_with_pushdown(input, &child_required, pushdown_filters)?),
                group_keys: group_keys.clone(),
                agg_column: agg_column.clone(),
                agg_func: agg_func.clone(),
            })
        }
        LogicalPlan::GroupFilter { input, predicate } => {
            let mut child_required = required_columns.clone();
            collect_expr_columns(predicate, &mut child_required);
            Ok(PhysicalPlan::GroupFilter {
                input: Box::new(create_physical_plan_with_pushdown(input, &child_required, pushdown_filters)?),
                predicate: predicate.clone(),
            })
        }
        LogicalPlan::Join { left, right, left_on, right_on, output_schema } => {
            let left_prefix = plan_prefix(left)?;
            let right_prefix = plan_prefix(right)?;
            let mut left_required = split_required_for_side(required_columns, &left_prefix);
            let mut right_required = split_required_for_side(required_columns, &right_prefix);
            left_required.insert(left_on.clone());
            right_required.insert(right_on.clone());

            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_filters = filters_for_side(pushdown_filters, &left_prefix, &left_schema);
            let right_filters = filters_for_side(pushdown_filters, &right_prefix, &right_schema);

            Ok(PhysicalPlan::HashJoin {
                left: Box::new(create_physical_plan_with_pushdown(left, &left_required, &left_filters)?),
                right: Box::new(create_physical_plan_with_pushdown(right, &right_required, &right_filters)?),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                output_schema: output_schema.clone().ok_or_else(|| AdolapError::ExecutionError("Join plan has not been bound to an output schema".into()))?,
            })
        }
        LogicalPlan::Sort { input, order_by } => {
            let mut child_required = required_columns.clone();
            for order in order_by {
                collect_expr_columns(&order.expr, &mut child_required);
            }
            Ok(PhysicalPlan::Sort {
                input: Box::new(create_physical_plan_with_pushdown(input, &child_required, pushdown_filters)?),
                order_by: order_by.clone(),
            })
        }
        LogicalPlan::Limit { input, limit, offset } => Ok(PhysicalPlan::Limit {
            input: Box::new(create_physical_plan_with_pushdown(input, required_columns, pushdown_filters)?),
            limit: *limit,
            offset: *offset,
        }),
    }
}

fn canonicalize_column_name(schema: &TableSchema, name: &str) -> Result<String, AdolapError> {
    if require_column(schema, name).is_ok() {
        return Ok(name.to_string());
    }

    let matches = schema
        .columns
        .iter()
        .filter(|column| column.name.rsplit('.').next() == Some(name))
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [single] => Ok(single.clone()),
        [] => Err(AdolapError::ExecutionError(format!("Unknown column: {}", name))),
        _ => Err(AdolapError::ExecutionError(format!("Ambiguous column: {}", name))),
    }
}

fn resolve_join_column(schema: &TableSchema, prefix: &str, name: &str) -> Result<String, AdolapError> {
    if let Some(stripped) = name.strip_prefix(&format!("{}.", prefix)) {
        return canonicalize_column_name(schema, stripped);
    }
    canonicalize_column_name(schema, name)
}

fn plan_prefix(plan: &LogicalPlan) -> Result<String, AdolapError> {
    match plan {
        LogicalPlan::Scan { table_ref, .. } => Ok(table_ref.clone()),
        LogicalPlan::Join { .. } => Err(AdolapError::ExecutionError(
            "Nested join references must use already bound column names".into(),
        )),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::GroupFilter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => plan_prefix(input),
    }
}

fn join_output_schema(
    left: &LogicalPlan,
    left_schema: &TableSchema,
    right: &LogicalPlan,
    right_schema: &TableSchema,
) -> Result<TableSchema, AdolapError> {
    let left_prefix = plan_prefix(left)?;
    let right_prefix = plan_prefix(right)?;
    Ok(TableSchema {
        columns: qualify_schema(left_schema, &left_prefix)
            .into_iter()
            .chain(qualify_schema(right_schema, &right_prefix))
            .collect(),
    })
}

fn qualify_schema(schema: &TableSchema, prefix: &str) -> Vec<ColumnSchema> {
    schema
        .columns
        .iter()
        .map(|column| {
            if column.name.contains('.') {
                column.clone()
            } else {
                let mut qualified = column.clone();
                qualified.name = format!("{}.{}", prefix, qualified.name);
                qualified
            }
        })
        .collect()
}

fn collect_expr_columns(expr: &Expr, output: &mut BTreeSet<String>) {
    match expr {
        Expr::Column(name) => {
            output.insert(name.clone());
        }
        Expr::Aggregate { .. } | Expr::Literal(_) => {}
        Expr::Eq(lhs, rhs)
        | Expr::Gt(lhs, rhs)
        | Expr::Lt(lhs, rhs)
        | Expr::And(lhs, rhs)
        | Expr::Or(lhs, rhs) => {
            collect_expr_columns(lhs, output);
            collect_expr_columns(rhs, output);
        }
    }
}

fn split_conjuncts(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::And(lhs, rhs) => {
            let mut out = split_conjuncts(*lhs);
            out.extend(split_conjuncts(*rhs));
            out
        }
        other => vec![other],
    }
}

fn derive_scan_projection(
    table_ref: &str,
    schema: &TableSchema,
    required_columns: &BTreeSet<String>,
) -> Option<Vec<String>> {
    if required_columns.is_empty() {
        return None;
    }

    let mut projected = required_columns
        .iter()
        .filter_map(|column| scan_local_column_name(table_ref, schema, column))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    if projected.is_empty() || projected.len() == schema.columns.len() {
        None
    } else {
        projected.sort();
        Some(projected)
    }
}

fn filters_for_side(filters: &[Expr], table_ref: &str, schema: &TableSchema) -> Vec<Expr> {
    filters
        .iter()
        .filter_map(|expr| rewrite_expr_for_scan(table_ref, schema, expr))
        .collect()
}

fn build_pushdown_predicate(filters: &[Expr], table_ref: &str, schema: &TableSchema) -> Option<Predicate> {
    let predicates = filters
        .iter()
        .filter_map(|expr| rewrite_expr_for_scan(table_ref, schema, expr))
        .filter_map(|expr| expr_to_pushdown_predicate(&expr))
        .collect::<Vec<_>>();

    match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(Predicate::And(predicates)),
    }
}

fn rewrite_expr_for_scan(table_ref: &str, schema: &TableSchema, expr: &Expr) -> Option<Expr> {
    match expr {
        Expr::Column(name) => scan_local_column_name(table_ref, schema, name).map(Expr::Column),
        Expr::Literal(literal) => Some(Expr::Literal(literal.clone())),
        Expr::Aggregate { .. } => None,
        Expr::Eq(lhs, rhs) => Some(Expr::Eq(
            Box::new(rewrite_expr_for_scan(table_ref, schema, lhs)?),
            Box::new(rewrite_expr_for_scan(table_ref, schema, rhs)?),
        )),
        Expr::Gt(lhs, rhs) => Some(Expr::Gt(
            Box::new(rewrite_expr_for_scan(table_ref, schema, lhs)?),
            Box::new(rewrite_expr_for_scan(table_ref, schema, rhs)?),
        )),
        Expr::Lt(lhs, rhs) => Some(Expr::Lt(
            Box::new(rewrite_expr_for_scan(table_ref, schema, lhs)?),
            Box::new(rewrite_expr_for_scan(table_ref, schema, rhs)?),
        )),
        Expr::And(lhs, rhs) => Some(Expr::And(
            Box::new(rewrite_expr_for_scan(table_ref, schema, lhs)?),
            Box::new(rewrite_expr_for_scan(table_ref, schema, rhs)?),
        )),
        Expr::Or(_, _) => None,
    }
}

fn scan_local_column_name(table_ref: &str, schema: &TableSchema, name: &str) -> Option<String> {
    if schema.columns.iter().any(|column| column.name == name) {
        return Some(name.to_string());
    }

    let prefixed = format!("{}.{}", table_ref, name);
    if let Some(stripped) = name.strip_prefix(&format!("{}.", table_ref)) {
        if schema.columns.iter().any(|column| column.name == stripped) {
            return Some(stripped.to_string());
        }
    }

    schema
        .columns
        .iter()
        .find(|column| format!("{}.{}", table_ref, column.name) == prefixed)
        .map(|column| column.name.clone())
}

fn expr_to_pushdown_predicate(expr: &Expr) -> Option<Predicate> {
    match expr {
        Expr::Eq(lhs, rhs) => column_literal_predicate(lhs, rhs, Predicate::Equals),
        Expr::Gt(lhs, rhs) => column_literal_predicate(lhs, rhs, Predicate::GreaterThan),
        Expr::Lt(lhs, rhs) => column_literal_predicate(lhs, rhs, Predicate::LessThan),
        Expr::And(lhs, rhs) => Some(Predicate::And(vec![
            expr_to_pushdown_predicate(lhs)?,
            expr_to_pushdown_predicate(rhs)?,
        ])),
        _ => None,
    }
}

fn column_literal_predicate(
    lhs: &Expr,
    rhs: &Expr,
    ctor: fn(String, storage::column::ColumnValue) -> Predicate,
) -> Option<Predicate> {
    match (lhs, rhs) {
        (Expr::Column(name), Expr::Literal(literal)) => Some(ctor(name.clone(), literal_to_value(literal)?)),
        _ => None,
    }
}

fn literal_to_value(literal: &crate::predicate::Literal) -> Option<storage::column::ColumnValue> {
    match literal {
        crate::predicate::Literal::Utf8(value) => Some(storage::column::ColumnValue::Utf8(value.clone())),
        crate::predicate::Literal::I32(value) => Some(storage::column::ColumnValue::I32(*value)),
        crate::predicate::Literal::U32(value) => Some(storage::column::ColumnValue::U32(*value)),
        crate::predicate::Literal::Bool(value) => Some(storage::column::ColumnValue::Bool(*value)),
    }
}

fn split_required_for_side(required: &BTreeSet<String>, prefix: &str) -> BTreeSet<String> {
    required
        .iter()
        .filter_map(|column| column.strip_prefix(&format!("{}.", prefix)).map(str::to_string))
        .collect()
}

trait PlanSchema {
    fn schema(&self) -> TableSchema;
}

impl PlanSchema for LogicalPlan {
    fn schema(&self) -> TableSchema {
        match self {
            LogicalPlan::Scan { table: Some(table), .. } => table.schema.clone(),
            LogicalPlan::Join { output_schema: Some(schema), .. } => schema.clone(),
            _ => TableSchema { columns: Vec::new() },
        }
    }
}

fn require_column<'a>(
    schema: &'a TableSchema,
    column: &str,
) -> Result<&'a ColumnSchema, AdolapError> {
    schema
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| AdolapError::ExecutionError(format!("Unknown column: {}", column)))
}

#[cfg(test)]
mod tests {
    use super::{canonicalize_column_name, derive_scan_projection};
    use std::collections::BTreeSet;
    use storage::schema::{ColumnSchema, ColumnType, TableSchema};

    fn qualified_schema() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnSchema { name: "events.id".into(), column_type: ColumnType::U32, nullable: false },
                ColumnSchema { name: "events.country".into(), column_type: ColumnType::Utf8, nullable: false },
            ],
        }
    }

    #[test]
    fn canonicalizes_unique_unqualified_column_names() {
        assert_eq!(canonicalize_column_name(&qualified_schema(), "country").unwrap(), "events.country");
    }

    #[test]
    fn rejects_ambiguous_column_names() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema { name: "left.id".into(), column_type: ColumnType::U32, nullable: false },
                ColumnSchema { name: "right.id".into(), column_type: ColumnType::U32, nullable: false },
            ],
        };

        assert!(canonicalize_column_name(&schema, "id").is_err());
    }

    #[test]
    fn derives_scan_projection_from_required_prefixed_columns() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false },
                ColumnSchema { name: "country".into(), column_type: ColumnType::Utf8, nullable: false },
            ],
        };
        let required = BTreeSet::from(["events.country".to_string()]);

        assert_eq!(derive_scan_projection("events", &schema, &required), Some(vec!["country".into()]));
    }
}

