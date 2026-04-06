//! Physical plan executor.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use crate::{
    aggregate::{agg_output_name, AggFunc, AggState},
    filter::filter,
    projection::project,
    logical_plan::{OrderBy, OrderDirection},
    physical_plan::PhysicalPlan,
};
use core::error::AdolapError;
use storage::{
    column::ColumnValue,
    record_batch::RecordBatch,
    schema::{ColumnSchema, ColumnType, TableSchema},
    table_reader::TableReader,
};
use tracing::{debug, info};

pub struct Executor;

impl Executor {
    /// Construct a stateless executor.
    pub fn new() -> Self {
        Self
    }

    /// Execute a physical plan into one or more record batches.
    pub fn execute<'b>(
        &'b self,
        plan: &'b PhysicalPlan<'b>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, AdolapError>> + Send + 'b>> {
        Box::pin(async move {
            match plan {
            PhysicalPlan::Scan { table, projected_columns, predicate } => {
                debug!(table = %table.path.display(), projection = ?projected_columns, has_predicate = predicate.is_some(), "executing scan");
                let table_reader = TableReader::new(&table.path, &table.schema);
                let projected_columns = projected_columns.clone();
                let predicate = predicate.as_ref();
                table_reader.read_table(predicate, projected_columns).await
            }
            PhysicalPlan::Filter { input, predicate } => {
                debug!(?predicate, "executing filter");
                let batches = self.execute(input).await?;
                filter_batches(&batches, predicate)
            }
            PhysicalPlan::Project { input, columns } => {
                debug!(columns = ?columns, "executing projection");
                let batches = self.execute(input).await?;
                let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                project_batches(&batches, &cols)
            }
            PhysicalPlan::HashAggregate {
                input,
                group_keys,
                agg_column,
                agg_func,
            } => {
                info!(group_keys = ?group_keys, agg_column = %agg_column, agg_func = ?agg_func, "executing hash aggregate");
                let batches = self.execute(input).await?;
                if group_keys.is_empty() {
                    Ok(vec![materialize_global_aggregate(&batches, agg_column, agg_func)?])
                } else {
                    Ok(vec![materialize_grouped_aggregate(&batches, group_keys, agg_column, agg_func)?])
                }
            }
            PhysicalPlan::GroupFilter { input, predicate } => {
                debug!(?predicate, "executing group filter");
                let batches = self.execute(input).await?;
                filter_batches(&batches, predicate)
            }
            PhysicalPlan::HashJoin { left, right, left_on, right_on, output_schema } => {
                info!(left_on = %left_on, right_on = %right_on, output_columns = output_schema.columns.len(), "executing hash join");
                let left_batches = self.execute(left).await?;
                let right_batches = self.execute(right).await?;
                hash_join_batches(&left_batches, &right_batches, left_on, right_on, output_schema)
            }
            PhysicalPlan::Sort { input, order_by } => {
                debug!(terms = order_by.len(), "executing sort");
                let batches = self.execute(input).await?;
                sort_batches(batches, order_by)
            }
            PhysicalPlan::Limit { input, limit, offset } => {
                debug!(?limit, offset, "executing limit");
                let batches = self.execute(input).await?;
                apply_limit(batches, *limit, *offset)
            }
        }
        })
    }
}

fn filter_batches(
    batches: &[RecordBatch],
    predicate: &crate::predicate::Expr,
) -> Result<Vec<RecordBatch>, AdolapError> {
    batches
        .iter()
        .map(|batch| {
            let mask = predicate.evaluate_to_bool_mask(batch)?;
            filter(batch, &mask)
        })
        .collect()
}

fn project_batches(batches: &[RecordBatch], columns: &[&str]) -> Result<Vec<RecordBatch>, AdolapError> {
    batches.iter().map(|batch| project(batch, columns)).collect()
}

fn apply_limit(
    batches: Vec<RecordBatch>,
    limit: Option<usize>,
    offset: usize,
) -> Result<Vec<RecordBatch>, AdolapError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema.clone();
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(batch.to_rows()?);
    }

    let rows = rows.into_iter().skip(offset);
    let rows = match limit {
        Some(limit) => rows.take(limit).collect::<Vec<_>>(),
        None => rows.collect::<Vec<_>>(),
    };

    Ok(vec![RecordBatch::from_rows(schema, &rows)?])
}

fn hash_join_batches(
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    left_on: &str,
    right_on: &str,
    output_schema: &TableSchema,
) -> Result<Vec<RecordBatch>, AdolapError> {
    let mut left_rows = Vec::new();
    let mut right_rows = Vec::new();
    for batch in left_batches {
        left_rows.extend(batch.to_rows()?);
    }
    for batch in right_batches {
        right_rows.extend(batch.to_rows()?);
    }

    if left_batches.is_empty() || right_batches.is_empty() {
        return Ok(vec![RecordBatch::from_rows(output_schema.clone(), &[])?]);
    }

    let left_schema = &left_batches[0].schema;
    let right_schema = &right_batches[0].schema;
    let left_index = left_schema.columns.iter().position(|column| column.name == left_on)
        .ok_or_else(|| AdolapError::ExecutionError(format!("Unknown join column: {}", left_on)))?;
    let right_index = right_schema.columns.iter().position(|column| column.name == right_on)
        .ok_or_else(|| AdolapError::ExecutionError(format!("Unknown join column: {}", right_on)))?;

    let mut right_map: HashMap<Option<ColumnValue>, Vec<Vec<Option<ColumnValue>>>> = HashMap::new();
    for row in right_rows {
        right_map.entry(row[right_index].clone()).or_default().push(row);
    }

    let mut joined_rows = Vec::new();
    for left_row in left_rows {
        if let Some(matches) = right_map.get(&left_row[left_index]) {
            for right_row in matches {
                let mut row = left_row.clone();
                row.extend(right_row.clone());
                joined_rows.push(row);
            }
        }
    }

    Ok(vec![RecordBatch::from_rows(output_schema.clone(), &joined_rows)?])
}

fn materialize_global_aggregate(
    batches: &[RecordBatch],
    agg_column: &str,
    agg_func: &AggFunc,
) -> Result<RecordBatch, AdolapError> {
    let mut state = AggState::default();
    for batch in batches {
        let partial = crate::aggregate::aggregate_column_state(batch, agg_column)?;
        state.sum += partial.sum;
        state.count += partial.count;
        state.min = match (state.min, partial.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (None, Some(b)) => Some(b),
            (value, None) => value,
        };
        state.max = match (state.max, partial.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (None, Some(b)) => Some(b),
            (value, None) => value,
        };
    }

    let row = vec![Some(ColumnValue::I32(finalize_agg(&state, agg_func)?))];
    RecordBatch::from_rows(
        TableSchema {
            columns: vec![ColumnSchema {
                name: agg_output_name(agg_func, agg_column),
                column_type: ColumnType::I32,
                nullable: false,
            }],
        },
        &[row],
    )
}

fn materialize_grouped_aggregate(
    batches: &[RecordBatch],
    group_keys: &[String],
    agg_column: &str,
    agg_func: &AggFunc,
) -> Result<RecordBatch, AdolapError> {
    let schema = batches
        .iter()
        .find(|batch| !batch.schema.columns.is_empty())
        .map(|batch| &batch.schema)
        .ok_or_else(|| AdolapError::ExecutionError("No batches available for aggregation".into()))?;

    let key_columns = group_keys
        .iter()
        .map(|key| {
            schema
                .columns
                .iter()
                .find(|column| column.name == *key)
                .cloned()
                .ok_or_else(|| AdolapError::ExecutionError(format!("Unknown key column: {}", key)))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut groups = std::collections::HashMap::<Vec<Option<ColumnValue>>, AggState>::new();
    for batch in batches {
        let key_indices = group_keys
            .iter()
            .map(|key| batch.column_index(key).ok_or_else(|| AdolapError::ExecutionError(format!("Unknown key column: {}", key))))
            .collect::<Result<Vec<_>, _>>()?;
        let agg_index = batch.column_index(agg_column).ok_or_else(|| AdolapError::ExecutionError(format!("Unknown agg column: {}", agg_column)))?;
        let rows = batch.to_rows()?;

        for row in rows {
            let key = key_indices.iter().map(|index| row[*index].clone()).collect::<Vec<_>>();
            let entry = groups.entry(key).or_default();
            if let Some(value) = &row[agg_index] {
                match value {
                    ColumnValue::I32(value) => entry.update_i64(*value as i64),
                    ColumnValue::U32(value) => entry.update_i64(*value as i64),
                    _ => {
                        return Err(AdolapError::ExecutionError(
                            "Grouped aggregation only supports I32 and U32 inputs".into(),
                        ))
                    }
                }
            }
        }
    }

    let mut rows = groups
        .into_iter()
        .map(|(mut key, state)| {
            key.push(Some(ColumnValue::I32(finalize_agg(&state, agg_func)?)));
            Ok(key)
        })
        .collect::<Result<Vec<Vec<Option<ColumnValue>>>, AdolapError>>()?;

    rows.sort_by(|left, right| compare_optional_values(&left[0], &right[0], OrderDirection::Asc));

    let mut columns = key_columns;
    columns.push(ColumnSchema {
        name: agg_output_name(agg_func, agg_column),
        column_type: ColumnType::I32,
        nullable: false,
    });

    RecordBatch::from_rows(TableSchema { columns }, &rows)
}

fn sort_batches(batches: Vec<RecordBatch>, order_by: &[OrderBy]) -> Result<Vec<RecordBatch>, AdolapError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema.clone();
    let mut rows = Vec::new();
    for batch in batches {
        rows.extend(batch.to_rows()?);
    }

    let order_indices = order_by
        .iter()
        .map(|order| match &order.expr {
            crate::predicate::Expr::Column(name) => schema
                .columns
                .iter()
                .position(|column| column.name == *name)
                .ok_or_else(|| AdolapError::ExecutionError(format!("Unknown ORDER BY column: {}", name))),
            _ => Err(AdolapError::ExecutionError(
                "ORDER BY expressions must be bound to columns".into(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;

    rows.sort_by(|left, right| {
        for (position, order) in order_by.iter().enumerate() {
            let ordering = compare_optional_values(
                &left[order_indices[position]],
                &right[order_indices[position]],
                order.direction,
            );
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });

    Ok(vec![RecordBatch::from_rows(schema, &rows)?])
}

fn finalize_agg(state: &AggState, agg_func: &AggFunc) -> Result<i32, AdolapError> {
    let value = match agg_func {
        AggFunc::Count => state.count,
        AggFunc::Sum => state.sum,
        AggFunc::Avg => {
            if state.count == 0 {
                0
            } else {
                state.sum / state.count
            }
        }
        AggFunc::Min => state.min.unwrap_or(0),
        AggFunc::Max => state.max.unwrap_or(0),
    };

    i32::try_from(value).map_err(|_| {
        AdolapError::ExecutionError("Aggregate result does not fit in I32".into())
    })
}

fn compare_optional_values(
    left: &Option<ColumnValue>,
    right: &Option<ColumnValue>,
    direction: OrderDirection,
) -> Ordering {
    let ordering = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(ColumnValue::Utf8(left)), Some(ColumnValue::Utf8(right))) => left.cmp(right),
        (Some(ColumnValue::I32(left)), Some(ColumnValue::I32(right))) => left.cmp(right),
        (Some(ColumnValue::U32(left)), Some(ColumnValue::U32(right))) => left.cmp(right),
        (Some(ColumnValue::Bool(left)), Some(ColumnValue::Bool(right))) => left.cmp(right),
        (Some(left), Some(right)) => format_value(left).cmp(&format_value(right)),
    };

    match direction {
        OrderDirection::Asc => ordering,
        OrderDirection::Desc => ordering.reverse(),
    }
}

fn format_value(value: &ColumnValue) -> String {
    match value {
        ColumnValue::Utf8(value) => value.clone(),
        ColumnValue::I32(value) => value.to_string(),
        ColumnValue::U32(value) => value.to_string(),
        ColumnValue::Bool(value) => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_limit, hash_join_batches, materialize_global_aggregate, sort_batches};
    use crate::{aggregate::AggFunc, logical_plan::{OrderBy, OrderDirection}, predicate::col};
    use storage::{
        catalog::TableMetadata,
        column::ColumnValue,
        config::TableStorageConfig,
        record_batch::RecordBatch,
        schema::{ColumnSchema, ColumnType, TableSchema},
    };
    use std::path::PathBuf;

    fn values_schema() -> TableSchema {
        TableSchema {
            columns: vec![ColumnSchema {
                name: "value".into(),
                column_type: ColumnType::I32,
                nullable: false,
            }],
        }
    }

    fn batch(values: &[i32]) -> RecordBatch {
        RecordBatch::from_rows(
            values_schema(),
            &values
                .iter()
                .map(|value| vec![Some(ColumnValue::I32(*value))])
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    #[test]
    fn apply_limit_merges_batches_with_offset() {
        let limited = apply_limit(vec![batch(&[1, 2]), batch(&[3])], Some(1), 1).unwrap();

        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].to_rows().unwrap(), vec![vec![Some(ColumnValue::I32(2))]]);
    }

    #[test]
    fn materializes_and_sorts_aggregates() {
        let aggregate = materialize_global_aggregate(&[batch(&[2, 4])], "value", &AggFunc::Sum).unwrap();
        assert_eq!(aggregate.to_rows().unwrap(), vec![vec![Some(ColumnValue::I32(6))]]);

        let sorted = sort_batches(
            vec![batch(&[2, 1, 3])],
            &[OrderBy {
                expr: col("value"),
                direction: OrderDirection::Desc,
            }],
        )
        .unwrap();
        assert_eq!(sorted[0].to_rows().unwrap(), vec![
            vec![Some(ColumnValue::I32(3))],
            vec![Some(ColumnValue::I32(2))],
            vec![Some(ColumnValue::I32(1))],
        ]);
    }

    #[test]
    fn hash_join_batches_matches_rows_on_join_key() {
        let left_schema = TableSchema {
            columns: vec![
                ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false },
                ColumnSchema { name: "left_name".into(), column_type: ColumnType::Utf8, nullable: false },
            ],
        };
        let right_schema = TableSchema {
            columns: vec![
                ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false },
                ColumnSchema { name: "right_name".into(), column_type: ColumnType::Utf8, nullable: false },
            ],
        };
        let left = RecordBatch::from_rows(
            left_schema.clone(),
            &[vec![Some(ColumnValue::U32(1)), Some(ColumnValue::Utf8("left".into()))]],
        )
        .unwrap();
        let right = RecordBatch::from_rows(
            right_schema.clone(),
            &[vec![Some(ColumnValue::U32(1)), Some(ColumnValue::Utf8("right".into()))]],
        )
        .unwrap();
        let output_schema = TableSchema {
            columns: left_schema.columns.into_iter().chain(right_schema.columns).collect(),
        };

        let joined = hash_join_batches(&[left], &[right], "id", "id", &output_schema).unwrap();
        assert_eq!(joined[0].to_rows().unwrap(), vec![vec![
            Some(ColumnValue::U32(1)),
            Some(ColumnValue::Utf8("left".into())),
            Some(ColumnValue::U32(1)),
            Some(ColumnValue::Utf8("right".into())),
        ]]);
    }

    #[test]
    fn sample_table_metadata_is_constructible() {
        let table = TableMetadata {
            database: "default".into(),
            name: "events".into(),
            path: PathBuf::from("data/default/events"),
            schema: values_schema(),
            storage_config: TableStorageConfig::default(),
        };

        assert_eq!(table.fqn(), "default.events");
    }
}
