//! Group-by helpers used by the physical executor.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use crate::aggregate::{AggFunc, AggResult, AggState};
use core::error::AdolapError;
use storage::{column::ColumnValuesOwned, null::is_null};
use storage::record_batch::RecordBatch;

/// Materialized grouped aggregation output.
#[derive(Debug, Clone)]
pub struct GroupByResult {
    pub keys: Vec<GroupKey>,
    pub aggregates: Vec<AggResult>,
}

/// Stable hashable representation of a row's grouping key.
#[derive(Debug, Clone, Eq)]
pub struct GroupKey {
    pub values: Vec<String>,
}

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in &self.values {
            v.hash(state);
        }
    }
}

/// Group a batch by one or more key columns and aggregate a numeric column.
///
/// Example:
/// ```text
/// group_by_multi_key(batch, &["region", "city"], "sales", AggFunc::Sum)
/// ```
pub fn group_by_multi_key(
    batch: &RecordBatch,
    key_columns: &[&str],
    agg_column: &str,
    agg_func: AggFunc,
) -> Result<GroupByResult, AdolapError> {
    let mut key_indices = Vec::with_capacity(key_columns.len());
    for &name in key_columns {
        let idx = batch
            .column_index(name)
            .ok_or_else(|| AdolapError::ExecutionError(format!("Unknown key column: {}", name)))?;
        key_indices.push(idx);
    }

    let agg_idx = batch
        .column_index(agg_column)
        .ok_or_else(|| AdolapError::ExecutionError(format!("Unknown agg column: {}", agg_column)))?;

    let agg_col = &batch.columns[agg_idx];

    let mut map: HashMap<GroupKey, AggState> = HashMap::new();

    match &agg_col.values {
        ColumnValuesOwned::I32(values) => {
            for row in 0..batch.row_count {
                update_group(values[row] as i64, row, agg_col, batch, &key_indices, &mut map);
            }
        }
        ColumnValuesOwned::U32(values) => {
            for row in 0..batch.row_count {
                update_group(values[row] as i64, row, agg_col, batch, &key_indices, &mut map);
            }
        }
        _ => {
            return Err(AdolapError::ExecutionError(
                "group_by_multi_key not implemented for this agg column type".into(),
            ))
        }
    }

    let mut keys = Vec::with_capacity(map.len());
    let mut aggs = Vec::with_capacity(map.len());

    for (k, state) in map.into_iter() {
        keys.push(k);
        aggs.push(state.to_result(&agg_func)?);
    }

    Ok(GroupByResult { keys, aggregates: aggs })
}

fn update_group(
    value: i64,
    row: usize,
    agg_col: &storage::column::ColumnInputOwned,
    batch: &RecordBatch,
    key_indices: &[usize],
    map: &mut HashMap<GroupKey, AggState>,
) {
    if is_null(agg_col.validity.as_deref(), row) {
        return;
    }

    let key = GroupKey {
        values: key_indices
            .iter()
            .map(|index| key_value(&batch.columns[*index], row))
            .collect(),
    };
    map.entry(key).or_default().update_i64(value);
}

fn key_value(column: &storage::column::ColumnInputOwned, row: usize) -> String {
    if is_null(column.validity.as_deref(), row) {
        return "NULL".to_string();
    }

    match &column.values {
        ColumnValuesOwned::Utf8(values) => values[row].clone(),
        ColumnValuesOwned::I32(values) => values[row].to_string(),
        ColumnValuesOwned::U32(values) => values[row].to_string(),
        ColumnValuesOwned::Bool(values) => values[row].to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::group_by_multi_key;
    use crate::aggregate::{AggFunc, AggResult};
    use storage::{
        column::ColumnValue,
        record_batch::RecordBatch,
        schema::{ColumnSchema, ColumnType, TableSchema},
    };

    fn sample_batch() -> RecordBatch {
        RecordBatch::from_rows(
            TableSchema {
                columns: vec![
                    ColumnSchema {
                        name: "country".into(),
                        column_type: ColumnType::Utf8,
                        nullable: true,
                    },
                    ColumnSchema {
                        name: "value".into(),
                        column_type: ColumnType::I32,
                        nullable: true,
                    },
                ],
            },
            &[
                vec![Some(ColumnValue::Utf8("US".into())), Some(ColumnValue::I32(1))],
                vec![None, Some(ColumnValue::I32(2))],
            ],
        )
        .unwrap()
    }

    #[test]
    fn groups_rows_with_null_keys() {
        let result = group_by_multi_key(&sample_batch(), &["country"], "value", AggFunc::Sum).unwrap();
        let mut grouped = result
            .keys
            .into_iter()
            .zip(result.aggregates)
            .map(|(key, aggregate)| (key.values, aggregate))
            .collect::<Vec<_>>();
        grouped.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].0, vec!["NULL"]);
        assert_eq!(grouped[1].0, vec!["US"]);
        match grouped[0].1 {
            AggResult::I64(value) => assert_eq!(value, 2),
            ref other => panic!("unexpected aggregate result: {:?}", other),
        }
    }
}
