use std::collections::HashMap;

use storage::record_batch::RecordBatch;

use crate::group_by::{group_by_multi_key, GroupByResult, GroupKey};
use crate::aggregate::{AggFunc, AggResult, AggState};
use core::error::AdolapError;

pub fn global_group_by_multi_key(
    batches: &[RecordBatch],
    key_columns: &[&str],
    agg_column: &str,
    agg_func: AggFunc,
) -> Result<GroupByResult, AdolapError> {
    let mut map: HashMap<GroupKey, AggState> = HashMap::new();

    for batch in batches {
        let partial = group_by_multi_key(batch, key_columns, agg_column, agg_func.clone())?;

        for (i, key) in partial.keys.into_iter().enumerate() {
            let agg_res = &partial.aggregates[i];
            let entry = map.entry(key).or_insert_with(AggState::default);

            match agg_res {
                AggResult::I64(v) => entry.update_i64(*v),
                AggResult::F64(v) => entry.update_i64(*v as i64),
            }
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

#[cfg(test)]
mod tests {
    use super::global_group_by_multi_key;
    use crate::aggregate::{AggFunc, AggResult};
    use storage::{
        column::ColumnValue,
        record_batch::RecordBatch,
        schema::{ColumnSchema, ColumnType, TableSchema},
    };

    fn batch(rows: &[(Option<&str>, Option<i32>)]) -> RecordBatch {
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
            &rows
                .iter()
                .map(|(country, value)| {
                    vec![
                        country.map(|value| ColumnValue::Utf8(value.into())),
                        value.map(ColumnValue::I32),
                    ]
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    #[test]
    fn merges_grouped_results_from_multiple_batches() {
        let result = global_group_by_multi_key(
            &[batch(&[(Some("US"), Some(1))]), batch(&[(Some("US"), Some(2))])],
            &["country"],
            "value",
            AggFunc::Sum,
        )
        .unwrap();

        assert_eq!(result.keys.len(), 1);
        assert_eq!(result.keys[0].values, vec!["US"]);
        match result.aggregates[0] {
            AggResult::I64(value) => assert_eq!(value, 3),
            ref other => panic!("unexpected aggregate result: {:?}", other),
        }
    }
}
