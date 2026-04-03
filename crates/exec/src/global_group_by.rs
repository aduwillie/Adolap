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
