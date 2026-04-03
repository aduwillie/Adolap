use storage::record_batch::RecordBatch;

use crate::aggregate::{aggregate_column_state, AggFunc, AggResult, AggState};
use core::error::AdolapError;

pub fn global_aggregate(
    batches: &[RecordBatch],
    column: &str,
    func: AggFunc,
) -> Result<AggResult, AdolapError> {
    let mut state = AggState::default();

    for batch in batches {
        let s = aggregate_column_state(batch, column)?;
        state.sum += s.sum;
        state.count += s.count;
        state.min = match (state.min, s.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (None, Some(b)) => Some(b),
            (a, None) => a,
        };
        state.max = match (state.max, s.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (None, Some(b)) => Some(b),
            (a, None) => a,
        };
    }

    state.to_result(&func)
}
