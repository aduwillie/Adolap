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

#[cfg(test)]
mod tests {
    use super::global_aggregate;
    use crate::aggregate::{AggFunc, AggResult};
    use storage::{
        column::ColumnValue,
        record_batch::RecordBatch,
        schema::{ColumnSchema, ColumnType, TableSchema},
    };

    fn batch(values: &[Option<i32>]) -> RecordBatch {
        RecordBatch::from_rows(
            TableSchema {
                columns: vec![ColumnSchema {
                    name: "value".into(),
                    column_type: ColumnType::I32,
                    nullable: true,
                }],
            },
            &values
                .iter()
                .map(|value| vec![value.map(ColumnValue::I32)])
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    #[test]
    fn aggregates_across_batches() {
        match global_aggregate(&[batch(&[Some(1), None]), batch(&[Some(3)])], "value", AggFunc::Sum).unwrap() {
            AggResult::I64(value) => assert_eq!(value, 4),
            other => panic!("unexpected aggregate result: {:?}", other),
        }
    }
}
