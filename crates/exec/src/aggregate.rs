use core::error::AdolapError;

use storage::{column::ColumnValuesOwned, null::is_null, record_batch::RecordBatch};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub enum AggResult {
    I64(i64),
    F64(f64),
}

#[derive(Debug, Clone, Default)]
pub struct AggState {
    pub sum: i64,
    pub count: i64,
    pub min: Option<i64>,
    pub max: Option<i64>,
}

impl AggState {
    pub fn update_i64(&mut self, v: i64) {
        self.sum += v;
        self.count += 1;
        self.min = Some(self.min.map_or(v, |m| m.min(v)));
        self.max = Some(self.max.map_or(v, |m| m.max(v)));
    }

    pub fn to_result(&self, func: &AggFunc) -> Result<AggResult, AdolapError> {
        match func {
            AggFunc::Count => Ok(AggResult::I64(self.count)),
            AggFunc::Sum => Ok(AggResult::I64(self.sum)),
            AggFunc::Avg => {
                if self.count == 0 {
                    Ok(AggResult::F64(0.0))
                } else {
                    Ok(AggResult::F64(self.sum as f64 / self.count as f64))
                }
            }
            AggFunc::Min => self
                .min
                .map(AggResult::I64)
                .ok_or_else(|| AdolapError::ExecutionError("MIN on empty input".into())),
            AggFunc::Max => self
                .max
                .map(AggResult::I64)
                .ok_or_else(|| AdolapError::ExecutionError("MAX on empty input".into())),
        }
    }
}

pub fn aggregate_column_state(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<AggState, AdolapError> {
    let idx = batch
        .column_index(column_name)
        .ok_or_else(|| AdolapError::ExecutionError(format!("Unknown column: {}", column_name)))?;

    let col = &batch.columns[idx];
    let mut state = AggState::default();

    match &col.values {
        ColumnValuesOwned::I32(values) => {
            for (row_index, v) in values.iter().enumerate() {
                if !is_null(col.validity.as_deref(), row_index) {
                    state.update_i64(*v as i64);
                }
            }
        }
        ColumnValuesOwned::U32(values) => {
            for (row_index, v) in values.iter().enumerate() {
                if !is_null(col.validity.as_deref(), row_index) {
                    state.update_i64(*v as i64);
                }
            }
        }
        _ => {
            return Err(AdolapError::ExecutionError(
                "aggregate_column_state not implemented for this column type".into(),
            ))
        }
    }

    Ok(state)
}

pub fn sum(col: &str) -> (AggFunc, String) {
    (AggFunc::Sum, col.to_string())
}

pub fn avg(col: &str) -> (AggFunc, String) {
    (AggFunc::Avg, col.to_string())
}

pub fn min(col: &str) -> (AggFunc, String) {
    (AggFunc::Min, col.to_string())
}

pub fn max(col: &str) -> (AggFunc, String) {
    (AggFunc::Max, col.to_string())
}

pub fn count(col: &str) -> (AggFunc, String) {
    (AggFunc::Count, col.to_string())
}

pub fn agg_output_name(func: &AggFunc, col: &str) -> String {
    let prefix = match func {
        AggFunc::Count => "count",
        AggFunc::Sum => "sum",
        AggFunc::Avg => "avg",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
    };
    format!("{}_{}", prefix, col)
}

