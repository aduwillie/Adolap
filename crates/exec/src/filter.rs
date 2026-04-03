//! Vectorized row filtering helpers.

use core::error::AdolapError;
use storage::{column::{ColumnInputOwned, ColumnValuesOwned}, null::is_null, record_batch::RecordBatch};

/// Apply a boolean keep-mask to every column in a batch.
///
/// Example:
/// ```text
/// let filtered = filter(&batch, &[true, false, true])?;
/// ```
pub fn filter(batch: &RecordBatch, mask: &[bool]) -> Result<RecordBatch, AdolapError> {
    if mask.len() != batch.row_count {
        return Err(AdolapError::ExecutionError(
            "Filter mask length does not match row count".into(),
        ));
    }

    let mut filtered_columns = Vec::with_capacity(batch.columns.len());

    for col in &batch.columns {
        let filtered_values = match &col.values {
            ColumnValuesOwned::I32(values) => ColumnValuesOwned::I32(filter_copy_values(values, mask)),
            ColumnValuesOwned::U32(values) => ColumnValuesOwned::U32(filter_copy_values(values, mask)),
            ColumnValuesOwned::Bool(values) => ColumnValuesOwned::Bool(filter_copy_values(values, mask)),
            ColumnValuesOwned::Utf8(values) => ColumnValuesOwned::Utf8(filter_clone_values(values, mask)),
        };

        let filtered_validity = rebuild_validity(col.validity.as_deref(), mask);

        filtered_columns.push(ColumnInputOwned {
            values: filtered_values,
            validity: filtered_validity.filter(|bits| !bits.is_empty()),
        });
    }

    Ok(RecordBatch {
        schema: batch.schema.clone(),
        row_count: mask.iter().filter(|b| **b).count(),
        columns: filtered_columns,
    })
}

fn filter_copy_values<T: Copy>(values: &[T], mask: &[bool]) -> Vec<T> {
    values
        .iter()
        .zip(mask.iter())
        .filter_map(|(value, keep)| keep.then_some(*value))
        .collect()
}

fn filter_clone_values<T: Clone>(values: &[T], mask: &[bool]) -> Vec<T> {
    values
        .iter()
        .zip(mask.iter())
        .filter_map(|(value, keep)| keep.then_some(value.clone()))
        .collect()
}

fn rebuild_validity(validity: Option<&[u8]>, mask: &[bool]) -> Option<Vec<u8>> {
    validity.map(|bits| {
        let mut kept = Vec::with_capacity(mask.iter().filter(|keep| **keep).count().div_ceil(8));
        let mut current = 0u8;
        let mut bit_count = 0usize;

        for (row_index, keep) in mask.iter().enumerate() {
            if !keep {
                continue;
            }

            if !is_null(Some(bits), row_index) {
                current |= 1 << (bit_count % 8);
            }

            bit_count += 1;
            if bit_count % 8 == 0 {
                kept.push(current);
                current = 0;
            }
        }

        if bit_count % 8 != 0 {
            kept.push(current);
        }

        kept
    })
}
