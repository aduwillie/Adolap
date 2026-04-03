use core::error::AdolapError;
use crate::null::{count_nulls, is_null};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Statistics about a column's values, used for pruning and optimizing reads.
/// This is agnostic of Rust types and is just a byte representation of the min value for now.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
    pub null_count: u32,
    pub distinct_count: u32,
}

impl ColumnStats {
    pub fn empty() -> Self {
        Self {
            min: None,
            max: None,
            null_count: 0,
            distinct_count: 0,
        }
    }
}

fn serialize_stats_value<T: Serialize>(value: &T) -> Result<Vec<u8>, AdolapError> {
    postcard::to_allocvec(value)
        .map_err(|e| AdolapError::Serialization(format!("Cannot serialize stats value: {}", e)))
}

pub fn compute_utf8_column_stats(values: &[String], validity: Option<&[u8]>) -> Result<ColumnStats, AdolapError> {
    if values.is_empty() {
        return Ok(ColumnStats::empty());
    }

    let mut min: Option<&String> = None;
    let mut max: Option<&String> = None;
    let mut distinct = HashSet::new();

    for (i, value) in values.iter().enumerate() {
        if is_null(validity, i) {
            continue;
        }

        distinct.insert(value.clone());

        min = Some(match min {
            None => value,
            Some(current_min) => if value < current_min { value } else { current_min },
        });
        max = Some(match max {
            None => value,
            Some(current_max) => if value > current_max { value } else { current_max },
        });
    }

    if min.is_none() || max.is_none() {
        return Ok(ColumnStats::empty());
    }

    Ok(ColumnStats {
        min: Some(serialize_stats_value(min.unwrap())?),
        max: Some(serialize_stats_value(max.unwrap())?),
        null_count: count_nulls(validity.unwrap_or(&[]), values.len()) as u32,
        distinct_count: distinct.len() as u32,
    })
}

pub fn compute_i32_column_stats(values: &[i32], validity: Option<&[u8]>) -> Result<ColumnStats, AdolapError> {
    if values.is_empty() {
        return Ok(ColumnStats::empty());
    }

    let mut min: Option<i32> = None;
    let mut max: Option<i32> = None;
    let mut distinct = HashSet::new();

    for (i, &v) in values.iter().enumerate() {
        if is_null(validity, i) {
            continue;
        }

        distinct.insert(v);

        min = Some(min.map_or(v, |current_min| current_min.min(v)));
        max = Some(max.map_or(v, |current_max| current_max.max(v)));
    }

    if min.is_none() || max.is_none() {
        return Ok(ColumnStats::empty());
    }

    Ok(ColumnStats {
        min: Some(serialize_stats_value(&min.unwrap())?),
        max: Some(serialize_stats_value(&max.unwrap())?),
        null_count: count_nulls(validity.unwrap_or(&[]), values.len()) as u32,
        distinct_count: distinct.len() as u32,
    })
}

pub fn compute_u32_column_stats(values: &[u32], validity: Option<&[u8]>) -> Result<ColumnStats, AdolapError> {
    if values.is_empty() {
        return Ok(ColumnStats::empty());
    }

    let mut min: Option<u32> = None;
    let mut max: Option<u32> = None;
    let mut distinct = HashSet::new();

    for (i, &v) in values.iter().enumerate() {
        if is_null(validity, i) {
            continue;
        }

        distinct.insert(v);

        min = Some(min.map_or(v, |current_min| current_min.min(v)));
        max = Some(max.map_or(v, |current_max| current_max.max(v)));
    }

    if min.is_none() || max.is_none() {
        return Ok(ColumnStats::empty());
    }

    Ok(ColumnStats {
        min: Some(serialize_stats_value(&min.unwrap())?),
        max: Some(serialize_stats_value(&max.unwrap())?),
        null_count: count_nulls(validity.unwrap_or(&[]), values.len()) as u32,
        distinct_count: distinct.len() as u32,
    })
}

pub fn compute_bool_column_stats(values: &[bool], validity: Option<&[u8]>) -> Result<ColumnStats, AdolapError> {
    if values.is_empty() {
        return Ok(ColumnStats::empty());
    }

    let mut min: Option<bool> = None;
    let mut max: Option<bool> = None;
    let mut distinct = HashSet::new();

    for (i, &v) in values.iter().enumerate() {
        if is_null(validity, i) {
            continue;
        }

        distinct.insert(v);

        min = Some(match min {
            None => v,
            Some(current_min) => if v < current_min { v } else { current_min },
        });
        max = Some(match max {
            None => v,
            Some(current_max) => if v > current_max { v } else { current_max },
        });
    }

    if min.is_none() || max.is_none() {
        return Ok(ColumnStats::empty());
    }

    Ok(ColumnStats {
        min: Some(serialize_stats_value(&min.unwrap())?),
        max: Some(serialize_stats_value(&max.unwrap())?),
        null_count: count_nulls(validity.unwrap_or(&[]), values.len()) as u32,
        distinct_count: distinct.len() as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::{compute_i32_column_stats, compute_utf8_column_stats};

    #[test]
    fn computes_distinct_count_for_non_null_i32_values() {
        let stats = compute_i32_column_stats(&[1, 2, 2, 3], None).unwrap();
        assert_eq!(stats.distinct_count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn excludes_nulls_from_distinct_count() {
        let validity = [0b0000_0101];
        let stats = compute_utf8_column_stats(
            &["US".to_string(), "CA".to_string(), "US".to_string()],
            Some(&validity),
        )
        .unwrap();

        assert_eq!(stats.distinct_count, 1);
        assert_eq!(stats.null_count, 1);
    }
}

pub trait StatsComputable: serde::Serialize + PartialOrd {}

impl StatsComputable for String {}
impl StatsComputable for i32 {}
impl StatsComputable for i64 {}
impl StatsComputable for u32 {}
impl StatsComputable for u64 {}
