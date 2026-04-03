//! Predicate expression model used by the planner and executor.

use core::error::AdolapError;

use crate::aggregate::AggFunc;
use storage::{column::{ColumnInputOwned, ColumnValuesOwned}, null::is_null, record_batch::RecordBatch};

/// Scalar literal values that can appear in expressions.
#[derive(Debug, Clone)]
pub enum Literal {
    Utf8(String),
    I32(i32),
    U32(u32),
    Bool(bool),
}

/// Expression tree used for filters and simple computed predicates.
#[derive(Debug, Clone)]
pub enum Expr {
    Column(String),
    Aggregate { func: AggFunc, column: String },
    Literal(Literal),
    Eq(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}

impl Expr {
    /// Evaluate the expression into a row-wise boolean mask.
    pub fn evaluate_to_bool_mask(&self, batch: &RecordBatch) -> Result<Vec<bool>, AdolapError> {
        match self {
            Expr::Eq(lhs, rhs) => compare_eq(lhs, rhs, batch),
            Expr::Gt(lhs, rhs) => compare_gt(lhs, rhs, batch),
            Expr::Lt(lhs, rhs) => compare_lt(lhs, rhs, batch),
            Expr::And(lhs, rhs) => and_mask(lhs, rhs, batch),
            Expr::Or(lhs, rhs) => or_mask(lhs, rhs, batch),
            _ => Err(AdolapError::ExecutionError("Expression not supported as boolean predicate".into())),
        }
    }

    pub fn eq(self, other: Expr) -> Self {
        Expr::Eq(Box::new(self), Box::new(other))
    }

    pub fn gt(self, other: Expr) -> Self {
        Expr::Gt(Box::new(self), Box::new(other))
    }

    pub fn lt(self, other: Expr) -> Self {
        Expr::Lt(Box::new(self), Box::new(other))
    }

    pub fn and(self, other: Expr) -> Self {
        Expr::And(Box::new(self), Box::new(other))
    }

    pub fn or(self, other: Expr) -> Self {
        Expr::Or(Box::new(self), Box::new(other))
    }
}

fn resolve_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a ColumnInputOwned, AdolapError> {
    let idx = batch
        .column_index(name)
        .ok_or_else(|| AdolapError::ExecutionError(format!("Unknown column: {}", name)))?;
    Ok(&batch.columns[idx])
}

// For now, implement Eq/Gt/Lt only for a few concrete type combos.
// You can extend this later.
fn compare_eq(lhs: &Expr, rhs: &Expr, batch: &RecordBatch) -> Result<Vec<bool>, AdolapError> {
    match (lhs, rhs) {
        (Expr::Column(name), Expr::Literal(Literal::I32(value))) => compare_i32_literal(batch, name, *value, |left, right| left == right, "Eq"),
        _ => Err(AdolapError::ExecutionError(
            "Eq not implemented for this expression combination".into(),
        )),
    }
}

fn compare_gt(lhs: &Expr, rhs: &Expr, batch: &RecordBatch) -> Result<Vec<bool>, AdolapError> {
    match (lhs, rhs) {
        (Expr::Column(name), Expr::Literal(Literal::I32(value))) => compare_i32_literal(batch, name, *value, |left, right| left > right, "Gt"),
        _ => Err(AdolapError::ExecutionError(
            "Gt not implemented for this expression combination".into(),
        )),
    }
}

fn compare_lt(lhs: &Expr, rhs: &Expr, batch: &RecordBatch) -> Result<Vec<bool>, AdolapError> {
    match (lhs, rhs) {
        (Expr::Column(name), Expr::Literal(Literal::I32(value))) => compare_i32_literal(batch, name, *value, |left, right| left < right, "Lt"),
        _ => Err(AdolapError::ExecutionError(
            "Lt not implemented for this expression combination".into(),
        )),
    }
}

fn and_mask(lhs: &Expr, rhs: &Expr, batch: &RecordBatch) -> Result<Vec<bool>, AdolapError> {
    let l = lhs.evaluate_to_bool_mask(batch)?;
    let r = rhs.evaluate_to_bool_mask(batch)?;
    Ok(l.iter().zip(r.iter()).map(|(a, b)| *a && *b).collect())
}

fn or_mask(lhs: &Expr, rhs: &Expr, batch: &RecordBatch) -> Result<Vec<bool>, AdolapError> {
    let l = lhs.evaluate_to_bool_mask(batch)?;
    let r = rhs.evaluate_to_bool_mask(batch)?;
    Ok(l.iter().zip(r.iter()).map(|(a, b)| *a || *b).collect())
}

fn compare_i32_literal(
    batch: &RecordBatch,
    name: &str,
    value: i32,
    compare: impl Fn(i32, i32) -> bool,
    operator_name: &str,
) -> Result<Vec<bool>, AdolapError> {
    let col = resolve_column(batch, name)?;
    if let ColumnValuesOwned::I32(values) = &col.values {
        Ok(values
            .iter()
            .enumerate()
            .map(|(idx, current)| !is_null(col.validity.as_deref(), idx) && compare(*current, value))
            .collect())
    } else {
        Err(AdolapError::ExecutionError(format!("Type mismatch in {}", operator_name)))
    }
}

pub fn col(name: &str) -> Expr {
    Expr::Column(name.to_string())
}

pub fn lit_i32(v: i32) -> Expr {
    Expr::Literal(Literal::I32(v))
}
