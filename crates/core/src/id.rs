use std::fmt;
use serde::{Serialize, Deserialize};

// Identifer for tables
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId {
  pub value: u64,
}

impl fmt::Display for TableId {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "table-{}", self.value)
  }
}

// Identifier for a part (storage unit for a subset of rows in a table)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartId {
  pub value: u64,
}

impl fmt::Display for PartId {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "part-{}", self.value)
  }
}

// Identifier for a column in a table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnId {
  pub value: u64,
}

impl fmt::Display for ColumnId {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "col-{}", self.value)
  }
}
