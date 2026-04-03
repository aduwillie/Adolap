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

#[cfg(test)]
mod tests {
 use super::*;

 #[test]
 fn test_table_id_display() {
   let table_id = TableId { value: 42 };
   assert_eq!(table_id.to_string(), "table-42");
 }

  #[test]
  fn test_part_id_display() {
    let part_id = PartId { value: 7 };
    assert_eq!(part_id.to_string(), "part-7");
}

  #[test]
  fn test_column_id_display() {
    let column_id = ColumnId { value: 3 };
    assert_eq!(column_id.to_string(), "col-3");
  }
}
