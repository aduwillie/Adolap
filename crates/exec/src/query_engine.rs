use core::error::AdolapError;

use storage::{record_batch::RecordBatch, table_reader::TableReader};

use crate::{
  filter::filter, 
  predicate::Expr, 
  projection::project,
};

pub struct QueryEngine<'a> {
    pub table_reader: &'a TableReader<'a>,
}

impl<'a> QueryEngine<'a> {
    pub fn new(table_reader: &'a TableReader) -> Self {
        Self { table_reader }
    }

    pub async fn execute(
        &self,
        predicate: Option<&Expr>,
        projection: Option<&[&str]>,
    ) -> Result<Vec<RecordBatch>, AdolapError> {
        // 1. Scan table (SegmentReader + RowGroupReader)
        let batches = self.table_reader.read_table(None, None).await?;
        let mut out = Vec::new();

        for batch in batches.into_iter() {
            let mut current = batch;

            // 2. Apply predicate (vectorized)
            if let Some(expr) = predicate {
                let mask = expr.evaluate_to_bool_mask(&current)?;
                current = filter(&current, &mask)?;
            }

            // 3. Apply projection
            if let Some(cols) = projection {
                current = project(&current, cols)?;
            }

            out.push(current);
        }

        Ok(out)
    }
}
