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

#[cfg(test)]
mod tests {
    use super::QueryEngine;
    use crate::predicate::{col, lit_i32};
    use storage::{
        column::ColumnValue,
        schema::{ColumnSchema, ColumnType, TableSchema},
        table_reader::TableReader,
        table_writer::TableWriter,
    };
    use tempfile::tempdir;
    use tokio::runtime::Runtime;

    fn sample_schema() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnSchema { name: "id".into(), column_type: ColumnType::U32, nullable: false },
                ColumnSchema { name: "value".into(), column_type: ColumnType::I32, nullable: false },
            ],
        }
    }

    #[test]
    fn executes_predicate_and_projection_pipeline() {
        Runtime::new().unwrap().block_on(async {
            let temp_dir = tempdir().unwrap();
            let schema = sample_schema();
            let writer = TableWriter::create_table(temp_dir.path(), "default", "events", &schema, &Default::default())
                .await
                .unwrap();

            writer
                .insert_rows(&[
                    vec![Some(ColumnValue::U32(1)), Some(ColumnValue::I32(10))],
                    vec![Some(ColumnValue::U32(2)), Some(ColumnValue::I32(20))],
                ])
                .await
                .unwrap();

            let reader = TableReader::new(&writer.table_dir, &schema);
            let engine = QueryEngine::new(&reader);
            let batches = engine
                .execute(Some(&col("value").gt(lit_i32(10))), Some(&["id"]))
                .await
                .unwrap();

            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].to_rows().unwrap(), vec![vec![Some(ColumnValue::U32(2))]]);
        });
    }
}
