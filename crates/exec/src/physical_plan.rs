use crate::aggregate::AggFunc;
use crate::logical_plan::OrderBy;
use crate::predicate::Expr;
use storage::{catalog::TableMetadata, schema::TableSchema, segment_reader::Predicate};

#[derive(Debug)]
pub enum PhysicalPlan<'a> {
    Scan {
        table: &'a TableMetadata,
        projected_columns: Option<Vec<String>>,
        predicate: Option<Predicate>,
    },
    Filter {
        input: Box<PhysicalPlan<'a>>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan<'a>>,
        columns: Vec<String>,
    },
    HashAggregate {
        input: Box<PhysicalPlan<'a>>,
        group_keys: Vec<String>,
        agg_column: String,
        agg_func: AggFunc,
    },
    GroupFilter {
        input: Box<PhysicalPlan<'a>>,
        predicate: Expr,
    },
    HashJoin {
        left: Box<PhysicalPlan<'a>>,
        right: Box<PhysicalPlan<'a>>,
        left_on: String,
        right_on: String,
        output_schema: TableSchema,
    },
    Sort {
        input: Box<PhysicalPlan<'a>>,
        order_by: Vec<OrderBy>,
    },
    Limit {
        input: Box<PhysicalPlan<'a>>,
        limit: Option<usize>,
        offset: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::PhysicalPlan;
    use crate::aggregate::AggFunc;
    use storage::{
        catalog::TableMetadata,
        column::ColumnValue,
        config::TableStorageConfig,
        schema::{ColumnSchema, ColumnType, TableSchema},
        segment_reader::Predicate,
    };
    use std::path::PathBuf;

    fn sample_table() -> TableMetadata {
        TableMetadata {
            database: "default".into(),
            name: "events".into(),
            path: PathBuf::from("data/default/events"),
            schema: TableSchema {
                columns: vec![ColumnSchema {
                    name: "value".into(),
                    column_type: ColumnType::I32,
                    nullable: false,
                }],
            },
            storage_config: TableStorageConfig::default(),
        }
    }

    #[test]
    fn constructs_nested_scan_and_limit_plan() {
        let table = sample_table();
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::HashAggregate {
                input: Box::new(PhysicalPlan::Scan {
                    table: &table,
                    projected_columns: Some(vec!["value".into()]),
                    predicate: Some(Predicate::GreaterThan("value".into(), ColumnValue::I32(10))),
                }),
                group_keys: Vec::new(),
                agg_column: "value".into(),
                agg_func: AggFunc::Sum,
            }),
            limit: Some(1),
            offset: 2,
        };

        match plan {
            PhysicalPlan::Limit { input, limit, offset } => {
                assert_eq!(limit, Some(1));
                assert_eq!(offset, 2);
                assert!(matches!(*input, PhysicalPlan::HashAggregate { .. }));
            }
            other => panic!("unexpected plan: {:?}", other),
        }
    }
}
