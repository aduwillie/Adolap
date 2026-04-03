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
