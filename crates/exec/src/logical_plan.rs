use crate::predicate::Expr;
use storage::catalog::TableMetadata;
use storage::schema::TableSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub expr: Expr,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone)]
pub struct JoinSpec {
    pub right_table_ref: String,
    pub left_on: String,
    pub right_on: String,
}

#[derive(Debug, Clone)]
pub struct LimitSpec {
    pub limit: Option<usize>,
    pub offset: usize,
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table_ref: String,
        table: Option<TableMetadata>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_keys: Vec<String>,
        agg_column: String,
        agg_func: crate::aggregate::AggFunc,
    },
    GroupFilter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_on: String,
        right_on: String,
        output_schema: Option<TableSchema>,
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderBy>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<usize>,
        offset: usize,
    },
}

impl LogicalPlan {
    pub fn scan(table: &str) -> Self {
        LogicalPlan::Scan {
            table_ref: table.to_string(),
            table: None,
        }
    }

    pub fn filter(self, predicate: Expr) -> Self {
        LogicalPlan::Filter { input: Box::new(self), predicate }
    }

    pub fn project<S: Into<String>>(self, columns: Vec<S>) -> Self {
        LogicalPlan::Project { 
            input: Box::new(self), 
            columns: columns.into_iter().map(Into::into).collect() 
        }
    }

    pub fn aggregate<S: Into<String>>(
        self, 
        group_keys: Vec<S>, 
        agg_column: S, 
        agg_func: crate::aggregate::AggFunc
    ) -> Self {
        LogicalPlan::Aggregate { 
            input: Box::new(self), 
            group_keys: group_keys.into_iter().map(Into::into).collect(), 
            agg_column: agg_column.into(), 
            agg_func 
        }
    }

    pub fn group_filter(self, predicate: Expr) -> Self {
        LogicalPlan::GroupFilter {
            input: Box::new(self),
            predicate,
        }
    }

    pub fn join(self, right: LogicalPlan, left_on: String, right_on: String) -> Self {
        LogicalPlan::Join {
            left: Box::new(self),
            right: Box::new(right),
            left_on,
            right_on,
            output_schema: None,
        }
    }

    pub fn sort(self, order_by: Vec<OrderBy>) -> Self {
        LogicalPlan::Sort {
            input: Box::new(self),
            order_by,
        }
    }

    pub fn limit(self, limit: Option<usize>, offset: usize) -> Self {
        LogicalPlan::Limit {
            input: Box::new(self),
            limit,
            offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LogicalPlan, OrderBy, OrderDirection};
    use crate::{aggregate::AggFunc, predicate::col};

    #[test]
    fn builder_methods_wrap_plan_nodes_in_order() {
        let plan = LogicalPlan::scan("events")
            .filter(col("value"))
            .project(vec!["country"])
            .aggregate(vec!["country"], "value", AggFunc::Sum)
            .sort(vec![OrderBy {
                expr: col("country"),
                direction: OrderDirection::Desc,
            }])
            .limit(Some(10), 4);

        match plan {
            LogicalPlan::Limit { input, limit, offset } => {
                assert_eq!(limit, Some(10));
                assert_eq!(offset, 4);
                assert!(matches!(*input, LogicalPlan::Sort { .. }));
            }
            other => panic!("unexpected plan: {:?}", other),
        }
    }
}
