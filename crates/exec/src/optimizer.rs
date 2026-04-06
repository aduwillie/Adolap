use crate::logical_plan::LogicalPlan;

pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    // For now, just a simple recursive pass that:
    // - pushes Filter below Project when possible
    // - leaves everything else as-is
    match plan {
        LogicalPlan::Project { input, columns } => {
            match *input {
                LogicalPlan::Filter { input: inner, predicate } => {
                    // Project(Filter(Scan)) -> Filter(Project(Scan))
                    let projected = LogicalPlan::Project { input: inner, columns };
                    LogicalPlan::Filter {
                        input: Box::new(projected),
                        predicate,
                    }
                }
                other => LogicalPlan::Project {
                    input: Box::new(optimize(other)),
                    columns,
                },
            }
        }
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(optimize(*input)),
            predicate,
        },
        LogicalPlan::Aggregate { input, group_keys, agg_column, agg_func } => {
            LogicalPlan::Aggregate {
                input: Box::new(optimize(*input)),
                group_keys,
                agg_column,
                agg_func,
            }
        }
        LogicalPlan::GroupFilter { input, predicate } => LogicalPlan::GroupFilter {
            input: Box::new(optimize(*input)),
            predicate,
        },
        LogicalPlan::Join { left, right, left_on, right_on, output_schema } => LogicalPlan::Join {
            left: Box::new(optimize(*left)),
            right: Box::new(optimize(*right)),
            left_on,
            right_on,
            output_schema,
        },
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(optimize(*input)),
            order_by,
        },
        LogicalPlan::Limit { input, limit, offset } => LogicalPlan::Limit {
            input: Box::new(optimize(*input)),
            limit,
            offset,
        },
        scan @ LogicalPlan::Scan { .. } => scan,
    }
}

#[cfg(test)]
mod tests {
    use super::optimize;
    use crate::{logical_plan::LogicalPlan, predicate::col};

    #[test]
    fn rewrites_project_over_filter_into_filter_over_project() {
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::scan("events")),
                predicate: col("value"),
            }),
            columns: vec!["country".into()],
        };

        let optimized = optimize(plan);

        match optimized {
            LogicalPlan::Filter { input, .. } => match *input {
                LogicalPlan::Project { columns, .. } => assert_eq!(columns, vec!["country"]),
                other => panic!("expected projected child, got {:?}", other),
            },
            other => panic!("unexpected optimized plan: {:?}", other),
        }
    }
}
