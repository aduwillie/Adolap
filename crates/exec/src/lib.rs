//! Query planning and execution for Adolap.
//!
//! This crate contains the parser, logical and physical plans, optimizers, and
//! the in-process execution engine used by the TCP server.

pub mod projection;
pub mod predicate;
pub mod filter;
pub mod aggregate;
pub mod global_aggregate;
pub mod group_by;
pub mod global_group_by;
pub mod query_engine;
pub mod logical_plan;
pub mod physical_plan;
pub mod executor;
pub mod optimizer;
pub mod planner;

pub mod dsl;
pub mod parser;

#[cfg(test)]
mod tests {
	#[test]
	fn exports_planning_and_parser_surface() {
		let sum = crate::aggregate::sum("value");
		assert_eq!(sum.1, "value");
		assert!(crate::parser::parse_statement("CREATE DATABASE analytics").is_ok());
	}
}
