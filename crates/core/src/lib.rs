//! Shared primitives used across the Adolap workspace.
//!
//! This crate keeps reusable concerns such as errors, identifiers, logging, and
//! time helpers out of the higher-level execution and storage crates.

pub mod error;
pub mod logging;
pub mod config;
pub mod types;
pub mod time;
pub mod util;
pub mod id;

#[cfg(test)]
mod tests {
	#[test]
	fn exports_shared_modules() {
		let _ = crate::error::AdolapError::ExecutionError(String::new());
		let _ = crate::logging::init_logging;
		let _: crate::types::RowCount = 1;
	}
}
