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
