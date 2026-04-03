//! Adolap server crate.
//!
//! This crate exposes the TCP server, request dispatcher, and server-side
//! meta-command handling used by the interactive CLI.

pub mod tcp;
pub mod handler;
pub mod meta;
