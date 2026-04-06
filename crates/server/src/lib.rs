//! Adolap server crate.
//!
//! This crate exposes the TCP server, request dispatcher, and server-side
//! meta-command handling used by the interactive CLI.

pub mod tcp;
pub mod handler;
pub mod meta;

#[cfg(test)]
mod tests {
	#[test]
	fn exports_server_modules() {
		let _start_server = crate::tcp::start_server;
		let _handle_message = crate::handler::handle_message;
		let _handle_meta = crate::meta::handle_meta_command;
	}
}
