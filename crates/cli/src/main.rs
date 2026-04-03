//! Binary entrypoint for the interactive Adolap shell.
//!
//! Example:
//! ```text
//! cargo run -p cli
//! ```

use adolap_core::logging::init_logging;

mod client;
mod ctrlc_handler;
mod meta;
mod printer;
mod repl;

#[tokio::main]
async fn main() {
    init_logging("cli");

    if let Err(error) = repl::run().await {
        tracing::error!(error = %error, "cli terminated with error");
        eprintln!("CLI error: {}", error);
        std::process::exit(1);
    }
}
