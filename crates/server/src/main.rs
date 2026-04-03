//! Binary entrypoint for the Adolap TCP server.
//!
//! Example:
//! ```text
//! cargo run -p server
//! ```

use adolap_core::logging::init_logging;
use server::tcp::start_server;

#[tokio::main]
async fn main() {
    init_logging("server");

    if let Err(e) = start_server("0.0.0.0:5999").await {
        tracing::error!(error = %e, "server terminated with error");
        eprintln!("Server error: {}", e);
    }
}
