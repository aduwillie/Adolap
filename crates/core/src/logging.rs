use std::sync::OnceLock;
use tracing_subscriber::EnvFilter;

static LOGGING_INIT: OnceLock<()> = OnceLock::new();

pub fn init_logging(service_name: &str) {
    LOGGING_INIT.get_or_init(|| {
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"));

        let _ = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .try_init();
    });

    tracing::info!(%service_name, "logging initialized");
}