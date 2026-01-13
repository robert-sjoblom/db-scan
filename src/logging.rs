use tracing_subscriber::EnvFilter;

pub(crate) fn setup(log_level: EnvFilter) {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .pretty()
        .with_env_filter(log_level)
        .init();

    tracing::info!("logging initialized");
}
