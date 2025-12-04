pub fn setup_proposer_logger() {
    let log_color = std::env::var("LOG_COLOR")
        .map(|v| v.parse::<bool>().unwrap_or(false))
        .unwrap_or_default();

    let format = tracing_subscriber::fmt::format()
        .with_level(true)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(false)
        .with_line_number(false)
        .with_ansi(log_color);

    // Use RUST_LOG env var for log level, defaulting to INFO if not set.
    let env_filter = tracing_subscriber::filter::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::filter::EnvFilter::new("info"));

    // Turn off all logging from kona and SP1.
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .event_format(format)
        .init();
}
