use std::sync::OnceLock;

static TRACING_INIT: OnceLock<()> = OnceLock::new();

pub fn init_logger() {
    TRACING_INIT.get_or_init(|| {
        let format = tracing_subscriber::fmt::format()
            .with_level(true)
            .with_target(true)
            .with_thread_ids(false)
            .with_thread_names(false)
            .with_file(true)
            .with_line_number(true)
            .with_ansi(true);

        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .event_format(format)
            .init();
    });
}
