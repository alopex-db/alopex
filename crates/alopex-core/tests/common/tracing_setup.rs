use alopex_core::Result as CoreResult;
use tracing::info;

/// テスト用トレース初期化（stderr JSON）。
pub fn init_test_tracing() {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

/// ファイル出力のトレース初期化。
pub fn init_test_tracing_to_file(path: &std::path::Path) -> CoreResult<()> {
    let log_path = path.to_path_buf();
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(move || {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
                .expect("open log file")
        })
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
    Ok(())
}

pub fn log_test_result(name: &str, success: bool, summary: &str) {
    info!(test = name, success, summary, "test_result");
}

pub fn log_watchdog_event(event: &str) {
    info!(event, "watchdog_event");
}
