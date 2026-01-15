use std::time::Duration;

use alopex_cli::batch::{BatchMode, BatchModeSource};
use alopex_cli::cli::SqlCommand;
use alopex_cli::commands::sql::{execute_with_formatter_control, SqlExecutionOptions};
use alopex_cli::error::CliError;
use alopex_cli::output::formatter::create_formatter;
use alopex_cli::streaming::{CancelSignal, Deadline};
use alopex_embedded::Database;

fn batch_mode() -> BatchMode {
    BatchMode {
        is_batch: true,
        is_tty: true,
        source: BatchModeSource::Explicit,
    }
}

fn setup_db() -> Database {
    let db = Database::open_in_memory().expect("db");
    db.execute_sql("CREATE TABLE streaming_test (id INTEGER PRIMARY KEY);")
        .expect("create table");
    for i in 1..=5 {
        db.execute_sql(&format!("INSERT INTO streaming_test (id) VALUES ({});", i))
            .expect("insert row");
    }
    db
}

#[test]
fn streaming_max_rows_limits_output() {
    let db = setup_db();
    let cmd = SqlCommand {
        query: Some("SELECT id FROM streaming_test;".to_string()),
        file: None,
        fetch_size: None,
        max_rows: Some(2),
        deadline: None,
        tui: false,
    };
    let mut output = Vec::new();
    let formatter = create_formatter(alopex_cli::cli::OutputFormat::Json);
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(60));

    execute_with_formatter_control(
        &db,
        cmd,
        &batch_mode(),
        &mut output,
        formatter,
        SqlExecutionOptions {
            limit: None,
            quiet: false,
            cancel: &cancel,
            deadline: &deadline,
        },
    )
    .expect("execute sql");

    let stdout = String::from_utf8(output).expect("utf8");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    let rows = value.as_array().expect("array");
    assert_eq!(rows.len(), 2);
}

#[test]
fn streaming_deadline_exceeded() {
    let db = setup_db();
    let cmd = SqlCommand {
        query: Some("SELECT id FROM streaming_test;".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    };
    let mut output = Vec::new();
    let formatter = create_formatter(alopex_cli::cli::OutputFormat::Json);
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(0));

    let err = execute_with_formatter_control(
        &db,
        cmd,
        &batch_mode(),
        &mut output,
        formatter,
        SqlExecutionOptions {
            limit: None,
            quiet: false,
            cancel: &cancel,
            deadline: &deadline,
        },
    )
    .unwrap_err();

    assert!(matches!(err, CliError::Timeout(_)));
}

#[test]
fn streaming_cancelled() {
    let db = setup_db();
    let cmd = SqlCommand {
        query: Some("SELECT id FROM streaming_test;".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    };
    let mut output = Vec::new();
    let formatter = create_formatter(alopex_cli::cli::OutputFormat::Json);
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(60));
    cancel.cancel();

    let err = execute_with_formatter_control(
        &db,
        cmd,
        &batch_mode(),
        &mut output,
        formatter,
        SqlExecutionOptions {
            limit: None,
            quiet: false,
            cancel: &cancel,
            deadline: &deadline,
        },
    )
    .unwrap_err();

    assert!(matches!(err, CliError::Cancelled));
}
