use std::fs;

use alopex_cli::batch::{BatchMode, BatchModeSource};
use alopex_cli::cli::{LifecycleCommand, OutputFormat};
use alopex_cli::output::formatter::create_formatter;
use alopex_cli::tui::admin::actions::{
    execute_local_action, AdminAction, AdminCommand, AdminRequest,
};
use alopex_cli::tui::admin::write_non_tty_fallback;
use alopex_cli::ui::mode::UiMode;
use alopex_embedded::Database;
use tempfile::tempdir;

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn admin_non_tty_fallback_uses_formatter() {
    let mut output = Vec::new();
    write_non_tty_fallback(&mut output, OutputFormat::Json).expect("fallback output");
    let value: serde_json::Value = serde_json::from_slice(&output).expect("json");
    let rows = value.as_array().expect("array");
    assert_eq!(rows.len(), 1);
    let row = rows[0].as_object().expect("row object");
    assert_eq!(row.get("Status").and_then(|v| v.as_str()), Some("Error"));
    let message = row
        .get("Message")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(message.contains("Admin UI is unavailable without a TTY."));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn admin_dispatches_lifecycle_archive() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path();
    fs::write(data_dir.join("data.txt"), "payload").expect("seed data");

    let db = Database::open_in_memory().expect("db");
    let batch_mode = BatchMode {
        is_batch: true,
        is_tty: false,
        source: BatchModeSource::Explicit,
    };
    let request = AdminRequest {
        action: AdminAction::Archive,
        command: AdminCommand::Lifecycle(LifecycleCommand::Archive),
        limit: None,
        quiet: false,
        ui_mode: UiMode::Batch,
        connection_label: "local".to_string(),
        output: OutputFormat::Json,
        data_dir: Some(data_dir.to_path_buf()),
    };
    let mut output = Vec::new();
    let mut make_formatter = || create_formatter(OutputFormat::Json);
    execute_local_action(&db, &batch_mode, request, &mut output, &mut make_formatter)
        .expect("archive");

    assert!(data_dir.join(".lifecycle/archive/latest").exists());
}
