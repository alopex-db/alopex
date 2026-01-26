use std::fs;

use alopex_cli::cli::{LifecycleCommand, OutputFormat};
use alopex_cli::commands::lifecycle;
use tempfile::tempdir;

#[test]
fn e2e_lifecycle_actions_report_success() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path();
    fs::write(data_dir.join("data.txt"), "payload").expect("seed data");

    let cases = [
        (LifecycleCommand::Archive, "Archived"),
        (LifecycleCommand::Backup { command: None }, "Backup"),
        (LifecycleCommand::Export, "Exported"),
        (
            LifecycleCommand::Restore {
                source: None,
                command: None,
            },
            "Restored",
        ),
    ];

    for (command, label) in cases {
        let mut output = Vec::new();
        lifecycle::execute(&command, Some(data_dir), &mut output, OutputFormat::Json)
            .expect("lifecycle execute");
        let text = String::from_utf8(output).expect("utf8");
        assert!(text.contains("OK"));
        assert!(text.contains(label));
    }

    assert!(data_dir.join("data.txt").exists());
}
