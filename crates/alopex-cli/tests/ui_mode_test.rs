use alopex_cli::batch::{BatchMode, BatchModeSource};
use alopex_cli::cli::{Cli, Command};
use alopex_cli::ui::mode::{resolve_ui_mode, UiMode, UiModeSource, UiModeWarning};
use clap::Parser;

fn parse_cli(args: &[&str]) -> Cli {
    Cli::try_parse_from(args).expect("cli parse")
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn default_tui_on_tty_for_row_commands() {
    let cli = parse_cli(&["alopex", "kv", "list"]);
    let batch_mode = BatchMode {
        is_batch: false,
        is_tty: true,
        source: BatchModeSource::Default,
    };
    let resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);

    assert_eq!(resolution.mode, UiMode::Tui);
    assert_eq!(resolution.source, UiModeSource::Default);
    assert!(resolution.warnings.is_empty());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn non_tty_defaults_to_batch_with_warning() {
    let cli = parse_cli(&["alopex", "kv", "list"]);
    let batch_mode = BatchMode {
        is_batch: true,
        is_tty: false,
        source: BatchModeSource::PipeDetected,
    };
    let resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);

    assert_eq!(resolution.mode, UiMode::Batch);
    assert_eq!(resolution.source, UiModeSource::BatchMode);
    assert!(resolution
        .warnings
        .contains(&UiModeWarning::TuiDisabledByNonTty));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn explicit_output_table_forces_batch() {
    let cli = parse_cli(&["alopex", "--output", "table", "sql", "SELECT 1"]);
    let batch_mode = BatchMode {
        is_batch: false,
        is_tty: true,
        source: BatchModeSource::Default,
    };
    let resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);

    assert_eq!(resolution.mode, UiMode::Batch);
    assert_eq!(resolution.source, UiModeSource::OutputFormat);
    assert!(resolution.warnings.is_empty());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn explicit_tui_is_disabled_by_output() {
    let cli = parse_cli(&["alopex", "--output", "json", "sql", "--tui", "SELECT 1"]);
    let batch_mode = BatchMode {
        is_batch: false,
        is_tty: true,
        source: BatchModeSource::Default,
    };
    let resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);

    assert_eq!(resolution.mode, UiMode::Batch);
    assert_eq!(resolution.source, UiModeSource::OutputFormat);
    assert!(resolution
        .warnings
        .contains(&UiModeWarning::TuiDisabledByOutput));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn explicit_tui_non_tty_batch_emits_fallback_warnings() {
    let cli = parse_cli(&["alopex", "--batch", "sql", "--tui", "SELECT 1"]);
    let batch_mode = BatchMode {
        is_batch: true,
        is_tty: false,
        source: BatchModeSource::Explicit,
    };
    let resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);

    assert_eq!(resolution.mode, UiMode::Batch);
    assert_eq!(resolution.source, UiModeSource::BatchMode);
    assert!(resolution.warnings.contains(&UiModeWarning::TuiRequiresTty));
    assert!(resolution
        .warnings
        .contains(&UiModeWarning::TuiDisabledByBatch));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn parses_admin_console_entry() {
    let cli = parse_cli(&["alopex"]);
    assert!(cli.command.is_none());
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn parses_admin_console_server_entry() {
    let cli = parse_cli(&["alopex", "server"]);
    assert!(matches!(
        cli.command,
        Some(Command::Server { command: None })
    ));
}
