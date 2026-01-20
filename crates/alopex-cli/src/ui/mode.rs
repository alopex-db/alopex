//! UI mode resolution for TUI vs batch output.

use crate::batch::BatchMode;
use crate::cli::{Cli, Command};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiMode {
    Tui,
    Batch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiModeSource {
    Explicit,
    OutputFormat,
    BatchMode,
    Default,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiModeWarning {
    TuiRequiresTty,
    TuiDisabledByOutput,
    TuiDisabledByBatch,
    TuiDisabledByNonTty,
}

impl UiModeWarning {
    pub fn message(&self) -> &'static str {
        match self {
            UiModeWarning::TuiRequiresTty => {
                "Warning: --tui requires a TTY, falling back to batch output."
            }
            UiModeWarning::TuiDisabledByOutput => {
                "Warning: --tui is ignored when --output is set, falling back to batch output."
            }
            UiModeWarning::TuiDisabledByBatch => {
                "Warning: --tui is ignored in batch mode, falling back to batch output."
            }
            UiModeWarning::TuiDisabledByNonTty => {
                "Warning: no TTY detected, default TUI is disabled, falling back to batch output."
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiModeResolution {
    pub mode: UiMode,
    pub source: UiModeSource,
    pub warnings: Vec<UiModeWarning>,
}

pub fn resolve_ui_mode(
    cli: &Cli,
    command: Option<&Command>,
    batch_mode: &BatchMode,
) -> UiModeResolution {
    let mut warnings = Vec::new();
    let explicit_tui = command_requests_tui(command);

    if explicit_tui && !batch_mode.is_tty {
        warnings.push(UiModeWarning::TuiRequiresTty);
    }

    if cli.output_is_explicit() {
        if explicit_tui {
            warnings.push(UiModeWarning::TuiDisabledByOutput);
        }
        return UiModeResolution {
            mode: UiMode::Batch,
            source: UiModeSource::OutputFormat,
            warnings,
        };
    }

    if batch_mode.is_batch {
        if explicit_tui {
            warnings.push(UiModeWarning::TuiDisabledByBatch);
        } else if !batch_mode.is_tty {
            warnings.push(UiModeWarning::TuiDisabledByNonTty);
        }
        return UiModeResolution {
            mode: UiMode::Batch,
            source: UiModeSource::BatchMode,
            warnings,
        };
    }

    if explicit_tui {
        return UiModeResolution {
            mode: UiMode::Tui,
            source: UiModeSource::Explicit,
            warnings,
        };
    }

    UiModeResolution {
        mode: UiMode::Tui,
        source: UiModeSource::Default,
        warnings,
    }
}

fn command_requests_tui(command: Option<&Command>) -> bool {
    match command {
        Some(Command::Sql(cmd)) => cmd.tui,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse_cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("cli parse")
    }

    #[test]
    fn resolves_default_tui_on_tty() {
        let cli = parse_cli(&["alopex", "kv", "list"]);
        let batch_mode = BatchMode::detect_with(&cli, true, None);
        let resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);

        assert_eq!(resolution.mode, UiMode::Tui);
        assert_eq!(resolution.source, UiModeSource::Default);
        assert!(resolution.warnings.is_empty());
    }

    #[test]
    fn output_format_forces_batch() {
        let cli = parse_cli(&["alopex", "--output", "json", "sql", "--tui", "SELECT 1"]);
        let batch_mode = BatchMode::detect_with(&cli, true, None);
        let resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);

        assert_eq!(resolution.mode, UiMode::Batch);
        assert_eq!(resolution.source, UiModeSource::OutputFormat);
        assert!(resolution
            .warnings
            .contains(&UiModeWarning::TuiDisabledByOutput));
    }

    #[test]
    fn batch_forces_batch_with_tui_warning() {
        let cli = parse_cli(&["alopex", "--batch", "sql", "--tui", "SELECT 1"]);
        let batch_mode = BatchMode::detect_with(&cli, true, None);
        let resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);

        assert_eq!(resolution.mode, UiMode::Batch);
        assert_eq!(resolution.source, UiModeSource::BatchMode);
        assert!(resolution
            .warnings
            .contains(&UiModeWarning::TuiDisabledByBatch));
    }

    #[test]
    fn non_tty_warns_and_falls_back() {
        let cli = parse_cli(&["alopex", "sql", "--tui", "SELECT 1"]);
        let batch_mode = BatchMode::detect_with(&cli, false, None);
        let resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);

        assert_eq!(resolution.mode, UiMode::Batch);
        assert_eq!(resolution.source, UiModeSource::BatchMode);
        assert!(resolution.warnings.contains(&UiModeWarning::TuiRequiresTty));
    }

    #[test]
    fn non_tty_default_warns() {
        let cli = parse_cli(&["alopex", "kv", "list"]);
        let batch_mode = BatchMode::detect_with(&cli, false, None);
        let resolution = resolve_ui_mode(&cli, cli.command.as_ref(), &batch_mode);

        assert_eq!(resolution.mode, UiMode::Batch);
        assert_eq!(resolution.source, UiModeSource::BatchMode);
        assert!(resolution
            .warnings
            .contains(&UiModeWarning::TuiDisabledByNonTty));
    }
}
