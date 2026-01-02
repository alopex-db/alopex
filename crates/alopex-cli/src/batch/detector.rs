use std::io::{self, IsTerminal};

use crate::cli::Cli;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchMode {
    pub is_batch: bool,
    pub is_tty: bool,
    pub source: BatchModeSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchModeSource {
    Explicit,
    Environment,
    PipeDetected,
    Default,
}

impl BatchMode {
    pub fn detect(cli: &Cli) -> Self {
        let is_tty = io::stdin().is_terminal();
        let env_mode = std::env::var("ALOPEX_MODE").ok();

        Self::detect_with(cli, is_tty, env_mode.as_deref())
    }

    fn detect_with(cli: &Cli, is_tty: bool, env_mode: Option<&str>) -> Self {
        if cli.batch {
            return Self {
                is_batch: true,
                is_tty,
                source: BatchModeSource::Explicit,
            };
        }

        if matches!(env_mode, Some("batch")) {
            return Self {
                is_batch: true,
                is_tty,
                source: BatchModeSource::Environment,
            };
        }

        if !is_tty {
            return Self {
                is_batch: true,
                is_tty,
                source: BatchModeSource::PipeDetected,
            };
        }

        Self {
            is_batch: false,
            is_tty,
            source: BatchModeSource::Default,
        }
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
    fn detect_explicit_overrides_env_and_pipe() {
        let cli = parse_cli(&["alopex", "--batch", "kv", "list"]);
        let mode = BatchMode::detect_with(&cli, false, Some("batch"));

        assert!(mode.is_batch);
        assert_eq!(mode.source, BatchModeSource::Explicit);
        assert!(!mode.is_tty);
    }

    #[test]
    fn detect_env_overrides_pipe() {
        let cli = parse_cli(&["alopex", "kv", "list"]);
        let mode = BatchMode::detect_with(&cli, false, Some("batch"));

        assert!(mode.is_batch);
        assert_eq!(mode.source, BatchModeSource::Environment);
        assert!(!mode.is_tty);
    }

    #[test]
    fn detect_pipe_when_not_tty() {
        let cli = parse_cli(&["alopex", "kv", "list"]);
        let mode = BatchMode::detect_with(&cli, false, None);

        assert!(mode.is_batch);
        assert_eq!(mode.source, BatchModeSource::PipeDetected);
        assert!(!mode.is_tty);
    }

    #[test]
    fn detect_default_when_tty() {
        let cli = parse_cli(&["alopex", "kv", "list"]);
        let mode = BatchMode::detect_with(&cli, true, None);

        assert!(!mode.is_batch);
        assert_eq!(mode.source, BatchModeSource::Default);
        assert!(mode.is_tty);
    }
}
