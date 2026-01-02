use std::io::{self, IsTerminal};

use crate::cli::{Cli, Command, HnswCommand, KvCommand, ProfileCommand};

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

    pub fn should_prompt(&self, cli: &Cli, is_destructive: bool) -> bool {
        if cli.yes || self.is_batch {
            return false;
        }

        is_destructive
    }

    pub fn should_show_progress(&self, explicit_progress: bool) -> bool {
        explicit_progress || !self.is_batch
    }
}

pub fn is_destructive_command(command: &Command) -> bool {
    matches!(
        command,
        Command::Kv {
            command: KvCommand::Delete { .. }
        } | Command::Hnsw {
            command: HnswCommand::Drop { .. }
        } | Command::Profile {
            command: ProfileCommand::Delete { .. }
        }
    )
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

    #[test]
    fn should_prompt_respects_yes_and_batch() {
        let cli = parse_cli(&["alopex", "--yes", "kv", "list"]);
        let batch_mode = BatchMode {
            is_batch: false,
            is_tty: true,
            source: BatchModeSource::Default,
        };
        let batch = BatchMode {
            is_batch: true,
            is_tty: true,
            source: BatchModeSource::Explicit,
        };

        assert!(!batch_mode.should_prompt(&cli, true));
        assert!(!batch.should_prompt(&cli, true));
    }

    #[test]
    fn should_prompt_only_for_destructive_commands() {
        let cli = parse_cli(&["alopex", "kv", "list"]);
        let mode = BatchMode {
            is_batch: false,
            is_tty: true,
            source: BatchModeSource::Default,
        };

        assert!(mode.should_prompt(&cli, true));
        assert!(!mode.should_prompt(&cli, false));
    }

    #[test]
    fn should_show_progress_respects_explicit_flag() {
        let batch = BatchMode {
            is_batch: true,
            is_tty: false,
            source: BatchModeSource::PipeDetected,
        };
        let interactive = BatchMode {
            is_batch: false,
            is_tty: true,
            source: BatchModeSource::Default,
        };

        assert!(batch.should_show_progress(true));
        assert!(!batch.should_show_progress(false));
        assert!(interactive.should_show_progress(false));
    }

    #[test]
    fn destructive_command_detection() {
        let kv_delete = Command::Kv {
            command: KvCommand::Delete { key: "key".into() },
        };
        let kv_list = Command::Kv {
            command: KvCommand::List { prefix: None },
        };
        let profile_delete = Command::Profile {
            command: ProfileCommand::Delete { name: "dev".into() },
        };
        let hnsw_drop = Command::Hnsw {
            command: HnswCommand::Drop { name: "idx".into() },
        };

        assert!(is_destructive_command(&kv_delete));
        assert!(!is_destructive_command(&kv_list));
        assert!(is_destructive_command(&profile_delete));
        assert!(is_destructive_command(&hnsw_drop));
    }
}
