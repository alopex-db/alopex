pub mod detector;
pub mod exit_code;

#[allow(unused_imports)]
pub use detector::{BatchMode, BatchModeSource};
#[allow(unused_imports)]
pub use exit_code::{ExitCode, ExitCodeCollector};

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    use crate::cli::Cli;

    fn parse_cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("cli parse")
    }

    #[test]
    fn detect_batch_mode_sources() {
        let base = parse_cli(&["alopex", "kv", "list"]);
        let explicit_cli = parse_cli(&["alopex", "--batch", "kv", "list"]);

        let explicit = BatchMode::detect_with(&explicit_cli, true, Some("batch"));
        let env = BatchMode::detect_with(&base, true, Some("batch"));
        let pipe = BatchMode::detect_with(&base, false, None);
        let default = BatchMode::detect_with(&base, true, None);

        assert_eq!(explicit.source, BatchModeSource::Explicit);
        assert_eq!(env.source, BatchModeSource::Environment);
        assert_eq!(pipe.source, BatchModeSource::PipeDetected);
        assert_eq!(default.source, BatchModeSource::Default);
    }

    #[test]
    fn should_prompt_requires_destructive_action() {
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
    fn exit_code_collector_reports_warning_on_mixed_results() {
        let mut collector = ExitCodeCollector::new();
        collector.record_success();
        collector.record_error();

        assert_eq!(collector.finalize(), ExitCode::Warning);
    }
}
