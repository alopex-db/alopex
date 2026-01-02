use crate::config::EXIT_CODE_INTERRUPTED;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitCode {
    Success = 0,
    Error = 1,
    Warning = 2,
    Interrupted = EXIT_CODE_INTERRUPTED,
}

impl ExitCode {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

#[derive(Debug, Default)]
pub struct ExitCodeCollector {
    success_count: usize,
    error_count: usize,
}

impl ExitCodeCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_success(&mut self) {
        self.success_count += 1;
    }

    pub fn record_error(&mut self) {
        self.error_count += 1;
    }

    pub fn finalize(&self) -> ExitCode {
        match (self.success_count > 0, self.error_count > 0) {
            (false, false) => ExitCode::Success,
            (true, false) => ExitCode::Success,
            (false, true) => ExitCode::Error,
            (true, true) => ExitCode::Warning,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_values() {
        assert_eq!(ExitCode::Success.as_i32(), 0);
        assert_eq!(ExitCode::Error.as_i32(), 1);
        assert_eq!(ExitCode::Warning.as_i32(), 2);
        assert_eq!(ExitCode::Interrupted.as_i32(), EXIT_CODE_INTERRUPTED);
    }

    #[test]
    fn collector_defaults_to_success() {
        let collector = ExitCodeCollector::new();

        assert_eq!(collector.finalize(), ExitCode::Success);
    }

    #[test]
    fn collector_reports_success_only() {
        let mut collector = ExitCodeCollector::new();
        collector.record_success();

        assert_eq!(collector.finalize(), ExitCode::Success);
    }

    #[test]
    fn collector_reports_error_only() {
        let mut collector = ExitCodeCollector::new();
        collector.record_error();

        assert_eq!(collector.finalize(), ExitCode::Error);
    }

    #[test]
    fn collector_reports_warning_on_mixed_results() {
        let mut collector = ExitCodeCollector::new();
        collector.record_success();
        collector.record_error();

        assert_eq!(collector.finalize(), ExitCode::Warning);
    }
}
