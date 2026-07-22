use crate::config::EXIT_CODE_INTERRUPTED;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitCode {
    Success = 0,
    Error = 1,
    Warning = 2,
    Retryable = 3,
    Authorization = 4,
    Unsupported = 5,
    Interrupted = EXIT_CODE_INTERRUPTED,
}

/// Normalized terminal class for distributed SQL reads. It deliberately has a
/// narrower vocabulary than arbitrary HTTP/server errors so shell automation
/// can distinguish unsupported input from retryable infrastructure failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistributedReadOutcome {
    Success,
    Unsupported,
    AuthorizationFailure,
    RetryableFailure,
    TerminalFailure,
    Cancelled,
}

impl DistributedReadOutcome {
    pub fn exit_code(self) -> ExitCode {
        match self {
            Self::Success => ExitCode::Success,
            Self::Unsupported => ExitCode::Unsupported,
            Self::AuthorizationFailure => ExitCode::Authorization,
            Self::RetryableFailure => ExitCode::Retryable,
            Self::TerminalFailure => ExitCode::Error,
            Self::Cancelled => ExitCode::Interrupted,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Unsupported => "unsupported",
            Self::AuthorizationFailure => "authorization_failure",
            Self::RetryableFailure => "retryable_failure",
            Self::TerminalFailure => "terminal_failure",
            Self::Cancelled => "cancelled",
        }
    }
}

#[allow(dead_code)]
impl ExitCode {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Normalized outcome classes for `server cluster` management operations.
/// Unknown server values deliberately fail closed as terminal failures, rather
/// than allowing an unrecognized result to look successful to automation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterManagementOutcome {
    Succeeded,
    Pending,
    RetryableFailure,
    TerminalFailure,
    AuthorizationFailure,
}

impl ClusterManagementOutcome {
    pub fn from_wire(outcome_class: &str, reason: &str) -> Self {
        let outcome_class = outcome_class.to_ascii_lowercase();
        let reason = reason.to_ascii_lowercase();
        if outcome_class == "authorization_failure"
            || reason.contains("authorization")
            || reason.contains("permission")
            || reason.contains("forbidden")
        {
            return Self::AuthorizationFailure;
        }
        match outcome_class.as_str() {
            "succeeded" | "success" => Self::Succeeded,
            "pending" => Self::Pending,
            "retryable_failure" => Self::RetryableFailure,
            "terminal_failure" => Self::TerminalFailure,
            _ => Self::TerminalFailure,
        }
    }

    pub fn exit_code(self) -> ExitCode {
        match self {
            Self::Succeeded => ExitCode::Success,
            Self::Pending => ExitCode::Warning,
            Self::RetryableFailure => ExitCode::Retryable,
            Self::TerminalFailure => ExitCode::Error,
            Self::AuthorizationFailure => ExitCode::Authorization,
        }
    }

    pub fn is_success(self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct ExitCodeCollector {
    success_count: usize,
    error_count: usize,
}

#[allow(dead_code)]
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
        assert_eq!(ExitCode::Retryable.as_i32(), 3);
        assert_eq!(ExitCode::Authorization.as_i32(), 4);
        assert_eq!(ExitCode::Unsupported.as_i32(), 5);
        assert_eq!(ExitCode::Interrupted.as_i32(), EXIT_CODE_INTERRUPTED);
    }

    #[test]
    fn cluster_management_outcomes_have_stable_exit_classes() {
        assert_eq!(
            ClusterManagementOutcome::from_wire("succeeded", "committed").exit_code(),
            ExitCode::Success
        );
        assert_eq!(
            ClusterManagementOutcome::from_wire("pending", "waiting_for_quorum").exit_code(),
            ExitCode::Warning
        );
        assert_eq!(
            ClusterManagementOutcome::from_wire("retryable_failure", "not_leader").exit_code(),
            ExitCode::Retryable
        );
        assert_eq!(
            ClusterManagementOutcome::from_wire("terminal_failure", "stale_version").exit_code(),
            ExitCode::Error
        );
        assert_eq!(
            ClusterManagementOutcome::from_wire("terminal_failure", "authorization_denied")
                .exit_code(),
            ExitCode::Authorization
        );
        assert_eq!(
            ClusterManagementOutcome::from_wire("new_outcome", "unknown").exit_code(),
            ExitCode::Error
        );
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

    #[test]
    fn distributed_read_outcomes_have_distinct_exit_classes() {
        assert_eq!(
            DistributedReadOutcome::Success.exit_code(),
            ExitCode::Success
        );
        assert_eq!(
            DistributedReadOutcome::Unsupported.exit_code(),
            ExitCode::Unsupported
        );
        assert_eq!(
            DistributedReadOutcome::AuthorizationFailure.exit_code(),
            ExitCode::Authorization
        );
        assert_eq!(
            DistributedReadOutcome::RetryableFailure.exit_code(),
            ExitCode::Retryable
        );
        assert_eq!(
            DistributedReadOutcome::TerminalFailure.exit_code(),
            ExitCode::Error
        );
        assert_eq!(
            DistributedReadOutcome::Cancelled.exit_code(),
            ExitCode::Interrupted
        );
    }
}
