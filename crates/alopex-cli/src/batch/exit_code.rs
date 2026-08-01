use crate::config::EXIT_CODE_INTERRUPTED;
use serde_json::Value as JsonValue;

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

/// Normalized terminal class for a versioned transaction outcome returned by
/// the HTTP adapters. The CLI keeps the complete outcome document in its
/// additive output column; this type only decides the stable shell exit code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionCliOutcome {
    Success,
    Pending,
    RetryableFailure,
    TerminalFailure,
    AuthorizationFailure,
    Unsupported,
    Cancelled,
}

impl TransactionCliOutcome {
    /// Classify a transaction envelope without guessing a success from a 2xx
    /// status. Unknown states and incomplete canonical data fail closed.
    pub fn from_transaction(transaction: Option<&JsonValue>, http_status: u16) -> Self {
        let Some(transaction) = transaction else {
            return Self::from_http_status(http_status);
        };
        if !is_canonical_transaction_outcome(transaction) {
            return Self::TerminalFailure;
        }
        let state = transaction
            .get("state")
            .and_then(JsonValue::as_str)
            .map(str::to_ascii_lowercase);
        let failure_class = transaction
            .get("failure_class")
            .and_then(JsonValue::as_str)
            .map(str::to_ascii_lowercase);
        let reason_code = transaction
            .get("reason_code")
            .and_then(JsonValue::as_str)
            .map(str::to_ascii_lowercase);
        let routing_kind = transaction
            .get("routing")
            .and_then(|routing| routing.get("kind"))
            .and_then(JsonValue::as_str)
            .map(str::to_ascii_lowercase);
        let retryable = transaction
            .get("retryable")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);

        if state.as_deref() == Some("cancelled") {
            return Self::Cancelled;
        }
        if failure_class.as_deref() == Some("unauthorized") {
            return Self::AuthorizationFailure;
        }
        if matches!(
            routing_kind.as_deref(),
            Some("unsupported") | Some("blocked")
        ) || failure_class.as_deref() == Some("prerequisite_missing")
            || reason_code
                .as_deref()
                .is_some_and(|reason| reason.ends_with("_unsupported"))
        {
            return Self::Unsupported;
        }
        if retryable || state.as_deref() == Some("retryable_failure") {
            return Self::RetryableFailure;
        }
        match state.as_deref() {
            Some("committed") => Self::Success,
            Some("accepted") | Some("running") | Some("recovery_pending") => Self::Pending,
            Some("rejected") | Some("terminal_failure") | None => Self::TerminalFailure,
            Some(_) => Self::TerminalFailure,
        }
    }

    pub fn exit_code(self) -> ExitCode {
        match self {
            Self::Success => ExitCode::Success,
            Self::Pending => ExitCode::Warning,
            Self::RetryableFailure => ExitCode::Retryable,
            Self::TerminalFailure => ExitCode::Error,
            Self::AuthorizationFailure => ExitCode::Authorization,
            Self::Unsupported => ExitCode::Unsupported,
            Self::Cancelled => ExitCode::Interrupted,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Pending => "pending",
            Self::RetryableFailure => "retryable_failure",
            Self::TerminalFailure => "terminal_failure",
            Self::AuthorizationFailure => "authorization_failure",
            Self::Unsupported => "unsupported",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_http_status(status: u16) -> Self {
        match status {
            200..=201 => Self::Success,
            202 => Self::Pending,
            401 | 403 => Self::AuthorizationFailure,
            408 | 409 | 429 | 503 => Self::RetryableFailure,
            499 => Self::Cancelled,
            501 => Self::Unsupported,
            _ => Self::TerminalFailure,
        }
    }
}

/// A transaction document is authoritative only when it contains the complete
/// versioned contract. A partial object must not turn a failed request into a
/// successful or retryable shell outcome merely because it has a familiar
/// `state` field.
fn is_canonical_transaction_outcome(transaction: &JsonValue) -> bool {
    let non_empty_string = |field: &str| {
        transaction
            .get(field)
            .and_then(JsonValue::as_str)
            .is_some_and(|value| !value.is_empty())
    };
    let routing = transaction.get("routing");
    let idempotency = transaction.get("idempotency");

    transaction
        .get("outcome_version")
        .and_then(JsonValue::as_str)
        == Some("v0.9")
        && non_empty_string("transaction_id")
        && non_empty_string("request_id")
        && non_empty_string("state")
        && transaction
            .get("participating_ranges")
            .is_some_and(JsonValue::is_array)
        && transaction.get("failure_class").is_some()
        && non_empty_string("reason_code")
        && transaction
            .get("retryable")
            .and_then(JsonValue::as_bool)
            .is_some()
        && routing
            .and_then(|routing| routing.get("kind"))
            .and_then(JsonValue::as_str)
            .is_some_and(|kind| !kind.is_empty())
        && idempotency
            .and_then(|idempotency| idempotency.get("operation_id"))
            .and_then(JsonValue::as_str)
            .is_some_and(|operation_id| !operation_id.is_empty())
        && idempotency
            .and_then(|idempotency| idempotency.get("request_id"))
            .and_then(JsonValue::as_str)
            .is_some_and(|request_id| !request_id.is_empty())
        && idempotency
            .and_then(|idempotency| idempotency.get("state"))
            .and_then(JsonValue::as_str)
            .is_some_and(|state| !state.is_empty())
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

/// Normalized outcome classes for a canonical changefeed response. The CLI
/// keeps the response document intact and uses this type only for its stable
/// shell exit matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangefeedCliOutcome {
    Success,
    Pending,
    RetryableFailure,
    TerminalFailure,
    AuthorizationFailure,
    Unsupported,
    Cancelled,
}

impl ChangefeedCliOutcome {
    /// Classify canonical outcome documents without treating a non-2xx
    /// response as a transport error. A response that lacks a canonical state
    /// falls back to its HTTP status; an unknown canonical state fails closed.
    pub fn from_changefeed_response(documents: &[JsonValue], http_status: u16) -> Self {
        documents
            .iter()
            .filter_map(changefeed_document_outcome)
            .fold(Self::from_http_status(http_status), Self::combine)
    }

    pub fn exit_code(self) -> ExitCode {
        match self {
            Self::Success => ExitCode::Success,
            Self::Pending => ExitCode::Warning,
            Self::RetryableFailure => ExitCode::Retryable,
            Self::TerminalFailure => ExitCode::Error,
            Self::AuthorizationFailure => ExitCode::Authorization,
            Self::Unsupported => ExitCode::Unsupported,
            Self::Cancelled => ExitCode::Interrupted,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Pending => "pending",
            Self::RetryableFailure => "retryable_failure",
            Self::TerminalFailure => "terminal_failure",
            Self::AuthorizationFailure => "authorization_failure",
            Self::Unsupported => "unsupported",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_http_status(status: u16) -> Self {
        match status {
            200..=201 => Self::Success,
            202 => Self::Pending,
            401 | 403 => Self::AuthorizationFailure,
            408 | 429 | 503 => Self::RetryableFailure,
            499 => Self::Cancelled,
            501 => Self::Unsupported,
            _ => Self::TerminalFailure,
        }
    }

    fn combine(self, other: Self) -> Self {
        if other.precedence() > self.precedence() {
            other
        } else {
            self
        }
    }

    fn precedence(self) -> u8 {
        match self {
            Self::Success => 0,
            Self::Pending => 1,
            Self::RetryableFailure => 2,
            Self::TerminalFailure => 3,
            Self::Unsupported => 4,
            Self::AuthorizationFailure => 5,
            Self::Cancelled => 6,
        }
    }
}

fn changefeed_document_outcome(document: &JsonValue) -> Option<ChangefeedCliOutcome> {
    let state = document.get("operation_state").and_then(JsonValue::as_str);
    let failure_class = document.get("failure_class").and_then(JsonValue::as_str);
    let reason_code = document.get("reason_code").and_then(JsonValue::as_str);
    let retryable = document
        .get("retryable")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    let routing_unsupported = document
        .get("routing")
        .and_then(|routing| routing.get("kind"))
        .and_then(JsonValue::as_str)
        .is_some_and(|kind| kind.eq_ignore_ascii_case("unsupported"));

    let has_canonical_outcome = state.is_some() || failure_class.is_some() || reason_code.is_some();
    if !has_canonical_outcome {
        return None;
    }

    if state.is_some_and(|state| state.eq_ignore_ascii_case("cancelled")) {
        return Some(ChangefeedCliOutcome::Cancelled);
    }
    if failure_class.is_some_and(|class| class.eq_ignore_ascii_case("unauthorized")) {
        return Some(ChangefeedCliOutcome::AuthorizationFailure);
    }
    if routing_unsupported || reason_code.is_some_and(|reason| reason.ends_with("_unsupported")) {
        return Some(ChangefeedCliOutcome::Unsupported);
    }
    if state.is_some_and(|state| {
        matches!(
            state,
            "accepted" | "running" | "recovery_pending" | "pending"
        )
    }) {
        return Some(ChangefeedCliOutcome::Pending);
    }
    if retryable
        || state.is_some_and(|state| state.eq_ignore_ascii_case("retryable_failure"))
        || reason_code.is_some_and(|reason| reason.eq_ignore_ascii_case("backpressure"))
    {
        return Some(ChangefeedCliOutcome::RetryableFailure);
    }
    if failure_class.is_some_and(|class| class.eq_ignore_ascii_case("prerequisite_missing")) {
        return Some(ChangefeedCliOutcome::Unsupported);
    }
    if failure_class.is_some()
        || reason_code
            .is_some_and(|reason| matches!(reason, "retention_expired" | "resource_limit"))
        || state.is_some_and(|state| matches!(state, "rejected" | "terminal_failure" | "failed"))
    {
        return Some(ChangefeedCliOutcome::TerminalFailure);
    }
    if state.is_some_and(|state| state.eq_ignore_ascii_case("committed")) {
        return Some(ChangefeedCliOutcome::Success);
    }

    Some(ChangefeedCliOutcome::TerminalFailure)
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

    fn canonical_transaction(state: &str) -> JsonValue {
        serde_json::json!({
            "outcome_version": "v0.9",
            "transaction_id": "txn-1",
            "request_id": "request-1",
            "participating_ranges": [],
            "state": state,
            "failure_class": null,
            "reason_code": "transaction_outcome",
            "routing": {"kind": "single_range"},
            "retryable": false,
            "idempotency": {
                "operation_id": "txn-1",
                "request_id": "request-1",
                "state": state,
            },
        })
    }

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

    #[test]
    fn transaction_outcomes_use_the_documented_exit_matrix_and_fail_closed() {
        let committed = canonical_transaction("committed");
        let running = canonical_transaction("running");
        let mut blocked = canonical_transaction("rejected");
        blocked["failure_class"] = serde_json::json!("prerequisite_missing");
        blocked["routing"]["kind"] = serde_json::json!("blocked");
        let mut retryable = canonical_transaction("retryable_failure");
        retryable["retryable"] = serde_json::json!(true);
        let unknown = canonical_transaction("future_state");
        let incomplete = serde_json::json!({"state": "committed"});

        assert_eq!(
            TransactionCliOutcome::from_transaction(Some(&committed), 200).exit_code(),
            ExitCode::Success
        );
        assert_eq!(
            TransactionCliOutcome::from_transaction(Some(&running), 200).exit_code(),
            ExitCode::Warning
        );
        assert_eq!(
            TransactionCliOutcome::from_transaction(Some(&blocked), 501).exit_code(),
            ExitCode::Unsupported
        );
        assert_eq!(
            TransactionCliOutcome::from_transaction(Some(&retryable), 409).exit_code(),
            ExitCode::Retryable
        );
        assert_eq!(
            TransactionCliOutcome::from_transaction(Some(&unknown), 200).exit_code(),
            ExitCode::Error
        );
        assert_eq!(
            TransactionCliOutcome::from_transaction(Some(&incomplete), 200).exit_code(),
            ExitCode::Error
        );
    }
}
