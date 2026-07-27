use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::{FailureClass, OperationState, RoutingOutcomeKind};

use super::{CounterValue, CrdtCommonFields, CrdtObjectType, SetMemberVersion, SetValue};

/// Deterministic Counter or Set value together with its accepted-operation
/// evidence.  This is nested under the canonical public `value` field so all
/// adapters serialize the same shape.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "value_type", rename_all = "snake_case")]
pub enum CrdtValue {
    Counter {
        initial_value: i64,
        accepted_delta_total: i64,
        value: i64,
        accepted_operation_versions: BTreeMap<String, u64>,
    },
    Set {
        members: Vec<String>,
        member_versions: BTreeMap<String, SetMemberVersion>,
        accepted_operation_versions: BTreeMap<String, u64>,
    },
}

impl From<CounterValue> for CrdtValue {
    fn from(value: CounterValue) -> Self {
        Self::Counter {
            initial_value: value.initial_value,
            accepted_delta_total: value.accepted_delta_total,
            value: value.value,
            accepted_operation_versions: value.accepted_operation_versions,
        }
    }
}

impl From<SetValue> for CrdtValue {
    fn from(value: SetValue) -> Self {
        Self::Set {
            members: value.members,
            member_versions: value.member_versions,
            accepted_operation_versions: value.accepted_operation_versions,
        }
    }
}

/// Exact public Phase 2 result.  Common Phase 1 fields are flattened so every
/// supported surface returns one field-for-field contract rather than a
/// transport-specific wrapper.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CrdtOutcome {
    #[serde(flatten)]
    common: CrdtCommonFields,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<CrdtValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value_unavailable: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    membership_unavailable: Option<String>,
}

impl CrdtOutcome {
    /// Creates a Counter result with the deterministic current numeric value.
    pub fn counter(common: CrdtCommonFields, value: CounterValue) -> Self {
        assert_eq!(common.object_type, CrdtObjectType::Counter);
        Self {
            common,
            value: Some(value.into()),
            value_unavailable: None,
            membership_unavailable: None,
        }
    }

    /// Creates a Set result with canonical members and winner evidence.
    pub fn set(common: CrdtCommonFields, value: SetValue) -> Self {
        assert_eq!(common.object_type, CrdtObjectType::Set);
        Self {
            common,
            value: Some(value.into()),
            value_unavailable: None,
            membership_unavailable: None,
        }
    }

    /// Creates an unresolved Counter result.  A missing Counter value is never
    /// ambiguous: the public response always contains an explicit reason.
    pub fn counter_unavailable(common: CrdtCommonFields, reason: impl Into<String>) -> Self {
        assert_eq!(common.object_type, CrdtObjectType::Counter);
        Self {
            common,
            value: None,
            value_unavailable: Some(non_empty_reason(reason)),
            membership_unavailable: None,
        }
    }

    /// Creates an unresolved Set result with an explicit membership reason.
    pub fn set_unavailable(common: CrdtCommonFields, reason: impl Into<String>) -> Self {
        assert_eq!(common.object_type, CrdtObjectType::Set);
        Self {
            common,
            value: None,
            value_unavailable: None,
            membership_unavailable: Some(non_empty_reason(reason)),
        }
    }

    pub fn common(&self) -> &CrdtCommonFields {
        &self.common
    }

    pub fn value(&self) -> Option<&CrdtValue> {
        self.value.as_ref()
    }

    pub fn value_unavailable(&self) -> Option<&str> {
        self.value_unavailable.as_deref()
    }

    pub fn membership_unavailable(&self) -> Option<&str> {
        self.membership_unavailable.as_deref()
    }

    /// Stable JSON bytes used by HTTP, CLI machine output, Python status and
    /// cross-surface fixture comparisons.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn canonical_digest(&self) -> Result<String, serde_json::Error> {
        Ok(Sha256::digest(self.canonical_bytes()?)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
    }

    /// Computes the one cross-surface mapping.  The structured common fields
    /// remain authoritative; this projection only supplies each adapter's
    /// transport status, CLI exit class, and Python status family.
    pub fn surface_status(&self) -> CrdtSurfaceStatus {
        CrdtSurfaceStatus::from_common(&self.common)
    }
}

fn non_empty_reason(reason: impl Into<String>) -> String {
    let reason = reason.into();
    if reason.is_empty() {
        "outcome_unavailable".to_string()
    } else {
        reason
    }
}

/// Transport projections for one canonical result.  These strings are the
/// stable names used by HTTP, gRPC and Python adapters; this type does not add
/// a Phase 2 operation state or failure class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrdtSurfaceStatus {
    pub http_status: u16,
    pub grpc_code: &'static str,
    pub cli_exit_code: i32,
    pub python_error_code: Option<&'static str>,
}

impl CrdtSurfaceStatus {
    pub fn from_common(common: &CrdtCommonFields) -> Self {
        if common.routing.kind == RoutingOutcomeKind::Unsupported {
            return Self {
                http_status: 501,
                grpc_code: "UNIMPLEMENTED",
                cli_exit_code: 5,
                python_error_code: Some("crdt_unsupported"),
            };
        }

        let (http_status, grpc_code, python_error_code) = match common.failure_class {
            Some(FailureClass::Unauthorized) => (401, "UNAUTHENTICATED", Some("crdt_unauthorized")),
            Some(
                FailureClass::StaleMetadata
                | FailureClass::Gap
                | FailureClass::Overlap
                | FailureClass::EpochMismatch
                | FailureClass::Conflict,
            ) => (409, "ABORTED", Some("crdt_conflict")),
            Some(
                FailureClass::NotLeader
                | FailureClass::NodeUnavailable
                | FailureClass::PrerequisiteMissing,
            ) => (503, "UNAVAILABLE", Some("crdt_unavailable")),
            Some(FailureClass::Timeout) => (408, "DEADLINE_EXCEEDED", Some("crdt_timeout")),
            Some(FailureClass::InvalidRequest) => {
                (400, "INVALID_ARGUMENT", Some("crdt_invalid_request"))
            }
            Some(FailureClass::Internal) => (500, "INTERNAL", Some("crdt_internal")),
            None if common.state == OperationState::Cancelled => {
                (408, "CANCELLED", Some("crdt_cancelled"))
            }
            None if matches!(
                common.state,
                OperationState::Accepted
                    | OperationState::Running
                    | OperationState::RecoveryPending
            ) =>
            {
                (202, "OK", None)
            }
            None if common.state == OperationState::Committed => (200, "OK", None),
            None => (500, "INTERNAL", Some("crdt_internal")),
        };

        let cli_exit_code = if common.state == OperationState::Cancelled {
            130
        } else if common.failure_class == Some(FailureClass::Unauthorized) {
            4
        } else if matches!(
            common.state,
            OperationState::Accepted | OperationState::Running | OperationState::RecoveryPending
        ) {
            2
        } else if common.state == OperationState::RetryableFailure || common.retryable {
            3
        } else if common.failure_class.is_some() || common.state != OperationState::Committed {
            1
        } else {
            0
        };

        Self {
            http_status,
            grpc_code,
            cli_exit_code,
            python_error_code,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        FailureClass, IdempotencyResult, OperationState, RangeIdentity, RoutingOutcome,
        RoutingOutcomeKind,
    };

    use super::{CounterValue, CrdtCommonFields, CrdtObjectType, CrdtOutcome};

    fn common(
        object_type: CrdtObjectType,
        state: OperationState,
        failure_class: Option<FailureClass>,
        routing_kind: RoutingOutcomeKind,
        retryable: bool,
    ) -> CrdtCommonFields {
        let range = RangeIdentity::new("cluster-a", 7, "range-a", None, None, 1, 9);
        CrdtCommonFields {
            object_type,
            object_id: "object-a".to_string(),
            range: range.clone(),
            state_epoch: 9,
            actor: "node-a".into(),
            request_id: "request-a".into(),
            operation_id: "operation-a".to_string(),
            state,
            failure_class,
            routing: RoutingOutcome::new(routing_kind, Some(range), 4, "fixture"),
            retryable,
            idempotency: IdempotencyResult {
                operation_id: "operation-a".to_string(),
                request_id: "request-a".into(),
                first_outcome: "committed".to_string(),
                state,
                duplicate_count: 1,
            },
        }
    }

    #[test]
    fn counter_outcome_flattens_all_common_fields_and_is_deterministic() {
        let outcome = CrdtOutcome::counter(
            common(
                CrdtObjectType::Counter,
                OperationState::Committed,
                None,
                RoutingOutcomeKind::SingleRange,
                false,
            ),
            CounterValue {
                initial_value: -2,
                accepted_delta_total: 7,
                value: 5,
                accepted_operation_versions: BTreeMap::from([("operation-a".to_string(), 8)]),
            },
        );

        let encoded = String::from_utf8(outcome.canonical_bytes().unwrap()).unwrap();
        assert!(encoded.contains("\"object_type\":\"counter\""));
        assert!(encoded.contains("\"range\":"));
        assert!(encoded.contains("\"idempotency\":"));
        assert!(encoded.contains("\"value_type\":\"counter\""));
        assert_eq!(
            outcome.canonical_digest().unwrap(),
            outcome.canonical_digest().unwrap()
        );
        assert_eq!(outcome.surface_status().http_status, 200);
        assert_eq!(outcome.surface_status().cli_exit_code, 0);
    }

    #[test]
    fn unresolved_value_is_explicit_and_keeps_common_failure_fields() {
        let outcome = CrdtOutcome::counter_unavailable(
            common(
                CrdtObjectType::Counter,
                OperationState::RecoveryPending,
                Some(FailureClass::NodeUnavailable),
                RoutingOutcomeKind::Retryable,
                true,
            ),
            "replica_convergence_pending",
        );

        assert!(outcome.value().is_none());
        assert_eq!(
            outcome.value_unavailable(),
            Some("replica_convergence_pending")
        );
        assert_eq!(outcome.surface_status().http_status, 503);
        assert_eq!(outcome.surface_status().grpc_code, "UNAVAILABLE");
        assert_eq!(outcome.surface_status().cli_exit_code, 2);
        assert_eq!(
            outcome.surface_status().python_error_code,
            Some("crdt_unavailable")
        );
    }

    #[test]
    fn all_failure_families_and_unsupported_use_the_fixed_mapping() {
        let cases = [
            (
                Some(FailureClass::Unauthorized),
                401,
                "UNAUTHENTICATED",
                4,
                Some("crdt_unauthorized"),
            ),
            (
                Some(FailureClass::Conflict),
                409,
                "ABORTED",
                1,
                Some("crdt_conflict"),
            ),
            (
                Some(FailureClass::PrerequisiteMissing),
                503,
                "UNAVAILABLE",
                1,
                Some("crdt_unavailable"),
            ),
            (
                Some(FailureClass::Timeout),
                408,
                "DEADLINE_EXCEEDED",
                1,
                Some("crdt_timeout"),
            ),
            (
                Some(FailureClass::InvalidRequest),
                400,
                "INVALID_ARGUMENT",
                1,
                Some("crdt_invalid_request"),
            ),
            (
                Some(FailureClass::Internal),
                500,
                "INTERNAL",
                1,
                Some("crdt_internal"),
            ),
        ];
        for (failure_class, http_status, grpc_code, cli_exit_code, python_error_code) in cases {
            let status = CrdtOutcome::counter_unavailable(
                common(
                    CrdtObjectType::Counter,
                    OperationState::Rejected,
                    failure_class,
                    RoutingOutcomeKind::Blocked,
                    false,
                ),
                "failure",
            )
            .surface_status();
            assert_eq!(status.http_status, http_status);
            assert_eq!(status.grpc_code, grpc_code);
            assert_eq!(status.cli_exit_code, cli_exit_code);
            assert_eq!(status.python_error_code, python_error_code);
        }

        let unsupported = CrdtOutcome::counter_unavailable(
            common(
                CrdtObjectType::Counter,
                OperationState::Rejected,
                Some(FailureClass::PrerequisiteMissing),
                RoutingOutcomeKind::Unsupported,
                false,
            ),
            "unsupported",
        )
        .surface_status();
        assert_eq!(unsupported.http_status, 501);
        assert_eq!(unsupported.grpc_code, "UNIMPLEMENTED");
        assert_eq!(unsupported.cli_exit_code, 5);
        assert_eq!(unsupported.python_error_code, Some("crdt_unsupported"));
    }
}
