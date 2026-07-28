use crate::{FailureClass, OperationState, RoutingOutcome, RoutingOutcomeKind};

/// Lifecycle action requested at the CRDT public boundary.  Internal range
/// reconciliation uses the existing Phase 1 diagnostics path and is never
/// admitted as a public CRDT ledger operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrdtLifecycleAction {
    Create,
    Update,
    Read,
    Merge,
    Reconcile,
    Recover,
    Retire,
    Cancel,
}

impl CrdtLifecycleAction {
    fn is_public_operation(self) -> bool {
        matches!(self, Self::Create | Self::Update | Self::Read)
    }
}

/// Phase 1 freshness observed before a CRDT request is admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrdtRangeFreshness {
    Current,
    StaleMetadata,
    Gap,
    Overlap,
    EpochMismatch,
}

/// Inputs independently established before a public CRDT request can write
/// the durable ledger or report a successful state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrdtPolicyInput {
    pub lifecycle: CrdtLifecycleAction,
    pub authorized: bool,
    pub range_freshness: CrdtRangeFreshness,
    pub chirps_ready: bool,
    pub node_available: bool,
    pub resource_available: bool,
    pub timed_out: bool,
    pub routing: RoutingOutcome,
}

/// A pre-execution fail-closed decision.  `permit_ledger` is the required
/// guard: adapters must not call projection or ledger code when it is false.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrdtPolicyDecision {
    pub state: OperationState,
    pub failure_class: Option<FailureClass>,
    pub routing: RoutingOutcome,
    pub retryable: bool,
    pub permit_ledger: bool,
}

/// Stateless policy shared by each F2 adapter before it creates a durable
/// CRDT envelope.  The decision only uses existing Phase 1 state/failure and
/// routing contracts.
pub struct CrdtPreExecutionPolicy;

impl CrdtPreExecutionPolicy {
    pub fn evaluate(input: &CrdtPolicyInput) -> CrdtPolicyDecision {
        if !input.authorized {
            return Self::rejected(input, FailureClass::Unauthorized, "authorization_denied");
        }
        if !input.lifecycle.is_public_operation() {
            return Self::unsupported(input, "lifecycle_action_unsupported");
        }
        let freshness_failure = match input.range_freshness {
            CrdtRangeFreshness::Current => None,
            CrdtRangeFreshness::StaleMetadata => Some(FailureClass::StaleMetadata),
            CrdtRangeFreshness::Gap => Some(FailureClass::Gap),
            CrdtRangeFreshness::Overlap => Some(FailureClass::Overlap),
            CrdtRangeFreshness::EpochMismatch => Some(FailureClass::EpochMismatch),
        };
        if let Some(failure) = freshness_failure {
            return Self::rejected(input, failure, "range_metadata_not_current");
        }
        if !input.chirps_ready {
            return Self::rejected(
                input,
                FailureClass::PrerequisiteMissing,
                "chirps_prerequisite_missing",
            );
        }
        if !input.node_available {
            return Self::retryable(input, FailureClass::NodeUnavailable, "node_unavailable");
        }
        if !input.resource_available {
            return Self::rejected(input, FailureClass::InvalidRequest, "resource_limit");
        }
        if input.timed_out {
            return Self::retryable(input, FailureClass::Timeout, "timeout");
        }

        CrdtPolicyDecision {
            state: OperationState::Accepted,
            failure_class: None,
            routing: input.routing.clone(),
            retryable: false,
            permit_ledger: true,
        }
    }

    fn rejected(
        input: &CrdtPolicyInput,
        failure_class: FailureClass,
        reason_code: &str,
    ) -> CrdtPolicyDecision {
        CrdtPolicyDecision {
            state: OperationState::Rejected,
            failure_class: Some(failure_class),
            routing: Self::routing(input, RoutingOutcomeKind::Blocked, reason_code),
            retryable: false,
            permit_ledger: false,
        }
    }

    fn retryable(
        input: &CrdtPolicyInput,
        failure_class: FailureClass,
        reason_code: &str,
    ) -> CrdtPolicyDecision {
        CrdtPolicyDecision {
            state: OperationState::RetryableFailure,
            failure_class: Some(failure_class),
            routing: Self::routing(input, RoutingOutcomeKind::Retryable, reason_code),
            retryable: true,
            permit_ledger: false,
        }
    }

    fn unsupported(input: &CrdtPolicyInput, reason_code: &str) -> CrdtPolicyDecision {
        CrdtPolicyDecision {
            state: OperationState::Rejected,
            failure_class: Some(FailureClass::InvalidRequest),
            routing: Self::routing(input, RoutingOutcomeKind::Unsupported, reason_code),
            retryable: false,
            permit_ledger: false,
        }
    }

    fn routing(
        input: &CrdtPolicyInput,
        kind: RoutingOutcomeKind,
        reason_code: &str,
    ) -> RoutingOutcome {
        RoutingOutcome::new(
            kind,
            input.routing.range_identity.clone(),
            input.routing.metadata_version,
            reason_code,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{FailureClass, OperationState, RangeIdentity, RoutingOutcome, RoutingOutcomeKind};

    use super::{CrdtLifecycleAction, CrdtPolicyInput, CrdtPreExecutionPolicy, CrdtRangeFreshness};

    fn input() -> CrdtPolicyInput {
        let range = RangeIdentity::new("cluster-a", 7, "range-a", None, None, 1, 9);
        CrdtPolicyInput {
            lifecycle: CrdtLifecycleAction::Update,
            authorized: true,
            range_freshness: CrdtRangeFreshness::Current,
            chirps_ready: true,
            node_available: true,
            resource_available: true,
            timed_out: false,
            routing: RoutingOutcome::new(RoutingOutcomeKind::SingleRange, Some(range), 4, "ok"),
        }
    }

    #[test]
    fn valid_public_operation_is_admitted_without_claiming_commit() {
        let decision = CrdtPreExecutionPolicy::evaluate(&input());
        assert_eq!(decision.state, OperationState::Accepted);
        assert_eq!(decision.failure_class, None);
        assert!(decision.permit_ledger);
    }

    #[test]
    fn authorization_and_freshness_fail_closed_before_the_ledger() {
        let mut unauthorized = input();
        unauthorized.authorized = false;
        let unauthorized = CrdtPreExecutionPolicy::evaluate(&unauthorized);
        assert_eq!(unauthorized.failure_class, Some(FailureClass::Unauthorized));
        assert!(!unauthorized.permit_ledger);

        for (freshness, expected) in [
            (
                CrdtRangeFreshness::StaleMetadata,
                FailureClass::StaleMetadata,
            ),
            (CrdtRangeFreshness::Gap, FailureClass::Gap),
            (CrdtRangeFreshness::Overlap, FailureClass::Overlap),
            (
                CrdtRangeFreshness::EpochMismatch,
                FailureClass::EpochMismatch,
            ),
        ] {
            let mut stale = input();
            stale.range_freshness = freshness;
            let decision = CrdtPreExecutionPolicy::evaluate(&stale);
            assert_eq!(decision.state, OperationState::Rejected);
            assert_eq!(decision.failure_class, Some(expected));
            assert!(!decision.permit_ledger);
        }
    }

    #[test]
    fn unavailable_resource_timeout_and_internal_lifecycle_never_mutate() {
        let mut unavailable = input();
        unavailable.node_available = false;
        let unavailable = CrdtPreExecutionPolicy::evaluate(&unavailable);
        assert_eq!(unavailable.state, OperationState::RetryableFailure);
        assert_eq!(
            unavailable.failure_class,
            Some(FailureClass::NodeUnavailable)
        );
        assert!(!unavailable.permit_ledger);

        let mut resource = input();
        resource.resource_available = false;
        let resource = CrdtPreExecutionPolicy::evaluate(&resource);
        assert_eq!(resource.failure_class, Some(FailureClass::InvalidRequest));
        assert_eq!(resource.routing.reason_code, "resource_limit");
        assert!(!resource.permit_ledger);

        let mut timeout = input();
        timeout.timed_out = true;
        let timeout = CrdtPreExecutionPolicy::evaluate(&timeout);
        assert_eq!(timeout.failure_class, Some(FailureClass::Timeout));
        assert!(timeout.retryable);
        assert!(!timeout.permit_ledger);

        let mut lifecycle = input();
        lifecycle.lifecycle = CrdtLifecycleAction::Reconcile;
        let lifecycle = CrdtPreExecutionPolicy::evaluate(&lifecycle);
        assert_eq!(lifecycle.state, OperationState::Rejected);
        assert_eq!(lifecycle.routing.kind, RoutingOutcomeKind::Unsupported);
        assert!(!lifecycle.permit_ledger);
    }
}
