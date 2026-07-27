use std::collections::BTreeMap;

use crate::{
    ClusterBootstrapOutcome, FailureClass, NodeId, OperationState, RangeIdentity,
    RangeReplicaDirectory, RoutingOutcome, RoutingOutcomeKind,
};

/// Phase 1 evidence that must be true before a Counter or Set operation can
/// enter the durable operation ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrdtReadinessGate {
    pub phase_one_approved: bool,
    pub phase_one_evidence_complete: bool,
    pub range_lease_valid: bool,
    pub placement_ready: bool,
    pub recovery_ready: bool,
    pub bootstrap: ClusterBootstrapOutcome,
    pub range: RangeIdentity,
    pub metadata_version: u64,
}

impl CrdtReadinessGate {
    /// Derives the placement portion only from committed Phase 1 replica
    /// readiness, never from gossip reachability.
    pub fn from_range_directory(
        phase_one_approved: bool,
        phase_one_evidence_complete: bool,
        range_lease_valid: bool,
        recovery_ready: bool,
        bootstrap: ClusterBootstrapOutcome,
        range: RangeIdentity,
        metadata_version: u64,
        directory: &RangeReplicaDirectory,
    ) -> Self {
        let placement_ready = !directory.routing_eligible(&range.range_id).is_empty();
        Self {
            phase_one_approved,
            phase_one_evidence_complete,
            range_lease_valid,
            placement_ready,
            recovery_ready,
            bootstrap,
            range,
            metadata_version,
        }
    }

    fn blocked_reason(&self) -> Option<&'static str> {
        if !self.phase_one_approved {
            Some("phase1_approval_missing")
        } else if !self.phase_one_evidence_complete {
            Some("phase1_evidence_missing")
        } else if !self.range_lease_valid {
            Some("range_lease_invalid")
        } else if !self.placement_ready {
            Some("placement_not_ready")
        } else if !self.recovery_ready {
            Some("range_recovery_pending")
        } else {
            None
        }
    }
}

/// One deterministic replica observation for a locally durable operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrdtReplicaObservation {
    pub node_id: NodeId,
    pub data_epoch: u64,
    pub accepted_operation_digest: String,
    pub ready: bool,
    pub reachable: bool,
    pub durable_owner: bool,
}

/// Inputs defining a single-node or cluster CRDT completion policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrdtCoordinatorConfig {
    pub gate: CrdtReadinessGate,
    pub local_node: NodeId,
    pub configured_replicas: usize,
    pub quorum: usize,
    pub local_durable: bool,
}

/// A decision made before an adapter reports the public F2 outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrdtCoordinationOutcome {
    pub state: OperationState,
    pub failure_class: Option<FailureClass>,
    pub routing: RoutingOutcome,
    pub retryable: bool,
    /// `true` means a locally durable operation may enter the ledger. A
    /// blocked or retry-before-durability decision must not write it.
    pub permit_local_durability: bool,
    pub pending_reason: Option<String>,
}

/// Deterministic F1-gated cluster convergence classifier. It does not mutate
/// the projection or ledger; callers use `permit_local_durability` to preserve
/// the required pre-execution boundary.
pub struct CrdtConvergenceCoordinator {
    config: CrdtCoordinatorConfig,
}

impl CrdtConvergenceCoordinator {
    pub fn new(config: CrdtCoordinatorConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &CrdtCoordinatorConfig {
        &self.config
    }

    pub fn evaluate(&self, observations: &[CrdtReplicaObservation]) -> CrdtCoordinationOutcome {
        if let Some(reason) = self.config.gate.blocked_reason() {
            return self.blocked(reason);
        }

        match &self.config.gate.bootstrap {
            ClusterBootstrapOutcome::SingleNode => return self.local_committed(),
            ClusterBootstrapOutcome::CapabilityUnavailable { .. } => {
                return self.blocked("cluster_capability_unavailable");
            }
            ClusterBootstrapOutcome::ReadyForClusterControl => {}
        }

        if self.config.configured_replicas < 2
            || self.config.quorum == 0
            || self.config.quorum > self.config.configured_replicas
        {
            return self.invalid_configuration();
        }

        let Some(local) = observations
            .iter()
            .find(|observation| observation.node_id == self.config.local_node)
        else {
            return self.retry_before_durability("local_replica_observation_missing");
        };
        if local.data_epoch != self.config.gate.range.data_epoch {
            return self.epoch_mismatch();
        }
        if !local.ready || !local.reachable {
            return self.retry_before_durability("local_replica_unavailable");
        }
        if observations
            .iter()
            .any(|observation| observation.data_epoch != self.config.gate.range.data_epoch)
        {
            return self.epoch_mismatch();
        }

        let eligible = observations
            .iter()
            .filter(|observation| observation.ready && observation.reachable)
            .collect::<Vec<_>>();
        let matching = eligible
            .iter()
            .filter(|observation| {
                observation.accepted_operation_digest == local.accepted_operation_digest
            })
            .count();
        let has_durable_owner = eligible.iter().any(|observation| observation.durable_owner);
        let converged = if self.config.configured_replicas == 2 {
            eligible.len() == 2 && matching == 2
        } else {
            matching >= self.config.quorum && has_durable_owner
        };
        if converged {
            return CrdtCoordinationOutcome {
                state: OperationState::Committed,
                failure_class: None,
                routing: self.routing(RoutingOutcomeKind::SingleRange, "replica_converged"),
                retryable: false,
                permit_local_durability: true,
                pending_reason: None,
            };
        }

        if self.config.local_durable {
            CrdtCoordinationOutcome {
                state: OperationState::RecoveryPending,
                failure_class: Some(FailureClass::NodeUnavailable),
                routing: self.routing(RoutingOutcomeKind::Retryable, "replica_convergence_pending"),
                retryable: true,
                permit_local_durability: true,
                pending_reason: Some("eligible_replica_digest_not_converged".to_string()),
            }
        } else {
            self.retry_before_durability("replica_convergence_pending")
        }
    }

    fn blocked(&self, reason: impl Into<String>) -> CrdtCoordinationOutcome {
        CrdtCoordinationOutcome {
            state: OperationState::Rejected,
            failure_class: Some(FailureClass::PrerequisiteMissing),
            routing: self.routing(RoutingOutcomeKind::Blocked, reason),
            retryable: false,
            permit_local_durability: false,
            pending_reason: None,
        }
    }

    fn local_committed(&self) -> CrdtCoordinationOutcome {
        CrdtCoordinationOutcome {
            state: OperationState::Committed,
            failure_class: None,
            routing: self.routing(RoutingOutcomeKind::LocalOnly, "single_node_valid_lease"),
            retryable: false,
            permit_local_durability: true,
            pending_reason: None,
        }
    }

    fn retry_before_durability(&self, reason: impl Into<String>) -> CrdtCoordinationOutcome {
        CrdtCoordinationOutcome {
            state: OperationState::RetryableFailure,
            failure_class: Some(FailureClass::NodeUnavailable),
            routing: self.routing(RoutingOutcomeKind::Retryable, reason),
            retryable: true,
            permit_local_durability: false,
            pending_reason: None,
        }
    }

    fn epoch_mismatch(&self) -> CrdtCoordinationOutcome {
        CrdtCoordinationOutcome {
            state: OperationState::RetryableFailure,
            failure_class: Some(FailureClass::EpochMismatch),
            routing: self.routing(RoutingOutcomeKind::Blocked, "replica_epoch_mismatch"),
            retryable: true,
            permit_local_durability: false,
            pending_reason: None,
        }
    }

    fn invalid_configuration(&self) -> CrdtCoordinationOutcome {
        CrdtCoordinationOutcome {
            state: OperationState::Rejected,
            failure_class: Some(FailureClass::InvalidRequest),
            routing: self.routing(RoutingOutcomeKind::Blocked, "invalid_replica_quorum"),
            retryable: false,
            permit_local_durability: false,
            pending_reason: None,
        }
    }

    fn routing(&self, kind: RoutingOutcomeKind, reason: impl Into<String>) -> RoutingOutcome {
        RoutingOutcome::new(
            kind,
            Some(self.config.gate.range.clone()),
            self.config.gate.metadata_version,
            reason,
        )
    }
}

/// Returns a stable digest-count view used by deterministic fixture reports.
pub fn accepted_digest_counts(observations: &[CrdtReplicaObservation]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for observation in observations {
        *counts
            .entry(observation.accepted_operation_digest.clone())
            .or_insert(0) += 1;
    }
    counts
}
