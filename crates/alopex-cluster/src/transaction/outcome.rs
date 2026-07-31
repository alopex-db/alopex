use std::collections::BTreeSet;

use crate::{
    ClusterReadPoint, FailureClass, IdempotencyResult, OperationState, Placement, RangeIdentity,
    RequestId, RoutingOutcome,
};

/// The only v0.9 distributed-transaction isolation contract.
///
/// A transaction reads from its immutable `ClusterReadPoint`, observes its own
/// writes through the participant layer, and exposes no writes to another
/// transaction before a committed outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionIsolation {
    Snapshot,
}

/// One range and its committed placement captured by a transaction.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionParticipant {
    pub range: RangeIdentity,
    /// Committed range generation captured by the fixed read point.
    pub range_generation: u64,
    pub placement: Placement,
}

impl TransactionParticipant {
    pub fn validate(&self) -> Result<(), TransactionOutcomeError> {
        require_non_empty(
            "participant.range.cluster_id",
            self.range.cluster_id.as_str(),
        )?;
        require_non_empty("participant.range.range_id", self.range.range_id.as_str())?;
        require_non_empty(
            "participant.placement.owner_node",
            self.placement.owner_node.as_str(),
        )?;
        if self.range_generation == 0 {
            return Err(TransactionOutcomeError::InvalidRangeGeneration {
                range_id: self.range.range_id.as_str().to_string(),
            });
        }
        Ok(())
    }
}

/// Versioned transaction result shared by every public v0.9 surface.
///
/// The type deliberately reuses Phase 1 state, failure, routing, read-point,
/// and idempotency types.  It is an additive logical projection, not a Raft,
/// TSO, coordinator, or transport protocol type.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionOutcome {
    pub transaction_id: String,
    pub request_id: RequestId,
    pub participating_ranges: Vec<TransactionParticipant>,
    pub read_point: ClusterReadPoint,
    pub schema_version: u64,
    pub data_epoch: u64,
    pub isolation: TransactionIsolation,
    pub state: OperationState,
    pub failure_class: Option<FailureClass>,
    pub reason_code: Option<String>,
    pub routing: RoutingOutcome,
    pub retryable: bool,
    pub idempotency: IdempotencyResult,
}

impl TransactionOutcome {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        transaction_id: impl Into<String>,
        request_id: RequestId,
        participating_ranges: Vec<TransactionParticipant>,
        read_point: ClusterReadPoint,
        schema_version: u64,
        data_epoch: u64,
        isolation: TransactionIsolation,
        state: OperationState,
        failure_class: Option<FailureClass>,
        reason_code: Option<String>,
        routing: RoutingOutcome,
        retryable: bool,
        idempotency: IdempotencyResult,
    ) -> Result<Self, TransactionOutcomeError> {
        let outcome = Self {
            transaction_id: transaction_id.into(),
            request_id,
            participating_ranges,
            read_point,
            schema_version,
            data_epoch,
            isolation,
            state,
            failure_class,
            reason_code,
            routing,
            retryable,
            idempotency,
        };
        outcome.validate()?;
        Ok(outcome)
    }

    /// A committed outcome is the only successful terminal transaction state.
    /// `recovery_pending` is intentionally observable but never successful.
    pub fn is_success(&self) -> bool {
        self.state == OperationState::Committed
    }

    pub fn is_recovery_pending(&self) -> bool {
        self.state == OperationState::RecoveryPending
    }

    pub fn validate(&self) -> Result<(), TransactionOutcomeError> {
        require_non_empty("transaction_id", &self.transaction_id)?;
        require_non_empty("request_id", self.request_id.as_str())?;
        require_non_empty("routing.reason_code", &self.routing.reason_code)?;

        if self.participating_ranges.is_empty() {
            return Err(TransactionOutcomeError::MissingParticipants);
        }

        let mut seen_ranges = BTreeSet::new();
        for participant in &self.participating_ranges {
            participant.validate()?;
            let range_id = participant.range.range_id.clone();
            if !seen_ranges.insert(range_id.clone()) {
                return Err(TransactionOutcomeError::DuplicateParticipant {
                    range_id: range_id.as_str().to_string(),
                });
            }
            let Some(read_point_generation) = self.read_point.range_generations.get(&range_id)
            else {
                return Err(TransactionOutcomeError::ReadPointMissingRange {
                    range_id: range_id.as_str().to_string(),
                });
            };
            if *read_point_generation != participant.range_generation {
                return Err(TransactionOutcomeError::RangeGenerationMismatch {
                    range_id: range_id.as_str().to_string(),
                    participant_generation: participant.range_generation,
                    read_point_generation: *read_point_generation,
                });
            }
            if participant.range.schema_version != self.schema_version {
                return Err(TransactionOutcomeError::SchemaVersionMismatch {
                    range_id: range_id.as_str().to_string(),
                    participant_schema_version: participant.range.schema_version,
                    outcome_schema_version: self.schema_version,
                });
            }
            if participant.range.data_epoch != self.data_epoch {
                return Err(TransactionOutcomeError::ParticipantEpochMismatch {
                    range_id: range_id.as_str().to_string(),
                    participant_data_epoch: participant.range.data_epoch,
                    outcome_data_epoch: self.data_epoch,
                });
            }
        }

        if self.read_point.data_epoch != self.data_epoch {
            return Err(TransactionOutcomeError::ReadPointEpochMismatch {
                read_point_epoch: self.read_point.data_epoch,
                outcome_epoch: self.data_epoch,
            });
        }
        if self.routing.metadata_version != self.read_point.metadata_version {
            return Err(TransactionOutcomeError::RoutingMetadataMismatch {
                routing_version: self.routing.metadata_version,
                read_point_version: self.read_point.metadata_version,
            });
        }
        match (
            self.participating_ranges.len(),
            self.routing.kind,
            &self.routing.range_identity,
        ) {
            (1, crate::RoutingOutcomeKind::SingleRange, Some(identity))
                if identity == &self.participating_ranges[0].range => {}
            (count, crate::RoutingOutcomeKind::MultiRange, None) if count > 1 => {}
            _ => return Err(TransactionOutcomeError::RoutingParticipantMismatch),
        }

        validate_state_failure(self.state, self.failure_class, self.reason_code.as_deref())?;
        validate_retryability(self.state, self.retryable)?;
        validate_idempotency(
            &self.idempotency,
            &self.transaction_id,
            &self.request_id,
            self.state,
        )
    }
}

/// Invalid public transaction-outcome combinations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransactionOutcomeError {
    #[error("{field} must not be empty")]
    EmptyIdentity { field: &'static str },
    #[error("transaction outcome requires at least one participating range")]
    MissingParticipants,
    #[error("range {range_id} appears more than once in the participant set")]
    DuplicateParticipant { range_id: String },
    #[error("range {range_id} has no committed generation")]
    InvalidRangeGeneration { range_id: String },
    #[error("read point does not cover participating range {range_id}")]
    ReadPointMissingRange { range_id: String },
    #[error(
        "range {range_id} generation {participant_generation} differs from read point generation {read_point_generation}"
    )]
    RangeGenerationMismatch {
        range_id: String,
        participant_generation: u64,
        read_point_generation: u64,
    },
    #[error(
        "participant {range_id} schema version {participant_schema_version} differs from outcome schema version {outcome_schema_version}"
    )]
    SchemaVersionMismatch {
        range_id: String,
        participant_schema_version: u64,
        outcome_schema_version: u64,
    },
    #[error(
        "participant {range_id} data epoch {participant_data_epoch} differs from outcome data epoch {outcome_data_epoch}"
    )]
    ParticipantEpochMismatch {
        range_id: String,
        participant_data_epoch: u64,
        outcome_data_epoch: u64,
    },
    #[error("read point epoch {read_point_epoch} differs from outcome epoch {outcome_epoch}")]
    ReadPointEpochMismatch {
        read_point_epoch: u64,
        outcome_epoch: u64,
    },
    #[error(
        "routing metadata version {routing_version} differs from read point metadata version {read_point_version}"
    )]
    RoutingMetadataMismatch {
        routing_version: u64,
        read_point_version: u64,
    },
    #[error("routing outcome does not exactly describe the immutable participant set")]
    RoutingParticipantMismatch,
    #[error("failure and recovery-pending states require failure_class and reason_code")]
    MissingFailureDetails,
    #[error("non-failure transaction state cannot carry failure_class or reason_code")]
    AmbiguousSuccessFailure,
    #[error("only retryable_failure may set retryable=true")]
    InvalidRetryability,
    #[error("idempotency identity or state differs from the transaction outcome")]
    IdempotencyMismatch,
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), TransactionOutcomeError> {
    if value.is_empty() {
        Err(TransactionOutcomeError::EmptyIdentity { field })
    } else {
        Ok(())
    }
}

fn validate_state_failure(
    state: OperationState,
    failure_class: Option<FailureClass>,
    reason_code: Option<&str>,
) -> Result<(), TransactionOutcomeError> {
    let requires_failure = matches!(
        state,
        OperationState::Rejected
            | OperationState::RetryableFailure
            | OperationState::TerminalFailure
            | OperationState::RecoveryPending
    );
    if requires_failure && (failure_class.is_none() || reason_code.is_none_or(str::is_empty)) {
        return Err(TransactionOutcomeError::MissingFailureDetails);
    }
    if !requires_failure && (failure_class.is_some() || reason_code.is_some()) {
        return Err(TransactionOutcomeError::AmbiguousSuccessFailure);
    }
    Ok(())
}

fn validate_retryability(
    state: OperationState,
    retryable: bool,
) -> Result<(), TransactionOutcomeError> {
    if retryable != (state == OperationState::RetryableFailure) {
        return Err(TransactionOutcomeError::InvalidRetryability);
    }
    Ok(())
}

fn validate_idempotency(
    idempotency: &IdempotencyResult,
    transaction_id: &str,
    request_id: &RequestId,
    state: OperationState,
) -> Result<(), TransactionOutcomeError> {
    require_non_empty("idempotency.operation_id", &idempotency.operation_id)?;
    require_non_empty("idempotency.request_id", idempotency.request_id.as_str())?;
    require_non_empty("idempotency.first_outcome", &idempotency.first_outcome)?;
    if idempotency.operation_id != transaction_id
        || idempotency.request_id != *request_id
        || idempotency.state != state
    {
        return Err(TransactionOutcomeError::IdempotencyMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        TransactionIsolation, TransactionOutcome, TransactionOutcomeError, TransactionParticipant,
    };
    use crate::{
        ClusterReadPoint, FailureClass, IdempotencyResult, OperationState, Placement,
        PlacementReadiness, PlacementRole, RangeId, RangeIdentity, ReadConsistencyMode, RequestId,
        RoutingOutcome, RoutingOutcomeKind,
    };

    fn participant(range_id: &str) -> TransactionParticipant {
        TransactionParticipant {
            range: RangeIdentity::new("cluster-a", 7, range_id, None, None, 3, 11),
            range_generation: 1,
            placement: Placement::new(
                "node-a",
                Vec::new(),
                PlacementRole::Owner,
                PlacementReadiness::Ready,
                5,
            ),
        }
    }

    fn read_point(range_ids: &[&str]) -> ClusterReadPoint {
        let range_generations = range_ids
            .iter()
            .map(|range_id| (RangeId::from(*range_id), 1))
            .collect();
        let index_epochs = range_ids
            .iter()
            .map(|range_id| (RangeId::from(*range_id), 0))
            .collect::<BTreeMap<_, _>>();
        ClusterReadPoint {
            data_epoch: 11,
            metadata_version: 8,
            schema_manifest_id: None,
            range_generations,
            index_epochs,
            consistency: ReadConsistencyMode::Strong,
        }
    }

    fn outcome(
        state: OperationState,
        failure_class: Option<FailureClass>,
        reason_code: Option<&str>,
        retryable: bool,
    ) -> TransactionOutcome {
        let participating_ranges = vec![participant("range-a")];
        TransactionOutcome::new(
            "txn-1",
            RequestId::from("request-1"),
            participating_ranges.clone(),
            read_point(&["range-a"]),
            3,
            11,
            TransactionIsolation::Snapshot,
            state,
            failure_class,
            reason_code.map(str::to_owned),
            RoutingOutcome::new(
                RoutingOutcomeKind::SingleRange,
                Some(participating_ranges[0].range.clone()),
                8,
                "transaction_route",
            ),
            retryable,
            IdempotencyResult {
                operation_id: "txn-1".into(),
                request_id: RequestId::from("request-1"),
                first_outcome: "begin".into(),
                state,
                duplicate_count: 0,
            },
        )
        .expect("test outcome is valid")
    }

    #[test]
    fn committed_is_the_only_successful_terminal_outcome() {
        let committed = outcome(OperationState::Committed, None, None, false);
        assert!(committed.is_success());

        let pending = outcome(
            OperationState::RecoveryPending,
            Some(FailureClass::NodeUnavailable),
            Some("decision_unobservable"),
            false,
        );
        assert!(pending.is_recovery_pending());
        assert!(!pending.is_success());
    }

    #[test]
    fn recovery_pending_and_retryability_cannot_masquerade_as_success() {
        let mut pending = outcome(
            OperationState::RecoveryPending,
            Some(FailureClass::NodeUnavailable),
            Some("decision_unobservable"),
            false,
        );
        pending.failure_class = None;
        assert_eq!(
            pending.validate(),
            Err(TransactionOutcomeError::MissingFailureDetails)
        );

        let mut committed = outcome(OperationState::Committed, None, None, false);
        committed.retryable = true;
        assert_eq!(
            committed.validate(),
            Err(TransactionOutcomeError::InvalidRetryability)
        );
    }

    #[test]
    fn participant_and_idempotency_mismatches_are_rejected() {
        let mut missing_read_point_range = outcome(OperationState::Committed, None, None, false);
        missing_read_point_range
            .read_point
            .range_generations
            .clear();
        assert!(matches!(
            missing_read_point_range.validate(),
            Err(TransactionOutcomeError::ReadPointMissingRange { .. })
        ));

        let mut mismatched_idempotency = outcome(OperationState::Committed, None, None, false);
        mismatched_idempotency.idempotency.state = OperationState::Cancelled;
        assert_eq!(
            mismatched_idempotency.validate(),
            Err(TransactionOutcomeError::IdempotencyMismatch)
        );
    }

    #[test]
    fn unknown_or_missing_contract_fields_are_rejected_by_deserialization() {
        let valid = outcome(OperationState::Committed, None, None, false);
        let mut unknown_state = serde_json::to_value(&valid).expect("serialize valid outcome");
        unknown_state["state"] = json!("in_doubt");
        assert!(serde_json::from_value::<TransactionOutcome>(unknown_state).is_err());

        let mut missing_identity = serde_json::to_value(&valid).expect("serialize valid outcome");
        missing_identity
            .as_object_mut()
            .expect("outcome is an object")
            .remove("transaction_id");
        assert!(serde_json::from_value::<TransactionOutcome>(missing_identity).is_err());

        let mut unknown_field = serde_json::to_value(&valid).expect("serialize valid outcome");
        unknown_field["unapproved_state"] = json!(true);
        assert!(serde_json::from_value::<TransactionOutcome>(unknown_field).is_err());
    }
}
