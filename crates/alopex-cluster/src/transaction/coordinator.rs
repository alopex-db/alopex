//! Atomic transaction decision coordinator.
//!
//! The coordinator owns the all-participant barriers; its durable ledger keeps
//! the corresponding prepare, discard, and decision acknowledgements so a
//! crash cannot turn an incomplete decision into success.

use std::collections::BTreeSet;

use alopex_core::KVStore;

use crate::{CommittedMetadata, FailureClass, NodeId, OperationState, RangeReplicaDirectory};

use super::recovery::{
    TransactionLedgerAdmission, TransactionRecoveryLedger, TransactionRecoveryRecord,
};

/// Mandatory admission boundary for an authenticated actor and the current
/// committed range/placement fence. A coordinator has no permissive default.
pub trait TransactionAdmissionVerifier {
    /// Fail closed unless the actor, routing view, ownership, and read fence
    /// are still valid for this exact immutable intent.
    fn verify(&self, intent: &TransactionIntent) -> Result<(), TransactionAdmissionError>;
}

/// A verifier used only by [`TransactionCoordinator::new`]. It blocks all
/// distributed execution until a hosting adapter provides a real verifier.
#[derive(Debug, Default, Clone, Copy)]
pub struct BlockedTransactionAdmissionVerifier;

impl TransactionAdmissionVerifier for BlockedTransactionAdmissionVerifier {
    fn verify(&self, _intent: &TransactionIntent) -> Result<(), TransactionAdmissionError> {
        Err(TransactionAdmissionError::PrerequisiteMissing)
    }
}

/// Supplies the committed metadata projection at the instant an admission is
/// checked. Production implementations must obtain the current consensus
/// projection on every call instead of retaining a construction-time snapshot.
pub trait CommittedMetadataProvider {
    /// Return the current committed projection, or `None` when it cannot be
    /// proved available.
    fn current_metadata(&self) -> Option<CommittedMetadata>;
}

impl CommittedMetadataProvider for CommittedMetadata {
    fn current_metadata(&self) -> Option<CommittedMetadata> {
        Some(self.clone())
    }
}

/// Verifies that the host's authenticated principal may act as the intent
/// actor. Authentication tokens themselves never enter this transaction type.
pub trait TransactionActorAuthorizer {
    /// Return true only when the host has authenticated and authorized the
    /// claimed actor for this exact immutable intent.
    fn authorize(&self, actor: &NodeId, intent: &TransactionIntent) -> bool;
}

impl TransactionActorAuthorizer for BTreeSet<NodeId> {
    fn authorize(&self, actor: &NodeId, _intent: &TransactionIntent) -> bool {
        self.contains(actor)
    }
}

/// Verifies every admission against fresh committed metadata and a host-owned
/// actor authorization boundary. `M` and `A` are called on every
/// execute/status/recover entry, so a split, merge, move, replica loss, or
/// credential change cannot be accepted from a stale constructor snapshot.
#[derive(Debug, Clone)]
pub struct CommittedTransactionAdmissionVerifier<M, A> {
    metadata_provider: M,
    actor_authorizer: A,
}

impl<M, A> CommittedTransactionAdmissionVerifier<M, A> {
    /// Create a verifier from a current-metadata provider and an authenticated
    /// actor authorizer owned by the hosting boundary.
    pub fn new(metadata_provider: M, actor_authorizer: A) -> Self {
        Self {
            metadata_provider,
            actor_authorizer,
        }
    }
}

impl<M: CommittedMetadataProvider, A: TransactionActorAuthorizer> TransactionAdmissionVerifier
    for CommittedTransactionAdmissionVerifier<M, A>
{
    fn verify(&self, intent: &TransactionIntent) -> Result<(), TransactionAdmissionError> {
        if !self.actor_authorizer.authorize(&intent.actor, intent) {
            return Err(TransactionAdmissionError::UnauthorizedActor);
        }
        let metadata = self
            .metadata_provider
            .current_metadata()
            .ok_or(TransactionAdmissionError::PrerequisiteMissing)?;
        if metadata.state_version() != intent.read_point.metadata_version {
            return Err(TransactionAdmissionError::StaleMetadata);
        }
        let replicas = RangeReplicaDirectory::from_committed(&metadata);
        for participant in &intent.participants {
            if participant.range.cluster_id != *metadata.cluster_id() {
                return Err(TransactionAdmissionError::OwnershipMismatch);
            }
            let definition = metadata
                .ranges()
                .get(&participant.range.range_id)
                .ok_or(TransactionAdmissionError::StaleMetadata)?;
            if definition.table_id != participant.range.table_id
                || definition.lower_inclusive != participant.range.lower_bound
                || definition.upper_exclusive != participant.range.upper_bound
                || definition.generation != participant.range_generation
            {
                return Err(TransactionAdmissionError::StaleMetadata);
            }
            let eligible = replicas.routing_eligible(&participant.range.range_id);
            let Some(owner) = eligible.first() else {
                return Err(TransactionAdmissionError::PrerequisiteMissing);
            };
            let replica_nodes = eligible
                .iter()
                .skip(1)
                .map(|entry| entry.node_id.clone())
                .collect::<Vec<_>>();
            if participant.placement.owner_node != owner.node_id
                || participant.placement.replica_nodes != replica_nodes
            {
                return Err(TransactionAdmissionError::OwnershipMismatch);
            }
            let data_epoch = metadata
                .range_replicas()
                .get(&participant.range.range_id)
                .and_then(|entries| entries.get(&owner.node_id))
                .map(|evidence| evidence.data_epoch)
                .ok_or(TransactionAdmissionError::PrerequisiteMissing)?;
            if data_epoch != participant.range.data_epoch
                || data_epoch != intent.read_point.data_epoch
            {
                return Err(TransactionAdmissionError::EpochMismatch);
            }
        }
        Ok(())
    }
}

/// Classified fail-closed admission error for adapter mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TransactionAdmissionError {
    /// The authenticated host did not authorize the claimed actor.
    #[error("transaction actor is not authorized")]
    UnauthorizedActor,
    /// The committed metadata version or range definition changed.
    #[error("transaction routing metadata is stale")]
    StaleMetadata,
    /// The current committed owner/replica set differs from the frozen intent.
    #[error("transaction range ownership does not match committed metadata")]
    OwnershipMismatch,
    /// The fixed user-data epoch is no longer the committed participant epoch.
    #[error("transaction data epoch does not match committed metadata")]
    EpochMismatch,
    /// No complete verified placement/capability is currently available.
    #[error("transaction admission prerequisite is unavailable")]
    PrerequisiteMissing,
}

impl TransactionAdmissionError {
    /// Shared outcome classification for adapters.
    pub const fn failure_class(self) -> FailureClass {
        match self {
            Self::UnauthorizedActor => FailureClass::Unauthorized,
            Self::StaleMetadata => FailureClass::StaleMetadata,
            Self::OwnershipMismatch => FailureClass::NotLeader,
            Self::EpochMismatch => FailureClass::EpochMismatch,
            Self::PrerequisiteMissing => FailureClass::PrerequisiteMissing,
        }
    }

    /// Stable reason code for surface-neutral error mapping.
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::UnauthorizedActor => "transaction_actor_unauthorized",
            Self::StaleMetadata => "transaction_metadata_stale",
            Self::OwnershipMismatch => "transaction_ownership_mismatch",
            Self::EpochMismatch => "transaction_epoch_mismatch",
            Self::PrerequisiteMissing => "transaction_prerequisite_missing",
        }
    }
}
use super::{
    TransactionDecision, TransactionIntent, TransactionOutcome, TransactionOutcomeError,
    TransactionParticipant, TransactionRecoveryError,
};

/// One participant acknowledgement observed by the coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionParticipantAck {
    /// The participant durably completed the requested operation.
    Durable,
    /// No durable commit decision is known at this participant. Before a
    /// coordinator decision this may permit retry with the same request.
    Rejected {
        /// Stable failure category.
        failure_class: FailureClass,
        /// Stable diagnostic reason.
        reason_code: String,
        /// Whether no commit decision was published and retry is permitted.
        retryable: bool,
    },
    /// The participant cannot prove its durable state. This is never success.
    RecoveryPending {
        /// Stable failure category.
        failure_class: FailureClass,
        /// Stable diagnostic reason.
        reason_code: String,
    },
}

/// Internal participant adapter boundary. Implementations must make every
/// method idempotent for the immutable transaction identity and must never
/// report `Durable` after an unknown WAL fsync result.
pub trait TransactionParticipantDriver {
    /// Durably validate the participant's staged writes at the fixed intent fence.
    fn prepare(
        &mut self,
        intent: &TransactionIntent,
        participant: &TransactionParticipant,
    ) -> TransactionParticipantAck;

    /// Discard local pre-decision work. This has no published commit decision;
    /// it is used only to make a retryable prepare failure safe to retry.
    fn discard(
        &mut self,
        intent: &TransactionIntent,
        participant: &TransactionParticipant,
    ) -> TransactionParticipantAck;

    /// Apply one already-durable coordinator decision at a participant.
    fn apply_decision(
        &mut self,
        intent: &TransactionIntent,
        participant: &TransactionParticipant,
        decision: TransactionDecision,
    ) -> TransactionParticipantAck;

    /// Observe the durable result of an already-published decision. It must
    /// not reapply a write set or send another decision.
    fn decision_status(
        &mut self,
        intent: &TransactionIntent,
        participant: &TransactionParticipant,
        decision: TransactionDecision,
    ) -> TransactionParticipantAck;
}

/// Coordinator that couples participant I/O to durable barrier evidence and a
/// mandatory admission boundary.
pub struct TransactionCoordinator<S, V = BlockedTransactionAdmissionVerifier> {
    ledger: TransactionRecoveryLedger<S>,
    verifier: V,
}

impl<S> TransactionCoordinator<S, BlockedTransactionAdmissionVerifier> {
    /// Create a fail-closed coordinator. Distributed execution remains blocked
    /// until [`Self::with_verifier`] receives an authentication and current
    /// committed-metadata verifier from its host.
    pub fn new(store: S) -> Self {
        Self {
            ledger: TransactionRecoveryLedger::new(store),
            verifier: BlockedTransactionAdmissionVerifier,
        }
    }
}

impl<S, V> TransactionCoordinator<S, V> {
    /// Create a coordinator with the required authentication/ownership/fence
    /// verifier. The verifier is called again for execute, status, and recover.
    pub fn with_verifier(store: S, verifier: V) -> Self {
        Self {
            ledger: TransactionRecoveryLedger::new(store),
            verifier,
        }
    }
}

impl<S: KVStore, V: TransactionAdmissionVerifier> TransactionCoordinator<S, V> {
    /// Execute or safely resume one all-or-nothing transaction request.
    ///
    /// A terminal or in-doubt duplicate never invokes the driver. A retryable
    /// pre-decision failure may resume only after every participant has a
    /// durable discard acknowledgement. Commit is returned only after the
    /// ledger proves every prepare and commit acknowledgement.
    pub fn execute<D: TransactionParticipantDriver>(
        &self,
        intent: &TransactionIntent,
        driver: &mut D,
    ) -> Result<TransactionOutcome, TransactionCoordinatorError> {
        self.verify_intent(intent)?;
        match self.ledger.admit(intent)? {
            TransactionLedgerAdmission::Duplicate(record) => outcome_from_record(record),
            TransactionLedgerAdmission::First(_) | TransactionLedgerAdmission::Resume(_) => {
                self.run_attempt(intent, driver)
            }
        }
    }

    /// Return a durable status only when the caller presents the original
    /// authenticated intent. This method never contacts a participant.
    pub fn status(
        &self,
        intent: &TransactionIntent,
    ) -> Result<Option<TransactionOutcome>, TransactionCoordinatorError> {
        self.verify_intent(intent)?;
        self.ledger
            .read_for_intent(intent)?
            .map(outcome_from_record)
            .transpose()
    }

    /// Reconcile an already-published decision by querying missing durable
    /// participant acknowledgements. It never reapplies participant writes or
    /// resends a decision.
    pub fn recover<D: TransactionParticipantDriver>(
        &self,
        intent: &TransactionIntent,
        driver: &mut D,
    ) -> Result<Option<TransactionOutcome>, TransactionCoordinatorError> {
        self.verify_intent(intent)?;
        let Some(mut record) = self.ledger.read_for_intent(intent)? else {
            return Ok(None);
        };
        let Some(decision) = record.decision() else {
            return Ok(Some(outcome_from_record(record)?));
        };

        for participant in &intent.participants {
            if record
                .decision_acknowledgements
                .contains(&participant.range.range_id)
            {
                continue;
            }
            match driver.decision_status(intent, participant, decision) {
                TransactionParticipantAck::Durable => {
                    record = self.ledger.record_decision_acknowledgement(
                        intent,
                        participant,
                        decision,
                    )?;
                }
                TransactionParticipantAck::Rejected {
                    failure_class,
                    reason_code,
                    ..
                }
                | TransactionParticipantAck::RecoveryPending {
                    failure_class,
                    reason_code,
                } => {
                    return Ok(Some(self.recovery_pending(
                        intent,
                        failure_class,
                        reason_code,
                    )?));
                }
            }
        }
        Ok(Some(outcome_from_record(
            self.ledger.complete_decision(intent)?,
        )?))
    }

    fn run_attempt<D: TransactionParticipantDriver>(
        &self,
        intent: &TransactionIntent,
        driver: &mut D,
    ) -> Result<TransactionOutcome, TransactionCoordinatorError> {
        for participant in &intent.participants {
            match driver.prepare(intent, participant) {
                TransactionParticipantAck::Durable => {
                    self.ledger.record_prepared(intent, participant)?;
                }
                TransactionParticipantAck::Rejected {
                    failure_class,
                    reason_code,
                    retryable: true,
                } => {
                    return self.retryable_after_prepare_failure(
                        intent,
                        driver,
                        failure_class,
                        reason_code,
                    );
                }
                TransactionParticipantAck::Rejected {
                    failure_class,
                    reason_code,
                    retryable: false,
                }
                | TransactionParticipantAck::RecoveryPending {
                    failure_class,
                    reason_code,
                } => {
                    return self.abort_after_prepare_failure(
                        intent,
                        driver,
                        failure_class,
                        reason_code,
                    );
                }
            }
        }

        self.ledger
            .record_decision(intent, TransactionDecision::Commit, None)?;
        self.send_published_decision(intent, driver, TransactionDecision::Commit)
    }

    fn retryable_after_prepare_failure<D: TransactionParticipantDriver>(
        &self,
        intent: &TransactionIntent,
        driver: &mut D,
        failure_class: FailureClass,
        reason_code: String,
    ) -> Result<TransactionOutcome, TransactionCoordinatorError> {
        for participant in &intent.participants {
            match driver.discard(intent, participant) {
                TransactionParticipantAck::Durable => {
                    self.ledger
                        .record_predecision_discard(intent, participant)?;
                }
                TransactionParticipantAck::Rejected {
                    failure_class,
                    reason_code,
                    ..
                }
                | TransactionParticipantAck::RecoveryPending {
                    failure_class,
                    reason_code,
                } => {
                    return self.abort_after_prepare_failure(
                        intent,
                        driver,
                        failure_class,
                        reason_code,
                    );
                }
            }
        }
        outcome_from_record(self.ledger.record_retryable_failure(
            intent,
            failure_class,
            reason_code,
        )?)
    }

    fn abort_after_prepare_failure<D: TransactionParticipantDriver>(
        &self,
        intent: &TransactionIntent,
        driver: &mut D,
        failure_class: FailureClass,
        reason_code: String,
    ) -> Result<TransactionOutcome, TransactionCoordinatorError> {
        self.ledger.record_decision(
            intent,
            TransactionDecision::Abort,
            Some((failure_class, reason_code)),
        )?;
        self.send_published_decision(intent, driver, TransactionDecision::Abort)
    }

    fn send_published_decision<D: TransactionParticipantDriver>(
        &self,
        intent: &TransactionIntent,
        driver: &mut D,
        decision: TransactionDecision,
    ) -> Result<TransactionOutcome, TransactionCoordinatorError> {
        for participant in &intent.participants {
            match driver.apply_decision(intent, participant, decision) {
                TransactionParticipantAck::Durable => {
                    self.ledger
                        .record_decision_acknowledgement(intent, participant, decision)?;
                }
                TransactionParticipantAck::Rejected {
                    failure_class,
                    reason_code,
                    ..
                }
                | TransactionParticipantAck::RecoveryPending {
                    failure_class,
                    reason_code,
                } => return self.recovery_pending(intent, failure_class, reason_code),
            }
        }
        outcome_from_record(self.ledger.complete_decision(intent)?)
    }

    fn recovery_pending(
        &self,
        intent: &TransactionIntent,
        failure_class: FailureClass,
        reason_code: String,
    ) -> Result<TransactionOutcome, TransactionCoordinatorError> {
        outcome_from_record(self.ledger.mark_recovery_pending(
            intent,
            failure_class,
            reason_code,
        )?)
    }

    fn verify_intent(&self, intent: &TransactionIntent) -> Result<(), TransactionCoordinatorError> {
        intent.validate()?;
        self.verifier.verify(intent)?;
        Ok(())
    }
}

fn outcome_from_record(
    record: TransactionRecoveryRecord,
) -> Result<TransactionOutcome, TransactionCoordinatorError> {
    let idempotency = record.idempotency_result();
    TransactionOutcome::new(
        record.intent.transaction_id.clone(),
        record.intent.request_id.clone(),
        record.intent.participants.clone(),
        record.intent.read_point.clone(),
        record.intent.schema_version,
        record.intent.data_epoch,
        super::TransactionIsolation::Snapshot,
        record.state,
        record.failure_class,
        record.reason_code,
        record.intent.routing.clone(),
        record.state == OperationState::RetryableFailure,
        idempotency,
    )
    .map_err(TransactionCoordinatorError::Outcome)
}

/// Coordinator execution or durable recovery failure.
#[derive(Debug, thiserror::Error)]
pub enum TransactionCoordinatorError {
    /// Authentication, ownership, metadata, epoch, or capability admission
    /// failed before any participant I/O.
    #[error(transparent)]
    Admission(#[from] TransactionAdmissionError),
    /// Durable ledger rejected or could not persist a coordinator fact.
    #[error(transparent)]
    Recovery(#[from] TransactionRecoveryError),
    /// A stored record could not form the public common outcome contract.
    #[error(transparent)]
    Outcome(#[from] TransactionOutcomeError),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use super::{
        CommittedMetadataProvider, CommittedTransactionAdmissionVerifier,
        TransactionAdmissionError, TransactionCoordinator, TransactionCoordinatorError,
        TransactionParticipantAck, TransactionParticipantDriver,
    };
    use crate::{
        ClusterReadPoint, CommittedMetadata, FailureClass, NodeId, OperationState, Placement,
        PlacementReadiness, PlacementRole, RangeCoverageProof, RangeId, RangeIdentity,
        RangeReplicaEvidence, RangeReplicaLifecycle, RangeRoutingDefinition, ReadConsistencyMode,
        RequestId, RoutingOutcome, RoutingOutcomeKind, TableRef, TransactionDecision,
        TransactionIntent, TransactionParticipant,
    };

    #[derive(Default)]
    struct Driver {
        prepares: VecDeque<TransactionParticipantAck>,
        discards: VecDeque<TransactionParticipantAck>,
        commits: VecDeque<TransactionParticipantAck>,
        aborts: VecDeque<TransactionParticipantAck>,
        statuses: VecDeque<TransactionParticipantAck>,
        prepare_calls: usize,
        discard_calls: usize,
        commit_calls: usize,
        abort_calls: usize,
        status_calls: usize,
    }

    type TestAdmissionVerifier =
        CommittedTransactionAdmissionVerifier<CommittedMetadata, BTreeSet<NodeId>>;

    fn coordinator() -> TransactionCoordinator<alopex_core::MemoryKV, TestAdmissionVerifier> {
        TransactionCoordinator::with_verifier(alopex_core::MemoryKV::new(), committed_verifier())
    }

    impl Driver {
        fn next(queue: &mut VecDeque<TransactionParticipantAck>) -> TransactionParticipantAck {
            queue
                .pop_front()
                .unwrap_or(TransactionParticipantAck::Durable)
        }
    }

    impl TransactionParticipantDriver for Driver {
        fn prepare(
            &mut self,
            _intent: &TransactionIntent,
            _participant: &TransactionParticipant,
        ) -> TransactionParticipantAck {
            self.prepare_calls += 1;
            Self::next(&mut self.prepares)
        }

        fn discard(
            &mut self,
            _intent: &TransactionIntent,
            _participant: &TransactionParticipant,
        ) -> TransactionParticipantAck {
            self.discard_calls += 1;
            Self::next(&mut self.discards)
        }

        fn apply_decision(
            &mut self,
            _intent: &TransactionIntent,
            _participant: &TransactionParticipant,
            decision: TransactionDecision,
        ) -> TransactionParticipantAck {
            match decision {
                TransactionDecision::Commit => {
                    self.commit_calls += 1;
                    Self::next(&mut self.commits)
                }
                TransactionDecision::Abort => {
                    self.abort_calls += 1;
                    Self::next(&mut self.aborts)
                }
            }
        }

        fn decision_status(
            &mut self,
            _intent: &TransactionIntent,
            _participant: &TransactionParticipant,
            _decision: TransactionDecision,
        ) -> TransactionParticipantAck {
            self.status_calls += 1;
            Self::next(&mut self.statuses)
        }
    }

    fn intent(actor: &str) -> TransactionIntent {
        let participants = ["range-a", "range-b"]
            .into_iter()
            .map(|range_id| TransactionParticipant {
                range: RangeIdentity::new("cluster-a", 7, range_id, None, None, 3, 11),
                range_generation: 1,
                placement: Placement::new(
                    "node-a",
                    Vec::new(),
                    PlacementRole::Owner,
                    PlacementReadiness::Ready,
                    0,
                ),
            })
            .collect::<Vec<_>>();
        TransactionIntent {
            transaction_id: "transaction-1".to_owned(),
            request_id: RequestId::from("request-1"),
            request_fingerprint: "authenticated-request-digest".to_owned(),
            actor: NodeId::from(actor),
            participants,
            read_point: ClusterReadPoint {
                data_epoch: 11,
                metadata_version: 0,
                schema_manifest_id: None,
                range_generations: BTreeMap::from([
                    (RangeId::from("range-a"), 1),
                    (RangeId::from("range-b"), 1),
                ]),
                index_epochs: BTreeMap::new(),
                consistency: ReadConsistencyMode::Strong,
            },
            schema_version: 3,
            data_epoch: 11,
            routing: RoutingOutcome::new(
                RoutingOutcomeKind::MultiRange,
                None,
                0,
                "transaction_multi_range_route",
            ),
        }
    }

    fn committed_metadata() -> CommittedMetadata {
        let request = intent("actor-a");
        let mut metadata = CommittedMetadata::new("cluster-a");
        for participant in &request.participants {
            metadata.record_range_for_apply(RangeRoutingDefinition {
                range_id: participant.range.range_id.clone(),
                table_ref: TableRef::from("default.public.users"),
                table_id: participant.range.table_id,
                lower_inclusive: participant.range.lower_bound.clone(),
                upper_exclusive: participant.range.upper_bound.clone(),
                generation: participant.range_generation,
            });
            metadata.record_replica_for_apply(RangeReplicaEvidence {
                range_id: participant.range.range_id.clone(),
                node_id: participant.placement.owner_node.clone(),
                generation: participant.range_generation,
                schema_manifest_id: None,
                data_epoch: participant.range.data_epoch,
                index_epoch: 0,
                lifecycle: RangeReplicaLifecycle::Ready,
                coverage: Some(RangeCoverageProof {
                    generation: participant.range_generation,
                    lower_inclusive: participant.range.lower_bound.clone(),
                    upper_exclusive: participant.range.upper_bound.clone(),
                    data_epoch: participant.range.data_epoch,
                    index_epoch: 0,
                    content_hash: "verified".to_owned(),
                }),
            });
        }
        metadata
    }

    fn committed_verifier() -> TestAdmissionVerifier {
        CommittedTransactionAdmissionVerifier::new(
            committed_metadata(),
            BTreeSet::from([NodeId::from("actor-a")]),
        )
    }

    #[derive(Clone)]
    struct SwitchingMetadataProvider {
        valid: CommittedMetadata,
        unavailable: CommittedMetadata,
        switched: Arc<AtomicBool>,
    }

    impl CommittedMetadataProvider for SwitchingMetadataProvider {
        fn current_metadata(&self) -> Option<CommittedMetadata> {
            if self.switched.load(Ordering::SeqCst) {
                Some(self.unavailable.clone())
            } else {
                Some(self.valid.clone())
            }
        }
    }

    #[test]
    fn default_constructor_blocks_before_any_participant_io() {
        let coordinator = TransactionCoordinator::new(alopex_core::MemoryKV::new());
        let mut driver = Driver::default();
        assert!(matches!(
            coordinator.execute(&intent("actor-a"), &mut driver),
            Err(TransactionCoordinatorError::Admission(
                TransactionAdmissionError::PrerequisiteMissing
            ))
        ));
        assert_eq!(driver.prepare_calls, 0);
    }

    #[test]
    fn admission_rechecks_actor_metadata_ownership_and_epoch_on_every_entry() {
        let coordinator = coordinator();
        let unauthorized = intent("actor-b");
        let mut driver = Driver::default();
        assert!(matches!(
            coordinator.execute(&unauthorized, &mut driver),
            Err(TransactionCoordinatorError::Admission(
                TransactionAdmissionError::UnauthorizedActor
            ))
        ));
        assert!(matches!(
            coordinator.status(&unauthorized),
            Err(TransactionCoordinatorError::Admission(
                TransactionAdmissionError::UnauthorizedActor
            ))
        ));
        assert!(matches!(
            coordinator.recover(&unauthorized, &mut driver),
            Err(TransactionCoordinatorError::Admission(
                TransactionAdmissionError::UnauthorizedActor
            ))
        ));
        assert_eq!(driver.prepare_calls, 0);
        assert_eq!(driver.status_calls, 0);

        let mut stale = intent("actor-a");
        stale.read_point.metadata_version = 1;
        stale.routing.metadata_version = 1;
        for participant in &mut stale.participants {
            participant.placement.placement_epoch = 1;
        }
        assert!(matches!(
            coordinator.execute(&stale, &mut driver),
            Err(TransactionCoordinatorError::Admission(
                TransactionAdmissionError::StaleMetadata
            ))
        ));

        let mut moved = intent("actor-a");
        moved.participants[0].placement.owner_node = NodeId::from("node-b");
        assert!(matches!(
            coordinator.execute(&moved, &mut driver),
            Err(TransactionCoordinatorError::Admission(
                TransactionAdmissionError::OwnershipMismatch
            ))
        ));

        let mut epoch_changed = intent("actor-a");
        epoch_changed.data_epoch = 10;
        epoch_changed.read_point.data_epoch = 10;
        for participant in &mut epoch_changed.participants {
            participant.range.data_epoch = 10;
        }
        assert!(matches!(
            coordinator.execute(&epoch_changed, &mut driver),
            Err(TransactionCoordinatorError::Admission(
                TransactionAdmissionError::EpochMismatch
            ))
        ));
        assert_eq!(driver.prepare_calls, 0);
    }

    #[test]
    fn current_metadata_provider_is_rechecked_after_admission() {
        let switched = Arc::new(AtomicBool::new(false));
        let provider = SwitchingMetadataProvider {
            valid: committed_metadata(),
            unavailable: CommittedMetadata::new("cluster-a"),
            switched: switched.clone(),
        };
        let coordinator = TransactionCoordinator::with_verifier(
            alopex_core::MemoryKV::new(),
            CommittedTransactionAdmissionVerifier::new(
                provider,
                BTreeSet::from([NodeId::from("actor-a")]),
            ),
        );
        let request = intent("actor-a");
        coordinator
            .execute(&request, &mut Driver::default())
            .unwrap();
        switched.store(true, Ordering::SeqCst);
        assert!(matches!(
            coordinator.status(&request),
            Err(TransactionCoordinatorError::Admission(
                TransactionAdmissionError::StaleMetadata
            ))
        ));
    }

    #[test]
    fn in_flight_duplicate_returns_accepted_without_participant_io() {
        let coordinator = coordinator();
        let request = intent("actor-a");
        assert!(matches!(
            coordinator.ledger.admit(&request).unwrap(),
            super::TransactionLedgerAdmission::First(_)
        ));
        let mut duplicate_driver = Driver::default();
        let duplicate = coordinator
            .execute(&request, &mut duplicate_driver)
            .unwrap();
        assert_eq!(duplicate.state, OperationState::Accepted);
        assert_eq!(duplicate_driver.prepare_calls, 0);
        assert_eq!(duplicate_driver.commit_calls, 0);
        assert_eq!(duplicate_driver.abort_calls, 0);
    }

    #[test]
    fn commit_requires_every_durable_ack_and_duplicate_never_replays() {
        let coordinator = coordinator();
        let mut driver = Driver::default();
        let request = intent("actor-a");
        let committed = coordinator.execute(&request, &mut driver).unwrap();
        assert_eq!(committed.state, OperationState::Committed);
        assert_eq!(driver.prepare_calls, 2);
        assert_eq!(driver.commit_calls, 2);

        let duplicate = coordinator.execute(&request, &mut driver).unwrap();
        assert_eq!(duplicate.state, OperationState::Committed);
        assert_eq!(duplicate.idempotency.duplicate_count, 1);
        assert_eq!(driver.prepare_calls, 2);
        assert_eq!(driver.commit_calls, 2);
    }

    #[test]
    fn prepare_failure_aborts_every_participant_before_terminal_failure() {
        let coordinator = coordinator();
        let mut driver = Driver {
            prepares: VecDeque::from([TransactionParticipantAck::Rejected {
                failure_class: FailureClass::Conflict,
                reason_code: "prepare_conflict".to_owned(),
                retryable: false,
            }]),
            ..Driver::default()
        };
        let outcome = coordinator
            .execute(&intent("actor-a"), &mut driver)
            .unwrap();
        assert_eq!(outcome.state, OperationState::TerminalFailure);
        assert_eq!(driver.prepare_calls, 1);
        assert_eq!(driver.abort_calls, 2);
        assert_eq!(driver.commit_calls, 0);
    }

    #[test]
    fn retryable_prepare_failure_discards_all_then_resumes_same_request() {
        let coordinator = coordinator();
        let request = intent("actor-a");
        let mut driver = Driver {
            prepares: VecDeque::from([TransactionParticipantAck::Rejected {
                failure_class: FailureClass::Conflict,
                reason_code: "retryable_prepare_conflict".to_owned(),
                retryable: true,
            }]),
            ..Driver::default()
        };
        let retryable = coordinator.execute(&request, &mut driver).unwrap();
        assert_eq!(retryable.state, OperationState::RetryableFailure);
        assert_eq!(driver.discard_calls, 2);
        assert_eq!(driver.abort_calls, 0);

        let committed = coordinator.execute(&request, &mut driver).unwrap();
        assert_eq!(committed.state, OperationState::Committed);
        assert_eq!(committed.idempotency.duplicate_count, 1);
    }

    #[test]
    fn unknown_commit_ack_is_pending_and_status_never_replays() {
        let coordinator = coordinator();
        let request = intent("actor-a");
        let mut driver = Driver {
            commits: VecDeque::from([TransactionParticipantAck::RecoveryPending {
                failure_class: FailureClass::NodeUnavailable,
                reason_code: "commit_ack_unknown".to_owned(),
            }]),
            statuses: VecDeque::from([TransactionParticipantAck::RecoveryPending {
                failure_class: FailureClass::Timeout,
                reason_code: "recovery_status_timeout".to_owned(),
            }]),
            ..Driver::default()
        };
        let outcome = coordinator.execute(&request, &mut driver).unwrap();
        assert_eq!(outcome.state, OperationState::RecoveryPending);
        let writes = (
            driver.prepare_calls,
            driver.commit_calls,
            driver.abort_calls,
        );
        assert_eq!(
            coordinator
                .status(&request)
                .unwrap()
                .expect("record is durable")
                .state,
            OperationState::RecoveryPending
        );
        assert_eq!(
            writes,
            (
                driver.prepare_calls,
                driver.commit_calls,
                driver.abort_calls
            )
        );

        let recovered = coordinator
            .recover(&request, &mut driver)
            .unwrap()
            .expect("record is durable");
        assert_eq!(recovered.state, OperationState::RecoveryPending);
        assert_eq!(
            writes,
            (
                driver.prepare_calls,
                driver.commit_calls,
                driver.abort_calls
            )
        );
        assert_eq!(driver.status_calls, 1);
    }

    #[test]
    fn recovery_collects_missing_acknowledgement_without_reapplying_writes() {
        let coordinator = coordinator();
        let request = intent("actor-a");
        let mut driver = Driver {
            commits: VecDeque::from([
                TransactionParticipantAck::Durable,
                TransactionParticipantAck::RecoveryPending {
                    failure_class: FailureClass::NodeUnavailable,
                    reason_code: "second_commit_ack_unknown".to_owned(),
                },
            ]),
            statuses: VecDeque::from([TransactionParticipantAck::Durable]),
            ..Driver::default()
        };
        assert_eq!(
            coordinator.execute(&request, &mut driver).unwrap().state,
            OperationState::RecoveryPending
        );
        let writes = (
            driver.prepare_calls,
            driver.commit_calls,
            driver.abort_calls,
        );
        assert_eq!(
            coordinator
                .recover(&request, &mut driver)
                .unwrap()
                .expect("record is durable")
                .state,
            OperationState::Committed
        );
        assert_eq!(
            writes,
            (
                driver.prepare_calls,
                driver.commit_calls,
                driver.abort_calls
            )
        );
        assert_eq!(driver.status_calls, 1);
    }

    #[test]
    fn mismatched_actor_cannot_read_or_reuse_a_transaction_identity() {
        let coordinator = coordinator();
        let request = intent("actor-a");
        coordinator
            .execute(&request, &mut Driver::default())
            .unwrap();
        let other_actor = intent("actor-b");
        assert!(coordinator.status(&other_actor).is_err());
        assert!(
            coordinator
                .execute(&other_actor, &mut Driver::default())
                .is_err()
        );
    }

    #[test]
    fn request_identity_cannot_be_reused_with_another_transaction() {
        let coordinator = coordinator();
        coordinator
            .execute(&intent("actor-a"), &mut Driver::default())
            .unwrap();
        let mut conflicting = intent("actor-a");
        conflicting.transaction_id = "transaction-2".to_owned();
        assert!(
            coordinator
                .execute(&conflicting, &mut Driver::default())
                .is_err()
        );
    }

    #[test]
    fn recreated_coordinator_replays_the_durable_duplicate_without_participant_io() {
        let store = alopex_core::MemoryKV::new();
        let request = intent("actor-a");
        let coordinator =
            TransactionCoordinator::with_verifier(store.clone(), committed_verifier());
        coordinator
            .execute(&request, &mut Driver::default())
            .unwrap();
        drop(coordinator);

        let restarted = TransactionCoordinator::with_verifier(store, committed_verifier());
        let mut driver = Driver::default();
        let duplicate = restarted.execute(&request, &mut driver).unwrap();
        assert_eq!(duplicate.state, OperationState::Committed);
        assert_eq!(duplicate.idempotency.duplicate_count, 1);
        assert_eq!(driver.prepare_calls, 0);
        assert_eq!(driver.commit_calls, 0);
        assert_eq!(driver.abort_calls, 0);
    }

    #[test]
    fn malformed_range_generation_is_rejected_before_participant_io() {
        let coordinator = coordinator();
        let mut request = intent("actor-a");
        request.participants[1].range_generation = 2;
        let mut driver = Driver::default();
        assert!(coordinator.execute(&request, &mut driver).is_err());
        assert_eq!(driver.prepare_calls, 0);
    }
}
