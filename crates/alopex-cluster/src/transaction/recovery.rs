//! Durable transaction intent, acknowledgement, and recovery evidence.
//!
//! This module is intentionally crate-private at its mutation boundary.  A
//! coordinator may complete a decision only after the durable record proves
//! that every enlisted range prepared and acknowledged that same decision.

use std::collections::BTreeSet;

use alopex_core::{KVStore, KVTransaction, TxnMode};

use crate::{
    ClusterReadPoint, FailureClass, IdempotencyResult, NodeId, OperationState, PlacementReadiness,
    PlacementRole, RangeId, RequestId, RoutingOutcome, RoutingOutcomeKind,
};

use super::TransactionParticipant;

const RECORD_PREFIX: &[u8] = b"alopex/transaction/recovery/v1/record/";
const REQUEST_PREFIX: &[u8] = b"alopex/transaction/recovery/v1/request/";

/// Immutable coordinator intent recorded before participant prepare begins.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionIntent {
    /// Stable transaction identity chosen by the caller/coordinator.
    pub transaction_id: String,
    /// Stable request identity used for restart-safe duplicate detection.
    pub request_id: RequestId,
    /// Digest of the authenticated actor and canonical transaction request.
    pub request_fingerprint: String,
    /// Authenticated actor that owns this transaction request.
    pub actor: NodeId,
    /// Immutable participant set selected from committed metadata.
    pub participants: Vec<TransactionParticipant>,
    /// Shared fixed snapshot fence for every participant.
    pub read_point: ClusterReadPoint,
    /// Schema version captured with the request.
    pub schema_version: u64,
    /// User-data epoch captured with the request.
    pub data_epoch: u64,
    /// Routing classification captured before participant enlistment.
    pub routing: RoutingOutcome,
}

impl TransactionIntent {
    /// Reject malformed or mixed committed-metadata fences before a durable
    /// intent or participant operation is created.
    pub fn validate(&self) -> Result<(), TransactionRecoveryError> {
        if self.transaction_id.is_empty()
            || self.request_id.as_str().is_empty()
            || self.request_fingerprint.is_empty()
            || self.actor.as_str().is_empty()
            || self.participants.is_empty()
            || self.routing.reason_code.is_empty()
            || self.read_point.data_epoch != self.data_epoch
            || self.routing.metadata_version != self.read_point.metadata_version
        {
            return Err(TransactionRecoveryError::InvalidIntent);
        }

        let mut participant_ranges = BTreeSet::new();
        let mut cluster_id = None;
        for participant in &self.participants {
            participant
                .validate()
                .map_err(TransactionRecoveryError::Participant)?;
            if !participant_ranges.insert(participant.range.range_id.clone())
                || participant.range.schema_version != self.schema_version
                || participant.range.data_epoch != self.data_epoch
                || participant.placement.placement_epoch != self.read_point.metadata_version
                || participant.placement.role != PlacementRole::Owner
                || participant.placement.readiness != PlacementReadiness::Ready
                || participant.placement.replica_nodes.iter().any(|node| {
                    node.as_str().is_empty() || node == &participant.placement.owner_node
                })
            {
                return Err(TransactionRecoveryError::InvalidIntent);
            }
            if let Some(expected_cluster) = cluster_id {
                if expected_cluster != &participant.range.cluster_id {
                    return Err(TransactionRecoveryError::InvalidIntent);
                }
            } else {
                cluster_id = Some(&participant.range.cluster_id);
            }
            if self
                .read_point
                .range_generations
                .get(&participant.range.range_id)
                != Some(&participant.range_generation)
            {
                return Err(TransactionRecoveryError::InvalidIntent);
            }
        }
        if self.read_point.range_generations.len() != participant_ranges.len() {
            return Err(TransactionRecoveryError::InvalidIntent);
        }
        match (
            self.participants.len(),
            self.routing.kind,
            &self.routing.range_identity,
        ) {
            (1, RoutingOutcomeKind::SingleRange, Some(identity))
                if identity == &self.participants[0].range => {}
            (count, RoutingOutcomeKind::MultiRange, None) if count > 1 => {}
            _ => return Err(TransactionRecoveryError::InvalidIntent),
        }
        Ok(())
    }

    fn participant_ranges(&self) -> BTreeSet<RangeId> {
        self.participants
            .iter()
            .map(|participant| participant.range.range_id.clone())
            .collect()
    }
}

/// One terminal coordinator decision. It is an internal participant-adapter
/// input, not a transport protocol surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionDecision {
    /// Publish the prepared write set.
    Commit,
    /// Discard the write set without publishing it.
    Abort,
}

/// Internal durable phase. Public results use only [`OperationState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TransactionLedgerPhase {
    IntentRecorded,
    CommitDecision,
    AbortDecision,
    Committed,
    Aborted,
}

impl TransactionLedgerPhase {
    fn decision(self) -> Option<TransactionDecision> {
        match self {
            Self::CommitDecision => Some(TransactionDecision::Commit),
            Self::AbortDecision => Some(TransactionDecision::Abort),
            Self::IntentRecorded | Self::Committed | Self::Aborted => None,
        }
    }

    fn for_decision(decision: TransactionDecision) -> Self {
        match decision {
            TransactionDecision::Commit => Self::CommitDecision,
            TransactionDecision::Abort => Self::AbortDecision,
        }
    }
}

/// Persisted evidence for one coordinator request. The acknowledgement sets
/// are never projected as a public protocol; they are the proof required to
/// turn a decision into a terminal outcome.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TransactionRecoveryRecord {
    pub(crate) intent: TransactionIntent,
    pub(crate) phase: TransactionLedgerPhase,
    pub(crate) state: OperationState,
    #[serde(default)]
    pub(crate) failure_class: Option<FailureClass>,
    #[serde(default)]
    pub(crate) reason_code: Option<String>,
    /// This is immutable after intent admission.
    pub(crate) first_outcome: String,
    #[serde(default)]
    pub(crate) duplicate_count: u64,
    #[serde(default)]
    pub(crate) prepared_ranges: BTreeSet<RangeId>,
    #[serde(default)]
    pub(crate) discarded_ranges: BTreeSet<RangeId>,
    #[serde(default)]
    pub(crate) decision_acknowledgements: BTreeSet<RangeId>,
}

impl TransactionRecoveryRecord {
    pub(crate) fn idempotency_result(&self) -> IdempotencyResult {
        IdempotencyResult {
            operation_id: self.intent.transaction_id.clone(),
            request_id: self.intent.request_id.clone(),
            first_outcome: self.first_outcome.clone(),
            state: self.state,
            duplicate_count: self.duplicate_count,
        }
    }

    pub(crate) fn decision(&self) -> Option<TransactionDecision> {
        self.phase.decision()
    }

    fn has_every_prepared_range(&self) -> bool {
        self.prepared_ranges == self.intent.participant_ranges()
    }

    fn has_every_discarded_range(&self) -> bool {
        self.discarded_ranges == self.intent.participant_ranges()
    }

    fn has_every_decision_acknowledgement(&self) -> bool {
        self.decision_acknowledgements == self.intent.participant_ranges()
    }
}

/// Result of durable request admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TransactionLedgerAdmission {
    First(TransactionRecoveryRecord),
    Resume(TransactionRecoveryRecord),
    Duplicate(TransactionRecoveryRecord),
}

/// Durable coordinator evidence backed by the KV/WAL transaction boundary.
/// It is crate-private so callers cannot manufacture a terminal result around
/// the acknowledgement barriers below.
pub(crate) struct TransactionRecoveryLedger<S> {
    store: S,
}

impl<S> TransactionRecoveryLedger<S> {
    pub(crate) fn new(store: S) -> Self {
        Self { store }
    }
}

impl<S: KVStore> TransactionRecoveryLedger<S> {
    pub(crate) fn admit(
        &self,
        intent: &TransactionIntent,
    ) -> Result<TransactionLedgerAdmission, TransactionRecoveryError> {
        intent.validate()?;
        let mut transaction = self.store.begin(TxnMode::ReadWrite)?;
        let transaction_key = record_key(&intent.transaction_id);
        let request_key = request_key(&intent.request_id);
        let admission = if let Some(encoded) = transaction.get(&transaction_key)? {
            let mut record = decode_record(&encoded)?;
            if record.intent != *intent {
                return Err(TransactionRecoveryError::IdempotencyConflict);
            }
            assert_request_identity(&mut transaction, &request_key, intent)?;
            record.duplicate_count = record
                .duplicate_count
                .checked_add(1)
                .ok_or(TransactionRecoveryError::DuplicateCountOverflow)?;
            let admission = if record.phase == TransactionLedgerPhase::IntentRecorded
                && record.state == OperationState::RetryableFailure
            {
                if !record.has_every_discarded_range() {
                    return Err(TransactionRecoveryError::IncompleteDiscardEvidence);
                }
                record.state = OperationState::Accepted;
                record.failure_class = None;
                record.reason_code = None;
                record.prepared_ranges.clear();
                record.discarded_ranges.clear();
                TransactionLedgerAdmission::Resume(record.clone())
            } else {
                TransactionLedgerAdmission::Duplicate(record.clone())
            };
            write_record(&mut transaction, &record)?;
            admission
        } else {
            if transaction.get(&request_key)?.is_some() {
                return Err(TransactionRecoveryError::IdempotencyConflict);
            }
            let record = TransactionRecoveryRecord {
                intent: intent.clone(),
                phase: TransactionLedgerPhase::IntentRecorded,
                state: OperationState::Accepted,
                failure_class: None,
                reason_code: None,
                first_outcome: "transaction_accepted".to_owned(),
                duplicate_count: 0,
                prepared_ranges: BTreeSet::new(),
                discarded_ranges: BTreeSet::new(),
                decision_acknowledgements: BTreeSet::new(),
            };
            write_record(&mut transaction, &record)?;
            transaction.put(request_key, intent.transaction_id.as_bytes().to_vec())?;
            TransactionLedgerAdmission::First(record)
        };
        transaction.commit_self()?;
        Ok(admission)
    }

    pub(crate) fn record_prepared(
        &self,
        intent: &TransactionIntent,
        participant: &TransactionParticipant,
    ) -> Result<TransactionRecoveryRecord, TransactionRecoveryError> {
        self.update(intent, |record| {
            require_intent_phase(record, TransactionLedgerPhase::IntentRecorded)?;
            require_participant(record, participant)?;
            if !matches!(
                record.state,
                OperationState::Accepted | OperationState::Running
            ) {
                return Err(TransactionRecoveryError::IllegalTransition);
            }
            record.state = OperationState::Running;
            record
                .prepared_ranges
                .insert(participant.range.range_id.clone());
            Ok(())
        })
    }

    pub(crate) fn record_predecision_discard(
        &self,
        intent: &TransactionIntent,
        participant: &TransactionParticipant,
    ) -> Result<TransactionRecoveryRecord, TransactionRecoveryError> {
        self.update(intent, |record| {
            require_intent_phase(record, TransactionLedgerPhase::IntentRecorded)?;
            require_participant(record, participant)?;
            if !matches!(
                record.state,
                OperationState::Accepted | OperationState::Running
            ) {
                return Err(TransactionRecoveryError::IllegalTransition);
            }
            record
                .discarded_ranges
                .insert(participant.range.range_id.clone());
            Ok(())
        })
    }

    pub(crate) fn record_retryable_failure(
        &self,
        intent: &TransactionIntent,
        failure_class: FailureClass,
        reason_code: String,
    ) -> Result<TransactionRecoveryRecord, TransactionRecoveryError> {
        self.update(intent, |record| {
            require_intent_phase(record, TransactionLedgerPhase::IntentRecorded)?;
            if !record.has_every_discarded_range() || reason_code.is_empty() {
                return Err(TransactionRecoveryError::IncompleteDiscardEvidence);
            }
            record.state = OperationState::RetryableFailure;
            record.failure_class = Some(failure_class);
            record.reason_code = Some(reason_code);
            Ok(())
        })
    }

    pub(crate) fn record_decision(
        &self,
        intent: &TransactionIntent,
        decision: TransactionDecision,
        failure: Option<(FailureClass, String)>,
    ) -> Result<TransactionRecoveryRecord, TransactionRecoveryError> {
        self.update(intent, |record| {
            require_intent_phase(record, TransactionLedgerPhase::IntentRecorded)?;
            if decision == TransactionDecision::Commit && !record.has_every_prepared_range() {
                return Err(TransactionRecoveryError::IncompletePrepareEvidence);
            }
            let (failure_class, reason_code) = match failure {
                Some((failure_class, reason_code)) if !reason_code.is_empty() => {
                    (failure_class, reason_code)
                }
                Some(_) => return Err(TransactionRecoveryError::InvalidResolution),
                None => (
                    FailureClass::Internal,
                    "decision_acknowledgements_pending".to_owned(),
                ),
            };
            record.phase = TransactionLedgerPhase::for_decision(decision);
            record.state = OperationState::RecoveryPending;
            record.failure_class = Some(failure_class);
            record.reason_code = Some(reason_code);
            record.decision_acknowledgements.clear();
            Ok(())
        })
    }

    pub(crate) fn record_decision_acknowledgement(
        &self,
        intent: &TransactionIntent,
        participant: &TransactionParticipant,
        decision: TransactionDecision,
    ) -> Result<TransactionRecoveryRecord, TransactionRecoveryError> {
        self.update(intent, |record| {
            if record.phase != TransactionLedgerPhase::for_decision(decision) {
                return Err(TransactionRecoveryError::IllegalTransition);
            }
            require_participant(record, participant)?;
            record
                .decision_acknowledgements
                .insert(participant.range.range_id.clone());
            Ok(())
        })
    }

    pub(crate) fn mark_recovery_pending(
        &self,
        intent: &TransactionIntent,
        failure_class: FailureClass,
        reason_code: String,
    ) -> Result<TransactionRecoveryRecord, TransactionRecoveryError> {
        self.update(intent, |record| {
            if record.decision().is_none() || reason_code.is_empty() {
                return Err(TransactionRecoveryError::IllegalTransition);
            }
            record.state = OperationState::RecoveryPending;
            record.failure_class = Some(failure_class);
            record.reason_code = Some(reason_code);
            Ok(())
        })
    }

    pub(crate) fn complete_decision(
        &self,
        intent: &TransactionIntent,
    ) -> Result<TransactionRecoveryRecord, TransactionRecoveryError> {
        self.update(intent, |record| {
            let Some(decision) = record.decision() else {
                return Err(TransactionRecoveryError::IllegalTransition);
            };
            if !record.has_every_decision_acknowledgement() {
                return Err(TransactionRecoveryError::IncompleteDecisionAcknowledgements);
            }
            match decision {
                TransactionDecision::Commit => {
                    record.phase = TransactionLedgerPhase::Committed;
                    record.state = OperationState::Committed;
                    record.failure_class = None;
                    record.reason_code = None;
                }
                TransactionDecision::Abort => {
                    if record.failure_class.is_none()
                        || record.reason_code.as_deref().is_none_or(str::is_empty)
                    {
                        return Err(TransactionRecoveryError::InvalidResolution);
                    }
                    record.phase = TransactionLedgerPhase::Aborted;
                    record.state = OperationState::TerminalFailure;
                }
            }
            Ok(())
        })
    }

    pub(crate) fn read_for_intent(
        &self,
        intent: &TransactionIntent,
    ) -> Result<Option<TransactionRecoveryRecord>, TransactionRecoveryError> {
        intent.validate()?;
        let mut transaction = self.store.begin(TxnMode::ReadOnly)?;
        let result = transaction
            .get(&record_key(&intent.transaction_id))?
            .map(|encoded| decode_record(&encoded))
            .transpose()?;
        transaction.rollback_self()?;
        if let Some(record) = &result
            && record.intent != *intent
        {
            return Err(TransactionRecoveryError::IdempotencyConflict);
        }
        Ok(result)
    }

    fn update(
        &self,
        intent: &TransactionIntent,
        update: impl FnOnce(&mut TransactionRecoveryRecord) -> Result<(), TransactionRecoveryError>,
    ) -> Result<TransactionRecoveryRecord, TransactionRecoveryError> {
        let mut transaction = self.store.begin(TxnMode::ReadWrite)?;
        let key = record_key(&intent.transaction_id);
        let encoded = transaction
            .get(&key)?
            .ok_or(TransactionRecoveryError::MissingTransaction)?;
        let mut record = decode_record(&encoded)?;
        if record.intent != *intent {
            return Err(TransactionRecoveryError::IdempotencyConflict);
        }
        update(&mut record)?;
        write_record(&mut transaction, &record)?;
        transaction.commit_self()?;
        Ok(record)
    }
}

fn require_intent_phase(
    record: &TransactionRecoveryRecord,
    phase: TransactionLedgerPhase,
) -> Result<(), TransactionRecoveryError> {
    if record.phase == phase {
        Ok(())
    } else {
        Err(TransactionRecoveryError::IllegalTransition)
    }
}

fn require_participant(
    record: &TransactionRecoveryRecord,
    participant: &TransactionParticipant,
) -> Result<(), TransactionRecoveryError> {
    if record
        .intent
        .participants
        .iter()
        .any(|known| known == participant)
    {
        Ok(())
    } else {
        Err(TransactionRecoveryError::UnknownParticipant)
    }
}

fn write_record<'a, T: KVTransaction<'a>>(
    transaction: &mut T,
    record: &TransactionRecoveryRecord,
) -> Result<(), TransactionRecoveryError> {
    transaction.put(
        record_key(&record.intent.transaction_id),
        serde_json::to_vec(record)?,
    )?;
    Ok(())
}

fn decode_record(encoded: &[u8]) -> Result<TransactionRecoveryRecord, TransactionRecoveryError> {
    Ok(serde_json::from_slice(encoded)?)
}

fn record_key(transaction_id: &str) -> Vec<u8> {
    let mut key = RECORD_PREFIX.to_vec();
    key.extend_from_slice(&(transaction_id.len() as u32).to_be_bytes());
    key.extend_from_slice(transaction_id.as_bytes());
    key
}

fn request_key(request_id: &RequestId) -> Vec<u8> {
    let encoded = request_id.as_str().as_bytes();
    let mut key = REQUEST_PREFIX.to_vec();
    key.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    key.extend_from_slice(encoded);
    key
}

fn assert_request_identity<'a, T: KVTransaction<'a>>(
    transaction: &mut T,
    key: &Vec<u8>,
    intent: &TransactionIntent,
) -> Result<(), TransactionRecoveryError> {
    match transaction.get(key)? {
        Some(existing_transaction_id)
            if existing_transaction_id == intent.transaction_id.as_bytes() =>
        {
            Ok(())
        }
        Some(_) => Err(TransactionRecoveryError::IdempotencyConflict),
        None => {
            transaction.put(key.to_vec(), intent.transaction_id.as_bytes().to_vec())?;
            Ok(())
        }
    }
}

/// Failure while recording or reading durable coordinator evidence.
#[derive(Debug, thiserror::Error)]
pub enum TransactionRecoveryError {
    #[error("transaction intent is invalid")]
    InvalidIntent,
    #[error("transaction participant is invalid: {0}")]
    Participant(#[from] super::TransactionOutcomeError),
    #[error("transaction recovery storage failed: {0}")]
    Storage(#[from] alopex_core::Error),
    #[error("transaction recovery record serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("transaction intent is not retained")]
    MissingTransaction,
    #[error("transaction idempotency identity conflicts with retained intent")]
    IdempotencyConflict,
    #[error("transaction duplicate count overflowed")]
    DuplicateCountOverflow,
    #[error("transaction ledger transition is not permitted")]
    IllegalTransition,
    #[error("transaction commit requires durable prepare evidence from every participant")]
    IncompletePrepareEvidence,
    #[error("transaction retry requires durable discard evidence from every participant")]
    IncompleteDiscardEvidence,
    #[error(
        "transaction terminal decision requires durable acknowledgement from every participant"
    )]
    IncompleteDecisionAcknowledgements,
    #[error("transaction acknowledgement names a participant outside the immutable intent")]
    UnknownParticipant,
    #[error("transaction decision resolution is invalid")]
    InvalidResolution,
}
