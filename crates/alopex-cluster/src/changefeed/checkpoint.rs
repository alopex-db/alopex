//! Checkpoint acknowledgement, monotonicity, and resume planning contracts.
//!
//! This module owns the logical checkpoint state only.  A caller may advance
//! an acknowledgement to `Committed` only after the Durable adapter has
//! persisted it; an accepted or pending record is intentionally not usable for
//! a durable resume.

use std::collections::BTreeMap;

use crate::{FailureClass, IdempotencyResult, OperationState, RequestId};

use super::{
    AckResult, AckState, ChangefeedModelError, Checkpoint, CheckpointPosition, FeedIdentity,
};

/// Caller-supplied identity and position for one acknowledgement attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckRequest {
    /// Stable operation identity for this acknowledgement lifecycle.
    operation_id: String,
    /// Idempotency key supplied by the caller.
    request_id: RequestId,
    /// Identifier for the acknowledgement itself.
    ack_id: String,
    /// Source replay identity tied to the delivered event/checkpoint.
    source_replay_id: String,
    /// The checkpoint the consumer asks to make durable.
    checkpoint: Checkpoint,
}

impl AckRequest {
    /// Creates a request with non-empty operation, request, acknowledgement,
    /// and source replay identities.
    pub fn new(
        operation_id: impl Into<String>,
        request_id: impl Into<RequestId>,
        ack_id: impl Into<String>,
        source_replay_id: impl Into<String>,
        checkpoint: Checkpoint,
    ) -> Result<Self, CheckpointError> {
        let request = Self {
            operation_id: operation_id.into(),
            request_id: request_id.into(),
            ack_id: ack_id.into(),
            source_replay_id: source_replay_id.into(),
            checkpoint,
        };
        request.validate()?;
        Ok(request)
    }

    fn validate(&self) -> Result<(), CheckpointError> {
        for (field, value) in [
            ("operation_id", self.operation_id.as_str()),
            ("request_id", self.request_id.as_str()),
            ("ack_id", self.ack_id.as_str()),
            ("source_replay_id", self.source_replay_id.as_str()),
        ] {
            if value.is_empty() {
                return Err(CheckpointError::EmptyRequestField { field });
            }
        }
        self.checkpoint
            .validate()
            .map_err(CheckpointError::InvalidCheckpoint)
    }

    /// Returns the stable operation identity.
    #[must_use]
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    /// Returns the idempotency key supplied by the caller.
    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// Returns the acknowledgement identifier.
    #[must_use]
    pub fn ack_id(&self) -> &str {
        &self.ack_id
    }

    /// Returns the source replay identity associated with this checkpoint.
    #[must_use]
    pub fn source_replay_id(&self) -> &str {
        &self.source_replay_id
    }

    /// Returns the validated checkpoint requested for durable storage.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
}

/// In-process representation of acknowledged checkpoint records.
///
/// A Durable transport owns persistence and recovery of this state.  The
/// store never invents a local WAL fallback and never treats `Accepted` or
/// `Pending` records as a committed checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    feed: FeedIdentity,
    records: BTreeMap<String, StoredAck>,
    latest_committed: Option<Checkpoint>,
}

#[derive(Debug, Clone)]
struct StoredAck {
    request: AckRequest,
    ack_state: AckState,
    failure: Option<StoredAckFailure>,
    duplicate_count: u64,
}

#[derive(Debug, Clone, Copy)]
struct StoredAckFailure {
    failure_class: FailureClass,
    reason_code: &'static str,
    retryable: bool,
}

impl CheckpointStore {
    /// Creates an empty store for one exact feed/range/generation identity.
    pub fn new(feed: FeedIdentity) -> Result<Self, CheckpointError> {
        feed.validate().map_err(CheckpointError::InvalidFeed)?;
        Ok(Self {
            feed,
            records: BTreeMap::new(),
            latest_committed: None,
        })
    }

    /// Returns the feed identity enforced by all stored acknowledgements.
    #[must_use]
    pub const fn feed(&self) -> &FeedIdentity {
        &self.feed
    }

    /// Returns the latest checkpoint known to have crossed the Durable commit
    /// boundary.  Pending and accepted acknowledgements are excluded.
    #[must_use]
    pub const fn latest_committed(&self) -> Option<&Checkpoint> {
        self.latest_committed.as_ref()
    }

    fn record(&self, request_id: &str) -> Option<&StoredAck> {
        self.records.get(request_id)
    }

    fn record_mut(&mut self, request_id: &str) -> Option<&mut StoredAck> {
        self.records.get_mut(request_id)
    }

    fn insert(&mut self, request: AckRequest) {
        self.records.insert(
            request.request_id.as_str().to_string(),
            StoredAck {
                request,
                ack_state: AckState::Accepted,
                failure: None,
                duplicate_count: 0,
            },
        );
    }

    fn validate_for_feed(&self, request: &AckRequest) -> Result<(), ResumeFailure> {
        if request.checkpoint.feed_id != self.feed.feed_id
            || request.checkpoint.range_id != self.feed.range.range_id
            || request.checkpoint.generation != self.feed.generation
        {
            return Err(ResumeFailure::terminal(
                FailureClass::InvalidRequest,
                "invalid_checkpoint",
            ));
        }
        if request.checkpoint.epoch != self.feed.range.data_epoch {
            return Err(ResumeFailure::retryable(
                FailureClass::EpochMismatch,
                "range_epoch_mismatch",
            ));
        }
        Ok(())
    }

    fn highest_known_checkpoint(&self) -> Option<&Checkpoint> {
        self.records
            .values()
            .map(|record| &record.request.checkpoint)
            .max_by_key(|checkpoint| CheckpointPosition::from(*checkpoint))
    }
}

/// State machine for acknowledge → durable-write-pending → committed.
#[derive(Debug, Clone)]
pub struct AckProcessor {
    store: CheckpointStore,
}

impl AckProcessor {
    /// Creates a processor backed by a recovered or newly initialized store.
    #[must_use]
    pub const fn new(store: CheckpointStore) -> Self {
        Self { store }
    }

    /// Accepts an acknowledgement without claiming it is durable.
    ///
    /// Identical request/payload replays return the existing state and advance
    /// only the observable duplicate counter.  Reusing a request id for a
    /// different ack id, replay id, or checkpoint returns explicit conflict.
    pub fn accept(&mut self, request: AckRequest) -> AckResult {
        if let Err(error) = request.validate() {
            return rejected_ack(
                &request,
                FailureClass::InvalidRequest,
                error.reason_code(),
                false,
            );
        }
        if let Err(failure) = self.store.validate_for_feed(&request) {
            return rejected_ack(
                &request,
                failure.failure_class,
                failure.reason_code,
                failure.retryable,
            );
        }
        if let Some(existing) = self.store.record_mut(request.request_id.as_str()) {
            if existing.request == request {
                existing.duplicate_count = existing.duplicate_count.saturating_add(1);
                return ack_from_record(existing);
            }
            return rejected_ack(
                &request,
                FailureClass::Conflict,
                "ack_idempotency_conflict",
                false,
            );
        }
        if self
            .store
            .highest_known_checkpoint()
            .is_some_and(|highest| {
                CheckpointPosition::from(&request.checkpoint) <= CheckpointPosition::from(highest)
            })
        {
            return rejected_ack(
                &request,
                FailureClass::Conflict,
                "checkpoint_not_monotonic",
                false,
            );
        }
        self.store.insert(request.clone());
        let record = self
            .store
            .record(request.request_id.as_str())
            .expect("acknowledgement inserted before result construction");
        ack_from_record(record)
    }

    /// Marks an accepted acknowledgement as awaiting the Durable write.
    ///
    /// This operation itself is not proof of durability; callers must still
    /// invoke [`Self::commit_after_durable_write`] after the adapter confirms
    /// persistence.
    pub fn mark_pending(&mut self, request_id: &str) -> Result<AckResult, CheckpointError> {
        let record = self
            .store
            .record_mut(request_id)
            .ok_or_else(|| CheckpointError::UnknownRequest(request_id.to_string()))?;
        if record.ack_state == AckState::Accepted {
            record.ack_state = AckState::Pending;
        }
        Ok(ack_from_record(record))
    }

    /// Crosses the committed boundary only after the Durable adapter confirms
    /// the stored checkpoint.  Calling this is the integration's proof point;
    /// accepted and pending acknowledgements cannot be resumed durably.
    pub fn commit_after_durable_write(
        &mut self,
        request_id: &str,
    ) -> Result<AckResult, CheckpointError> {
        let existing = self
            .store
            .record(request_id)
            .ok_or_else(|| CheckpointError::UnknownRequest(request_id.to_string()))?;
        if matches!(
            existing.ack_state,
            AckState::Committed | AckState::Rejected | AckState::Expired
        ) {
            return Ok(ack_from_record(existing));
        }
        let checkpoint = existing.request.checkpoint.clone();
        if self.store.latest_committed().is_some_and(|committed| {
            CheckpointPosition::from(&checkpoint) < CheckpointPosition::from(committed)
        }) {
            let record = self
                .store
                .record_mut(request_id)
                .expect("record was checked before monotonic commit validation");
            record.ack_state = AckState::Rejected;
            record.failure = Some(StoredAckFailure {
                failure_class: FailureClass::Conflict,
                reason_code: "checkpoint_not_monotonic",
                retryable: false,
            });
            return Ok(ack_from_record(record));
        }
        {
            let record = self
                .store
                .record_mut(request_id)
                .expect("record was checked before durable commit");
            record.ack_state = AckState::Committed;
        }
        self.store.latest_committed = Some(checkpoint);
        Ok(ack_from_record(
            self.store
                .record(request_id)
                .expect("record remains after durable commit"),
        ))
    }

    /// Returns the retained store so a Durable backend can persist or recover
    /// it without rebuilding acknowledgement state from transient delivery.
    #[must_use]
    pub fn into_store(self) -> CheckpointStore {
        self.store
    }
}

/// Evidence supplied by the range/retention/Durable integration before resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeSourceStatus {
    /// The source is reachable and range metadata remains current.
    Ready,
    /// Placement metadata must be refreshed before reading.
    MetadataStale,
    /// Source order has a discontinuity.
    Gap,
    /// Source range generation or epoch no longer continues the cursor.
    EpochMismatch,
    /// Durable storage or profile evidence is no longer available.
    DurableUnavailable,
    /// The range source is temporarily unavailable.
    NodeUnavailable,
}

/// Canonical resume decision, including the checkpoint only when delivery may
/// continue strictly after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumePlan {
    /// Checkpoint after which delivery may resume on a ready source.
    pub checkpoint: Option<Checkpoint>,
    /// Canonical operation state for the resume request.
    pub operation_state: OperationState,
    /// Stable failure classification when delivery cannot continue.
    pub failure_class: Option<FailureClass>,
    /// Stable machine-readable reason for the decision.
    pub reason_code: Option<String>,
    /// Whether retrying after remediation can be valid.
    pub retryable: bool,
}

impl ResumePlan {
    /// Returns whether event delivery may continue after [`Self::checkpoint`].
    #[must_use]
    pub const fn can_resume(&self) -> bool {
        self.checkpoint.is_some() && self.failure_class.is_none()
    }
}

/// Plans resumption against one feed and its earliest retained position.
#[derive(Debug, Clone)]
pub struct ResumePlanner {
    feed: FeedIdentity,
    retained_through: Option<Checkpoint>,
}

impl ResumePlanner {
    /// Creates a planner for one feed and optional earliest retained position.
    pub fn new(
        feed: FeedIdentity,
        retained_through: Option<Checkpoint>,
    ) -> Result<Self, CheckpointError> {
        feed.validate().map_err(CheckpointError::InvalidFeed)?;
        if let Some(checkpoint) = &retained_through {
            checkpoint
                .validate()
                .map_err(CheckpointError::InvalidCheckpoint)?;
        }
        Ok(Self {
            feed,
            retained_through,
        })
    }

    /// Returns an explicit plan for an acknowledgement and source condition.
    ///
    /// Only a committed acknowledgement yields an eligible checkpoint.  An
    /// uncommitted ack, retention-expired checkpoint, or any source failure is
    /// returned as a classified non-success rather than an empty delivery.
    #[must_use]
    pub fn plan(&self, acknowledgement: &AckResult, source: ResumeSourceStatus) -> ResumePlan {
        let Some(checkpoint) = acknowledgement.committed_checkpoint.as_ref() else {
            return ResumeFailure::terminal(
                FailureClass::PrerequisiteMissing,
                "durable_checkpoint_uncommitted",
            )
            .into_plan();
        };
        if acknowledgement.ack_state != AckState::Committed {
            return ResumeFailure::terminal(
                FailureClass::PrerequisiteMissing,
                "durable_checkpoint_uncommitted",
            )
            .into_plan();
        }
        if checkpoint.feed_id != self.feed.feed_id
            || checkpoint.range_id != self.feed.range.range_id
            || checkpoint.generation != self.feed.generation
        {
            return ResumeFailure::terminal(FailureClass::InvalidRequest, "invalid_checkpoint")
                .into_plan();
        }
        if checkpoint.epoch != self.feed.range.data_epoch {
            return ResumeFailure::retryable(FailureClass::EpochMismatch, "range_order_gap")
                .into_plan();
        }
        if self.retained_through.as_ref().is_some_and(|retained| {
            CheckpointPosition::from(checkpoint) < CheckpointPosition::from(retained)
        }) {
            return ResumeFailure::terminal(FailureClass::StaleMetadata, "retention_expired")
                .into_plan();
        }
        match source {
            ResumeSourceStatus::Ready => ResumePlan {
                checkpoint: Some(checkpoint.clone()),
                operation_state: OperationState::RecoveryPending,
                failure_class: None,
                reason_code: None,
                retryable: false,
            },
            ResumeSourceStatus::MetadataStale => {
                ResumeFailure::retryable(FailureClass::StaleMetadata, "metadata_refresh_required")
                    .into_plan()
            }
            ResumeSourceStatus::Gap => {
                ResumeFailure::retryable(FailureClass::Gap, "range_order_gap").into_plan()
            }
            ResumeSourceStatus::EpochMismatch => {
                ResumeFailure::retryable(FailureClass::EpochMismatch, "range_order_gap").into_plan()
            }
            ResumeSourceStatus::DurableUnavailable => {
                ResumeFailure::terminal(FailureClass::PrerequisiteMissing, "durable_unavailable")
                    .into_plan()
            }
            ResumeSourceStatus::NodeUnavailable => {
                ResumeFailure::retryable(FailureClass::NodeUnavailable, "source_unavailable")
                    .into_plan()
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ResumeFailure {
    failure_class: FailureClass,
    reason_code: &'static str,
    retryable: bool,
}

impl ResumeFailure {
    const fn terminal(failure_class: FailureClass, reason_code: &'static str) -> Self {
        Self {
            failure_class,
            reason_code,
            retryable: false,
        }
    }

    const fn retryable(failure_class: FailureClass, reason_code: &'static str) -> Self {
        Self {
            failure_class,
            reason_code,
            retryable: true,
        }
    }

    fn into_plan(self) -> ResumePlan {
        ResumePlan {
            checkpoint: None,
            operation_state: if self.retryable {
                OperationState::RetryableFailure
            } else {
                OperationState::TerminalFailure
            },
            failure_class: Some(self.failure_class),
            reason_code: Some(self.reason_code.to_string()),
            retryable: self.retryable,
        }
    }
}

fn ack_from_record(record: &StoredAck) -> AckResult {
    let committed_checkpoint =
        (record.ack_state == AckState::Committed).then(|| record.request.checkpoint.clone());
    let (operation_state, failure_class, reason_code, retryable) = match record.ack_state {
        AckState::Accepted | AckState::Pending => (OperationState::Accepted, None, None, false),
        AckState::Committed => (OperationState::Committed, None, None, false),
        AckState::Rejected => record.failure.map_or(
            (
                OperationState::TerminalFailure,
                Some(FailureClass::Conflict),
                Some("ack_idempotency_conflict".to_string()),
                false,
            ),
            |failure| {
                (
                    if failure.retryable {
                        OperationState::RetryableFailure
                    } else {
                        OperationState::TerminalFailure
                    },
                    Some(failure.failure_class),
                    Some(failure.reason_code.to_string()),
                    failure.retryable,
                )
            },
        ),
        AckState::Expired => (
            OperationState::TerminalFailure,
            Some(FailureClass::StaleMetadata),
            Some("retention_expired".to_string()),
            false,
        ),
    };
    AckResult::new(
        record.request.ack_id.clone(),
        record.ack_state,
        committed_checkpoint.clone(),
        committed_checkpoint,
        operation_state,
        failure_class,
        reason_code,
        retryable,
        idempotency(&record.request, operation_state, record.duplicate_count),
    )
    .expect("stored acknowledgement state must satisfy the canonical model")
}

fn rejected_ack(
    request: &AckRequest,
    failure_class: FailureClass,
    reason_code: impl Into<String>,
    retryable: bool,
) -> AckResult {
    let operation_state = if retryable {
        OperationState::RetryableFailure
    } else {
        OperationState::TerminalFailure
    };
    AckResult::new(
        request.ack_id.clone(),
        AckState::Rejected,
        None,
        None,
        operation_state,
        Some(failure_class),
        Some(reason_code.into()),
        retryable,
        idempotency(request, operation_state, 0),
    )
    .expect("explicit rejected acknowledgement must satisfy the canonical model")
}

fn idempotency(
    request: &AckRequest,
    state: OperationState,
    duplicate_count: u64,
) -> IdempotencyResult {
    IdempotencyResult {
        operation_id: request.operation_id.clone(),
        request_id: request.request_id.clone(),
        first_outcome: "acknowledgement".to_string(),
        state,
        duplicate_count,
    }
}

/// Internal errors for invalid checkpoint-store construction or unsupported
/// state-machine operations that have no caller request to carry an outcome.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// A required acknowledgement request field was empty.
    #[error("acknowledgement {field} must not be empty")]
    EmptyRequestField { field: &'static str },
    /// Checkpoint model validation failed before acknowledgement processing.
    #[error("invalid checkpoint: {0}")]
    InvalidCheckpoint(ChangefeedModelError),
    /// Feed model validation failed while creating a store or planner.
    #[error("invalid feed: {0}")]
    InvalidFeed(ChangefeedModelError),
    /// A pending acknowledgement was not found in the recovered store.
    #[error("unknown acknowledgement request {0}")]
    UnknownRequest(String),
}

impl CheckpointError {
    const fn reason_code(&self) -> &'static str {
        match self {
            Self::EmptyRequestField { .. } | Self::InvalidCheckpoint(_) => "invalid_checkpoint",
            Self::InvalidFeed(_) => "invalid_feed",
            Self::UnknownRequest(_) => "ack_unknown",
        }
    }
}
