use crate::{
    FailureClass, IdempotencyResult, OperationState, Placement, RangeId, RangeIdentity, RequestId,
    RoutingOutcome, RoutingOutcomeKind,
};

/// The only ordering scope promised by the Phase 3 contract.  In particular,
/// a feed never implies table-wide or cluster-wide total ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingScope {
    Range,
}

/// The retention facts visible to a consumer when a feed is created or
/// inspected.  Policy evaluation and durable storage remain separate from the
/// public model.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetentionWindow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_through_position: Option<u64>,
}

impl RetentionWindow {
    pub const fn unbounded() -> Self {
        Self {
            deadline_epoch: None,
            retained_through_position: None,
        }
    }
}

/// Identity and range metadata returned for every public changefeed outcome.
/// `OperationState` is reused rather than creating a feed-only lifecycle enum.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FeedIdentity {
    pub feed_id: String,
    pub range: RangeIdentity,
    /// The range generation is separate from `data_epoch`: generation changes
    /// when the range topology changes, while data epoch tracks source data.
    pub generation: u64,
    pub placement: Placement,
    pub ordering_scope: OrderingScope,
    pub retention: RetentionWindow,
    pub status: OperationState,
}

impl FeedIdentity {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        feed_id: impl Into<String>,
        range: RangeIdentity,
        generation: u64,
        placement: Placement,
        ordering_scope: OrderingScope,
        retention: RetentionWindow,
        status: OperationState,
    ) -> Result<Self, ChangefeedModelError> {
        let feed = Self {
            feed_id: feed_id.into(),
            range,
            generation,
            placement,
            ordering_scope,
            retention,
            status,
        };
        feed.validate()?;
        Ok(feed)
    }

    pub fn validate(&self) -> Result<(), ChangefeedModelError> {
        require_non_empty("feed_id", &self.feed_id)
    }
}

/// Event kinds which can appear in the versioned public schema.  `Schema` is
/// reserved by the model; Phase 3 support registration decides whether it is
/// accepted before execution and must not manufacture schema events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOperationType {
    Insert,
    Update,
    Delete,
    Schema,
    Tombstone,
}

/// A payload is always explicit: a consumer either receives bytes or receives
/// a non-empty reason why the payload cannot be supplied.  It is never a
/// successful, unexplained `null`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChangePayload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_unavailable: Option<String>,
}

impl ChangePayload {
    pub fn available(payload: Vec<u8>) -> Self {
        Self {
            payload: Some(payload),
            payload_unavailable: None,
        }
    }

    pub fn unavailable(reason: impl Into<String>) -> Result<Self, ChangefeedModelError> {
        let payload = Self {
            payload: None,
            payload_unavailable: Some(reason.into()),
        };
        payload.validate()?;
        Ok(payload)
    }

    pub fn validate(&self) -> Result<(), ChangefeedModelError> {
        match (&self.payload, &self.payload_unavailable) {
            (Some(_), None) => Ok(()),
            (None, Some(reason)) => require_non_empty("payload_unavailable", reason),
            (Some(_), Some(_)) => Err(ChangefeedModelError::AmbiguousPayload),
            (None, None) => Err(ChangefeedModelError::MissingPayloadAvailability),
        }
    }
}

/// The stable checkpoint identity used by the later cursor codec, ack store,
/// and resume planner.  A cursor string is intentionally not introduced here:
/// task 3.2 owns its versioned wire encoding.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    pub feed_id: String,
    pub range_id: RangeId,
    pub generation: u64,
    pub commit_position: u64,
    pub payload_ordinal: u32,
    pub epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_deadline: Option<u64>,
}

impl Checkpoint {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        feed_id: impl Into<String>,
        range_id: impl Into<RangeId>,
        generation: u64,
        commit_position: u64,
        payload_ordinal: u32,
        epoch: u64,
        retention_deadline: Option<u64>,
    ) -> Result<Self, ChangefeedModelError> {
        let checkpoint = Self {
            feed_id: feed_id.into(),
            range_id: range_id.into(),
            generation,
            commit_position,
            payload_ordinal,
            epoch,
            retention_deadline,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    pub fn validate(&self) -> Result<(), ChangefeedModelError> {
        require_non_empty("checkpoint.feed_id", &self.feed_id)?;
        require_non_empty("checkpoint.range_id", self.range_id.as_str())
    }
}

/// Durable acknowledgement state.  Only `Committed` with a checkpoint is a
/// resume-safe acknowledgement; accepted and pending are intentionally not
/// represented as a successful durable checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckState {
    Accepted,
    Pending,
    Committed,
    Rejected,
    Expired,
}

/// Acknowledgement result with an explicit durability boundary.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AckResult {
    pub ack_id: String,
    pub ack_state: AckState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_checkpoint: Option<Checkpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_resume_position: Option<Checkpoint>,
    pub operation_state: OperationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<FailureClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    pub retryable: bool,
    pub idempotency: IdempotencyResult,
}

impl AckResult {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ack_id: impl Into<String>,
        ack_state: AckState,
        committed_checkpoint: Option<Checkpoint>,
        next_resume_position: Option<Checkpoint>,
        operation_state: OperationState,
        failure_class: Option<FailureClass>,
        reason_code: Option<String>,
        retryable: bool,
        idempotency: IdempotencyResult,
    ) -> Result<Self, ChangefeedModelError> {
        let result = Self {
            ack_id: ack_id.into(),
            ack_state,
            committed_checkpoint,
            next_resume_position,
            operation_state,
            failure_class,
            reason_code,
            retryable,
            idempotency,
        };
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), ChangefeedModelError> {
        require_non_empty("ack_id", &self.ack_id)?;
        validate_state_failure(
            self.operation_state,
            self.failure_class,
            self.reason_code.as_deref(),
        )?;
        if self.ack_state == AckState::Committed && self.committed_checkpoint.is_none() {
            return Err(ChangefeedModelError::CommittedAckMissingCheckpoint);
        }
        if matches!(self.ack_state, AckState::Accepted | AckState::Pending)
            && self.committed_checkpoint.is_some()
        {
            return Err(ChangefeedModelError::UncommittedAckHasCheckpoint);
        }
        if let Some(checkpoint) = &self.committed_checkpoint {
            checkpoint.validate()?;
        }
        if let Some(checkpoint) = &self.next_resume_position {
            checkpoint.validate()?;
        }
        validate_idempotency(&self.idempotency, None, None)
    }
}

/// One immutable event exposed by any supported public surface.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChangeEventEnvelope {
    pub event_id: String,
    pub feed_id: String,
    #[serde(flatten)]
    pub range: RangeIdentity,
    pub generation: u64,
    pub operation_id: String,
    pub request_id: RequestId,
    pub commit_position: u64,
    pub payload_ordinal: u32,
    pub operation_type: ChangeOperationType,
    pub key_or_hash: String,
    #[serde(flatten)]
    pub payload: ChangePayload,
    pub checkpoint: Checkpoint,
    pub operation_state: OperationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<FailureClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    pub routing: RoutingOutcome,
    pub retryable: bool,
    pub idempotency: IdempotencyResult,
}

impl ChangeEventEnvelope {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_id: impl Into<String>,
        feed_id: impl Into<String>,
        range: RangeIdentity,
        generation: u64,
        operation_id: impl Into<String>,
        request_id: impl Into<RequestId>,
        commit_position: u64,
        payload_ordinal: u32,
        operation_type: ChangeOperationType,
        key_or_hash: impl Into<String>,
        payload: ChangePayload,
        checkpoint: Checkpoint,
        operation_state: OperationState,
        failure_class: Option<FailureClass>,
        reason_code: Option<String>,
        routing: RoutingOutcome,
        retryable: bool,
        idempotency: IdempotencyResult,
    ) -> Result<Self, ChangefeedModelError> {
        let event = Self {
            event_id: event_id.into(),
            feed_id: feed_id.into(),
            range,
            generation,
            operation_id: operation_id.into(),
            request_id: request_id.into(),
            commit_position,
            payload_ordinal,
            operation_type,
            key_or_hash: key_or_hash.into(),
            payload,
            checkpoint,
            operation_state,
            failure_class,
            reason_code,
            routing,
            retryable,
            idempotency,
        };
        event.validate()?;
        Ok(event)
    }

    pub fn validate(&self) -> Result<(), ChangefeedModelError> {
        require_non_empty("event_id", &self.event_id)?;
        require_non_empty("feed_id", &self.feed_id)?;
        require_non_empty("operation_id", &self.operation_id)?;
        require_non_empty("request_id", self.request_id.as_str())?;
        require_non_empty("key_or_hash", &self.key_or_hash)?;
        self.payload.validate()?;
        self.checkpoint.validate()?;
        if self.checkpoint.feed_id != self.feed_id
            || self.checkpoint.range_id != self.range.range_id
            || self.checkpoint.generation != self.generation
            || self.checkpoint.commit_position != self.commit_position
            || self.checkpoint.payload_ordinal != self.payload_ordinal
            || self.checkpoint.epoch != self.range.data_epoch
        {
            return Err(ChangefeedModelError::EventCheckpointMismatch);
        }
        validate_state_failure(
            self.operation_state,
            self.failure_class,
            self.reason_code.as_deref(),
        )?;
        validate_idempotency(
            &self.idempotency,
            Some(&self.operation_id),
            Some(&self.request_id),
        )
    }
}

/// The non-null response payload of a canonical outcome.  A lifecycle call
/// that has no event or acknowledgement returns `Feed`; it is therefore
/// impossible to use a missing payload as an implicit success marker.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "result_type", content = "result", rename_all = "snake_case")]
pub enum ChangefeedResult {
    Feed,
    Event(Box<ChangeEventEnvelope>),
    Ack(Box<AckResult>),
}

/// Common canonical result used by embedded and all wire adapters.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChangefeedOutcome {
    pub feed: FeedIdentity,
    pub operation_id: String,
    pub request_id: RequestId,
    pub operation_state: OperationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<FailureClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    pub routing: RoutingOutcome,
    pub retryable: bool,
    pub idempotency: IdempotencyResult,
    pub result: ChangefeedResult,
}

impl ChangefeedOutcome {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        feed: FeedIdentity,
        operation_id: impl Into<String>,
        request_id: impl Into<RequestId>,
        operation_state: OperationState,
        failure_class: Option<FailureClass>,
        reason_code: Option<String>,
        routing: RoutingOutcome,
        retryable: bool,
        idempotency: IdempotencyResult,
        result: ChangefeedResult,
    ) -> Result<Self, ChangefeedModelError> {
        let outcome = Self {
            feed,
            operation_id: operation_id.into(),
            request_id: request_id.into(),
            operation_state,
            failure_class,
            reason_code,
            routing,
            retryable,
            idempotency,
            result,
        };
        outcome.validate()?;
        Ok(outcome)
    }

    pub fn validate(&self) -> Result<(), ChangefeedModelError> {
        self.feed.validate()?;
        require_non_empty("operation_id", &self.operation_id)?;
        require_non_empty("request_id", self.request_id.as_str())?;
        validate_state_failure(
            self.operation_state,
            self.failure_class,
            self.reason_code.as_deref(),
        )?;
        validate_idempotency(
            &self.idempotency,
            Some(&self.operation_id),
            Some(&self.request_id),
        )?;
        match &self.result {
            ChangefeedResult::Feed => Ok(()),
            ChangefeedResult::Event(event) => {
                event.validate()?;
                if event.feed_id != self.feed.feed_id
                    || event.range != self.feed.range
                    || event.generation != self.feed.generation
                    || event.operation_id != self.operation_id
                    || event.request_id != self.request_id
                {
                    return Err(ChangefeedModelError::OutcomeEventMismatch);
                }
                Ok(())
            }
            ChangefeedResult::Ack(ack) => {
                ack.validate()?;
                if let Some(checkpoint) = &ack.committed_checkpoint
                    && (checkpoint.feed_id != self.feed.feed_id
                        || checkpoint.range_id != self.feed.range.range_id
                        || checkpoint.generation != self.feed.generation)
                {
                    return Err(ChangefeedModelError::OutcomeAckMismatch);
                }
                Ok(())
            }
        }
    }

    /// One transport projection shared by later HTTP, gRPC, CLI and Python
    /// adapters.  The structured fields above remain authoritative.
    pub fn surface_status(&self) -> ChangefeedSurfaceStatus {
        ChangefeedSurfaceStatus::from_outcome(self)
    }
}

/// Transport status names for the canonical outcome.  No transport is allowed
/// to turn a terminal failure or unsupported routing result into success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangefeedSurfaceStatus {
    pub http_status: u16,
    pub grpc_code: &'static str,
    pub cli_exit_code: i32,
    pub python_error_code: Option<&'static str>,
}

impl ChangefeedSurfaceStatus {
    pub fn from_outcome(outcome: &ChangefeedOutcome) -> Self {
        if outcome.routing.kind == RoutingOutcomeKind::Unsupported {
            return Self {
                http_status: 501,
                grpc_code: "UNIMPLEMENTED",
                cli_exit_code: 5,
                python_error_code: Some("changefeed_unsupported"),
            };
        }

        let (http_status, grpc_code, python_error_code) = match outcome.failure_class {
            Some(FailureClass::Unauthorized) => {
                (401, "UNAUTHENTICATED", Some("changefeed_unauthorized"))
            }
            Some(
                FailureClass::StaleMetadata
                | FailureClass::Gap
                | FailureClass::Overlap
                | FailureClass::EpochMismatch
                | FailureClass::Conflict,
            ) => (409, "ABORTED", Some("changefeed_conflict")),
            Some(FailureClass::NotLeader | FailureClass::NodeUnavailable) => {
                (503, "UNAVAILABLE", Some("changefeed_unavailable"))
            }
            Some(FailureClass::PrerequisiteMissing) => (
                503,
                "FAILED_PRECONDITION",
                Some("changefeed_prerequisite_missing"),
            ),
            Some(FailureClass::Timeout) => (408, "DEADLINE_EXCEEDED", Some("changefeed_timeout")),
            Some(FailureClass::InvalidRequest) => {
                (400, "INVALID_ARGUMENT", Some("changefeed_invalid_request"))
            }
            Some(FailureClass::Internal) => (500, "INTERNAL", Some("changefeed_internal")),
            None if outcome.operation_state == OperationState::Cancelled => {
                (408, "CANCELLED", Some("changefeed_cancelled"))
            }
            None if matches!(
                outcome.operation_state,
                OperationState::Accepted
                    | OperationState::Running
                    | OperationState::RecoveryPending
            ) =>
            {
                (202, "OK", None)
            }
            None if outcome.operation_state == OperationState::Committed => (200, "OK", None),
            None => (500, "INTERNAL", Some("changefeed_internal")),
        };

        let cli_exit_code = if outcome.operation_state == OperationState::Cancelled {
            130
        } else if outcome.failure_class == Some(FailureClass::Unauthorized) {
            4
        } else if matches!(
            outcome.operation_state,
            OperationState::Accepted | OperationState::Running | OperationState::RecoveryPending
        ) {
            2
        } else if outcome.operation_state == OperationState::RetryableFailure || outcome.retryable {
            3
        } else if outcome.failure_class == Some(FailureClass::PrerequisiteMissing) {
            5
        } else if outcome.failure_class.is_some()
            || outcome.operation_state != OperationState::Committed
        {
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

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ChangefeedModelError {
    #[error("{field} must not be empty")]
    EmptyIdentity { field: &'static str },
    #[error("payload and payload_unavailable cannot both be present")]
    AmbiguousPayload,
    #[error("payload availability must be explicit")]
    MissingPayloadAvailability,
    #[error("a committed acknowledgement requires a committed checkpoint")]
    CommittedAckMissingCheckpoint,
    #[error("an accepted or pending acknowledgement cannot claim a committed checkpoint")]
    UncommittedAckHasCheckpoint,
    #[error("failure state requires failure_class and reason_code")]
    MissingFailureDetails,
    #[error("success state cannot carry failure_class or reason_code")]
    AmbiguousSuccessFailure,
    #[error("idempotency identity differs from the canonical outcome")]
    IdempotencyIdentityMismatch,
    #[error("event checkpoint does not describe the same feed position")]
    EventCheckpointMismatch,
    #[error("event differs from the enclosing canonical outcome")]
    OutcomeEventMismatch,
    #[error("acknowledgement differs from the enclosing canonical outcome")]
    OutcomeAckMismatch,
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), ChangefeedModelError> {
    if value.is_empty() {
        Err(ChangefeedModelError::EmptyIdentity { field })
    } else {
        Ok(())
    }
}

fn validate_state_failure(
    state: OperationState,
    failure_class: Option<FailureClass>,
    reason_code: Option<&str>,
) -> Result<(), ChangefeedModelError> {
    let is_failure = matches!(
        state,
        OperationState::Rejected
            | OperationState::RetryableFailure
            | OperationState::TerminalFailure
    );
    if is_failure && (failure_class.is_none() || reason_code.is_none_or(str::is_empty)) {
        return Err(ChangefeedModelError::MissingFailureDetails);
    }
    if !is_failure && (failure_class.is_some() || reason_code.is_some()) {
        return Err(ChangefeedModelError::AmbiguousSuccessFailure);
    }
    Ok(())
}

fn validate_idempotency(
    idempotency: &IdempotencyResult,
    operation_id: Option<&str>,
    request_id: Option<&RequestId>,
) -> Result<(), ChangefeedModelError> {
    require_non_empty("idempotency.operation_id", &idempotency.operation_id)?;
    require_non_empty("idempotency.request_id", idempotency.request_id.as_str())?;
    require_non_empty("idempotency.first_outcome", &idempotency.first_outcome)?;
    if operation_id.is_some_and(|expected| expected != idempotency.operation_id)
        || request_id.is_some_and(|expected| expected != &idempotency.request_id)
    {
        return Err(ChangefeedModelError::IdempotencyIdentityMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        FailureClass, IdempotencyResult, OperationState, Placement, PlacementReadiness,
        PlacementRole, RangeIdentity, RoutingOutcome, RoutingOutcomeKind,
    };

    use super::{
        AckResult, AckState, ChangeEventEnvelope, ChangeOperationType, ChangePayload,
        ChangefeedModelError, ChangefeedOutcome, ChangefeedResult, Checkpoint, FeedIdentity,
        OrderingScope, RetentionWindow,
    };

    fn range() -> RangeIdentity {
        RangeIdentity::new(
            "cluster-a",
            7,
            "range-a",
            Some(vec![0]),
            Some(vec![255]),
            3,
            9,
        )
    }

    fn placement() -> Placement {
        Placement::new(
            "node-a",
            vec!["node-b".into()],
            PlacementRole::Owner,
            PlacementReadiness::Ready,
            11,
        )
    }

    fn feed() -> FeedIdentity {
        FeedIdentity::new(
            "feed-a",
            range(),
            4,
            placement(),
            OrderingScope::Range,
            RetentionWindow::unbounded(),
            OperationState::Committed,
        )
        .expect("valid feed")
    }

    fn idempotency(state: OperationState) -> IdempotencyResult {
        IdempotencyResult {
            operation_id: "operation-a".to_string(),
            request_id: "request-a".into(),
            first_outcome: "committed".to_string(),
            state,
            duplicate_count: 0,
        }
    }

    fn routing() -> RoutingOutcome {
        RoutingOutcome::new(
            RoutingOutcomeKind::SingleRange,
            Some(range()),
            15,
            "placement_ready",
        )
    }

    fn checkpoint() -> Checkpoint {
        Checkpoint::new("feed-a", "range-a", 4, 23, 2, 9, Some(90)).expect("valid checkpoint")
    }

    #[test]
    fn event_and_outcome_preserve_all_identity_and_canonical_status_fields() {
        let event = ChangeEventEnvelope::new(
            "event-a",
            "feed-a",
            range(),
            4,
            "operation-a",
            "request-a",
            23,
            2,
            ChangeOperationType::Delete,
            "key:abc",
            ChangePayload::available(vec![7, 8]),
            checkpoint(),
            OperationState::Committed,
            None,
            None,
            routing(),
            false,
            idempotency(OperationState::Committed),
        )
        .expect("valid event");
        let outcome = ChangefeedOutcome::new(
            feed(),
            "operation-a",
            "request-a",
            OperationState::Committed,
            None,
            None,
            routing(),
            false,
            idempotency(OperationState::Committed),
            ChangefeedResult::Event(Box::new(event)),
        )
        .expect("valid canonical outcome");

        let encoded = serde_json::to_string(&outcome).expect("serialize outcome");
        assert!(encoded.contains("\"feed_id\":\"feed-a\""));
        assert!(encoded.contains("\"cluster_id\":\"cluster-a\""));
        assert!(encoded.contains("\"operation_type\":\"delete\""));
        assert!(encoded.contains("\"checkpoint\":"));
        assert!(encoded.contains("\"idempotency\":"));
        assert_eq!(outcome.surface_status().http_status, 200);
        assert_eq!(outcome.surface_status().grpc_code, "OK");
        assert_eq!(outcome.surface_status().cli_exit_code, 0);
    }

    #[test]
    fn missing_identity_ambiguous_payload_and_failure_success_are_rejected() {
        let empty_feed = FeedIdentity::new(
            "",
            range(),
            4,
            placement(),
            OrderingScope::Range,
            RetentionWindow::unbounded(),
            OperationState::Accepted,
        );
        assert_eq!(
            empty_feed.unwrap_err(),
            ChangefeedModelError::EmptyIdentity { field: "feed_id" }
        );

        let ambiguous = ChangePayload {
            payload: Some(vec![1]),
            payload_unavailable: Some("journal_missing".to_string()),
        };
        assert_eq!(
            ambiguous.validate(),
            Err(ChangefeedModelError::AmbiguousPayload)
        );

        let outcome = ChangefeedOutcome::new(
            feed(),
            "operation-a",
            "request-a",
            OperationState::Committed,
            Some(FailureClass::Gap),
            Some("gap".to_string()),
            routing(),
            false,
            idempotency(OperationState::Committed),
            ChangefeedResult::Feed,
        );
        assert_eq!(
            outcome.unwrap_err(),
            ChangefeedModelError::AmbiguousSuccessFailure
        );
    }

    #[test]
    fn only_committed_ack_can_claim_durable_checkpoint_and_failure_maps_consistently() {
        let pending = AckResult::new(
            "ack-a",
            AckState::Pending,
            Some(checkpoint()),
            Some(checkpoint()),
            OperationState::Accepted,
            None,
            None,
            false,
            idempotency(OperationState::Accepted),
        );
        assert_eq!(
            pending.unwrap_err(),
            ChangefeedModelError::UncommittedAckHasCheckpoint
        );

        let committed = AckResult::new(
            "ack-a",
            AckState::Committed,
            Some(checkpoint()),
            Some(checkpoint()),
            OperationState::Committed,
            None,
            None,
            false,
            idempotency(OperationState::Committed),
        )
        .expect("durable acknowledgement");
        let outcome = ChangefeedOutcome::new(
            feed(),
            "operation-a",
            "request-a",
            OperationState::TerminalFailure,
            Some(FailureClass::PrerequisiteMissing),
            Some("durable_unavailable".to_string()),
            routing(),
            false,
            idempotency(OperationState::TerminalFailure),
            ChangefeedResult::Ack(Box::new(committed)),
        )
        .expect("terminal outcome retains explicit canonical status");
        let status = outcome.surface_status();
        assert_eq!(status.http_status, 503);
        assert_eq!(status.grpc_code, "FAILED_PRECONDITION");
        assert_eq!(status.cli_exit_code, 5);
        assert_eq!(
            status.python_error_code,
            Some("changefeed_prerequisite_missing")
        );
    }
}
