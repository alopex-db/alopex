use std::collections::BTreeMap;

use crate::{FailureClass, IdempotencyResult, OperationState, RequestId, RoutingOutcome};

use super::{
    AckResult, AckState, ChangeEventEnvelope, ChangefeedModelError, ChangefeedOutcome,
    ChangefeedResult, CheckpointCursor, CursorError, FeedIdentity,
};

/// Caller-supplied idempotency identity for one changefeed lifecycle request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedRequest {
    pub operation_id: String,
    pub request_id: RequestId,
}

impl FeedRequest {
    pub fn new(
        operation_id: impl Into<String>,
        request_id: impl Into<RequestId>,
    ) -> Result<Self, CoordinatorError> {
        let request = Self {
            operation_id: operation_id.into(),
            request_id: request_id.into(),
        };
        if request.operation_id.is_empty() || request.request_id.as_str().is_empty() {
            return Err(CoordinatorError::InvalidRequest(
                "missing_operation_or_request_id",
            ));
        }
        Ok(request)
    }

    fn idempotency(
        &self,
        state: OperationState,
        first_outcome: &str,
        duplicate_count: u64,
    ) -> IdempotencyResult {
        IdempotencyResult {
            operation_id: self.operation_id.clone(),
            request_id: self.request_id.clone(),
            first_outcome: first_outcome.to_string(),
            state,
            duplicate_count,
        }
    }
}

/// The only readiness input accepted by the coordinator. The later Durable
/// adapter owns how this value is proved; callers cannot silently default to a
/// local-WAL or best-effort feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedPreflight {
    Ready {
        _durable_evidence: DurablePreflightEvidence,
    },
    Rejected {
        failure_class: FailureClass,
        reason_code: String,
        retryable: bool,
    },
}

/// Opaque marker carried by a successful Durable capability check.
///
/// Its tuple field is private, so public callers cannot construct a `Ready`
/// preflight without passing through `DurableProfileAdapter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurablePreflightEvidence(());

impl FeedPreflight {
    /// Produces a Ready state only for the in-crate Durable adapter after it
    /// has verified all mandatory capability evidence.
    pub(crate) const fn ready() -> Self {
        Self::Ready {
            _durable_evidence: DurablePreflightEvidence(()),
        }
    }

    /// Reports whether Durable evidence passed every mandatory preflight check.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }

    pub fn rejected(
        failure_class: FailureClass,
        reason_code: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self::Rejected {
            failure_class,
            reason_code: reason_code.into(),
            retryable,
        }
    }
}

/// The event list returned by poll/stream/resume together with its canonical
/// lifecycle outcome. An empty list is success only when no gap/continuity
/// failure is recorded; those failures return a terminal/retryable outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedDelivery {
    pub outcome: ChangefeedOutcome,
    pub events: Vec<ChangeEventEnvelope>,
}

/// Authoritative in-memory lifecycle and range-order coordinator. Durable ack
/// persistence is intentionally delegated to task 3.7; this coordinator only
/// returns `accepted` acknowledgement state until that store commits one.
#[derive(Debug)]
pub struct FeedCoordinator {
    preflight: FeedPreflight,
    sessions: BTreeMap<String, FeedSession>,
}

#[derive(Debug)]
struct FeedSession {
    feed: FeedIdentity,
    routing: RoutingOutcome,
    events: Vec<ChangeEventEnvelope>,
    continuity_failure: Option<ContinuityFailure>,
    create_request: FeedRequest,
    lifecycle_replays: BTreeMap<String, LifecycleReplay>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContinuityFailure {
    failure_class: FailureClass,
    reason_code: String,
    retryable: bool,
}

/// One replay-safe lifecycle request retained for the lifetime of a feed
/// session.  The coordinator caches delivery batches as well as terminal
/// outcomes so a repeated request cannot observe later events or apply a
/// terminal transition twice.
#[derive(Debug, Clone)]
struct LifecycleReplay {
    request: FeedRequest,
    action: ReplayAction,
    result: ReplayResult,
    duplicate_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReplayAction {
    Resume { checkpoint: String },
    Close,
}

#[derive(Debug, Clone)]
enum ReplayResult {
    Delivery(FeedDelivery),
    Outcome(ChangefeedOutcome),
}

impl LifecycleReplay {
    fn is_same_request(&self, request: &FeedRequest, action: &ReplayAction) -> bool {
        self.request == *request && self.action == *action
    }

    fn duplicate_delivery(&mut self) -> FeedDelivery {
        self.duplicate_count = self.duplicate_count.saturating_add(1);
        let ReplayResult::Delivery(delivery) = &self.result else {
            unreachable!("delivery replay stored an outcome")
        };
        let mut duplicate = delivery.clone();
        duplicate.outcome.idempotency.duplicate_count = self.duplicate_count;
        duplicate
    }

    fn duplicate_outcome(&mut self) -> ChangefeedOutcome {
        self.duplicate_count = self.duplicate_count.saturating_add(1);
        let ReplayResult::Outcome(outcome) = &self.result else {
            unreachable!("outcome replay stored a delivery")
        };
        let mut duplicate = outcome.clone();
        duplicate.idempotency.duplicate_count = self.duplicate_count;
        duplicate
    }
}

impl FeedCoordinator {
    pub fn new(preflight: FeedPreflight) -> Self {
        Self {
            preflight,
            sessions: BTreeMap::new(),
        }
    }

    /// Creates a feed only after explicit preflight. Repeating the same create
    /// request leaves the one feed session untouched and returns a replayed
    /// canonical outcome rather than allocating another subscription.
    pub fn create(
        &mut self,
        mut feed: FeedIdentity,
        routing: RoutingOutcome,
        request: FeedRequest,
    ) -> Result<ChangefeedOutcome, CoordinatorError> {
        feed.validate().map_err(CoordinatorError::Model)?;
        if let FeedPreflight::Rejected {
            failure_class,
            reason_code,
            retryable,
        } = &self.preflight
        {
            feed.status = if *retryable {
                OperationState::RetryableFailure
            } else {
                OperationState::TerminalFailure
            };
            let state = feed.status;
            return outcome(
                feed,
                routing,
                &request,
                state,
                Some(*failure_class),
                Some(reason_code.clone()),
                *retryable,
                0,
                ChangefeedResult::Feed,
            );
        }
        if let Some(session) = self.sessions.get_mut(&feed.feed_id) {
            let same_request = session.create_request == request;
            let state = session.feed.status;
            return if same_request {
                outcome(
                    session.feed.clone(),
                    session.routing.clone(),
                    &request,
                    state,
                    None,
                    None,
                    false,
                    1,
                    ChangefeedResult::Feed,
                )
            } else {
                failure_outcome(
                    &session.feed,
                    &session.routing,
                    &request,
                    FailureClass::Conflict,
                    "feed_already_exists",
                    false,
                )
            };
        }
        feed.status = OperationState::Accepted;
        let created = outcome(
            feed.clone(),
            routing.clone(),
            &request,
            OperationState::Accepted,
            None,
            None,
            false,
            0,
            ChangefeedResult::Feed,
        )?;
        self.sessions.insert(
            feed.feed_id.clone(),
            FeedSession {
                feed,
                routing,
                events: Vec::new(),
                continuity_failure: None,
                create_request: request,
                lifecycle_replays: BTreeMap::new(),
            },
        );
        Ok(created)
    }

    pub fn subscribe(
        &mut self,
        feed_id: &str,
        expected_generation: u64,
        expected_epoch: u64,
        request: FeedRequest,
    ) -> Result<ChangefeedOutcome, CoordinatorError> {
        let session = self.session_mut(feed_id)?;
        if session.feed.generation != expected_generation
            || session.feed.range.data_epoch != expected_epoch
        {
            return failure_outcome(
                &session.feed,
                &session.routing,
                &request,
                FailureClass::EpochMismatch,
                "expected_range_version_mismatch",
                true,
            );
        }
        if let Some(failure) = &session.continuity_failure {
            return failure_outcome(
                &session.feed,
                &session.routing,
                &request,
                failure.failure_class,
                &failure.reason_code,
                failure.retryable,
            );
        }
        session.feed.status = OperationState::Running;
        outcome(
            session.feed.clone(),
            session.routing.clone(),
            &request,
            OperationState::Running,
            None,
            None,
            false,
            0,
            ChangefeedResult::Feed,
        )
    }

    /// Queues a committed event in range order. The same `event_id` at the
    /// same source position is kept as an observable at-least-once duplicate;
    /// another event at an old/equal position is rejected rather than reordered.
    pub fn publish(&mut self, event: ChangeEventEnvelope) -> Result<(), CoordinatorError> {
        event.validate().map_err(CoordinatorError::Model)?;
        if event.operation_state != OperationState::Committed || event.failure_class.is_some() {
            return Err(CoordinatorError::UncommittedEvent);
        }
        let session = self.session_mut(&event.feed_id)?;
        if event.range != session.feed.range || event.generation != session.feed.generation {
            return Err(CoordinatorError::EventFeedMismatch);
        }
        if let Some(last) = session.events.last() {
            let candidate = CheckpointCursor::new(event.checkpoint.clone())
                .map_err(CoordinatorError::Cursor)?;
            if !candidate
                .is_strictly_after(&last.checkpoint)
                .map_err(CoordinatorError::Cursor)?
                && last.event_id != event.event_id
            {
                return Err(CoordinatorError::RangeOrderViolation);
            }
        }
        session.events.push(event);
        Ok(())
    }

    /// Records a proven range movement/catch-up discontinuity. Poll and resume
    /// then return this explicit outcome instead of an empty successful stream.
    pub fn mark_continuity_failure(
        &mut self,
        feed_id: &str,
        failure_class: FailureClass,
        reason_code: impl Into<String>,
        retryable: bool,
    ) -> Result<(), CoordinatorError> {
        if !matches!(
            failure_class,
            FailureClass::Gap | FailureClass::EpochMismatch
        ) {
            return Err(CoordinatorError::InvalidRequest("continuity_failure_class"));
        }
        self.session_mut(feed_id)?.continuity_failure = Some(ContinuityFailure {
            failure_class,
            reason_code: reason_code.into(),
            retryable,
        });
        Ok(())
    }

    pub fn poll(
        &self,
        feed_id: &str,
        max_events: usize,
        request: FeedRequest,
    ) -> Result<FeedDelivery, CoordinatorError> {
        if max_events == 0 {
            return Err(CoordinatorError::InvalidRequest("max_events_zero"));
        }
        let session = self.session(feed_id)?;
        if let Some(failure) = &session.continuity_failure {
            return Ok(FeedDelivery {
                outcome: failure_outcome(
                    &session.feed,
                    &session.routing,
                    &request,
                    failure.failure_class,
                    &failure.reason_code,
                    failure.retryable,
                )?,
                events: Vec::new(),
            });
        }
        let state = session.feed.status;
        let events = if state == OperationState::Cancelled {
            Vec::new()
        } else {
            session.events.iter().take(max_events).cloned().collect()
        };
        Ok(FeedDelivery {
            outcome: outcome(
                session.feed.clone(),
                session.routing.clone(),
                &request,
                state,
                None,
                None,
                false,
                0,
                ChangefeedResult::Feed,
            )?,
            events,
        })
    }

    /// This synchronous contract exposes the same range-ordered batch as poll;
    /// public async transports own wake-up and cancellation wiring.
    pub fn stream(
        &self,
        feed_id: &str,
        max_events: usize,
        request: FeedRequest,
    ) -> Result<FeedDelivery, CoordinatorError> {
        self.poll(feed_id, max_events, request)
    }

    /// Accepts an acknowledgement but deliberately does not claim a durable
    /// checkpoint. Task 3.7 upgrades this transition only after its store has
    /// committed the checkpoint.
    pub fn ack(
        &self,
        feed_id: &str,
        ack_id: impl Into<String>,
        request: FeedRequest,
    ) -> Result<ChangefeedOutcome, CoordinatorError> {
        let session = self.session(feed_id)?;
        let ack = AckResult::new(
            ack_id,
            AckState::Accepted,
            None,
            None,
            OperationState::Accepted,
            None,
            None,
            false,
            request.idempotency(OperationState::Accepted, "ack_accepted", 0),
        )
        .map_err(CoordinatorError::Model)?;
        outcome(
            session.feed.clone(),
            session.routing.clone(),
            &request,
            OperationState::Accepted,
            None,
            None,
            false,
            0,
            ChangefeedResult::Ack(Box::new(ack)),
        )
    }

    pub fn resume(
        &mut self,
        feed_id: &str,
        checkpoint: &str,
        request: FeedRequest,
    ) -> Result<FeedDelivery, CoordinatorError> {
        let session = self.session_mut(feed_id)?;
        let replay_key = request.request_id.as_str().to_string();
        let action = ReplayAction::Resume {
            checkpoint: checkpoint.to_string(),
        };
        if let Some(replay) = session.lifecycle_replays.get_mut(&replay_key) {
            return if replay.is_same_request(&request, &action) {
                Ok(replay.duplicate_delivery())
            } else {
                replay_conflict_delivery(session, &request)
            };
        }
        let cursor = match CheckpointCursor::decode_for(
            checkpoint,
            &session.feed.feed_id,
            &session.feed.range.range_id,
        ) {
            Ok(cursor) => cursor,
            Err(error) => {
                return Ok(FeedDelivery {
                    outcome: failure_outcome(
                        &session.feed,
                        &session.routing,
                        &request,
                        error.failure_class(),
                        error.reason_code(),
                        false,
                    )?,
                    events: Vec::new(),
                });
            }
        };
        if cursor.checkpoint().generation != session.feed.generation
            || cursor.checkpoint().epoch != session.feed.range.data_epoch
        {
            return Ok(FeedDelivery {
                outcome: failure_outcome(
                    &session.feed,
                    &session.routing,
                    &request,
                    FailureClass::EpochMismatch,
                    "range_order_gap",
                    true,
                )?,
                events: Vec::new(),
            });
        }
        if let Some(failure) = &session.continuity_failure {
            return Ok(FeedDelivery {
                outcome: failure_outcome(
                    &session.feed,
                    &session.routing,
                    &request,
                    failure.failure_class,
                    &failure.reason_code,
                    failure.retryable,
                )?,
                events: Vec::new(),
            });
        }
        let mut events = Vec::new();
        for event in &session.events {
            let event_cursor = CheckpointCursor::new(event.checkpoint.clone())
                .map_err(CoordinatorError::Cursor)?;
            if event_cursor
                .is_strictly_after(cursor.checkpoint())
                .map_err(CoordinatorError::Cursor)?
            {
                events.push(event.clone());
            }
        }
        let delivery = FeedDelivery {
            outcome: outcome(
                session.feed.clone(),
                session.routing.clone(),
                &request,
                OperationState::RecoveryPending,
                None,
                None,
                true,
                0,
                ChangefeedResult::Feed,
            )?,
            events,
        };
        session.lifecycle_replays.insert(
            replay_key,
            LifecycleReplay {
                request,
                action,
                result: ReplayResult::Delivery(delivery.clone()),
                duplicate_count: 0,
            },
        );
        Ok(delivery)
    }

    pub fn cancel(
        &mut self,
        feed_id: &str,
        request: FeedRequest,
    ) -> Result<ChangefeedOutcome, CoordinatorError> {
        self.close(feed_id, request)
    }

    pub fn close(
        &mut self,
        feed_id: &str,
        request: FeedRequest,
    ) -> Result<ChangefeedOutcome, CoordinatorError> {
        let session = self.session_mut(feed_id)?;
        let replay_key = request.request_id.as_str().to_string();
        let action = ReplayAction::Close;
        if let Some(replay) = session.lifecycle_replays.get_mut(&replay_key) {
            return if replay.is_same_request(&request, &action) {
                Ok(replay.duplicate_outcome())
            } else {
                replay_conflict_outcome(session, &request)
            };
        }
        session.feed.status = OperationState::Cancelled;
        let closed = outcome(
            session.feed.clone(),
            session.routing.clone(),
            &request,
            OperationState::Cancelled,
            None,
            None,
            false,
            0,
            ChangefeedResult::Feed,
        )?;
        session.lifecycle_replays.insert(
            replay_key,
            LifecycleReplay {
                request,
                action,
                result: ReplayResult::Outcome(closed.clone()),
                duplicate_count: 0,
            },
        );
        Ok(closed)
    }

    pub fn status(
        &self,
        feed_id: &str,
        request: FeedRequest,
    ) -> Result<ChangefeedOutcome, CoordinatorError> {
        let session = self.session(feed_id)?;
        outcome(
            session.feed.clone(),
            session.routing.clone(),
            &request,
            session.feed.status,
            None,
            None,
            false,
            0,
            ChangefeedResult::Feed,
        )
    }

    fn session(&self, feed_id: &str) -> Result<&FeedSession, CoordinatorError> {
        self.sessions
            .get(feed_id)
            .ok_or_else(|| CoordinatorError::UnknownFeed(feed_id.to_string()))
    }

    fn session_mut(&mut self, feed_id: &str) -> Result<&mut FeedSession, CoordinatorError> {
        self.sessions
            .get_mut(feed_id)
            .ok_or_else(|| CoordinatorError::UnknownFeed(feed_id.to_string()))
    }
}

#[allow(clippy::too_many_arguments)]
fn outcome(
    feed: FeedIdentity,
    routing: RoutingOutcome,
    request: &FeedRequest,
    state: OperationState,
    failure_class: Option<FailureClass>,
    reason_code: Option<String>,
    retryable: bool,
    duplicate_count: u64,
    result: ChangefeedResult,
) -> Result<ChangefeedOutcome, CoordinatorError> {
    ChangefeedOutcome::new(
        feed,
        request.operation_id.clone(),
        request.request_id.clone(),
        state,
        failure_class,
        reason_code,
        routing,
        retryable,
        request.idempotency(state, "coordinator", duplicate_count),
        result,
    )
    .map_err(CoordinatorError::Model)
}

fn failure_outcome(
    feed: &FeedIdentity,
    routing: &RoutingOutcome,
    request: &FeedRequest,
    failure_class: FailureClass,
    reason_code: impl Into<String>,
    retryable: bool,
) -> Result<ChangefeedOutcome, CoordinatorError> {
    outcome(
        feed.clone(),
        routing.clone(),
        request,
        if retryable {
            OperationState::RetryableFailure
        } else {
            OperationState::TerminalFailure
        },
        Some(failure_class),
        Some(reason_code.into()),
        retryable,
        0,
        ChangefeedResult::Feed,
    )
}

fn replay_conflict_outcome(
    session: &FeedSession,
    request: &FeedRequest,
) -> Result<ChangefeedOutcome, CoordinatorError> {
    failure_outcome(
        &session.feed,
        &session.routing,
        request,
        FailureClass::Conflict,
        "request_idempotency_conflict",
        false,
    )
}

fn replay_conflict_delivery(
    session: &FeedSession,
    request: &FeedRequest,
) -> Result<FeedDelivery, CoordinatorError> {
    Ok(FeedDelivery {
        outcome: replay_conflict_outcome(session, request)?,
        events: Vec::new(),
    })
}

#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error("changefeed {0} does not exist")]
    UnknownFeed(String),
    #[error("invalid changefeed request: {0}")]
    InvalidRequest(&'static str),
    #[error("uncommitted or failed event cannot be delivered")]
    UncommittedEvent,
    #[error("event belongs to another feed/range/generation")]
    EventFeedMismatch,
    #[error("range event would reorder a distinct source position")]
    RangeOrderViolation,
    #[error(transparent)]
    Cursor(CursorError),
    #[error(transparent)]
    Model(ChangefeedModelError),
}
