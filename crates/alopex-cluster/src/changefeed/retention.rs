//! Retention and delivery-budget decisions for the durable changefeed.
//!
//! The policy never evicts or skips a consumer's checkpoint.  When retention
//! or a delivery limit prevents progress, it returns the last committed
//! checkpoint and a classified result so a caller can resume explicitly.

use crate::{FailureClass, OperationState};

use super::{Checkpoint, CheckpointPosition, FeedIdentity};

/// Retention boundary for exactly one feed/range/generation.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    feed: FeedIdentity,
    retained_through: Option<Checkpoint>,
    deadline_epoch: Option<u64>,
}

impl RetentionPolicy {
    /// Creates a policy from the earliest retained checkpoint and optional
    /// expiry epoch.  The retained checkpoint, if present, must belong to the
    /// same feed, range, generation, and epoch.
    pub fn new(
        feed: FeedIdentity,
        retained_through: Option<Checkpoint>,
        deadline_epoch: Option<u64>,
    ) -> Result<Self, RetentionError> {
        feed.validate().map_err(RetentionError::InvalidFeed)?;
        let policy = Self {
            feed,
            retained_through,
            deadline_epoch,
        };
        if let Some(checkpoint) = &policy.retained_through
            && policy.validate_checkpoint(checkpoint).is_err()
        {
            return Err(RetentionError::InvalidRetainedCheckpoint);
        }
        Ok(policy)
    }

    /// Classifies a requested resume checkpoint before any event is removed or
    /// delivered.  Expired retention is terminal and never restarts from the
    /// earliest retained event.
    #[must_use]
    pub fn classify_resume(
        &self,
        requested: &Checkpoint,
        now_epoch: u64,
        last_committed: Option<Checkpoint>,
    ) -> DeliveryOutcome {
        if let Err(error) = self.validate_checkpoint(requested) {
            return error.into_outcome(last_committed);
        }
        if let Some(committed) = &last_committed
            && let Err(error) = self.validate_checkpoint(committed)
        {
            return error.into_outcome(None);
        }
        if self
            .deadline_epoch
            .is_some_and(|deadline| now_epoch >= deadline)
            || requested
                .retention_deadline
                .is_some_and(|deadline| now_epoch >= deadline)
        {
            return DeliveryFailure::terminal(FailureClass::StaleMetadata, "retention_expired")
                .into_outcome(last_committed);
        }
        if self.retained_through.as_ref().is_some_and(|retained| {
            CheckpointPosition::from(requested) < CheckpointPosition::from(retained)
        }) {
            return DeliveryFailure::terminal(FailureClass::StaleMetadata, "retention_expired")
                .into_outcome(last_committed);
        }
        DeliveryOutcome::ready(last_committed)
    }

    fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), DeliveryFailure> {
        if checkpoint.validate().is_err()
            || checkpoint.feed_id != self.feed.feed_id
            || checkpoint.range_id != self.feed.range.range_id
            || checkpoint.generation != self.feed.generation
        {
            return Err(DeliveryFailure::terminal(
                FailureClass::InvalidRequest,
                "invalid_checkpoint",
            ));
        }
        if checkpoint.epoch != self.feed.range.data_epoch {
            return Err(DeliveryFailure::retryable(
                FailureClass::EpochMismatch,
                "range_epoch_mismatch",
            ));
        }
        Ok(())
    }
}

/// Configured delivery limits. Every limit is explicit so an unbounded buffer
/// cannot silently absorb consumer lag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryBudget {
    /// Maximum events permitted in the unacknowledged delivery buffer.
    pub max_buffered_events: usize,
    /// Maximum bytes permitted in the unacknowledged delivery buffer.
    pub max_buffered_bytes: u64,
    /// Maximum observed consumer lag before delivery is throttled.
    pub max_consumer_lag: u64,
}

impl DeliveryBudget {
    /// Creates a non-zero budget for all three bounded resources.
    pub fn new(
        max_buffered_events: usize,
        max_buffered_bytes: u64,
        max_consumer_lag: u64,
    ) -> Result<Self, RetentionError> {
        if max_buffered_events == 0 || max_buffered_bytes == 0 || max_consumer_lag == 0 {
            return Err(RetentionError::ZeroDeliveryBudget);
        }
        Ok(Self {
            max_buffered_events,
            max_buffered_bytes,
            max_consumer_lag,
        })
    }

    /// Classifies buffer and lag usage without dropping a queued event.
    #[must_use]
    pub fn evaluate(
        &self,
        usage: DeliveryUsage,
        last_committed: Option<Checkpoint>,
    ) -> DeliveryOutcome {
        if usage.storage_limit_reached {
            return DeliveryFailure::terminal(FailureClass::InvalidRequest, "resource_limit")
                .into_outcome(last_committed);
        }
        if usage.buffered_events > self.max_buffered_events
            || usage.buffered_bytes > self.max_buffered_bytes
            || usage.consumer_lag > self.max_consumer_lag
        {
            return DeliveryFailure::retryable(FailureClass::Timeout, "backpressure")
                .into_outcome(last_committed);
        }
        DeliveryOutcome::ready(last_committed)
    }
}

/// Observed state used to evaluate one delivery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryUsage {
    /// Events currently buffered but not durably acknowledged.
    pub buffered_events: usize,
    /// Bytes currently buffered but not durably acknowledged.
    pub buffered_bytes: u64,
    /// Consumer distance from the source's retained head.
    pub consumer_lag: u64,
    /// Whether the Durable storage resource itself has reached its limit.
    pub storage_limit_reached: bool,
}

/// Explicit lifecycle transition observed by a delivery loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryTransition {
    /// The consumer ended a poll or stream before the normal boundary.
    EarlyClose,
    /// The caller deadline elapsed while delivery was still incomplete.
    Timeout,
    /// The caller explicitly cancelled the feed operation.
    Cancel,
    /// The caller reconnects from the last committed checkpoint.
    Reconnect,
}

/// Canonical terminal, retryable, or resumable delivery decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryOutcome {
    /// Canonical state for the delivery operation.
    pub operation_state: OperationState,
    /// Stable failure class when delivery cannot continue now.
    pub failure_class: Option<FailureClass>,
    /// Machine-readable result code, including close/reconnect reason.
    pub reason_code: Option<String>,
    /// Whether remediation and retry can continue from the returned checkpoint.
    pub retryable: bool,
    /// Most recent checkpoint proven durable before this outcome.
    pub last_committed_checkpoint: Option<Checkpoint>,
    /// Explicit next checkpoint from which a retry/reconnect may resume.
    pub next_resume_position: Option<Checkpoint>,
}

impl DeliveryOutcome {
    /// Turns close, timeout, cancel, and reconnect into explicit outcomes that
    /// preserve the last committed checkpoint instead of silently dropping it.
    #[must_use]
    pub fn transition(transition: DeliveryTransition, last_committed: Option<Checkpoint>) -> Self {
        match transition {
            DeliveryTransition::EarlyClose => Self {
                operation_state: OperationState::Cancelled,
                failure_class: None,
                reason_code: Some("early_close".to_string()),
                retryable: false,
                next_resume_position: last_committed.clone(),
                last_committed_checkpoint: last_committed,
            },
            DeliveryTransition::Timeout => {
                DeliveryFailure::retryable(FailureClass::Timeout, "delivery_timeout")
                    .into_outcome(last_committed)
            }
            DeliveryTransition::Cancel => Self {
                operation_state: OperationState::Cancelled,
                failure_class: None,
                reason_code: Some("cancelled".to_string()),
                retryable: false,
                next_resume_position: last_committed.clone(),
                last_committed_checkpoint: last_committed,
            },
            DeliveryTransition::Reconnect => Self {
                operation_state: OperationState::RecoveryPending,
                failure_class: None,
                reason_code: Some("reconnect".to_string()),
                retryable: true,
                next_resume_position: last_committed.clone(),
                last_committed_checkpoint: last_committed,
            },
        }
    }

    /// Returns whether the source may hand the caller another event now.
    #[must_use]
    pub const fn may_deliver(&self) -> bool {
        matches!(
            self.operation_state,
            OperationState::Accepted | OperationState::Running | OperationState::RecoveryPending
        ) && self.failure_class.is_none()
    }

    fn ready(last_committed: Option<Checkpoint>) -> Self {
        Self {
            operation_state: OperationState::Accepted,
            failure_class: None,
            reason_code: None,
            retryable: false,
            next_resume_position: last_committed.clone(),
            last_committed_checkpoint: last_committed,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DeliveryFailure {
    failure_class: FailureClass,
    reason_code: &'static str,
    retryable: bool,
}

impl DeliveryFailure {
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

    fn into_outcome(self, last_committed: Option<Checkpoint>) -> DeliveryOutcome {
        DeliveryOutcome {
            operation_state: if self.retryable {
                OperationState::RetryableFailure
            } else {
                OperationState::TerminalFailure
            },
            failure_class: Some(self.failure_class),
            reason_code: Some(self.reason_code.to_string()),
            retryable: self.retryable,
            next_resume_position: last_committed.clone(),
            last_committed_checkpoint: last_committed,
        }
    }
}

/// Errors returned while creating an invalid retention policy or delivery
/// budget before an operation carries enough request context for an outcome.
#[derive(Debug, thiserror::Error)]
pub enum RetentionError {
    /// Feed identity failed common changefeed validation.
    #[error("invalid feed: {0}")]
    InvalidFeed(super::ChangefeedModelError),
    /// The configured retained position did not belong to the policy feed.
    #[error("retained checkpoint does not match the retention policy feed")]
    InvalidRetainedCheckpoint,
    /// One of the explicit delivery limits was zero.
    #[error("delivery budget limits must all be greater than zero")]
    ZeroDeliveryBudget,
}
