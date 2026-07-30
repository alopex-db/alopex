//! Canonical contracts for the v0.9 durable-profile changefeed.
//!
//! This module deliberately contains only the cross-surface logical model.
//! Journal adaptation, Durable capability checks, delivery coordination, and
//! checkpoint persistence are separate responsibilities added by later Phase 3
//! tasks.  Keeping the contract here prevents a transport or a storage record
//! from becoming the public feed schema by accident.

mod checkpoint;
mod coordinator;
mod cursor;
mod durable;
mod journal_adapter;
mod model;
mod retention;

pub use checkpoint::{
    AckProcessor, AckRequest, CheckpointError, CheckpointStore, ResumePlan, ResumePlanner,
    ResumeSourceStatus,
};
pub use coordinator::{
    CoordinatorError, DurablePreflightEvidence, FeedCoordinator, FeedDelivery, FeedPreflight,
    FeedRequest,
};
pub use cursor::{CheckpointCursor, CheckpointPosition, CursorError, EventIdentity};
pub use durable::{
    DurableAuthorization, DurableCapabilityVersion, DurableProfileAdapter, DurableProfileEvidence,
};
pub use journal_adapter::{JournalAdapterError, JournalEventAdapter};

pub use model::{
    AckResult, AckState, ChangeEventEnvelope, ChangeOperationType, ChangePayload,
    ChangefeedModelError, ChangefeedOutcome, ChangefeedResult, ChangefeedSurfaceStatus, Checkpoint,
    FeedIdentity, OrderingScope, RetentionWindow,
};
pub use retention::{
    DeliveryBudget, DeliveryOutcome, DeliveryTransition, DeliveryUsage, RetentionError,
    RetentionPolicy,
};
