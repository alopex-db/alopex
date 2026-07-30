//! Canonical contracts for the v0.9 durable-profile changefeed.
//!
//! This module deliberately contains only the cross-surface logical model.
//! Journal adaptation, Durable capability checks, delivery coordination, and
//! checkpoint persistence are separate responsibilities added by later Phase 3
//! tasks.  Keeping the contract here prevents a transport or a storage record
//! from becoming the public feed schema by accident.

mod model;

pub use model::{
    AckResult, AckState, ChangeEventEnvelope, ChangeOperationType, ChangePayload,
    ChangefeedModelError, ChangefeedOutcome, ChangefeedResult, ChangefeedSurfaceStatus, Checkpoint,
    FeedIdentity, OrderingScope, RetentionWindow,
};
