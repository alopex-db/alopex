//! Fenced planning contracts for v0.8 distributed reads.
//!
//! This module deliberately plans only remote targets.  A missing or
//! incompatible target is a classified planning failure, never a signal to
//! retry through a local SQL path.

pub mod auth;
pub mod planner;
pub mod transport;
pub mod worker;

#[cfg(test)]
mod worker_tests;

pub use auth::{
    AuthenticatedSubject, DelegationAuthorizationError, DelegationValidationContext,
    LocalReadAuthorizationRecheck, LocalReadAuthorizationRequest, ReadDelegationCredential,
    ReadDelegationVerifier, ReadOperationScope, verify_and_recheck,
};

pub use planner::{
    DistributedReadPlan, RangeTarget, ReadFence, ReadModeRequest, ReadRoutePlanRequest,
    ReadRoutePlanner, ReadRoutePlanningError, RouteDecision,
};
pub use transport::{
    CleanupAcknowledgement, RangeReadBatch, RangeReadEnd, RemoteRangeReadRequest,
    RemoteRangeReadRequestError, descriptor_digest, range_fence_digest,
};
pub use transport::{RemoteReadAuthorizationEnvelope, authorize_remote_read};
pub use worker::{
    FencedRangeReadBackend, FencedRangeReadSession, RangeReadExecution, RangeReadWorker,
    RangeReadWorkerClock, RangeReadWorkerConfig, RangeReadWorkerConfigError, RangeReadWorkerError,
};
