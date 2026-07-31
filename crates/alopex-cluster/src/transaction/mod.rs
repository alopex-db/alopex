//! Public, transport-neutral contracts for v0.9 distributed transactions.
//!
//! The coordinator, routing, and participant implementations are deliberately
//! separate.  This module exposes only the stable outcome that those layers
//! must agree on.

mod coordinator;
mod outcome;
mod recovery;
mod routing;

pub use coordinator::{
    BlockedTransactionAdmissionVerifier, CommittedMetadataProvider,
    CommittedTransactionAdmissionVerifier, TransactionActorAuthorizer, TransactionAdmissionError,
    TransactionAdmissionVerifier, TransactionCoordinator, TransactionCoordinatorError,
    TransactionParticipantAck, TransactionParticipantDriver,
};
pub use outcome::{
    TransactionIsolation, TransactionOutcome, TransactionOutcomeError, TransactionParticipant,
};
pub use recovery::{TransactionDecision, TransactionIntent, TransactionRecoveryError};
pub use routing::{
    TransactionRoutePlan, TransactionRouteRequest, TransactionRouteTarget, TransactionRoutingError,
    TransactionRoutingPlanner,
};
