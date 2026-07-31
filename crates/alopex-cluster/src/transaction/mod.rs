//! Public, transport-neutral contracts for v0.9 distributed transactions.
//!
//! The coordinator, routing, and participant implementations are deliberately
//! separate.  This module exposes only the stable outcome that those layers
//! must agree on.

mod outcome;
mod routing;

pub use outcome::{
    TransactionIsolation, TransactionOutcome, TransactionOutcomeError, TransactionParticipant,
};
pub use routing::{
    TransactionRoutePlan, TransactionRouteRequest, TransactionRouteTarget, TransactionRoutingError,
    TransactionRoutingPlanner,
};
