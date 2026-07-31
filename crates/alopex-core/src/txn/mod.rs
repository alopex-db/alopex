//! Transaction management traits.

mod participant;

pub use participant::{
    ParticipantDecision, ParticipantDecisionRecord, ParticipantDecisionResult,
    ParticipantDecisionStore, ParticipantIdentity, ParticipantJournalPhase, ParticipantOpenResult,
    ParticipantTransaction, ParticipantTransactionError, ParticipantTransactionState,
    WalParticipantDecisionStore,
};

use crate::error::Result;
use crate::types::TxnMode;

/// Terminal input reported by a stream lease to its owning session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedLeaseOutcome {
    /// The result was fully consumed and leaves a transaction committable.
    Exhausted,
    /// The consumer ended early.  The generic core contract conservatively requires rollback.
    Closed,
    /// Cancellation or timeout invalidated the active operation.
    Cancelled,
    /// A source, conversion, or resource failure invalidated the active operation.
    Failed,
}

/// One-way lifecycle state for an owned read-only session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedReadSessionStatus {
    /// The session is ready for its single stream lease.
    Open,
    /// One lease currently owns iterator advancement.
    LeaseActive,
    /// The stream ended normally and released its transaction.
    Exhausted,
    /// The consumer closed the stream before normal end.
    Closed,
    /// The consumer cancelled the stream or its deadline elapsed.
    Cancelled,
    /// Source cleanup failed after a stream error.
    Failed,
}

impl OwnedReadSessionStatus {
    /// Return whether this status has no legal later transition.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Exhausted | Self::Closed | Self::Cancelled | Self::Failed
        )
    }
}

impl From<OwnedLeaseOutcome> for OwnedReadSessionStatus {
    fn from(outcome: OwnedLeaseOutcome) -> Self {
        match outcome {
            OwnedLeaseOutcome::Exhausted => Self::Exhausted,
            OwnedLeaseOutcome::Closed => Self::Closed,
            OwnedLeaseOutcome::Cancelled => Self::Cancelled,
            OwnedLeaseOutcome::Failed => Self::Failed,
        }
    }
}

/// One-way lifecycle state for an owned transaction and its optional stream lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedTransactionSessionStatus {
    /// No stream lease is active; point operations and a final action are allowed.
    Open,
    /// Exactly one lease owns iterator advancement.  Commit and rollback are blocked.
    LeaseActive,
    /// A normally exhausted lease released the transaction for later commit or another lease.
    Committable,
    /// An early close, cancellation, failure, or dropped lease requires rollback.
    MustAbort,
    /// Commit completed exactly once.
    Committed,
    /// Rollback completed exactly once.
    RolledBack,
    /// A terminal action consumed the backend transaction but returned an error.
    Closed,
}

impl OwnedTransactionSessionStatus {
    /// Return whether a new lease may be started.
    pub const fn can_acquire_lease(self) -> bool {
        matches!(self, Self::Open | Self::Committable)
    }

    /// Return whether commit may consume the transaction.
    pub const fn can_commit(self) -> bool {
        matches!(self, Self::Open | Self::Committable)
    }

    /// Return whether rollback may consume the transaction.
    pub const fn can_rollback(self) -> bool {
        matches!(self, Self::Open | Self::Committable | Self::MustAbort)
    }
}

impl From<OwnedLeaseOutcome> for OwnedTransactionSessionStatus {
    fn from(outcome: OwnedLeaseOutcome) -> Self {
        match outcome {
            OwnedLeaseOutcome::Exhausted => Self::Committable,
            OwnedLeaseOutcome::Closed
            | OwnedLeaseOutcome::Cancelled
            | OwnedLeaseOutcome::Failed => Self::MustAbort,
        }
    }
}

/// A manager for creating and committing transactions.
///
/// This trait is generic over the transaction type it manages.
pub trait TxnManager<'a, T> {
    /// Begins a new transaction in the specified mode.
    fn begin(&'a self, mode: TxnMode) -> Result<T>;

    /// Commits a transaction, applying its changes to the store.
    fn commit(&'a self, txn: T) -> Result<()>;

    /// Rolls back a transaction, discarding its changes.
    fn rollback(&'a self, txn: T) -> Result<()>;
}
