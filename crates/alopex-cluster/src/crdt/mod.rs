//! Canonical contracts shared by the Phase 2 Counter and Set adapters.
//!
//! This module intentionally contains no projection, persistence or transport
//! implementation. Those concerns are owned by the ledger, Counter/Set and
//! coordinator modules so every public surface starts from one identity and
//! outcome contract.

mod coordinator;
mod counter;
mod envelope;
mod ledger;
mod outcome;
mod policy;
mod set;

pub use coordinator::{
    CrdtConvergenceCoordinator, CrdtCoordinationOutcome, CrdtCoordinatorConfig,
    CrdtReadinessEvidence, CrdtReadinessGate, CrdtReplicaObservation, accepted_digest_counts,
};
pub use counter::{
    CounterApplyResult, CounterProjectionState, CounterValue, CrdtCounterError,
    CrdtCounterProjection,
};
pub use envelope::{
    CrdtCommonFields, CrdtEnvelopeError, CrdtObjectType, CrdtOperationEnvelope, CrdtOperationKind,
    CrdtPayload,
};
pub use ledger::{
    CrdtLedgerAdmission, CrdtLedgerError, CrdtLedgerIdentity, CrdtLedgerRecord, CrdtOperationLedger,
};
pub use outcome::{CrdtOutcome, CrdtSurfaceStatus, CrdtValue};
pub use policy::{
    CrdtLifecycleAction, CrdtPolicyDecision, CrdtPolicyInput, CrdtPreExecutionPolicy,
    CrdtRangeFreshness,
};
pub use set::{
    CrdtSetError, CrdtSetProjection, SetApplyResult, SetMemberVersion, SetProjectionLimits,
    SetProjectionState, SetValue,
};
