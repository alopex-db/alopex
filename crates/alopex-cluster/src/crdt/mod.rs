//! Canonical contracts shared by the Phase 2 Counter and Set adapters.
//!
//! This module intentionally contains no projection, persistence or transport
//! implementation. Those concerns are owned by the ledger, Counter/Set and
//! coordinator modules so every public surface starts from one identity and
//! outcome contract.

mod envelope;

pub use envelope::{
    CrdtCommonFields, CrdtEnvelopeError, CrdtObjectType, CrdtOperationEnvelope, CrdtOperationKind,
    CrdtPayload,
};
