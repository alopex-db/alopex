//! Cluster control services built on immutable committed metadata.

mod membership;
mod range_directory;
mod range_transfer;
mod schema;
mod schema_apply;
mod upgrade;

pub use membership::{
    EnrollmentCredential, MembershipOperation, MembershipOperationKind, MembershipOperationStore,
    MembershipSaga, RaftMembershipView,
};

pub use range_directory::{
    RangeDirectory, RangeDirectoryError, RangeReplicaDirectory, RangeReplicaReadiness,
    RangeReplicaReadinessState, RangeTransition, RangeTransitionKind,
};

pub use range_transfer::{
    RangeChangeEnvelope, RangeSnapshotChunk, RangeSnapshotEntry, RangeTransferAck,
    RangeTransferApplyOutcome, RangeTransferError, RangeTransferExpectation,
    RangeTransferFrameHandler, RangeTransferManifest, RangeTransferResumePoint,
    RangeTransferSession, RangeTransferWireFrame, RangeTransferWireMessage,
    VerifiedRangeTransferReceiver,
};

pub use schema::{SchemaControlError, SchemaControlResult, SchemaControlService};
pub use schema_apply::{SchemaApplyEvidenceAdapter, SchemaApplyEvidenceRequest};
pub use upgrade::{
    SUPPORTED_UPGRADE_SOURCE_VERSION, UpgradeCheckpoint, UpgradeInput, UpgradeOperation,
    UpgradeOutcome, UpgradePlanner, UpgradePlanningError, UpgradeSourceKind,
};
