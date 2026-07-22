//! Immutable committed metadata and its admission validator.

mod command;
mod consensus;
mod model;

pub use command::{
    AuthorizationScope, MetadataActor, MetadataCommand, MetadataCommandEnvelope,
    MetadataCommandValidator, MetadataValidationError, ValidatedMetadataCommand,
    ValidationDecision,
};
pub use consensus::{
    ChirpsMetadataBackend, ChirpsMetadataConsensusAdapter, MetadataConsensusError,
    MetadataConsensusStore, MetadataSnapshot, compiled_chirps_bootstrap,
};
pub use model::{
    ClusterReadConsistency, ClusterReadPolicy, CommittedMetadata, ManagementOutcome,
    ManagementOutcomeClass, MemberLifecycle, MemberRecord, ObservedHealth, RangeCoverageProof,
    RangeReplicaEvidence, RangeReplicaLifecycle, RangeRoutingDefinition, ReadPolicyOverride,
    SchemaApplyEvidence, SchemaApplyState, SchemaCompatibility, SchemaManifest, SchemaRolloutState,
};
