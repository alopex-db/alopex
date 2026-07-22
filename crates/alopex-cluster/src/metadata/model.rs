//! Versioned, committed cluster metadata types.
//!
//! The public fields on the leaf records are serialization contracts. The
//! aggregate [`CommittedMetadata`] keeps its collections private so callers
//! can observe an immutable committed version but cannot manufacture a new
//! authoritative state outside the consensus boundary.

use super::command::{MetadataCommand, MetadataCommandEnvelope};
use crate::{
    ClusterId, MemberIdentity, NodeId, NodeRole, PlacementMetadata, RangeId, RequestId,
    SchemaManifestId, StableDiagnosticCode, TableId, TableRef,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub type MetadataVersion = u64;

/// Durable membership lifecycle, distinct from transient reachability input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberLifecycle {
    Admitted,
    LearnerAdded,
    CaughtUp,
    JointConsensusCommitted,
    MetadataPublished,
    Active,
    Draining,
    VoterRemoved,
    PlacementSafe,
    Retired,
    RecoveryRequired,
}

/// Non-authoritative liveness evidence retained for projection diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedHealth {
    pub observed_at_epoch: u64,
    #[serde(default)]
    pub replication_lag: Option<u64>,
    pub reachable: bool,
}

/// A member's committed identity and intended lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberRecord {
    pub identity: MemberIdentity,
    pub intended_role: NodeRole,
    pub lifecycle: MemberLifecycle,
    #[serde(default)]
    pub observed_health: Option<ObservedHealth>,
}

impl MemberRecord {
    pub fn new(identity: MemberIdentity, lifecycle: MemberLifecycle) -> Self {
        Self {
            intended_role: identity.role,
            identity,
            lifecycle,
            observed_health: None,
        }
    }
}

/// A canonical primary-row-key interval. Bounds are half-open
/// `[lower_inclusive, upper_exclusive)` and intentionally use bytes rather
/// than SQL or secondary-index ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeRoutingDefinition {
    pub range_id: RangeId,
    pub table_ref: TableRef,
    pub table_id: TableId,
    #[serde(default)]
    pub lower_inclusive: Option<Vec<u8>>,
    #[serde(default)]
    pub upper_exclusive: Option<Vec<u8>>,
    pub generation: u64,
}

impl RangeRoutingDefinition {
    pub fn contains(&self, row_key: &[u8]) -> bool {
        let lower_matches = self
            .lower_inclusive
            .as_deref()
            .is_none_or(|lower| row_key >= lower);
        let upper_matches = self
            .upper_exclusive
            .as_deref()
            .is_none_or(|upper| row_key < upper);
        lower_matches && upper_matches
    }
}

/// Evidence that one replica has verified complete range contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeCoverageProof {
    pub generation: u64,
    #[serde(default)]
    pub lower_inclusive: Option<Vec<u8>>,
    #[serde(default)]
    pub upper_exclusive: Option<Vec<u8>>,
    pub data_epoch: u64,
    pub index_epoch: u64,
    pub content_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeReplicaLifecycle {
    Provisioning,
    CatchingUp,
    Ready,
    Stale,
    Failed,
    Retired,
}

/// A physical range replica's committed readiness evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeReplicaEvidence {
    pub range_id: RangeId,
    pub node_id: NodeId,
    pub generation: u64,
    #[serde(default)]
    pub schema_manifest_id: Option<SchemaManifestId>,
    pub data_epoch: u64,
    pub index_epoch: u64,
    pub lifecycle: RangeReplicaLifecycle,
    #[serde(default)]
    pub coverage: Option<RangeCoverageProof>,
}

/// Cluster-level default and per-query permitted read semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterReadConsistency {
    Leader,
    Quorum,
    BoundedStaleness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadPolicyOverride {
    pub consistency: ClusterReadConsistency,
    #[serde(default)]
    pub max_staleness_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterReadPolicy {
    pub default_consistency: ClusterReadConsistency,
    #[serde(default)]
    pub permitted_overrides: BTreeSet<ClusterReadConsistency>,
}

impl Default for ClusterReadPolicy {
    fn default() -> Self {
        Self {
            default_consistency: ClusterReadConsistency::Leader,
            permitted_overrides: BTreeSet::new(),
        }
    }
}

/// Compatibility declaration for a versioned catalog delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaCompatibility {
    pub minimum_catalog_version: u64,
    pub maximum_catalog_version: u64,
}

/// Immutable metadata-management manifest. `catalog_delta` is a versioned
/// catalog representation, never a transported user SQL statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaManifest {
    pub id: SchemaManifestId,
    #[serde(default)]
    pub parent_id: Option<SchemaManifestId>,
    pub schema_version: u64,
    pub catalog_delta_format: String,
    pub catalog_delta: Vec<u8>,
    pub checksum: String,
    pub compatibility: SchemaCompatibility,
    pub owner: NodeId,
    pub created_at_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaApplyState {
    Pending,
    Applying,
    Applied,
    Failed,
    Incompatible,
}

/// Immutable, member-authenticated evidence for one schema-manifest apply
/// attempt.  A member is never considered [`SchemaApplyState::Applied`] from
/// a liveness signal alone: the evidence retains the manifest identity and
/// the catalog facts that the admission boundary verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaApplyEvidence {
    pub manifest_id: SchemaManifestId,
    pub member: NodeId,
    pub state: SchemaApplyState,
    #[serde(default)]
    pub catalog_version: Option<u64>,
    #[serde(default)]
    pub checksum: Option<String>,
    #[serde(default)]
    pub compatibility_verified: bool,
    #[serde(default)]
    pub failure_detail: Option<String>,
}

impl SchemaApplyEvidence {
    pub fn pending(manifest_id: impl Into<SchemaManifestId>, member: impl Into<NodeId>) -> Self {
        Self {
            manifest_id: manifest_id.into(),
            member: member.into(),
            state: SchemaApplyState::Pending,
            catalog_version: None,
            checksum: None,
            compatibility_verified: false,
            failure_detail: None,
        }
    }
}

/// Committed schema owner, selected manifest, and per-member apply evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaRolloutState {
    #[serde(default)]
    pub owner: Option<NodeId>,
    #[serde(default)]
    pub active_manifest: Option<SchemaManifestId>,
    #[serde(default)]
    pub member_apply: BTreeMap<NodeId, SchemaApplyEvidence>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagementOutcomeClass {
    Pending,
    RetryableFailure,
    TerminalFailure,
    Succeeded,
}

/// Immutable idempotency ledger item stored in committed metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagementOutcome {
    pub request_id: RequestId,
    /// Stable caller-supplied digest of the canonical management envelope.
    /// Reusing a request ID with a different digest is a conflict, not a retry.
    pub request_fingerprint: String,
    pub class: ManagementOutcomeClass,
    #[serde(default)]
    pub committed_version: Option<MetadataVersion>,
    pub reason: StableDiagnosticCode,
}

impl ManagementOutcome {
    pub fn pending(
        request_id: impl Into<RequestId>,
        request_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            request_fingerprint: request_fingerprint.into(),
            class: ManagementOutcomeClass::Pending,
            committed_version: None,
            reason: StableDiagnosticCode::OperationPending,
        }
    }
}

/// The only authoritative metadata aggregate. It is written later by the
/// consensus state machine; this task only defines its immutable shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedMetadata {
    cluster_id: ClusterId,
    state_version: MetadataVersion,
    members: BTreeMap<NodeId, MemberRecord>,
    ranges: BTreeMap<RangeId, RangeRoutingDefinition>,
    placements: BTreeMap<RangeId, PlacementMetadata>,
    range_replicas: BTreeMap<RangeId, BTreeMap<NodeId, RangeReplicaEvidence>>,
    read_policy: ClusterReadPolicy,
    schema: SchemaRolloutState,
    schema_manifests: BTreeMap<SchemaManifestId, SchemaManifest>,
    membership_operations: BTreeMap<RequestId, ManagementOutcome>,
    operations: BTreeMap<RequestId, ManagementOutcome>,
}

impl CommittedMetadata {
    pub fn new(cluster_id: impl Into<ClusterId>) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            state_version: 0,
            members: BTreeMap::new(),
            ranges: BTreeMap::new(),
            placements: BTreeMap::new(),
            range_replicas: BTreeMap::new(),
            read_policy: ClusterReadPolicy::default(),
            schema: SchemaRolloutState::default(),
            schema_manifests: BTreeMap::new(),
            membership_operations: BTreeMap::new(),
            operations: BTreeMap::new(),
        }
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn state_version(&self) -> MetadataVersion {
        self.state_version
    }

    pub fn members(&self) -> &BTreeMap<NodeId, MemberRecord> {
        &self.members
    }

    pub fn ranges(&self) -> &BTreeMap<RangeId, RangeRoutingDefinition> {
        &self.ranges
    }

    pub fn placements(&self) -> &BTreeMap<RangeId, PlacementMetadata> {
        &self.placements
    }

    pub fn range_replicas(&self) -> &BTreeMap<RangeId, BTreeMap<NodeId, RangeReplicaEvidence>> {
        &self.range_replicas
    }

    pub fn read_policy(&self) -> &ClusterReadPolicy {
        &self.read_policy
    }

    pub fn schema(&self) -> &SchemaRolloutState {
        &self.schema
    }

    pub fn schema_manifests(&self) -> &BTreeMap<SchemaManifestId, SchemaManifest> {
        &self.schema_manifests
    }

    pub fn membership_operation(&self, request_id: &RequestId) -> Option<&ManagementOutcome> {
        self.membership_operations.get(request_id)
    }

    pub fn operation(&self, request_id: &RequestId) -> Option<&ManagementOutcome> {
        self.operations.get(request_id)
    }

    /// Mutation hook reserved for the consensus apply boundary. It is not part
    /// of the public observation API and is used only after validation.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn record_operation_for_apply(&mut self, outcome: ManagementOutcome) {
        self.operations.insert(outcome.request_id.clone(), outcome);
    }

    /// Mutation hook reserved for a validated consensus apply. It exists so
    /// range admission tests and the later state machine share one model.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn record_range_for_apply(&mut self, definition: RangeRoutingDefinition) {
        self.ranges.insert(definition.range_id.clone(), definition);
    }

    /// Mutation hook reserved for validated consensus application.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn record_member_for_apply(&mut self, member: MemberRecord) {
        self.members.insert(member.identity.node_id.clone(), member);
    }

    /// Mutation hook reserved for validated consensus application.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn record_replica_for_apply(&mut self, evidence: RangeReplicaEvidence) {
        self.range_replicas
            .entry(evidence.range_id.clone())
            .or_default()
            .insert(evidence.node_id.clone(), evidence);
    }

    /// Test-only-style fixture hook for projections that need to prove they
    /// faithfully render already committed schema evidence.  Production
    /// mutation remains limited to `apply_validated_for_consensus` below.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn schema_mut_for_apply(&mut self) -> &mut SchemaRolloutState {
        &mut self.schema
    }

    /// Applies an already validated envelope to a new immutable committed
    /// version. This is deliberately crate-private: only the consensus adapter
    /// may make the returned value authoritative.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn apply_validated_for_consensus(&self, envelope: &MetadataCommandEnvelope) -> Self {
        let mut next = self.clone();
        next.state_version = next.state_version.saturating_add(1);
        match &envelope.command {
            MetadataCommand::AdmitMember { member } => {
                next.members
                    .insert(member.identity.node_id.clone(), member.clone());
            }
            MetadataCommand::ReplaceMember {
                retired_node_id,
                replacement,
            } => {
                if let Some(retired) = next.members.get_mut(retired_node_id) {
                    retired.lifecycle = MemberLifecycle::Retired;
                }
                next.members
                    .insert(replacement.identity.node_id.clone(), replacement.clone());
            }
            MetadataCommand::RegisterRange { definition }
            | MetadataCommand::UpdateRange { definition } => {
                next.ranges
                    .insert(definition.range_id.clone(), definition.clone());
            }
            MetadataCommand::RetireRange { range_id } => {
                next.ranges.remove(range_id);
                next.placements.remove(range_id);
                next.range_replicas.remove(range_id);
            }
            MetadataCommand::SetPlacement {
                range_id,
                placement,
            } => {
                next.placements.insert(range_id.clone(), placement.clone());
            }
            MetadataCommand::RecordRangeReplica { evidence } => {
                next.range_replicas
                    .entry(evidence.range_id.clone())
                    .or_default()
                    .insert(evidence.node_id.clone(), evidence.clone());
            }
            MetadataCommand::SetReadPolicy { policy } => next.read_policy = policy.clone(),
            MetadataCommand::SetSchemaOwner { owner } => next.schema.owner = Some(owner.clone()),
            MetadataCommand::ProposeSchemaManifest { manifest } => {
                next.schema_manifests
                    .insert(manifest.id.clone(), manifest.clone());
            }
            MetadataCommand::CommitSchemaManifest { manifest_id } => {
                next.schema.active_manifest = Some(manifest_id.clone());
                next.schema.member_apply = next
                    .members
                    .iter()
                    .filter(|(_, member)| member.lifecycle == MemberLifecycle::Active)
                    .map(|(node_id, _)| {
                        (
                            node_id.clone(),
                            SchemaApplyEvidence::pending(manifest_id.clone(), node_id.clone()),
                        )
                    })
                    .collect();
            }
            MetadataCommand::RecordSchemaApply { evidence } => {
                next.schema
                    .member_apply
                    .insert(evidence.member.clone(), evidence.clone());
            }
        }
        let outcome = ManagementOutcome {
            request_id: envelope.request_id.clone(),
            request_fingerprint: envelope.request_fingerprint.clone(),
            class: ManagementOutcomeClass::Succeeded,
            committed_version: Some(next.state_version),
            reason: StableDiagnosticCode::MetadataCommitted,
        };
        if matches!(
            &envelope.command,
            MetadataCommand::AdmitMember { .. } | MetadataCommand::ReplaceMember { .. }
        ) {
            next.membership_operations
                .insert(outcome.request_id.clone(), outcome.clone());
        }
        next.operations.insert(outcome.request_id.clone(), outcome);
        next
    }
}
