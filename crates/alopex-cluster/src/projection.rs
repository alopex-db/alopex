//! Read-only projection of committed metadata plus non-authoritative evidence.

use crate::{
    CommittedMetadata, MemberLifecycle, NodeId, ObservedHealth, RangeReplicaDirectory,
    RangeReplicaReadiness, SchemaApplyState, SchemaManifestId,
};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataProjectionFreshness {
    Committed,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedMember {
    pub node_id: NodeId,
    pub committed_lifecycle: MemberLifecycle,
    pub observed_health: Option<ObservedHealth>,
}

/// Committed, per-member schema rollout evidence.  This deliberately carries
/// the verified facts behind an `Applied` state instead of deriving it from
/// liveness or a local applier attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedSchemaApply {
    pub member: NodeId,
    pub manifest_id: SchemaManifestId,
    pub state: SchemaApplyState,
    pub catalog_version: Option<u64>,
    pub checksum: Option<String>,
    pub compatibility_verified: bool,
    pub failure_detail: Option<String>,
}

/// Read-only schema-management status from one committed metadata version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedSchemaRollout {
    pub state_version: u64,
    pub owner: Option<NodeId>,
    pub active_manifest: Option<SchemaManifestId>,
    pub members: Vec<ProjectedSchemaApply>,
}

/// A view that is current only when it came from one committed metadata
/// version. Reachability evidence is attached for diagnostics and cannot alter
/// membership, placement, or schema ownership here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedMetadataProjection {
    pub freshness: MetadataProjectionFreshness,
    pub metadata: Option<CommittedMetadata>,
    pub members: Vec<ProjectedMember>,
    /// Routing eligibility derived from the same committed range/replica state.
    pub replica_readiness: Vec<RangeReplicaReadiness>,
    /// Schema ownership and apply evidence from the same committed version.
    /// `None` means the metadata projection itself is unavailable.
    pub schema_rollout: Option<ProjectedSchemaRollout>,
}

impl CommittedMetadataProjection {
    pub fn unavailable() -> Self {
        Self {
            freshness: MetadataProjectionFreshness::Unavailable,
            metadata: None,
            members: Vec::new(),
            replica_readiness: Vec::new(),
            schema_rollout: None,
        }
    }

    pub fn is_current(&self) -> bool {
        self.freshness == MetadataProjectionFreshness::Committed
    }
}

#[derive(Debug, Default)]
pub struct CommittedMetadataProjector;

impl CommittedMetadataProjector {
    pub fn project(
        &self,
        metadata: &CommittedMetadata,
        observed_health: &BTreeMap<NodeId, ObservedHealth>,
    ) -> CommittedMetadataProjection {
        let members = metadata
            .members()
            .iter()
            .map(|(node_id, record)| ProjectedMember {
                node_id: node_id.clone(),
                committed_lifecycle: record.lifecycle,
                observed_health: observed_health
                    .get(node_id)
                    .cloned()
                    .or_else(|| record.observed_health.clone()),
            })
            .collect();
        let replica_readiness = RangeReplicaDirectory::from_committed(metadata)
            .entries()
            .to_vec();
        let schema_rollout = ProjectedSchemaRollout {
            state_version: metadata.state_version(),
            owner: metadata.schema().owner.clone(),
            active_manifest: metadata.schema().active_manifest.clone(),
            members: metadata
                .schema()
                .member_apply
                .values()
                .map(|evidence| ProjectedSchemaApply {
                    member: evidence.member.clone(),
                    manifest_id: evidence.manifest_id.clone(),
                    state: evidence.state,
                    catalog_version: evidence.catalog_version,
                    checksum: evidence.checksum.clone(),
                    compatibility_verified: evidence.compatibility_verified,
                    failure_detail: evidence.failure_detail.clone(),
                })
                .collect(),
        };
        CommittedMetadataProjection {
            freshness: MetadataProjectionFreshness::Committed,
            metadata: Some(metadata.clone()),
            members,
            replica_readiness,
            schema_rollout: Some(schema_rollout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClusterId, MemberIdentity, MemberRecord, NodeRole, SchemaApplyEvidence, SchemaManifestId,
    };

    #[test]
    fn gossip_evidence_cannot_mutate_committed_member_lifecycle() {
        let mut metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        let member = MemberRecord::new(
            MemberIdentity {
                node_id: NodeId::new("node-a"),
                cluster_id: Some(ClusterId::new("cluster-a")),
                advertised_endpoint: None,
                role: NodeRole::Worker,
            },
            MemberLifecycle::Active,
        );
        // This is a consensus-only fixture hook, not a reachability update.
        metadata.record_member_for_apply(member);
        let observations = BTreeMap::from([(
            NodeId::new("node-a"),
            ObservedHealth {
                observed_at_epoch: 9,
                replication_lag: Some(4),
                reachable: false,
            },
        )]);

        let projection = CommittedMetadataProjector.project(&metadata, &observations);

        assert!(projection.is_current());
        assert_eq!(
            projection.members[0].committed_lifecycle,
            MemberLifecycle::Active
        );
        assert!(
            !projection.members[0]
                .observed_health
                .as_ref()
                .unwrap()
                .reachable
        );
        assert_eq!(
            metadata.members()[&NodeId::new("node-a")].lifecycle,
            MemberLifecycle::Active
        );
    }

    #[test]
    fn unavailable_projection_cannot_claim_current_metadata() {
        let projection = CommittedMetadataProjection::unavailable();

        assert!(!projection.is_current());
        assert!(projection.metadata.is_none());
        assert!(projection.schema_rollout.is_none());
    }

    #[test]
    fn schema_projection_preserves_failed_evidence_without_calling_it_applied() {
        let mut metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        metadata.schema_mut_for_apply().owner = Some(NodeId::new("node-a"));
        metadata.schema_mut_for_apply().active_manifest = Some(SchemaManifestId::new("schema-1"));
        metadata.schema_mut_for_apply().member_apply.insert(
            NodeId::new("node-b"),
            SchemaApplyEvidence {
                manifest_id: SchemaManifestId::new("schema-1"),
                member: NodeId::new("node-b"),
                state: crate::SchemaApplyState::Failed,
                catalog_version: Some(9),
                checksum: Some("wrong".to_string()),
                compatibility_verified: false,
                failure_detail: Some("checksum mismatch".to_string()),
            },
        );

        let projection = CommittedMetadataProjector.project(&metadata, &BTreeMap::new());
        let schema = projection.schema_rollout.expect("committed schema rollout");
        assert_eq!(schema.owner, Some(NodeId::new("node-a")));
        assert_eq!(schema.members[0].state, crate::SchemaApplyState::Failed);
        assert!(!schema.members[0].compatibility_verified);
    }
}
