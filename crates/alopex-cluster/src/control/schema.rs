//! Schema-management control over immutable, committed metadata.
//!
//! This module deliberately coordinates catalog *manifests*, rather than SQL
//! statements.  A catalog applier may later turn a committed manifest into a
//! local catalog mutation, but no user DDL is transported or executed here.

use crate::{
    CommittedMetadata, ManagementOutcome, ManagementOutcomeClass, MetadataCommandEnvelope,
    MetadataCommandValidator, MetadataConsensusError, MetadataConsensusStore,
    MetadataValidationError, SchemaRolloutState, StableDiagnosticCode, ValidationDecision,
};
use std::{error::Error, fmt};

/// Committed schema control result.  `outcome` describes the management
/// request; member apply truth remains in `rollout`, so recording a failed
/// member apply can succeed without ever presenting that member as Applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaControlResult {
    pub outcome: ManagementOutcome,
    pub state_version: u64,
    pub rollout: SchemaRolloutState,
}

/// A classified schema-control failure before a new committed metadata
/// version exists.
#[derive(Debug)]
pub enum SchemaControlError {
    Rejected(MetadataValidationError),
    Consensus(MetadataConsensusError),
}

impl SchemaControlError {
    pub fn class(&self) -> ManagementOutcomeClass {
        match self {
            Self::Rejected(_) => ManagementOutcomeClass::TerminalFailure,
            Self::Consensus(MetadataConsensusError::NotLeader { .. })
            | Self::Consensus(MetadataConsensusError::CapabilityUnavailable { .. })
            | Self::Consensus(MetadataConsensusError::Storage { .. }) => {
                ManagementOutcomeClass::RetryableFailure
            }
            Self::Consensus(MetadataConsensusError::Snapshot { .. }) => {
                ManagementOutcomeClass::TerminalFailure
            }
        }
    }

    pub fn code(&self) -> StableDiagnosticCode {
        match self {
            Self::Rejected(error) => error.code,
            Self::Consensus(MetadataConsensusError::NotLeader { .. }) => {
                StableDiagnosticCode::RetryScheduled
            }
            Self::Consensus(MetadataConsensusError::CapabilityUnavailable { .. }) => {
                StableDiagnosticCode::ChirpsUnavailable
            }
            Self::Consensus(MetadataConsensusError::Storage { .. }) => {
                StableDiagnosticCode::OperationPending
            }
            Self::Consensus(MetadataConsensusError::Snapshot { .. }) => {
                StableDiagnosticCode::RequestConflict
            }
        }
    }
}

impl fmt::Display for SchemaControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(error) => write!(formatter, "schema control rejected: {error}"),
            Self::Consensus(error) => write!(formatter, "schema control unavailable: {error}"),
        }
    }
}

impl Error for SchemaControlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rejected(error) => Some(error),
            Self::Consensus(error) => Some(error),
        }
    }
}

/// Applies schema-owner, immutable-manifest, rollout, and member evidence
/// commands through the metadata consensus boundary.
#[derive(Debug)]
pub struct SchemaControlService<S> {
    store: S,
    validator: MetadataCommandValidator,
}

impl<S> SchemaControlService<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            validator: MetadataCommandValidator,
        }
    }

    pub fn into_inner(self) -> S {
        self.store
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }
}

impl<S: MetadataConsensusStore> SchemaControlService<S> {
    /// Reads, validates, and proposes exactly one idempotent management
    /// envelope.  The only authority for the returned rollout is the metadata
    /// version committed by `store`; no local applier state is promoted here.
    pub fn submit(
        &mut self,
        envelope: MetadataCommandEnvelope,
    ) -> Result<SchemaControlResult, SchemaControlError> {
        let request_id = envelope.request_id.clone();
        let current = self
            .store
            .read_current()
            .map_err(SchemaControlError::Consensus)?;
        match self
            .validator
            .validate(&current, envelope)
            .map_err(SchemaControlError::Rejected)?
        {
            ValidationDecision::Idempotent(outcome) => Ok(result_from(&current, outcome)),
            ValidationDecision::Apply(command) => {
                let committed = self
                    .store
                    .submit(*command)
                    .map_err(SchemaControlError::Consensus)?;
                let outcome = committed.operation(&request_id).cloned().ok_or_else(|| {
                    SchemaControlError::Consensus(MetadataConsensusError::Storage {
                        message: "committed schema operation is absent from the idempotency ledger"
                            .to_string(),
                    })
                })?;
                Ok(result_from(&committed, outcome))
            }
        }
    }
}

fn result_from(metadata: &CommittedMetadata, outcome: ManagementOutcome) -> SchemaControlResult {
    SchemaControlResult {
        outcome,
        state_version: metadata.state_version(),
        rollout: metadata.schema().clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AuthorizationScope, ClusterId, MemberIdentity, MemberLifecycle, MemberRecord,
        MetadataActor, MetadataCommand, NodeId, NodeRole, RequestId, SchemaApplyEvidence,
        SchemaApplyState, SchemaCompatibility, SchemaManifest, SchemaManifestId,
    };
    use sha2::Digest;

    struct TestStore {
        current: CommittedMetadata,
    }

    impl TestStore {
        fn with_active_members() -> Self {
            let mut current = CommittedMetadata::new(ClusterId::new("cluster-a"));
            for node_id in ["node-a", "node-b"] {
                current.record_member_for_apply(MemberRecord::new(
                    MemberIdentity {
                        node_id: NodeId::new(node_id),
                        cluster_id: Some(ClusterId::new("cluster-a")),
                        advertised_endpoint: None,
                        role: NodeRole::Worker,
                    },
                    MemberLifecycle::Active,
                ));
            }
            Self { current }
        }
    }

    impl MetadataConsensusStore for TestStore {
        fn read_current(&self) -> Result<CommittedMetadata, MetadataConsensusError> {
            Ok(self.current.clone())
        }

        fn submit(
            &mut self,
            command: crate::ValidatedMetadataCommand,
        ) -> Result<CommittedMetadata, MetadataConsensusError> {
            self.current = self
                .current
                .apply_validated_for_consensus(command.envelope());
            Ok(self.current.clone())
        }

        fn snapshot(&self) -> Result<crate::MetadataSnapshot, MetadataConsensusError> {
            unreachable!("schema-control tests do not snapshot")
        }

        fn restore(
            &mut self,
            _snapshot: crate::MetadataSnapshot,
        ) -> Result<(), MetadataConsensusError> {
            unreachable!("schema-control tests do not restore")
        }
    }

    fn actor(node_id: &str) -> MetadataActor {
        MetadataActor::authorized_for(node_id, AuthorizationScope::Schema)
    }

    fn request(
        id: &str,
        actor: MetadataActor,
        command: MetadataCommand,
    ) -> MetadataCommandEnvelope {
        MetadataCommandEnvelope {
            request_id: RequestId::new(id),
            request_fingerprint: format!("fingerprint-{id}"),
            actor,
            expected_version: None,
            command,
        }
    }

    fn request_at_version(
        id: &str,
        actor: MetadataActor,
        expected_version: u64,
        command: MetadataCommand,
    ) -> MetadataCommandEnvelope {
        let mut request = request(id, actor, command);
        request.expected_version = Some(expected_version);
        request
    }

    fn manifest(id: &str, parent_id: Option<&str>, owner: &str) -> SchemaManifest {
        let catalog_delta = br#"{\"format\":\"catalog-v1\"}"#.to_vec();
        SchemaManifest {
            id: SchemaManifestId::new(id),
            parent_id: parent_id.map(SchemaManifestId::new),
            schema_version: 12,
            catalog_delta_format: "catalog-v1".to_string(),
            checksum: format!("{:x}", sha2::Sha256::digest(&catalog_delta)),
            catalog_delta,
            compatibility: SchemaCompatibility {
                minimum_catalog_version: 10,
                maximum_catalog_version: 12,
            },
            owner: NodeId::new(owner),
            created_at_epoch: 22,
        }
    }

    fn configured_service() -> SchemaControlService<TestStore> {
        let mut service = SchemaControlService::new(TestStore::with_active_members());
        service
            .submit(request(
                "owner-a",
                actor("node-a"),
                MetadataCommand::SetSchemaOwner {
                    owner: NodeId::new("node-a"),
                },
            ))
            .unwrap();
        service
    }

    #[test]
    fn committed_rollout_has_one_owner_and_only_pending_members_before_verified_apply() {
        let mut service = configured_service();
        service
            .submit(request(
                "propose-1",
                actor("node-a"),
                MetadataCommand::ProposeSchemaManifest {
                    manifest: manifest("manifest-1", None, "node-a"),
                },
            ))
            .unwrap();
        let committed = service
            .submit(request(
                "commit-1",
                actor("node-a"),
                MetadataCommand::CommitSchemaManifest {
                    manifest_id: SchemaManifestId::new("manifest-1"),
                },
            ))
            .unwrap();

        assert_eq!(committed.rollout.owner, Some(NodeId::new("node-a")));
        assert_eq!(
            committed.rollout.active_manifest,
            Some(SchemaManifestId::new("manifest-1"))
        );
        assert!(
            committed
                .rollout
                .member_apply
                .values()
                .all(|evidence| evidence.state == SchemaApplyState::Pending)
        );
        assert!(
            committed
                .rollout
                .member_apply
                .values()
                .all(|evidence| evidence.manifest_id == SchemaManifestId::new("manifest-1"))
        );
    }

    #[test]
    fn non_owner_cannot_propose_or_commit_a_manifest() {
        let mut service = configured_service();
        service
            .submit(request(
                "propose-by-a",
                actor("node-a"),
                MetadataCommand::ProposeSchemaManifest {
                    manifest: manifest("manifest-1", None, "node-a"),
                },
            ))
            .unwrap();
        let error = service
            .submit(request(
                "commit-by-b",
                actor("node-b"),
                MetadataCommand::CommitSchemaManifest {
                    manifest_id: SchemaManifestId::new("manifest-1"),
                },
            ))
            .unwrap_err();

        assert_eq!(error.class(), ManagementOutcomeClass::TerminalFailure);
        assert_eq!(error.code(), StableDiagnosticCode::SchemaOwnerRequired);
        assert!(service.store().current.schema().active_manifest.is_none());
    }

    #[test]
    fn stale_owner_change_is_rejected_and_keeps_exactly_one_committed_owner() {
        let mut service = configured_service();
        let first = service
            .submit(request_at_version(
                "owner-b",
                actor("node-a"),
                1,
                MetadataCommand::SetSchemaOwner {
                    owner: NodeId::new("node-b"),
                },
            ))
            .unwrap();
        assert_eq!(first.rollout.owner, Some(NodeId::new("node-b")));

        let conflict = service
            .submit(request_at_version(
                "owner-a-stale",
                actor("node-a"),
                1,
                MetadataCommand::SetSchemaOwner {
                    owner: NodeId::new("node-a"),
                },
            ))
            .unwrap_err();
        assert_eq!(conflict.code(), StableDiagnosticCode::StaleMetadataVersion);
        assert_eq!(
            service.store().current.schema().owner,
            Some(NodeId::new("node-b"))
        );
    }

    #[test]
    fn failed_or_unverified_evidence_never_promotes_a_member_to_applied() {
        let mut service = configured_service();
        let proposed = manifest("manifest-1", None, "node-a");
        service
            .submit(request(
                "propose-1",
                actor("node-a"),
                MetadataCommand::ProposeSchemaManifest { manifest: proposed },
            ))
            .unwrap();
        service
            .submit(request(
                "commit-1",
                actor("node-a"),
                MetadataCommand::CommitSchemaManifest {
                    manifest_id: SchemaManifestId::new("manifest-1"),
                },
            ))
            .unwrap();

        let fake_applied = SchemaApplyEvidence {
            manifest_id: SchemaManifestId::new("manifest-1"),
            member: NodeId::new("node-b"),
            state: SchemaApplyState::Applied,
            catalog_version: Some(12),
            checksum: Some("wrong".to_string()),
            compatibility_verified: true,
            failure_detail: None,
        };
        let error = service
            .submit(request(
                "apply-bad",
                actor("node-b"),
                MetadataCommand::RecordSchemaApply {
                    evidence: fake_applied,
                },
            ))
            .unwrap_err();
        assert_eq!(error.code(), StableDiagnosticCode::RequestConflict);
        assert_eq!(
            service.store().current.schema().member_apply[&NodeId::new("node-b")].state,
            SchemaApplyState::Pending
        );

        let failed = SchemaApplyEvidence {
            manifest_id: SchemaManifestId::new("manifest-1"),
            member: NodeId::new("node-b"),
            state: SchemaApplyState::Failed,
            catalog_version: Some(11),
            checksum: Some("wrong".to_string()),
            compatibility_verified: false,
            failure_detail: Some("catalog checksum mismatch".to_string()),
        };
        let result = service
            .submit(request(
                "apply-failed",
                actor("node-b"),
                MetadataCommand::RecordSchemaApply { evidence: failed },
            ))
            .unwrap();
        assert_eq!(result.outcome.class, ManagementOutcomeClass::Succeeded);
        assert_eq!(
            result.rollout.member_apply[&NodeId::new("node-b")].state,
            SchemaApplyState::Failed
        );

        let checksum = result
            .rollout
            .active_manifest
            .as_ref()
            .and_then(|id| service.store().current.schema_manifests().get(id))
            .expect("committed manifest")
            .checksum
            .clone();
        let applied = SchemaApplyEvidence {
            manifest_id: SchemaManifestId::new("manifest-1"),
            member: NodeId::new("node-a"),
            state: SchemaApplyState::Applied,
            catalog_version: Some(12),
            checksum: Some(checksum),
            compatibility_verified: true,
            failure_detail: None,
        };
        let verified = service
            .submit(request(
                "apply-good",
                actor("node-a"),
                MetadataCommand::RecordSchemaApply { evidence: applied },
            ))
            .unwrap();
        assert_eq!(
            verified.rollout.member_apply[&NodeId::new("node-a")].state,
            SchemaApplyState::Applied
        );
        assert_eq!(
            verified.rollout.member_apply[&NodeId::new("node-b")].state,
            SchemaApplyState::Failed
        );
    }
}
