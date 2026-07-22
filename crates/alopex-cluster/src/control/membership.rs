//! Durable membership-operation state machine gated by Raft and range readiness.

use crate::{MemberLifecycle, NodeId, RangeReplicaDirectory, RequestId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipOperationKind {
    Join,
    Leave,
    Replace,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollmentCredential {
    pub cluster_id: crate::ClusterId,
    pub requested_node_id: NodeId,
    pub allowed_role: crate::NodeRole,
    pub expires_at_epoch: u64,
    pub nonce: String,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipOperation {
    pub request_id: RequestId,
    pub kind: MembershipOperationKind,
    pub member: NodeId,
    pub lifecycle: MemberLifecycle,
}

pub trait MembershipOperationStore {
    fn save(&mut self, operation: MembershipOperation);
}

pub trait RaftMembershipView {
    fn is_learner(&self, node_id: &NodeId) -> bool;
    fn is_voter(&self, node_id: &NodeId) -> bool;
}

#[derive(Debug, Default)]
pub struct MembershipSaga;

impl MembershipSaga {
    pub fn advance(
        &self,
        operation: &mut MembershipOperation,
        raft: &dyn RaftMembershipView,
        directory: &RangeReplicaDirectory,
        store: &mut dyn MembershipOperationStore,
    ) {
        operation.lifecycle = match operation.kind {
            MembershipOperationKind::Join | MembershipOperationKind::Replace => {
                match operation.lifecycle {
                    MemberLifecycle::Admitted if raft.is_learner(&operation.member) => {
                        MemberLifecycle::LearnerAdded
                    }
                    MemberLifecycle::LearnerAdded
                        if member_has_ready_replica(&operation.member, directory) =>
                    {
                        MemberLifecycle::CaughtUp
                    }
                    MemberLifecycle::CaughtUp if raft.is_voter(&operation.member) => {
                        MemberLifecycle::JointConsensusCommitted
                    }
                    MemberLifecycle::JointConsensusCommitted => MemberLifecycle::MetadataPublished,
                    MemberLifecycle::MetadataPublished => MemberLifecycle::Active,
                    lifecycle => lifecycle,
                }
            }
            MembershipOperationKind::Leave => match operation.lifecycle {
                MemberLifecycle::Draining if !raft.is_voter(&operation.member) => {
                    MemberLifecycle::VoterRemoved
                }
                MemberLifecycle::VoterRemoved
                    if member_can_be_replaced(&operation.member, directory) =>
                {
                    MemberLifecycle::PlacementSafe
                }
                MemberLifecycle::PlacementSafe => MemberLifecycle::MetadataPublished,
                MemberLifecycle::MetadataPublished => MemberLifecycle::Retired,
                lifecycle => lifecycle,
            },
        };
        store.save(operation.clone());
    }

    pub fn recover(
        &self,
        operation: &mut MembershipOperation,
        raft: &dyn RaftMembershipView,
        directory: &RangeReplicaDirectory,
        store: &mut dyn MembershipOperationStore,
    ) {
        let consistent = match operation.lifecycle {
            MemberLifecycle::LearnerAdded | MemberLifecycle::CaughtUp => {
                raft.is_learner(&operation.member) || raft.is_voter(&operation.member)
            }
            MemberLifecycle::JointConsensusCommitted
            | MemberLifecycle::MetadataPublished
            | MemberLifecycle::Active => raft.is_voter(&operation.member),
            MemberLifecycle::VoterRemoved
            | MemberLifecycle::PlacementSafe
            | MemberLifecycle::Retired => !raft.is_voter(&operation.member),
            _ => true,
        };
        if !consistent
            || (operation.lifecycle == MemberLifecycle::Active
                && !member_has_ready_replica(&operation.member, directory))
        {
            operation.lifecycle = MemberLifecycle::RecoveryRequired;
            store.save(operation.clone());
            return;
        }
        self.advance(operation, raft, directory, store);
    }
}

fn member_has_ready_replica(member: &NodeId, directory: &RangeReplicaDirectory) -> bool {
    directory.entries().iter().any(|entry| {
        entry.node_id == *member && entry.state == crate::RangeReplicaReadinessState::Ready
    })
}

fn member_can_be_replaced(member: &NodeId, directory: &RangeReplicaDirectory) -> bool {
    directory
        .entries()
        .iter()
        .filter(|entry| entry.node_id == *member)
        .all(|entry| directory.can_retire_replica(&entry.range_id, member))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClusterId, CommittedMetadata, RangeCoverageProof, RangeId, RangeReplicaEvidence,
        RangeReplicaLifecycle, RangeRoutingDefinition, TableRef,
    };
    use alopex_core::CanonicalRowKey;

    #[derive(Default)]
    struct TestStore(Vec<MembershipOperation>);

    impl MembershipOperationStore for TestStore {
        fn save(&mut self, operation: MembershipOperation) {
            self.0.push(operation);
        }
    }

    struct TestRaft {
        learner: bool,
        voter: bool,
    }

    impl RaftMembershipView for TestRaft {
        fn is_learner(&self, _node_id: &NodeId) -> bool {
            self.learner
        }
        fn is_voter(&self, _node_id: &NodeId) -> bool {
            self.voter
        }
    }

    fn directory_with_ready(node: &str) -> RangeReplicaDirectory {
        let mut metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        let range = RangeRoutingDefinition {
            range_id: RangeId::new("range-a"),
            table_ref: TableRef::new("default.public.users"),
            table_id: 1,
            lower_inclusive: Some(CanonicalRowKey::new(1, 0).encode()),
            upper_exclusive: Some(CanonicalRowKey::new(1, 10).encode()),
            generation: 1,
        };
        metadata.record_range_for_apply(range.clone());
        metadata.record_replica_for_apply(RangeReplicaEvidence {
            range_id: range.range_id.clone(),
            node_id: NodeId::new(node),
            generation: 1,
            schema_manifest_id: None,
            data_epoch: 1,
            index_epoch: 1,
            lifecycle: RangeReplicaLifecycle::Ready,
            coverage: Some(RangeCoverageProof {
                generation: 1,
                lower_inclusive: range.lower_inclusive,
                upper_exclusive: range.upper_exclusive,
                data_epoch: 1,
                index_epoch: 1,
                content_hash: "ok".into(),
            }),
        });
        RangeReplicaDirectory::from_committed(&metadata)
    }

    #[test]
    fn join_never_becomes_active_without_raft_and_ready_replica_evidence() {
        let saga = MembershipSaga;
        let mut store = TestStore::default();
        let mut operation = MembershipOperation {
            request_id: RequestId::new("join-1"),
            kind: MembershipOperationKind::Join,
            member: NodeId::new("node-a"),
            lifecycle: MemberLifecycle::Admitted,
        };
        let raft = TestRaft {
            learner: true,
            voter: false,
        };
        let no_replica = RangeReplicaDirectory::from_committed(&CommittedMetadata::new(
            ClusterId::new("cluster-a"),
        ));

        saga.advance(&mut operation, &raft, &no_replica, &mut store);
        assert_eq!(operation.lifecycle, MemberLifecycle::LearnerAdded);
        saga.advance(&mut operation, &raft, &no_replica, &mut store);
        assert_eq!(operation.lifecycle, MemberLifecycle::LearnerAdded);

        let ready = directory_with_ready("node-a");
        saga.advance(&mut operation, &raft, &ready, &mut store);
        assert_eq!(operation.lifecycle, MemberLifecycle::CaughtUp);
        saga.advance(
            &mut operation,
            &TestRaft {
                learner: true,
                voter: true,
            },
            &ready,
            &mut store,
        );
        assert_eq!(
            operation.lifecycle,
            MemberLifecycle::JointConsensusCommitted
        );
    }

    #[test]
    fn recovery_marks_active_member_inconsistent_when_raft_or_coverage_disagrees() {
        let saga = MembershipSaga;
        let mut store = TestStore::default();
        let mut operation = MembershipOperation {
            request_id: RequestId::new("join-1"),
            kind: MembershipOperationKind::Join,
            member: NodeId::new("node-a"),
            lifecycle: MemberLifecycle::Active,
        };
        let no_replica = RangeReplicaDirectory::from_committed(&CommittedMetadata::new(
            ClusterId::new("cluster-a"),
        ));

        saga.recover(
            &mut operation,
            &TestRaft {
                learner: false,
                voter: true,
            },
            &no_replica,
            &mut store,
        );

        assert_eq!(operation.lifecycle, MemberLifecycle::RecoveryRequired);
        assert_eq!(
            store.0.last().unwrap().lifecycle,
            MemberLifecycle::RecoveryRequired
        );
    }
}
