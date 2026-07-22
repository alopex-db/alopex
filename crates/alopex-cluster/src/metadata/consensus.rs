//! Consensus-facing metadata storage boundary.
//!
//! Production integration receives a compatible, authenticated Chirps backend
//! only after the bootstrap gate succeeds. The deterministic implementation is
//! test-only and cannot become a configured multi-node fallback.

use super::{CommittedMetadata, ValidatedMetadataCommand};
use crate::{
    ClusterBootstrapOutcome, ClusterCapabilityPrerequisite, NodeId, bootstrap_cluster_control,
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

/// Versioned serialized committed metadata used for durable snapshot/restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    pub state_version: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataConsensusError {
    CapabilityUnavailable {
        missing_prerequisites: Vec<ClusterCapabilityPrerequisite>,
    },
    NotLeader {
        leader_hint: Option<NodeId>,
    },
    Storage {
        message: String,
    },
    Snapshot {
        message: String,
    },
}

impl fmt::Display for MetadataConsensusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapabilityUnavailable {
                missing_prerequisites,
            } => write!(
                f,
                "cluster metadata capability unavailable: {missing_prerequisites:?}"
            ),
            Self::NotLeader { leader_hint } => {
                write!(f, "metadata proposal reached a follower: {leader_hint:?}")
            }
            Self::Storage { message } => write!(f, "metadata durable storage error: {message}"),
            Self::Snapshot { message } => write!(f, "metadata snapshot error: {message}"),
        }
    }
}

impl Error for MetadataConsensusError {}

/// Public storage contract that never exposes Raft objects to callers.
pub trait MetadataConsensusStore {
    fn read_current(&self) -> Result<CommittedMetadata, MetadataConsensusError>;

    fn submit(
        &mut self,
        command: ValidatedMetadataCommand,
    ) -> Result<CommittedMetadata, MetadataConsensusError>;

    fn snapshot(&self) -> Result<MetadataSnapshot, MetadataConsensusError>;

    fn restore(&mut self, snapshot: MetadataSnapshot) -> Result<(), MetadataConsensusError>;
}

/// Minimal production backend boundary implemented by the compatible Chirps
/// adapter. It carries only validated metadata and snapshots, not public Raft
/// internals.
pub trait ChirpsMetadataBackend: Send {
    fn read_current(&self) -> Result<CommittedMetadata, MetadataConsensusError>;

    fn propose(
        &mut self,
        command: ValidatedMetadataCommand,
    ) -> Result<CommittedMetadata, MetadataConsensusError>;

    fn snapshot(&self) -> Result<MetadataSnapshot, MetadataConsensusError>;

    fn restore(&mut self, snapshot: MetadataSnapshot) -> Result<(), MetadataConsensusError>;
}

/// Capability-gated adapter around a compatible Chirps Raft/state-machine
/// implementation. Construction fails before endpoint advertisement when the
/// current foundation lacks authenticated dispatch or durable storage.
pub struct ChirpsMetadataConsensusAdapter<B> {
    backend: B,
}

impl<B: ChirpsMetadataBackend> ChirpsMetadataConsensusAdapter<B> {
    pub fn new(
        bootstrap: ClusterBootstrapOutcome,
        backend: B,
    ) -> Result<Self, MetadataConsensusError> {
        if bootstrap.can_advertise_cluster_control() {
            Ok(Self { backend })
        } else {
            let missing_prerequisites = match bootstrap {
                ClusterBootstrapOutcome::CapabilityUnavailable {
                    missing_prerequisites,
                } => missing_prerequisites,
                ClusterBootstrapOutcome::SingleNode => {
                    vec![ClusterCapabilityPrerequisite::ChirpsFeature]
                }
                ClusterBootstrapOutcome::ReadyForClusterControl => unreachable!("checked above"),
            };
            Err(MetadataConsensusError::CapabilityUnavailable {
                missing_prerequisites,
            })
        }
    }
}

impl<B: ChirpsMetadataBackend> MetadataConsensusStore for ChirpsMetadataConsensusAdapter<B> {
    fn read_current(&self) -> Result<CommittedMetadata, MetadataConsensusError> {
        self.backend.read_current()
    }

    fn submit(
        &mut self,
        command: ValidatedMetadataCommand,
    ) -> Result<CommittedMetadata, MetadataConsensusError> {
        self.backend.propose(command)
    }

    fn snapshot(&self) -> Result<MetadataSnapshot, MetadataConsensusError> {
        self.backend.snapshot()
    }

    fn restore(&mut self, snapshot: MetadataSnapshot) -> Result<(), MetadataConsensusError> {
        self.backend.restore(snapshot)
    }
}

/// Exposes the same gate used by production construction without allowing an
/// unconfigured caller to create a control-plane store.
pub fn compiled_chirps_bootstrap() -> ClusterBootstrapOutcome {
    bootstrap_cluster_control(&crate::ClusterBootstrapConfig::compiled_chirps(
        crate::ClusterBootstrapMode::ClusterAware,
    ))
}

#[cfg(test)]
pub(crate) struct DeterministicMetadataConsensus {
    current: CommittedMetadata,
    leader: bool,
    leader_hint: Option<NodeId>,
}

#[cfg(test)]
impl DeterministicMetadataConsensus {
    pub(crate) fn new(current: CommittedMetadata) -> Self {
        Self {
            current,
            leader: true,
            leader_hint: None,
        }
    }

    pub(crate) fn set_follower(&mut self, leader_hint: impl Into<NodeId>) {
        self.leader = false;
        self.leader_hint = Some(leader_hint.into());
    }
}

#[cfg(test)]
impl MetadataConsensusStore for DeterministicMetadataConsensus {
    fn read_current(&self) -> Result<CommittedMetadata, MetadataConsensusError> {
        Ok(self.current.clone())
    }

    fn submit(
        &mut self,
        command: ValidatedMetadataCommand,
    ) -> Result<CommittedMetadata, MetadataConsensusError> {
        if !self.leader {
            return Err(MetadataConsensusError::NotLeader {
                leader_hint: self.leader_hint.clone(),
            });
        }
        self.current = self
            .current
            .apply_validated_for_consensus(command.envelope());
        Ok(self.current.clone())
    }

    fn snapshot(&self) -> Result<MetadataSnapshot, MetadataConsensusError> {
        let payload = serde_json::to_vec(&self.current).map_err(|error| {
            MetadataConsensusError::Snapshot {
                message: error.to_string(),
            }
        })?;
        Ok(MetadataSnapshot {
            state_version: self.current.state_version(),
            payload,
        })
    }

    fn restore(&mut self, snapshot: MetadataSnapshot) -> Result<(), MetadataConsensusError> {
        let restored =
            serde_json::from_slice::<CommittedMetadata>(&snapshot.payload).map_err(|error| {
                MetadataConsensusError::Snapshot {
                    message: error.to_string(),
                }
            })?;
        if restored.state_version() != snapshot.state_version {
            return Err(MetadataConsensusError::Snapshot {
                message: "snapshot state_version does not match its committed payload".to_string(),
            });
        }
        self.current = restored;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AuthorizationScope, ClusterId, ClusterReadPolicy, MetadataActor, MetadataCommand,
        MetadataCommandEnvelope, MetadataCommandValidator, RequestId,
    };

    fn validated_read_policy() -> ValidatedMetadataCommand {
        let metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        MetadataCommandValidator
            .validate(
                &metadata,
                MetadataCommandEnvelope {
                    request_id: RequestId::new("request-1"),
                    request_fingerprint: "digest-1".to_string(),
                    actor: MetadataActor::authorized_for("node-a", AuthorizationScope::ReadPolicy),
                    expected_version: Some(0),
                    command: MetadataCommand::SetReadPolicy {
                        policy: ClusterReadPolicy::default(),
                    },
                },
            )
            .expect("valid metadata command")
            .expect_apply()
    }

    trait ExpectApply {
        fn expect_apply(self) -> ValidatedMetadataCommand;
    }

    impl ExpectApply for crate::ValidationDecision {
        fn expect_apply(self) -> ValidatedMetadataCommand {
            match self {
                Self::Apply(command) => *command,
                Self::Idempotent(_) => panic!("new request should be proposed"),
            }
        }
    }

    struct NeverStartedBackend;

    impl ChirpsMetadataBackend for NeverStartedBackend {
        fn read_current(&self) -> Result<CommittedMetadata, MetadataConsensusError> {
            Err(MetadataConsensusError::Storage {
                message: "backend must not start before capability bootstrap".to_string(),
            })
        }

        fn propose(
            &mut self,
            _command: ValidatedMetadataCommand,
        ) -> Result<CommittedMetadata, MetadataConsensusError> {
            self.read_current()
        }

        fn snapshot(&self) -> Result<MetadataSnapshot, MetadataConsensusError> {
            Err(MetadataConsensusError::Storage {
                message: "backend must not start before capability bootstrap".to_string(),
            })
        }

        fn restore(&mut self, _snapshot: MetadataSnapshot) -> Result<(), MetadataConsensusError> {
            Err(MetadataConsensusError::Storage {
                message: "backend must not start before capability bootstrap".to_string(),
            })
        }
    }

    #[test]
    fn apply_snapshot_and_restore_publish_only_committed_state() {
        let initial = CommittedMetadata::new(ClusterId::new("cluster-a"));
        let mut store = DeterministicMetadataConsensus::new(initial);
        let committed = store.submit(validated_read_policy()).unwrap();
        assert_eq!(committed.state_version(), 1);
        assert_eq!(
            committed
                .operation(&RequestId::new("request-1"))
                .unwrap()
                .committed_version,
            Some(1)
        );

        let snapshot = store.snapshot().unwrap();
        let mut restarted =
            DeterministicMetadataConsensus::new(CommittedMetadata::new("cluster-a"));
        restarted.restore(snapshot).unwrap();
        assert_eq!(restarted.read_current().unwrap(), committed);
    }

    #[test]
    fn follower_never_reports_a_proposal_as_committed() {
        let mut store = DeterministicMetadataConsensus::new(CommittedMetadata::new("cluster-a"));
        store.set_follower("node-leader");

        assert_eq!(
            store.submit(validated_read_policy()).unwrap_err(),
            MetadataConsensusError::NotLeader {
                leader_hint: Some(NodeId::new("node-leader"))
            }
        );
        assert_eq!(store.read_current().unwrap().state_version(), 0);
    }

    #[test]
    fn incompatible_compiled_foundation_cannot_construct_a_consensus_adapter() {
        let error = match ChirpsMetadataConsensusAdapter::new(
            compiled_chirps_bootstrap(),
            NeverStartedBackend,
        ) {
            Ok(_) => panic!("incompatible foundation must not start a consensus adapter"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            MetadataConsensusError::CapabilityUnavailable { .. }
        ));
    }
}
