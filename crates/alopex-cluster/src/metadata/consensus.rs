//! Consensus-facing metadata storage boundary.
//!
//! Production integration receives a compatible, authenticated Chirps backend
//! only after the bootstrap gate succeeds. The deterministic implementation is
//! test-only and cannot become a configured multi-node fallback.

use super::{CommittedMetadata, ValidatedMetadataCommand};
use crate::{
    ClusterBootstrapOutcome, ClusterCapabilityPrerequisite, ClusterId, NodeId,
    bootstrap_cluster_control,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{error::Error, fmt};

pub const METADATA_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Versioned serialized committed metadata used for durable snapshot/restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    #[serde(default = "default_snapshot_schema_version")]
    pub schema_version: u32,
    pub state_version: u64,
    #[serde(default)]
    pub cluster_id: Option<ClusterId>,
    pub payload: Vec<u8>,
    #[serde(default)]
    pub checksum: String,
}

fn default_snapshot_schema_version() -> u32 {
    METADATA_SNAPSHOT_SCHEMA_VERSION
}

impl MetadataSnapshot {
    pub fn from_metadata(metadata: &CommittedMetadata) -> Result<Self, MetadataConsensusError> {
        let payload =
            serde_json::to_vec(metadata).map_err(|error| MetadataConsensusError::Snapshot {
                message: error.to_string(),
            })?;
        Ok(Self {
            schema_version: METADATA_SNAPSHOT_SCHEMA_VERSION,
            state_version: metadata.state_version(),
            cluster_id: Some(metadata.cluster_id().clone()),
            checksum: format!("{:x}", Sha256::digest(&payload)),
            payload,
        })
    }

    pub fn validate_for(
        &self,
        current: &CommittedMetadata,
    ) -> Result<CommittedMetadata, MetadataConsensusError> {
        if self.schema_version != METADATA_SNAPSHOT_SCHEMA_VERSION {
            return Err(MetadataConsensusError::Snapshot {
                message: format!(
                    "unsupported metadata snapshot schema version {}",
                    self.schema_version
                ),
            });
        }
        if !self.checksum.is_empty() {
            let checksum = format!("{:x}", Sha256::digest(&self.payload));
            if checksum != self.checksum {
                return Err(MetadataConsensusError::Snapshot {
                    message: "metadata snapshot checksum mismatch".to_string(),
                });
            }
        }
        let restored =
            serde_json::from_slice::<CommittedMetadata>(&self.payload).map_err(|error| {
                MetadataConsensusError::Snapshot {
                    message: error.to_string(),
                }
            })?;
        if restored.state_version() != self.state_version {
            return Err(MetadataConsensusError::Snapshot {
                message: "snapshot state_version does not match its committed payload".to_string(),
            });
        }
        if let Some(cluster_id) = &self.cluster_id
            && cluster_id != restored.cluster_id()
            && cluster_id != &ClusterId::new("")
        {
            return Err(MetadataConsensusError::Snapshot {
                message: "snapshot cluster_id does not match its committed payload".to_string(),
            });
        }
        if restored.cluster_id() != current.cluster_id() {
            return Err(MetadataConsensusError::Snapshot {
                message: "snapshot cluster_id does not match the current cluster".to_string(),
            });
        }
        Ok(restored)
    }
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
        let current = self.backend.read_current()?;
        if let Some(existing) = current
            .operation(&command.envelope().request_id)
            .or_else(|| current.membership_operation(&command.envelope().request_id))
        {
            if existing.request_fingerprint == command.envelope().request_fingerprint {
                return Ok(current);
            }
            return Err(MetadataConsensusError::Storage {
                message: "request_id was already committed with a different fingerprint"
                    .to_string(),
            });
        }
        self.backend.propose(command)
    }

    fn snapshot(&self) -> Result<MetadataSnapshot, MetadataConsensusError> {
        let current = self.backend.read_current()?;
        let snapshot = self.backend.snapshot()?;
        snapshot.validate_for(&current).map(|_| snapshot)
    }

    fn restore(&mut self, snapshot: MetadataSnapshot) -> Result<(), MetadataConsensusError> {
        let current = self.backend.read_current()?;
        snapshot.validate_for(&current)?;
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
        if let Some(existing) = self
            .current
            .operation(&command.envelope().request_id)
            .or_else(|| {
                self.current
                    .membership_operation(&command.envelope().request_id)
            })
        {
            if existing.request_fingerprint == command.envelope().request_fingerprint {
                return Ok(self.current.clone());
            }
            return Err(MetadataConsensusError::Storage {
                message: "request_id was already committed with a different fingerprint"
                    .to_string(),
            });
        }
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
        MetadataSnapshot::from_metadata(&self.current)
    }

    fn restore(&mut self, snapshot: MetadataSnapshot) -> Result<(), MetadataConsensusError> {
        self.current = snapshot.validate_for(&self.current)?;
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
    fn duplicate_validated_request_is_not_applied_twice() {
        let mut store = DeterministicMetadataConsensus::new(CommittedMetadata::new("cluster-a"));
        let first = store.submit(validated_read_policy()).unwrap();
        let duplicate = store.submit(validated_read_policy()).unwrap();

        assert_eq!(duplicate, first);
        assert_eq!(store.read_current().unwrap().state_version(), 1);
    }

    #[test]
    fn snapshot_checksum_and_schema_guard_restore() {
        let mut store = DeterministicMetadataConsensus::new(CommittedMetadata::new("cluster-a"));
        store.submit(validated_read_policy()).unwrap();
        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.schema_version, METADATA_SNAPSHOT_SCHEMA_VERSION);
        assert!(!snapshot.checksum.is_empty());
        assert_eq!(snapshot.cluster_id, Some(ClusterId::new("cluster-a")));

        let mut tampered = snapshot.clone();
        tampered.payload[0] ^= 0xff;
        assert!(matches!(
            store.restore(tampered),
            Err(MetadataConsensusError::Snapshot { message })
                if message.contains("checksum mismatch")
        ));

        let mut unknown_schema = snapshot;
        unknown_schema.schema_version = METADATA_SNAPSHOT_SCHEMA_VERSION + 1;
        assert!(matches!(
            store.restore(unknown_schema),
            Err(MetadataConsensusError::Snapshot { message })
                if message.contains("unsupported metadata snapshot schema version")
        ));
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
