//! Startup gating for optional multi-node cluster control.
//!
//! This module decides before endpoint advertisement whether a configured
//! process has every durable and authenticated prerequisite.  It deliberately
//! does not construct an in-memory control plane when a configured cluster is
//! unavailable.

use crate::{ClusterCapabilityPrerequisite, ClusterCapabilityStatus, chirps_cluster_capability};

/// Requested startup mode for the cluster-control boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterBootstrapMode {
    SingleNode,
    ClusterAware,
}

/// Inputs evaluated before a cluster endpoint or management operation is made
/// available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterBootstrapConfig {
    pub mode: ClusterBootstrapMode,
    pub foundation: ClusterCapabilityStatus,
    pub durable_raft_storage: bool,
    pub recoverable_metadata_storage: bool,
}

impl ClusterBootstrapConfig {
    /// Evaluates the Chirps foundation compiled into this binary.
    pub fn compiled_chirps(mode: ClusterBootstrapMode) -> Self {
        Self {
            mode,
            foundation: chirps_cluster_capability(),
            durable_raft_storage: false,
            recoverable_metadata_storage: false,
        }
    }

    /// Constructs a configuration for deterministic adapter tests. Production
    /// callers must use [`Self::compiled_chirps`] and provide durable adapters.
    pub fn with_foundation(
        mode: ClusterBootstrapMode,
        foundation: ClusterCapabilityStatus,
    ) -> Self {
        Self {
            mode,
            foundation,
            durable_raft_storage: false,
            recoverable_metadata_storage: false,
        }
    }
}

/// Classified result of the startup gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterBootstrapOutcome {
    /// No cluster control was requested; retain the existing local behavior.
    SingleNode,
    /// Every prerequisite was present. The next layer may construct control.
    ReadyForClusterControl,
    /// Cluster control must not start or be advertised.
    CapabilityUnavailable {
        missing_prerequisites: Vec<ClusterCapabilityPrerequisite>,
    },
}

impl ClusterBootstrapOutcome {
    pub fn can_advertise_cluster_control(&self) -> bool {
        matches!(self, Self::ReadyForClusterControl)
    }
}

/// Performs the pre-operation cluster capability check.
pub fn bootstrap_cluster_control(config: &ClusterBootstrapConfig) -> ClusterBootstrapOutcome {
    if config.mode == ClusterBootstrapMode::SingleNode {
        return ClusterBootstrapOutcome::SingleNode;
    }

    let mut missing_prerequisites = config.foundation.missing_prerequisites.clone();
    if !config.foundation.available && missing_prerequisites.is_empty() {
        // Prevent an invalid third-party capability implementation from
        // accidentally turning an unknown failure into an advertised endpoint.
        missing_prerequisites.push(ClusterCapabilityPrerequisite::ChirpsFeature);
    }
    if !config.durable_raft_storage {
        push_missing(
            &mut missing_prerequisites,
            ClusterCapabilityPrerequisite::DurableRaftStorage,
        );
    }
    if !config.recoverable_metadata_storage {
        push_missing(
            &mut missing_prerequisites,
            ClusterCapabilityPrerequisite::RecoverableMetadataStorage,
        );
    }

    if missing_prerequisites.is_empty() && config.foundation.available {
        ClusterBootstrapOutcome::ReadyForClusterControl
    } else {
        ClusterBootstrapOutcome::CapabilityUnavailable {
            missing_prerequisites,
        }
    }
}

fn push_missing(
    missing_prerequisites: &mut Vec<ClusterCapabilityPrerequisite>,
    prerequisite: ClusterCapabilityPrerequisite,
) {
    if !missing_prerequisites.contains(&prerequisite) {
        missing_prerequisites.push(prerequisite);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_bootstrap_keeps_local_behavior_without_cluster_prerequisites() {
        let outcome = bootstrap_cluster_control(&ClusterBootstrapConfig::compiled_chirps(
            ClusterBootstrapMode::SingleNode,
        ));

        assert_eq!(outcome, ClusterBootstrapOutcome::SingleNode);
        assert!(!outcome.can_advertise_cluster_control());
    }

    #[test]
    fn cluster_bootstrap_rejects_missing_durable_storage_before_advertisement() {
        let config = ClusterBootstrapConfig::with_foundation(
            ClusterBootstrapMode::ClusterAware,
            ClusterCapabilityStatus::available(),
        );

        let outcome = bootstrap_cluster_control(&config);

        assert_eq!(
            outcome,
            ClusterBootstrapOutcome::CapabilityUnavailable {
                missing_prerequisites: vec![
                    ClusterCapabilityPrerequisite::DurableRaftStorage,
                    ClusterCapabilityPrerequisite::RecoverableMetadataStorage,
                ],
            }
        );
        assert!(!outcome.can_advertise_cluster_control());
    }

    #[test]
    fn cluster_bootstrap_allows_only_complete_test_foundation() {
        let mut config = ClusterBootstrapConfig::with_foundation(
            ClusterBootstrapMode::ClusterAware,
            ClusterCapabilityStatus::available(),
        );
        config.durable_raft_storage = true;
        config.recoverable_metadata_storage = true;

        let outcome = bootstrap_cluster_control(&config);

        assert_eq!(outcome, ClusterBootstrapOutcome::ReadyForClusterControl);
        assert!(outcome.can_advertise_cluster_control());
    }
}
