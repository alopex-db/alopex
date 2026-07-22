//! Input-bound planning for the supported v0.7.4 to v0.8 metadata upgrade.
//!
//! This planner never reads source files or publishes metadata. It records the
//! safety conditions that a durable server-side operation must satisfy before
//! making a new metadata projection authoritative.

use crate::{RequestId, SchemaManifestId};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

/// The only pre-v0.8 source version accepted by this upgrade contract.
pub const SUPPORTED_UPGRADE_SOURCE_VERSION: &str = "0.7.4";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeSourceKind {
    SingleNode,
    ClusterAware,
}

/// Exact source identity required for a resumable upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeInput {
    pub source_version: String,
    pub source_kind: UpgradeSourceKind,
    pub source_hash: String,
    #[serde(default)]
    pub legacy_metadata_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeCheckpoint {
    Planned,
    CompatibilityValidated,
    MetadataPrepared,
    Published,
    RolledBack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeOutcome {
    Pending,
    ResumeRequired,
    Succeeded,
    RollbackAvailable,
    RolledBack,
    IncompatibleInput,
    InputChanged,
}

/// Persistable operation state for recovery, resume, and rollback status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeOperation {
    pub request_id: RequestId,
    pub input: UpgradeInput,
    pub checkpoint: UpgradeCheckpoint,
    pub outcome: UpgradeOutcome,
    #[serde(default)]
    pub prepared_schema_manifest: Option<SchemaManifestId>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradePlanningError {
    UnsupportedSourceVersion(String),
    MissingSourceHash,
    MissingClusterMetadataHash,
    UnexpectedSingleNodeMetadataHash,
    InvalidTransition {
        from: UpgradeCheckpoint,
        requested: UpgradeCheckpoint,
    },
}

impl fmt::Display for UpgradePlanningError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSourceVersion(version) => {
                write!(formatter, "unsupported upgrade source version: {version}")
            }
            Self::MissingSourceHash => write!(formatter, "upgrade source hash is required"),
            Self::MissingClusterMetadataHash => {
                write!(formatter, "cluster-aware upgrade requires a metadata hash")
            }
            Self::UnexpectedSingleNodeMetadataHash => {
                write!(
                    formatter,
                    "single-node upgrade must not claim cluster metadata"
                )
            }
            Self::InvalidTransition { from, requested } => {
                write!(
                    formatter,
                    "invalid upgrade transition from {from:?} to {requested:?}"
                )
            }
        }
    }
}

impl Error for UpgradePlanningError {}

#[derive(Debug, Default)]
pub struct UpgradePlanner;

impl UpgradePlanner {
    pub fn plan(
        &self,
        request_id: impl Into<RequestId>,
        input: UpgradeInput,
    ) -> Result<UpgradeOperation, UpgradePlanningError> {
        validate_input(&input)?;
        Ok(UpgradeOperation {
            request_id: request_id.into(),
            input,
            checkpoint: UpgradeCheckpoint::Planned,
            outcome: UpgradeOutcome::Pending,
            prepared_schema_manifest: None,
            reason: None,
        })
    }

    /// Moves a compatible operation through the only publish-safe sequence.
    /// Callers persist the operation before each corresponding external action.
    pub fn advance(
        &self,
        operation: &mut UpgradeOperation,
        checkpoint: UpgradeCheckpoint,
        prepared_schema_manifest: Option<SchemaManifestId>,
    ) -> Result<(), UpgradePlanningError> {
        let allowed = matches!(
            (operation.checkpoint, checkpoint),
            (
                UpgradeCheckpoint::Planned,
                UpgradeCheckpoint::CompatibilityValidated
            ) | (
                UpgradeCheckpoint::CompatibilityValidated,
                UpgradeCheckpoint::MetadataPrepared
            ) | (
                UpgradeCheckpoint::MetadataPrepared,
                UpgradeCheckpoint::Published
            ) | (
                UpgradeCheckpoint::MetadataPrepared,
                UpgradeCheckpoint::RolledBack
            ) | (UpgradeCheckpoint::Published, UpgradeCheckpoint::RolledBack)
        );
        if !allowed
            || (checkpoint == UpgradeCheckpoint::MetadataPrepared
                && prepared_schema_manifest
                    .as_ref()
                    .is_none_or(|id| id.as_str().trim().is_empty()))
        {
            return Err(UpgradePlanningError::InvalidTransition {
                from: operation.checkpoint,
                requested: checkpoint,
            });
        }
        operation.checkpoint = checkpoint;
        operation.prepared_schema_manifest = prepared_schema_manifest;
        operation.outcome = match checkpoint {
            UpgradeCheckpoint::Published => UpgradeOutcome::Succeeded,
            UpgradeCheckpoint::RolledBack => UpgradeOutcome::RolledBack,
            UpgradeCheckpoint::MetadataPrepared => UpgradeOutcome::RollbackAvailable,
            UpgradeCheckpoint::Planned | UpgradeCheckpoint::CompatibilityValidated => {
                UpgradeOutcome::Pending
            }
        };
        operation.reason = None;
        Ok(())
    }

    /// The source hash and source kind must be byte-for-byte identical before
    /// an interrupted operation can be resumed.
    pub fn resume(&self, operation: &mut UpgradeOperation, input: &UpgradeInput) -> UpgradeOutcome {
        if operation.input != *input {
            operation.outcome = UpgradeOutcome::InputChanged;
            operation.reason =
                Some("upgrade source differs from the durable operation input".to_string());
            return operation.outcome;
        }
        operation.outcome = match operation.checkpoint {
            UpgradeCheckpoint::Published => UpgradeOutcome::Succeeded,
            UpgradeCheckpoint::RolledBack => UpgradeOutcome::RolledBack,
            UpgradeCheckpoint::MetadataPrepared => UpgradeOutcome::RollbackAvailable,
            UpgradeCheckpoint::Planned | UpgradeCheckpoint::CompatibilityValidated => {
                UpgradeOutcome::ResumeRequired
            }
        };
        operation.outcome
    }

    pub fn incompatible(
        &self,
        request_id: impl Into<RequestId>,
        input: UpgradeInput,
        reason: impl Into<String>,
    ) -> UpgradeOperation {
        UpgradeOperation {
            request_id: request_id.into(),
            input,
            checkpoint: UpgradeCheckpoint::Planned,
            outcome: UpgradeOutcome::IncompatibleInput,
            prepared_schema_manifest: None,
            reason: Some(reason.into()),
        }
    }
}

fn validate_input(input: &UpgradeInput) -> Result<(), UpgradePlanningError> {
    if input.source_version != SUPPORTED_UPGRADE_SOURCE_VERSION {
        return Err(UpgradePlanningError::UnsupportedSourceVersion(
            input.source_version.clone(),
        ));
    }
    if input.source_hash.trim().is_empty() {
        return Err(UpgradePlanningError::MissingSourceHash);
    }
    match (&input.source_kind, &input.legacy_metadata_hash) {
        (UpgradeSourceKind::ClusterAware, None) => {
            Err(UpgradePlanningError::MissingClusterMetadataHash)
        }
        (UpgradeSourceKind::ClusterAware, Some(value)) if value.trim().is_empty() => {
            Err(UpgradePlanningError::MissingClusterMetadataHash)
        }
        (UpgradeSourceKind::SingleNode, Some(_)) => {
            Err(UpgradePlanningError::UnexpectedSingleNodeMetadataHash)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single_input() -> UpgradeInput {
        UpgradeInput {
            source_version: SUPPORTED_UPGRADE_SOURCE_VERSION.to_string(),
            source_kind: UpgradeSourceKind::SingleNode,
            source_hash: "source-a".to_string(),
            legacy_metadata_hash: None,
        }
    }

    #[test]
    fn supported_single_node_upgrade_is_input_bound_and_publish_ordered() {
        let planner = UpgradePlanner;
        let mut operation = planner.plan("upgrade-1", single_input()).unwrap();
        planner
            .advance(
                &mut operation,
                UpgradeCheckpoint::CompatibilityValidated,
                None,
            )
            .unwrap();
        planner
            .advance(
                &mut operation,
                UpgradeCheckpoint::MetadataPrepared,
                Some(SchemaManifestId::new("schema-1")),
            )
            .unwrap();
        assert_eq!(operation.outcome, UpgradeOutcome::RollbackAvailable);
        planner
            .advance(&mut operation, UpgradeCheckpoint::Published, None)
            .unwrap();
        assert_eq!(operation.outcome, UpgradeOutcome::Succeeded);
        assert_eq!(
            planner.resume(&mut operation, &single_input()),
            UpgradeOutcome::Succeeded
        );
    }

    #[test]
    fn changed_or_incompatible_input_never_resumes_or_publishes() {
        let planner = UpgradePlanner;
        let mut operation = planner.plan("upgrade-1", single_input()).unwrap();
        let changed = UpgradeInput {
            source_hash: "source-b".to_string(),
            ..single_input()
        };
        assert_eq!(
            planner.resume(&mut operation, &changed),
            UpgradeOutcome::InputChanged
        );
        assert_eq!(operation.checkpoint, UpgradeCheckpoint::Planned);
        assert!(matches!(
            planner.plan(
                "upgrade-2",
                UpgradeInput {
                    source_version: "0.7.3".to_string(),
                    ..single_input()
                }
            ),
            Err(UpgradePlanningError::UnsupportedSourceVersion(_))
        ));
    }

    #[test]
    fn cluster_aware_input_requires_its_own_metadata_identity() {
        let planner = UpgradePlanner;
        let error = planner
            .plan(
                "upgrade-1",
                UpgradeInput {
                    source_kind: UpgradeSourceKind::ClusterAware,
                    ..single_input()
                },
            )
            .unwrap_err();
        assert_eq!(error, UpgradePlanningError::MissingClusterMetadataHash);
    }
}
