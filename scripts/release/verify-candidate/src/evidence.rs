use crate::inventory::ArtifactInventory;
use crate::manifest::{CapabilityEvidenceManifest, SupportState};
use crate::VerificationError;
use std::collections::BTreeSet;

/// Cross-check capability evidence that is independent of the schema parser.
/// In particular, a phase 1/2 row may be marked supported only when the
/// candidate carries concrete external-foundation integration evidence.
pub fn validate_manifest_evidence(
    manifest: &CapabilityEvidenceManifest,
    inventory: &ArtifactInventory,
) -> Vec<VerificationError> {
    let mut errors = Vec::new();
    let artifact_ids: BTreeSet<_> = inventory
        .artifacts
        .iter()
        .map(|artifact| artifact.id.as_str())
        .collect();
    for row in &manifest.rows {
        for artifact in &row.artifacts {
            if !artifact_ids.contains(artifact.as_str()) {
                errors.push(VerificationError::new(
                    "manifest_artifact_missing",
                    format!("{} references {artifact}", row.id),
                ));
            }
        }
        if row.phase <= 2 && matches!(row.support, SupportState::Supported) {
            let has_external_proof = row
                .evidence
                .iter()
                .any(|evidence| evidence.kind == "external_prerequisite_integration");
            if !has_external_proof {
                errors.push(VerificationError::new(
                    "external_prerequisite_evidence_missing",
                    format!(
                        "{} is marked supported without compatible foundation evidence",
                        row.id
                    ),
                ));
            }
        }
    }
    errors
}
