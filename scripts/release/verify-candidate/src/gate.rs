use crate::evidence::validate_manifest_evidence;
use crate::input_bundle::{validate_snapshot_approval_evidence, InputBundle};
use crate::inventory::{ArtifactInventory, InventoryDeclaration};
use crate::manifest::{CapabilityEvidenceManifest, CapabilityEvidenceRow};
use crate::python_verify::{validate_cli_startup_audits, validate_python_artifact_audits};
use crate::sandbox::SandboxAudit;
use crate::scope_snapshot::ApprovedScopeSnapshot;
use crate::VerificationError;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct GateInputs<'a> {
    pub requirements_root: &'a Path,
    pub source_root: &'a Path,
    pub bundle_root: &'a Path,
    pub artifacts_root: &'a Path,
    pub snapshot: &'a ApprovedScopeSnapshot,
    pub manifest: &'a CapabilityEvidenceManifest,
    pub bundle: &'a InputBundle,
    pub inventory_declaration: &'a InventoryDeclaration,
    pub sandbox_audits: &'a [SandboxAudit],
    pub sandbox_errors: &'a [VerificationError],
    pub requested_publication: Option<&'a PublicationRequest>,
    pub authorization: Option<&'a AuthorizationRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicationRequest {
    pub action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthorizationRecord {
    pub action: String,
    pub evidence_uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessVerdict {
    Ready,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessBlocker {
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessReport {
    pub verdict: ReadinessVerdict,
    pub blockers: Vec<ReadinessBlocker>,
    pub scope_hashes: Vec<ScopeHash>,
    pub inventory: ArtifactInventory,
    pub rows: Vec<CapabilityEvidenceRow>,
    pub post_release_verification: String,
    pub external_publication_performed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScopeHash {
    pub phase: u8,
    pub requirement_path: String,
    pub sha256: String,
}

pub fn evaluate(inputs: GateInputs<'_>) -> ReadinessReport {
    let mut errors = inputs.snapshot.validate(inputs.requirements_root);
    errors.extend(inputs.bundle.validate(inputs.bundle_root));
    errors.extend(validate_snapshot_approval_evidence(
        inputs.snapshot,
        inputs.bundle,
        inputs.bundle_root,
    ));
    let (inventory, inventory_errors) = inputs
        .inventory_declaration
        .verify(inputs.source_root, inputs.artifacts_root);
    errors.extend(inventory_errors);
    errors.extend(inputs.manifest.validate(
        inputs.snapshot,
        inputs.bundle_root,
        inputs.source_root,
    ));
    errors.extend(validate_manifest_evidence(inputs.manifest, &inventory));
    errors.extend(validate_required_documentation(inputs.source_root));
    errors.extend(validate_cli_startup_audits(
        &inventory,
        inputs.sandbox_audits,
    ));
    errors.extend(validate_python_artifact_audits(
        &inventory,
        inputs.sandbox_audits,
    ));
    errors.extend(inputs.sandbox_errors.iter().cloned());
    errors.extend(validate_authorization(
        inputs.requested_publication,
        inputs.authorization,
    ));
    errors.sort_by(|left, right| {
        left.code
            .cmp(right.code)
            .then(left.detail.cmp(&right.detail))
    });
    errors.dedup_by(|left, right| left.code == right.code && left.detail == right.detail);
    let blockers = errors
        .into_iter()
        .map(|error| ReadinessBlocker {
            code: error.code.to_owned(),
            detail: error.detail,
        })
        .collect::<Vec<_>>();
    ReadinessReport {
        verdict: if blockers.is_empty() {
            ReadinessVerdict::Ready
        } else {
            ReadinessVerdict::Blocked
        },
        blockers,
        scope_hashes: inputs
            .snapshot
            .rows
            .iter()
            .map(|row| ScopeHash {
                phase: row.phase,
                requirement_path: row.requirement_path.clone(),
                sha256: row.requirements_sha256.clone(),
            })
            .collect(),
        inventory,
        rows: inputs.manifest.rows.clone(),
        post_release_verification: "not_run".to_owned(),
        // This binary has no publication command path. A Ready verdict is not
        // an authorization and deliberately cannot change this field.
        external_publication_performed: false,
    }
}

fn validate_required_documentation(source_root: &Path) -> Vec<VerificationError> {
    let required = [
        "docs/cluster-operations.md",
        "docs/distributed-read.md",
        "docs/dataframe-streaming.md",
        "crates/alopex-py/README.md",
        "docs/release-v0.8-support.md",
        "docs/upgrade-v0.7.4-to-v0.8.md",
    ];
    required
        .iter()
        .filter(|path| !source_root.join(path).is_file())
        .map(|path| VerificationError::new("required_documentation_missing", *path))
        .collect()
}

fn validate_authorization(
    request: Option<&PublicationRequest>,
    authorization: Option<&AuthorizationRecord>,
) -> Vec<VerificationError> {
    let Some(request) = request else {
        return Vec::new();
    };
    match authorization {
        Some(record)
            if record.action == request.action
                && record
                    .evidence_uri
                    .starts_with("spec-workflow://authorization/") =>
        {
            Vec::new()
        }
        _ => vec![VerificationError::new(
            "publication_authorization_missing",
            format!("{} has no explicit authorization record", request.action),
        )],
    }
}
