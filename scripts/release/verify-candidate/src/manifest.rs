use crate::scope_snapshot::{ApprovedScopeSnapshot, ScopeReference};
use crate::{io_error, Result, VerificationError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityEvidenceManifest {
    pub schema_version: u32,
    pub rows: Vec<CapabilityEvidenceRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityEvidenceRow {
    pub id: String,
    pub phase: u8,
    pub scope: ScopeReference,
    pub public_surface: String,
    pub support: SupportState,
    #[serde(default)]
    pub prerequisite: Option<String>,
    pub normal_outcome: String,
    pub failure_outcome: String,
    pub artifacts: Vec<String>,
    pub evidence: Vec<EvidenceRef>,
    pub documentation: DocumentationRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SupportState {
    Supported,
    LocalOnly,
    Unavailable,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceRef {
    pub path: String,
    pub sha256: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentationRef {
    pub path: String,
    pub anchor: String,
}

impl CapabilityEvidenceManifest {
    pub fn read(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| io_error("open capability manifest", error))?;
        serde_json::from_reader(file).map_err(|error| {
            VerificationError::new("invalid_capability_manifest", error.to_string())
        })
    }

    /// Validates traceability only. Capability names are not inferred from
    /// design/task files: every row must name one immutable requirement anchor.
    pub fn validate(
        &self,
        snapshot: &ApprovedScopeSnapshot,
        evidence_root: &Path,
        documentation_root: &Path,
    ) -> Vec<VerificationError> {
        let mut errors = Vec::new();
        if self.schema_version != 1 {
            errors.push(VerificationError::new(
                "manifest_schema_invalid",
                format!("unsupported schema version {}", self.schema_version),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut referenced_scope = BTreeSet::new();
        for row in &self.rows {
            if row.id.trim().is_empty() || !ids.insert(row.id.clone()) {
                errors.push(VerificationError::new(
                    "manifest_row_id_invalid",
                    row.id.clone(),
                ));
            }
            if !(1..=4).contains(&row.phase) {
                errors.push(VerificationError::new(
                    "manifest_phase_invalid",
                    row.id.clone(),
                ));
            }
            match snapshot.find(&row.scope) {
                Some(scope_row) if scope_row.phase == row.phase => {
                    referenced_scope.insert(row.scope.clone());
                }
                _ => errors.push(VerificationError::new(
                    "scope_evidence_missing",
                    format!(
                        "{} references an unknown, tampered, or unauthorised anchor",
                        row.id
                    ),
                )),
            }
            if row.public_surface.trim().is_empty()
                || row.normal_outcome.trim().is_empty()
                || row.failure_outcome.trim().is_empty()
            {
                errors.push(VerificationError::new(
                    "manifest_contract_missing",
                    row.id.clone(),
                ));
            }
            if matches!(row.support, SupportState::Unavailable)
                && row.prerequisite.as_deref().unwrap_or("").trim().is_empty()
            {
                errors.push(VerificationError::new(
                    "manifest_prerequisite_missing",
                    format!("{} is unavailable without a prerequisite", row.id),
                ));
            }
            if !matches!(row.support, SupportState::Unavailable) && row.prerequisite.is_some() {
                errors.push(VerificationError::new(
                    "manifest_prerequisite_inconsistent",
                    format!("{} is not unavailable", row.id),
                ));
            }
            if row.artifacts.is_empty() || row.evidence.is_empty() {
                errors.push(VerificationError::new(
                    "manifest_evidence_missing",
                    row.id.clone(),
                ));
            }
            let mut artifact_ids = BTreeSet::new();
            for artifact in &row.artifacts {
                if artifact.trim().is_empty() || !artifact_ids.insert(artifact.clone()) {
                    errors.push(VerificationError::new(
                        "manifest_artifact_invalid",
                        format!("{}: {artifact}", row.id),
                    ));
                }
            }
            for evidence in &row.evidence {
                validate_evidence(evidence, evidence_root, &row.id, &mut errors);
            }
            validate_documentation(&row.documentation, documentation_root, &row.id, &mut errors);
        }
        for scope in snapshot.authorised_references() {
            if !referenced_scope.contains(&scope) {
                errors.push(VerificationError::new(
                    "scope_coverage_missing",
                    format!("{}#{}", scope.requirement_path, scope.requirement_anchor),
                ));
            }
        }
        errors
    }
}

fn validate_evidence(
    evidence: &EvidenceRef,
    root: &Path,
    row_id: &str,
    errors: &mut Vec<VerificationError>,
) {
    if evidence.kind.trim().is_empty() || !crate::scope_snapshot::is_sha256(&evidence.sha256) {
        errors.push(VerificationError::new(
            "manifest_evidence_invalid",
            format!("{row_id}: {}", evidence.path),
        ));
        return;
    }
    match crate::scope_snapshot::resolve_read_only(root, &evidence.path)
        .and_then(|path| crate::scope_snapshot::sha256_file(&path))
    {
        Ok(actual) if actual == evidence.sha256 => {}
        Ok(actual) => errors.push(VerificationError::new(
            "manifest_evidence_hash_mismatch",
            format!(
                "{row_id}: {} expected {}, got {actual}",
                evidence.path, evidence.sha256
            ),
        )),
        Err(error) => errors.push(error),
    }
}

fn validate_documentation(
    documentation: &DocumentationRef,
    root: &Path,
    row_id: &str,
    errors: &mut Vec<VerificationError>,
) {
    if documentation.anchor.trim().is_empty() {
        errors.push(VerificationError::new(
            "manifest_documentation_missing",
            row_id.to_owned(),
        ));
        return;
    }
    match crate::scope_snapshot::resolve_read_only(root, &documentation.path).and_then(|path| {
        std::fs::read_to_string(path).map_err(|error| io_error("read documentation", error))
    }) {
        Ok(contents) if contents.contains(&documentation.anchor) => {}
        Ok(_) => errors.push(VerificationError::new(
            "manifest_documentation_anchor_missing",
            format!("{row_id}: {}#{}", documentation.path, documentation.anchor),
        )),
        Err(error) => errors.push(error),
    }
}
