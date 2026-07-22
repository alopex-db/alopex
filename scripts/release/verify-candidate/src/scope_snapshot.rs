use crate::{io_error, Result, VerificationError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

/// The immutable scope record included with a release candidate.  Approval is
/// an externally produced evidence reference; a local status line or an
/// arbitrary approval identifier is never accepted as a substitute.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovedScopeSnapshot {
    pub schema_version: u32,
    pub candidate_commit: String,
    pub rows: Vec<ApprovedScopeRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovedScopeRow {
    pub phase: u8,
    pub requirement_path: String,
    pub requirements_sha256: String,
    pub approved_revision: String,
    pub approval: ApprovalEvidence,
    /// Canonical requirement headings authorised for capability rows.
    pub anchors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovalEvidence {
    /// Currently `spec-workflow-dashboard`; keeping this explicit prevents a
    /// local draft or task status from being misrepresented as approval.
    pub authority: String,
    pub decision_uri: String,
    /// Relative path inside the hash-pinned input bundle to the immutable
    /// dashboard decision export.
    pub evidence_path: String,
    /// SHA-256 of the immutable dashboard decision export held in the
    /// candidate evidence bundle. The verifier does not contact the dashboard.
    pub evidence_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovedScopeInput {
    pub phase: u8,
    pub requirement_path: String,
    pub approved_revision: String,
    pub approval: ApprovalEvidence,
    pub anchors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScopeReference {
    pub requirement_path: String,
    pub requirement_anchor: String,
    pub requirements_sha256: String,
}

impl ApprovedScopeSnapshot {
    /// Creates a fresh snapshot from exact requirement files. Replacing a
    /// snapshot is forbidden by [`Self::write_append_only`].
    pub fn create(
        candidate_commit: String,
        requirements_root: &Path,
        inputs: Vec<ApprovedScopeInput>,
    ) -> Result<Self> {
        let mut rows = Vec::with_capacity(inputs.len());
        for input in inputs {
            let path = resolve_read_only(requirements_root, &input.requirement_path)?;
            rows.push(ApprovedScopeRow {
                phase: input.phase,
                requirement_path: input.requirement_path,
                requirements_sha256: sha256_file(&path)?,
                approved_revision: input.approved_revision,
                approval: input.approval,
                anchors: input.anchors,
            });
        }
        let snapshot = Self {
            schema_version: 1,
            candidate_commit,
            rows,
        };
        let errors = snapshot.validate(requirements_root);
        if errors.is_empty() {
            Ok(snapshot)
        } else {
            Err(VerificationError::new(
                "scope_snapshot_invalid",
                errors
                    .into_iter()
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>()
                    .join("; "),
            ))
        }
    }

    pub fn write_append_only(&self, path: &Path) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| io_error("create append-only scope snapshot", error))?;
        serde_json::to_writer_pretty(file, self).map_err(|error| {
            VerificationError::new("scope_snapshot_write_failed", error.to_string())
        })
    }

    pub fn read(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| io_error("open scope snapshot", error))?;
        serde_json::from_reader(file)
            .map_err(|error| VerificationError::new("invalid_scope_snapshot", error.to_string()))
    }

    /// Checks the immutable record against the supplied read-only requirements
    /// root. This intentionally does not infer approval from Markdown status.
    pub fn validate(&self, requirements_root: &Path) -> Vec<VerificationError> {
        let mut errors = Vec::new();
        if self.schema_version != 1 {
            errors.push(VerificationError::new(
                "scope_snapshot_schema",
                format!("unsupported schema version {}", self.schema_version),
            ));
        }
        if self.candidate_commit.trim().is_empty() {
            errors.push(VerificationError::new(
                "scope_candidate_commit_missing",
                "candidate_commit is required",
            ));
        }

        let mut seen_paths = BTreeSet::new();
        let mut seen_phases = BTreeSet::new();
        for row in &self.rows {
            if !(1..=4).contains(&row.phase) {
                errors.push(VerificationError::new(
                    "scope_phase_invalid",
                    format!("{} has phase {}", row.requirement_path, row.phase),
                ));
            }
            if !seen_paths.insert(row.requirement_path.clone()) {
                errors.push(VerificationError::new(
                    "scope_path_duplicate",
                    row.requirement_path.clone(),
                ));
            }
            seen_phases.insert(row.phase);
            validate_approval(&row.approval, &row.requirement_path, &mut errors);
            if row.approved_revision.trim().is_empty() {
                errors.push(VerificationError::new(
                    "scope_revision_missing",
                    row.requirement_path.clone(),
                ));
            }
            if !is_sha256(&row.requirements_sha256) {
                errors.push(VerificationError::new(
                    "scope_hash_invalid",
                    row.requirement_path.clone(),
                ));
            }

            let expected_fragment = format!("phase-{}-", row.phase);
            if !row.requirement_path.contains(&expected_fragment)
                || !row.requirement_path.ends_with("/requirements.md")
            {
                errors.push(VerificationError::new(
                    "scope_path_invalid",
                    format!(
                        "{} is not the canonical Phase {} requirements path",
                        row.requirement_path, row.phase
                    ),
                ));
                continue;
            }
            let path = match resolve_read_only(requirements_root, &row.requirement_path) {
                Ok(path) => path,
                Err(error) => {
                    errors.push(error);
                    continue;
                }
            };
            match sha256_file(&path) {
                Ok(actual) if actual == row.requirements_sha256 => {}
                Ok(actual) => errors.push(VerificationError::new(
                    "scope_hash_mismatch",
                    format!(
                        "{} expected {}, got {actual}",
                        row.requirement_path, row.requirements_sha256
                    ),
                )),
                Err(error) => errors.push(error),
            }
            match std::fs::read_to_string(&path) {
                Ok(contents) => {
                    let mut anchors = BTreeSet::new();
                    for anchor in &row.anchors {
                        if anchor.trim().is_empty() || !anchors.insert(anchor.clone()) {
                            errors.push(VerificationError::new(
                                "scope_anchor_invalid",
                                format!("{}: {anchor}", row.requirement_path),
                            ));
                        } else if !contents.contains(&format!("### {anchor}")) {
                            errors.push(VerificationError::new(
                                "scope_anchor_missing",
                                format!("{}: {anchor}", row.requirement_path),
                            ));
                        }
                    }
                    if row.anchors.is_empty() {
                        errors.push(VerificationError::new(
                            "scope_anchor_missing",
                            format!("{} has no approved anchors", row.requirement_path),
                        ));
                    }
                }
                Err(error) => errors.push(io_error("read requirements", error)),
            }
        }
        for phase in 1..=4 {
            if !seen_phases.contains(&phase) {
                errors.push(VerificationError::new(
                    "scope_phase_missing",
                    format!("Phase {phase} has no approved requirements row"),
                ));
            }
        }
        errors
    }

    pub fn find(&self, reference: &ScopeReference) -> Option<&ApprovedScopeRow> {
        self.rows.iter().find(|row| {
            row.requirement_path == reference.requirement_path
                && row.requirements_sha256 == reference.requirements_sha256
                && row
                    .anchors
                    .iter()
                    .any(|anchor| anchor == &reference.requirement_anchor)
        })
    }

    pub fn authorised_references(&self) -> BTreeSet<ScopeReference> {
        self.rows
            .iter()
            .flat_map(|row| {
                row.anchors.iter().map(|anchor| ScopeReference {
                    requirement_path: row.requirement_path.clone(),
                    requirement_anchor: anchor.clone(),
                    requirements_sha256: row.requirements_sha256.clone(),
                })
            })
            .collect()
    }
}

fn validate_approval(approval: &ApprovalEvidence, path: &str, errors: &mut Vec<VerificationError>) {
    if approval.authority != "spec-workflow-dashboard" {
        errors.push(VerificationError::new(
            "scope_approval_authority_invalid",
            format!("{path}: expected spec-workflow-dashboard"),
        ));
    }
    if !approval
        .decision_uri
        .starts_with("spec-workflow://approval/")
    {
        errors.push(VerificationError::new(
            "scope_approval_evidence_missing",
            format!("{path}: dashboard decision URI is required"),
        ));
    }
    if approval.evidence_path.trim().is_empty()
        || Path::new(&approval.evidence_path).is_absolute()
        || Path::new(&approval.evidence_path)
            .components()
            .any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
    {
        errors.push(VerificationError::new(
            "scope_approval_evidence_path_invalid",
            path.to_owned(),
        ));
    }
    if !is_sha256(&approval.evidence_sha256) {
        errors.push(VerificationError::new(
            "scope_approval_hash_invalid",
            path.to_owned(),
        ));
    }
}

pub fn resolve_read_only(root: &Path, relative: &str) -> Result<PathBuf> {
    let relative_path = Path::new(relative);
    if relative_path.is_absolute()
        || relative_path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(VerificationError::new(
            "unsafe_input_path",
            format!("{relative} escapes the supplied root"),
        ));
    }
    let root =
        std::fs::canonicalize(root).map_err(|error| io_error("canonicalize input root", error))?;
    let candidate = std::fs::canonicalize(root.join(relative_path))
        .map_err(|error| io_error("canonicalize input path", error))?;
    if !candidate.starts_with(&root) {
        return Err(VerificationError::new(
            "unsafe_input_path",
            format!("{relative} resolves outside the supplied root"),
        ));
    }
    Ok(candidate)
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).map_err(|error| io_error("open hashed file", error))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| io_error("read hashed file", error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

pub fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
