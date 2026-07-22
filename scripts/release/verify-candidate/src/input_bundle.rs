use crate::scope_snapshot::{is_sha256, resolve_read_only, sha256_file, ApprovedScopeSnapshot};
use crate::{io_error, Result, VerificationError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Hash-pinned, read-only inputs that make candidate verification independent
/// of registries, package indexes, and ambient build caches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputBundle {
    pub schema_version: u32,
    pub entries: Vec<InputBundleEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputBundleEntry {
    pub id: String,
    pub kind: BundleKind,
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum BundleKind {
    CargoDependencies,
    LocalWheels,
    CandidateArtifacts,
    ApprovalEvidence,
    BaseImage,
}

impl InputBundle {
    pub fn read(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| io_error("open input bundle", error))?;
        serde_json::from_reader(file)
            .map_err(|error| VerificationError::new("invalid_input_bundle", error.to_string()))
    }

    pub fn validate(&self, bundle_root: &Path) -> Vec<VerificationError> {
        let mut errors = Vec::new();
        if self.schema_version != 1 {
            errors.push(VerificationError::new(
                "input_bundle_schema_invalid",
                self.schema_version.to_string(),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut kinds = BTreeSet::new();
        for entry in &self.entries {
            if entry.id.trim().is_empty() || !ids.insert(entry.id.clone()) {
                errors.push(VerificationError::new(
                    "input_bundle_id_invalid",
                    entry.id.clone(),
                ));
            }
            kinds.insert(entry.kind.clone());
            if !is_sha256(&entry.sha256) {
                errors.push(VerificationError::new(
                    "input_bundle_hash_invalid",
                    entry.id.clone(),
                ));
                continue;
            }
            match resolve_read_only(bundle_root, &entry.path).and_then(|path| hash_path(&path)) {
                Ok(actual) if actual == entry.sha256 => {}
                Ok(actual) => errors.push(VerificationError::new(
                    "input_bundle_hash_mismatch",
                    format!("{} expected {}, got {actual}", entry.id, entry.sha256),
                )),
                Err(error) => errors.push(error),
            }
        }
        for required in [
            BundleKind::CargoDependencies,
            BundleKind::LocalWheels,
            BundleKind::CandidateArtifacts,
        ] {
            if !kinds.contains(&required) {
                errors.push(VerificationError::new(
                    "input_bundle_required_entry_missing",
                    format!("{required:?}"),
                ));
            }
        }
        errors
    }

    pub fn entry(&self, kind: BundleKind) -> Option<&InputBundleEntry> {
        self.entries.iter().find(|entry| entry.kind == kind)
    }
}

/// Binds each approval reference to a hash-pinned dashboard-decision export.
/// This intentionally does not infer approval from a local Markdown status or
/// an arbitrary identifier.
pub fn validate_snapshot_approval_evidence(
    snapshot: &ApprovedScopeSnapshot,
    bundle: &InputBundle,
    bundle_root: &Path,
) -> Vec<VerificationError> {
    let Some(entry) = bundle.entry(BundleKind::ApprovalEvidence) else {
        return vec![VerificationError::new(
            "scope_approval_evidence_missing",
            "input bundle has no approval_evidence entry",
        )];
    };
    let entry_root = match resolve_read_only(bundle_root, &entry.path) {
        Ok(path) => path,
        Err(error) => return vec![error],
    };
    let mut errors = Vec::new();
    for row in &snapshot.rows {
        match resolve_read_only(bundle_root, &row.approval.evidence_path) {
            Ok(path) if path.starts_with(&entry_root) => match sha256_file(&path) {
                Ok(actual) if actual == row.approval.evidence_sha256 => {}
                Ok(actual) => errors.push(VerificationError::new(
                    "scope_approval_hash_mismatch",
                    format!(
                        "{} expected {}, got {actual}",
                        row.approval.evidence_path, row.approval.evidence_sha256
                    ),
                )),
                Err(error) => errors.push(error),
            },
            Ok(_) => errors.push(VerificationError::new(
                "scope_approval_evidence_path_invalid",
                row.approval.evidence_path.clone(),
            )),
            Err(error) => errors.push(error),
        }
    }
    errors
}

/// Hashes a file directly, or a directory as a deterministic sequence of its
/// relative names and file hashes. Symlinks are not accepted in candidate
/// bundles because they can bypass the read-only input boundary.
pub fn hash_path(path: &Path) -> Result<String> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| io_error("stat bundle path", error))?;
    if metadata.file_type().is_symlink() {
        return Err(VerificationError::new(
            "input_bundle_symlink_forbidden",
            path.display().to_string(),
        ));
    }
    if metadata.is_file() {
        return sha256_file(path);
    }
    if !metadata.is_dir() {
        return Err(VerificationError::new(
            "input_bundle_path_invalid",
            path.display().to_string(),
        ));
    }
    let mut files = Vec::new();
    collect_files(path, path, &mut files)?;
    files.sort();
    let mut hasher = Sha256::new();
    for relative in files {
        let file = path.join(&relative);
        let file_hash = sha256_file(&file)?;
        hasher.update(relative.to_string_lossy().as_bytes());
        hasher.update([0]);
        hasher.update(file_hash.as_bytes());
        hasher.update([b'\n']);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn collect_files(root: &Path, directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        std::fs::read_dir(directory).map_err(|error| io_error("read bundle directory", error))?
    {
        let entry = entry.map_err(|error| io_error("read bundle entry", error))?;
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|error| io_error("stat bundle entry", error))?;
        if metadata.file_type().is_symlink() {
            return Err(VerificationError::new(
                "input_bundle_symlink_forbidden",
                path.display().to_string(),
            ));
        }
        if metadata.is_dir() {
            collect_files(root, &path, files)?;
        } else if metadata.is_file() {
            files.push(
                path.strip_prefix(root)
                    .map_err(|_| {
                        VerificationError::new(
                            "input_bundle_path_invalid",
                            path.display().to_string(),
                        )
                    })?
                    .to_path_buf(),
            );
        } else {
            return Err(VerificationError::new(
                "input_bundle_path_invalid",
                path.display().to_string(),
            ));
        }
    }
    Ok(())
}
