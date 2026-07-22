use crate::scope_snapshot::{is_sha256, resolve_read_only, sha256_file};
use crate::{io_error, Result, VerificationError};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    CrateArchive,
    CliBinary,
    PythonWheel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateArtifact {
    pub id: String,
    pub crate_name: String,
    pub kind: ArtifactKind,
    pub path: String,
    pub sha256: String,
    pub platform: String,
}

pub fn validate_artifact(
    artifact: &CandidateArtifact,
    artifacts_root: &Path,
    expected_version: &str,
) -> Vec<VerificationError> {
    let mut errors = Vec::new();
    if artifact.id.trim().is_empty()
        || !is_sha256(&artifact.sha256)
        || artifact.platform.trim().is_empty()
    {
        errors.push(VerificationError::new(
            "artifact_identity_invalid",
            artifact.id.clone(),
        ));
        return errors;
    }
    let path = match resolve_read_only(artifacts_root, &artifact.path) {
        Ok(path) => path,
        Err(error) => {
            errors.push(error);
            return errors;
        }
    };
    match sha256_file(&path) {
        Ok(actual) if actual == artifact.sha256 => {}
        Ok(actual) => errors.push(VerificationError::new(
            "artifact_hash_mismatch",
            format!("{} expected {}, got {actual}", artifact.id, artifact.sha256),
        )),
        Err(error) => errors.push(error),
    }
    let file_name = match path.file_name().and_then(|name| name.to_str()) {
        Some(name) => name,
        None => {
            errors.push(VerificationError::new(
                "artifact_filename_invalid",
                artifact.id.clone(),
            ));
            return errors;
        }
    };
    match artifact.kind {
        ArtifactKind::CrateArchive => {
            let expected = format!("{}-{}.crate", artifact.crate_name, expected_version);
            if file_name != expected {
                errors.push(VerificationError::new(
                    "crate_artifact_version_mismatch",
                    format!("{} expected {expected}, got {file_name}", artifact.id),
                ));
            }
        }
        ArtifactKind::CliBinary => {
            if file_name != "alopex" && file_name != "alopex.exe" {
                errors.push(VerificationError::new(
                    "cli_artifact_name_invalid",
                    format!("{}: {file_name}", artifact.id),
                ));
            }
        }
        ArtifactKind::PythonWheel => {
            let expected_prefix = format!("alopex-{}", expected_version.replace('-', "_"));
            if !file_name.ends_with(".whl") || !file_name.starts_with(&expected_prefix) {
                errors.push(VerificationError::new(
                    "wheel_artifact_version_mismatch",
                    format!(
                        "{} expected {expected_prefix}*.whl, got {file_name}",
                        artifact.id
                    ),
                ));
            }
            match wheel_entries(&path) {
                Ok(entries) if entries.iter().any(|entry| is_native_extension(entry)) => {}
                Ok(_) => errors.push(VerificationError::new(
                    "wheel_native_extension_missing",
                    artifact.id.clone(),
                )),
                Err(error) => errors.push(error),
            }
        }
    }
    errors
}

fn is_native_extension(entry: &str) -> bool {
    entry.starts_with("alopex/_alopex")
        && [".so", ".pyd", ".dylib", ".dll"]
            .iter()
            .any(|suffix| entry.ends_with(suffix))
}

/// Lists ZIP central-directory names without unpacking the wheel. Wheels use
/// normal ZIP entries; deliberately reject ZIP64/ambiguous records rather than
/// silently accepting an artifact whose native contents cannot be inspected.
fn wheel_entries(path: &Path) -> Result<Vec<String>> {
    let bytes = std::fs::read(path).map_err(|error| io_error("read wheel", error))?;
    let eocd = bytes
        .windows(4)
        .rposition(|window| window == [0x50, 0x4b, 0x05, 0x06])
        .ok_or_else(|| {
            VerificationError::new("wheel_archive_invalid", path.display().to_string())
        })?;
    if eocd + 22 > bytes.len() {
        return Err(VerificationError::new(
            "wheel_archive_invalid",
            path.display().to_string(),
        ));
    }
    let entries = u16::from_le_bytes([bytes[eocd + 10], bytes[eocd + 11]]) as usize;
    let offset = u32::from_le_bytes([
        bytes[eocd + 16],
        bytes[eocd + 17],
        bytes[eocd + 18],
        bytes[eocd + 19],
    ]) as usize;
    let mut cursor = offset;
    let mut names = Vec::with_capacity(entries);
    for _ in 0..entries {
        if cursor + 46 > bytes.len() || bytes[cursor..cursor + 4] != [0x50, 0x4b, 0x01, 0x02] {
            return Err(VerificationError::new(
                "wheel_archive_invalid",
                path.display().to_string(),
            ));
        }
        let name_len = u16::from_le_bytes([bytes[cursor + 28], bytes[cursor + 29]]) as usize;
        let extra_len = u16::from_le_bytes([bytes[cursor + 30], bytes[cursor + 31]]) as usize;
        let comment_len = u16::from_le_bytes([bytes[cursor + 32], bytes[cursor + 33]]) as usize;
        let name_start = cursor + 46;
        let name_end = name_start + name_len;
        if name_end > bytes.len() {
            return Err(VerificationError::new(
                "wheel_archive_invalid",
                path.display().to_string(),
            ));
        }
        let name = std::str::from_utf8(&bytes[name_start..name_end]).map_err(|_| {
            VerificationError::new("wheel_archive_invalid", path.display().to_string())
        })?;
        names.push(name.replace('\\', "/"));
        cursor = name_end + extra_len + comment_len;
    }
    Ok(names)
}
