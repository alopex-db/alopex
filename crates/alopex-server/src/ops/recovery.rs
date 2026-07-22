use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use alopex_cluster::{
    SchemaManifestId, UpgradeCheckpoint, UpgradeInput, UpgradeOperation, UpgradePlanner,
    UpgradeSourceKind, SUPPORTED_UPGRADE_SOURCE_VERSION,
};
use alopex_core::kv::any::AnyKV;
use alopex_core::lsm::{LsmKV, LsmKVConfig, RecoveryResult};
use serde::{Deserialize, Serialize};

use crate::error::{Result, ServerError};
use crate::ops::restore::restore_source_fingerprint;
use crate::ops::state::{LifecycleStateManager, Mode};

const UPGRADE_OPERATION_FILE: &str = "upgrade-operation.json";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOutcome {
    Success,
    ReadOnly,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoveryInfo {
    pub outcome: RecoveryOutcome,
    pub duration_ms: u64,
    pub entries_replayed: u64,
    pub warnings: u64,
    pub finished_at_ms: u64,
    pub reason: Option<String>,
}

impl RecoveryInfo {
    fn from_result(result: &RecoveryResult, elapsed: std::time::Duration) -> Self {
        let outcome = if result.stop_reason.is_some() {
            RecoveryOutcome::ReadOnly
        } else {
            RecoveryOutcome::Success
        };
        Self {
            outcome,
            duration_ms: elapsed.as_millis() as u64,
            entries_replayed: result.entries_recovered as u64,
            warnings: result.warnings.len() as u64,
            finished_at_ms: now_ms(),
            reason: result.stop_reason.clone(),
        }
    }
}

pub struct RecoveryCoordinator;

impl RecoveryCoordinator {
    pub fn open_store(data_dir: &Path) -> Result<(AnyKV, RecoveryInfo)> {
        let start = Instant::now();
        match LsmKV::open_with_config(data_dir, LsmKVConfig::default()) {
            Ok((store, recovery)) => {
                let info = RecoveryInfo::from_result(&recovery, start.elapsed());
                Ok((AnyKV::Lsm(Box::new(store)), info))
            }
            Err(err) => {
                let mut reason = format!("initial recovery failed: {err}");
                match quarantine_wal(data_dir) {
                    Ok(Some(path)) => {
                        reason = format!("{reason}; quarantined WAL to {}", path.display());
                    }
                    Ok(None) => {
                        reason = format!("{reason}; WAL not found for quarantine");
                    }
                    Err(rename_err) => {
                        reason = format!("{reason}; failed to quarantine WAL: {rename_err}");
                    }
                }

                let (store, recovery) = LsmKV::open_with_config(data_dir, LsmKVConfig::default())?;
                let mut info = RecoveryInfo::from_result(&recovery, start.elapsed());
                info.outcome = RecoveryOutcome::ReadOnly;
                info.reason = Some(reason);
                Ok((AnyKV::Lsm(Box::new(store)), info))
            }
        }
    }

    pub fn apply_initial_mode(state: &LifecycleStateManager, info: &RecoveryInfo) {
        if matches!(
            info.outcome,
            RecoveryOutcome::ReadOnly | RecoveryOutcome::Failed
        ) {
            state.set_mode(Mode::ReadOnly);
        }
    }
}

/// Durable coordinator for the v0.7.4 upgrade planner.  The journal is kept
/// below the server lifecycle directory and written through a temporary file,
/// so interrupted processes leave either the old complete operation or the
/// new complete operation, never a published partial record.
#[derive(Debug, Clone)]
pub struct UpgradeCoordinator {
    journal_path: PathBuf,
}

impl UpgradeCoordinator {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            journal_path: data_dir
                .as_ref()
                .join(".lifecycle")
                .join(UPGRADE_OPERATION_FILE),
        }
    }

    pub fn start(
        &self,
        request_id: impl Into<alopex_cluster::RequestId>,
        source: &Path,
        source_kind: UpgradeSourceKind,
        legacy_metadata_hash: Option<String>,
    ) -> Result<UpgradeOperation> {
        let request_id = request_id.into();
        let input = UpgradeInput {
            source_version: SUPPORTED_UPGRADE_SOURCE_VERSION.to_string(),
            source_kind,
            source_hash: restore_source_fingerprint(source)?,
            legacy_metadata_hash,
        };
        let planner = UpgradePlanner;
        let operation = match planner.plan(request_id.clone(), input.clone()) {
            Ok(operation) => operation,
            Err(error) => planner.incompatible(request_id, input, error.to_string()),
        };
        self.save(&operation)?;
        Ok(operation)
    }

    pub fn status(&self) -> Result<UpgradeOperation> {
        self.load()?
            .ok_or_else(|| ServerError::NotFound("upgrade operation not found".to_string()))
    }

    pub fn resume(
        &self,
        source: &Path,
        source_kind: UpgradeSourceKind,
        legacy_metadata_hash: Option<String>,
    ) -> Result<UpgradeOperation> {
        let mut operation = self.status()?;
        let input = UpgradeInput {
            source_version: SUPPORTED_UPGRADE_SOURCE_VERSION.to_string(),
            source_kind,
            source_hash: restore_source_fingerprint(source)?,
            legacy_metadata_hash,
        };
        UpgradePlanner.resume(&mut operation, &input);
        self.save(&operation)?;
        Ok(operation)
    }

    pub fn checkpoint(
        &self,
        checkpoint: UpgradeCheckpoint,
        prepared_schema_manifest: Option<SchemaManifestId>,
    ) -> Result<UpgradeOperation> {
        let mut operation = self.status()?;
        UpgradePlanner
            .advance(&mut operation, checkpoint, prepared_schema_manifest)
            .map_err(|error| ServerError::Conflict(error.to_string()))?;
        self.save(&operation)?;
        Ok(operation)
    }

    fn load(&self) -> Result<Option<UpgradeOperation>> {
        if !self.journal_path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&self.journal_path)?;
        serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            ServerError::Internal(format!("invalid upgrade operation journal: {error}"))
        })
    }

    fn save(&self, operation: &UpgradeOperation) -> Result<()> {
        let parent = self.journal_path.parent().ok_or_else(|| {
            ServerError::Internal("upgrade journal has no parent directory".to_string())
        })?;
        fs::create_dir_all(parent)?;
        let bytes = serde_json::to_vec(operation).map_err(|error| {
            ServerError::Internal(format!("cannot encode upgrade operation: {error}"))
        })?;
        let temporary = self.journal_path.with_extension("json.tmp");
        fs::write(&temporary, bytes)?;
        fs::rename(temporary, &self.journal_path)?;
        Ok(())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn quarantine_wal(data_dir: &Path) -> std::io::Result<Option<PathBuf>> {
    let wal_path = data_dir.join("lsm.wal");
    if !wal_path.exists() {
        return Ok(None);
    }
    let bad_path = wal_path.with_extension(format!("wal.bad.{}", now_ms()));
    fs::rename(&wal_path, &bad_path)?;
    Ok(Some(bad_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn durable_upgrade_journal_resumes_only_the_identical_v074_source() {
        let data_dir = tempdir().unwrap();
        let source = tempdir().unwrap();
        fs::write(source.path().join("catalog.bin"), b"v0.7.4 source").unwrap();
        let coordinator = UpgradeCoordinator::new(data_dir.path());

        let started = coordinator
            .start(
                "upgrade-1",
                source.path(),
                UpgradeSourceKind::SingleNode,
                None,
            )
            .unwrap();
        assert_eq!(started.checkpoint, UpgradeCheckpoint::Planned);
        assert_eq!(
            UpgradeCoordinator::new(data_dir.path()).status().unwrap(),
            started
        );

        fs::write(source.path().join("catalog.bin"), b"changed source").unwrap();
        let changed = coordinator
            .resume(source.path(), UpgradeSourceKind::SingleNode, None)
            .unwrap();
        assert_eq!(
            changed.outcome,
            alopex_cluster::UpgradeOutcome::InputChanged
        );
        assert_eq!(changed.checkpoint, UpgradeCheckpoint::Planned);
    }

    #[test]
    fn prepared_upgrade_exposes_rollback_before_publication() {
        let data_dir = tempdir().unwrap();
        let source = tempdir().unwrap();
        fs::write(source.path().join("catalog.bin"), b"v0.7.4 source").unwrap();
        let coordinator = UpgradeCoordinator::new(data_dir.path());
        coordinator
            .start(
                "upgrade-1",
                source.path(),
                UpgradeSourceKind::SingleNode,
                None,
            )
            .unwrap();
        coordinator
            .checkpoint(UpgradeCheckpoint::CompatibilityValidated, None)
            .unwrap();
        let prepared = coordinator
            .checkpoint(
                UpgradeCheckpoint::MetadataPrepared,
                Some(SchemaManifestId::new("schema-1")),
            )
            .unwrap();
        assert_eq!(
            prepared.outcome,
            alopex_cluster::UpgradeOutcome::RollbackAvailable
        );
        let rolled_back = coordinator
            .checkpoint(UpgradeCheckpoint::RolledBack, None)
            .unwrap();
        assert_eq!(
            rolled_back.outcome,
            alopex_cluster::UpgradeOutcome::RolledBack
        );
    }
}
