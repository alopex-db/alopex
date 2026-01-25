use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use alopex_core::kv::any::AnyKV;
use alopex_core::lsm::{LsmKV, LsmKVConfig, RecoveryResult};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::ops::state::{LifecycleStateManager, Mode};

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
        if matches!(info.outcome, RecoveryOutcome::ReadOnly | RecoveryOutcome::Failed) {
            state.set_mode(Mode::ReadOnly);
        }
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
