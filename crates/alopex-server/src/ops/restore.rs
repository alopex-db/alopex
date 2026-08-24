use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::task;
use uuid::Uuid;

use crate::error::{Result, ServerError};
use crate::ops::backup::{copy_dir_filtered, verify_snapshot_integrity};
use crate::ops::state::{LifecycleStateManager, Mode, OperationState, Progress, RestoreMetadata};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RestoreHandle {
    pub id: Uuid,
}

#[derive(Debug, Clone)]
pub struct RestoreSource {
    pub path: PathBuf,
}

#[derive(Debug, Default)]
struct RestoreRuntime {
    active: Option<RestoreHandle>,
    history: HashMap<RestoreHandle, RestoreRecord>,
}

#[derive(Debug, Clone)]
pub struct RestoreCoordinator {
    data_dir: PathBuf,
    state: Arc<LifecycleStateManager>,
    runtime: Arc<Mutex<RestoreRuntime>>,
}

impl RestoreCoordinator {
    pub fn new(data_dir: PathBuf, state: Arc<LifecycleStateManager>) -> Self {
        Self {
            data_dir,
            state,
            runtime: Arc::new(Mutex::new(RestoreRuntime::default())),
        }
    }

    pub async fn start_restore(&self, source: RestoreSource) -> Result<RestoreHandle> {
        preflight_restore_source(&source.path)?;
        let mut runtime = self.runtime.lock().expect("restore runtime lock poisoned");
        if runtime.active.is_some() {
            return Err(ServerError::Conflict("restore already running".to_string()));
        }

        let handle = RestoreHandle { id: Uuid::new_v4() };
        let mut running = OperationState::running();
        running.set_progress(Progress::percent(0))?;
        runtime.active = Some(handle.clone());
        runtime.history.insert(
            handle.clone(),
            RestoreRecord {
                state: running.clone(),
                metadata: None,
            },
        );
        self.state.set_restore_state(running);
        self.state.set_mode(Mode::Maintenance);

        let state = self.state.clone();
        let data_dir = self.data_dir.clone();
        let runtime = self.runtime.clone();
        let handle_for_task = handle.clone();
        task::spawn(async move {
            let result = task::spawn_blocking(move || run_restore(&data_dir, &source.path))
                .await
                .map_err(|err| ServerError::Internal(err.to_string()))
                .and_then(|res| res);

            let mut runtime = runtime.lock().expect("restore runtime lock poisoned");
            runtime.active = None;

            match result {
                Ok(metadata) => {
                    state.set_mode(Mode::Normal);
                    state.set_restore_metadata(Some(metadata.clone()));
                    let completed = OperationState::completed(Some(Progress::percent(100)))
                        .unwrap_or_else(|err| OperationState::failed(err.to_string()));
                    if let Some(record) = runtime.history.get_mut(&handle_for_task) {
                        record.state = completed.clone();
                        record.metadata = Some(metadata);
                    }
                    state.set_restore_state(completed);
                }
                Err(err) => {
                    state.set_mode(Mode::ReadOnly);
                    let failed = OperationState::failed(err.to_string());
                    if let Some(record) = runtime.history.get_mut(&handle_for_task) {
                        record.state = failed.clone();
                    }
                    state.set_restore_state(failed);
                }
            }
        });

        Ok(handle)
    }

    pub fn status(&self, handle: &RestoreHandle) -> Result<OperationState> {
        let runtime = self.runtime.lock().expect("restore runtime lock poisoned");
        runtime
            .history
            .get(handle)
            .map(|record| record.state.clone())
            .ok_or_else(|| ServerError::NotFound("restore handle not found".to_string()))
    }

    pub fn metadata(&self, handle: &RestoreHandle) -> Result<Option<RestoreMetadata>> {
        let runtime = self.runtime.lock().expect("restore runtime lock poisoned");
        runtime
            .history
            .get(handle)
            .map(|record| record.metadata.clone())
            .ok_or_else(|| ServerError::NotFound("restore handle not found".to_string()))
    }
}

fn run_restore(data_dir: &Path, source: &Path) -> Result<RestoreMetadata> {
    let backup_dir = restore_backup_dir(data_dir);
    fs::create_dir_all(&backup_dir)?;
    copy_dir_filtered(data_dir, &backup_dir)?;
    clear_data_dir(data_dir)?;
    copy_dir_filtered(source, data_dir)?;
    Ok(RestoreMetadata {
        backup_id: backup_id_from_path(source),
        location: source.display().to_string(),
        restored_at_ms: now_ms(),
        size_bytes: dir_size_bytes(source)?,
    })
}

pub fn resolve_default_source(data_dir: &Path) -> Result<PathBuf> {
    let lifecycle_root = data_dir.join(".lifecycle");
    let backup_root = lifecycle_root.join("backup");
    let archive_root = lifecycle_root.join("archive");
    if let Ok(path) = read_latest_marker(&backup_root) {
        return Ok(path);
    }
    read_latest_marker(&archive_root)
}

fn validate_source(source: &Path) -> Result<()> {
    if !source.exists() {
        return Err(ServerError::NotFound(format!(
            "restore source does not exist: {}",
            source.display()
        )));
    }
    if !source.is_dir() {
        return Err(ServerError::BadRequest(format!(
            "restore source is not a directory: {}",
            source.display()
        )));
    }
    Ok(())
}

fn preflight_restore_source(source: &Path) -> Result<()> {
    validate_source(source)?;
    let manifest_path = source.join("snapshot.manifest");
    if !manifest_path.exists() {
        return Ok(());
    }
    verify_snapshot_integrity(source).map_err(|err| {
        ServerError::RestoreIntegrityMismatch(format!(
            "source snapshot failed integrity checks: {err}"
        ))
    })
}

/// Computes a deterministic SHA-256 identity for a restore/upgrade source.
/// Every directory entry path, type, and file byte is included in sorted
/// order, while `.lifecycle` remains excluded because it is server-local
/// operation history rather than source database content.
pub fn restore_source_fingerprint(source: &Path) -> Result<String> {
    validate_source(source)?;
    let mut digest = Sha256::new();
    hash_restore_tree(source, source, &mut digest)?;
    Ok(format!("{:x}", digest.finalize()))
}

fn hash_restore_tree(root: &Path, path: &Path, digest: &mut Sha256) -> Result<()> {
    let mut entries = fs::read_dir(path)?.collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name();
        if path == root && name == ".lifecycle" {
            continue;
        }
        let child = entry.path();
        let relative = child
            .strip_prefix(root)
            .map_err(|error| ServerError::Internal(error.to_string()))?;
        digest.update(relative.to_string_lossy().as_bytes());
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            digest.update(b"directory");
            hash_restore_tree(root, &child, digest)?;
        } else if metadata.is_file() {
            digest.update(b"file");
            let mut file = fs::File::open(&child)?;
            let mut buffer = [0u8; 8192];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                digest.update(&buffer[..read]);
            }
        }
    }
    Ok(())
}

fn restore_backup_dir(data_dir: &Path) -> PathBuf {
    data_dir
        .join(".lifecycle")
        .join("restore-backup")
        .join(timestamp_dir())
}

fn timestamp_dir() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("ts-{seconds}")
}

fn clear_data_dir(dir: &Path) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        // 裁定 D15: restore runs *inside* the server that holds this directory's
        // lock. Deleting the lock file here would unlink the inode we hold, so
        // a second process could create a fresh one at the same path and lock
        // it — two live writers, which is issue #181 all over again.
        if name == ".lifecycle" || alopex_core::lsm::is_lock_file(Path::new(&name)) {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

fn backup_id_from_path(source: &Path) -> String {
    source
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| source.display().to_string())
}

fn read_latest_marker(root: &Path) -> Result<PathBuf> {
    let marker = root.join("latest");
    if !marker.exists() {
        return Err(ServerError::NotFound(format!(
            "no snapshots found in {}",
            root.display()
        )));
    }
    let path = fs::read_to_string(&marker)?;
    let path = PathBuf::from(path.trim());
    if !path.exists() {
        return Err(ServerError::NotFound(format!(
            "latest snapshot path does not exist: {}",
            path.display()
        )));
    }
    Ok(path)
}

fn dir_size_bytes(dir: &Path) -> Result<u64> {
    let mut size = 0u64;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".lifecycle" || alopex_core::lsm::is_lock_file(Path::new(&name)) {
            continue;
        }
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            size = size.saturating_add(dir_size_bytes(&path)?);
        } else {
            size = size.saturating_add(metadata.len());
        }
    }
    Ok(size)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone)]
struct RestoreRecord {
    state: OperationState,
    metadata: Option<RestoreMetadata>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clear_data_dir_preserves_lifecycle_and_lock_files() {
        let data_dir = tempfile::tempdir().expect("data tempdir");
        fs::create_dir_all(data_dir.path().join(".lifecycle/restore")).expect("lifecycle");
        fs::create_dir_all(data_dir.path().join("sst")).expect("sst dir");
        fs::write(data_dir.path().join(".alopex.lock"), b"pid=1").expect("plain lock");
        fs::write(data_dir.path().join("mydb.alopex.lock"), b"pid=1").expect("sidecar lock");
        fs::write(data_dir.path().join("lsm.wal"), b"wal").expect("wal");
        fs::write(data_dir.path().join("sst/1.sst"), b"sst").expect("sst");

        assert_eq!(
            dir_size_bytes(data_dir.path()).expect("size data directory"),
            6,
            "restore metadata must count canonical data, not local lock artifacts"
        );
        clear_data_dir(data_dir.path()).expect("clear data directory");

        assert!(data_dir.path().join(".lifecycle").exists());
        assert!(data_dir.path().join(".alopex.lock").exists());
        assert!(data_dir.path().join("mydb.alopex.lock").exists());
        assert!(!data_dir.path().join("lsm.wal").exists());
        assert!(!data_dir.path().join("sst").exists());
    }
}
