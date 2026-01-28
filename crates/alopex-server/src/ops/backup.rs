use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use alopex_core::lsm::checkpoint::load_checkpoint_meta;
use alopex_core::lsm::sstable::SSTableReader;
use alopex_core::lsm::wal::WalReader;
use alopex_core::lsm::LsmKVConfig;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use tokio::task;
use uuid::Uuid;

use crate::error::{Result, ServerError};
use crate::ops::state::{LifecycleStateManager, OperationState, Progress};

const SNAPSHOT_MANIFEST_NAME: &str = "snapshot.manifest";
const SNAPSHOT_MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BackupHandle {
    pub id: Uuid,
}

#[derive(Debug, Clone)]
pub struct BackupMetadata {
    pub handle: BackupHandle,
    pub location: PathBuf,
}

#[derive(Debug, Clone)]
struct BackupRecord {
    metadata: BackupMetadata,
    state: OperationState,
}

#[derive(Debug, Default)]
struct BackupRuntime {
    active: Option<BackupHandle>,
    history: HashMap<BackupHandle, BackupRecord>,
    last_location: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotManifest {
    version: u32,
    entries: Vec<SnapshotEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotEntry {
    path: String,
    size: u64,
    crc32: u32,
}

#[derive(Clone)]
pub struct BackupCoordinator {
    data_dir: PathBuf,
    state: Arc<LifecycleStateManager>,
    checkpoint: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    runtime: Arc<Mutex<BackupRuntime>>,
}

impl BackupCoordinator {
    pub fn new(
        data_dir: PathBuf,
        state: Arc<LifecycleStateManager>,
        checkpoint: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> Self {
        Self {
            data_dir,
            state,
            checkpoint,
            runtime: Arc::new(Mutex::new(BackupRuntime::default())),
        }
    }

    pub async fn start_backup(&self) -> Result<BackupHandle> {
        let mut runtime = self.runtime.lock().expect("backup runtime lock poisoned");
        if runtime.active.is_some() {
            return Err(ServerError::Conflict("backup already running".to_string()));
        }

        let handle = BackupHandle { id: Uuid::new_v4() };
        let dest = backup_destination(&self.data_dir);
        fs::create_dir_all(&dest)?;
        let metadata = BackupMetadata {
            handle: handle.clone(),
            location: dest.clone(),
        };
        let mut running = OperationState::running();
        running.set_progress(Progress::percent(0))?;
        runtime.active = Some(handle.clone());
        runtime.last_location = Some(dest.clone());
        runtime.history.insert(
            handle.clone(),
            BackupRecord {
                metadata: metadata.clone(),
                state: running.clone(),
            },
        );
        self.state.set_backup_state(running);

        let state = self.state.clone();
        let data_dir = self.data_dir.clone();
        let runtime = self.runtime.clone();
        let checkpoint = self.checkpoint.clone();
        let handle_for_task = handle.clone();
        task::spawn(async move {
            let result = task::spawn_blocking(move || run_backup(&data_dir, &dest, checkpoint))
                .await
                .map_err(|err| ServerError::Internal(err.to_string()))
                .and_then(|res| res);

            let mut runtime = runtime.lock().expect("backup runtime lock poisoned");
            runtime.active = None;

            match result {
                Ok(()) => {
                    let completed = OperationState::completed(Some(Progress::percent(100)))
                        .unwrap_or_else(|err| OperationState::failed(err.to_string()));
                    if let Some(record) = runtime.history.get_mut(&handle_for_task) {
                        record.state = completed.clone();
                    }
                    state.set_backup_state(completed);
                }
                Err(err) => {
                    let failed = OperationState::failed(err.to_string());
                    if let Some(record) = runtime.history.get_mut(&handle_for_task) {
                        record.state = failed.clone();
                    }
                    state.set_backup_state(failed);
                }
            }
        });

        Ok(handle)
    }

    pub fn status(&self, handle: &BackupHandle) -> Result<OperationState> {
        let runtime = self.runtime.lock().expect("backup runtime lock poisoned");
        runtime
            .history
            .get(handle)
            .map(|record| record.state.clone())
            .ok_or_else(|| ServerError::NotFound("backup handle not found".to_string()))
    }

    pub fn location(&self, handle: &BackupHandle) -> Result<PathBuf> {
        let runtime = self.runtime.lock().expect("backup runtime lock poisoned");
        runtime
            .history
            .get(handle)
            .map(|record| record.metadata.location.clone())
            .ok_or_else(|| ServerError::NotFound("backup handle not found".to_string()))
    }

    pub fn latest_location(&self) -> Option<PathBuf> {
        let runtime = self.runtime.lock().expect("backup runtime lock poisoned");
        runtime.last_location.clone()
    }
}

fn run_backup(
    data_dir: &Path,
    dest: &Path,
    checkpoint: Arc<dyn Fn() -> Result<()> + Send + Sync>,
) -> Result<()> {
    if !data_dir.exists() {
        return Err(ServerError::NotFound(format!(
            "data directory does not exist: {}",
            data_dir.display()
        )));
    }
    if !data_dir.is_dir() {
        return Err(ServerError::BadRequest(format!(
            "data directory is not a directory: {}",
            data_dir.display()
        )));
    }

    checkpoint().map_err(|err| ServerError::Internal(format!("checkpoint failed: {err}")))?;
    fs::create_dir_all(dest)?;
    let manifest = build_snapshot_manifest(data_dir)?;
    copy_dir_filtered(data_dir, dest)?;
    write_snapshot_manifest(dest, &manifest)?;
    verify_snapshot(dest)?;
    write_latest_marker(&backup_root(data_dir), dest)?;
    Ok(())
}

fn backup_destination(data_dir: &Path) -> PathBuf {
    backup_root(data_dir).join(timestamp_dir())
}

fn backup_root(data_dir: &Path) -> PathBuf {
    data_dir.join(".lifecycle").join("backup")
}

fn timestamp_dir() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("ts-{seconds}")
}

fn write_latest_marker(root: &Path, latest: &Path) -> Result<()> {
    fs::create_dir_all(root)?;
    let marker = root.join("latest");
    fs::write(marker, latest.display().to_string().as_bytes())?;
    Ok(())
}

pub(crate) fn copy_dir_filtered(src: &Path, dest: &Path) -> Result<()> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        if name == ".lifecycle" {
            continue;
        }
        let dest_path = dest.join(name);
        if file_type.is_dir() {
            fs::create_dir_all(&dest_path)?;
            copy_dir_filtered(&entry.path(), &dest_path)?;
        } else {
            fs::copy(entry.path(), &dest_path)?;
        }
    }
    Ok(())
}

fn verify_snapshot(dest: &Path) -> Result<()> {
    let manifest = read_snapshot_manifest(dest)?;
    validate_manifest(dest, &manifest)?;

    let checkpoint_path = dest.join("checkpoint.meta");
    let meta = load_checkpoint_meta(&checkpoint_path)?;
    if meta.is_none() {
        return Err(ServerError::Internal(
            "checkpoint metadata missing or corrupted".to_string(),
        ));
    }
    let wal_path = dest.join("lsm.wal");
    if !wal_path.exists() {
        return Err(ServerError::Internal(
            "snapshot missing lsm.wal".to_string(),
        ));
    }
    let sst_dir = dest.join("sst");
    if !sst_dir.exists() {
        return Err(ServerError::Internal(
            "snapshot missing sst directory".to_string(),
        ));
    }

    let wal_config = LsmKVConfig::default().wal;
    let mut reader = WalReader::open(&wal_path, wal_config)?;
    let _ = reader.replay()?;

    for entry in fs::read_dir(&sst_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sst"))
        {
            let _ = SSTableReader::open(&path)?;
        }
    }
    Ok(())
}

fn build_snapshot_manifest(source: &Path) -> Result<SnapshotManifest> {
    let mut entries = Vec::new();
    collect_manifest_entries(source, source, &mut entries, true)?;
    Ok(SnapshotManifest {
        version: SNAPSHOT_MANIFEST_VERSION,
        entries,
    })
}

fn write_snapshot_manifest(dest: &Path, manifest: &SnapshotManifest) -> Result<()> {
    let manifest_path = dest.join(SNAPSHOT_MANIFEST_NAME);
    let payload = serde_json::to_vec_pretty(&manifest)
        .map_err(|err| ServerError::Internal(format!("manifest encode failed: {err}")))?;
    fs::write(&manifest_path, payload)?;
    Ok(())
}

fn read_snapshot_manifest(dest: &Path) -> Result<SnapshotManifest> {
    let manifest_path = dest.join(SNAPSHOT_MANIFEST_NAME);
    let payload = fs::read(&manifest_path)?;
    let manifest: SnapshotManifest = serde_json::from_slice(&payload)
        .map_err(|err| ServerError::Internal(format!("manifest decode failed: {err}")))?;
    if manifest.version != SNAPSHOT_MANIFEST_VERSION {
        return Err(ServerError::Internal(format!(
            "unsupported manifest version: {}",
            manifest.version
        )));
    }
    Ok(manifest)
}

fn validate_manifest(dest: &Path, manifest: &SnapshotManifest) -> Result<()> {
    for entry in &manifest.entries {
        let path = dest.join(&entry.path);
        let metadata = path.metadata().map_err(|err| {
            ServerError::Internal(format!("snapshot entry missing {}: {err}", entry.path))
        })?;
        if metadata.len() != entry.size {
            return Err(ServerError::Internal(format!(
                "snapshot entry size mismatch {}",
                entry.path
            )));
        }
        let crc = crc32_file(&path)?;
        if crc != entry.crc32 {
            return Err(ServerError::Internal(format!(
                "snapshot entry crc mismatch {}",
                entry.path
            )));
        }
    }
    Ok(())
}

fn collect_manifest_entries(
    root: &Path,
    current: &Path,
    entries: &mut Vec<SnapshotEntry>,
    skip_lifecycle: bool,
) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        if skip_lifecycle && name == ".lifecycle" {
            continue;
        }
        if name == SNAPSHOT_MANIFEST_NAME {
            continue;
        }
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_manifest_entries(root, &path, entries, skip_lifecycle)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|err| ServerError::Internal(format!("manifest path error: {err}")))?;
            let crc32 = crc32_file(&path)?;
            entries.push(SnapshotEntry {
                path: relative.to_string_lossy().replace('\\', "/"),
                size: metadata.len(),
                crc32,
            });
        }
    }
    Ok(())
}

fn crc32_file(path: &Path) -> Result<u32> {
    let mut file = fs::File::open(path)?;
    let mut buf = [0u8; 8192];
    let mut hasher = Hasher::new();
    loop {
        let read = std::io::Read::read(&mut file, &mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize())
}
