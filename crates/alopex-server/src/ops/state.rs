use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{Result, ServerError};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Normal,
    ReadOnly,
    Maintenance,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Progress {
    pub percent: Option<u8>,
    pub bytes_processed: Option<u64>,
}

impl Progress {
    pub fn percent(value: u8) -> Self {
        Self {
            percent: Some(value),
            bytes_processed: None,
        }
    }

    pub fn bytes(value: u64) -> Self {
        Self {
            percent: None,
            bytes_processed: Some(value),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.percent.is_none() && self.bytes_processed.is_none() {
            return Err(ServerError::BadRequest(
                "progress must include percent or bytes_processed".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperationState {
    pub status: OperationStatus,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub progress: Option<Progress>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreMetadata {
    pub backup_id: String,
    pub location: String,
    pub restored_at_ms: u64,
    pub size_bytes: u64,
}

impl OperationState {
    pub fn queued() -> Self {
        Self {
            status: OperationStatus::Queued,
            started_at_ms: None,
            finished_at_ms: None,
            progress: None,
            reason: None,
        }
    }

    pub fn running() -> Self {
        Self {
            status: OperationStatus::Running,
            started_at_ms: Some(now_ms()),
            finished_at_ms: None,
            progress: None,
            reason: None,
        }
    }

    pub fn completed(progress: Option<Progress>) -> Result<Self> {
        if let Some(progress) = &progress {
            progress.validate()?;
        }
        Ok(Self {
            status: OperationStatus::Completed,
            started_at_ms: None,
            finished_at_ms: Some(now_ms()),
            progress,
            reason: None,
        })
    }

    pub fn failed(reason: impl Into<String>) -> Self {
        Self {
            status: OperationStatus::Failed,
            started_at_ms: None,
            finished_at_ms: Some(now_ms()),
            progress: None,
            reason: Some(reason.into()),
        }
    }

    pub fn cancelled(reason: impl Into<String>) -> Self {
        Self {
            status: OperationStatus::Cancelled,
            started_at_ms: None,
            finished_at_ms: Some(now_ms()),
            progress: None,
            reason: Some(reason.into()),
        }
    }

    pub fn mark_running(&mut self) {
        self.status = OperationStatus::Running;
        self.started_at_ms = Some(now_ms());
        self.finished_at_ms = None;
    }

    pub fn mark_finished(&mut self, status: OperationStatus, reason: Option<String>) {
        self.status = status;
        self.finished_at_ms = Some(now_ms());
        self.reason = reason;
    }

    pub fn set_progress(&mut self, progress: Progress) -> Result<()> {
        progress.validate()?;
        self.progress = Some(progress);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct LifecycleState {
    mode: Mode,
    backup_state: OperationState,
    restore_state: OperationState,
    restore_metadata: Option<RestoreMetadata>,
}

#[derive(Debug)]
pub struct LifecycleStateManager {
    inner: RwLock<LifecycleState>,
}

impl LifecycleStateManager {
    pub fn new(initial_mode: Mode) -> Self {
        Self {
            inner: RwLock::new(LifecycleState {
                mode: initial_mode,
                backup_state: OperationState::queued(),
                restore_state: OperationState::queued(),
                restore_metadata: None,
            }),
        }
    }

    pub fn current_mode(&self) -> Mode {
        self.inner
            .read()
            .expect("lifecycle state lock poisoned")
            .mode
    }

    pub fn set_mode(&self, mode: Mode) {
        self.inner
            .write()
            .expect("lifecycle state lock poisoned")
            .mode = mode;
    }

    pub fn backup_state(&self) -> OperationState {
        self.inner
            .read()
            .expect("lifecycle state lock poisoned")
            .backup_state
            .clone()
    }

    pub fn set_backup_state(&self, state: OperationState) {
        self.inner
            .write()
            .expect("lifecycle state lock poisoned")
            .backup_state = state;
    }

    pub fn restore_state(&self) -> OperationState {
        self.inner
            .read()
            .expect("lifecycle state lock poisoned")
            .restore_state
            .clone()
    }

    pub fn set_restore_state(&self, state: OperationState) {
        self.inner
            .write()
            .expect("lifecycle state lock poisoned")
            .restore_state = state;
    }

    pub fn restore_metadata(&self) -> Option<RestoreMetadata> {
        self.inner
            .read()
            .expect("lifecycle state lock poisoned")
            .restore_metadata
            .clone()
    }

    pub fn set_restore_metadata(&self, metadata: Option<RestoreMetadata>) {
        self.inner
            .write()
            .expect("lifecycle state lock poisoned")
            .restore_metadata = metadata;
    }

    pub fn should_block_writes(&self) -> bool {
        matches!(self.current_mode(), Mode::ReadOnly | Mode::Maintenance)
    }

    pub fn check_write_allowed(&self) -> Result<()> {
        match self.current_mode() {
            Mode::Normal => Ok(()),
            Mode::ReadOnly => Err(ServerError::Conflict(
                "writes are blocked in read_only mode".to_string(),
            )),
            Mode::Maintenance => Err(ServerError::Conflict(
                "writes are blocked in maintenance mode".to_string(),
            )),
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
