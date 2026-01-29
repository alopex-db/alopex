use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::metrics::Metrics;
use crate::ops::recovery::RecoveryInfo;
use crate::ops::state::{LifecycleStateManager, Mode, OperationState, RestoreMetadata};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OverallStatus {
    Ok,
    Degraded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusView {
    pub overall_status: OverallStatus,
    pub read_only: bool,
    pub maintenance: bool,
    pub recovery_state: RecoveryInfo,
    pub backup_state: OperationState,
    pub restore_state: OperationState,
    pub restore_metadata: Option<RestoreMetadata>,
    pub last_recovery_at_ms: Option<u64>,
    pub last_backup_at_ms: Option<u64>,
    pub last_restore_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StatusReporter {
    lifecycle_state: Arc<LifecycleStateManager>,
    recovery_info: RecoveryInfo,
}

impl StatusReporter {
    pub fn new(lifecycle_state: Arc<LifecycleStateManager>, recovery_info: RecoveryInfo) -> Self {
        Self {
            lifecycle_state,
            recovery_info,
        }
    }

    pub fn status_view(&self) -> StatusView {
        let mode = self.lifecycle_state.current_mode();
        let read_only = mode == Mode::ReadOnly;
        let maintenance = mode == Mode::Maintenance;
        let overall_status = if read_only || maintenance {
            OverallStatus::Degraded
        } else {
            OverallStatus::Ok
        };

        let backup_state = self.lifecycle_state.backup_state();
        let restore_state = self.lifecycle_state.restore_state();
        let restore_metadata = self.lifecycle_state.restore_metadata();
        let last_recovery_at_ms = Some(self.recovery_info.finished_at_ms);
        let last_backup_at_ms = backup_state.finished_at_ms;
        let last_restore_at_ms = restore_metadata
            .as_ref()
            .map(|meta| meta.restored_at_ms)
            .or(restore_state.finished_at_ms);

        StatusView {
            overall_status,
            read_only,
            maintenance,
            recovery_state: self.recovery_info.clone(),
            backup_state,
            restore_state,
            restore_metadata,
            last_recovery_at_ms,
            last_backup_at_ms,
            last_restore_at_ms,
        }
    }

    pub fn refresh_metrics(&self, metrics: &Metrics) {
        let view = self.status_view();
        metrics.record_operational_state(
            &view.recovery_state,
            &view.backup_state,
            &view.restore_state,
        );
    }
}
