use std::time::Duration;

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};

use crate::error::{Result, ServerError};
use crate::ops::recovery::{RecoveryInfo, RecoveryOutcome};
use crate::ops::state::{OperationState, OperationStatus};

/// Prometheus metrics registry.
#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    query_count: IntCounter,
    query_latency: Histogram,
    active_connections: IntGauge,
    stream_backpressure: IntCounter,
    error_count: IntCounterVec,
    recovery_outcome: IntGauge,
    recovery_duration_ms: IntGauge,
    recovery_entries_replayed: IntGauge,
    recovery_warnings: IntGauge,
    backup_status: IntGauge,
    backup_progress_percent: IntGauge,
    restore_status: IntGauge,
    restore_progress_percent: IntGauge,
}

impl Metrics {
    /// Create a new metrics registry.
    pub fn new() -> Result<Self> {
        let registry = Registry::new();
        let query_count = IntCounter::with_opts(Opts::new("query_count", "Total queries"))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        let query_latency = Histogram::with_opts(HistogramOpts::new(
            "query_latency_seconds",
            "Query latency in seconds",
        ))
        .map_err(|err| ServerError::Internal(err.to_string()))?;
        let active_connections =
            IntGauge::with_opts(Opts::new("active_connections", "Active connections"))
                .map_err(|err| ServerError::Internal(err.to_string()))?;
        let stream_backpressure =
            IntCounter::with_opts(Opts::new("stream_backpressure", "Backpressure events"))
                .map_err(|err| ServerError::Internal(err.to_string()))?;
        let error_count = IntCounterVec::new(Opts::new("error_count", "Error count"), &["kind"])
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        let recovery_outcome =
            IntGauge::with_opts(Opts::new("recovery_outcome", "Recovery outcome state"))
                .map_err(|err| ServerError::Internal(err.to_string()))?;
        let recovery_duration_ms = IntGauge::with_opts(Opts::new(
            "recovery_duration_ms",
            "Recovery duration in milliseconds",
        ))
        .map_err(|err| ServerError::Internal(err.to_string()))?;
        let recovery_entries_replayed = IntGauge::with_opts(Opts::new(
            "recovery_entries_replayed",
            "WAL entries replayed during recovery",
        ))
        .map_err(|err| ServerError::Internal(err.to_string()))?;
        let recovery_warnings = IntGauge::with_opts(Opts::new(
            "recovery_warning_count",
            "Recovery warning count",
        ))
        .map_err(|err| ServerError::Internal(err.to_string()))?;
        let backup_status = IntGauge::with_opts(Opts::new(
            "backup_operation_status",
            "Backup operation status",
        ))
        .map_err(|err| ServerError::Internal(err.to_string()))?;
        let backup_progress_percent = IntGauge::with_opts(Opts::new(
            "backup_progress_percent",
            "Backup operation progress percent",
        ))
        .map_err(|err| ServerError::Internal(err.to_string()))?;
        let restore_status = IntGauge::with_opts(Opts::new(
            "restore_operation_status",
            "Restore operation status",
        ))
        .map_err(|err| ServerError::Internal(err.to_string()))?;
        let restore_progress_percent = IntGauge::with_opts(Opts::new(
            "restore_progress_percent",
            "Restore operation progress percent",
        ))
        .map_err(|err| ServerError::Internal(err.to_string()))?;

        registry
            .register(Box::new(query_count.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(query_latency.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(active_connections.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(stream_backpressure.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(error_count.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(recovery_outcome.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(recovery_duration_ms.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(recovery_entries_replayed.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(recovery_warnings.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(backup_status.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(backup_progress_percent.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(restore_status.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(restore_progress_percent.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;

        Ok(Self {
            registry,
            query_count,
            query_latency,
            active_connections,
            stream_backpressure,
            error_count,
            recovery_outcome,
            recovery_duration_ms,
            recovery_entries_replayed,
            recovery_warnings,
            backup_status,
            backup_progress_percent,
            restore_status,
            restore_progress_percent,
        })
    }

    /// Record query completion.
    pub fn record_query(&self, duration: Duration, success: bool) {
        self.query_count.inc();
        self.query_latency.observe(duration.as_secs_f64());
        if !success {
            self.error_count.with_label_values(&["query"]).inc();
        }
    }

    /// Track connection count delta.
    pub fn record_connection(&self, delta: i64) {
        if delta >= 0 {
            self.active_connections.add(delta);
        } else {
            self.active_connections.sub(-delta);
        }
    }

    /// Track a backpressure event.
    pub fn record_backpressure(&self) {
        self.stream_backpressure.inc();
    }

    /// Record a generic error.
    pub fn record_error(&self, kind: &str) {
        self.error_count.with_label_values(&[kind]).inc();
    }

    /// Record operational recovery/backup/restore state metrics.
    pub fn record_operational_state(
        &self,
        recovery: &RecoveryInfo,
        backup: &OperationState,
        restore: &OperationState,
    ) {
        self.recovery_outcome
            .set(recovery_outcome_value(recovery.outcome));
        self.recovery_duration_ms.set(recovery.duration_ms as i64);
        self.recovery_entries_replayed
            .set(recovery.entries_replayed as i64);
        self.recovery_warnings.set(recovery.warnings as i64);
        self.backup_status
            .set(operation_status_value(backup.status));
        self.backup_progress_percent
            .set(progress_percent(backup) as i64);
        self.restore_status
            .set(operation_status_value(restore.status));
        self.restore_progress_percent
            .set(progress_percent(restore) as i64);
    }

    /// Render metrics in Prometheus text format.
    pub fn expose_prometheus(&self) -> Result<String> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        encoder
            .encode(&self.registry.gather(), &mut buffer)
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        String::from_utf8(buffer)
            .map_err(|err| ServerError::Internal(format!("invalid metrics utf8: {err}")))
    }
}

fn operation_status_value(status: OperationStatus) -> i64 {
    match status {
        OperationStatus::Queued => 0,
        OperationStatus::Running => 1,
        OperationStatus::Completed => 2,
        OperationStatus::Failed => 3,
        OperationStatus::Cancelled => 4,
    }
}

fn recovery_outcome_value(outcome: RecoveryOutcome) -> i64 {
    match outcome {
        RecoveryOutcome::Success => 0,
        RecoveryOutcome::ReadOnly => 1,
        RecoveryOutcome::Failed => 2,
    }
}

fn progress_percent(state: &OperationState) -> u8 {
    state
        .progress
        .as_ref()
        .and_then(|progress| progress.percent)
        .unwrap_or(0)
}
