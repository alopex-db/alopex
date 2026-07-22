//! Fenced range-worker lifecycle for distributed reads.
//!
//! This worker owns the authorization, deadline/cancellation, and cleanup
//! gates around a storage-specific `open_read_at` implementation. It has no
//! local fallback and does not accept executable logical plans.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use alopex_sql::executor::Row;

use crate::VerifiedPeerIdentity;

use super::{
    CleanupAcknowledgement, LocalReadAuthorizationRecheck, RangeReadBatch, RangeReadEnd,
    ReadDelegationVerifier, RemoteRangeReadRequest, RemoteRangeReadRequestError,
    authorize_remote_read,
};

/// Clock owned by the worker runtime. Tests provide a deterministic clock;
/// production transport supplies the local monotonic deadline clock.
pub trait RangeReadWorkerClock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// Storage implementation selected only after the worker has authenticated the
/// peer and validated the complete request fence.
///
/// Implementations must open the exact `constraint.snapshot().read_at()` and
/// use P2.6's bounded range scan entry point; a normal local transaction or a
/// table-prefix scan is not an implementation of this trait's contract.
pub trait FencedRangeReadBackend: Send + Sync {
    fn open_read_at(
        &self,
        request: &RemoteRangeReadRequest,
    ) -> Result<Box<dyn FencedRangeReadSession>, String>;
}

/// One backend session already opened at the validated range/read-at fence.
pub trait FencedRangeReadSession: Send {
    /// Return at most `max_rows` rows from the fenced range, or `None` at end.
    fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<Row>>, String>;

    /// Release iterator, snapshot transaction, and any backend state exactly
    /// once. The worker only emits `End` after this returns success.
    fn cleanup(self: Box<Self>) -> Result<(), String>;
}

/// Per-worker batch limit. Oversized backend output is treated as an error,
/// never an unbounded success payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeReadWorkerConfig {
    pub max_batch_rows: usize,
}

impl Default for RangeReadWorkerConfig {
    fn default() -> Self {
        Self {
            max_batch_rows: 1_024,
        }
    }
}

/// Invalid range-worker runtime configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RangeReadWorkerConfigError {
    #[error("range worker batch limit must be non-zero")]
    ZeroBatchRows,
}

/// Classified non-success worker outcome. Successful rows are never returned
/// with any of these errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RangeReadWorkerError {
    #[error("remote range request is invalid: {0}")]
    InvalidRequest(String),
    #[error("remote range request authorization failed: {0}")]
    Authorization(String),
    #[error("remote range request is already active: {0}")]
    DuplicateRequest(String),
    #[error("remote range request deadline elapsed")]
    DeadlineElapsed,
    #[error("remote range request was cancelled")]
    Cancelled,
    #[error("remote range peer disconnected")]
    PeerDisconnected,
    #[error("could not open fenced read-at snapshot: {0}")]
    OpenReadAt(String),
    #[error("fenced range scan failed: {0}")]
    Scan(String),
    #[error("fenced range backend emitted more than {max_rows} rows in one batch")]
    BatchLimitExceeded { max_rows: usize },
    #[error("range worker cleanup acknowledgement failed: {0}")]
    CleanupFailed(String),
}

/// All batches and the terminal cleanup acknowledgement for one successful
/// worker request. It is returned only after the snapshot has been cleaned up.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeReadExecution {
    pub batches: Vec<RangeReadBatch>,
    pub end: RangeReadEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopReason {
    Cancelled,
    PeerDisconnected,
}

#[derive(Default)]
struct WorkerControl {
    active: Mutex<BTreeMap<crate::RequestId, Option<StopReason>>>,
}

impl WorkerControl {
    fn active(&self) -> MutexGuard<'_, BTreeMap<crate::RequestId, Option<StopReason>>> {
        self.active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn begin(&self, request_id: &crate::RequestId) -> bool {
        let mut active = self.active();
        if active.contains_key(request_id) {
            false
        } else {
            active.insert(request_id.clone(), None);
            true
        }
    }

    fn stop(&self, request_id: &crate::RequestId, reason: StopReason) -> bool {
        let mut active = self.active();
        let Some(existing) = active.get_mut(request_id) else {
            return false;
        };
        if existing.is_none() {
            *existing = Some(reason);
            true
        } else {
            false
        }
    }

    fn stop_reason(&self, request_id: &crate::RequestId) -> Option<StopReason> {
        self.active().get(request_id).copied().flatten()
    }

    fn finish(&self, request_id: &crate::RequestId) {
        self.active().remove(request_id);
    }

    fn active_count(&self) -> usize {
        self.active().len()
    }
}

struct ActiveRequest {
    control: Arc<WorkerControl>,
    request_id: crate::RequestId,
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.control.finish(&self.request_id);
    }
}

/// Executes only authenticated, catalog-bound, range-fenced read sessions.
pub struct RangeReadWorker {
    verifier: Arc<dyn ReadDelegationVerifier>,
    local_authorizer: Arc<dyn LocalReadAuthorizationRecheck>,
    clock: Arc<dyn RangeReadWorkerClock>,
    config: RangeReadWorkerConfig,
    control: Arc<WorkerControl>,
}

impl RangeReadWorker {
    pub fn new(
        verifier: Arc<dyn ReadDelegationVerifier>,
        local_authorizer: Arc<dyn LocalReadAuthorizationRecheck>,
        clock: Arc<dyn RangeReadWorkerClock>,
    ) -> Self {
        Self {
            verifier,
            local_authorizer,
            clock,
            config: RangeReadWorkerConfig::default(),
            control: Arc::new(WorkerControl::default()),
        }
    }

    pub fn with_config(
        verifier: Arc<dyn ReadDelegationVerifier>,
        local_authorizer: Arc<dyn LocalReadAuthorizationRecheck>,
        clock: Arc<dyn RangeReadWorkerClock>,
        config: RangeReadWorkerConfig,
    ) -> Result<Self, RangeReadWorkerConfigError> {
        if config.max_batch_rows == 0 {
            return Err(RangeReadWorkerConfigError::ZeroBatchRows);
        }
        Ok(Self {
            verifier,
            local_authorizer,
            clock,
            config,
            control: Arc::new(WorkerControl::default()),
        })
    }

    /// Idempotently request cancellation of an active range session.
    /// Returns false when the request has already completed or was cancelled.
    pub fn cancel(&self, request_id: &crate::RequestId) -> bool {
        self.control.stop(request_id, StopReason::Cancelled)
    }

    /// Delivers an authenticated peer-disconnect event to an active session.
    pub fn peer_disconnected(&self, request_id: &crate::RequestId) -> bool {
        self.control.stop(request_id, StopReason::PeerDisconnected)
    }

    /// Number of live worker sessions. Intended for lifecycle monitoring and
    /// recovery tests; it returns zero after every terminal outcome.
    pub fn active_session_count(&self) -> usize {
        self.control.active_count()
    }

    /// Execute an entire fenced range read. Rows remain private to the worker
    /// result until cleanup has acknowledged success, preventing a terminal
    /// error from being mistaken for a partial successful stream.
    pub fn execute(
        &self,
        peer: VerifiedPeerIdentity,
        request: &RemoteRangeReadRequest,
        backend: &dyn FencedRangeReadBackend,
    ) -> Result<RangeReadExecution, RangeReadWorkerError> {
        let request_id = request.authorization.request_id.clone();
        let now_ms = self.clock.now_ms();
        match request.validate_before_open(now_ms) {
            Ok(()) => {}
            Err(RemoteRangeReadRequestError::DeadlineElapsed) => {
                return Err(RangeReadWorkerError::DeadlineElapsed);
            }
            Err(error) => return Err(RangeReadWorkerError::InvalidRequest(error.to_string())),
        }
        authorize_remote_read(
            peer,
            &request.authorization,
            now_ms,
            self.verifier.as_ref(),
            self.local_authorizer.as_ref(),
        )
        .map_err(|error| RangeReadWorkerError::Authorization(error.to_string()))?;
        if !self.control.begin(&request_id) {
            return Err(RangeReadWorkerError::DuplicateRequest(
                request_id.as_str().to_string(),
            ));
        }
        let _active = ActiveRequest {
            control: Arc::clone(&self.control),
            request_id: request_id.clone(),
        };
        self.check_terminal(&request_id, request.deadline_ms)?;
        let mut session = backend
            .open_read_at(request)
            .map_err(RangeReadWorkerError::OpenReadAt)?;
        let mut batches = Vec::new();
        let mut row_count = 0_u64;
        let terminal = loop {
            if let Err(error) = self.check_terminal(&request_id, request.deadline_ms) {
                break Some(error);
            }
            match session.next_batch(self.config.max_batch_rows) {
                Ok(Some(rows)) if rows.len() > self.config.max_batch_rows => {
                    break Some(RangeReadWorkerError::BatchLimitExceeded {
                        max_rows: self.config.max_batch_rows,
                    });
                }
                Ok(Some(rows)) => {
                    row_count = row_count.saturating_add(rows.len() as u64);
                    batches.push(RangeReadBatch {
                        request_id: request_id.clone(),
                        rows,
                    });
                }
                Ok(None) => break None,
                Err(error) => break Some(RangeReadWorkerError::Scan(error)),
            }
        };
        session
            .cleanup()
            .map_err(RangeReadWorkerError::CleanupFailed)?;
        if let Some(error) = terminal {
            return Err(error);
        }
        let cleanup = CleanupAcknowledgement {
            request_id: request_id.clone(),
        };
        Ok(RangeReadExecution {
            batches,
            end: RangeReadEnd {
                request_id,
                row_count,
                cleanup,
            },
        })
    }

    fn check_terminal(
        &self,
        request_id: &crate::RequestId,
        deadline_ms: u64,
    ) -> Result<(), RangeReadWorkerError> {
        match self.control.stop_reason(request_id) {
            Some(StopReason::Cancelled) => Err(RangeReadWorkerError::Cancelled),
            Some(StopReason::PeerDisconnected) => Err(RangeReadWorkerError::PeerDisconnected),
            None if self.clock.now_ms() >= deadline_ms => {
                Err(RangeReadWorkerError::DeadlineElapsed)
            }
            None => Ok(()),
        }
    }
}
