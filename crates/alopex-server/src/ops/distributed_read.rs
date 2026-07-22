//! Server-owned authorization entry point for a remote range worker.
//!
//! The cluster crate validates signed transport facts. This module injects the
//! server-local data policy into that validation and binds a successful
//! delegation subject to a fresh worker session before P2.10 opens storage.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use alopex_cluster::{
    authorize_remote_read, LocalReadAuthorizationRecheck, ReadDelegationVerifier,
    RemoteReadAuthorizationEnvelope, RequestId, VerifiedPeerIdentity,
};
use alopex_sql::distributed_read::{PreparedResult, PreparedResultStream};
use dashmap::DashMap;
use serde::Serialize;
use uuid::Uuid;

use crate::error::{Result, ServerError};
use crate::session::{SessionId, SessionManager};

/// The ID shared by the coordinator, every range-worker request, the terminal
/// summary, and the HTTP cancellation route.
///
/// This deliberately reuses the cluster request ID rather than introducing a
/// second correlation value which could be accidentally omitted from a worker
/// cancellation message.
pub type ReadExecutionId = RequestId;

const READ_EXECUTION_SUMMARY_VERSION: u32 = 1;

/// Authenticated ownership of a coordinator-side read execution.
///
/// The profile is never included in the public summary. It is retained only
/// to ensure that another authenticated caller cannot inspect or cancel the
/// execution. A supplied SQL session is correlation state, not a substitute
/// for the profile authorization check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadExecutionOwner {
    profile: String,
    session_id: Option<SessionId>,
}

impl ReadExecutionOwner {
    pub fn new(profile: impl Into<String>, session_id: Option<SessionId>) -> Result<Self> {
        let profile = profile.into();
        if profile.trim().is_empty() {
            return Err(ServerError::Unauthorized(
                "distributed read requires an authenticated profile".into(),
            ));
        }
        Ok(Self {
            profile,
            session_id,
        })
    }

    fn permits(&self, profile: Option<&str>) -> bool {
        profile == Some(self.profile.as_str())
    }

    pub fn session_id(&self) -> Option<&SessionId> {
        self.session_id.as_ref()
    }
}

/// Immutable routing evidence which must accompany every public summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadExecutionPlanSummary {
    pub requested_mode: String,
    pub effective_mode: String,
    pub metadata_version: u64,
    pub ranges: Vec<String>,
    pub freshness: String,
    pub retry_count: u32,
    pub failover_count: u32,
}

impl ReadExecutionPlanSummary {
    pub fn validate(&self) -> Result<()> {
        if self.requested_mode.trim().is_empty()
            || self.effective_mode.trim().is_empty()
            || self.freshness.trim().is_empty()
        {
            return Err(ServerError::BadRequest(
                "distributed read summary requires requested/effective mode and freshness".into(),
            ));
        }
        if self.ranges.is_empty() || self.ranges.iter().any(|range| range.trim().is_empty()) {
            return Err(ServerError::BadRequest(
                "distributed read summary requires at least one range".into(),
            ));
        }
        Ok(())
    }
}

/// Stable terminal classification exposed to HTTP and CLI adapters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadExecutionOutcome {
    Preparing,
    Success,
    RetryableFailure,
    TerminalFailure,
    Cancelled,
}

/// Versioned public terminal summary. It contains no per-range rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReadExecutionSummary {
    pub schema_version: u32,
    pub execution_id: ReadExecutionId,
    pub outcome: ReadExecutionOutcome,
    pub requested_mode: String,
    pub effective_mode: String,
    pub metadata_version: u64,
    pub ranges: Vec<String>,
    pub freshness: String,
    pub retry_count: u32,
    pub failover_count: u32,
    pub resource_outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl ReadExecutionSummary {
    fn preparing(execution_id: ReadExecutionId, plan: ReadExecutionPlanSummary) -> Self {
        Self {
            schema_version: READ_EXECUTION_SUMMARY_VERSION,
            execution_id,
            outcome: ReadExecutionOutcome::Preparing,
            requested_mode: plan.requested_mode,
            effective_mode: plan.effective_mode,
            metadata_version: plan.metadata_version,
            ranges: plan.ranges,
            freshness: plan.freshness,
            retry_count: plan.retry_count,
            failover_count: plan.failover_count,
            resource_outcome: "preparing".into(),
            row_count: None,
            reason: None,
        }
    }
}

/// One idempotent cancellation result for the HTTP route.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReadCancellation {
    pub summary: ReadExecutionSummary,
    /// Number of registered peer cleanup deliveries made by this call. It is
    /// zero for repeated cancellation and for an already-finished result.
    pub peer_cleanup_deliveries: usize,
    pub already_terminal: bool,
}

/// Coordinator failure type. It intentionally separates retryability from a
/// terminal failure before any result row can become visible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadExecutionFailure {
    Retryable(String),
    Terminal(String),
}

/// A cancellation delivery registered before a peer range request is sent.
/// The shared execution ID is passed to the transport-specific cancellation
/// implementation, which may in turn call `RangeReadWorker::cancel`.
pub type PeerCancellation = Arc<dyn Fn(&ReadExecutionId) + Send + Sync>;

/// Server-owned coordinator registry. It is deliberately independent of the
/// HTTP connection: timeout, disconnect, and explicit cancel all use the same
/// idempotent transition and invoke peer cleanup outside its entry lock.
#[derive(Clone, Default)]
pub struct DistributedReadRegistry {
    entries: Arc<DashMap<ReadExecutionId, Arc<Mutex<ReadExecutionEntry>>>>,
}

struct ReadExecutionEntry {
    owner: ReadExecutionOwner,
    summary: ReadExecutionSummary,
    prepared: Option<PreparedResult>,
    peer_cancellations: Vec<PeerCancellation>,
    updated_at: SystemTime,
}

/// A prepared stream lease. Dropping a partially consumed HTTP response uses
/// the same registry cancellation path as `POST .../cancel`.
pub struct PreparedReadLease {
    registry: DistributedReadRegistry,
    execution_id: ReadExecutionId,
    entry: Arc<Mutex<ReadExecutionEntry>>,
    stream: PreparedResultStream,
    closed: bool,
}

impl PreparedReadLease {
    pub fn columns(&self) -> Vec<alopex_sql::executor::ColumnInfo> {
        let entry = lock_entry(&self.entry);
        entry
            .prepared
            .as_ref()
            .map(|result| result.columns().to_vec())
            .unwrap_or_default()
    }

    /// Open a second immutable cursor for response-size preflight. It never
    /// exposes rows to a caller and leaves the delivery cursor untouched.
    pub fn preview_stream(&self) -> Option<PreparedResultStream> {
        let entry = lock_entry(&self.entry);
        (entry.summary.outcome == ReadExecutionOutcome::Success)
            .then(|| {
                entry
                    .prepared
                    .as_ref()
                    .map(PreparedResult::open_prepared_stream)
            })
            .flatten()
    }

    /// Returns a row only while the registered execution remains successful.
    /// A cancellation that arrives between HTTP body polls is observed before
    /// the next row is made available to the response stream.
    pub fn next_row(&mut self) -> Option<Vec<alopex_sql::storage::SqlValue>> {
        if self.summary().outcome != ReadExecutionOutcome::Success {
            return None;
        }
        self.stream.next_row()
    }

    pub fn summary(&self) -> ReadExecutionSummary {
        lock_entry(&self.entry).summary.clone()
    }

    /// Discard coordinator-owned result state after the terminal summary was
    /// delivered successfully. A small terminal tombstone remains only until
    /// registry cleanup, keeping repeated cancellation idempotent.
    pub fn finish(mut self) {
        self.registry
            .finish_delivery(&self.execution_id, &self.entry);
        self.closed = true;
    }
}

impl Drop for PreparedReadLease {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.registry.cancel_unchecked(&self.execution_id);
        }
    }
}

impl DistributedReadRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register before dispatching any worker request. Supplying the planner's
    /// `request_id` makes that one ID the coordinator/worker/HTTP correlation
    /// key required by the distributed-read protocol.
    pub fn register_with_id(
        &self,
        execution_id: ReadExecutionId,
        owner: ReadExecutionOwner,
        plan: ReadExecutionPlanSummary,
        peer_cancellations: Vec<PeerCancellation>,
    ) -> Result<()> {
        plan.validate()?;
        if self.entries.contains_key(&execution_id) {
            return Err(ServerError::Conflict(format!(
                "distributed read execution '{}' is already registered",
                execution_id.as_str()
            )));
        }
        let entry = Arc::new(Mutex::new(ReadExecutionEntry {
            owner,
            summary: ReadExecutionSummary::preparing(execution_id.clone(), plan),
            prepared: None,
            peer_cancellations,
            updated_at: SystemTime::now(),
        }));
        if self.entries.insert(execution_id.clone(), entry).is_some() {
            return Err(ServerError::Conflict(format!(
                "distributed read execution '{}' is already registered",
                execution_id.as_str()
            )));
        }
        Ok(())
    }

    /// Allocate a fresh request ID for a coordinator that does not already
    /// have one from route planning.
    pub fn register(
        &self,
        owner: ReadExecutionOwner,
        plan: ReadExecutionPlanSummary,
        peer_cancellations: Vec<PeerCancellation>,
    ) -> Result<ReadExecutionId> {
        let execution_id = RequestId::new(Uuid::new_v4().to_string());
        self.register_with_id(execution_id.clone(), owner, plan, peer_cancellations)?;
        Ok(execution_id)
    }

    /// Publish only P2.11's immutable prepared result. This is the sole
    /// transition that permits the HTTP adapter to open a row stream.
    pub fn publish_prepared(
        &self,
        execution_id: &ReadExecutionId,
        prepared: PreparedResult,
    ) -> Result<ReadExecutionSummary> {
        let entry = self.entry(execution_id)?;
        let mut entry = lock_entry(&entry);
        match entry.summary.outcome {
            ReadExecutionOutcome::Preparing => {
                entry.summary.outcome = ReadExecutionOutcome::Success;
                entry.summary.resource_outcome = "prepared".into();
                entry.summary.row_count = Some(prepared.row_count());
                entry.summary.reason = None;
                entry.prepared = Some(prepared);
                entry.updated_at = SystemTime::now();
                Ok(entry.summary.clone())
            }
            ReadExecutionOutcome::Cancelled => Err(ServerError::Conflict(
                "distributed read was cancelled before preparation completed".into(),
            )),
            _ => Err(ServerError::Conflict(
                "distributed read no longer accepts a prepared result".into(),
            )),
        }
    }

    /// Record a classified coordinator failure and cancel every registered
    /// peer exactly once. No prepared rows survive this transition.
    pub fn fail(
        &self,
        execution_id: &ReadExecutionId,
        failure: ReadExecutionFailure,
    ) -> Result<ReadExecutionSummary> {
        let entry = self.entry(execution_id)?;
        let (summary, callbacks) = {
            let mut entry = lock_entry(&entry);
            if !matches!(
                entry.summary.outcome,
                ReadExecutionOutcome::Preparing | ReadExecutionOutcome::Success
            ) {
                return Ok(entry.summary.clone());
            }
            let (outcome, reason) = match failure {
                ReadExecutionFailure::Retryable(reason) => {
                    (ReadExecutionOutcome::RetryableFailure, reason)
                }
                ReadExecutionFailure::Terminal(reason) => {
                    (ReadExecutionOutcome::TerminalFailure, reason)
                }
            };
            entry.summary.outcome = outcome;
            entry.summary.resource_outcome = "released".into();
            entry.summary.reason = Some(reason);
            entry.prepared = None;
            entry.updated_at = SystemTime::now();
            let callbacks = std::mem::take(&mut entry.peer_cancellations);
            (entry.summary.clone(), callbacks)
        };
        deliver_cancellations(execution_id, callbacks);
        Ok(summary)
    }

    pub fn cancel(
        &self,
        execution_id: &ReadExecutionId,
        requester_profile: Option<&str>,
    ) -> Result<ReadCancellation> {
        let entry = self.entry(execution_id)?;
        {
            let entry = lock_entry(&entry);
            if !entry.owner.permits(requester_profile) {
                return Err(ServerError::Unauthorized(
                    "distributed read belongs to a different authenticated profile".into(),
                ));
            }
        }
        Ok(self.cancel_entry(execution_id, &entry))
    }

    pub fn summary(
        &self,
        execution_id: &ReadExecutionId,
        requester_profile: Option<&str>,
    ) -> Result<ReadExecutionSummary> {
        let entry = self.entry(execution_id)?;
        let entry = lock_entry(&entry);
        if !entry.owner.permits(requester_profile) {
            return Err(ServerError::Unauthorized(
                "distributed read belongs to a different authenticated profile".into(),
            ));
        }
        Ok(entry.summary.clone())
    }

    /// Obtain a read-only lease only after P2.11 preparation succeeded.
    pub fn open_prepared(
        &self,
        execution_id: &ReadExecutionId,
        requester_profile: Option<&str>,
    ) -> Result<PreparedReadLease> {
        let entry = self.entry(execution_id)?;
        let prepared = {
            let entry = lock_entry(&entry);
            if !entry.owner.permits(requester_profile) {
                return Err(ServerError::Unauthorized(
                    "distributed read belongs to a different authenticated profile".into(),
                ));
            }
            if entry.summary.outcome != ReadExecutionOutcome::Success {
                return Err(ServerError::Conflict(format!(
                    "distributed read is {:?}, not prepared",
                    entry.summary.outcome
                )));
            }
            entry.prepared.clone().ok_or_else(|| {
                ServerError::Conflict("distributed read result has already been released".into())
            })?
        };
        Ok(PreparedReadLease {
            registry: self.clone(),
            execution_id: execution_id.clone(),
            entry,
            stream: prepared.open_prepared_stream(),
            closed: false,
        })
    }

    /// Remove only small, terminal tombstones and never a live preparation or
    /// a retained prepared result. The server calls this with session TTL.
    pub fn cleanup_terminal_before(&self, cutoff: SystemTime) {
        let removable = self
            .entries
            .iter()
            .filter_map(|entry| {
                let state = lock_entry(entry.value());
                (state.updated_at <= cutoff && state.prepared.is_none())
                    .then(|| entry.key().clone())
            })
            .collect::<Vec<_>>();
        for execution_id in removable {
            self.entries.remove(&execution_id);
        }
    }

    pub fn cleanup_after(&self, ttl: Duration) {
        let cutoff = SystemTime::now()
            .checked_sub(ttl)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        self.cleanup_terminal_before(cutoff);
    }

    #[cfg(test)]
    fn active_count(&self) -> usize {
        self.entries.len()
    }

    fn entry(&self, execution_id: &ReadExecutionId) -> Result<Arc<Mutex<ReadExecutionEntry>>> {
        self.entries
            .get(execution_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                ServerError::NotFound(format!(
                    "distributed read execution '{}' was not found",
                    execution_id.as_str()
                ))
            })
    }

    fn cancel_unchecked(&self, execution_id: &ReadExecutionId) -> Result<ReadCancellation> {
        let entry = self.entry(execution_id)?;
        Ok(self.cancel_entry(execution_id, &entry))
    }

    fn cancel_entry(
        &self,
        execution_id: &ReadExecutionId,
        entry: &Arc<Mutex<ReadExecutionEntry>>,
    ) -> ReadCancellation {
        let (summary, callbacks, already_terminal) = {
            let mut entry = lock_entry(entry);
            if !matches!(
                entry.summary.outcome,
                ReadExecutionOutcome::Preparing | ReadExecutionOutcome::Success
            ) || entry.prepared.is_none()
                && entry.summary.outcome == ReadExecutionOutcome::Success
            {
                (entry.summary.clone(), Vec::new(), true)
            } else {
                entry.summary.outcome = ReadExecutionOutcome::Cancelled;
                entry.summary.resource_outcome = "released".into();
                entry.summary.reason = Some("cancelled".into());
                entry.prepared = None;
                entry.updated_at = SystemTime::now();
                let callbacks = std::mem::take(&mut entry.peer_cancellations);
                (entry.summary.clone(), callbacks, false)
            }
        };
        let peer_cleanup_deliveries = callbacks.len();
        deliver_cancellations(execution_id, callbacks);
        ReadCancellation {
            summary,
            peer_cleanup_deliveries,
            already_terminal,
        }
    }

    fn finish_delivery(
        &self,
        _execution_id: &ReadExecutionId,
        entry: &Arc<Mutex<ReadExecutionEntry>>,
    ) {
        let mut entry = lock_entry(entry);
        if entry.summary.outcome == ReadExecutionOutcome::Success {
            entry.prepared = None;
            entry.peer_cancellations.clear();
            entry.summary.resource_outcome = "released".into();
            entry.updated_at = SystemTime::now();
        }
    }
}

fn lock_entry(entry: &Arc<Mutex<ReadExecutionEntry>>) -> MutexGuard<'_, ReadExecutionEntry> {
    entry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn deliver_cancellations(execution_id: &ReadExecutionId, callbacks: Vec<PeerCancellation>) {
    for callback in callbacks {
        callback(execution_id);
    }
}

/// Injects the configured server-local read policy at every worker boundary.
#[derive(Clone)]
pub struct RemoteReadWorkerAuthorizer {
    local_authorizer: Arc<dyn LocalReadAuthorizationRecheck>,
}

impl RemoteReadWorkerAuthorizer {
    /// Create a worker authorizer from the exact policy used for local reads.
    pub fn new(local_authorizer: Arc<dyn LocalReadAuthorizationRecheck>) -> Self {
        Self { local_authorizer }
    }

    /// Verify the transport delegation and locally recheck the end-user's
    /// permission before any worker storage/session operation occurs.
    pub fn authorize(
        &self,
        peer: VerifiedPeerIdentity,
        envelope: &RemoteReadAuthorizationEnvelope,
        now_ms: u64,
        verifier: &dyn ReadDelegationVerifier,
    ) -> Result<()> {
        authorize_remote_read(
            peer,
            envelope,
            now_ms,
            verifier,
            self.local_authorizer.as_ref(),
        )
        .map_err(|error| {
            ServerError::Unauthorized(format!("remote read delegation rejected: {error}"))
        })
    }

    /// Authorize a delegation and create a new subject-bound worker session.
    ///
    /// A rejected scope never creates a session, so a later worker cannot use
    /// a broader authority than the corresponding local read.
    pub async fn begin_authorized_worker_session(
        &self,
        sessions: &SessionManager,
        peer: VerifiedPeerIdentity,
        envelope: &RemoteReadAuthorizationEnvelope,
        now_ms: u64,
        verifier: &dyn ReadDelegationVerifier,
    ) -> Result<SessionId> {
        self.authorize(peer, envelope, now_ms, verifier)?;
        sessions
            .create_authenticated_session(envelope.credential.subject.clone())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use alopex_cluster::{
        AuthenticatedSubject, ClusterId, LocalReadAuthorizationRequest, NodeId, RangeId,
        ReadDelegationCredential, ReadOperationScope, RequestId,
    };
    use alopex_core::ReadAtPoint;
    use alopex_sql::distributed_read::{
        AssemblerRow, AssemblyPlan, DistributedReadBudget, GlobalResultAssembler,
        RangeAssemblerInput, RangeAssemblerPayload, RangeTerminal, ResultPresentation,
        RowMergePlan,
    };
    use alopex_sql::executor::ColumnInfo;
    use alopex_sql::planner::ResolvedType;
    use alopex_sql::storage::SqlValue;
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::auth::{LocalReadAuthorizationPolicy, ServerLocalReadAuthorizationRecheck};
    use crate::session::{SessionConfig, TransactionFactory};

    struct DigestVerifier;

    impl ReadDelegationVerifier for DigestVerifier {
        fn verify(&self, _key_id: &str, payload: &[u8], signature: &[u8]) -> bool {
            signature == Sha256::digest(payload).as_slice()
        }
    }

    struct TableSevenPolicy;

    impl LocalReadAuthorizationPolicy for TableSevenPolicy {
        fn authorize_local_read(
            &self,
            request: &LocalReadAuthorizationRequest,
        ) -> std::result::Result<(), String> {
            (request.subject.as_str() == "user-a"
                && request.table_id == 7
                && request.range_id == RangeId::new("range-a"))
            .then_some(())
            .ok_or_else(|| "not permitted by local table/range policy".into())
        }
    }

    fn envelope() -> RemoteReadAuthorizationEnvelope {
        let mut credential = ReadDelegationCredential {
            issuer: NodeId::new("gateway-a"),
            cluster_id: ClusterId::new("cluster-a"),
            subject: AuthenticatedSubject::new("user-a"),
            operation: ReadOperationScope::Select,
            table_id: 7,
            allowed_ranges: BTreeSet::from([RangeId::new("range-a")]),
            query_digest: "query-a".into(),
            request_id: RequestId::new("request-a"),
            read_fence_digest: "fence-a".into(),
            audience: NodeId::new("gateway-a"),
            read_at: ReadAtPoint::new(4, 3, 2, 1),
            issued_at_ms: 10,
            expires_at_ms: 20,
            key_id: "test-key".into(),
            signature: Vec::new(),
        };
        credential.signature = Sha256::digest(credential.signed_payload().unwrap()).to_vec();
        RemoteReadAuthorizationEnvelope {
            range_id: RangeId::new("range-a"),
            table_id: 7,
            operation: ReadOperationScope::Select,
            request_id: RequestId::new("request-a"),
            query_digest: "query-a".into(),
            read_fence_digest: "fence-a".into(),
            read_at: ReadAtPoint::new(4, 3, 2, 1),
            credential,
        }
    }

    fn authorizer() -> RemoteReadWorkerAuthorizer {
        RemoteReadWorkerAuthorizer::new(Arc::new(ServerLocalReadAuthorizationRecheck::new(
            Arc::new(TableSevenPolicy),
        )))
    }

    fn peer() -> VerifiedPeerIdentity {
        VerifiedPeerIdentity::new("gateway-a", "cluster-a")
    }

    fn session_manager() -> SessionManager {
        let factory: TransactionFactory = Arc::new(|| {
            Box::pin(async {
                Err(ServerError::Internal(
                    "test factory must not create a transaction".into(),
                ))
            })
        });
        SessionManager::new(
            SessionConfig {
                ttl: Duration::from_secs(60),
            },
            factory,
        )
    }

    fn execution_plan() -> ReadExecutionPlanSummary {
        ReadExecutionPlanSummary {
            requested_mode: "strong".into(),
            effective_mode: "strong".into(),
            metadata_version: 41,
            ranges: vec!["range-a".into()],
            freshness: "current_committed_prefix".into(),
            retry_count: 0,
            failover_count: 0,
        }
    }

    fn execution_owner() -> ReadExecutionOwner {
        ReadExecutionOwner::new("profile-a", None).unwrap()
    }

    fn prepared_result() -> PreparedResult {
        let columns = vec![ColumnInfo::new("name", ResolvedType::Text)];
        let plan = AssemblyPlan::Rows(RowMergePlan {
            presentation: ResultPresentation {
                columns: columns.clone(),
                distinct: false,
                order: Vec::new(),
                final_order_key_indexes: Vec::new(),
                offset: 0,
                limit: None,
            },
        });
        let mut assembler = GlobalResultAssembler::new(
            vec!["range-a".into()],
            plan,
            DistributedReadBudget::default(),
        )
        .unwrap();
        assembler
            .push_range(RangeAssemblerInput {
                range_id: "range-a".into(),
                columns,
                payloads: vec![RangeAssemblerPayload::Rows(vec![AssemblerRow {
                    values: vec![SqlValue::Text("prepared".into())],
                    order_keys: Vec::new(),
                    row_key: 1,
                }])],
                terminal: RangeTerminal::Completed {
                    cleanup_acknowledged: true,
                },
            })
            .unwrap();
        assembler.prepare().unwrap()
    }

    #[tokio::test]
    async fn worker_injection_creates_a_session_bound_to_the_locally_allowed_subject() {
        let sessions = session_manager();
        let session = authorizer()
            .begin_authorized_worker_session(&sessions, peer(), &envelope(), 15, &DigestVerifier)
            .await
            .unwrap();
        assert_eq!(
            sessions
                .authenticated_subject(&session)
                .await
                .unwrap()
                .as_str(),
            "user-a"
        );
    }

    #[tokio::test]
    async fn scope_tamper_is_rejected_without_creating_a_worker_session() {
        let sessions = session_manager();
        let mut tampered = envelope();
        tampered.range_id = RangeId::new("range-b");
        let result = authorizer()
            .begin_authorized_worker_session(&sessions, peer(), &tampered, 15, &DigestVerifier)
            .await;
        assert!(matches!(result, Err(ServerError::Unauthorized(_))));
        assert_eq!(sessions.active_session_count(), 0);
    }

    #[tokio::test]
    async fn valid_transport_cannot_broaden_a_locally_denied_table_scope() {
        let sessions = session_manager();
        let mut denied = envelope();
        denied.credential.table_id = 8;
        denied.table_id = 8;
        denied.credential.signature =
            Sha256::digest(denied.credential.signed_payload().unwrap()).to_vec();
        let result = authorizer()
            .begin_authorized_worker_session(&sessions, peer(), &denied, 15, &DigestVerifier)
            .await;
        assert!(matches!(result, Err(ServerError::Unauthorized(_))));
        assert_eq!(sessions.active_session_count(), 0);
    }

    #[test]
    fn cancellation_is_owner_bound_idempotent_and_delivered_once_outside_registry_state() {
        let registry = DistributedReadRegistry::new();
        let execution_id = RequestId::new("read-cancel");
        let deliveries = Arc::new(AtomicUsize::new(0));
        let observed_id = Arc::new(std::sync::Mutex::new(None));
        let callback_deliveries = deliveries.clone();
        let callback_id = observed_id.clone();
        registry
            .register_with_id(
                execution_id.clone(),
                execution_owner(),
                execution_plan(),
                vec![Arc::new(move |id| {
                    callback_deliveries.fetch_add(1, Ordering::SeqCst);
                    *callback_id.lock().unwrap() = Some(id.clone());
                })],
            )
            .unwrap();

        assert!(matches!(
            registry.cancel(&execution_id, Some("profile-b")),
            Err(ServerError::Unauthorized(_))
        ));
        assert_eq!(deliveries.load(Ordering::SeqCst), 0);

        let cancelled = registry.cancel(&execution_id, Some("profile-a")).unwrap();
        assert_eq!(cancelled.summary.outcome, ReadExecutionOutcome::Cancelled);
        assert_eq!(cancelled.peer_cleanup_deliveries, 1);
        assert!(!cancelled.already_terminal);
        assert_eq!(deliveries.load(Ordering::SeqCst), 1);
        assert_eq!(observed_id.lock().unwrap().as_ref(), Some(&execution_id));

        let repeated = registry.cancel(&execution_id, Some("profile-a")).unwrap();
        assert_eq!(repeated.summary.outcome, ReadExecutionOutcome::Cancelled);
        assert_eq!(repeated.peer_cleanup_deliveries, 0);
        assert!(repeated.already_terminal);
        assert_eq!(deliveries.load(Ordering::SeqCst), 1);
        assert!(matches!(
            registry.open_prepared(&execution_id, Some("profile-a")),
            Err(ServerError::Conflict(_))
        ));
    }

    #[test]
    fn only_prepared_results_open_and_disconnect_uses_the_same_peer_cleanup_once() {
        let registry = DistributedReadRegistry::new();
        let execution_id = RequestId::new("read-prepared");
        let deliveries = Arc::new(AtomicUsize::new(0));
        let callback_deliveries = deliveries.clone();
        registry
            .register_with_id(
                execution_id.clone(),
                execution_owner(),
                execution_plan(),
                vec![Arc::new(move |_| {
                    callback_deliveries.fetch_add(1, Ordering::SeqCst);
                })],
            )
            .unwrap();

        assert!(matches!(
            registry.open_prepared(&execution_id, Some("profile-a")),
            Err(ServerError::Conflict(_))
        ));
        let summary = registry
            .publish_prepared(&execution_id, prepared_result())
            .unwrap();
        assert_eq!(summary.outcome, ReadExecutionOutcome::Success);
        assert_eq!(summary.row_count, Some(1));

        let mut lease = registry
            .open_prepared(&execution_id, Some("profile-a"))
            .unwrap();
        assert_eq!(
            lease.next_row(),
            Some(vec![SqlValue::Text("prepared".into())])
        );
        // Simulate client disconnect before the HTTP terminal summary.
        drop(lease);

        assert_eq!(deliveries.load(Ordering::SeqCst), 1);
        assert_eq!(
            registry
                .summary(&execution_id, Some("profile-a"))
                .unwrap()
                .outcome,
            ReadExecutionOutcome::Cancelled
        );
        registry.cleanup_terminal_before(SystemTime::now());
        assert_eq!(registry.active_count(), 0);
    }
}
