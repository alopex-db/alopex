use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use alopex_core::{ReadAtPoint, RowKeyRange};
use alopex_sql::distributed_read::{
    REMOTE_READ_CATALOG_VERSION, RemoteReadDescriptor, RemoteReadOperators, RemoteReadShape,
};
use alopex_sql::executor::Row;
use alopex_sql::{RangeReadSnapshot, StorageRangeConstraint};
use sha2::{Digest, Sha256};

use super::*;
use crate::{ClusterId, NodeId, RangeId, VerifiedPeerIdentity};

#[derive(Default)]
struct TestClock(AtomicU64);

impl TestClock {
    fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }
}

impl RangeReadWorkerClock for TestClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

struct DigestVerifier;

impl ReadDelegationVerifier for DigestVerifier {
    fn verify(&self, _key_id: &str, payload: &[u8], signature: &[u8]) -> bool {
        signature == Sha256::digest(payload).as_slice()
    }
}

struct UserAPolicy;

impl LocalReadAuthorizationRecheck for UserAPolicy {
    fn authorize(&self, request: &LocalReadAuthorizationRequest) -> Result<(), String> {
        (request.subject.as_str() == "user-a" && request.table_id == 7)
            .then_some(())
            .ok_or_else(|| "denied by local policy".into())
    }
}

struct TestBackend {
    opened: AtomicUsize,
    cleanup: Arc<AtomicUsize>,
    cleanup_error: Option<String>,
    batches: Mutex<VecDeque<Result<Option<Vec<Row>>, String>>>,
    on_next_batch: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl TestBackend {
    fn new(batches: Vec<Result<Option<Vec<Row>>, String>>) -> Self {
        Self {
            opened: AtomicUsize::new(0),
            cleanup: Arc::new(AtomicUsize::new(0)),
            cleanup_error: None,
            batches: Mutex::new(batches.into()),
            on_next_batch: None,
        }
    }

    fn with_next_batch_hook(mut self, hook: Arc<dyn Fn() + Send + Sync>) -> Self {
        self.on_next_batch = Some(hook);
        self
    }

    fn with_cleanup_error(mut self, error: impl Into<String>) -> Self {
        self.cleanup_error = Some(error.into());
        self
    }
}

impl FencedRangeReadBackend for TestBackend {
    fn open_read_at(
        &self,
        request: &RemoteRangeReadRequest,
    ) -> Result<Box<dyn FencedRangeReadSession>, String> {
        assert_eq!(request.constraint.range_id(), "range-a");
        assert_eq!(
            request.constraint.snapshot().read_at(),
            request.authorization.read_at
        );
        self.opened.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TestSession {
            batches: std::mem::take(&mut *self.batches.lock().expect("test batches lock poisoned")),
            cleanup: Arc::clone(&self.cleanup),
            cleanup_error: self.cleanup_error.clone(),
            on_next_batch: self.on_next_batch.clone(),
        }))
    }
}

struct TestSession {
    batches: VecDeque<Result<Option<Vec<Row>>, String>>,
    cleanup: Arc<AtomicUsize>,
    cleanup_error: Option<String>,
    on_next_batch: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl FencedRangeReadSession for TestSession {
    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<Vec<Row>>, String> {
        if let Some(hook) = self.on_next_batch.take() {
            hook();
        }
        self.batches.pop_front().unwrap_or(Ok(None))
    }

    fn cleanup(self: Box<Self>) -> Result<(), String> {
        self.cleanup.fetch_add(1, Ordering::SeqCst);
        self.cleanup_error.map_or(Ok(()), Err)
    }
}

fn descriptor() -> RemoteReadDescriptor {
    RemoteReadDescriptor {
        catalog_version: REMOTE_READ_CATALOG_VERSION.into(),
        table: "users".into(),
        shape: RemoteReadShape::Rows,
        operators: RemoteReadOperators::default(),
    }
}

fn request(request_id: &str) -> RemoteRangeReadRequest {
    let point = ReadAtPoint::new(9, 2, 3, 4);
    let constraint = StorageRangeConstraint::new(
        "range-a",
        3,
        RowKeyRange::full_table(7),
        RangeReadSnapshot::new(point, "schema-3").unwrap(),
    )
    .unwrap();
    let descriptor = descriptor();
    let mut credential = ReadDelegationCredential {
        issuer: NodeId::new("gateway-a"),
        cluster_id: ClusterId::new("cluster-a"),
        subject: AuthenticatedSubject::new("user-a"),
        operation: ReadOperationScope::Select,
        table_id: 7,
        allowed_ranges: BTreeSet::from([RangeId::new("range-a")]),
        query_digest: descriptor_digest(&descriptor).unwrap(),
        request_id: crate::RequestId::new(request_id),
        read_fence_digest: range_fence_digest(&constraint).unwrap(),
        audience: NodeId::new("gateway-a"),
        read_at: point,
        issued_at_ms: 10,
        expires_at_ms: 30,
        key_id: "test-key".into(),
        signature: Vec::new(),
    };
    credential.signature = Sha256::digest(credential.signed_payload().unwrap()).to_vec();
    RemoteRangeReadRequest {
        authorization: RemoteReadAuthorizationEnvelope {
            range_id: RangeId::new("range-a"),
            table_id: 7,
            operation: ReadOperationScope::Select,
            request_id: crate::RequestId::new(request_id),
            query_digest: credential.query_digest.clone(),
            read_fence_digest: credential.read_fence_digest.clone(),
            read_at: point,
            credential,
        },
        descriptor,
        constraint,
        deadline_ms: 25,
    }
}

fn worker(clock: Arc<TestClock>) -> RangeReadWorker {
    RangeReadWorker::new(Arc::new(DigestVerifier), Arc::new(UserAPolicy), clock)
}

#[test]
fn zero_batch_limit_is_rejected_without_constructing_a_worker() {
    let clock = Arc::new(TestClock::default());
    assert!(matches!(
        RangeReadWorker::with_config(
            Arc::new(DigestVerifier),
            Arc::new(UserAPolicy),
            clock,
            RangeReadWorkerConfig { max_batch_rows: 0 },
        ),
        Err(RangeReadWorkerConfigError::ZeroBatchRows)
    ));
}

fn peer() -> VerifiedPeerIdentity {
    VerifiedPeerIdentity::new("gateway-a", "cluster-a")
}

#[test]
fn end_is_emitted_only_after_fenced_session_cleanup_acknowledges() {
    let clock = Arc::new(TestClock::default());
    clock.set(15);
    let worker = worker(Arc::clone(&clock));
    let backend = TestBackend::new(vec![Ok(Some(vec![Row::new(1, vec![])])), Ok(None)]);

    let execution = worker
        .execute(peer(), &request("read-a"), &backend)
        .unwrap();

    assert_eq!(execution.batches.len(), 1);
    assert_eq!(execution.end.row_count, 1);
    assert_eq!(execution.end.cleanup.request_id.as_str(), "read-a");
    assert_eq!(backend.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(worker.active_session_count(), 0);
}

#[test]
fn deadline_expiry_never_opens_a_snapshot() {
    let clock = Arc::new(TestClock::default());
    clock.set(25);
    let worker = worker(Arc::clone(&clock));
    let backend = TestBackend::new(vec![Ok(None)]);

    assert!(matches!(
        worker.execute(peer(), &request("read-deadline"), &backend),
        Err(RangeReadWorkerError::DeadlineElapsed)
    ));
    assert_eq!(backend.opened.load(Ordering::SeqCst), 0);
    assert_eq!(worker.active_session_count(), 0);
}

#[test]
fn deadline_after_open_cleans_the_session_and_later_request_recovers() {
    let clock = Arc::new(TestClock::default());
    clock.set(15);
    let worker = worker(Arc::clone(&clock));
    let deadline_clock = Arc::clone(&clock);
    let deadline_hook_clock = Arc::clone(&deadline_clock);
    let backend = TestBackend::new(vec![Ok(Some(vec![Row::new(1, vec![])])), Ok(None)])
        .with_next_batch_hook(Arc::new(move || deadline_hook_clock.set(25)));

    assert!(matches!(
        worker.execute(peer(), &request("read-deadline-after-open"), &backend),
        Err(RangeReadWorkerError::DeadlineElapsed)
    ));
    assert_eq!(backend.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(worker.active_session_count(), 0);

    deadline_clock.set(15);
    let recovery_backend = TestBackend::new(vec![Ok(None)]);
    assert!(
        worker
            .execute(
                peer(),
                &request("read-after-deadline-recovery"),
                &recovery_backend,
            )
            .is_ok()
    );
    assert_eq!(recovery_backend.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(worker.active_session_count(), 0);
}

#[test]
fn cleanup_failure_never_emits_end_or_leaks_the_active_session() {
    let clock = Arc::new(TestClock::default());
    clock.set(15);
    let worker = worker(Arc::clone(&clock));
    let backend =
        TestBackend::new(vec![Ok(None)]).with_cleanup_error("rollback did not acknowledge");

    assert!(matches!(
        worker.execute(peer(), &request("read-cleanup-failure"), &backend),
        Err(RangeReadWorkerError::CleanupFailed(reason)) if reason == "rollback did not acknowledge"
    ));
    assert_eq!(backend.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(worker.active_session_count(), 0);
}

#[test]
fn cancellation_cleans_one_session_and_later_request_recovers() {
    let clock = Arc::new(TestClock::default());
    clock.set(15);
    let worker = Arc::new(worker(Arc::clone(&clock)));
    let cancel_worker = Arc::clone(&worker);
    let cancel_request = crate::RequestId::new("read-cancelled");
    let backend = TestBackend::new(vec![Ok(Some(vec![Row::new(1, vec![])])), Ok(None)])
        .with_next_batch_hook(Arc::new(move || {
            assert!(cancel_worker.cancel(&cancel_request));
        }));

    assert!(matches!(
        worker.execute(peer(), &request("read-cancelled"), &backend),
        Err(RangeReadWorkerError::Cancelled)
    ));
    assert_eq!(backend.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(worker.active_session_count(), 0);

    let recovery_backend = TestBackend::new(vec![Ok(Some(vec![Row::new(2, vec![])])), Ok(None)]);
    let recovery = worker
        .execute(peer(), &request("read-independent"), &recovery_backend)
        .unwrap();
    assert_eq!(recovery.end.row_count, 1);
    assert_eq!(recovery_backend.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(worker.active_session_count(), 0);
}

#[test]
fn peer_disconnect_and_constraint_tamper_are_terminal_without_partial_output() {
    let clock = Arc::new(TestClock::default());
    clock.set(15);
    let worker = Arc::new(worker(Arc::clone(&clock)));
    let disconnect_worker = Arc::clone(&worker);
    let disconnect_request = crate::RequestId::new("read-disconnect");
    let backend = TestBackend::new(vec![Ok(Some(vec![Row::new(1, vec![])])), Ok(None)])
        .with_next_batch_hook(Arc::new(move || {
            assert!(disconnect_worker.peer_disconnected(&disconnect_request));
        }));
    assert!(matches!(
        worker.execute(peer(), &request("read-disconnect"), &backend),
        Err(RangeReadWorkerError::PeerDisconnected)
    ));
    assert_eq!(backend.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(worker.active_session_count(), 0);

    let recovery_backend = TestBackend::new(vec![Ok(None)]);
    assert!(
        worker
            .execute(
                peer(),
                &request("read-after-disconnect-recovery"),
                &recovery_backend,
            )
            .is_ok()
    );
    assert_eq!(recovery_backend.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(worker.active_session_count(), 0);

    let mut tampered = request("read-tampered");
    tampered.constraint = StorageRangeConstraint::new(
        "range-a",
        4,
        RowKeyRange::full_table(7),
        RangeReadSnapshot::new(ReadAtPoint::new(9, 2, 3, 4), "schema-3").unwrap(),
    )
    .unwrap();
    let never_opened = TestBackend::new(vec![Ok(None)]);
    assert!(matches!(
        worker.execute(peer(), &tampered, &never_opened),
        Err(RangeReadWorkerError::InvalidRequest(_))
    ));
    assert_eq!(never_opened.opened.load(Ordering::SeqCst), 0);
    assert_eq!(worker.active_session_count(), 0);

    let mut descriptor_tampered = request("read-descriptor-tampered");
    descriptor_tampered.descriptor.table = "other_table".into();
    let descriptor_never_opened = TestBackend::new(vec![Ok(None)]);
    assert!(matches!(
        worker.execute(peer(), &descriptor_tampered, &descriptor_never_opened),
        Err(RangeReadWorkerError::InvalidRequest(_))
    ));
    assert_eq!(descriptor_never_opened.opened.load(Ordering::SeqCst), 0);
    assert_eq!(worker.active_session_count(), 0);
}
