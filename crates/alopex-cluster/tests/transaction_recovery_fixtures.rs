//! Fixed F4 recovery, retry, and in-doubt transaction fixtures.

use std::collections::{BTreeMap, VecDeque};

use alopex_cluster::{
    ClusterReadPoint, FailureClass, NodeId, OperationState, Placement, PlacementReadiness,
    PlacementRole, RangeIdentity, ReadConsistencyMode, RequestId, RoutingOutcome,
    RoutingOutcomeKind, TransactionAdmissionError, TransactionAdmissionVerifier,
    TransactionCoordinator, TransactionCoordinatorError, TransactionDecision, TransactionIntent,
    TransactionParticipant, TransactionParticipantAck, TransactionParticipantDriver,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Manifest {
    schema_version: u32,
    fixtures: Vec<Fixture>,
}

#[derive(Debug, Deserialize)]
struct Fixture {
    id: String,
    transaction_id: String,
    request_id: String,
    expected_state: String,
    coverage: Vec<String>,
}

fn manifest() -> Manifest {
    serde_json::from_str(include_str!("../../../tests/fixtures/f4_recovery.json"))
        .expect("F4 recovery manifest must be valid JSON")
}

fn fixture<'a>(manifest: &'a Manifest, id: &str) -> &'a Fixture {
    manifest
        .fixtures
        .iter()
        .find(|fixture| fixture.id == id)
        .unwrap_or_else(|| panic!("missing fixture {id}"))
}

#[derive(Debug, Clone, Copy)]
struct FixtureVerifier {
    rejection: Option<TransactionAdmissionError>,
}

impl FixtureVerifier {
    const fn permit() -> Self {
        Self { rejection: None }
    }

    const fn reject(rejection: TransactionAdmissionError) -> Self {
        Self {
            rejection: Some(rejection),
        }
    }
}

impl TransactionAdmissionVerifier for FixtureVerifier {
    fn verify(&self, _intent: &TransactionIntent) -> Result<(), TransactionAdmissionError> {
        self.rejection.map_or(Ok(()), Err)
    }
}

#[derive(Default)]
struct Driver {
    prepares: VecDeque<TransactionParticipantAck>,
    discards: VecDeque<TransactionParticipantAck>,
    commits: VecDeque<TransactionParticipantAck>,
    statuses: VecDeque<TransactionParticipantAck>,
    prepare_calls: usize,
    discard_calls: usize,
    commit_calls: usize,
    abort_calls: usize,
    status_calls: usize,
}

impl Driver {
    fn next(queue: &mut VecDeque<TransactionParticipantAck>) -> TransactionParticipantAck {
        queue
            .pop_front()
            .unwrap_or(TransactionParticipantAck::Durable)
    }
}

impl TransactionParticipantDriver for Driver {
    fn prepare(
        &mut self,
        _intent: &TransactionIntent,
        _participant: &TransactionParticipant,
    ) -> TransactionParticipantAck {
        self.prepare_calls += 1;
        Self::next(&mut self.prepares)
    }

    fn discard(
        &mut self,
        _intent: &TransactionIntent,
        _participant: &TransactionParticipant,
    ) -> TransactionParticipantAck {
        self.discard_calls += 1;
        Self::next(&mut self.discards)
    }

    fn apply_decision(
        &mut self,
        _intent: &TransactionIntent,
        _participant: &TransactionParticipant,
        decision: TransactionDecision,
    ) -> TransactionParticipantAck {
        match decision {
            TransactionDecision::Commit => {
                self.commit_calls += 1;
                Self::next(&mut self.commits)
            }
            TransactionDecision::Abort => {
                self.abort_calls += 1;
                TransactionParticipantAck::Durable
            }
        }
    }

    fn decision_status(
        &mut self,
        _intent: &TransactionIntent,
        _participant: &TransactionParticipant,
        _decision: TransactionDecision,
    ) -> TransactionParticipantAck {
        self.status_calls += 1;
        Self::next(&mut self.statuses)
    }
}

fn intent(fixture: &Fixture) -> TransactionIntent {
    let participants = ["range-a", "range-b"]
        .into_iter()
        .map(|range_id| TransactionParticipant {
            range: RangeIdentity::new("cluster-f4", 7, range_id, None, None, 3, 11),
            range_generation: 1,
            placement: Placement::new(
                "node-a",
                Vec::new(),
                PlacementRole::Owner,
                PlacementReadiness::Ready,
                0,
            ),
        })
        .collect::<Vec<_>>();
    TransactionIntent {
        transaction_id: fixture.transaction_id.clone(),
        request_id: RequestId::from(fixture.request_id.as_str()),
        request_fingerprint: format!("f4-recovery:{}", fixture.id),
        actor: NodeId::from("actor-a"),
        participants,
        read_point: ClusterReadPoint {
            data_epoch: 11,
            metadata_version: 0,
            schema_manifest_id: None,
            range_generations: BTreeMap::from([("range-a".into(), 1), ("range-b".into(), 1)]),
            index_epochs: BTreeMap::new(),
            consistency: ReadConsistencyMode::Strong,
        },
        schema_version: 3,
        data_epoch: 11,
        routing: RoutingOutcome::new(
            RoutingOutcomeKind::MultiRange,
            None,
            0,
            "f4_recovery_multi_range_route",
        ),
    }
}

fn coordinator(
    store: alopex_core::MemoryKV,
    verifier: FixtureVerifier,
) -> TransactionCoordinator<alopex_core::MemoryKV, FixtureVerifier> {
    TransactionCoordinator::with_verifier(store, verifier)
}

#[test]
fn manifest_register_is_exact_and_covers_every_recovery_trace() {
    let manifest = manifest();
    assert_eq!(manifest.schema_version, 1);
    assert_eq!(manifest.fixtures.len(), 15);
    for coverage in [
        "first",
        "second_duplicate",
        "post_restart_duplicate",
        "timeout",
        "conflict",
        "stale_metadata",
        "not_leader",
        "node_loss",
        "in_doubt_decision",
        "status_lookup",
        "recover_lookup",
        "range_split",
        "range_merge",
        "range_move",
        "restart",
        "reconnect",
        "unauthorized",
        "prerequisite_missing",
        "backpressure",
        "cancellation",
    ] {
        assert!(
            manifest
                .fixtures
                .iter()
                .any(|fixture| { fixture.coverage.iter().any(|actual| actual == coverage) })
        );
    }
}

#[test]
fn duplicate_retry_and_recreated_coordinator_preserve_the_first_outcome() {
    let manifest = manifest();
    let duplicate_fixture = fixture(&manifest, "F4-RCV-DUP-01");
    let request = intent(duplicate_fixture);
    let store = alopex_core::MemoryKV::new();
    let first = coordinator(store.clone(), FixtureVerifier::permit());
    let mut first_driver = Driver::default();
    assert_eq!(
        first.execute(&request, &mut first_driver).unwrap().state,
        OperationState::Committed
    );
    assert_eq!(first_driver.prepare_calls, 2);
    assert_eq!(first_driver.commit_calls, 2);

    let mut duplicate_driver = Driver::default();
    let duplicate = first.execute(&request, &mut duplicate_driver).unwrap();
    assert_eq!(duplicate.state, OperationState::Committed);
    assert_eq!(duplicate.idempotency.duplicate_count, 1);
    assert_eq!(duplicate_driver.prepare_calls, 0);
    drop(first);

    let restarted = coordinator(store, FixtureVerifier::permit());
    let mut restarted_driver = Driver::default();
    let replay = restarted.execute(&request, &mut restarted_driver).unwrap();
    assert_eq!(duplicate_fixture.expected_state, "committed");
    assert_eq!(replay.state, OperationState::Committed);
    assert_eq!(replay.idempotency.duplicate_count, 2);
    assert_eq!(restarted_driver.prepare_calls, 0);
    assert_eq!(restarted_driver.commit_calls, 0);

    let timeout_fixture = fixture(&manifest, "F4-RCV-TIMEOUT-01");
    let timeout_request = intent(timeout_fixture);
    let retry_coordinator = coordinator(alopex_core::MemoryKV::new(), FixtureVerifier::permit());
    let mut retry_driver = Driver {
        prepares: VecDeque::from([TransactionParticipantAck::Rejected {
            failure_class: FailureClass::Timeout,
            reason_code: "f4_timeout".to_owned(),
            retryable: true,
        }]),
        ..Driver::default()
    };
    assert_eq!(
        retry_coordinator
            .execute(&timeout_request, &mut retry_driver)
            .unwrap()
            .state,
        OperationState::RetryableFailure
    );
    let retried = retry_coordinator
        .execute(&timeout_request, &mut retry_driver)
        .unwrap();
    assert_eq!(timeout_fixture.expected_state, "retryable_failure");
    assert_eq!(retried.state, OperationState::Committed);
    assert_eq!(retried.idempotency.duplicate_count, 1);
    assert_eq!(retry_driver.discard_calls, 2);
}

#[test]
fn in_doubt_status_and_recover_never_reapply_the_decision() {
    let manifest = manifest();
    let fixture = fixture(&manifest, "F4-RCV-IN-DOUBT-01");
    let request = intent(fixture);
    let coordinator = coordinator(alopex_core::MemoryKV::new(), FixtureVerifier::permit());
    let mut driver = Driver {
        commits: VecDeque::from([
            TransactionParticipantAck::Durable,
            TransactionParticipantAck::RecoveryPending {
                failure_class: FailureClass::NodeUnavailable,
                reason_code: "f4_in_doubt_commit_ack".to_owned(),
            },
        ]),
        statuses: VecDeque::from([TransactionParticipantAck::Durable]),
        ..Driver::default()
    };
    let pending = coordinator.execute(&request, &mut driver).unwrap();
    assert_eq!(fixture.expected_state, "recovery_pending");
    assert_eq!(pending.state, OperationState::RecoveryPending);
    let writes = (
        driver.prepare_calls,
        driver.commit_calls,
        driver.abort_calls,
    );
    assert_eq!(
        coordinator.status(&request).unwrap().unwrap().state,
        OperationState::RecoveryPending
    );
    let recovered = coordinator
        .recover(&request, &mut driver)
        .unwrap()
        .expect("durable recovery record");
    assert_eq!(recovered.state, OperationState::Committed);
    assert_eq!(
        writes,
        (
            driver.prepare_calls,
            driver.commit_calls,
            driver.abort_calls
        )
    );
    assert_eq!(driver.status_calls, 1);
}

#[test]
fn fixed_admission_lifecycle_failures_do_not_start_participant_io() {
    let manifest = manifest();
    let cases = [
        ("F4-RCV-STALE-01", TransactionAdmissionError::StaleMetadata),
        (
            "F4-RCV-NOT-LEADER-01",
            TransactionAdmissionError::OwnershipMismatch,
        ),
        (
            "F4-RCV-NODE-LOSS-01",
            TransactionAdmissionError::PrerequisiteMissing,
        ),
        ("F4-RCV-SPLIT-01", TransactionAdmissionError::StaleMetadata),
        ("F4-RCV-MERGE-01", TransactionAdmissionError::StaleMetadata),
        ("F4-RCV-MOVE-01", TransactionAdmissionError::StaleMetadata),
        (
            "F4-RCV-UNAUTHORIZED-01",
            TransactionAdmissionError::UnauthorizedActor,
        ),
        (
            "F4-RCV-PREREQUISITE-01",
            TransactionAdmissionError::PrerequisiteMissing,
        ),
    ];
    for (id, error) in cases {
        let request = intent(fixture(&manifest, id));
        let coordinator = coordinator(alopex_core::MemoryKV::new(), FixtureVerifier::reject(error));
        let mut driver = Driver::default();
        assert!(matches!(
            coordinator.execute(&request, &mut driver),
            Err(TransactionCoordinatorError::Admission(actual)) if actual == error
        ));
        assert_eq!(driver.prepare_calls, 0, "{id} must be pre-I/O");
        assert_eq!(driver.commit_calls, 0, "{id} must be pre-I/O");
    }
}

#[test]
fn conflict_and_backpressure_failures_never_claim_a_commit() {
    let manifest = manifest();
    for (id, ack, expected) in [
        (
            "F4-RCV-CONFLICT-01",
            TransactionParticipantAck::Rejected {
                failure_class: FailureClass::Conflict,
                reason_code: "f4_conflict".to_owned(),
                retryable: false,
            },
            OperationState::TerminalFailure,
        ),
        (
            "F4-RCV-BACKPRESSURE-01",
            TransactionParticipantAck::Rejected {
                failure_class: FailureClass::NodeUnavailable,
                reason_code: "f4_backpressure".to_owned(),
                retryable: true,
            },
            OperationState::RetryableFailure,
        ),
    ] {
        let fixture = fixture(&manifest, id);
        let request = intent(fixture);
        let coordinator = coordinator(alopex_core::MemoryKV::new(), FixtureVerifier::permit());
        let mut driver = Driver {
            prepares: VecDeque::from([ack]),
            ..Driver::default()
        };
        let outcome = coordinator.execute(&request, &mut driver).unwrap();
        assert_eq!(outcome.state, expected);
        assert_ne!(outcome.state, OperationState::Committed);
        assert_eq!(driver.commit_calls, 0);
    }
}
