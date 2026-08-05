//! Deterministic F4 distributed transaction fixtures.
//!
//! These tests intentionally use a fixed driver rather than timing or process
//! scheduling. They exercise the coordinator boundary with fixed identities,
//! metadata epochs, and explicit participant acknowledgements.

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
struct FixtureManifest {
    schema_version: u32,
    fixtures: Vec<Fixture>,
}

#[derive(Debug, Deserialize)]
struct Fixture {
    id: String,
    transaction_id: String,
    request_id: String,
    ranges: Vec<String>,
    expected_state: String,
    coverage: Vec<String>,
}

fn manifest() -> FixtureManifest {
    serde_json::from_str(include_str!("../../../tests/fixtures/f4_transactions.json"))
        .expect("F4 fixture manifest must be valid JSON")
}

fn fixture<'a>(manifest: &'a FixtureManifest, id: &str) -> &'a Fixture {
    manifest
        .fixtures
        .iter()
        .find(|fixture| fixture.id == id)
        .unwrap_or_else(|| panic!("missing fixture {id}"))
}

#[derive(Default)]
struct Driver {
    prepares: VecDeque<TransactionParticipantAck>,
    discards: VecDeque<TransactionParticipantAck>,
    commits: VecDeque<TransactionParticipantAck>,
    aborts: VecDeque<TransactionParticipantAck>,
    prepare_calls: usize,
    discard_calls: usize,
    commit_calls: usize,
    abort_calls: usize,
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
                Self::next(&mut self.aborts)
            }
        }
    }

    fn decision_status(
        &mut self,
        _intent: &TransactionIntent,
        _participant: &TransactionParticipant,
        _decision: TransactionDecision,
    ) -> TransactionParticipantAck {
        TransactionParticipantAck::Durable
    }
}

/// Fixed fixture admission remains independent from the implementation's
/// committed-metadata verifier tests. The coordinator receives only an already
/// authenticated, immutable fence, so this adapter rejects any divergence
/// from that fixture's actor/epoch before participant I/O.
#[derive(Debug, Clone, Copy)]
struct FixtureVerifier {
    data_epoch: u64,
}

impl TransactionAdmissionVerifier for FixtureVerifier {
    fn verify(&self, intent: &TransactionIntent) -> Result<(), TransactionAdmissionError> {
        if intent.actor != NodeId::from("actor-a") {
            return Err(TransactionAdmissionError::UnauthorizedActor);
        }
        if intent.data_epoch != self.data_epoch || intent.read_point.data_epoch != self.data_epoch {
            return Err(TransactionAdmissionError::EpochMismatch);
        }
        Ok(())
    }
}

fn intent(fixture: &Fixture) -> TransactionIntent {
    let participants = fixture
        .ranges
        .iter()
        .map(|range_id| TransactionParticipant {
            range: RangeIdentity::new("cluster-f4", 7, range_id.as_str(), None, None, 3, 11),
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
    let range_generations = participants
        .iter()
        .map(|participant| {
            (
                participant.range.range_id.clone(),
                participant.range_generation,
            )
        })
        .collect();
    let routing = if participants.len() == 1 {
        RoutingOutcome::new(
            RoutingOutcomeKind::SingleRange,
            Some(participants[0].range.clone()),
            0,
            "f4_single_range_route",
        )
    } else {
        RoutingOutcome::new(
            RoutingOutcomeKind::MultiRange,
            None,
            0,
            "f4_multi_range_route",
        )
    };
    TransactionIntent {
        transaction_id: fixture.transaction_id.clone(),
        request_id: RequestId::from(fixture.request_id.as_str()),
        request_fingerprint: format!("authenticated:{}", fixture.id),
        actor: NodeId::from("actor-a"),
        participants,
        read_point: ClusterReadPoint {
            data_epoch: 11,
            metadata_version: 0,
            schema_manifest_id: None,
            range_generations,
            index_epochs: BTreeMap::new(),
            consistency: ReadConsistencyMode::Strong,
        },
        schema_version: 3,
        data_epoch: 11,
        routing,
    }
}

fn coordinator(
    request: &TransactionIntent,
) -> TransactionCoordinator<alopex_core::MemoryKV, FixtureVerifier> {
    TransactionCoordinator::with_verifier(
        alopex_core::MemoryKV::new(),
        FixtureVerifier {
            data_epoch: request.data_epoch,
        },
    )
}

#[test]
fn fixed_manifest_covers_the_required_distributed_failure_and_visibility_cases() {
    let manifest = manifest();
    assert_eq!(manifest.schema_version, 1);
    for expected in [
        "two_or_more_ranges",
        "atomic_commit",
        "partial_participant_failure",
        "timeout",
        "conflict",
        "epoch_mismatch",
        "no_partial_visibility",
    ] {
        assert!(
            manifest
                .fixtures
                .iter()
                .any(|fixture| fixture.coverage.iter().any(|coverage| coverage == expected)),
            "manifest is missing {expected}"
        );
    }
}

#[test]
fn single_and_multi_range_fixtures_commit_only_after_every_prepare_ack() {
    let manifest = manifest();
    for id in ["F4-TXN-SINGLE-01", "F4-TXN-MULTI-01"] {
        let fixture = fixture(&manifest, id);
        let request = intent(fixture);
        let coordinator = coordinator(&request);
        let mut driver = Driver::default();
        let outcome = coordinator
            .execute(&request, &mut driver)
            .expect("execute fixture");
        assert_eq!(outcome.state, OperationState::Committed);
        assert_eq!(fixture.expected_state, "committed");
        assert_eq!(outcome.transaction_id, fixture.transaction_id);
        assert_eq!(outcome.request_id.as_str(), fixture.request_id);
        assert_eq!(driver.prepare_calls, request.participants.len());
        assert_eq!(driver.commit_calls, request.participants.len());
        assert_eq!(driver.abort_calls, 0);
    }
}

#[test]
fn partial_failure_conflict_and_timeout_never_publish_a_successful_partial_result() {
    let manifest = manifest();
    let cases = [
        (
            "F4-TXN-ROLLBACK-01",
            TransactionParticipantAck::Rejected {
                failure_class: FailureClass::Conflict,
                reason_code: "f4_partial_participant_failure".to_owned(),
                retryable: false,
            },
            OperationState::TerminalFailure,
            2,
            0,
        ),
        (
            "F4-TXN-CONFLICT-01",
            TransactionParticipantAck::Rejected {
                failure_class: FailureClass::Conflict,
                reason_code: "f4_conflict".to_owned(),
                retryable: false,
            },
            OperationState::TerminalFailure,
            2,
            0,
        ),
        (
            "F4-TXN-TIMEOUT-01",
            TransactionParticipantAck::Rejected {
                failure_class: FailureClass::Timeout,
                reason_code: "f4_timeout".to_owned(),
                retryable: true,
            },
            OperationState::RetryableFailure,
            0,
            2,
        ),
    ];
    for (id, prepare, expected, aborts, discards) in cases {
        let fixture = fixture(&manifest, id);
        let request = intent(fixture);
        let coordinator = coordinator(&request);
        let mut driver = Driver {
            prepares: VecDeque::from([prepare]),
            ..Driver::default()
        };
        let outcome = coordinator
            .execute(&request, &mut driver)
            .expect("execute fixture");
        assert_eq!(outcome.state, expected);
        assert_ne!(outcome.state, OperationState::Committed);
        assert_eq!(
            driver.commit_calls, 0,
            "failed fixtures cannot publish a commit"
        );
        assert_eq!(driver.abort_calls, aborts);
        assert_eq!(driver.discard_calls, discards);
    }
}

#[test]
fn epoch_mismatch_is_rejected_before_participant_io() {
    let manifest = manifest();
    let fixture = fixture(&manifest, "F4-TXN-EPOCH-01");
    let mut request = intent(fixture);
    let coordinator = coordinator(&request);
    request.data_epoch = 12;
    request.read_point.data_epoch = 12;
    // Keep the intent self-consistent so `intent.validate()` succeeds and the
    // test reaches the admission fence. The stale epoch is intentionally only
    // between the immutable intent and the verifier's committed epoch (11).
    for participant in &mut request.participants {
        participant.range.data_epoch = 12;
    }
    if let Some(range_identity) = &mut request.routing.range_identity {
        range_identity.data_epoch = 12;
    }
    let mut driver = Driver::default();
    assert!(matches!(
        coordinator.execute(&request, &mut driver),
        Err(TransactionCoordinatorError::Admission(
            TransactionAdmissionError::EpochMismatch
        ))
    ));
    assert_eq!(driver.prepare_calls, 0);
    assert_eq!(driver.commit_calls, 0);
    assert_eq!(driver.abort_calls, 0);
}

#[test]
fn unknown_commit_ack_remains_recovery_pending_and_never_claims_visibility() {
    let manifest = manifest();
    let fixture = fixture(&manifest, "F4-TXN-PENDING-01");
    let request = intent(fixture);
    let coordinator = coordinator(&request);
    let mut driver = Driver {
        commits: VecDeque::from([
            TransactionParticipantAck::Durable,
            TransactionParticipantAck::RecoveryPending {
                failure_class: FailureClass::NodeUnavailable,
                reason_code: "f4_unknown_commit_ack".to_owned(),
            },
        ]),
        ..Driver::default()
    };
    let outcome = coordinator
        .execute(&request, &mut driver)
        .expect("execute fixture");
    assert_eq!(fixture.expected_state, "recovery_pending");
    assert_eq!(outcome.state, OperationState::RecoveryPending);
    assert!(!outcome.is_success());
    assert_eq!(driver.prepare_calls, 2);
    assert_eq!(driver.commit_calls, 2);
    assert_eq!(driver.abort_calls, 0);
}
