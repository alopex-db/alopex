use std::collections::BTreeMap;

use alopex_cluster::crdt::CrdtOutcome;
use alopex_cluster::{
    CounterValue, CrdtCommonFields, CrdtObjectType, FailureClass, IdempotencyResult,
    OperationState, RangeIdentity, RoutingOutcome, RoutingOutcomeKind,
};

fn common(
    state: OperationState,
    failure: Option<FailureClass>,
    routing: RoutingOutcomeKind,
) -> CrdtCommonFields {
    let range = RangeIdentity::new("fixture", 7, "range-fixture", None, None, 1, 9);
    CrdtCommonFields {
        object_type: CrdtObjectType::Counter,
        object_id: "counter-fixture".into(),
        range: range.clone(),
        state_epoch: 9,
        actor: "node-a".into(),
        request_id: "request-fixture".into(),
        operation_id: "operation-fixture".into(),
        state,
        failure_class: failure,
        routing: RoutingOutcome::new(routing, Some(range), 4, "fixture"),
        retryable: false,
        idempotency: IdempotencyResult {
            operation_id: "operation-fixture".into(),
            request_id: "request-fixture".into(),
            first_outcome: "counter_committed".into(),
            state,
            duplicate_count: 1,
        },
    }
}

#[test]
fn independent_f2_common_contract_has_one_serialization_and_transport_mapping() {
    let committed = CrdtOutcome::counter(
        common(
            OperationState::Committed,
            None,
            RoutingOutcomeKind::SingleRange,
        ),
        CounterValue {
            initial_value: -4,
            accepted_delta_total: 5,
            value: 1,
            accepted_operation_versions: BTreeMap::from([("increment".into(), 1)]),
        },
    );
    let encoded = committed.canonical_bytes().expect("canonical JSON");
    let decoded: CrdtOutcome = serde_json::from_slice(&encoded).expect("canonical decode");
    assert_eq!(decoded, committed);
    assert_eq!(
        committed.canonical_digest().expect("first digest"),
        decoded.canonical_digest().expect("decoded digest")
    );
    let status = committed.surface_status();
    assert_eq!(
        (
            status.http_status,
            status.grpc_code,
            status.cli_exit_code,
            status.python_error_code
        ),
        (200, "OK", 0, None)
    );

    let unsupported = CrdtOutcome::unsupported(
        common(
            OperationState::Rejected,
            Some(FailureClass::InvalidRequest),
            RoutingOutcomeKind::Unsupported,
        ),
        "pre_execution_unsupported",
    );
    let status = unsupported.surface_status();
    assert_eq!(
        (
            status.http_status,
            status.grpc_code,
            status.cli_exit_code,
            status.python_error_code
        ),
        (501, "UNIMPLEMENTED", 5, Some("crdt_unsupported"))
    );
}
