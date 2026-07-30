use std::future::Future;
use std::task::{Context, Poll, Waker};

use alopex_cluster::crdt::{CrdtOperationEnvelope, CrdtOutcome, CrdtPayload};
use alopex_cluster::{
    CrdtOperationKind, FailureClass, OperationState, RangeIdentity, RoutingOutcomeKind,
};
use alopex_embedded::Database;

fn envelope(
    object_id: &str,
    request_id: &str,
    operation_id: &str,
    update_version: u64,
    kind: CrdtOperationKind,
    payload: CrdtPayload,
) -> CrdtOperationEnvelope {
    CrdtOperationEnvelope::new(
        object_id,
        RangeIdentity::new("embedded-gate", 7, "range-gate", None, None, 1, 9),
        "embedded-gate-actor",
        request_id,
        operation_id,
        update_version,
        kind,
        payload,
    )
    .expect("gate envelope must be valid")
}

fn counter_create() -> CrdtOperationEnvelope {
    envelope(
        "counter-gate",
        "request-counter-create",
        "operation-counter-create",
        1,
        CrdtOperationKind::CounterCreate,
        CrdtPayload::Counter {
            initial_value: Some(-4),
            delta: None,
        },
    )
}

fn counter_read() -> CrdtOperationEnvelope {
    envelope(
        "counter-gate",
        "request-counter-read",
        "operation-counter-read",
        0,
        CrdtOperationKind::CounterRead,
        CrdtPayload::None,
    )
}

fn counter_increment() -> CrdtOperationEnvelope {
    envelope(
        "counter-gate",
        "request-counter-increment",
        "operation-counter-increment",
        2,
        CrdtOperationKind::CounterIncrement,
        CrdtPayload::Counter {
            initial_value: None,
            delta: Some(3),
        },
    )
}

fn counter_decrement() -> CrdtOperationEnvelope {
    envelope(
        "counter-gate",
        "request-counter-decrement",
        "operation-counter-decrement",
        3,
        CrdtOperationKind::CounterDecrement,
        CrdtPayload::Counter {
            initial_value: None,
            delta: Some(3),
        },
    )
}

fn set_envelope(
    request_id: &str,
    operation_id: &str,
    update_version: u64,
    kind: CrdtOperationKind,
    payload: CrdtPayload,
) -> CrdtOperationEnvelope {
    envelope(
        "set-gate",
        request_id,
        operation_id,
        update_version,
        kind,
        payload,
    )
}

fn assert_local(outcome: &CrdtOutcome) {
    assert_eq!(outcome.common().state, OperationState::Committed);
    assert_eq!(outcome.common().routing.kind, RoutingOutcomeKind::LocalOnly);
    assert_eq!(outcome.common().idempotency.duplicate_count, 0);
}

fn poll_immediately<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("embedded async CRDT capability must settle immediately"),
    }
}

fn assert_async_unsupported(outcome: CrdtOutcome) {
    assert_eq!(outcome.common().state, OperationState::Rejected);
    assert_eq!(
        outcome.common().failure_class,
        Some(FailureClass::InvalidRequest)
    );
    assert_eq!(
        outcome.common().routing.kind,
        RoutingOutcomeKind::Unsupported
    );
    assert!(outcome.value().is_none());
}

#[test]
fn f2_embedded_sync_register_exposes_all_ten_canonical_local_operations() {
    let db = Database::new();

    assert_local(&db.create_counter(counter_create()).expect("Counter create"));
    assert_local(&db.read_counter(counter_read()).expect("Counter read"));
    assert_local(
        &db.increment_counter(counter_increment())
            .expect("Counter increment"),
    );
    assert_local(
        &db.decrement_counter(counter_decrement())
            .expect("Counter decrement"),
    );

    assert_local(
        &db.create_set(set_envelope(
            "request-set-create",
            "operation-set-create",
            1,
            CrdtOperationKind::SetCreate,
            CrdtPayload::None,
        ))
        .expect("Set create"),
    );
    assert_local(
        &db.read_set(set_envelope(
            "request-set-read",
            "operation-set-read",
            0,
            CrdtOperationKind::SetRead,
            CrdtPayload::None,
        ))
        .expect("Set read"),
    );
    assert_local(
        &db.add_set(set_envelope(
            "request-set-add",
            "00000000-0000-0000-0000-000000000901",
            2,
            CrdtOperationKind::SetAdd,
            CrdtPayload::Set {
                member: Some("alice".to_owned()),
            },
        ))
        .expect("Set add"),
    );
    assert_local(
        &db.contains_set(set_envelope(
            "request-set-contains",
            "operation-set-contains",
            0,
            CrdtOperationKind::SetContains,
            CrdtPayload::Set {
                member: Some("alice".to_owned()),
            },
        ))
        .expect("Set contains"),
    );
    assert_local(
        &db.list_set(set_envelope(
            "request-set-list",
            "operation-set-list",
            0,
            CrdtOperationKind::SetList,
            CrdtPayload::None,
        ))
        .expect("Set list"),
    );
    assert_local(
        &db.remove_set(set_envelope(
            "request-set-remove",
            "00000000-0000-0000-0000-000000000902",
            3,
            CrdtOperationKind::SetRemove,
            CrdtPayload::Set {
                member: Some("alice".to_owned()),
            },
        ))
        .expect("Set remove"),
    );
}

#[test]
fn f2_embedded_async_register_is_explicitly_unsupported_without_sync_fallback() {
    let db = Database::new();

    assert_async_unsupported(poll_immediately(db.create_counter_async(counter_create())).unwrap());
    assert_async_unsupported(poll_immediately(db.read_counter_async(counter_read())).unwrap());
    assert_async_unsupported(
        poll_immediately(db.increment_counter_async(counter_increment())).unwrap(),
    );
    assert_async_unsupported(
        poll_immediately(db.decrement_counter_async(counter_decrement())).unwrap(),
    );
    assert_async_unsupported(
        poll_immediately(db.create_set_async(set_envelope(
            "request-set-create",
            "operation-set-create",
            1,
            CrdtOperationKind::SetCreate,
            CrdtPayload::None,
        )))
        .unwrap(),
    );
    assert_async_unsupported(
        poll_immediately(db.read_set_async(set_envelope(
            "request-set-read",
            "operation-set-read",
            0,
            CrdtOperationKind::SetRead,
            CrdtPayload::None,
        )))
        .unwrap(),
    );
    assert_async_unsupported(
        poll_immediately(db.add_set_async(set_envelope(
            "request-set-add",
            "00000000-0000-0000-0000-000000000901",
            2,
            CrdtOperationKind::SetAdd,
            CrdtPayload::Set {
                member: Some("alice".to_owned()),
            },
        )))
        .unwrap(),
    );
    assert_async_unsupported(
        poll_immediately(db.remove_set_async(set_envelope(
            "request-set-remove",
            "00000000-0000-0000-0000-000000000902",
            3,
            CrdtOperationKind::SetRemove,
            CrdtPayload::Set {
                member: Some("alice".to_owned()),
            },
        )))
        .unwrap(),
    );
    assert_async_unsupported(
        poll_immediately(db.contains_set_async(set_envelope(
            "request-set-contains",
            "operation-set-contains",
            0,
            CrdtOperationKind::SetContains,
            CrdtPayload::Set {
                member: Some("alice".to_owned()),
            },
        )))
        .unwrap(),
    );
    assert_async_unsupported(
        poll_immediately(db.list_set_async(set_envelope(
            "request-set-list",
            "operation-set-list",
            0,
            CrdtOperationKind::SetList,
            CrdtPayload::None,
        )))
        .unwrap(),
    );

    assert_local(
        &db.create_counter(counter_create())
            .expect("async rejection must not write the Counter ledger"),
    );
}
