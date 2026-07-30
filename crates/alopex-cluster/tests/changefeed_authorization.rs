use std::collections::BTreeSet;

use alopex_cluster::{
    AuthenticatedSubject, OperationState, OrderingScope, Placement, PlacementReadiness,
    PlacementRole, RangeIdentity, RetentionWindow, RoutingOutcome, RoutingOutcomeKind,
    changefeed::{
        ChangefeedAccessRequest, ChangefeedAction, ChangefeedAuthorization,
        ChangefeedAuthorizationDecision, ChangefeedScope,
    },
};

fn feed() -> alopex_cluster::FeedIdentity {
    alopex_cluster::FeedIdentity::new(
        "feed-a",
        RangeIdentity::new("cluster-a", 7, "range-a", None, None, 4, 9),
        3,
        Placement::new(
            "node-a",
            vec![],
            PlacementRole::Owner,
            PlacementReadiness::Ready,
            11,
        ),
        OrderingScope::Range,
        RetentionWindow {
            deadline_epoch: Some(90),
            retained_through_position: Some(1),
        },
        OperationState::Accepted,
    )
    .unwrap()
}

fn authorization(scopes: &[ChangefeedScope]) -> ChangefeedAuthorization {
    ChangefeedAuthorization {
        subject: AuthenticatedSubject::new("dev"),
        tenant: "tenant-a".to_string(),
        allowed_ranges: BTreeSet::from(["range-a".into()]),
        allowed_scopes: scopes.iter().copied().collect(),
    }
}

fn request(action: ChangefeedAction) -> ChangefeedAccessRequest {
    ChangefeedAccessRequest {
        action,
        tenant: "tenant-a".to_string(),
        range_id: "range-a".into(),
    }
}

fn routing() -> RoutingOutcome {
    RoutingOutcome::new(
        RoutingOutcomeKind::SingleRange,
        Some(feed().range),
        12,
        "placement_ready",
    )
}

#[test]
fn every_lifecycle_operation_requires_its_approved_scope() {
    let read = authorization(&[ChangefeedScope::Read]);
    for action in [
        ChangefeedAction::Create,
        ChangefeedAction::Subscribe,
        ChangefeedAction::Poll,
        ChangefeedAction::Stream,
        ChangefeedAction::Resume,
    ] {
        assert!(read.authorize(request(action)).permits());
    }
    for action in [
        ChangefeedAction::Ack,
        ChangefeedAction::Cancel,
        ChangefeedAction::Close,
        ChangefeedAction::ManageRetention,
    ] {
        assert!(!read.authorize(request(action)).permits());
    }

    let ack = authorization(&[ChangefeedScope::Ack]);
    for action in [
        ChangefeedAction::Ack,
        ChangefeedAction::Cancel,
        ChangefeedAction::Close,
    ] {
        assert!(ack.authorize(request(action)).permits());
    }
    assert!(
        !ack.authorize(request(ChangefeedAction::ManageRetention))
            .permits()
    );
    assert!(
        authorization(&[ChangefeedScope::RetentionAdmin])
            .authorize(request(ChangefeedAction::ManageRetention))
            .permits()
    );
}

#[test]
fn tenant_or_range_denial_is_indistinguishable_and_redacts_payload() {
    let authorization = authorization(&[ChangefeedScope::Read]);
    let tenant_denied = authorization.authorize(ChangefeedAccessRequest {
        tenant: "tenant-b".to_string(),
        ..request(ChangefeedAction::Poll)
    });
    let range_denied = authorization.authorize(ChangefeedAccessRequest {
        range_id: "range-b".into(),
        ..request(ChangefeedAction::Poll)
    });
    assert_eq!(tenant_denied, ChangefeedAuthorizationDecision::Denied);
    assert_eq!(range_denied, ChangefeedAuthorizationDecision::Denied);

    let outcome = tenant_denied
        .denied_outcome(feed(), routing(), "poll-a", "request-a")
        .unwrap();
    assert_eq!(outcome.operation_state, OperationState::TerminalFailure);
    assert_eq!(
        outcome.failure_class,
        Some(alopex_cluster::FailureClass::Unauthorized)
    );
    assert_eq!(
        outcome.reason_code.as_deref(),
        Some("changefeed_unauthorized")
    );
    assert!(matches!(
        outcome.result,
        alopex_cluster::ChangefeedResult::Feed
    ));
    assert_eq!(outcome.surface_status().http_status, 401);
}
