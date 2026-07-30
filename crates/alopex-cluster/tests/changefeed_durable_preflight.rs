use alopex_cluster::{
    FailureClass,
    changefeed::{
        DurableAuthorization, DurableCapabilityVersion, DurableProfileAdapter,
        DurableProfileEvidence, FeedPreflight,
    },
};

fn complete_evidence() -> DurableProfileEvidence {
    DurableProfileEvidence::complete(DurableCapabilityVersion::new(0, 7, 0))
}

fn assert_rejected(
    preflight: FeedPreflight,
    failure_class: FailureClass,
    reason_code: &str,
    retryable: bool,
) {
    assert_eq!(
        preflight,
        FeedPreflight::Rejected {
            failure_class,
            reason_code: reason_code.to_string(),
            retryable,
        }
    );
}

#[test]
fn chirps_v051_durable_evidence_is_terminally_incompatible() {
    let adapter = DurableProfileAdapter::new(DurableProfileEvidence::complete(
        DurableCapabilityVersion::new(0, 5, 1),
    ));

    assert_rejected(
        adapter.preflight(),
        FailureClass::PrerequisiteMissing,
        "durable_version_incompatible",
        false,
    );
}

#[test]
fn compiled_foundation_cannot_advertise_a_durable_feed() {
    let preflight = DurableProfileAdapter::compiled().preflight();

    assert!(matches!(
        preflight,
        FeedPreflight::Rejected {
            failure_class: FailureClass::PrerequisiteMissing,
            retryable: false,
            ..
        }
    ));
}

#[test]
fn absent_or_missing_durable_prerequisites_never_default_to_ready() {
    assert_rejected(
        DurableProfileAdapter::new(DurableProfileEvidence::unavailable()).preflight(),
        FailureClass::PrerequisiteMissing,
        "durable_capability_missing",
        false,
    );

    let mut evidence = complete_evidence();
    evidence.authenticated_dispatcher = false;
    assert_rejected(
        DurableProfileAdapter::new(evidence).preflight(),
        FailureClass::PrerequisiteMissing,
        "durable_authenticated_dispatcher_missing",
        false,
    );

    let mut evidence = complete_evidence();
    evidence.durable_storage = false;
    assert_rejected(
        DurableProfileAdapter::new(evidence).preflight(),
        FailureClass::PrerequisiteMissing,
        "durable_storage_missing",
        false,
    );

    let mut evidence = complete_evidence();
    evidence.range_routing = false;
    assert_rejected(
        DurableProfileAdapter::new(evidence).preflight(),
        FailureClass::PrerequisiteMissing,
        "durable_range_routing_missing",
        false,
    );

    let mut evidence = complete_evidence();
    evidence.retention = false;
    assert_rejected(
        DurableProfileAdapter::new(evidence).preflight(),
        FailureClass::PrerequisiteMissing,
        "durable_retention_missing",
        false,
    );
}

#[test]
fn authorization_and_service_unavailability_are_explicitly_classified() {
    let mut unauthorized = complete_evidence();
    unauthorized.authorization = DurableAuthorization::Unauthorized;
    assert_rejected(
        DurableProfileAdapter::new(unauthorized).preflight(),
        FailureClass::Unauthorized,
        "changefeed_unauthorized",
        false,
    );

    let mut unavailable = complete_evidence();
    unavailable.service_available = false;
    assert_rejected(
        DurableProfileAdapter::new(unavailable).preflight(),
        FailureClass::NodeUnavailable,
        "durable_unavailable",
        true,
    );
}

#[test]
fn only_complete_v07_or_newer_evidence_allows_feed_preflight() {
    let adapter = DurableProfileAdapter::new(complete_evidence());

    assert!(adapter.preflight().is_ready());
}
