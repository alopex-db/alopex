use std::sync::Arc;

use alopex_server::{
    auth::AuthMode,
    config::{
        ChangefeedAuthorizationConfig, ChangefeedScopeConfig, ChangefeedServerConfig, ServerConfig,
    },
    grpc,
    server::ServerState,
    Server,
};
use prost::Message;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tonic::transport::Channel;
use tonic::{Code, Request};

const API_KEY: &str = "changefeed-grpc-key";
const CONTRACT_VERSION: u32 = 1;

fn parity_fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/changefeed_surface_parity.json"
    ))
    .expect("valid parity fixture")
}

async fn build_state() -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::Dev {
            api_key: API_KEY.to_owned(),
        },
        audit_log_enabled: false,
        changefeed: ChangefeedServerConfig {
            authorizations: vec![ChangefeedAuthorizationConfig {
                subject: "dev".to_owned(),
                tenant: "tenant-a".to_owned(),
                allowed_ranges: vec!["range-a".to_owned()],
                allowed_scopes: vec![ChangefeedScopeConfig::Read, ChangefeedScopeConfig::Ack],
            }],
        },
        ..ServerConfig::default()
    })
    .expect("server");
    (server.state, temp)
}

async fn spawn_network_grpc_server(
    state: Arc<ServerState>,
) -> (Channel, broadcast::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve grpc port");
    let addr = listener.local_addr().expect("grpc address");
    drop(listener);

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handle = tokio::spawn(async move {
        grpc::serve(state, addr, shutdown_rx)
            .await
            .expect("serve grpc transport");
    });

    let endpoint = format!("http://{addr}");
    for _ in 0..40 {
        if let Ok(channel) = Channel::from_shared(endpoint.clone())
            .expect("grpc endpoint")
            .connect()
            .await
        {
            return (channel, shutdown_tx, handle);
        }
        sleep(Duration::from_millis(25)).await;
    }
    let _ = shutdown_tx.send(());
    let _ = handle.await;
    panic!("gRPC transport did not become ready at {endpoint}");
}

fn authenticated<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("x-api-key", API_KEY.parse().expect("metadata value"));
    request
}

fn create() -> grpc::proto::CreateChangefeedRequestV1 {
    grpc::proto::CreateChangefeedRequestV1 {
        contract_version: CONTRACT_VERSION,
        request_id: "grpc-create".to_owned(),
        tenant: "tenant-a".to_owned(),
        actor: "dev".to_owned(),
        target: Some(grpc::proto::create_changefeed_request_v1::Target::RangeId(
            "range-a".to_owned(),
        )),
        retention: None,
        change_kinds: Vec::new(),
    }
}

fn subscribe() -> grpc::proto::SubscribeChangefeedRequestV1 {
    grpc::proto::SubscribeChangefeedRequestV1 {
        contract_version: CONTRACT_VERSION,
        feed_id: "feed-a".to_owned(),
        request_id: "grpc-subscribe".to_owned(),
        expected_generation: 1,
        expected_epoch: 1,
    }
}

fn delivery() -> grpc::proto::DeliveryChangefeedRequestV1 {
    grpc::proto::DeliveryChangefeedRequestV1 {
        contract_version: CONTRACT_VERSION,
        feed_id: "feed-a".to_owned(),
        request_id: "grpc-delivery".to_owned(),
        max_events: 1,
        deadline_epoch: 1,
    }
}

fn ack() -> grpc::proto::AckChangefeedRequestV1 {
    grpc::proto::AckChangefeedRequestV1 {
        contract_version: CONTRACT_VERSION,
        feed_id: "feed-a".to_owned(),
        request_id: "grpc-ack".to_owned(),
        ack_id: "ack-a".to_owned(),
        checkpoint: "invalid".to_owned(),
    }
}

fn resume() -> grpc::proto::ResumeChangefeedRequestV1 {
    grpc::proto::ResumeChangefeedRequestV1 {
        contract_version: CONTRACT_VERSION,
        feed_id: "feed-a".to_owned(),
        request_id: "grpc-resume".to_owned(),
        checkpoint: "invalid".to_owned(),
    }
}

fn lifecycle() -> grpc::proto::LifecycleChangefeedRequestV1 {
    grpc::proto::LifecycleChangefeedRequestV1 {
        contract_version: CONTRACT_VERSION,
        feed_id: "feed-a".to_owned(),
        request_id: "grpc-lifecycle".to_owned(),
    }
}

macro_rules! assert_unauthenticated {
    ($client:expr, $method:ident, $request:expr, $name:literal) => {{
        let result = $client.$method($request).await;
        match result {
            Err(error) => assert_eq!(error.code(), Code::Unauthenticated, $name),
            Ok(_) => panic!("{} accepted a request without API key", $name),
        }
    }};
}

#[tokio::test]
async fn changefeed_grpc_register_enforces_existing_auth_for_all_eight_operations() {
    let (state, _temp) = build_state().await;
    let (channel, shutdown, handle) = spawn_network_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    assert_unauthenticated!(client, create_changefeed, create(), "CreateChangefeed");
    assert_unauthenticated!(
        client,
        subscribe_changefeed,
        subscribe(),
        "SubscribeChangefeed"
    );
    assert_unauthenticated!(client, poll_changefeed, delivery(), "PollChangefeed");
    assert_unauthenticated!(client, stream_changefeed, delivery(), "StreamChangefeed");
    assert_unauthenticated!(client, ack_changefeed, ack(), "AckChangefeed");
    assert_unauthenticated!(client, resume_changefeed, resume(), "ResumeChangefeed");
    assert_unauthenticated!(client, cancel_changefeed, lifecycle(), "CancelChangefeed");
    assert_unauthenticated!(client, close_changefeed, lifecycle(), "CloseChangefeed");

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn create_preserves_canonical_durable_failure_in_typed_status_details() {
    let fixture = parity_fixture();
    let (state, _temp) = build_state().await;
    let (channel, shutdown, handle) = spawn_network_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    let error = client
        .create_changefeed(authenticated(grpc::proto::CreateChangefeedRequestV1 {
            request_id: fixture["request_id"].as_str().unwrap().to_owned(),
            ..create()
        }))
        .await
        .expect_err("compiled Durable profile must fail closed");
    assert_eq!(fixture["grpc_code"], "FAILED_PRECONDITION");
    assert_eq!(error.code(), Code::FailedPrecondition);
    let outcome = grpc::proto::ChangefeedOutcomeV1::decode(error.details())
        .expect("typed canonical outcome in gRPC status details");
    assert_eq!(outcome.contract_version, CONTRACT_VERSION);
    assert_eq!(outcome.failure_class, fixture["failure_class"]);
    assert!(outcome
        .reason_code
        .starts_with(fixture["reason_prefix"].as_str().unwrap()));
    assert_eq!(outcome.operation_state, fixture["operation_state"]);
    assert_eq!(outcome.request_id, fixture["request_id"]);
    assert!(!outcome.retryable);
    assert!(outcome.feed.is_some());
    assert!(outcome.routing.is_some());
    assert!(outcome.idempotency.is_some());
    assert!(!outcome.correlation_id.is_empty());

    let invalid_version = client
        .create_changefeed(authenticated(grpc::proto::CreateChangefeedRequestV1 {
            contract_version: 99,
            ..create()
        }))
        .await
        .expect_err("unsupported wire version must be rejected before execution");
    assert_eq!(invalid_version.code(), Code::InvalidArgument);

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}
