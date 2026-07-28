use std::sync::Arc;

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::grpc;
use alopex_server::server::ServerState;
use alopex_server::Server;
use prost::Message;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tonic::transport::Channel;
use tonic::{Code, Request};

const I14_REGISTER: [&str; 19] = [
    "ExecuteSql",
    "ExecuteDdl",
    "ExecuteDml",
    "BeginTransaction",
    "CommitTransaction",
    "RollbackTransaction",
    "VectorSearch",
    "VectorUpsert",
    "VectorDelete",
    "VectorIndexCreate",
    "VectorIndexUpdate",
    "VectorIndexDelete",
    "VectorIndexCompact",
    "Health",
    "ClusterStatus",
    "ClusterJoin",
    "ClusterLeave",
    "CreateCounter",
    "ReadCounter",
];

async fn build_state() -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::Dev {
            api_key: "v09-key".to_owned(),
        },
        audit_log_enabled: false,
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
async fn i14_grpc_method_register_preserves_version_auth_status_and_unknown_fields() {
    let (state, _temp) = build_state().await;
    let (channel, shutdown, handle) = spawn_network_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    assert_unauthenticated!(
        client,
        execute_sql,
        grpc::proto::SqlRequest::default(),
        "ExecuteSql"
    );
    assert_unauthenticated!(
        client,
        execute_ddl,
        grpc::proto::DdlRequest::default(),
        "ExecuteDdl"
    );
    assert_unauthenticated!(
        client,
        execute_dml,
        grpc::proto::DmlRequest::default(),
        "ExecuteDml"
    );
    assert_unauthenticated!(
        client,
        begin_transaction,
        grpc::proto::BeginRequest {},
        "BeginTransaction"
    );
    assert_unauthenticated!(
        client,
        commit_transaction,
        grpc::proto::TransactionHandle::default(),
        "CommitTransaction"
    );
    assert_unauthenticated!(
        client,
        rollback_transaction,
        grpc::proto::TransactionHandle::default(),
        "RollbackTransaction"
    );
    assert_unauthenticated!(
        client,
        vector_search,
        grpc::proto::VectorSearchRequest::default(),
        "VectorSearch"
    );
    assert_unauthenticated!(
        client,
        vector_upsert,
        grpc::proto::VectorUpsertRequest::default(),
        "VectorUpsert"
    );
    assert_unauthenticated!(
        client,
        vector_delete,
        grpc::proto::VectorDeleteRequest::default(),
        "VectorDelete"
    );
    assert_unauthenticated!(
        client,
        vector_index_create,
        grpc::proto::VectorIndexCreateRequest::default(),
        "VectorIndexCreate"
    );
    assert_unauthenticated!(
        client,
        vector_index_update,
        grpc::proto::VectorIndexUpdateRequest::default(),
        "VectorIndexUpdate"
    );
    assert_unauthenticated!(
        client,
        vector_index_delete,
        grpc::proto::VectorIndexDeleteRequest::default(),
        "VectorIndexDelete"
    );
    assert_unauthenticated!(
        client,
        vector_index_compact,
        grpc::proto::VectorIndexCompactRequest::default(),
        "VectorIndexCompact"
    );
    assert_unauthenticated!(client, health, grpc::proto::HealthRequest {}, "Health");
    assert_unauthenticated!(
        client,
        cluster_status,
        grpc::proto::ClusterStatusRequest {},
        "ClusterStatus"
    );
    assert_unauthenticated!(
        client,
        cluster_join,
        grpc::proto::ClusterJoinRequest {},
        "ClusterJoin"
    );
    assert_unauthenticated!(
        client,
        cluster_leave,
        grpc::proto::ClusterLeaveRequest {},
        "ClusterLeave"
    );
    assert_unauthenticated!(
        client,
        create_counter,
        grpc::proto::CreateCounterRequest::default(),
        "CreateCounter"
    );
    assert_unauthenticated!(
        client,
        read_counter,
        grpc::proto::ReadCounterRequest::default(),
        "ReadCounter"
    );

    let decoded = grpc::proto::HealthRequest::decode(&[0x10, 0x01][..])
        .expect("unknown protobuf field must be ignored");
    assert_eq!(decoded, grpc::proto::HealthRequest {});
    assert_eq!(I14_REGISTER.len(), 19, "the I-14 RPC register drifted");

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn create_counter_uses_authenticated_actor_and_canonical_counter_outcome() {
    let (state, _temp) = build_state().await;
    let (channel, shutdown, handle) = spawn_network_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);
    let mut request = Request::new(grpc::proto::CreateCounterRequest {
        object_id: "counter-grpc".into(),
        range: Some(grpc::proto::CrdtRangeIdentity {
            cluster_id: "cluster-grpc".into(),
            table_id: 7,
            range_id: "range-grpc".into(),
            lower_bound: Vec::new(),
            has_lower_bound: false,
            upper_bound: Vec::new(),
            has_upper_bound: false,
            schema_version: 1,
            data_epoch: 9,
        }),
        request_id: "request-grpc".into(),
        operation_id: "operation-grpc".into(),
        update_version: 0,
        initial_value: -4,
    });
    request
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let outcome = client
        .create_counter(request)
        .await
        .expect("counter create")
        .into_inner();
    assert_eq!(outcome.object_type, "counter");
    assert_eq!(outcome.actor, "dev");
    assert_eq!(outcome.state, "committed");
    assert_eq!(outcome.routing_kind, "local_only");
    assert!(outcome.has_value);
    assert_eq!(outcome.initial_value, -4);
    assert_eq!(outcome.accepted_delta_total, 0);
    assert_eq!(outcome.value, -4);
    assert_eq!(outcome.duplicate_count, 0);

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn read_counter_uses_authenticated_actor_and_canonical_counter_outcome() {
    let (state, _temp) = build_state().await;
    let (channel, shutdown, handle) = spawn_network_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);
    let range = grpc::proto::CrdtRangeIdentity {
        cluster_id: "cluster-grpc".into(),
        table_id: 7,
        range_id: "range-grpc".into(),
        lower_bound: Vec::new(),
        has_lower_bound: false,
        upper_bound: Vec::new(),
        has_upper_bound: false,
        schema_version: 1,
        data_epoch: 9,
    };
    let mut create = Request::new(grpc::proto::CreateCounterRequest {
        object_id: "counter-grpc".into(),
        range: Some(range.clone()),
        request_id: "request-grpc-create".into(),
        operation_id: "operation-grpc-create".into(),
        update_version: 0,
        initial_value: -4,
    });
    create
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    client
        .create_counter(create)
        .await
        .expect("counter create before read");

    let mut request = Request::new(grpc::proto::ReadCounterRequest {
        object_id: "counter-grpc".into(),
        range: Some(range),
        request_id: "request-grpc-read".into(),
        operation_id: "operation-grpc-read".into(),
        update_version: 0,
    });
    request
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let outcome = client
        .read_counter(request)
        .await
        .expect("counter read")
        .into_inner();
    assert_eq!(outcome.object_type, "counter");
    assert_eq!(outcome.object_id, "counter-grpc");
    assert_eq!(outcome.actor, "dev");
    assert_eq!(outcome.request_id, "request-grpc-read");
    assert_eq!(outcome.operation_id, "operation-grpc-read");
    assert_eq!(outcome.state, "committed");
    assert_eq!(outcome.routing_kind, "local_only");
    assert!(outcome.has_value);
    assert_eq!(outcome.initial_value, -4);
    assert_eq!(outcome.accepted_delta_total, 0);
    assert_eq!(outcome.value, -4);
    assert_eq!(outcome.first_outcome, "counter_read");
    assert_eq!(outcome.duplicate_count, 0);

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}
