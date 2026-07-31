use std::path::Path;
use std::sync::{Arc, LazyLock};

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

const I14_REGISTER: [&str; 27] = [
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
    "IncrementCounter",
    "DecrementCounter",
    "CreateSet",
    "ReadSet",
    "ListSet",
    "AddSet",
    "RemoveSet",
    "ContainsSet",
];

// `serve` owns its listener and therefore binds after this test helper has
// released the provisional ephemeral-port reservation. Serialize only that
// hand-off through the first successful client connection; running RPCs still
// remains concurrent once each server owns its socket.
static NETWORK_GRPC_SERVER_START_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

async fn build_state() -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    (build_state_at(temp.path()), temp)
}

fn build_state_at(data_dir: &Path) -> Arc<ServerState> {
    let server = Server::new(ServerConfig {
        data_dir: data_dir.to_path_buf(),
        auth_mode: AuthMode::Dev {
            api_key: "v09-key".to_owned(),
        },
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .expect("server");
    server.state
}

async fn spawn_network_grpc_server(
    state: Arc<ServerState>,
) -> (Channel, broadcast::Sender<()>, tokio::task::JoinHandle<()>) {
    let _startup_guard = NETWORK_GRPC_SERVER_START_LOCK.lock().await;
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
            Err(error) => {
                assert_eq!(error.code(), Code::Unauthenticated, $name);
                assert_eq!(error.message(), "unauthorized", $name);
                let outcome = grpc::proto::TransactionOutcomeV09::decode(error.details())
                    .expect("denied gRPC method must carry a v0.9 outcome");
                assert_eq!(
                    outcome.state,
                    grpc::proto::TransactionOperationStateV09::Rejected as i32,
                    $name
                );
                assert_eq!(
                    outcome.failure_class,
                    grpc::proto::TransactionFailureClassV09::Unauthorized as i32,
                    $name
                );
                assert_eq!(
                    outcome.routing.expect("blocked routing").kind,
                    grpc::proto::TransactionRoutingKindV09::Blocked as i32,
                    $name
                );
            }
            Ok(_) => panic!("{} accepted a request without API key", $name),
        }
    }};
}

fn authorized_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    request
}

async fn execute_single_sql(
    client: &mut grpc::proto::alopex_service_client::AlopexServiceClient<Channel>,
    request: grpc::proto::SqlRequest,
) -> grpc::proto::SqlResultSet {
    let mut stream = client
        .execute_sql(authorized_request(request))
        .await
        .expect("SQL request")
        .into_inner();
    stream
        .message()
        .await
        .expect("SQL stream status")
        .expect("SQL result")
}

fn transaction_duplicate_count(transaction: &Option<grpc::proto::TransactionOutcomeV09>) -> u64 {
    transaction
        .as_ref()
        .and_then(|outcome| outcome.idempotency.as_ref())
        .expect("v0.9 idempotency outcome")
        .duplicate_count
}

fn assert_transaction_failure(
    error: tonic::Status,
    code: Code,
    failure_class: grpc::proto::TransactionFailureClassV09,
    routing: grpc::proto::TransactionRoutingKindV09,
) -> grpc::proto::TransactionOutcomeV09 {
    assert_eq!(error.code(), code);
    let outcome = grpc::proto::TransactionOutcomeV09::decode(error.details())
        .expect("gRPC failure must carry a v0.9 transaction outcome");
    assert_eq!(
        outcome.state,
        grpc::proto::TransactionOperationStateV09::Rejected as i32
    );
    assert_eq!(outcome.failure_class, failure_class as i32);
    assert_eq!(
        outcome.routing.as_ref().expect("failure routing").kind,
        routing as i32
    );
    outcome
}

fn assert_session_expired(error: tonic::Status, transaction_id: &str, request_id: &str) {
    assert_eq!(error.code(), Code::NotFound);
    let outcome = grpc::proto::TransactionOutcomeV09::decode(error.details())
        .expect("expired local session must carry a transaction outcome");
    assert_eq!(
        outcome.state,
        grpc::proto::TransactionOperationStateV09::Rejected as i32
    );
    assert_eq!(
        outcome.failure_class,
        grpc::proto::TransactionFailureClassV09::InvalidRequest as i32
    );
    assert_eq!(outcome.reason_code, "session_expired");
    assert_eq!(outcome.transaction_id, transaction_id);
    assert_eq!(outcome.request_id, request_id);
}

#[derive(Clone, PartialEq, Message)]
struct LegacyTransactionHandle {
    #[prost(string, tag = "1")]
    session_id: String,
    #[prost(int64, tag = "2")]
    expires_at_ms: i64,
}

#[derive(Clone, PartialEq, Message)]
struct LegacySuccessResponse {
    #[prost(bool, tag = "1")]
    success: bool,
}

#[derive(Clone, PartialEq, Message)]
struct LegacyDmlResponse {
    #[prost(uint64, tag = "1")]
    affected_rows: u64,
}

#[derive(Clone, PartialEq, Message)]
struct LegacySqlResultSet {
    #[prost(uint64, tag = "3")]
    affected_rows: u64,
    #[prost(bool, tag = "4")]
    has_affected_rows: bool,
    #[prost(bool, tag = "5")]
    success: bool,
}

#[tokio::test]
async fn i14_grpc_method_register_preserves_version_auth_status_and_unknown_fields() {
    let (state, _temp) = build_state().await;
    let (channel, shutdown, server_handle) = spawn_network_grpc_server(state).await;
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
        grpc::proto::BeginRequest::default(),
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
    assert_unauthenticated!(
        client,
        increment_counter,
        grpc::proto::IncrementCounterRequest::default(),
        "IncrementCounter"
    );
    assert_unauthenticated!(
        client,
        decrement_counter,
        grpc::proto::DecrementCounterRequest::default(),
        "DecrementCounter"
    );
    assert_unauthenticated!(
        client,
        create_set,
        grpc::proto::CreateSetRequest::default(),
        "CreateSet"
    );
    assert_unauthenticated!(
        client,
        read_set,
        grpc::proto::ReadSetRequest::default(),
        "ReadSet"
    );
    assert_unauthenticated!(
        client,
        add_set,
        grpc::proto::AddSetRequest::default(),
        "AddSet"
    );
    assert_unauthenticated!(
        client,
        remove_set,
        grpc::proto::RemoveSetRequest::default(),
        "RemoveSet"
    );
    assert_unauthenticated!(
        client,
        contains_set,
        grpc::proto::ContainsSetRequest::default(),
        "ContainsSet"
    );
    assert_unauthenticated!(
        client,
        list_set,
        grpc::proto::ListSetRequest::default(),
        "ListSet"
    );

    let decoded = grpc::proto::HealthRequest::decode(&[0x10, 0x01][..])
        .expect("unknown protobuf field must be ignored");
    assert_eq!(decoded, grpc::proto::HealthRequest {});
    assert_eq!(I14_REGISTER.len(), 27, "the I-14 RPC register drifted");

    let proto_source = include_str!("../proto/alopex.proto");
    for preserved_field in [
        "string session_id = 1;",
        "int64 expires_at_ms = 2;",
        "bool success = 1;",
        "uint64 affected_rows = 1;",
    ] {
        assert!(
            proto_source.contains(preserved_field),
            "v0.8 protobuf field changed: {preserved_field}"
        );
    }
    for absent_method in [
        "TransactionStatus",
        "RecoverTransaction",
        "CancelTransaction",
    ] {
        assert!(
            !proto_source.contains(&format!("rpc {absent_method}")),
            "v0.9 must keep absent transaction RPC unsupported: {absent_method}"
        );
    }

    let _ = shutdown.send(());
    server_handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn transaction_outcomes_are_additive_for_grpc_lifecycle_and_sql() {
    let (state, _temp) = build_state().await;
    let (channel, shutdown, server_handle) = spawn_network_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    let denied = client
        .begin_transaction(Request::new(grpc::proto::BeginRequest::default()))
        .await
        .expect_err("unauthenticated begin must fail closed");
    assert_eq!(denied.code(), Code::Unauthenticated);
    assert_eq!(denied.message(), "unauthorized", "legacy auth message");
    let denied_outcome = grpc::proto::TransactionOutcomeV09::decode(denied.details())
        .expect("denied request has versioned transaction outcome details");
    assert_eq!(
        denied_outcome.state,
        grpc::proto::TransactionOperationStateV09::Rejected as i32
    );
    assert_eq!(
        denied_outcome.failure_class,
        grpc::proto::TransactionFailureClassV09::Unauthorized as i32
    );
    assert_eq!(
        denied_outcome.routing.expect("routing").kind,
        grpc::proto::TransactionRoutingKindV09::Blocked as i32
    );

    // The versioned optional field makes an explicit empty value distinct from
    // a v0.8 request that did not send it at all. It must reject before begin
    // can allocate a session or DML can reach its write path.
    let mut invalid_begin = Request::new(grpc::proto::BeginRequest {
        request_id: Some(String::new()),
        require_distributed: None,
    });
    invalid_begin
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let invalid_begin = client
        .begin_transaction(invalid_begin)
        .await
        .expect_err("empty begin request id must fail before execution");
    assert_eq!(invalid_begin.code(), Code::InvalidArgument);
    let invalid_begin_outcome = grpc::proto::TransactionOutcomeV09::decode(invalid_begin.details())
        .expect("invalid begin has transaction outcome details");
    assert_eq!(
        invalid_begin_outcome.state,
        grpc::proto::TransactionOperationStateV09::Rejected as i32
    );
    assert_eq!(
        invalid_begin_outcome.failure_class,
        grpc::proto::TransactionFailureClassV09::InvalidRequest as i32
    );

    let mut invalid_dml = Request::new(grpc::proto::DmlRequest {
        sql: "INSERT INTO no_such_table VALUES (1)".to_owned(),
        session_id: String::new(),
        request_id: Some(String::new()),
        require_distributed: None,
    });
    invalid_dml
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let invalid_dml = client
        .execute_dml(invalid_dml)
        .await
        .expect_err("empty DML request id must fail before execution");
    assert_eq!(invalid_dml.code(), Code::InvalidArgument);
    let invalid_dml_outcome = grpc::proto::TransactionOutcomeV09::decode(invalid_dml.details())
        .expect("invalid DML has transaction outcome details");
    assert_eq!(
        invalid_dml_outcome.failure_class,
        grpc::proto::TransactionFailureClassV09::InvalidRequest as i32
    );

    let failure_request = grpc::proto::DmlRequest {
        sql: "INSERT INTO table_that_does_not_exist VALUES (1)".to_owned(),
        session_id: String::new(),
        request_id: Some("grpc-failure-replay".to_owned()),
        require_distributed: Some(false),
    };
    let mut first_failure = Request::new(failure_request.clone());
    first_failure
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let first_failure = client
        .execute_dml(first_failure)
        .await
        .expect_err("first failing DML must return status");
    let first_failure_outcome = grpc::proto::TransactionOutcomeV09::decode(first_failure.details())
        .expect("first failing DML outcome");

    let mut replay_failure = Request::new(failure_request);
    replay_failure
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let replay_failure = client
        .execute_dml(replay_failure)
        .await
        .expect_err("failing DML retry must replay the first status");
    assert_eq!(replay_failure.code(), first_failure.code());
    let replay_failure_outcome =
        grpc::proto::TransactionOutcomeV09::decode(replay_failure.details())
            .expect("replayed failing DML outcome");
    assert_eq!(replay_failure_outcome.state, first_failure_outcome.state);
    assert_eq!(
        replay_failure_outcome.failure_class,
        first_failure_outcome.failure_class
    );
    assert_eq!(
        replay_failure_outcome
            .idempotency
            .expect("replayed failure idempotency")
            .duplicate_count,
        1
    );

    let mut begin = Request::new(grpc::proto::BeginRequest {
        request_id: Some("grpc-begin-request".to_owned()),
        require_distributed: Some(false),
    });
    begin
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let handle = client
        .begin_transaction(begin)
        .await
        .expect("begin")
        .into_inner();
    assert!(!handle.session_id.is_empty(), "legacy session id");
    assert!(handle.expires_at_ms > 0, "legacy expiry");
    assert_eq!(handle.request_id.as_deref(), Some("grpc-begin-request"));
    let begin_outcome = handle.transaction.as_ref().expect("additive outcome");
    assert_eq!(begin_outcome.outcome_version, "v0.9");
    assert_eq!(begin_outcome.transaction_id, handle.session_id);
    assert_eq!(begin_outcome.request_id, "grpc-begin-request");
    assert_eq!(
        begin_outcome.state,
        grpc::proto::TransactionOperationStateV09::Running as i32
    );
    assert_eq!(
        begin_outcome.routing.as_ref().expect("routing").kind,
        grpc::proto::TransactionRoutingKindV09::LocalOnly as i32
    );

    let mut replay_begin = Request::new(grpc::proto::BeginRequest {
        request_id: Some("grpc-begin-request".to_owned()),
        require_distributed: Some(false),
    });
    replay_begin
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let replay_begin = client
        .begin_transaction(replay_begin)
        .await
        .expect("same begin request must replay")
        .into_inner();
    assert_eq!(replay_begin.session_id, handle.session_id);
    assert_eq!(
        replay_begin
            .transaction
            .as_ref()
            .and_then(|outcome| outcome.idempotency.as_ref())
            .expect("replayed begin idempotency")
            .duplicate_count,
        1
    );

    let mut conflicting_begin = Request::new(grpc::proto::BeginRequest {
        request_id: Some("grpc-begin-request".to_owned()),
        require_distributed: None,
    });
    conflicting_begin
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let conflicting_begin = client
        .begin_transaction(conflicting_begin)
        .await
        .expect_err("same request id with a different payload must conflict");
    assert_eq!(conflicting_begin.code(), Code::Aborted);
    let conflicting_outcome =
        grpc::proto::TransactionOutcomeV09::decode(conflicting_begin.details())
            .expect("conflict has transaction outcome details");
    assert_eq!(
        conflicting_outcome.failure_class,
        grpc::proto::TransactionFailureClassV09::Conflict as i32
    );
    assert_eq!(conflicting_outcome.reason_code, "idempotency_conflict");

    // A v0.8 client only sends the original fields. The additional request
    // identity/outcome fields are optional on the wire and remain absent here.
    let mut commit = Request::new(grpc::proto::TransactionHandle {
        session_id: handle.session_id.clone(),
        expires_at_ms: handle.expires_at_ms,
        request_id: None,
        transaction: None,
        require_distributed: None,
    });
    commit
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let commit = client
        .commit_transaction(commit)
        .await
        .expect("commit")
        .into_inner();
    assert!(commit.success, "legacy success");
    assert_eq!(
        commit.transaction.expect("additive outcome").state,
        grpc::proto::TransactionOperationStateV09::Committed as i32
    );

    let mut sql = Request::new(grpc::proto::SqlRequest {
        sql: "SELECT 1".to_owned(),
        session_id: String::new(),
        request_id: Some("grpc-sql-request".to_owned()),
        require_distributed: Some(false),
    });
    sql.metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let mut stream = client
        .execute_sql(sql)
        .await
        .expect("SQL stream")
        .into_inner();
    let result = stream
        .message()
        .await
        .expect("stream status")
        .expect("result set");
    assert!(!result.columns.is_empty(), "legacy result columns");
    let sql_outcome = result.transaction.expect("additive outcome");
    assert_eq!(sql_outcome.request_id, "grpc-sql-request");
    assert_eq!(
        sql_outcome.state,
        grpc::proto::TransactionOperationStateV09::Committed as i32
    );

    let mut distributed_begin = Request::new(grpc::proto::BeginRequest {
        request_id: Some("grpc-distributed-begin".to_owned()),
        require_distributed: Some(true),
    });
    distributed_begin
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let distributed_begin = client
        .begin_transaction(distributed_begin)
        .await
        .expect_err("unavailable distributed begin must be blocked before execution");
    assert_eq!(distributed_begin.code(), Code::Unavailable);
    let distributed_begin_outcome =
        grpc::proto::TransactionOutcomeV09::decode(distributed_begin.details())
            .expect("blocked begin outcome");
    assert_eq!(
        distributed_begin_outcome.failure_class,
        grpc::proto::TransactionFailureClassV09::PrerequisiteMissing as i32
    );
    assert_eq!(
        distributed_begin_outcome
            .routing
            .expect("blocked routing")
            .kind,
        grpc::proto::TransactionRoutingKindV09::Blocked as i32
    );

    let mut distributed_ddl = Request::new(grpc::proto::DdlRequest {
        sql: "CREATE TABLE distributed_ddl_is_not_supported (id INT PRIMARY KEY)".to_owned(),
        session_id: String::new(),
        request_id: Some("grpc-distributed-ddl".to_owned()),
        require_distributed: Some(true),
    });
    distributed_ddl
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let distributed_ddl = client
        .execute_ddl(distributed_ddl)
        .await
        .expect_err("distributed DDL must be unsupported before execution");
    assert_eq!(distributed_ddl.code(), Code::Unimplemented);
    let distributed_ddl_outcome =
        grpc::proto::TransactionOutcomeV09::decode(distributed_ddl.details())
            .expect("unsupported DDL outcome");
    assert_eq!(
        distributed_ddl_outcome.failure_class,
        grpc::proto::TransactionFailureClassV09::InvalidRequest as i32
    );
    assert_eq!(
        distributed_ddl_outcome
            .routing
            .expect("unsupported routing")
            .kind,
        grpc::proto::TransactionRoutingKindV09::Unsupported as i32
    );

    let mut distributed_vector = Request::new(grpc::proto::VectorSearchRequest {
        table: "not_reached".to_owned(),
        vector: vec![0.0],
        k: 1,
        index: String::new(),
        column: String::new(),
        request_id: Some("grpc-distributed-vector".to_owned()),
        require_distributed: Some(true),
    });
    distributed_vector
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let distributed_vector = client
        .vector_search(distributed_vector)
        .await
        .expect_err("distributed vector search must be blocked before local fallback");
    assert_eq!(distributed_vector.code(), Code::Unavailable);
    let distributed_vector_outcome =
        grpc::proto::TransactionOutcomeV09::decode(distributed_vector.details())
            .expect("blocked vector outcome");
    assert_eq!(
        distributed_vector_outcome.failure_class,
        grpc::proto::TransactionFailureClassV09::PrerequisiteMissing as i32
    );

    // Unknown enum values remain values on the protobuf wire; callers must
    // reject them rather than interpreting them as a known success state.
    let unknown_state = grpc::proto::TransactionOutcomeV09::decode(&[0x58, 0x7f][..])
        .expect("decode unknown enum value");
    assert_eq!(unknown_state.state, 127);
    assert_ne!(
        unknown_state.state,
        grpc::proto::TransactionOperationStateV09::Committed as i32
    );

    let _ = shutdown.send(());
    server_handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn explicit_grpc_requests_replay_first_duplicate_and_post_restart_results() {
    let data = tempdir().expect("durable gRPC data directory");
    let state = build_state_at(data.path());
    let (channel, shutdown, server_handle) = spawn_network_grpc_server(state.clone()).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    client
        .execute_ddl(authorized_request(grpc::proto::DdlRequest {
            sql: "CREATE TABLE grpc_restart_items (id INT PRIMARY KEY)".to_owned(),
            session_id: String::new(),
            request_id: None,
            require_distributed: None,
        }))
        .await
        .expect("restart fixture table");

    let autocommit_dml = grpc::proto::DmlRequest {
        sql: "INSERT INTO grpc_restart_items (id) VALUES (1)".to_owned(),
        session_id: String::new(),
        request_id: Some("restart-autocommit-dml".to_owned()),
        require_distributed: Some(false),
    };
    let first_autocommit = client
        .execute_dml(authorized_request(autocommit_dml.clone()))
        .await
        .expect("first autocommit DML")
        .into_inner();
    assert_eq!(first_autocommit.affected_rows, 1);
    assert_eq!(
        transaction_duplicate_count(&first_autocommit.transaction),
        0
    );
    let duplicate_autocommit = client
        .execute_dml(authorized_request(autocommit_dml.clone()))
        .await
        .expect("duplicate autocommit DML")
        .into_inner();
    assert_eq!(duplicate_autocommit.affected_rows, 1);
    assert_eq!(
        transaction_duplicate_count(&duplicate_autocommit.transaction),
        1
    );

    let begin_request = grpc::proto::BeginRequest {
        request_id: Some("restart-begin".to_owned()),
        require_distributed: Some(false),
    };
    let first_begin = client
        .begin_transaction(authorized_request(begin_request.clone()))
        .await
        .expect("first begin")
        .into_inner();
    assert_eq!(
        first_begin
            .transaction
            .as_ref()
            .expect("begin outcome")
            .state,
        grpc::proto::TransactionOperationStateV09::Running as i32
    );
    let duplicate_begin = client
        .begin_transaction(authorized_request(begin_request.clone()))
        .await
        .expect("duplicate begin")
        .into_inner();
    assert_eq!(duplicate_begin.session_id, first_begin.session_id);
    assert_eq!(transaction_duplicate_count(&duplicate_begin.transaction), 1);

    let session_sql = grpc::proto::SqlRequest {
        sql: "SELECT 1".to_owned(),
        session_id: first_begin.session_id.clone(),
        request_id: Some("restart-session-sql".to_owned()),
        require_distributed: Some(false),
    };
    let first_session_sql = execute_single_sql(&mut client, session_sql.clone()).await;
    assert_eq!(
        first_session_sql
            .transaction
            .as_ref()
            .expect("session SQL outcome")
            .state,
        grpc::proto::TransactionOperationStateV09::Running as i32
    );
    let duplicate_session_sql = execute_single_sql(&mut client, session_sql.clone()).await;
    assert_eq!(
        transaction_duplicate_count(&duplicate_session_sql.transaction),
        1
    );

    let commit_handle = grpc::proto::TransactionHandle {
        session_id: first_begin.session_id.clone(),
        expires_at_ms: first_begin.expires_at_ms,
        request_id: Some("restart-commit".to_owned()),
        transaction: first_begin.transaction.clone(),
        require_distributed: Some(false),
    };
    let first_commit = client
        .commit_transaction(authorized_request(commit_handle.clone()))
        .await
        .expect("first commit")
        .into_inner();
    assert_eq!(
        first_commit
            .transaction
            .as_ref()
            .expect("commit outcome")
            .state,
        grpc::proto::TransactionOperationStateV09::Committed as i32
    );
    let duplicate_commit = client
        .commit_transaction(authorized_request(commit_handle.clone()))
        .await
        .expect("duplicate commit")
        .into_inner();
    assert_eq!(
        transaction_duplicate_count(&duplicate_commit.transaction),
        1
    );

    let rollback_begin = client
        .begin_transaction(authorized_request(grpc::proto::BeginRequest {
            request_id: Some("restart-rollback-begin".to_owned()),
            require_distributed: Some(false),
        }))
        .await
        .expect("begin for rollback")
        .into_inner();
    let rollback_handle = grpc::proto::TransactionHandle {
        session_id: rollback_begin.session_id.clone(),
        expires_at_ms: rollback_begin.expires_at_ms,
        request_id: Some("restart-rollback".to_owned()),
        transaction: rollback_begin.transaction.clone(),
        require_distributed: Some(false),
    };
    let first_rollback = client
        .rollback_transaction(authorized_request(rollback_handle.clone()))
        .await
        .expect("first rollback")
        .into_inner();
    assert_eq!(
        first_rollback
            .transaction
            .as_ref()
            .expect("rollback outcome")
            .state,
        grpc::proto::TransactionOperationStateV09::Cancelled as i32
    );
    let duplicate_rollback = client
        .rollback_transaction(authorized_request(rollback_handle.clone()))
        .await
        .expect("duplicate rollback")
        .into_inner();
    assert_eq!(
        transaction_duplicate_count(&duplicate_rollback.transaction),
        1
    );

    let blocked_begin = grpc::proto::BeginRequest {
        request_id: Some("restart-rejected-begin".to_owned()),
        require_distributed: Some(true),
    };
    let first_blocked_begin = client
        .begin_transaction(authorized_request(blocked_begin.clone()))
        .await
        .expect_err("first distributed begin must be rejected");
    assert_eq!(
        transaction_duplicate_count(&Some(assert_transaction_failure(
            first_blocked_begin,
            Code::Unavailable,
            grpc::proto::TransactionFailureClassV09::PrerequisiteMissing,
            grpc::proto::TransactionRoutingKindV09::Blocked,
        ))),
        0,
    );
    let duplicate_blocked_begin = client
        .begin_transaction(authorized_request(blocked_begin.clone()))
        .await
        .expect_err("duplicate distributed begin must replay");
    assert_eq!(
        transaction_duplicate_count(&Some(assert_transaction_failure(
            duplicate_blocked_begin,
            Code::Unavailable,
            grpc::proto::TransactionFailureClassV09::PrerequisiteMissing,
            grpc::proto::TransactionRoutingKindV09::Blocked,
        ))),
        1,
    );

    let unsupported_ddl = grpc::proto::DdlRequest {
        sql: "CREATE TABLE restart_rejected_ddl (id INT PRIMARY KEY)".to_owned(),
        session_id: String::new(),
        request_id: Some("restart-rejected-ddl".to_owned()),
        require_distributed: Some(true),
    };
    let first_unsupported_ddl = client
        .execute_ddl(authorized_request(unsupported_ddl.clone()))
        .await
        .expect_err("first distributed DDL must be rejected");
    assert_eq!(
        transaction_duplicate_count(&Some(assert_transaction_failure(
            first_unsupported_ddl,
            Code::Unimplemented,
            grpc::proto::TransactionFailureClassV09::InvalidRequest,
            grpc::proto::TransactionRoutingKindV09::Unsupported,
        ))),
        0,
    );
    let duplicate_unsupported_ddl = client
        .execute_ddl(authorized_request(unsupported_ddl.clone()))
        .await
        .expect_err("duplicate distributed DDL must replay");
    assert_eq!(
        transaction_duplicate_count(&Some(assert_transaction_failure(
            duplicate_unsupported_ddl,
            Code::Unimplemented,
            grpc::proto::TransactionFailureClassV09::InvalidRequest,
            grpc::proto::TransactionRoutingKindV09::Unsupported,
        ))),
        1,
    );

    let _ = shutdown.send(());
    server_handle.await.expect("first gRPC server shutdown");
    drop(client);
    drop(state);

    let restarted_state = build_state_at(data.path());
    let (restarted_channel, restarted_shutdown, restarted_handle) =
        spawn_network_grpc_server(restarted_state.clone()).await;
    let mut restarted =
        grpc::proto::alopex_service_client::AlopexServiceClient::new(restarted_channel);

    let restarted_autocommit = restarted
        .execute_dml(authorized_request(autocommit_dml))
        .await
        .expect("post-restart autocommit DML replay")
        .into_inner();
    assert_eq!(restarted_autocommit.affected_rows, 1);
    assert_eq!(
        transaction_duplicate_count(&restarted_autocommit.transaction),
        2
    );

    let restarted_blocked_begin = restarted
        .begin_transaction(authorized_request(blocked_begin))
        .await
        .expect_err("post-restart distributed begin rejection must replay");
    assert_eq!(
        transaction_duplicate_count(&Some(assert_transaction_failure(
            restarted_blocked_begin,
            Code::Unavailable,
            grpc::proto::TransactionFailureClassV09::PrerequisiteMissing,
            grpc::proto::TransactionRoutingKindV09::Blocked,
        ))),
        2,
    );
    let restarted_unsupported_ddl = restarted
        .execute_ddl(authorized_request(unsupported_ddl))
        .await
        .expect_err("post-restart distributed DDL rejection must replay");
    assert_eq!(
        transaction_duplicate_count(&Some(assert_transaction_failure(
            restarted_unsupported_ddl,
            Code::Unimplemented,
            grpc::proto::TransactionFailureClassV09::InvalidRequest,
            grpc::proto::TransactionRoutingKindV09::Unsupported,
        ))),
        2,
    );

    let restarted_begin = restarted
        .begin_transaction(authorized_request(begin_request))
        .await
        .expect_err("post-restart local begin must not return a stale running handle");
    assert_session_expired(restarted_begin, &first_begin.session_id, "restart-begin");

    let restarted_session_sql = restarted
        .execute_sql(authorized_request(session_sql))
        .await
        .expect_err("post-restart duplicate session SQL must expire safely");
    assert_session_expired(
        restarted_session_sql,
        &first_begin.session_id,
        "restart-session-sql",
    );

    let stale_handle_sql = restarted
        .execute_sql(authorized_request(grpc::proto::SqlRequest {
            sql: "SELECT 1".to_owned(),
            session_id: first_begin.session_id.clone(),
            request_id: Some("restart-new-session-sql".to_owned()),
            require_distributed: Some(false),
        }))
        .await
        .expect_err("a restarted handle must reject a new session operation");
    assert_session_expired(
        stale_handle_sql,
        &first_begin.session_id,
        "restart-new-session-sql",
    );

    let restarted_commit = restarted
        .commit_transaction(authorized_request(commit_handle))
        .await
        .expect("post-restart commit replay")
        .into_inner();
    assert_eq!(
        restarted_commit
            .transaction
            .as_ref()
            .expect("post-restart commit outcome")
            .state,
        grpc::proto::TransactionOperationStateV09::Committed as i32
    );
    assert_eq!(
        transaction_duplicate_count(&restarted_commit.transaction),
        2
    );

    let restarted_rollback = restarted
        .rollback_transaction(authorized_request(rollback_handle))
        .await
        .expect("post-restart rollback replay")
        .into_inner();
    assert_eq!(
        restarted_rollback
            .transaction
            .as_ref()
            .expect("post-restart rollback outcome")
            .state,
        grpc::proto::TransactionOperationStateV09::Cancelled as i32
    );
    assert_eq!(
        transaction_duplicate_count(&restarted_rollback.transaction),
        2
    );

    let _ = restarted_shutdown.send(());
    restarted_handle
        .await
        .expect("restarted gRPC server shutdown");
    drop(restarted);
    drop(restarted_state);
}

#[tokio::test]
async fn old_grpc_clients_decode_new_server_response_bytes() {
    let (state, _temp) = build_state().await;
    let (channel, shutdown, server_handle) = spawn_network_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    let begin = client
        .begin_transaction(authorized_request(grpc::proto::BeginRequest::default()))
        .await
        .expect("begin")
        .into_inner();
    let legacy_begin = LegacyTransactionHandle::decode(begin.encode_to_vec().as_slice())
        .expect("v0.8 handle must ignore additive fields");
    assert_eq!(legacy_begin.session_id, begin.session_id);
    assert_eq!(legacy_begin.expires_at_ms, begin.expires_at_ms);

    let commit = client
        .commit_transaction(authorized_request(grpc::proto::TransactionHandle {
            session_id: legacy_begin.session_id,
            expires_at_ms: legacy_begin.expires_at_ms,
            request_id: None,
            transaction: None,
            require_distributed: None,
        }))
        .await
        .expect("commit")
        .into_inner();
    assert!(
        LegacySuccessResponse::decode(commit.encode_to_vec().as_slice())
            .expect("v0.8 commit response must ignore additive fields")
            .success
    );

    let rollback_handle = client
        .begin_transaction(authorized_request(grpc::proto::BeginRequest::default()))
        .await
        .expect("begin for rollback")
        .into_inner();
    let rollback = client
        .rollback_transaction(authorized_request(grpc::proto::TransactionHandle {
            session_id: rollback_handle.session_id,
            expires_at_ms: rollback_handle.expires_at_ms,
            request_id: None,
            transaction: None,
            require_distributed: None,
        }))
        .await
        .expect("rollback")
        .into_inner();
    assert!(
        LegacySuccessResponse::decode(rollback.encode_to_vec().as_slice())
            .expect("v0.8 rollback response must ignore additive fields")
            .success
    );

    let ddl = client
        .execute_ddl(authorized_request(grpc::proto::DdlRequest {
            sql: "CREATE TABLE legacy_wire_items (id INT PRIMARY KEY)".to_owned(),
            session_id: String::new(),
            request_id: None,
            require_distributed: None,
        }))
        .await
        .expect("legacy DDL")
        .into_inner();
    assert!(
        LegacySuccessResponse::decode(ddl.encode_to_vec().as_slice())
            .expect("v0.8 DDL response must decode")
            .success
    );

    let dml = client
        .execute_dml(authorized_request(grpc::proto::DmlRequest {
            sql: "INSERT INTO legacy_wire_items (id) VALUES (1)".to_owned(),
            session_id: String::new(),
            request_id: None,
            require_distributed: None,
        }))
        .await
        .expect("legacy DML")
        .into_inner();
    assert_eq!(
        LegacyDmlResponse::decode(dml.encode_to_vec().as_slice())
            .expect("v0.8 DML response must decode")
            .affected_rows,
        1
    );

    let mut stream = client
        .execute_sql(authorized_request(grpc::proto::SqlRequest {
            sql: "SELECT id FROM legacy_wire_items".to_owned(),
            session_id: String::new(),
            request_id: None,
            require_distributed: None,
        }))
        .await
        .expect("legacy SQL")
        .into_inner();
    let result = stream
        .message()
        .await
        .expect("legacy SQL stream status")
        .expect("legacy SQL result");
    let legacy_result = LegacySqlResultSet::decode(result.encode_to_vec().as_slice())
        .expect("v0.8 result set must ignore additive fields");
    assert!(!legacy_result.has_affected_rows);

    let _ = shutdown.send(());
    server_handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn every_g01_to_g13_explicit_distributed_request_fails_before_local_execution() {
    let (state, _temp) = build_state().await;
    let (channel, shutdown, server_handle) = spawn_network_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    client
        .execute_ddl(authorized_request(grpc::proto::DdlRequest {
            sql: "CREATE TABLE g_preflight_items (id INT PRIMARY KEY, embedding VECTOR(2, L2))"
                .to_owned(),
            session_id: String::new(),
            request_id: None,
            require_distributed: None,
        }))
        .await
        .expect("preflight fixture table");
    client
        .vector_upsert(authorized_request(grpc::proto::VectorUpsertRequest {
            table: "g_preflight_items".to_owned(),
            id: 1,
            vector: vec![0.0, 0.0],
            column: "embedding".to_owned(),
            request_id: Some("g-preflight-setup-vector".to_owned()),
            require_distributed: Some(false),
        }))
        .await
        .expect("preflight fixture vector");
    client
        .vector_index_create(authorized_request(grpc::proto::VectorIndexCreateRequest {
            name: "g_preflight_existing_index".to_owned(),
            table: "g_preflight_items".to_owned(),
            column: "embedding".to_owned(),
            method: "hnsw".to_owned(),
            options: Default::default(),
            if_not_exists: false,
            request_id: Some("g-preflight-setup-index".to_owned()),
            require_distributed: Some(false),
        }))
        .await
        .expect("preflight fixture index");

    macro_rules! assert_blocked {
        ($method:ident, $request:expr) => {{
            let request = $request;
            let error = client
                .$method(authorized_request(request.clone()))
                .await
                .expect_err(concat!(stringify!($method), " must be blocked"));
            let first = assert_transaction_failure(
                error,
                Code::Unavailable,
                grpc::proto::TransactionFailureClassV09::PrerequisiteMissing,
                grpc::proto::TransactionRoutingKindV09::Blocked,
            );
            assert_eq!(
                transaction_duplicate_count(&Some(first)),
                0,
                concat!(stringify!($method), " first rejection must be recorded"),
            );
            let replay = client
                .$method(authorized_request(request))
                .await
                .expect_err(concat!(stringify!($method), " rejection must replay"));
            let replay = assert_transaction_failure(
                replay,
                Code::Unavailable,
                grpc::proto::TransactionFailureClassV09::PrerequisiteMissing,
                grpc::proto::TransactionRoutingKindV09::Blocked,
            );
            assert_eq!(
                transaction_duplicate_count(&Some(replay)),
                1,
                concat!(
                    stringify!($method),
                    " rejected replay must increment duplicate count"
                ),
            );
        }};
    }
    macro_rules! assert_unsupported {
        ($method:ident, $request:expr) => {{
            let request = $request;
            let error = client
                .$method(authorized_request(request.clone()))
                .await
                .expect_err(concat!(stringify!($method), " must be unsupported"));
            let first = assert_transaction_failure(
                error,
                Code::Unimplemented,
                grpc::proto::TransactionFailureClassV09::InvalidRequest,
                grpc::proto::TransactionRoutingKindV09::Unsupported,
            );
            assert_eq!(
                transaction_duplicate_count(&Some(first)),
                0,
                concat!(stringify!($method), " first rejection must be recorded"),
            );
            let replay = client
                .$method(authorized_request(request))
                .await
                .expect_err(concat!(stringify!($method), " rejection must replay"));
            let replay = assert_transaction_failure(
                replay,
                Code::Unimplemented,
                grpc::proto::TransactionFailureClassV09::InvalidRequest,
                grpc::proto::TransactionRoutingKindV09::Unsupported,
            );
            assert_eq!(
                transaction_duplicate_count(&Some(replay)),
                1,
                concat!(
                    stringify!($method),
                    " rejected replay must increment duplicate count"
                ),
            );
        }};
    }

    assert_blocked!(
        begin_transaction,
        grpc::proto::BeginRequest {
            request_id: Some("g01".to_owned()),
            require_distributed: Some(true),
        }
    );
    assert_blocked!(
        commit_transaction,
        grpc::proto::TransactionHandle {
            session_id: "not-reached".to_owned(),
            request_id: Some("g02".to_owned()),
            require_distributed: Some(true),
            ..Default::default()
        }
    );
    assert_blocked!(
        rollback_transaction,
        grpc::proto::TransactionHandle {
            session_id: "not-reached".to_owned(),
            request_id: Some("g03".to_owned()),
            require_distributed: Some(true),
            ..Default::default()
        }
    );
    assert_blocked!(
        execute_sql,
        grpc::proto::SqlRequest {
            request_id: Some("g04".to_owned()),
            require_distributed: Some(true),
            ..Default::default()
        }
    );
    assert_blocked!(
        execute_dml,
        grpc::proto::DmlRequest {
            sql: "INSERT INTO g_preflight_items (id) VALUES (2)".to_owned(),
            request_id: Some("g05".to_owned()),
            require_distributed: Some(true),
            ..Default::default()
        }
    );
    assert_unsupported!(
        execute_ddl,
        grpc::proto::DdlRequest {
            sql: "CREATE TABLE g06_not_created (id INT PRIMARY KEY)".to_owned(),
            request_id: Some("g06".to_owned()),
            require_distributed: Some(true),
            ..Default::default()
        }
    );
    assert_blocked!(
        vector_search,
        grpc::proto::VectorSearchRequest {
            table: "g_preflight_items".to_owned(),
            vector: vec![0.0, 0.0],
            k: 1,
            column: "embedding".to_owned(),
            request_id: Some("g07".to_owned()),
            require_distributed: Some(true),
            ..Default::default()
        }
    );
    assert_blocked!(
        vector_upsert,
        grpc::proto::VectorUpsertRequest {
            table: "g_preflight_items".to_owned(),
            id: 2,
            vector: vec![1.0, 0.0],
            column: "embedding".to_owned(),
            request_id: Some("g08".to_owned()),
            require_distributed: Some(true),
        }
    );
    assert_blocked!(
        vector_delete,
        grpc::proto::VectorDeleteRequest {
            table: "g_preflight_items".to_owned(),
            id: 1,
            column: "embedding".to_owned(),
            request_id: Some("g09".to_owned()),
            require_distributed: Some(true),
        }
    );
    assert_unsupported!(
        vector_index_create,
        grpc::proto::VectorIndexCreateRequest {
            name: "g10_not_created".to_owned(),
            table: "g_preflight_items".to_owned(),
            column: "embedding".to_owned(),
            method: "hnsw".to_owned(),
            request_id: Some("g10".to_owned()),
            require_distributed: Some(true),
            ..Default::default()
        }
    );
    assert_unsupported!(
        vector_index_update,
        grpc::proto::VectorIndexUpdateRequest {
            name: "g_preflight_existing_index".to_owned(),
            table: "g_preflight_items".to_owned(),
            column: "embedding".to_owned(),
            method: "hnsw".to_owned(),
            request_id: Some("g11".to_owned()),
            require_distributed: Some(true),
            ..Default::default()
        }
    );
    assert_unsupported!(
        vector_index_delete,
        grpc::proto::VectorIndexDeleteRequest {
            name: "g_preflight_existing_index".to_owned(),
            if_exists: false,
            request_id: Some("g12".to_owned()),
            require_distributed: Some(true),
        }
    );
    assert_unsupported!(
        vector_index_compact,
        grpc::proto::VectorIndexCompactRequest {
            name: "g_preflight_existing_index".to_owned(),
            request_id: Some("g13".to_owned()),
            require_distributed: Some(true),
        }
    );

    let local_dml = client
        .execute_dml(authorized_request(grpc::proto::DmlRequest {
            sql: "INSERT INTO g_preflight_items (id, embedding) VALUES (2, [1.0, 0.0])".to_owned(),
            session_id: String::new(),
            request_id: Some("g-preflight-prove-dml".to_owned()),
            require_distributed: Some(false),
        }))
        .await
        .expect("blocked G05 must not insert a row")
        .into_inner();
    assert_eq!(local_dml.affected_rows, 1);
    client
        .execute_ddl(authorized_request(grpc::proto::DdlRequest {
            sql: "CREATE TABLE g06_not_created (id INT PRIMARY KEY)".to_owned(),
            session_id: String::new(),
            request_id: Some("g-preflight-prove-ddl".to_owned()),
            require_distributed: Some(false),
        }))
        .await
        .expect("blocked G06 must not create a table");
    let vector_rows = client
        .vector_search(authorized_request(grpc::proto::VectorSearchRequest {
            table: "g_preflight_items".to_owned(),
            vector: vec![0.0, 0.0],
            k: 3,
            index: String::new(),
            column: "embedding".to_owned(),
            request_id: Some("g-preflight-prove-vector".to_owned()),
            require_distributed: Some(false),
        }))
        .await
        .expect("blocked G08/G09 must not alter vectors")
        .into_inner()
        .results;
    assert_eq!(
        vector_rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![1, 2]
    );
    client
        .vector_index_create(authorized_request(grpc::proto::VectorIndexCreateRequest {
            name: "g10_not_created".to_owned(),
            table: "g_preflight_items".to_owned(),
            column: "embedding".to_owned(),
            method: "hnsw".to_owned(),
            options: Default::default(),
            if_not_exists: false,
            request_id: Some("g-preflight-prove-g10".to_owned()),
            require_distributed: Some(false),
        }))
        .await
        .expect("blocked G10 must not create an index");
    client
        .vector_index_update(authorized_request(grpc::proto::VectorIndexUpdateRequest {
            name: "g_preflight_existing_index".to_owned(),
            table: "g_preflight_items".to_owned(),
            column: "embedding".to_owned(),
            method: "hnsw".to_owned(),
            options: Default::default(),
            request_id: Some("g-preflight-prove-g12".to_owned()),
            require_distributed: Some(false),
        }))
        .await
        .expect("blocked G12 must not remove the existing index");
    client
        .vector_index_compact(authorized_request(grpc::proto::VectorIndexCompactRequest {
            name: "g_preflight_existing_index".to_owned(),
            request_id: Some("g-preflight-prove-g13".to_owned()),
            require_distributed: Some(false),
        }))
        .await
        .expect("blocked G13 must not invalidate the existing index");

    let _ = shutdown.send(());
    server_handle.await.expect("gRPC server shutdown");
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
async fn create_set_uses_authenticated_actor_and_canonical_set_outcome() {
    let (state, _temp) = build_state().await;
    let (channel, shutdown, handle) = spawn_network_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);
    let request_value = || grpc::proto::CreateSetRequest {
        object_id: "set-grpc".into(),
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
        request_id: "request-set-grpc".into(),
        operation_id: "operation-set-grpc".into(),
        update_version: 0,
    };
    let mut request = Request::new(request_value());
    request
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let outcome = client
        .create_set(request)
        .await
        .expect("Set create")
        .into_inner();
    assert_eq!(outcome.object_type, "set");
    assert_eq!(outcome.actor, "dev");
    assert_eq!(outcome.state, "committed");
    assert_eq!(outcome.routing_kind, "local_only");
    assert!(outcome.has_value);
    assert!(outcome.members.is_empty());
    assert!(outcome.member_versions.is_empty());
    assert_eq!(outcome.duplicate_count, 0);
    let mut replay = Request::new(request_value());
    replay
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    assert_eq!(
        client
            .create_set(replay)
            .await
            .expect("Set replay")
            .into_inner()
            .duplicate_count,
        1
    );
    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn read_set_uses_authenticated_actor_and_canonical_set_outcome() {
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
    let mut create = Request::new(grpc::proto::CreateSetRequest {
        object_id: "set-grpc".into(),
        range: Some(range.clone()),
        request_id: "request-set-grpc".into(),
        operation_id: "operation-set-grpc".into(),
        update_version: 0,
    });
    create
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    client
        .create_set(create)
        .await
        .expect("Set create before read");

    let mut request = Request::new(grpc::proto::ReadSetRequest {
        object_id: "set-grpc".into(),
        range: Some(range),
        request_id: "request-set-grpc-read".into(),
        operation_id: "operation-set-grpc-read".into(),
        update_version: 0,
    });
    request
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    let outcome = client
        .read_set(request)
        .await
        .expect("Set read")
        .into_inner();
    assert_eq!(outcome.object_type, "set");
    assert_eq!(outcome.object_id, "set-grpc");
    assert_eq!(outcome.actor, "dev");
    assert_eq!(outcome.request_id, "request-set-grpc-read");
    assert_eq!(outcome.operation_id, "operation-set-grpc-read");
    assert_eq!(outcome.state, "committed");
    assert_eq!(outcome.routing_kind, "local_only");
    assert!(outcome.has_value);
    assert!(outcome.members.is_empty());
    assert!(outcome.member_versions.is_empty());
    assert_eq!(outcome.first_outcome, "set_read");
    assert_eq!(outcome.duplicate_count, 0);

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn add_set_uses_authenticated_actor_and_canonical_set_outcome() {
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
    let mut create = Request::new(grpc::proto::CreateSetRequest {
        object_id: "set-grpc".into(),
        range: Some(range.clone()),
        request_id: "request-set-grpc".into(),
        operation_id: "operation-set-grpc".into(),
        update_version: 0,
    });
    create
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    client
        .create_set(create)
        .await
        .expect("Set create before add");

    let add = || {
        let mut request = Request::new(grpc::proto::AddSetRequest {
            object_id: "set-grpc".into(),
            range: Some(range.clone()),
            request_id: "request-set-grpc-add".into(),
            operation_id: "00000000-0000-0000-0000-000000000158".into(),
            update_version: 1,
            member: "alice".into(),
        });
        request
            .metadata_mut()
            .insert("x-api-key", "v09-key".parse().unwrap());
        request
    };
    let outcome = client.add_set(add()).await.expect("Set add").into_inner();
    assert_eq!(outcome.object_type, "set");
    assert_eq!(outcome.object_id, "set-grpc");
    assert_eq!(outcome.actor, "dev");
    assert_eq!(outcome.state, "committed");
    assert_eq!(outcome.routing_kind, "local_only");
    assert!(outcome.has_value);
    assert_eq!(outcome.members, vec!["alice"]);
    assert_eq!(outcome.member_versions.len(), 1);
    assert_eq!(outcome.member_versions[0].member, "alice");
    assert_eq!(outcome.member_versions[0].update_version, 1);
    assert_eq!(
        outcome.member_versions[0].operation_id,
        "00000000-0000-0000-0000-000000000158"
    );
    assert!(outcome.member_versions[0].present);
    assert_eq!(outcome.duplicate_count, 0);

    let replay = client
        .add_set(add())
        .await
        .expect("Set add replay")
        .into_inner();
    assert_eq!(replay.duplicate_count, 1);
    assert_eq!(replay.members, outcome.members);

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn remove_set_uses_authenticated_actor_and_canonical_set_outcome() {
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
    let mut create = Request::new(grpc::proto::CreateSetRequest {
        object_id: "set-grpc".into(),
        range: Some(range.clone()),
        request_id: "request-set-grpc".into(),
        operation_id: "operation-set-grpc".into(),
        update_version: 0,
    });
    create
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    client
        .create_set(create)
        .await
        .expect("Set create before remove");

    let mut add = Request::new(grpc::proto::AddSetRequest {
        object_id: "set-grpc".into(),
        range: Some(range.clone()),
        request_id: "request-set-grpc-add".into(),
        operation_id: "00000000-0000-0000-0000-000000000158".into(),
        update_version: 1,
        member: "alice".into(),
    });
    add.metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    client.add_set(add).await.expect("Set add before remove");

    let remove = || {
        let mut request = Request::new(grpc::proto::RemoveSetRequest {
            object_id: "set-grpc".into(),
            range: Some(range.clone()),
            request_id: "request-set-grpc-remove".into(),
            operation_id: "00000000-0000-0000-0000-000000000167".into(),
            update_version: 2,
            member: "alice".into(),
        });
        request
            .metadata_mut()
            .insert("x-api-key", "v09-key".parse().unwrap());
        request
    };
    let outcome = client
        .remove_set(remove())
        .await
        .expect("Set remove")
        .into_inner();
    assert_eq!(outcome.object_type, "set");
    assert_eq!(outcome.object_id, "set-grpc");
    assert_eq!(outcome.actor, "dev");
    assert_eq!(outcome.state, "committed");
    assert_eq!(outcome.routing_kind, "local_only");
    assert!(outcome.has_value);
    assert!(outcome.members.is_empty());
    assert_eq!(outcome.member_versions.len(), 1);
    assert_eq!(outcome.member_versions[0].member, "alice");
    assert_eq!(outcome.member_versions[0].update_version, 2);
    assert_eq!(
        outcome.member_versions[0].operation_id,
        "00000000-0000-0000-0000-000000000167"
    );
    assert!(!outcome.member_versions[0].present);
    assert_eq!(outcome.duplicate_count, 0);

    let replay = client
        .remove_set(remove())
        .await
        .expect("Set remove replay")
        .into_inner();
    assert_eq!(replay.duplicate_count, 1);
    assert_eq!(replay.members, outcome.members);

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn contains_set_uses_authenticated_actor_and_canonical_set_outcome() {
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
    let mut create = Request::new(grpc::proto::CreateSetRequest {
        object_id: "set-grpc".into(),
        range: Some(range.clone()),
        request_id: "request-set-grpc-create".into(),
        operation_id: "operation-set-grpc-create".into(),
        update_version: 0,
    });
    create
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    client
        .create_set(create)
        .await
        .expect("Set create before contains");

    let mut add = Request::new(grpc::proto::AddSetRequest {
        object_id: "set-grpc".into(),
        range: Some(range.clone()),
        request_id: "request-set-grpc-add".into(),
        operation_id: "00000000-0000-0000-0000-000000000158".into(),
        update_version: 1,
        member: "alice".into(),
    });
    add.metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    client.add_set(add).await.expect("Set add before contains");

    let contains = || {
        let mut request = Request::new(grpc::proto::ContainsSetRequest {
            object_id: "set-grpc".into(),
            range: Some(range.clone()),
            request_id: "request-set-grpc-contains".into(),
            operation_id: "operation-set-grpc-contains".into(),
            update_version: 0,
            member: "alice".into(),
        });
        request
            .metadata_mut()
            .insert("x-api-key", "v09-key".parse().unwrap());
        request
    };
    let outcome = client
        .contains_set(contains())
        .await
        .expect("Set contains")
        .into_inner();
    assert_eq!(outcome.object_type, "set");
    assert_eq!(outcome.object_id, "set-grpc");
    assert_eq!(outcome.actor, "dev");
    assert_eq!(outcome.state, "committed");
    assert_eq!(outcome.routing_kind, "local_only");
    assert!(outcome.has_value);
    assert_eq!(outcome.members, ["alice"]);
    assert_eq!(outcome.member_versions.len(), 1);
    assert_eq!(outcome.member_versions[0].member, "alice");
    assert!(outcome.member_versions[0].present);
    assert_eq!(outcome.duplicate_count, 0);

    let repeated = client
        .contains_set(contains())
        .await
        .expect("Set contains is read-only")
        .into_inner();
    assert_eq!(repeated.duplicate_count, 0);
    assert_eq!(repeated.members, outcome.members);

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn list_set_uses_authenticated_actor_and_canonical_set_outcome() {
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
    let mut create = Request::new(grpc::proto::CreateSetRequest {
        object_id: "set-grpc-list".into(),
        range: Some(range.clone()),
        request_id: "request-set-grpc-list-create".into(),
        operation_id: "operation-set-grpc-list-create".into(),
        update_version: 0,
    });
    create
        .metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    client
        .create_set(create)
        .await
        .expect("Set create before list");

    let mut add = Request::new(grpc::proto::AddSetRequest {
        object_id: "set-grpc-list".into(),
        range: Some(range.clone()),
        request_id: "request-set-grpc-list-add".into(),
        operation_id: "00000000-0000-0000-0000-000000000185".into(),
        update_version: 1,
        member: "alice".into(),
    });
    add.metadata_mut()
        .insert("x-api-key", "v09-key".parse().unwrap());
    client.add_set(add).await.expect("Set add before list");

    let list = || {
        let mut request = Request::new(grpc::proto::ListSetRequest {
            object_id: "set-grpc-list".into(),
            range: Some(range.clone()),
            request_id: "request-set-grpc-list".into(),
            operation_id: "operation-set-grpc-list".into(),
            update_version: 0,
        });
        request
            .metadata_mut()
            .insert("x-api-key", "v09-key".parse().unwrap());
        request
    };
    let outcome = client
        .list_set(list())
        .await
        .expect("Set list")
        .into_inner();
    assert_eq!(outcome.object_type, "set");
    assert_eq!(outcome.object_id, "set-grpc-list");
    assert_eq!(outcome.actor, "dev");
    assert_eq!(outcome.request_id, "request-set-grpc-list");
    assert_eq!(outcome.operation_id, "operation-set-grpc-list");
    assert_eq!(outcome.state, "committed");
    assert_eq!(outcome.routing_kind, "local_only");
    assert_eq!(outcome.range, Some(range.clone()));
    assert!(outcome.has_value);
    assert_eq!(outcome.members, ["alice"]);
    assert_eq!(outcome.member_versions.len(), 1);
    assert_eq!(outcome.member_versions[0].member, "alice");
    assert!(outcome.member_versions[0].present);
    assert_eq!(outcome.first_outcome, "set_list");
    assert_eq!(outcome.duplicate_count, 0);

    let repeated = client
        .list_set(list())
        .await
        .expect("Set list is read-only")
        .into_inner();
    assert_eq!(repeated.duplicate_count, 0);
    assert_eq!(repeated.members, outcome.members);

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

#[tokio::test]
async fn increment_counter_uses_the_authenticated_actor_and_replays_once() {
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
        .expect("counter create before increment");

    let increment = || {
        let mut request = Request::new(grpc::proto::IncrementCounterRequest {
            object_id: "counter-grpc".into(),
            range: Some(range.clone()),
            request_id: "request-grpc-increment".into(),
            operation_id: "operation-grpc-increment".into(),
            update_version: 1,
            delta: 3,
        });
        request
            .metadata_mut()
            .insert("x-api-key", "v09-key".parse().unwrap());
        request
    };
    let outcome = client
        .increment_counter(increment())
        .await
        .expect("counter increment")
        .into_inner();
    assert_eq!(outcome.object_type, "counter");
    assert_eq!(outcome.object_id, "counter-grpc");
    assert_eq!(outcome.actor, "dev");
    assert_eq!(outcome.state, "committed");
    assert_eq!(outcome.routing_kind, "local_only");
    assert_eq!(outcome.initial_value, -4);
    assert_eq!(outcome.accepted_delta_total, 3);
    assert_eq!(outcome.value, -1);
    assert_eq!(outcome.duplicate_count, 0);

    let replay = client
        .increment_counter(increment())
        .await
        .expect("counter increment replay")
        .into_inner();
    assert_eq!(replay.duplicate_count, 1);
    assert_eq!(replay.value, outcome.value);

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}

#[tokio::test]
async fn decrement_counter_uses_the_authenticated_actor_and_replays_once() {
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
        .expect("counter create before decrement");

    let decrement = || {
        let mut request = Request::new(grpc::proto::DecrementCounterRequest {
            object_id: "counter-grpc".into(),
            range: Some(range.clone()),
            request_id: "request-grpc-decrement".into(),
            operation_id: "operation-grpc-decrement".into(),
            update_version: 1,
            delta: 3,
        });
        request
            .metadata_mut()
            .insert("x-api-key", "v09-key".parse().unwrap());
        request
    };
    let outcome = client
        .decrement_counter(decrement())
        .await
        .expect("counter decrement")
        .into_inner();
    assert_eq!(outcome.object_type, "counter");
    assert_eq!(outcome.object_id, "counter-grpc");
    assert_eq!(outcome.actor, "dev");
    assert_eq!(outcome.state, "committed");
    assert_eq!(outcome.routing_kind, "local_only");
    assert_eq!(outcome.initial_value, -4);
    assert_eq!(outcome.accepted_delta_total, -3);
    assert_eq!(outcome.value, -7);
    assert_eq!(outcome.duplicate_count, 0);

    let replay = client
        .decrement_counter(decrement())
        .await
        .expect("counter decrement replay")
        .into_inner();
    assert_eq!(replay.duplicate_count, 1);
    assert_eq!(replay.value, outcome.value);

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}
