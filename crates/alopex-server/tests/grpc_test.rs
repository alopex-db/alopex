use std::sync::Arc;

use alopex_cluster::{
    ClusterId, ClusterIdentity, ClusterManager, ClusterManagerConfig, Endpoint, MemberIdentity,
    MemberStatus, MembershipSource, MembershipView, NodeId, NodeRole, NodeState, PlacementMetadata,
    RoutingTarget,
};
use alopex_server::auth::AuthMode;
use alopex_server::config::{ClusterServerConfig, ServerConfig};
use alopex_server::grpc;
use alopex_server::server::ServerState;
use alopex_server::Server;
use std::pin::Pin;
use std::task::{Context, Poll};
use tempfile::tempdir;

use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::Connected;
use tonic::transport::Server as TonicServer;
use tonic::transport::{Channel, Uri};
use tonic::Code;
use tower::service_fn;

async fn build_state(auth_mode: AuthMode) -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode,
        audit_log_enabled: false,
        ..ServerConfig::default()
    };
    let server = Server::new(config).expect("server");
    (server.state, temp)
}

async fn build_cluster_aware_state() -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::None,
        audit_log_enabled: false,
        cluster: ClusterServerConfig {
            mode: alopex_cluster::ClusterMode::ClusterAware,
            node_id: Some("node-a".to_string()),
            cluster_id: Some("cluster-a".to_string()),
            advertised_endpoint: Some("127.0.0.1:7001".to_string()),
            role: NodeRole::Worker,
            lifecycle_state: NodeState::Active,
            ..ClusterServerConfig::default()
        },
        ..ServerConfig::default()
    };
    let server = Server::new(config).expect("server");
    (server.state, temp)
}

fn table_ref_and_id(state: &ServerState, table_name: &str) -> (String, u32) {
    let guard = state.catalog.read().expect("catalog lock");
    let table = guard
        .list_tables()
        .into_iter()
        .find(|table| table.name == table_name)
        .expect("table metadata");
    (
        format!(
            "{}.{}.{}",
            table.catalog_name, table.namespace_name, table.name
        ),
        table.table_id,
    )
}

fn install_multi_node_placement(state: &ServerState, table_ref: &str, table_id: u32) {
    let mut placement = PlacementMetadata::new(table_ref, table_id, 7);
    placement
        .targets
        .push(RoutingTarget::table("node-a", table_ref, table_id));
    placement
        .targets
        .push(RoutingTarget::table("node-b", table_ref, table_id));

    let identity = ClusterIdentity {
        cluster_id: Some(ClusterId::new("cluster-a")),
        advertised_endpoint: Some(Endpoint::new("127.0.0.1:7001")),
        ..ClusterIdentity::new("node-a", NodeRole::Worker, NodeState::Active)
    };
    let mut membership = MembershipView::new(MembershipSource::Persisted, 7);
    membership.members.push(member("node-a"));
    membership.members.push(member("node-b"));

    let mut config = ClusterManagerConfig::cluster_aware(identity);
    config.membership_source = MembershipSource::Persisted;
    config.initial_membership = Some(membership);
    config.initial_placements = vec![placement];

    let manager = ClusterManager::new(config).expect("cluster manager");
    *state.cluster_manager.write().expect("cluster manager lock") = manager;
}

fn member(node_id: &str) -> MemberStatus {
    let endpoint = match node_id {
        "node-a" => "127.0.0.1:7001",
        "node-b" => "127.0.0.1:7002",
        _ => "127.0.0.1:7999",
    };
    MemberStatus {
        identity: MemberIdentity {
            node_id: NodeId::new(node_id),
            cluster_id: Some(ClusterId::new("cluster-a")),
            advertised_endpoint: Some(Endpoint::new(endpoint)),
            role: NodeRole::Worker,
        },
        raw_reachability_state: None,
        derived_state: NodeState::Active,
        transition_reason: Some("test".to_string()),
    }
}

struct InMemoryStream {
    inner: tokio::io::DuplexStream,
}

impl AsyncRead for InMemoryStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for InMemoryStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, data)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl Connected for InMemoryStream {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

async fn spawn_grpc_server(state: Arc<ServerState>) -> (Channel, tokio::task::JoinHandle<()>) {
    let (client_stream, server_stream) = tokio::io::duplex(1024);
    let (tx, rx) = mpsc::channel::<Result<InMemoryStream, std::io::Error>>(1);
    tx.send(Ok(InMemoryStream {
        inner: server_stream,
    }))
    .await
    .expect("send stream");

    let service = grpc::service(state);
    let handle = tokio::spawn(async move {
        let incoming = ReceiverStream::new(rx);
        TonicServer::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .expect("serve");
    });

    let client_stream = std::sync::Arc::new(Mutex::new(Some(InMemoryStream {
        inner: client_stream,
    })));
    let channel = Channel::from_static("http://[::]:50051")
        .connect_with_connector(service_fn(move |_uri: Uri| {
            let client_stream = client_stream.clone();
            async move {
                let mut guard = client_stream.lock().await;
                let stream = guard.take().expect("client stream");
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .expect("channel");

    (channel, handle)
}

fn extract_int(value: &grpc::proto::Value) -> Option<i64> {
    match &value.kind {
        Some(grpc::proto::value::Kind::IntValue(v)) => Some(*v as i64),
        Some(grpc::proto::value::Kind::BigintValue(v)) => Some(*v),
        _ => None,
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn grpc_sql_vector_transaction_flow() {
    let (state, _temp) = build_state(AuthMode::None).await;
    let (channel, _handle) = spawn_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    client
        .execute_ddl(grpc::proto::DdlRequest {
            sql: "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2, L2));".to_string(),
            session_id: String::new(),
        })
        .await
        .expect("ddl");
    let dml = client
        .execute_dml(grpc::proto::DmlRequest {
            sql: "INSERT INTO items (id, embedding) VALUES (1, [0.0, 0.0]);".to_string(),
            session_id: String::new(),
        })
        .await
        .expect("dml");
    assert_eq!(dml.into_inner().affected_rows, 1);

    client
        .vector_upsert(grpc::proto::VectorUpsertRequest {
            table: "items".to_string(),
            id: 2,
            vector: vec![1.0, 0.0],
            column: String::new(),
        })
        .await
        .expect("vector upsert");

    let search = client
        .vector_search(grpc::proto::VectorSearchRequest {
            table: "items".to_string(),
            vector: vec![0.9, 0.0],
            k: 2,
            index: String::new(),
            column: String::new(),
        })
        .await
        .expect("vector search")
        .into_inner();
    assert_eq!(search.results.len(), 2);

    let txn = client
        .begin_transaction(grpc::proto::BeginRequest {})
        .await
        .expect("begin")
        .into_inner();
    client
        .execute_dml(grpc::proto::DmlRequest {
            sql: "INSERT INTO items (id, embedding) VALUES (3, [0.2, 0.0]);".to_string(),
            session_id: txn.session_id.clone(),
        })
        .await
        .expect("dml");
    client
        .commit_transaction(txn.clone())
        .await
        .expect("commit");

    let rollback_txn = client
        .begin_transaction(grpc::proto::BeginRequest {})
        .await
        .expect("begin")
        .into_inner();
    client
        .execute_dml(grpc::proto::DmlRequest {
            sql: "INSERT INTO items (id, embedding) VALUES (4, [0.4, 0.0]);".to_string(),
            session_id: rollback_txn.session_id.clone(),
        })
        .await
        .expect("dml");
    client
        .rollback_transaction(rollback_txn.clone())
        .await
        .expect("rollback");

    let mut stream = client
        .execute_sql(grpc::proto::SqlRequest {
            sql: "SELECT id FROM items ORDER BY id;".to_string(),
            session_id: String::new(),
        })
        .await
        .expect("query")
        .into_inner();

    let mut ids = Vec::new();
    while let Some(result_set) = stream.message().await.expect("result set") {
        for row in result_set.rows {
            let value = row.values.first().expect("value");
            ids.push(extract_int(value).expect("int"));
        }
    }
    assert_eq!(ids, vec![1, 2, 3]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn grpc_multi_statement_returns_result_per_statement() {
    let (state, _temp) = build_state(AuthMode::None).await;
    let (channel, _handle) = spawn_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    let mut stream = client
        .execute_sql(grpc::proto::SqlRequest {
            sql: "SELECT 1; SELECT 2;".to_string(),
            session_id: String::new(),
        })
        .await
        .expect("query")
        .into_inner();

    let mut values = Vec::new();
    while let Some(result_set) = stream.message().await.expect("result set") {
        assert_eq!(result_set.columns.len(), 1);
        assert_eq!(result_set.rows.len(), 1);
        let value = result_set.rows[0].values.first().expect("value");
        values.push(extract_int(value).expect("integer result"));
    }
    assert_eq!(values, vec![1, 2]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn grpc_invalid_sql_returns_invalid_argument() {
    let (state, _temp) = build_state(AuthMode::None).await;
    let (channel, _handle) = spawn_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    let err = client
        .execute_dml(grpc::proto::DmlRequest {
            sql: String::new(),
            session_id: String::new(),
        })
        .await
        .expect_err("invalid sql");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn grpc_non_session_dml_rejects_future_distributed_routing() {
    let (state, _temp) = build_cluster_aware_state().await;
    let (channel, _handle) = spawn_grpc_server(state.clone()).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    client
        .execute_ddl(grpc::proto::DdlRequest {
            sql: "CREATE TABLE grpc_distributed_users (id INT PRIMARY KEY, name TEXT);".to_string(),
            session_id: String::new(),
        })
        .await
        .expect("ddl");
    let (table_ref, table_id) = table_ref_and_id(&state, "grpc_distributed_users");
    install_multi_node_placement(&state, &table_ref, table_id);

    let err = client
        .execute_dml(grpc::proto::DmlRequest {
            sql: "INSERT INTO grpc_distributed_users (id, name) VALUES (1, 'blocked');".to_string(),
            session_id: String::new(),
        })
        .await
        .expect_err("future distributed routing");
    assert_eq!(err.code(), Code::Unimplemented);
    assert!(err.message().contains("FutureDistributedExecutionRequired"));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[tokio::test]
async fn grpc_health_returns_ok() {
    let (state, _temp) = build_state(AuthMode::None).await;
    let (channel, _handle) = spawn_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    let response = client
        .health(grpc::proto::HealthRequest {})
        .await
        .expect("health")
        .into_inner();
    assert_eq!(response.status, "ok");
}

#[tokio::test]
async fn grpc_cluster_status_matches_server_snapshot_schema() {
    let (state, _temp) = build_cluster_aware_state().await;
    let expected = serde_json::to_value(state.cluster_status_snapshot().expect("snapshot"))
        .expect("expected snapshot json");
    let (channel, _handle) = spawn_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    let response = client
        .cluster_status(grpc::proto::ClusterStatusRequest {})
        .await
        .expect("cluster status")
        .into_inner();
    let actual: serde_json::Value =
        serde_json::from_str(&response.cluster_json).expect("cluster json");
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn grpc_cluster_join_and_leave_return_operation_and_status() {
    let (state, _temp) = build_cluster_aware_state().await;
    let (channel, _handle) = spawn_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    let joined = client
        .cluster_join(grpc::proto::ClusterJoinRequest {})
        .await
        .expect("cluster join")
        .into_inner();
    assert_eq!(joined.action, "join");
    let joined_json: serde_json::Value =
        serde_json::from_str(&joined.cluster_json).expect("join cluster json");
    assert_eq!(joined_json["mode"], "cluster_aware");
    assert_eq!(joined_json["identity"]["lifecycle_state"], "active");

    let left = client
        .cluster_leave(grpc::proto::ClusterLeaveRequest {})
        .await
        .expect("cluster leave")
        .into_inner();
    assert_eq!(left.action, "leave");
    let left_json: serde_json::Value =
        serde_json::from_str(&left.cluster_json).expect("leave cluster json");
    assert_eq!(left_json["identity"]["lifecycle_state"], "leaving");
}

#[tokio::test]
async fn grpc_cluster_join_rejects_single_node_mode() {
    let (state, _temp) = build_state(AuthMode::None).await;
    let (channel, _handle) = spawn_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    let err = client
        .cluster_join(grpc::proto::ClusterJoinRequest {})
        .await
        .expect_err("single-node cluster join");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("cluster_aware mode"));
}
