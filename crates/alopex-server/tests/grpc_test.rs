use std::sync::Arc;

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::grpc;
use alopex_server::server::ServerState;
use alopex_server::Server;
use std::pin::Pin;
use std::task::{Context, Poll};
use tempfile::tempdir;

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
                Ok::<_, std::io::Error>(stream)
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
    while let Some(row) = stream.message().await.expect("row") {
        let value = row.values.first().expect("value");
        ids.push(extract_int(value).expect("int"));
    }
    assert_eq!(ids, vec![1, 2, 3]);
}

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
