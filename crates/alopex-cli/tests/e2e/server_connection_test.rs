use std::sync::Arc;
use std::time::Duration;

use alopex_cli::client::http::HttpClient;
use alopex_cli::profile::config::{AuthType, ServerConfig};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{extract::State, Json};
use rcgen::generate_simple_self_signed;
use serde_json::json;
use tokio::sync::oneshot;

#[derive(Clone)]
struct AuthExpectation {
    exact: Option<String>,
    prefix: Option<&'static str>,
}

async fn auth_status_handler(
    State(expectation): State<Arc<AuthExpectation>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(expected) = &expectation.exact {
        match headers.get(header::AUTHORIZATION) {
            Some(value) if value == expected => {}
            _ => return StatusCode::UNAUTHORIZED,
        }
    }
    if let Some(prefix) = expectation.prefix {
        match headers.get(header::AUTHORIZATION) {
            Some(value) => {
                let value = value.to_str().unwrap_or_default();
                if !value.starts_with(prefix) {
                    return StatusCode::UNAUTHORIZED;
                }
            }
            None => return StatusCode::UNAUTHORIZED,
        }
    }

    Json(json!({"ok": true}))
}

async fn spawn_tls_server(router: axum::Router) -> (String, oneshot::Sender<()>) {
    let cert = generate_simple_self_signed(vec!["localhost".to_string()]).expect("cert");
    let dir = tempfile::tempdir().expect("tempdir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.serialize_pem().expect("cert pem")).expect("write cert");
    std::fs::write(&key_path, cert.serialize_private_key_pem()).expect("write key");

    let rustls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .expect("rustls config");

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = shutdown_rx.await;
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(5)));
    });

    let server = axum_server::bind_rustls(addr, rustls_config)
        .handle(handle)
        .serve(router.into_make_service());
    tokio::spawn(server);

    (format!("https://{}", addr), shutdown_tx)
}

fn build_client(config: &ServerConfig, identity: Option<reqwest::Identity>) -> HttpClient {
    let mut builder = reqwest::ClientBuilder::new()
        .danger_accept_invalid_certs(true)
        .use_rustls_tls();
    if let Some(identity) = identity {
        builder = builder.identity(identity);
    }
    let client = builder.build().expect("reqwest client");
    HttpClient::new_with_client(config, client).expect("http client")
}

fn build_identity() -> (reqwest::Identity, tempfile::TempDir, std::path::PathBuf, std::path::PathBuf)
{
    let cert = generate_simple_self_signed(vec!["client".to_string()]).expect("client cert");
    let cert_pem = cert.serialize_pem().expect("client pem");
    let key_pem = cert.serialize_private_key_pem();
    let mut combined = Vec::new();
    combined.extend_from_slice(cert_pem.as_bytes());
    if !combined.ends_with(b"\n") {
        combined.push(b'\n');
    }
    combined.extend_from_slice(key_pem.as_bytes());

    let dir = tempfile::tempdir().expect("tempdir");
    let cert_path = dir.path().join("client.pem");
    let key_path = dir.path().join("client-key.pem");
    std::fs::write(&cert_path, &cert_pem).expect("write client cert");
    std::fs::write(&key_path, &key_pem).expect("write client key");

    (
        reqwest::Identity::from_pem(&combined).expect("identity"),
        dir,
        cert_path,
        key_path,
    )
}

#[tokio::test]
async fn e2e_server_connection_token() {
    let expectation = Arc::new(AuthExpectation {
        exact: Some("Bearer test-token".to_string()),
        prefix: None,
    });
    let router = axum::Router::new()
        .route("/api/admin/status", get(auth_status_handler))
        .with_state(expectation);
    let (base_url, shutdown) = spawn_tls_server(router).await;

    let config = ServerConfig {
        url: base_url,
        auth: Some(AuthType::Token),
        token: Some("test-token".to_string()),
        username: None,
        password_command: None,
        cert_path: None,
        key_path: None,
    };
    let client = build_client(&config, None);
    let response: serde_json::Value = client
        .get_json("api/admin/status")
        .await
        .expect("status");
    assert!(response["ok"].as_bool().unwrap_or(false));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn e2e_server_connection_basic() {
    let expectation = Arc::new(AuthExpectation {
        exact: None,
        prefix: Some("Basic "),
    });
    let router = axum::Router::new()
        .route("/api/admin/status", get(auth_status_handler))
        .with_state(expectation);
    let (base_url, shutdown) = spawn_tls_server(router).await;

    let config = ServerConfig {
        url: base_url,
        auth: Some(AuthType::Basic),
        token: None,
        username: Some("alice".to_string()),
        password_command: Some("echo secret".to_string()),
        cert_path: None,
        key_path: None,
    };
    let client = build_client(&config, None);
    let response: serde_json::Value = client
        .get_json("api/admin/status")
        .await
        .expect("status");
    assert!(response["ok"].as_bool().unwrap_or(false));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn e2e_server_connection_mtls() {
    let expectation = Arc::new(AuthExpectation {
        exact: None,
        prefix: None,
    });
    let router = axum::Router::new()
        .route("/api/admin/status", get(auth_status_handler))
        .with_state(expectation);
    let (base_url, shutdown) = spawn_tls_server(router).await;

    let (identity, _dir, cert_path, key_path) = build_identity();
    let config = ServerConfig {
        url: base_url,
        auth: Some(AuthType::MTls),
        token: None,
        username: None,
        password_command: None,
        cert_path: Some(cert_path),
        key_path: Some(key_path),
    };
    let client = build_client(&config, Some(identity));
    let response: serde_json::Value = client
        .get_json("api/admin/status")
        .await
        .expect("status");
    assert!(response["ok"].as_bool().unwrap_or(false));

    let _ = shutdown.send(());
}
