use alopex_server::audit::AuditLogOutput;
use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tempfile::tempdir;
use tower::ServiceExt;

const I15_REGISTER: [&str; 8] = [
    "tls_tls12_tls13",
    "mtls_client_certificate",
    "api_key_auth",
    "session_lifecycle",
    "backpressure",
    "stream_timeout",
    "metrics_tracing",
    "audit_auth_failure",
];

#[tokio::test]
async fn i15_auth_failure_is_audited_with_the_traced_correlation_id() {
    let temp = tempdir().expect("tempdir");
    let audit_path = temp.path().join("audit.jsonl");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().join("data"),
        auth_mode: AuthMode::Dev {
            api_key: "v09-key".to_owned(),
        },
        audit_log_enabled: true,
        audit_log_output: AuditLogOutput::File {
            path: audit_path.clone(),
        },
        ..ServerConfig::default()
    })
    .expect("server");

    let response = http::router(server.state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/session/begin")
                .header("x-correlation-id", "v09-audit-trace")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let error: Value = serde_json::from_slice(&body).expect("error JSON");
    assert_eq!(error["error"]["code"], "UNAUTHORIZED");
    assert_eq!(error["error"]["correlation_id"], "v09-audit-trace");

    server.state.audit.flush().expect("flush audit");
    let lines = std::fs::read_to_string(audit_path).expect("audit log");
    let entry: Value = serde_json::from_str(lines.trim()).expect("audit JSON");
    assert_eq!(entry["event_type"], "auth_failure");
    assert_eq!(entry["target"], "auth");
    assert_eq!(entry["correlation_id"], "v09-audit-trace");
    assert_eq!(I15_REGISTER.len(), 8, "the I-15 register drifted");
}
