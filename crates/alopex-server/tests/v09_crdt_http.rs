use alopex_cluster::RangeIdentity;
use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tempfile::tempdir;
use tower::ServiceExt;

fn request(payload: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/crdt/counters")
        .header("content-type", "application/json")
        .header("x-api-key", "crdt-key")
        .body(Body::from(payload.to_string()))
        .expect("valid CRDT HTTP request")
}

fn counter_create_request() -> Value {
    json!({
        "object_id": "counter-http",
        "range": RangeIdentity::new("local", 7, "range-http", None, None, 1, 9),
        "request_id": "request-http",
        "operation_id": "operation-http",
        "update_version": 0,
        "initial_value": -4,
    })
}

#[tokio::test]
async fn counter_create_uses_the_authenticated_api_prefix_and_canonical_outcome() {
    let data_dir = tempdir().expect("data directory");
    let server = Server::new(ServerConfig {
        data_dir: data_dir.path().to_path_buf(),
        api_prefix: "/api".into(),
        auth_mode: AuthMode::Dev {
            api_key: "crdt-key".into(),
        },
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .expect("server");

    let response = http::router(server.state.clone())
        .oneshot(request(counter_create_request()))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let outcome: Value = serde_json::from_slice(&body).expect("canonical CRDT response");

    assert_eq!(outcome["object_type"], "counter");
    assert_eq!(outcome["object_id"], "counter-http");
    assert_eq!(outcome["actor"], "dev");
    assert_eq!(outcome["request_id"], "request-http");
    assert_eq!(outcome["operation_id"], "operation-http");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["initial_value"], -4);
    assert_eq!(outcome["value"]["accepted_delta_total"], 0);
    assert_eq!(outcome["value"]["value"], -4);
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);

    let replay = http::router(server.state)
        .oneshot(request(counter_create_request()))
        .await
        .expect("replay response");
    assert_eq!(replay.status(), StatusCode::OK);
    let body = axum::body::to_bytes(replay.into_body(), usize::MAX)
        .await
        .expect("replay body");
    let replay: Value = serde_json::from_slice(&body).expect("replay outcome");
    assert_eq!(replay["idempotency"]["duplicate_count"], 1);
    assert_eq!(replay["value"], outcome["value"]);
}
