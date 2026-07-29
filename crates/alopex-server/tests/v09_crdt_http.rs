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

fn counter_read_request() -> Request<Body> {
    let payload = json!({
        "range": RangeIdentity::new("local", 7, "range-http", None, None, 1, 9),
        "request_id": "request-http-read",
        "operation_id": "operation-http-read",
        "update_version": 0,
    });
    Request::builder()
        .method("POST")
        .uri("/api/crdt/counters/counter-http/read")
        .header("content-type", "application/json")
        .header("x-api-key", "crdt-key")
        .body(Body::from(payload.to_string()))
        .expect("valid CRDT HTTP read request")
}

fn counter_increment_request() -> Request<Body> {
    let payload = json!({
        "range": RangeIdentity::new("local", 7, "range-http", None, None, 1, 9),
        "request_id": "request-http-increment",
        "operation_id": "operation-http-increment",
        "update_version": 1,
        "delta": 3,
    });
    Request::builder()
        .method("POST")
        .uri("/api/crdt/counters/counter-http/increment")
        .header("content-type", "application/json")
        .header("x-api-key", "crdt-key")
        .body(Body::from(payload.to_string()))
        .expect("valid CRDT HTTP increment request")
}

fn counter_decrement_request() -> Request<Body> {
    let payload = json!({
        "range": RangeIdentity::new("local", 7, "range-http", None, None, 1, 9),
        "request_id": "request-http-decrement",
        "operation_id": "operation-http-decrement",
        "update_version": 1,
        "delta": 3,
    });
    Request::builder()
        .method("POST")
        .uri("/api/crdt/counters/counter-http/decrement")
        .header("content-type", "application/json")
        .header("x-api-key", "crdt-key")
        .body(Body::from(payload.to_string()))
        .expect("valid CRDT HTTP decrement request")
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

fn set_create_request() -> Request<Body> {
    let payload = json!({
        "object_id": "set-http",
        "range": RangeIdentity::new("local", 7, "range-http", None, None, 1, 9),
        "request_id": "request-set-http",
        "operation_id": "operation-set-http",
        "update_version": 0,
    });
    Request::builder()
        .method("POST")
        .uri("/api/crdt/sets")
        .header("content-type", "application/json")
        .header("x-api-key", "crdt-key")
        .body(Body::from(payload.to_string()))
        .expect("valid Set create request")
}

fn set_read_request() -> Request<Body> {
    let payload = json!({
        "range": RangeIdentity::new("local", 7, "range-http", None, None, 1, 9),
        "request_id": "request-set-http-read",
        "operation_id": "operation-set-http-read",
        "update_version": 0,
    });
    Request::builder()
        .method("POST")
        .uri("/api/crdt/sets/set-http/read")
        .header("content-type", "application/json")
        .header("x-api-key", "crdt-key")
        .body(Body::from(payload.to_string()))
        .expect("valid Set read request")
}

#[tokio::test]
async fn set_create_uses_the_authenticated_api_prefix_and_replays_once() {
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
        .oneshot(set_create_request())
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let outcome: Value = serde_json::from_slice(&body).expect("canonical Set response");
    assert_eq!(outcome["object_type"], "set");
    assert_eq!(outcome["object_id"], "set-http");
    assert_eq!(outcome["actor"], "dev");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["members"], json!([]));
    assert_eq!(outcome["value"]["member_versions"], json!({}));
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);

    let replay = http::router(server.state)
        .oneshot(set_create_request())
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

#[tokio::test]
async fn set_read_uses_the_authenticated_path_and_preserves_the_canonical_outcome() {
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

    let create = http::router(server.state.clone())
        .oneshot(set_create_request())
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::OK);

    let response = http::router(server.state)
        .oneshot(set_read_request())
        .await
        .expect("read response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let outcome: Value = serde_json::from_slice(&body).expect("canonical Set read response");

    assert_eq!(outcome["object_type"], "set");
    assert_eq!(outcome["object_id"], "set-http");
    assert_eq!(outcome["actor"], "dev");
    assert_eq!(outcome["request_id"], "request-set-http-read");
    assert_eq!(outcome["operation_id"], "operation-set-http-read");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["members"], json!([]));
    assert_eq!(outcome["value"]["member_versions"], json!({}));
    assert_eq!(outcome["idempotency"]["first_outcome"], "set_read");
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);
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

#[tokio::test]
async fn counter_read_uses_the_authenticated_path_and_preserves_the_canonical_outcome() {
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

    let create = http::router(server.state.clone())
        .oneshot(request(counter_create_request()))
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::OK);

    let response = http::router(server.state)
        .oneshot(counter_read_request())
        .await
        .expect("read response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected Counter read response: {}",
        String::from_utf8_lossy(&body)
    );
    let outcome: Value = serde_json::from_slice(&body).expect("canonical CRDT response");

    assert_eq!(outcome["object_type"], "counter");
    assert_eq!(outcome["object_id"], "counter-http");
    assert_eq!(outcome["actor"], "dev");
    assert_eq!(outcome["request_id"], "request-http-read");
    assert_eq!(outcome["operation_id"], "operation-http-read");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["initial_value"], -4);
    assert_eq!(outcome["value"]["value"], -4);
    assert_eq!(outcome["idempotency"]["first_outcome"], "counter_read");
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);
}

#[tokio::test]
async fn counter_increment_uses_the_authenticated_path_and_replays_once() {
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

    let create = http::router(server.state.clone())
        .oneshot(request(counter_create_request()))
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::OK);

    let response = http::router(server.state.clone())
        .oneshot(counter_increment_request())
        .await
        .expect("increment response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("increment body");
    let outcome: Value = serde_json::from_slice(&body).expect("canonical CRDT response");
    assert_eq!(outcome["object_type"], "counter");
    assert_eq!(outcome["object_id"], "counter-http");
    assert_eq!(outcome["actor"], "dev");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["initial_value"], -4);
    assert_eq!(outcome["value"]["accepted_delta_total"], 3);
    assert_eq!(outcome["value"]["value"], -1);
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);

    let replay = http::router(server.state)
        .oneshot(counter_increment_request())
        .await
        .expect("increment replay response");
    assert_eq!(replay.status(), StatusCode::OK);
    let body = axum::body::to_bytes(replay.into_body(), usize::MAX)
        .await
        .expect("increment replay body");
    let replay: Value = serde_json::from_slice(&body).expect("replay outcome");
    assert_eq!(replay["idempotency"]["duplicate_count"], 1);
    assert_eq!(replay["value"], outcome["value"]);
}

#[tokio::test]
async fn counter_decrement_uses_the_authenticated_path_and_replays_once() {
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

    let create = http::router(server.state.clone())
        .oneshot(request(counter_create_request()))
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::OK);

    let response = http::router(server.state.clone())
        .oneshot(counter_decrement_request())
        .await
        .expect("decrement response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("decrement body");
    let outcome: Value = serde_json::from_slice(&body).expect("canonical CRDT response");
    assert_eq!(outcome["object_type"], "counter");
    assert_eq!(outcome["object_id"], "counter-http");
    assert_eq!(outcome["actor"], "dev");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["initial_value"], -4);
    assert_eq!(outcome["value"]["accepted_delta_total"], -3);
    assert_eq!(outcome["value"]["value"], -7);
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);

    let replay = http::router(server.state)
        .oneshot(counter_decrement_request())
        .await
        .expect("decrement replay response");
    assert_eq!(replay.status(), StatusCode::OK);
    let body = axum::body::to_bytes(replay.into_body(), usize::MAX)
        .await
        .expect("decrement replay body");
    let replay: Value = serde_json::from_slice(&body).expect("replay outcome");
    assert_eq!(replay["idempotency"]["duplicate_count"], 1);
    assert_eq!(replay["value"], outcome["value"]);
}
