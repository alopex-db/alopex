use alopex_cluster::{
    ClusterMode, FailureClass, NodeRole, NodeState, OperationState, RequestId, RoutingOutcomeKind,
};
use alopex_server::auth::AuthMode;
use alopex_server::config::{ClusterServerConfig, ServerConfig};
use alopex_server::error::ServerError;
use alopex_server::http;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::{json, Value};
use tempfile::tempdir;
use tower::ServiceExt;

fn local_server() -> (
    std::sync::Arc<alopex_server::server::ServerState>,
    tempfile::TempDir,
) {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::None,
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .expect("server");
    (server.state, temp)
}

fn local_server_at(
    data_dir: std::path::PathBuf,
) -> std::sync::Arc<alopex_server::server::ServerState> {
    Server::new(ServerConfig {
        data_dir,
        auth_mode: AuthMode::None,
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .expect("server")
    .state
}

fn cluster_aware_server() -> (
    std::sync::Arc<alopex_server::server::ServerState>,
    tempfile::TempDir,
) {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::None,
        audit_log_enabled: false,
        cluster: ClusterServerConfig {
            mode: ClusterMode::ClusterAware,
            node_id: Some("node-a".to_string()),
            cluster_id: Some("cluster-a".to_string()),
            advertised_endpoint: Some("127.0.0.1:7001".to_string()),
            role: NodeRole::Worker,
            lifecycle_state: NodeState::Active,
            membership_source_available: false,
            ..ClusterServerConfig::default()
        },
        ..ServerConfig::default()
    })
    .expect("server");
    (server.state, temp)
}

async fn send(
    router: axum::Router,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    let body = match body {
        Some(body) => {
            builder = builder.header("content-type", "application/json");
            Body::from(body.to_string())
        }
        None => Body::empty(),
    };
    let response = router
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).expect("JSON response")
    };
    (status, value)
}

async fn send_jsonl(router: axum::Router, path: &str, body: Value) -> (StatusCode, String) {
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (
        status,
        String::from_utf8(body.to_vec()).expect("JSON lines"),
    )
}

async fn send_raw_json(
    router: axum::Router,
    path: &str,
    content_type: &str,
    body: impl Into<Body>,
) -> (StatusCode, Value) {
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(path)
                .header("content-type", content_type)
                .body(body.into())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (
        status,
        serde_json::from_slice(&body).expect("JSON response"),
    )
}

async fn send_raw_json_with_api_key(
    router: axum::Router,
    path: &str,
    api_key: &str,
    content_type: &str,
    body: impl Into<Body>,
) -> (StatusCode, Value) {
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(path)
                .header("x-api-key", api_key)
                .header("content-type", content_type)
                .body(body.into())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (
        status,
        serde_json::from_slice(&body).expect("JSON response"),
    )
}

fn assert_local_outcome(
    outcome: &Value,
    transaction_id: &str,
    request_id: &str,
    state: &str,
    reason_code: &str,
) {
    assert_eq!(outcome["outcome_version"], "v0.9");
    assert_eq!(outcome["transaction_id"], transaction_id);
    assert_eq!(outcome["request_id"], request_id);
    assert_eq!(outcome["participating_ranges"], json!([]));
    assert_eq!(outcome["read_point"], Value::Null);
    assert_eq!(outcome["schema_version"], Value::Null);
    assert_eq!(outcome["data_epoch"], Value::Null);
    assert_eq!(outcome["isolation"], "snapshot");
    assert_eq!(outcome["state"], state);
    assert_eq!(outcome["failure_class"], Value::Null);
    assert_eq!(outcome["reason_code"], Value::Null);
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["routing"]["reason_code"], reason_code);
    assert_eq!(outcome["retryable"], false);
    assert_eq!(outcome["idempotency"]["operation_id"], transaction_id);
    assert_eq!(outcome["idempotency"]["request_id"], request_id);
    assert_eq!(outcome["idempotency"]["state"], state);
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);
}

#[tokio::test]
async fn legacy_session_and_sql_fields_remain_while_v09_outcomes_are_additive() {
    let (state, _temp) = local_server();
    let router = http::router(state);

    let (status, begin) = send(router.clone(), Method::POST, "/session/begin", None).await;
    assert_eq!(status, StatusCode::OK);
    let session_id = begin["session_id"].as_str().expect("legacy session id");
    assert!(begin["expires_at"].as_str().is_some(), "legacy expiry");
    assert_local_outcome(
        &begin["transaction"],
        session_id,
        &format!("{session_id}:begin"),
        "running",
        "session_started",
    );

    let (status, sql) = send(
        router.clone(),
        Method::POST,
        "/sql",
        Some(json!({
            "sql": "SELECT 1;",
            "session_id": session_id,
            "request_id": "session-sql-request"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(sql["rows"].is_array(), "legacy SQL result fields");
    assert!(sql["results"].is_array(), "legacy multi-result field");
    assert_local_outcome(
        &sql["transaction"],
        session_id,
        "session-sql-request",
        "running",
        "local_session_sql",
    );

    let (status, commit) = send(
        router.clone(),
        Method::POST,
        &format!("/session/{session_id}/commit"),
        Some(json!({ "request_id": "session-commit-request" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(commit["success"], true, "legacy success field");
    assert_local_outcome(
        &commit["transaction"],
        session_id,
        "session-commit-request",
        "committed",
        "local_session_committed",
    );

    let (status, alias) = send(
        router.clone(),
        Method::POST,
        "/api/sql/query",
        Some(json!({ "sql": "SELECT 2;", "request_id": "alias-request" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(alias["rows"].is_array(), "compatibility alias result");
    assert_local_outcome(
        &alias["transaction"],
        "local-sql:alias-request",
        "alias-request",
        "committed",
        "local_sql_autocommit",
    );

    let (status, stream) = send_jsonl(
        router.clone(),
        "/sql",
        json!({
            "sql": "SELECT 3;",
            "streaming": true,
            "request_id": "stream-request"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let events: Vec<Value> = stream
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("stream event"))
        .collect();
    assert_eq!(events[0]["transaction"]["request_id"], "stream-request");
    assert_eq!(events[0]["transaction"]["state"], "running");
    assert_eq!(
        events[0]["transaction"]["routing"]["reason_code"],
        "local_sql_streaming"
    );
    assert!(
        events
            .iter()
            .any(|event| event["done"] == true && event["transaction"].is_null()),
        "stream completion must not invent a committed transaction outcome"
    );

    for suffix in ["status", "recover", "cancel"] {
        let (status, _) = send(
            router.clone(),
            Method::POST,
            &format!("/session/{session_id}/{suffix}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{suffix} must remain absent");
    }
}

#[tokio::test]
async fn cluster_aware_legacy_session_is_explicitly_local_only() {
    let (state, _temp) = cluster_aware_server();
    let (status, body) = send(
        http::router(state.clone()),
        Method::POST,
        "/session/begin",
        Some(json!({ "request_id": "cluster-session-begin" })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let outcome = &body["transaction"];
    let session_id = body["session_id"].as_str().expect("session id");
    assert_local_outcome(
        outcome,
        session_id,
        "cluster-session-begin",
        "running",
        "session_started",
    );

    // Cluster-aware metadata alone must not relabel an existing v0.8 session
    // as distributed.  Routing can still fail closed later for an operation
    // that actually requires multiple ranges (covered by the legacy E2E).
    let (status, sql) = send(
        http::router(state),
        Method::POST,
        "/sql",
        Some(json!({
            "sql": "SELECT 1;",
            "session_id": session_id.to_string(),
            "request_id": "cluster-session-sql"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_local_outcome(
        &sql["transaction"],
        session_id,
        "cluster-session-sql",
        "running",
        "local_session_sql",
    );
}

#[tokio::test]
async fn session_request_id_replays_only_while_the_local_session_is_live() {
    let (state, _temp) = local_server();
    let router = http::router(state);

    let (status, first_begin) = send(
        router.clone(),
        Method::POST,
        "/session/begin",
        Some(json!({ "request_id": "dedupe-begin" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let session_id = first_begin["session_id"]
        .as_str()
        .expect("session id")
        .to_owned();

    let (status, replay_begin) = send(
        router.clone(),
        Method::POST,
        "/session/begin",
        Some(json!({ "request_id": "dedupe-begin" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay_begin["session_id"], session_id);
    assert_eq!(
        replay_begin["transaction"]["idempotency"]["duplicate_count"],
        1
    );

    let commit_body = json!({ "request_id": "dedupe-commit" });
    let (status, first_commit) = send(
        router.clone(),
        Method::POST,
        &format!("/session/{session_id}/commit"),
        Some(commit_body.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_commit["success"], true);

    let (status, replay_commit) = send(
        router.clone(),
        Method::POST,
        &format!("/session/{session_id}/commit"),
        Some(commit_body),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay_commit["success"], true);
    assert_eq!(
        replay_commit["transaction"]["idempotency"]["duplicate_count"],
        1
    );

    let (status, conflict) = send(
        router,
        Method::POST,
        &format!("/session/{session_id}/rollback"),
        Some(json!({ "request_id": "dedupe-commit" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflict["transaction"]["failure_class"], "conflict");
    assert_eq!(
        conflict["transaction"]["reason_code"],
        "idempotency_conflict"
    );
}

#[tokio::test]
async fn restarted_local_session_request_id_is_a_durable_expiration_tombstone() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let first_state = local_server_at(data_dir.clone());
    let first_router = http::router(first_state.clone());

    let (status, first) = send(
        first_router.clone(),
        Method::POST,
        "/session/begin",
        Some(json!({ "request_id": "restart-begin" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let original_session_id = first["session_id"].as_str().expect("session id").to_owned();
    drop(first_router);
    drop(first_state);

    let second_state = local_server_at(data_dir);
    let second_router = http::router(second_state);
    let (status, replay) = send(
        second_router.clone(),
        Method::POST,
        "/session/begin",
        Some(json!({ "request_id": "restart-begin" })),
    )
    .await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(replay["error"]["code"], "SESSION_EXPIRED");
    assert_eq!(replay["transaction"]["transaction_id"], original_session_id);
    assert_eq!(replay["transaction"]["reason_code"], "session_expired");
    assert_eq!(replay["transaction"]["retryable"], false);

    let (status, third_replay) = send(
        second_router,
        Method::POST,
        "/session/begin",
        Some(json!({ "request_id": "restart-begin" })),
    )
    .await;
    assert_eq!(status, StatusCode::GONE);
    for response in [&replay, &third_replay] {
        assert_eq!(
            response["transaction"]["transaction_id"],
            original_session_id
        );
        assert_eq!(
            response["transaction"]["idempotency"]["operation_id"],
            original_session_id
        );
        assert_eq!(response["transaction"]["reason_code"], "session_expired");
    }
}

#[tokio::test]
async fn transaction_route_middleware_classifies_json_and_body_limit_rejections() {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().join("data"),
        auth_mode: AuthMode::None,
        audit_log_enabled: false,
        max_request_size: 16,
        ..ServerConfig::default()
    })
    .expect("server");
    let router = http::router(server.state);

    let (status, malformed) =
        send_raw_json(router.clone(), "/sql", "application/json", Body::from("{")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(malformed["error"]["code"], "INVALID_REQUEST");
    assert_eq!(malformed["transaction"]["state"], "rejected");
    assert_eq!(malformed["transaction"]["failure_class"], "invalid_request");

    let (status, unsupported_media_type) = send_raw_json(
        router.clone(),
        "/sql",
        "text/plain",
        Body::from("{\"sql\":\"SELECT 1\"}"),
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        unsupported_media_type["error"]["code"],
        "UNSUPPORTED_MEDIA_TYPE"
    );
    assert_eq!(
        unsupported_media_type["transaction"]["failure_class"],
        "invalid_request"
    );

    let (status, unprocessable_entity) = send_raw_json(
        router.clone(),
        "/sql",
        "application/json",
        Body::from("{\"sql\":1}"),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        unprocessable_entity["error"]["code"],
        "UNPROCESSABLE_ENTITY"
    );
    assert_eq!(
        unprocessable_entity["transaction"]["failure_class"],
        "invalid_request"
    );

    let (status, oversized) = send_raw_json(
        router,
        "/sql",
        "application/json",
        Body::from("{\"sql\":\"SELECT 12345678901234567890\"}"),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(oversized["error"]["code"], "PAYLOAD_TOO_LARGE");
    assert_eq!(oversized["transaction"]["reason_code"], "resource_limit");
    assert_eq!(oversized["transaction"]["retryable"], false);
}

#[tokio::test]
async fn configured_api_prefix_preserves_transaction_middleware_boundaries() {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().join("data"),
        api_prefix: "/api".to_owned(),
        auth_mode: AuthMode::Dev {
            api_key: "prefix-key".to_owned(),
        },
        audit_log_enabled: false,
        max_request_size: 16,
        ..ServerConfig::default()
    })
    .expect("server");
    let router = http::router(server.state);

    let (status, sql_auth) = send(router.clone(), Method::POST, "/api/sql", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(sql_auth["error"]["message"], "missing credentials");
    assert_eq!(sql_auth["transaction"]["failure_class"], "unauthorized");
    assert_eq!(sql_auth["transaction"]["routing"]["kind"], "blocked");

    let (status, session_auth) =
        send(router.clone(), Method::POST, "/api/session/begin", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(session_auth["transaction"]["failure_class"], "unauthorized");
    assert_eq!(session_auth["transaction"]["routing"]["kind"], "blocked");

    let (status, sql_type) = send_raw_json_with_api_key(
        router.clone(),
        "/api/sql",
        "prefix-key",
        "text/plain",
        Body::from("SELECT 1"),
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(sql_type["error"]["code"], "UNSUPPORTED_MEDIA_TYPE");
    assert_eq!(sql_type["transaction"]["state"], "rejected");
    assert_eq!(sql_type["transaction"]["failure_class"], "invalid_request");

    let (status, session_size) = send_raw_json_with_api_key(
        router,
        "/api/session/begin",
        "prefix-key",
        "application/json",
        Body::from("{\"request_id\":\"this-body-is-too-large\"}"),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(session_size["error"]["code"], "PAYLOAD_TOO_LARGE");
    assert_eq!(session_size["transaction"]["state"], "rejected");
    assert_eq!(session_size["transaction"]["reason_code"], "resource_limit");
}

#[tokio::test]
async fn transaction_response_limit_keeps_executed_outcomes_on_every_session_retry_path() {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().join("data"),
        auth_mode: AuthMode::None,
        audit_log_enabled: false,
        max_response_size: 1,
        ..ServerConfig::default()
    })
    .expect("server");
    let router = http::router(server.state);

    let (status, sql) = send(
        router.clone(),
        Method::POST,
        "/sql",
        Some(json!({ "sql": "SELECT 1" })),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(sql["error"]["code"], "PAYLOAD_TOO_LARGE");
    assert_eq!(sql["transaction"]["state"], "committed");
    assert_eq!(sql["transaction"]["routing"]["kind"], "local_only");

    let (status, begin) = send(router.clone(), Method::POST, "/session/begin", None).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(begin["error"]["code"], "PAYLOAD_TOO_LARGE");
    assert_eq!(begin["transaction"]["state"], "running");
    assert_eq!(begin["transaction"]["routing"]["kind"], "local_only");

    // A caller-supplied begin identity must not bypass the legacy response
    // limit, and its replay must preserve the already-running transaction.
    let (status, idempotent_begin) = send(
        router.clone(),
        Method::POST,
        "/session/begin",
        Some(json!({ "request_id": "response-limit-begin" })),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(idempotent_begin["error"]["code"], "PAYLOAD_TOO_LARGE");
    assert_eq!(idempotent_begin["transaction"]["state"], "running");
    let commit_session_id = idempotent_begin["transaction"]["transaction_id"]
        .as_str()
        .expect("session id retained in outcome")
        .to_owned();

    let (status, replay_begin) = send(
        router.clone(),
        Method::POST,
        "/session/begin",
        Some(json!({ "request_id": "response-limit-begin" })),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(replay_begin["transaction"]["state"], "running");
    assert_eq!(
        replay_begin["transaction"]["idempotency"]["duplicate_count"],
        1
    );

    let commit_path = format!("/session/{commit_session_id}/commit");
    let (status, commit) = send(
        router.clone(),
        Method::POST,
        &commit_path,
        Some(json!({ "request_id": "response-limit-commit" })),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(commit["error"]["code"], "PAYLOAD_TOO_LARGE");
    assert_eq!(commit["transaction"]["state"], "committed");

    let (status, replay_commit) = send(
        router.clone(),
        Method::POST,
        &commit_path,
        Some(json!({ "request_id": "response-limit-commit" })),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(replay_commit["transaction"]["state"], "committed");
    assert_eq!(
        replay_commit["transaction"]["idempotency"]["duplicate_count"],
        1
    );

    // Omitted action request IDs are derived from the session ID, so this is
    // also an idempotent ledger/replay path rather than a one-shot v0.8 call.
    let (status, rollback_begin) = send(
        router.clone(),
        Method::POST,
        "/session/begin",
        Some(json!({ "request_id": "response-limit-rollback-begin" })),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let rollback_session_id = rollback_begin["transaction"]["transaction_id"]
        .as_str()
        .expect("session id retained in outcome")
        .to_owned();
    let rollback_path = format!("/session/{rollback_session_id}/rollback");
    let (status, rollback) = send(router.clone(), Method::POST, &rollback_path, None).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(rollback["error"]["code"], "PAYLOAD_TOO_LARGE");
    assert_eq!(rollback["transaction"]["state"], "cancelled");

    let (status, replay_rollback) = send(router, Method::POST, &rollback_path, None).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(replay_rollback["transaction"]["state"], "cancelled");
    assert_eq!(
        replay_rollback["transaction"]["idempotency"]["duplicate_count"],
        1
    );
}

#[test]
fn sql_storage_conflict_keeps_the_http_409_retryable_outcome_mapping() {
    let (state, _temp) = local_server();
    let error = ServerError::Sql(alopex_sql::StorageError::TransactionConflict.into());
    let outcome = http::transaction_failure_outcome(
        &state,
        "local-sql:conflict",
        RequestId::new("conflict-request"),
        &error,
    );

    assert_eq!(error.status_code(), StatusCode::CONFLICT);
    assert_eq!(outcome.state, OperationState::RetryableFailure);
    assert_eq!(outcome.failure_class, Some(FailureClass::Conflict));
    assert_eq!(outcome.routing.kind, RoutingOutcomeKind::Retryable);
    assert_eq!(outcome.reason_code.as_deref(), Some("conflict"));
    assert!(outcome.retryable);
}
