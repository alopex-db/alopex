use alopex_cluster::RangeIdentity;
use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use tempfile::tempdir;
use tower::ServiceExt;

const API_KEY: &str = "f2-http-key";

#[derive(Clone, Copy, Debug)]
enum Setup {
    None,
    Counter,
    Set,
    SetWithMember,
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    CounterCreate,
    CounterRead,
    CounterIncrement,
    CounterDecrement,
    SetCreate,
    SetRead,
    SetAdd,
    SetRemove,
    SetContains,
    SetList,
}

impl Operation {
    const ALL: [Self; 10] = [
        Self::CounterCreate,
        Self::CounterRead,
        Self::CounterIncrement,
        Self::CounterDecrement,
        Self::SetCreate,
        Self::SetRead,
        Self::SetAdd,
        Self::SetRemove,
        Self::SetContains,
        Self::SetList,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::CounterCreate => "Counter create",
            Self::CounterRead => "Counter read",
            Self::CounterIncrement => "Counter increment",
            Self::CounterDecrement => "Counter decrement",
            Self::SetCreate => "Set create",
            Self::SetRead => "Set read",
            Self::SetAdd => "Set add",
            Self::SetRemove => "Set remove",
            Self::SetContains => "Set contains",
            Self::SetList => "Set list",
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::CounterCreate => "/api/crdt/counters",
            Self::CounterRead => "/api/crdt/counters/f2-counter/read",
            Self::CounterIncrement => "/api/crdt/counters/f2-counter/increment",
            Self::CounterDecrement => "/api/crdt/counters/f2-counter/decrement",
            Self::SetCreate => "/api/crdt/sets",
            Self::SetRead => "/api/crdt/sets/f2-set/read",
            Self::SetAdd => "/api/crdt/sets/f2-set/add",
            Self::SetRemove => "/api/crdt/sets/f2-set/remove",
            Self::SetContains => "/api/crdt/sets/f2-set/contains",
            Self::SetList => "/api/crdt/sets/f2-set/members",
        }
    }

    fn object_type(self) -> &'static str {
        match self {
            Self::CounterCreate
            | Self::CounterRead
            | Self::CounterIncrement
            | Self::CounterDecrement => "counter",
            Self::SetCreate
            | Self::SetRead
            | Self::SetAdd
            | Self::SetRemove
            | Self::SetContains
            | Self::SetList => "set",
        }
    }

    fn setup(self) -> Setup {
        match self {
            Self::CounterCreate | Self::SetCreate => Setup::None,
            Self::CounterRead | Self::CounterIncrement | Self::CounterDecrement => Setup::Counter,
            Self::SetRead | Self::SetAdd => Setup::Set,
            Self::SetRemove | Self::SetContains | Self::SetList => Setup::SetWithMember,
        }
    }

    fn replay_duplicate_count(self) -> u64 {
        match self {
            Self::CounterCreate
            | Self::CounterIncrement
            | Self::CounterDecrement
            | Self::SetCreate
            | Self::SetAdd
            | Self::SetRemove => 1,
            Self::CounterRead | Self::SetRead | Self::SetContains | Self::SetList => 0,
        }
    }

    fn payload(self) -> Value {
        let range = RangeIdentity::new("local", 7, "range-f2-http", None, None, 1, 9);
        match self {
            Self::CounterCreate => json!({
                "object_id": "f2-counter",
                "range": range,
                "request_id": "f2-counter-create-request",
                "operation_id": "f2-counter-create-operation",
                "update_version": 0,
                "initial_value": -4,
            }),
            Self::CounterRead => json!({
                "range": range,
                "request_id": "f2-counter-read-request",
                "operation_id": "f2-counter-read-operation",
                "update_version": 0,
            }),
            Self::CounterIncrement => json!({
                "range": range,
                "request_id": "f2-counter-increment-request",
                "operation_id": "f2-counter-increment-operation",
                "update_version": 1,
                "delta": 3,
            }),
            Self::CounterDecrement => json!({
                "range": range,
                "request_id": "f2-counter-decrement-request",
                "operation_id": "f2-counter-decrement-operation",
                "update_version": 1,
                "delta": 3,
            }),
            Self::SetCreate => json!({
                "object_id": "f2-set",
                "range": range,
                "request_id": "f2-set-create-request",
                "operation_id": "f2-set-create-operation",
                "update_version": 0,
            }),
            Self::SetRead => json!({
                "range": range,
                "request_id": "f2-set-read-request",
                "operation_id": "f2-set-read-operation",
                "update_version": 0,
            }),
            Self::SetAdd => json!({
                "range": range,
                "request_id": "f2-set-add-request",
                "operation_id": "00000000-0000-0000-0000-000000000902",
                "update_version": 1,
                "member": "alice",
            }),
            Self::SetRemove => json!({
                "range": range,
                "request_id": "f2-set-remove-request",
                "operation_id": "00000000-0000-0000-0000-000000000903",
                "update_version": 2,
                "member": "alice",
            }),
            Self::SetContains => json!({
                "range": range,
                "request_id": "f2-set-contains-request",
                "operation_id": "f2-set-contains-operation",
                "update_version": 0,
                "member": "alice",
            }),
            Self::SetList => json!({
                "range": range,
                "request_id": "f2-set-list-request",
                "operation_id": "f2-set-list-operation",
                "update_version": 0,
            }),
        }
    }
}

async fn send(
    app: Router,
    method: Method,
    path: &str,
    api_key: Option<&str>,
    payload: Value,
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(api_key) = api_key {
        builder = builder.header("x-api-key", api_key);
    }
    let request = builder
        .body(Body::from(payload.to_string()))
        .expect("valid HTTP request");
    let response = app.oneshot(request).await.expect("HTTP response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("HTTP body");
    (status, body.to_vec())
}

async fn assert_ok(app: &Router, operation: Operation) {
    let (status, _) = send(
        app.clone(),
        Method::POST,
        operation.path(),
        Some(API_KEY),
        operation.payload(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{} setup", operation.label());
}

async fn apply_setup(app: &Router, setup: Setup) {
    match setup {
        Setup::None => {}
        Setup::Counter => assert_ok(app, Operation::CounterCreate).await,
        Setup::Set => assert_ok(app, Operation::SetCreate).await,
        Setup::SetWithMember => {
            assert_ok(app, Operation::SetCreate).await;
            assert_ok(app, Operation::SetAdd).await;
        }
    }
}

#[tokio::test]
async fn f2_http_register_covers_all_crdt_paths_status_auth_and_idempotency() {
    for operation in Operation::ALL {
        let data_dir = tempdir().expect("data directory");
        let server = Server::new(ServerConfig {
            data_dir: data_dir.path().to_path_buf(),
            api_prefix: "/api".into(),
            auth_mode: AuthMode::Dev {
                api_key: API_KEY.into(),
            },
            audit_log_enabled: false,
            ..ServerConfig::default()
        })
        .expect("server");
        let app = http::router(server.state);
        apply_setup(&app, operation.setup()).await;

        let (status, _) = send(
            app.clone(),
            Method::GET,
            operation.path(),
            Some(API_KEY),
            json!({}),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::METHOD_NOT_ALLOWED,
            "{} must reject the wrong method",
            operation.label()
        );

        let (status, body) = send(
            app.clone(),
            Method::POST,
            operation.path(),
            None,
            operation.payload(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{} must require authentication",
            operation.label()
        );
        let error: Value = serde_json::from_slice(&body).expect("authentication error JSON");
        assert_eq!(
            error["error"]["code"],
            "UNAUTHORIZED",
            "{} error",
            operation.label()
        );

        let (status, body) = send(
            app.clone(),
            Method::POST,
            operation.path(),
            Some(API_KEY),
            operation.payload(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{} status", operation.label());
        let outcome: Value = serde_json::from_slice(&body).expect("canonical CRDT outcome");
        assert_eq!(
            outcome["object_type"],
            operation.object_type(),
            "{} type",
            operation.label()
        );
        assert_eq!(outcome["state"], "committed", "{} state", operation.label());
        assert_eq!(
            outcome["routing"]["kind"],
            "local_only",
            "{} routing",
            operation.label()
        );
        assert_eq!(
            outcome["idempotency"]["duplicate_count"],
            0,
            "{} first outcome",
            operation.label()
        );

        let (status, body) = send(
            app,
            Method::POST,
            operation.path(),
            Some(API_KEY),
            operation.payload(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{} replay status",
            operation.label()
        );
        let replay: Value = serde_json::from_slice(&body).expect("replay outcome");
        assert_eq!(
            replay["idempotency"]["duplicate_count"],
            operation.replay_duplicate_count(),
            "{} replay duplicate count",
            operation.label()
        );
        assert_eq!(
            replay["value"],
            outcome["value"],
            "{} replay value",
            operation.label()
        );
    }
}
