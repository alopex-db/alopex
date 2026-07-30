use axum::{
    body::Body,
    http::{header::CONTENT_TYPE, HeaderValue, Method, Request, StatusCode},
};
use serde_json::{json, Value};
use tempfile::tempdir;
use tower::ServiceExt;

use alopex_server::{
    auth::AuthMode,
    config::{
        ChangefeedAuthorizationConfig, ChangefeedScopeConfig, ChangefeedServerConfig, ServerConfig,
    },
    http,
    server::Server,
};

const CHANGEFEED_ROUTES: [(&str, Method); 8] = [
    ("/v1/changefeeds", Method::POST),
    ("/v1/changefeeds/feed-a/subscribe", Method::POST),
    ("/v1/changefeeds/feed-a/events", Method::GET),
    ("/v1/changefeeds/feed-a/stream", Method::GET),
    ("/v1/changefeeds/feed-a/ack", Method::POST),
    ("/v1/changefeeds/feed-a/resume", Method::POST),
    ("/v1/changefeeds/feed-a/cancel", Method::POST),
    ("/v1/changefeeds/feed-a/close", Method::POST),
];

fn server() -> Server {
    let temp = tempdir().expect("temporary data directory");
    Server::new(ServerConfig {
        data_dir: temp.keep(),
        auth_mode: AuthMode::Dev {
            api_key: "changefeed-key".to_owned(),
        },
        audit_log_enabled: false,
        changefeed: ChangefeedServerConfig {
            authorizations: vec![ChangefeedAuthorizationConfig {
                subject: "dev".to_owned(),
                tenant: "tenant-a".to_owned(),
                allowed_ranges: vec!["range-a".to_owned()],
                allowed_scopes: vec![ChangefeedScopeConfig::Read, ChangefeedScopeConfig::Ack],
            }],
        },
        ..ServerConfig::default()
    })
    .expect("server")
}

async fn send(
    api: axum::Router,
    method: Method,
    path: &str,
    api_key: Option<&str>,
    body: Body,
) -> (StatusCode, Vec<u8>) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .expect("request");
    if let Some(api_key) = api_key {
        request.headers_mut().insert(
            "x-api-key",
            HeaderValue::from_str(api_key).expect("api key"),
        );
    }
    let response = api.oneshot(request).await.expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    (status, body.to_vec())
}

#[tokio::test]
async fn changefeed_routes_are_exact_and_protected_by_existing_http_auth() {
    let server = server();
    let api = http::router(server.state);

    for (path, method) in CHANGEFEED_ROUTES {
        let wrong_method = if method == Method::GET {
            Method::POST
        } else {
            Method::GET
        };
        let (status, _) = send(
            api.clone(),
            wrong_method,
            path,
            Some("changefeed-key"),
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{path} method");

        let (status, body) = send(api.clone(), method, path, None, Body::empty()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path} auth");
        let value: Value = serde_json::from_slice(&body).expect("auth error JSON");
        assert_eq!(value["error"]["code"], "UNAUTHORIZED", "{path} code");
    }
}

#[tokio::test]
async fn create_reports_compiled_durable_prerequisite_in_the_canonical_envelope() {
    let server = server();
    let api = http::router(server.state);
    let request = json!({
        "request_id": "request-create",
        "tenant": "tenant-a",
        "actor": "dev",
        "range_id": "range-a"
    });
    let (status, body) = send(
        api,
        Method::POST,
        "/v1/changefeeds",
        Some("changefeed-key"),
        Body::from(request.to_string()),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let outcome: Value = serde_json::from_slice(&body).expect("canonical JSON outcome");
    assert_eq!(outcome["failure_class"], "prerequisite_missing");
    assert!(outcome["reason_code"]
        .as_str()
        .is_some_and(|code| code.starts_with("durable_")));
    assert_eq!(outcome["result"]["result_type"], "feed");
    assert!(outcome["correlation_id"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));
}
