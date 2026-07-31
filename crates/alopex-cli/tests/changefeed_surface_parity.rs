use alopex_cli::batch::ChangefeedCliOutcome;
use alopex_cli::cli::{ChangefeedCommand, OutputFormat};
use alopex_cli::client::http::HttpClient;
use alopex_cli::commands::changefeed::{authenticated_actor, invoke_remote};
use alopex_cli::profile::config::ServerConfig;
use axum::extract::Json;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{response::IntoResponse, Router};
use serde_json::{json, Value};
use tokio::sync::oneshot;

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/changefeed_surface_parity.json"
    ))
    .expect("valid parity fixture")
}

async fn create(Json(request): Json<Value>) -> impl IntoResponse {
    let fixture = fixture();
    assert_eq!(request["request_id"], fixture["request_id"]);
    assert_eq!(request["range_id"], fixture["range_id"]);
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "feed": {"feed_id": fixture["feed_id"], "range": {"range_id": fixture["range_id"]}},
            "request_id": fixture["request_id"],
            "operation_state": fixture["operation_state"],
            "failure_class": fixture["failure_class"],
            "reason_code": "durable_capability_missing",
            "retryable": fixture["retryable"],
            "idempotency": {"request_id": fixture["request_id"]},
            "result": {"result_type": fixture["result_type"]}
        })),
    )
}

async fn spawn_server(router: Router) -> (String, oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let address = listener.local_addr().expect("address");
    let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    (format!("http://{address}"), shutdown_tx)
}

#[tokio::test]
async fn cli_preserves_the_canonical_durable_preflight_fixture() {
    let fixture = fixture();
    let (url, shutdown) = spawn_server(Router::new().route("/v1/changefeeds", post(create))).await;
    let config = ServerConfig {
        url,
        insecure: true,
        auth: None,
        token: None,
        username: None,
        password_command: None,
        cert_path: None,
        key_path: None,
    };
    let client = HttpClient::new(&config).expect("client");
    let response = invoke_remote(
        &client,
        &ChangefeedCommand::Create {
            table: None,
            range: Some(fixture["range_id"].as_str().unwrap().to_owned()),
            tenant: "tenant-a".to_owned(),
            request_id: fixture["request_id"].as_str().unwrap().to_owned(),
            deadline: None,
            format: Some(OutputFormat::Json),
        },
        authenticated_actor(&config),
    )
    .await
    .expect("canonical HTTP error remains a CLI response");

    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    let document = &response.documents[0];
    for field in [
        "request_id",
        "operation_state",
        "failure_class",
        "retryable",
    ] {
        assert_eq!(document[field], fixture[field], "CLI retained {field}");
    }
    assert_eq!(document["result"]["result_type"], fixture["result_type"]);
    assert_eq!(document["idempotency"]["request_id"], fixture["request_id"]);
    assert!(document["reason_code"]
        .as_str()
        .is_some_and(|code| code.starts_with(fixture["reason_prefix"].as_str().unwrap())));
    assert_eq!(
        ChangefeedCliOutcome::from_changefeed_response(
            &response.documents,
            response.status.as_u16()
        )
        .exit_code()
        .as_i32(),
        fixture["cli_exit_code"].as_i64().unwrap() as i32
    );

    let _ = shutdown.send(());
}
