use alopex_cli::batch::{BatchMode, BatchModeSource, ExitCode};
use alopex_cli::cli::{KvCommand, KvTxnCommand, OutputFormat, SqlCommand, SqlReadMode};
use alopex_cli::client::http::HttpClient;
use alopex_cli::commands::{kv, sql};
use alopex_cli::error::CliError;
use alopex_cli::output::formatter::create_formatter;
use alopex_cli::profile::config::{ResolvedSqlReadMode, ServerConfig};
use alopex_cli::ui::mode::UiMode;
use axum::extract::Json;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{response::IntoResponse, Router};
use serde_json::{json, Value};
use tokio::sync::oneshot;

fn canonical_transaction(
    transaction_id: &str,
    request_id: &str,
    state: &str,
    routing_kind: &str,
    failure_class: Value,
    retryable: bool,
    reason_code: &str,
) -> Value {
    json!({
        "outcome_version": "v0.9",
        "transaction_id": transaction_id,
        "request_id": request_id,
        "participating_ranges": [],
        "read_point": null,
        "schema_version": null,
        "data_epoch": null,
        "isolation": "snapshot",
        "state": state,
        "failure_class": failure_class,
        "reason_code": reason_code,
        "routing": {
            "kind": routing_kind,
            "range": null,
            "metadata_version": 0,
            "reason_code": reason_code,
        },
        "retryable": retryable,
        "idempotency": {
            "operation_id": transaction_id,
            "request_id": request_id,
            "state": state,
            "duplicate_count": 0,
        },
    })
}

async fn kv_commit(Json(request): Json<Value>) -> impl IntoResponse {
    assert_eq!(request["txn_id"], "txn-kv-1");
    assert_eq!(request["request_id"], "kv-commit-1");
    Json(json!({
        "success": true,
        "transaction": canonical_transaction(
            "txn-kv-1",
            "kv-commit-1",
            "committed",
            "single_range",
            Value::Null,
            false,
            "local_kv_transaction_committed",
        ),
    }))
}

async fn kv_rollback_unsupported(Json(request): Json<Value>) -> impl IntoResponse {
    assert_eq!(request["txn_id"], "txn-kv-2");
    assert_eq!(request["request_id"], "kv-rollback-1");
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": {"code": "FUTURE_DISTRIBUTED_EXECUTION_REQUIRED"},
            "transaction": canonical_transaction(
                "txn-kv-2",
                "kv-rollback-1",
                "rejected",
                "blocked",
                json!("prerequisite_missing"),
                false,
                "distributed_rollback_unsupported",
            ),
        })),
    )
}

async fn sql_commit(Json(request): Json<Value>) -> impl IntoResponse {
    assert_eq!(request["sql"], "COMMIT");
    assert_eq!(request["streaming"], false);
    assert_eq!(request["request_id"], "sql-commit-1");
    Json(json!({
        "affected_rows": 0,
        "transaction": canonical_transaction(
            "local-sql:commit-1",
            "sql-commit-1",
            "committed",
            "local_only",
            Value::Null,
            false,
            "local_sql_autocommit",
        ),
    }))
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

fn client(base_url: String) -> HttpClient {
    HttpClient::new(&ServerConfig {
        url: base_url,
        insecure: true,
        auth: None,
        token: None,
        username: None,
        password_command: None,
        cert_path: None,
        key_path: None,
    })
    .expect("HTTP client")
}

fn batch_mode() -> BatchMode {
    BatchMode {
        is_batch: true,
        is_tty: false,
        source: BatchModeSource::Explicit,
    }
}

fn sql_command(query: &str, request_id: &str) -> SqlCommand {
    SqlCommand {
        query: Some(query.to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        request_id: Some(request_id.to_string()),
        read_mode: None,
        routing_report: None,
        tui: false,
    }
}

fn rendered_transaction(output: &[u8]) -> Value {
    let rows: Value = serde_json::from_slice(output).expect("JSON output");
    let rendered = rows.as_array().expect("row array")[0]["Transaction"]
        .as_str()
        .expect("canonical transaction string");
    serde_json::from_str(rendered).expect("canonical transaction document")
}

#[tokio::test]
async fn cli_kv_transaction_preserves_request_identity_outcome_and_exit_class() {
    let (base_url, shutdown) = spawn_server(
        Router::new()
            .route("/kv/txn/commit", post(kv_commit))
            .route("/kv/txn/rollback", post(kv_rollback_unsupported)),
    )
    .await;
    let client = client(base_url);

    let mut committed_output = Vec::new();
    kv::execute_remote_with_formatter(
        &client,
        &KvCommand::Txn(KvTxnCommand::Commit {
            txn_id: "txn-kv-1".to_string(),
            request_id: Some("kv-commit-1".to_string()),
        }),
        &mut committed_output,
        create_formatter(OutputFormat::Json),
        None,
        false,
    )
    .await
    .expect("committed transaction");
    let committed = rendered_transaction(&committed_output);
    assert_eq!(committed["outcome_version"], "v0.9");
    assert_eq!(committed["request_id"], "kv-commit-1");
    assert_eq!(committed["routing"]["kind"], "single_range");

    let mut rejected_output = Vec::new();
    let error = kv::execute_remote_with_formatter(
        &client,
        &KvCommand::Txn(KvTxnCommand::Rollback {
            txn_id: "txn-kv-2".to_string(),
            request_id: Some("kv-rollback-1".to_string()),
        }),
        &mut rejected_output,
        create_formatter(OutputFormat::Json),
        None,
        false,
    )
    .await
    .expect_err("unsupported transaction");
    assert!(matches!(
        error,
        CliError::TransactionOutcome {
            exit_code: ExitCode::Unsupported,
            ..
        }
    ));
    let rejected = rendered_transaction(&rejected_output);
    assert_eq!(rejected["failure_class"], "prerequisite_missing");
    assert_eq!(rejected["routing"]["kind"], "blocked");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn cli_sql_transaction_preserves_request_identity_and_outcome() {
    let (base_url, shutdown) =
        spawn_server(Router::new().route("/api/sql/query", post(sql_commit))).await;
    let client = client(base_url);
    let mut output = Vec::new();
    sql::execute_remote_with_formatter(
        &client,
        &sql_command("COMMIT", "sql-commit-1"),
        &batch_mode(),
        UiMode::Batch,
        &mut output,
        OutputFormat::Json,
        None,
        None,
        false,
    )
    .await
    .expect("committed SQL transaction");

    let outer: Value = serde_json::from_slice(&output).expect("SQL JSON output");
    let rendered = outer.as_array().expect("statement groups")[0]
        .as_array()
        .expect("result set")[0]
        .as_array()
        .expect("rows")[0]["Transaction"]
        .as_str()
        .expect("canonical transaction string");
    let transaction: Value =
        serde_json::from_str(rendered).expect("canonical transaction document");
    assert_eq!(transaction["outcome_version"], "v0.9");
    assert_eq!(transaction["request_id"], "sql-commit-1");
    assert_eq!(transaction["routing"]["kind"], "local_only");

    for statement in ["BEGIN", "COMMIT", "ROLLBACK", "ABORT"] {
        let mut cluster_output = Vec::new();
        let error = sql::execute_remote_with_routing(
            &client,
            &sql_command(statement, "sql-cluster-control-1"),
            ResolvedSqlReadMode::Cluster(SqlReadMode::Strong),
            &batch_mode(),
            UiMode::Batch,
            &mut cluster_output,
            OutputFormat::Json,
            None,
            None,
            false,
        )
        .await
        .expect_err("cluster transaction control must not fall back to local SQL");
        assert!(matches!(
            error,
            CliError::DistributedReadOutcome {
                exit_code: ExitCode::Unsupported,
                ..
            }
        ));
        assert!(
            cluster_output.is_empty(),
            "{statement} must not emit local output"
        );
    }

    let _ = shutdown.send(());
}
