use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::http;
use alopex_server::Server;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use serde_json::Value;
use tempfile::tempdir;
use tower::ServiceExt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RouteMethod {
    Get,
    Post,
}

impl RouteMethod {
    fn request_method(self) -> Method {
        match self {
            Self::Get => Method::GET,
            Self::Post => Method::POST,
        }
    }

    fn alternate_method(self) -> Method {
        match self {
            Self::Get => Method::POST,
            Self::Post => Method::GET,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StreamBehavior {
    None,
    OptionalJsonLines,
    JsonLines,
}

#[derive(Clone, Copy, Debug)]
struct ApiRoute {
    label: &'static str,
    path: &'static str,
    method: RouteMethod,
    request_schema: &'static str,
    response_schema: &'static str,
    streaming: StreamBehavior,
}

const API_ROUTES: [ApiRoute; 54] = [
    ApiRoute {
        label: "POST /kv/get",
        path: "/kv/get",
        method: RouteMethod::Post,
        request_schema: "KvGetRequest",
        response_schema: "KvGetResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /kv/put",
        path: "/kv/put",
        method: RouteMethod::Post,
        request_schema: "KvPutRequest",
        response_schema: "KvPutResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /kv/delete",
        path: "/kv/delete",
        method: RouteMethod::Post,
        request_schema: "KvDeleteRequest",
        response_schema: "KvDeleteResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /kv/list",
        path: "/kv/list",
        method: RouteMethod::Post,
        request_schema: "KvListRequest",
        response_schema: "KvListResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /kv/txn/begin",
        path: "/kv/txn/begin",
        method: RouteMethod::Post,
        request_schema: "KvTxnBeginRequest",
        response_schema: "KvTxnBeginResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /kv/txn/get",
        path: "/kv/txn/get",
        method: RouteMethod::Post,
        request_schema: "KvTxnGetRequest",
        response_schema: "KvGetResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /kv/txn/put",
        path: "/kv/txn/put",
        method: RouteMethod::Post,
        request_schema: "KvTxnPutRequest",
        response_schema: "KvPutResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /kv/txn/delete",
        path: "/kv/txn/delete",
        method: RouteMethod::Post,
        request_schema: "KvTxnDeleteRequest",
        response_schema: "KvDeleteResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /kv/txn/commit",
        path: "/kv/txn/commit",
        method: RouteMethod::Post,
        request_schema: "KvTxnActionRequest",
        response_schema: "KvTxnActionResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /kv/txn/rollback",
        path: "/kv/txn/rollback",
        method: RouteMethod::Post,
        request_schema: "KvTxnActionRequest",
        response_schema: "KvTxnActionResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /columnar/scan",
        path: "/columnar/scan",
        method: RouteMethod::Post,
        request_schema: "ColumnarScanRequest",
        response_schema: "ColumnarScanResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /columnar/stats",
        path: "/columnar/stats",
        method: RouteMethod::Post,
        request_schema: "ColumnarStatsRequest",
        response_schema: "ColumnarStatsResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /columnar/list",
        path: "/columnar/list",
        method: RouteMethod::Post,
        request_schema: "ColumnarListRequest",
        response_schema: "ColumnarListResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /columnar/ingest",
        path: "/columnar/ingest",
        method: RouteMethod::Post,
        request_schema: "ColumnarIngestRequest",
        response_schema: "ColumnarStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /columnar/index/create",
        path: "/columnar/index/create",
        method: RouteMethod::Post,
        request_schema: "ColumnarIndexCreateRequest",
        response_schema: "ColumnarStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /columnar/index/list",
        path: "/columnar/index/list",
        method: RouteMethod::Post,
        request_schema: "ColumnarIndexListRequest",
        response_schema: "ColumnarIndexListResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /columnar/index/drop",
        path: "/columnar/index/drop",
        method: RouteMethod::Post,
        request_schema: "ColumnarIndexDropRequest",
        response_schema: "ColumnarStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /hnsw/search",
        path: "/hnsw/search",
        method: RouteMethod::Post,
        request_schema: "HnswSearchRequest",
        response_schema: "HnswSearchResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /hnsw/upsert",
        path: "/hnsw/upsert",
        method: RouteMethod::Post,
        request_schema: "HnswUpsertRequest",
        response_schema: "HnswStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /hnsw/delete",
        path: "/hnsw/delete",
        method: RouteMethod::Post,
        request_schema: "HnswDeleteRequest",
        response_schema: "HnswStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /hnsw/create",
        path: "/hnsw/create",
        method: RouteMethod::Post,
        request_schema: "HnswCreateRequest",
        response_schema: "HnswStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /hnsw/drop",
        path: "/hnsw/drop",
        method: RouteMethod::Post,
        request_schema: "HnswDropRequest",
        response_schema: "HnswStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /hnsw/stats",
        path: "/hnsw/stats",
        method: RouteMethod::Post,
        request_schema: "HnswStatsRequest",
        response_schema: "HnswStatsResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /sql",
        path: "/sql",
        method: RouteMethod::Post,
        request_schema: "SqlRequest",
        response_schema: "SqlResponse",
        streaming: StreamBehavior::OptionalJsonLines,
    },
    ApiRoute {
        label: "POST /api/sql/query",
        path: "/api/sql/query",
        method: RouteMethod::Post,
        request_schema: "SqlRequest",
        response_schema: "SqlResponse",
        streaming: StreamBehavior::OptionalJsonLines,
    },
    ApiRoute {
        label: "POST /vector/search",
        path: "/vector/search",
        method: RouteMethod::Post,
        request_schema: "VectorSearchRequest",
        response_schema: "VectorSearchResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /vector/upsert",
        path: "/vector/upsert",
        method: RouteMethod::Post,
        request_schema: "VectorUpsertRequest",
        response_schema: "VectorStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /vector/delete",
        path: "/vector/delete",
        method: RouteMethod::Post,
        request_schema: "VectorDeleteRequest",
        response_schema: "VectorStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /vector/index/create",
        path: "/vector/index/create",
        method: RouteMethod::Post,
        request_schema: "VectorIndexCreateRequest",
        response_schema: "VectorStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /vector/index/update",
        path: "/vector/index/update",
        method: RouteMethod::Post,
        request_schema: "VectorIndexUpdateRequest",
        response_schema: "VectorStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /vector/index/delete",
        path: "/vector/index/delete",
        method: RouteMethod::Post,
        request_schema: "VectorIndexDeleteRequest",
        response_schema: "VectorStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /vector/index/compact",
        path: "/vector/index/compact",
        method: RouteMethod::Post,
        request_schema: "VectorIndexCompactRequest",
        response_schema: "VectorStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /api/admin/capabilities",
        path: "/api/admin/capabilities",
        method: RouteMethod::Get,
        request_schema: "NoBody",
        response_schema: "AdminCapabilitiesResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /api/admin/resources",
        path: "/api/admin/resources",
        method: RouteMethod::Get,
        request_schema: "AdminResourcesQuery",
        response_schema: "AdminResourcesResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /api/admin/status",
        path: "/api/admin/status",
        method: RouteMethod::Get,
        request_schema: "NoBody",
        response_schema: "AdminStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /api/admin/metrics",
        path: "/api/admin/metrics",
        method: RouteMethod::Get,
        request_schema: "NoBody",
        response_schema: "AdminMetricsResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /api/admin/health",
        path: "/api/admin/health",
        method: RouteMethod::Get,
        request_schema: "NoBody",
        response_schema: "AdminHealthResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /api/admin/cluster/join",
        path: "/api/admin/cluster/join",
        method: RouteMethod::Post,
        request_schema: "AdminClusterManagementRequest",
        response_schema: "AdminClusterStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /api/admin/cluster/leave",
        path: "/api/admin/cluster/leave",
        method: RouteMethod::Post,
        request_schema: "AdminClusterManagementRequest",
        response_schema: "AdminClusterStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /api/admin/cluster/metadata",
        path: "/api/admin/cluster/metadata",
        method: RouteMethod::Get,
        request_schema: "NoBody",
        response_schema: "AdminClusterMetadataResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /api/admin/cluster/operations",
        path: "/api/admin/cluster/operations",
        method: RouteMethod::Post,
        request_schema: "AdminClusterManagementRequest",
        response_schema: "AdminClusterOperationResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /api/admin/backup",
        path: "/api/admin/backup",
        method: RouteMethod::Post,
        request_schema: "AdminBackupRequest",
        response_schema: "AdminBackupResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /api/admin/export",
        path: "/api/admin/export",
        method: RouteMethod::Post,
        request_schema: "AdminExportRequest",
        response_schema: "AdminExportResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /api/admin/backup/{id}",
        path: "/api/admin/backup/test",
        method: RouteMethod::Get,
        request_schema: "BackupHandlePath",
        response_schema: "AdminBackupStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /api/admin/restore",
        path: "/api/admin/restore",
        method: RouteMethod::Post,
        request_schema: "AdminRestoreRequest",
        response_schema: "AdminRestoreResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /api/admin/restore/{id}",
        path: "/api/admin/restore/test",
        method: RouteMethod::Get,
        request_schema: "RestoreHandlePath",
        response_schema: "AdminRestoreStatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /api/admin/lifecycle",
        path: "/api/admin/lifecycle",
        method: RouteMethod::Post,
        request_schema: "AdminLifecycleRequest",
        response_schema: "AdminLifecycleResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /api/admin/compaction",
        path: "/api/admin/compaction",
        method: RouteMethod::Post,
        request_schema: "AdminCompactionRequest",
        response_schema: "AdminCompactionResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /session/begin",
        path: "/session/begin",
        method: RouteMethod::Post,
        request_schema: "NoBody",
        response_schema: "SessionBeginResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /session/{id}/commit",
        path: "/session/test/commit",
        method: RouteMethod::Post,
        request_schema: "SessionIdPath",
        response_schema: "SessionActionResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /session/{id}/rollback",
        path: "/session/test/rollback",
        method: RouteMethod::Post,
        request_schema: "SessionIdPath",
        response_schema: "SessionActionResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "POST /v1/sql/reads",
        path: "/v1/sql/reads",
        method: RouteMethod::Post,
        request_schema: "DistributedReadRequest",
        response_schema: "DistributedReadStartResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /v1/sql/reads/{id}",
        path: "/v1/sql/reads/test",
        method: RouteMethod::Get,
        request_schema: "ReadExecutionIdPath",
        response_schema: "DistributedReadEvent",
        streaming: StreamBehavior::JsonLines,
    },
    ApiRoute {
        label: "POST /v1/sql/reads/{id}/cancel",
        path: "/v1/sql/reads/test/cancel",
        method: RouteMethod::Post,
        request_schema: "ReadExecutionIdPath",
        response_schema: "DistributedReadCancellationResponse",
        streaming: StreamBehavior::None,
    },
];

const ADMIN_ROUTES: [ApiRoute; 3] = [
    ApiRoute {
        label: "GET /healthz",
        path: "/healthz",
        method: RouteMethod::Get,
        request_schema: "NoBody",
        response_schema: "EmptyOk",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /status",
        path: "/status",
        method: RouteMethod::Get,
        request_schema: "NoBody",
        response_schema: "StatusResponse",
        streaming: StreamBehavior::None,
    },
    ApiRoute {
        label: "GET /metrics",
        path: "/metrics",
        method: RouteMethod::Get,
        request_schema: "NoBody",
        response_schema: "PrometheusText",
        streaming: StreamBehavior::None,
    },
];

const I13_REGISTER: [&str; 57] = [
    "POST /kv/get",
    "POST /kv/put",
    "POST /kv/delete",
    "POST /kv/list",
    "POST /kv/txn/begin",
    "POST /kv/txn/get",
    "POST /kv/txn/put",
    "POST /kv/txn/delete",
    "POST /kv/txn/commit",
    "POST /kv/txn/rollback",
    "POST /columnar/scan",
    "POST /columnar/stats",
    "POST /columnar/list",
    "POST /columnar/ingest",
    "POST /columnar/index/create",
    "POST /columnar/index/list",
    "POST /columnar/index/drop",
    "POST /hnsw/search",
    "POST /hnsw/upsert",
    "POST /hnsw/delete",
    "POST /hnsw/create",
    "POST /hnsw/drop",
    "POST /hnsw/stats",
    "POST /sql",
    "POST /api/sql/query",
    "POST /vector/search",
    "POST /vector/upsert",
    "POST /vector/delete",
    "POST /vector/index/create",
    "POST /vector/index/update",
    "POST /vector/index/delete",
    "POST /vector/index/compact",
    "GET /api/admin/capabilities",
    "GET /api/admin/resources",
    "GET /api/admin/status",
    "GET /api/admin/metrics",
    "GET /api/admin/health",
    "POST /api/admin/cluster/join",
    "POST /api/admin/cluster/leave",
    "GET /api/admin/cluster/metadata",
    "POST /api/admin/cluster/operations",
    "POST /api/admin/backup",
    "POST /api/admin/export",
    "GET /api/admin/backup/{id}",
    "POST /api/admin/restore",
    "GET /api/admin/restore/{id}",
    "POST /api/admin/lifecycle",
    "POST /api/admin/compaction",
    "POST /session/begin",
    "POST /session/{id}/commit",
    "POST /session/{id}/rollback",
    "POST /v1/sql/reads",
    "GET /v1/sql/reads/{id}",
    "POST /v1/sql/reads/{id}/cancel",
    "GET /healthz",
    "GET /status",
    "GET /metrics",
];

async fn send(
    router: axum::Router,
    method: Method,
    path: &str,
    api_key: Option<&str>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::empty())
        .expect("request");
    if let Some(api_key) = api_key {
        request.headers_mut().insert(
            "x-api-key",
            HeaderValue::from_str(api_key).expect("api key"),
        );
    }
    let response = router.oneshot(request).await.expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, headers, body.to_vec())
}

fn assert_metadata(row: &ApiRoute) {
    assert!(
        !row.request_schema.is_empty(),
        "{} request schema",
        row.label
    );
    assert!(
        !row.response_schema.is_empty(),
        "{} response schema",
        row.label
    );
    match row.streaming {
        StreamBehavior::None | StreamBehavior::OptionalJsonLines | StreamBehavior::JsonLines => {}
    }
}

#[tokio::test]
async fn i13_http_method_path_register_has_auth_and_status_boundaries() {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::Dev {
            api_key: "v09-key".to_owned(),
        },
        metrics_enabled: true,
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .expect("server");
    let api = http::router(server.state.clone());
    let admin = http::admin_router(server.state.clone());

    let labels: Vec<_> = API_ROUTES
        .iter()
        .chain(ADMIN_ROUTES.iter())
        .map(|row| row.label)
        .collect();
    assert_eq!(labels, I13_REGISTER, "the I-13 HTTP register drifted");

    for row in API_ROUTES {
        assert_metadata(&row);
        let (status, _, _) = send(
            api.clone(),
            row.method.alternate_method(),
            row.path,
            Some("v09-key"),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::METHOD_NOT_ALLOWED,
            "{} method",
            row.label
        );

        let (status, _, body) =
            send(api.clone(), row.method.request_method(), row.path, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{} auth", row.label);
        let error: Value = serde_json::from_slice(&body).expect("auth error JSON");
        assert_eq!(
            error["error"]["code"], "UNAUTHORIZED",
            "{} error code",
            row.label
        );
        assert!(
            error["error"]["correlation_id"]
                .as_str()
                .is_some_and(|id| !id.is_empty()),
            "{} correlation id",
            row.label
        );
    }

    for row in ADMIN_ROUTES {
        assert_metadata(&row);
        let (status, _, _) =
            send(admin.clone(), row.method.alternate_method(), row.path, None).await;
        assert_eq!(
            status,
            StatusCode::METHOD_NOT_ALLOWED,
            "{} method",
            row.label
        );

        let (status, _, body) =
            send(admin.clone(), row.method.request_method(), row.path, None).await;
        assert_eq!(status, StatusCode::OK, "{} loopback allowlist", row.label);
        if row.path == "/status" {
            let status: Value = serde_json::from_slice(&body).expect("status JSON");
            assert_eq!(status["status"], "ok");
        }
    }
}
