use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client::http::{ClientResult, HttpClient};

#[derive(Debug, Default)]
pub struct AdminResourcesRequest {
    pub limit: Option<usize>,
    pub include_columnar_columns: Option<bool>,
    pub columnar_column_limit: Option<usize>,
    pub kv_prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AdminResourcesResponse {
    pub sql_tables: Vec<SqlTableResource>,
    pub columnar_segments: Vec<ColumnarSegmentResource>,
    pub kv_keys: Vec<String>,
    pub truncated: TruncatedSections,
}

#[derive(Debug, Deserialize)]
pub struct TruncatedSections {
    pub sql_tables: bool,
    pub columnar_segments: bool,
    pub kv_keys: bool,
}

#[derive(Debug, Deserialize)]
pub struct SqlTableResource {
    pub name: String,
    pub columns: Vec<SqlColumnResource>,
}

#[derive(Debug, Deserialize)]
pub struct SqlColumnResource {
    pub name: String,
    #[allow(dead_code)]
    pub data_type: String,
}

#[derive(Debug, Deserialize)]
pub struct ColumnarSegmentResource {
    pub id: String,
    pub columns: Option<Vec<String>>,
}

/// Public cluster-management verbs accepted by the v0.8 admin API.  This
/// mirrors the wire contract without exposing a consensus implementation to
/// the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterManagementOperation {
    MetadataShow,
    MembersList,
    MembersReplace,
    RangesList,
    RangesRegister,
    RangesUpdate,
    RangesRetire,
    PlacementGet,
    PlacementSet,
    PlacementReplace,
    ReadPolicyGet,
    ReadPolicySet,
    SchemaOwnerGet,
    SchemaOwnerSet,
    SchemaRolloutStart,
    SchemaRolloutStatus,
    RecoveryStatus,
    RecoveryRestore,
    UpgradeStatus,
    UpgradeStart,
}

/// Wire payload for one authenticated, idempotent cluster management
/// invocation. `target` is JSON because the server adapter intentionally
/// delegates operation-specific validation to the committed metadata layer.
#[derive(Debug, Serialize)]
pub struct ClusterManagementRequest {
    pub request_id: String,
    pub operation: ClusterManagementOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<Value>,
    pub confirmed: bool,
}

impl ClusterManagementRequest {
    pub fn new(
        request_id: impl Into<String>,
        operation: ClusterManagementOperation,
        expected_version: Option<u64>,
        target: Option<Value>,
        confirmed: bool,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            operation,
            expected_version,
            target,
            confirmed,
        }
    }
}

/// Stable machine-readable management result returned by the admin API.
#[derive(Debug, Deserialize)]
pub struct ClusterManagementResponse {
    pub operation_id: String,
    #[allow(dead_code)]
    pub operation: String,
    pub outcome_class: String,
    pub reason: String,
    pub state_version: Option<u64>,
    pub control: ClusterControlAvailability,
    pub actor: Option<String>,
}

/// Capability state included in each cluster management result.
#[derive(Debug, Deserialize)]
pub struct ClusterControlAvailability {
    pub available: bool,
    pub mode: String,
    pub reason: String,
    pub missing_prerequisites: Vec<Value>,
}

pub async fn fetch_admin_resources(
    client: &HttpClient,
    request: &AdminResourcesRequest,
) -> ClientResult<AdminResourcesResponse> {
    let path = build_query_path(request);
    client.get_json(&path).await
}

/// Invoke the same authenticated cluster-management endpoint for every
/// `server cluster` command. HTTP status and authorization handling stays in
/// `HttpClient`, so CLI commands cannot bypass the server's admin boundary.
pub async fn invoke_cluster_management(
    client: &HttpClient,
    request: &ClusterManagementRequest,
) -> ClientResult<ClusterManagementResponse> {
    client
        .post_json("api/admin/cluster/operations", request)
        .await
}

fn build_query_path(request: &AdminResourcesRequest) -> String {
    let mut params = Vec::new();
    if let Some(limit) = request.limit {
        params.push(format!("limit={limit}"));
    }
    if let Some(include) = request.include_columnar_columns {
        params.push(format!("include_columnar_columns={include}"));
    }
    if let Some(columnar_column_limit) = request.columnar_column_limit {
        params.push(format!("columnar_column_limit={columnar_column_limit}"));
    }
    if let Some(prefix) = request.kv_prefix.as_deref() {
        params.push(format!("kv_prefix={}", encode_query_component(prefix)));
    }

    if params.is_empty() {
        "api/admin/resources".to_string()
    } else {
        format!("api/admin/resources?{}", params.join("&"))
    }
}

fn encode_query_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        build_query_path, AdminResourcesRequest, ClusterManagementOperation,
        ClusterManagementRequest,
    };
    use serde_json::json;

    #[test]
    fn build_query_path_includes_params() {
        let request = AdminResourcesRequest {
            limit: Some(10),
            include_columnar_columns: Some(true),
            columnar_column_limit: Some(5),
            kv_prefix: Some("app/".to_string()),
        };
        let path = build_query_path(&request);
        assert!(path.starts_with("api/admin/resources?"));
        assert!(path.contains("limit=10"));
        assert!(path.contains("include_columnar_columns=true"));
        assert!(path.contains("columnar_column_limit=5"));
        assert!(path.contains("kv_prefix=app%2F"));
    }

    #[test]
    fn cluster_management_request_preserves_idempotency_and_confirmation() {
        let request = ClusterManagementRequest::new(
            "operation-42",
            ClusterManagementOperation::RangesRegister,
            Some(9),
            Some(json!({"range_id": "primary/0"})),
            true,
        );

        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "request_id": "operation-42",
                "operation": "ranges_register",
                "expected_version": 9,
                "target": {"range_id": "primary/0"},
                "confirmed": true,
            })
        );
    }
}
