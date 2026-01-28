use serde::Deserialize;

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

pub async fn fetch_admin_resources(
    client: &HttpClient,
    request: &AdminResourcesRequest,
) -> ClientResult<AdminResourcesResponse> {
    let path = build_query_path(request);
    client.get_json(&path).await
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
    use super::{build_query_path, AdminResourcesRequest};

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
}
