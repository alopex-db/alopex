use std::sync::Arc;

use alopex_core::columnar::kvs_bridge::ColumnarKvsBridge;
use alopex_core::columnar::segment_v2::SegmentMetaV2;
use alopex_core::storage::format::bincode_config;
use alopex_core::types::TxnMode;
use alopex_core::{KVStore, KVTransaction};
use alopex_sql::ast::ddl::VectorMetric;
use alopex_sql::catalog::persistent::{PersistedTableMeta, TABLES_PREFIX};
use alopex_sql::catalog::{
    Catalog, CatalogError, CatalogOverlay, PersistentCatalog, TableMetadata,
};
use alopex_sql::planner::types::ResolvedType;
use axum::extract::{Extension, Query};
use axum::response::Response;
use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::error::{Result, ServerError};
use crate::http::{error_response, json_response, RequestContext};
use crate::server::ServerState;

const DEFAULT_RESOURCE_LIMIT: usize = 50;
const DEFAULT_COLUMNAR_COLUMN_LIMIT: usize = 20;
const SYSTEM_PREFIXES: [&str; 6] = [
    "__catalog__/",
    "hnsw:",
    "__alopex_",
    "__alopex:",
    "vector:",
    "columnar:",
];

#[derive(Debug, Deserialize)]
pub struct AdminResourcesQuery {
    pub limit: Option<usize>,
    pub include_columnar_columns: Option<bool>,
    pub columnar_column_limit: Option<usize>,
    pub kv_prefix: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AdminResourcesResponse {
    pub sql_tables: Vec<SqlTableResource>,
    pub columnar_segments: Vec<ColumnarSegmentResource>,
    pub kv_keys: Vec<String>,
    pub truncated: TruncatedSections,
}

#[derive(Debug, Serialize)]
pub struct TruncatedSections {
    pub sql_tables: bool,
    pub columnar_segments: bool,
    pub kv_keys: bool,
}

#[derive(Debug, Serialize)]
pub struct SqlTableResource {
    pub name: String,
    pub columns: Vec<SqlColumnResource>,
}

#[derive(Debug, Serialize)]
pub struct SqlColumnResource {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Serialize)]
pub struct ColumnarSegmentResource {
    pub id: String,
    pub columns: Option<Vec<String>>,
}

pub async fn list(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Query(query): Query<AdminResourcesQuery>,
) -> Response {
    match list_impl(state.clone(), query) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

fn list_impl(
    state: Arc<ServerState>,
    query: AdminResourcesQuery,
) -> Result<AdminResourcesResponse> {
    let limit = query.limit.unwrap_or(DEFAULT_RESOURCE_LIMIT);
    let columnar_column_limit = query
        .columnar_column_limit
        .unwrap_or(DEFAULT_COLUMNAR_COLUMN_LIMIT);
    let include_columnar_columns = query.include_columnar_columns.unwrap_or(false);

    let (sql_tables, sql_truncated) =
        list_sql_resources(state.store.clone(), state.catalog.clone(), limit)?;
    let (columnar_segments, columnar_truncated) = list_columnar_resources(
        state.store.clone(),
        limit,
        include_columnar_columns,
        columnar_column_limit,
    )?;
    let (kv_keys, kv_truncated) = list_kv_resources(state.store.clone(), limit, query.kv_prefix)?;

    Ok(AdminResourcesResponse {
        sql_tables,
        columnar_segments,
        kv_keys,
        truncated: TruncatedSections {
            sql_tables: sql_truncated,
            columnar_segments: columnar_truncated,
            kv_keys: kv_truncated,
        },
    })
}

fn list_sql_resources(
    store: Arc<alopex_core::kv::any::AnyKV>,
    catalog: Arc<std::sync::RwLock<dyn Catalog + Send + Sync>>,
    limit: usize,
) -> Result<(Vec<SqlTableResource>, bool)> {
    let mut tables = list_sql_resources_from_store(store.clone())?;
    if tables.is_empty() {
        tables = match PersistentCatalog::load(store.clone()) {
            Ok(catalog) => {
                let overlay = CatalogOverlay::new();
                let mut tables = catalog.list_tables_in_txn("default", "default", &overlay);
                if tables.is_empty() {
                    tables = catalog.list_tables_in_txn("main", "default", &overlay);
                }
                tables
            }
            Err(CatalogError::Kv(alopex_core::Error::NotFound)) => Vec::new(),
            Err(err) => return Err(ServerError::Catalog(err)),
        };
    }
    if tables.is_empty() {
        let guard = catalog
            .read()
            .map_err(|_| ServerError::Internal("catalog lock poisoned".into()))?;
        tables = guard.list_tables();
    }

    tables.sort_by(|a, b| a.name.cmp(&b.name));
    let truncated = tables.len() > limit;
    if tables.len() > limit {
        tables.truncate(limit);
    }

    let resources = tables
        .into_iter()
        .map(|table| SqlTableResource {
            name: table.name,
            columns: table
                .columns
                .into_iter()
                .map(|column| SqlColumnResource {
                    name: column.name,
                    data_type: resolved_type_to_string(&column.data_type),
                })
                .collect(),
        })
        .collect();

    Ok((resources, truncated))
}

fn list_sql_resources_from_store(
    store: Arc<alopex_core::kv::any::AnyKV>,
) -> Result<Vec<TableMetadata>> {
    let mut txn = store.begin(TxnMode::ReadOnly)?;
    let mut tables = Vec::new();
    for (_key, value) in txn.scan_prefix(TABLES_PREFIX)? {
        let persisted: PersistedTableMeta = bincode_config()
            .deserialize(&value)
            .map_err(|err| ServerError::BadRequest(format!("catalog entry invalid: {err}")))?;
        tables.push(TableMetadata::from(persisted));
    }
    txn.commit_self()?;
    Ok(tables)
}

fn list_columnar_resources(
    store: Arc<alopex_core::kv::any::AnyKV>,
    limit: usize,
    include_columns: bool,
    columnar_column_limit: usize,
) -> Result<(Vec<ColumnarSegmentResource>, bool)> {
    let bridge = ColumnarKvsBridge::new(store);
    let mut segments = bridge.list_segments().map_err(map_columnar_error)?;
    segments.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let truncated = segments.len() > limit;
    if segments.len() > limit {
        segments.truncate(limit);
    }

    let mut resources = Vec::with_capacity(segments.len());
    for (table_id, segment_id) in segments {
        let id = format!("{}:{}", table_id, segment_id);
        let columns = if include_columns {
            Some(list_columnar_columns(
                &bridge,
                table_id,
                segment_id,
                columnar_column_limit,
            )?)
        } else {
            None
        };
        resources.push(ColumnarSegmentResource { id, columns });
    }

    Ok((resources, truncated))
}

fn list_columnar_columns(
    bridge: &ColumnarKvsBridge,
    table_id: u32,
    segment_id: u64,
    limit: usize,
) -> Result<Vec<String>> {
    let stats = bridge
        .read_statistics(table_id, segment_id)
        .map_err(map_columnar_error)?;
    let meta: SegmentMetaV2 = bincode_config()
        .deserialize(&stats)
        .map_err(|err| ServerError::BadRequest(format!("columnar segment invalid: {err}")))?;
    let mut names: Vec<String> = meta
        .schema
        .columns
        .into_iter()
        .map(|column| column.name)
        .collect();
    if names.len() > limit {
        names.truncate(limit);
    }
    Ok(names)
}

fn list_kv_resources(
    store: Arc<alopex_core::kv::any::AnyKV>,
    limit: usize,
    kv_prefix: Option<String>,
) -> Result<(Vec<String>, bool)> {
    let prefix = kv_prefix.unwrap_or_default();
    let mut txn = store.begin(TxnMode::ReadOnly)?;
    let mut keys = Vec::new();
    let mut truncated = false;
    for (key, _) in txn.scan_prefix(prefix.as_bytes())? {
        if is_system_key(&key) {
            continue;
        }
        let Ok(key_str) = std::str::from_utf8(&key) else {
            continue;
        };
        if key_str.is_empty() || key_str.chars().any(|ch| ch.is_control()) {
            continue;
        }
        if keys.len() < limit {
            keys.push(key_str.to_string());
        } else {
            truncated = true;
            break;
        }
    }
    txn.commit_self()?;
    Ok((keys, truncated))
}

fn is_system_key(key: &[u8]) -> bool {
    SYSTEM_PREFIXES
        .iter()
        .any(|prefix| key.starts_with(prefix.as_bytes()))
}

fn resolved_type_to_string(resolved_type: &ResolvedType) -> String {
    match resolved_type {
        ResolvedType::Integer => "INTEGER".to_string(),
        ResolvedType::BigInt => "BIGINT".to_string(),
        ResolvedType::Float => "FLOAT".to_string(),
        ResolvedType::Double => "DOUBLE".to_string(),
        ResolvedType::Text => "TEXT".to_string(),
        ResolvedType::Blob => "BLOB".to_string(),
        ResolvedType::Boolean => "BOOLEAN".to_string(),
        ResolvedType::Timestamp => "TIMESTAMP".to_string(),
        ResolvedType::Vector { dimension, metric } => {
            let metric = match metric {
                VectorMetric::Cosine => "COSINE",
                VectorMetric::L2 => "L2",
                VectorMetric::Inner => "INNER",
            };
            format!("VECTOR({dimension}, {metric})")
        }
        ResolvedType::Null => "NULL".to_string(),
    }
}

fn map_columnar_error(err: alopex_core::columnar::ColumnarError) -> ServerError {
    match err {
        alopex_core::columnar::ColumnarError::NotFound => {
            ServerError::NotFound("columnar segment not found".into())
        }
        alopex_core::columnar::ColumnarError::InvalidFormat(message) => {
            ServerError::BadRequest(format!("columnar segment invalid: {message}"))
        }
        alopex_core::columnar::ColumnarError::MemoryLimitExceeded { limit, requested } => {
            ServerError::PayloadTooLarge(format!(
                "memory limit {limit} exceeded by {requested} bytes"
            ))
        }
        other => ServerError::Core(other.into()),
    }
}
