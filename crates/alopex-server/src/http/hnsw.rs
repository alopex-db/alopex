use std::sync::Arc;

use alopex_core::kv::KVTransaction;
use alopex_core::types::TxnMode;
use alopex_core::vector::hnsw::{HnswConfig, HnswIndex, HnswSearchResult, HnswStats};
use alopex_core::vector::Metric;
use alopex_core::KVStore;
use axum::extract::Extension;
use axum::response::Response;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{Result, ServerError};
use crate::http::{error_response, json_response, RequestContext};
use crate::server::ServerState;

const DEFAULT_M: usize = 16;
const DEFAULT_EF_CONSTRUCTION: usize = 200;

#[derive(Debug, Deserialize)]
pub struct HnswSearchRequest {
    pub index: String,
    pub query: Vec<f32>,
    #[serde(default = "default_k")]
    pub k: usize,
}

#[derive(Debug, Deserialize)]
pub struct HnswUpsertRequest {
    pub index: String,
    pub key: Vec<u8>,
    pub vector: Vec<f32>,
}

#[derive(Debug, Deserialize)]
pub struct HnswDeleteRequest {
    pub index: String,
    pub key: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub struct HnswCreateRequest {
    pub index: String,
    pub dim: usize,
    pub metric: String,
}

#[derive(Debug, Deserialize)]
pub struct HnswDropRequest {
    pub index: String,
}

#[derive(Debug, Deserialize)]
pub struct HnswStatsRequest {
    pub index: String,
}

#[derive(Debug, Serialize)]
pub struct HnswSearchResponse {
    pub results: Vec<HnswSearchResult>,
}

#[derive(Debug, Serialize)]
pub struct HnswStatsResponse {
    pub stats: HnswStats,
}

#[derive(Debug, Serialize)]
pub struct HnswStatusResponse {
    pub success: bool,
}

pub async fn search(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<HnswSearchRequest>,
) -> Response {
    match search_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn upsert(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<HnswUpsertRequest>,
) -> Response {
    match upsert_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn delete(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<HnswDeleteRequest>,
) -> Response {
    match delete_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn create(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<HnswCreateRequest>,
) -> Response {
    match create_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn drop(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<HnswDropRequest>,
) -> Response {
    match drop_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

pub async fn stats(
    Extension(state): Extension<Arc<ServerState>>,
    Extension(ctx): Extension<RequestContext>,
    Json(request): Json<HnswStatsRequest>,
) -> Response {
    match stats_impl(state.clone(), request) {
        Ok(resp) => json_response(resp, state.config.max_response_size, &ctx),
        Err(err) => error_response(err, &ctx),
    }
}

fn search_impl(state: Arc<ServerState>, request: HnswSearchRequest) -> Result<HnswSearchResponse> {
    let mut txn = state.store.begin(TxnMode::ReadOnly)?;
    let index = HnswIndex::load(&request.index, &mut txn).map_err(map_core_error)?;
    let (results, _) = index
        .search(&request.query, request.k, None)
        .map_err(map_core_error)?;
    txn.commit_self()?;
    Ok(HnswSearchResponse { results })
}

fn upsert_impl(state: Arc<ServerState>, request: HnswUpsertRequest) -> Result<HnswStatusResponse> {
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let mut index = HnswIndex::load(&request.index, &mut txn).map_err(map_core_error)?;
    index
        .upsert(&request.key, &request.vector, &[])
        .map_err(map_core_error)?;
    index.save(&mut txn).map_err(map_core_error)?;
    txn.commit_self()?;
    Ok(HnswStatusResponse { success: true })
}

fn delete_impl(state: Arc<ServerState>, request: HnswDeleteRequest) -> Result<HnswStatusResponse> {
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let mut index = HnswIndex::load(&request.index, &mut txn).map_err(map_core_error)?;
    index.delete(&request.key).map_err(map_core_error)?;
    index.save(&mut txn).map_err(map_core_error)?;
    txn.commit_self()?;
    Ok(HnswStatusResponse { success: true })
}

fn create_impl(state: Arc<ServerState>, request: HnswCreateRequest) -> Result<HnswStatusResponse> {
    let metric = parse_metric(&request.metric)?;
    let config = HnswConfig {
        dimension: request.dim,
        metric,
        m: DEFAULT_M,
        ef_construction: DEFAULT_EF_CONSTRUCTION,
    };
    config.validate().map_err(map_core_error)?;

    let index = HnswIndex::create(&request.index, config).map_err(map_core_error)?;
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    index.save(&mut txn).map_err(map_core_error)?;
    txn.commit_self()?;
    Ok(HnswStatusResponse { success: true })
}

fn drop_impl(state: Arc<ServerState>, request: HnswDropRequest) -> Result<HnswStatusResponse> {
    let mut txn = state.store.begin(TxnMode::ReadWrite)?;
    let index = HnswIndex::load(&request.index, &mut txn).map_err(map_core_error)?;
    index.drop(&mut txn).map_err(map_core_error)?;
    txn.commit_self()?;
    Ok(HnswStatusResponse { success: true })
}

fn stats_impl(state: Arc<ServerState>, request: HnswStatsRequest) -> Result<HnswStatsResponse> {
    let mut txn = state.store.begin(TxnMode::ReadOnly)?;
    let index = HnswIndex::load(&request.index, &mut txn).map_err(map_core_error)?;
    let stats = index.stats();
    txn.commit_self()?;
    Ok(HnswStatsResponse { stats })
}

fn parse_metric(raw: &str) -> Result<Metric> {
    match raw {
        "cosine" => Ok(Metric::Cosine),
        "l2" => Ok(Metric::L2),
        "ip" => Ok(Metric::InnerProduct),
        other => Err(ServerError::BadRequest(format!("unknown metric: {other}"))),
    }
}

fn default_k() -> usize {
    10
}

fn map_core_error(err: alopex_core::Error) -> ServerError {
    match err {
        alopex_core::Error::NotFound => ServerError::NotFound("index not found".into()),
        alopex_core::Error::InvalidParameter { param, reason } => {
            ServerError::BadRequest(format!("invalid parameter {param}: {reason}"))
        }
        other => ServerError::Core(other),
    }
}
