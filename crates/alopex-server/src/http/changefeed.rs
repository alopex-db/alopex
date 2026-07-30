//! HTTP changefeed lifecycle routes and their canonical wire mapping.
//!
//! Authorization facts are loaded from server configuration and are bound to
//! the middleware-authenticated actor.  The request body never grants a
//! tenant, range, or scope.  The registry owns one coordinator for the server
//! lifetime; it never falls back to a local WAL when Durable preflight fails.

use std::collections::BTreeMap;

use alopex_cluster::{
    changefeed::{
        ChangefeedAccessRequest, ChangefeedAction, ChangefeedAuthorization,
        ChangefeedAuthorizationDecision, ChangefeedScope, DurableProfileAdapter, FeedCoordinator,
        FeedDelivery, FeedPreflight, FeedRequest,
    },
    AuthenticatedSubject, ChangeEventEnvelope, ChangeOperationType, ChangefeedOutcome,
    FailureClass, FeedIdentity, OperationState, OrderingScope, Placement, PlacementReadiness,
    PlacementRole, RangeIdentity, RetentionWindow, RoutingOutcome, RoutingOutcomeKind,
};
use axum::{
    extract::{Extension, Path, Query},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::{
    config::{ChangefeedScopeConfig, ChangefeedServerConfig},
    error::{Result, ServerError},
    server::ServerState,
};

use super::{error_response, RequestContext};

/// Server-owned changefeed state shared by every HTTP lifecycle route.
///
/// The preflight value is captured exactly once from the Durable adapter.  A
/// rejected preflight stays rejected; no handler may replace it with local
/// storage or create a best-effort subscription.
#[derive(Debug)]
pub struct ChangefeedRegistry {
    preflight: FeedPreflight,
    coordinator: FeedCoordinator,
    authorizations: BTreeMap<(String, String), ChangefeedAuthorization>,
    feeds: BTreeMap<String, RegisteredFeed>,
}

#[derive(Debug, Clone)]
struct RegisteredFeed {
    feed: FeedIdentity,
    routing: RoutingOutcome,
    authorization: ChangefeedAuthorization,
}

impl ChangefeedRegistry {
    /// Builds the registry from independently verified Durable evidence and
    /// server configuration. Duplicate subject/tenant entries are rejected so
    /// authorization resolution is never order-dependent.
    pub fn from_config(
        adapter: DurableProfileAdapter,
        config: &ChangefeedServerConfig,
    ) -> Result<Self> {
        let mut authorizations = BTreeMap::new();
        for grant in &config.authorizations {
            let authorization = ChangefeedAuthorization {
                subject: AuthenticatedSubject::new(grant.subject.trim()),
                tenant: grant.tenant.trim().to_owned(),
                allowed_ranges: grant
                    .allowed_ranges
                    .iter()
                    .map(|range| range.trim().into())
                    .collect(),
                allowed_scopes: grant
                    .allowed_scopes
                    .iter()
                    .copied()
                    .map(changefeed_scope)
                    .collect(),
            };
            let key = (
                authorization.subject.as_str().to_owned(),
                authorization.tenant.clone(),
            );
            if authorizations.insert(key, authorization).is_some() {
                return Err(ServerError::InvalidConfig(
                    "duplicate changefeed authorization subject/tenant".into(),
                ));
            }
        }

        let preflight = adapter.preflight();
        Ok(Self {
            coordinator: FeedCoordinator::new(preflight.clone()),
            preflight,
            authorizations,
            feeds: BTreeMap::new(),
        })
    }

    fn preflight_is_ready(&self) -> bool {
        self.preflight.is_ready()
    }

    fn authorization_for(
        &self,
        subject: &AuthenticatedSubject,
        tenant: &str,
    ) -> Option<ChangefeedAuthorization> {
        self.authorizations
            .get(&(subject.as_str().to_owned(), tenant.to_owned()))
            .cloned()
    }

    fn existing(&self, feed_id: &str) -> Option<RegisteredFeed> {
        self.feeds.get(feed_id).cloned()
    }

    fn register(
        &mut self,
        feed: FeedIdentity,
        routing: RoutingOutcome,
        authorization: ChangefeedAuthorization,
    ) {
        self.feeds.insert(
            feed.feed_id.clone(),
            RegisteredFeed {
                feed,
                routing,
                authorization,
            },
        );
    }
}

/// Checks that the server-established authorization belongs to the
/// middleware-authenticated actor, then checks tenant/range/scope ownership.
///
/// All denials collapse to `Denied`; callers must use the shared redacted
/// outcome and must not expose the failing scope, tenant, or range fact.
#[must_use]
pub fn authorize_changefeed(
    context: &RequestContext,
    authorization: &ChangefeedAuthorization,
    request: ChangefeedAccessRequest,
) -> ChangefeedAuthorizationDecision {
    let subject = AuthenticatedSubject::new(context.actor.as_deref().unwrap_or("anonymous"));
    if authorization.subject != subject {
        ChangefeedAuthorizationDecision::Denied
    } else {
        authorization.authorize(request)
    }
}

/// Request body for `POST /v1/changefeeds`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateChangefeedRequest {
    /// Caller-supplied idempotency identity for creation.
    pub request_id: String,
    /// Tenant selected by the caller and checked against server policy.
    pub tenant: String,
    /// Actor assertion that must equal the middleware-authenticated actor.
    pub actor: String,
    /// Table target. Exactly one of `table` or `range_id` is required.
    pub table: Option<String>,
    /// Explicit range target. Exactly one of `table` or `range_id` is required.
    pub range_id: Option<String>,
    /// Requested retention metadata for the new feed.
    #[serde(default = "RetentionWindow::unbounded")]
    pub retention: RetentionWindow,
    /// Explicit event kinds requested by the consumer. Unsupported kinds are
    /// rejected before the coordinator is called.
    #[serde(default)]
    pub change_kinds: Vec<ChangeOperationType>,
}

/// Request body for `POST /v1/changefeeds/{id}/subscribe`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeChangefeedRequest {
    /// Caller-supplied idempotency identity.
    pub request_id: String,
    /// Required range generation observed by the caller.
    pub expected_generation: u64,
    /// Required range data epoch observed by the caller.
    pub expected_epoch: u64,
}

/// Query parameters for polling or streaming one feed.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryQuery {
    /// Caller-supplied idempotency identity.
    pub request_id: String,
    /// Maximum retained events in this response.
    pub max_events: usize,
    /// Caller deadline represented in the versioned lifecycle schema.
    pub deadline_epoch: u64,
}

/// Request body for `POST /v1/changefeeds/{id}/ack`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckChangefeedRequest {
    /// Caller-supplied idempotency identity.
    pub request_id: String,
    /// Stable acknowledgement identity.
    pub ack_id: String,
    /// Encoded checkpoint whose feed/range binding is validated before ack.
    pub checkpoint: String,
}

/// Request body for `POST /v1/changefeeds/{id}/resume`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeChangefeedRequest {
    /// Caller-supplied idempotency identity.
    pub request_id: String,
    /// Encoded checkpoint to resume strictly after.
    pub checkpoint: String,
}

/// Request body for cancel and close operations.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleRequest {
    /// Caller-supplied idempotency identity.
    pub request_id: String,
}

#[derive(Serialize)]
struct OutcomeResponse {
    #[serde(flatten)]
    outcome: ChangefeedOutcome,
    correlation_id: String,
}

#[derive(Serialize)]
struct DeliveryResponse {
    #[serde(flatten)]
    outcome: ChangefeedOutcome,
    events: Vec<ChangeEventEnvelope>,
    correlation_id: String,
}

#[derive(Serialize)]
struct StreamEventLine {
    event: ChangeEventEnvelope,
    correlation_id: String,
}

/// Creates a feed after actor, tenant, range, scope, Durable, and target
/// metadata checks. Rejected checks are returned as canonical outcomes rather
/// than being remapped to successful empty JSON.
pub async fn create(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<CreateChangefeedRequest>,
) -> Response {
    let request_id = match non_empty(&context, &request.request_id, "request_id") {
        Ok(value) => value,
        Err(response) => return response,
    };
    if let Err(response) = validate_actor(&context, &request.actor) {
        return response;
    }
    let target = match create_target(&context, &request) {
        Ok(target) => target,
        Err(response) => return response,
    };
    let subject = match authenticated_subject(&state, &context) {
        Ok(subject) => subject,
        Err(response) => return response,
    };
    let operation = operation_id("create", request_id);

    let mut registry = match state.changefeed_registry.lock() {
        Ok(registry) => registry,
        Err(_) => {
            return error_response(
                ServerError::Internal("changefeed registry lock poisoned".into()),
                &context,
            )
        }
    };
    let authorization = registry.authorization_for(&subject, &request.tenant);
    let synthetic = redacted_feed(&feed_id_for(request_id), &target, request.retention.clone());
    let synthetic_routing = redacted_routing(&synthetic);
    let create_request = match feed_request(&context, &operation, request_id) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let Some(authorization) = authorization else {
        return denied_response(synthetic, synthetic_routing, create_request, &context);
    };
    let resolved = if target.table.is_some() {
        match resolve_feed(&state, &context, &target, request.retention.clone()) {
            Ok(metadata) => Some(metadata),
            Err(response) => return response,
        }
    } else {
        None
    };
    let (candidate_feed, candidate_routing) = resolved
        .clone()
        .unwrap_or_else(|| (synthetic.clone(), synthetic_routing.clone()));
    let access = ChangefeedAccessRequest {
        action: ChangefeedAction::Create,
        tenant: request.tenant.clone(),
        range_id: candidate_feed.range.range_id.clone(),
    };
    if !authorize_changefeed(&context, &authorization, access).permits() {
        return denied_response(synthetic, synthetic_routing, create_request, &context);
    }
    if let Some(response) = unsupported_change_kind_response(
        &request.change_kinds,
        candidate_feed.clone(),
        candidate_routing.clone(),
        create_request.clone(),
        &context,
    ) {
        return response;
    }
    if !registry.preflight_is_ready() {
        let outcome =
            match registry
                .coordinator
                .create(candidate_feed, candidate_routing, create_request)
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    return error_response(ServerError::Internal(error.to_string()), &context)
                }
            };
        return outcome_response(outcome, &context);
    }
    drop(registry);

    let (feed, routing) = match resolved {
        Some(metadata) => metadata,
        None => match resolve_feed(&state, &context, &target, request.retention) {
            Ok(metadata) => metadata,
            Err(response) => return response,
        },
    };
    let mut registry = match state.changefeed_registry.lock() {
        Ok(registry) => registry,
        Err(_) => {
            return error_response(
                ServerError::Internal("changefeed registry lock poisoned".into()),
                &context,
            )
        }
    };
    let create_request = match feed_request(&context, &operation, request_id) {
        Ok(request) => request,
        Err(response) => return response,
    };
    if !authorize_changefeed(
        &context,
        &authorization,
        ChangefeedAccessRequest {
            action: ChangefeedAction::Create,
            tenant: request.tenant.clone(),
            range_id: feed.range.range_id.clone(),
        },
    )
    .permits()
    {
        return denied_response(synthetic, synthetic_routing, create_request, &context);
    }
    let outcome = match registry
        .coordinator
        .create(feed.clone(), routing.clone(), create_request)
    {
        Ok(outcome) => outcome,
        Err(error) => return error_response(ServerError::Internal(error.to_string()), &context),
    };
    if outcome.failure_class.is_none() {
        registry.register(feed, routing, authorization);
    }
    outcome_response(outcome, &context)
}

/// Subscribes to a feed after generation/epoch and read-scope validation.
pub async fn subscribe(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Json(request): Json<SubscribeChangefeedRequest>,
) -> Response {
    let request_id = match non_empty(&context, &request.request_id, "request_id") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let mut registry = match state.changefeed_registry.lock() {
        Ok(registry) => registry,
        Err(_) => {
            return error_response(
                ServerError::Internal("changefeed registry lock poisoned".into()),
                &context,
            )
        }
    };
    let feed_request =
        match feed_request(&context, &operation_id("subscribe", request_id), request_id) {
            Ok(request) => request,
            Err(response) => return response,
        };
    let Some(registered) = authorized_existing(
        &state,
        &context,
        &registry,
        &feed_id,
        ChangefeedAction::Subscribe,
        &feed_request,
    ) else {
        return denied_response(
            redacted_feed_id(&feed_id),
            redacted_routing_for_id(&feed_id),
            feed_request,
            &context,
        );
    };
    let outcome = match registry.coordinator.subscribe(
        &feed_id,
        request.expected_generation,
        request.expected_epoch,
        feed_request,
    ) {
        Ok(outcome) => outcome,
        Err(error) => return coordinator_error_response(error.to_string(), &registered, &context),
    };
    outcome_response(outcome, &context)
}

/// Returns a bounded JSON delivery batch for one authorized feed.
pub async fn poll(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Query(query): Query<DeliveryQuery>,
) -> Response {
    delivery(state, context, feed_id, query, ChangefeedAction::Poll).await
}

/// Returns the same canonical delivery contract as poll, encoded as JSONL.
pub async fn stream(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Query(query): Query<DeliveryQuery>,
) -> Response {
    let delivery =
        match delivery_result(&state, &context, &feed_id, &query, ChangefeedAction::Stream) {
            Ok(delivery) => delivery,
            Err(response) => return response,
        };
    let mut lines = Vec::new();
    for event in delivery.events {
        let line = StreamEventLine {
            event,
            correlation_id: context.correlation_id.clone(),
        };
        match serde_json::to_string(&line) {
            Ok(line) => lines.push(line),
            Err(error) => {
                return error_response(ServerError::Internal(error.to_string()), &context)
            }
        }
    }
    match serde_json::to_string(&OutcomeResponse {
        outcome: delivery.outcome.clone(),
        correlation_id: context.correlation_id.clone(),
    }) {
        Ok(line) => lines.push(line),
        Err(error) => return error_response(ServerError::Internal(error.to_string()), &context),
    }
    let body = format!("{}\n", lines.join("\n"));
    if body.len() > state.config.max_response_size {
        return error_response(
            ServerError::PayloadTooLarge("response size exceeds limit".into()),
            &context,
        );
    }
    let status = status_code(&delivery.outcome);
    (
        status,
        [(header::CONTENT_TYPE, "application/x-ndjson")],
        body,
    )
        .into_response()
}

/// Validates checkpoint binding and accepts an acknowledgement through the
/// coordinator. The coordinator deliberately reports `accepted`, never a
/// fabricated durable checkpoint.
pub async fn ack(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Json(request): Json<AckChangefeedRequest>,
) -> Response {
    let request_id = match non_empty(&context, &request.request_id, "request_id") {
        Ok(value) => value,
        Err(response) => return response,
    };
    if request.ack_id.trim().is_empty() || request.checkpoint.trim().is_empty() {
        return error_response(
            ServerError::BadRequest("missing ack_id or checkpoint".into()),
            &context,
        );
    }
    let registry = match state.changefeed_registry.lock() {
        Ok(registry) => registry,
        Err(_) => {
            return error_response(
                ServerError::Internal("changefeed registry lock poisoned".into()),
                &context,
            )
        }
    };
    let feed_request = match feed_request(&context, &operation_id("ack", request_id), request_id) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let Some(registered) = authorized_existing(
        &state,
        &context,
        &registry,
        &feed_id,
        ChangefeedAction::Ack,
        &feed_request,
    ) else {
        return denied_response(
            redacted_feed_id(&feed_id),
            redacted_routing_for_id(&feed_id),
            feed_request,
            &context,
        );
    };
    if alopex_cluster::changefeed::CheckpointCursor::decode_for(
        &request.checkpoint,
        &registered.feed.feed_id,
        &registered.feed.range.range_id,
    )
    .is_err()
    {
        return invalid_checkpoint_response(&registered, feed_request, &context);
    }
    let outcome = match registry
        .coordinator
        .ack(&feed_id, request.ack_id, feed_request)
    {
        Ok(outcome) => outcome,
        Err(error) => return coordinator_error_response(error.to_string(), &registered, &context),
    };
    outcome_response(outcome, &context)
}

/// Resumes strictly after the supplied checkpoint and returns a JSON batch.
pub async fn resume(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Json(request): Json<ResumeChangefeedRequest>,
) -> Response {
    let request_id = match non_empty(&context, &request.request_id, "request_id") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let registry = match state.changefeed_registry.lock() {
        Ok(registry) => registry,
        Err(_) => {
            return error_response(
                ServerError::Internal("changefeed registry lock poisoned".into()),
                &context,
            )
        }
    };
    let feed_request = match feed_request(&context, &operation_id("resume", request_id), request_id)
    {
        Ok(request) => request,
        Err(response) => return response,
    };
    let Some(registered) = authorized_existing(
        &state,
        &context,
        &registry,
        &feed_id,
        ChangefeedAction::Resume,
        &feed_request,
    ) else {
        return denied_response(
            redacted_feed_id(&feed_id),
            redacted_routing_for_id(&feed_id),
            feed_request,
            &context,
        );
    };
    let delivery = match registry
        .coordinator
        .resume(&feed_id, &request.checkpoint, feed_request)
    {
        Ok(delivery) => delivery,
        Err(error) => return coordinator_error_response(error.to_string(), &registered, &context),
    };
    delivery_response(delivery, &context)
}

/// Cancels a feed exactly through the canonical lifecycle transition.
pub async fn cancel(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Json(request): Json<LifecycleRequest>,
) -> Response {
    close_like(state, context, feed_id, request, ChangefeedAction::Cancel).await
}

/// Closes a feed exactly through the canonical lifecycle transition.
pub async fn close(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Json(request): Json<LifecycleRequest>,
) -> Response {
    close_like(state, context, feed_id, request, ChangefeedAction::Close).await
}

async fn delivery(
    state: std::sync::Arc<ServerState>,
    context: RequestContext,
    feed_id: String,
    query: DeliveryQuery,
    action: ChangefeedAction,
) -> Response {
    match delivery_result(&state, &context, &feed_id, &query, action) {
        Ok(delivery) => delivery_response(delivery, &context),
        Err(response) => response,
    }
}

#[allow(clippy::result_large_err)] // Axum Response is returned immediately by the route handler.
fn delivery_result(
    state: &std::sync::Arc<ServerState>,
    context: &RequestContext,
    feed_id: &str,
    query: &DeliveryQuery,
    action: ChangefeedAction,
) -> std::result::Result<FeedDelivery, Response> {
    let request_id = non_empty(context, &query.request_id, "request_id")?;
    if query.max_events == 0 || query.deadline_epoch == 0 {
        return Err(error_response(
            ServerError::BadRequest("max_events and deadline_epoch are required".into()),
            context,
        ));
    }
    let registry = state.changefeed_registry.lock().map_err(|_| {
        error_response(
            ServerError::Internal("changefeed registry lock poisoned".into()),
            context,
        )
    })?;
    let feed_request = feed_request(
        context,
        &operation_id(action_name(action), request_id),
        request_id,
    )?;
    let registered = authorized_existing(state, context, &registry, feed_id, action, &feed_request)
        .ok_or_else(|| {
            denied_response(
                redacted_feed_id(feed_id),
                redacted_routing_for_id(feed_id),
                feed_request.clone(),
                context,
            )
        })?;
    match action {
        ChangefeedAction::Poll => {
            registry
                .coordinator
                .poll(feed_id, query.max_events, feed_request)
        }
        ChangefeedAction::Stream => {
            registry
                .coordinator
                .stream(feed_id, query.max_events, feed_request)
        }
        _ => unreachable!("delivery helper accepts only poll or stream"),
    }
    .map_err(|error| coordinator_error_response(error.to_string(), &registered, context))
}

async fn close_like(
    state: std::sync::Arc<ServerState>,
    context: RequestContext,
    feed_id: String,
    request: LifecycleRequest,
    action: ChangefeedAction,
) -> Response {
    let request_id = match non_empty(&context, &request.request_id, "request_id") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let mut registry = match state.changefeed_registry.lock() {
        Ok(registry) => registry,
        Err(_) => {
            return error_response(
                ServerError::Internal("changefeed registry lock poisoned".into()),
                &context,
            )
        }
    };
    let feed_request = match feed_request(
        &context,
        &operation_id(action_name(action), request_id),
        request_id,
    ) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let Some(registered) =
        authorized_existing(&state, &context, &registry, &feed_id, action, &feed_request)
    else {
        return denied_response(
            redacted_feed_id(&feed_id),
            redacted_routing_for_id(&feed_id),
            feed_request,
            &context,
        );
    };
    let outcome = match action {
        ChangefeedAction::Cancel => registry.coordinator.cancel(&feed_id, feed_request),
        ChangefeedAction::Close => registry.coordinator.close(&feed_id, feed_request),
        _ => unreachable!("close helper accepts only cancel or close"),
    };
    match outcome {
        Ok(outcome) => outcome_response(outcome, &context),
        Err(error) => coordinator_error_response(error.to_string(), &registered, &context),
    }
}

fn authorized_existing(
    state: &std::sync::Arc<ServerState>,
    context: &RequestContext,
    registry: &ChangefeedRegistry,
    feed_id: &str,
    action: ChangefeedAction,
    _request: &FeedRequest,
) -> Option<RegisteredFeed> {
    let subject = state
        .auth
        .authenticated_subject(context.actor.as_deref())
        .ok()?;
    let registered = registry.existing(feed_id)?;
    if registered.authorization.subject != subject
        || !authorize_changefeed(
            context,
            &registered.authorization,
            ChangefeedAccessRequest {
                action,
                tenant: registered.authorization.tenant.clone(),
                range_id: registered.feed.range.range_id.clone(),
            },
        )
        .permits()
    {
        return None;
    }
    Some(registered)
}

#[allow(clippy::result_large_err)] // Axum Response is returned immediately by the route handler.
fn authenticated_subject(
    state: &ServerState,
    context: &RequestContext,
) -> std::result::Result<AuthenticatedSubject, Response> {
    state
        .auth
        .authenticated_subject(context.actor.as_deref())
        .map_err(|_| {
            error_response(
                ServerError::Unauthorized("changefeed actor is not authenticated".into()),
                context,
            )
        })
}

#[allow(clippy::result_large_err)] // Axum Response is returned immediately by the route handler.
fn create_target(
    context: &RequestContext,
    request: &CreateChangefeedRequest,
) -> std::result::Result<ChangefeedTarget, Response> {
    match (&request.table, &request.range_id) {
        (Some(table), None) if !table.trim().is_empty() => Ok(ChangefeedTarget {
            table: Some(table.trim().to_owned()),
            range_id: "table-target".to_owned(),
        }),
        (None, Some(range_id)) if !range_id.trim().is_empty() => Ok(ChangefeedTarget {
            table: None,
            range_id: range_id.trim().to_owned(),
        }),
        _ => Err(error_response(
            ServerError::BadRequest("exactly one of table or range_id is required".into()),
            context,
        )),
    }
}

#[derive(Clone)]
struct ChangefeedTarget {
    table: Option<String>,
    range_id: String,
}

#[allow(clippy::result_large_err)] // Axum Response is returned immediately by the route handler.
fn resolve_feed(
    state: &ServerState,
    context: &RequestContext,
    target: &ChangefeedTarget,
    retention: RetentionWindow,
) -> std::result::Result<(FeedIdentity, RoutingOutcome), Response> {
    let snapshot = state
        .cluster_status_snapshot()
        .map_err(|error| error_response(error, context))?;
    let mut matches = snapshot
        .placement
        .placements
        .iter()
        .filter_map(|placement| {
            if target
                .table
                .as_deref()
                .is_some_and(|table| placement.table_ref.as_str() != table)
            {
                return None;
            }
            placement
                .ranges
                .iter()
                .find(|range| target.table.is_some() || range.range_id.as_str() == target.range_id)
                .map(|range| (placement, range))
        })
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(error_response(
            ServerError::CapabilityUnavailable(
                "changefeed target does not resolve to one committed range".into(),
            ),
            context,
        ));
    }
    let (placement_metadata, logical_range) = matches.pop().expect("one match checked");
    let cluster_id = snapshot
        .identity
        .cluster_id
        .as_ref()
        .map_or("unconfigured", |cluster| cluster.as_str());
    let range = RangeIdentity::new(
        cluster_id,
        placement_metadata.table_id,
        logical_range.range_id.clone(),
        None,
        None,
        u64::from(placement_metadata.schema_version),
        placement_metadata.update_epoch,
    );
    let owner = logical_range
        .target_node_ids
        .first()
        .cloned()
        .unwrap_or_else(|| snapshot.identity.node_id.clone());
    let placement = Placement::new(
        owner,
        logical_range.target_node_ids.clone(),
        PlacementRole::Owner,
        if snapshot.degraded {
            PlacementReadiness::Unavailable
        } else {
            PlacementReadiness::Ready
        },
        placement_metadata.update_epoch,
    );
    let routing = RoutingOutcome::new(
        if snapshot.degraded {
            RoutingOutcomeKind::Blocked
        } else {
            RoutingOutcomeKind::SingleRange
        },
        Some(range.clone()),
        placement_metadata.update_epoch,
        if snapshot.degraded {
            "placement_unavailable"
        } else {
            "placement_ready"
        },
    );
    let feed = FeedIdentity::new(
        format!("changefeed-{}", range.range_id.as_str()),
        range,
        placement_metadata.update_epoch,
        placement,
        OrderingScope::Range,
        retention,
        OperationState::Accepted,
    )
    .map_err(|error| error_response(ServerError::BadRequest(error.to_string()), context))?;
    Ok((feed, routing))
}

#[allow(clippy::result_large_err)] // Axum Response is returned immediately by the route handler.
fn validate_actor(context: &RequestContext, actor: &str) -> std::result::Result<(), Response> {
    let expected = context.actor.as_deref().unwrap_or("anonymous");
    if actor == expected {
        Ok(())
    } else {
        Err(error_response(
            ServerError::Unauthorized("changefeed actor differs from authenticated actor".into()),
            context,
        ))
    }
}

fn unsupported_change_kind_response(
    kinds: &[ChangeOperationType],
    feed: FeedIdentity,
    routing: RoutingOutcome,
    request: FeedRequest,
    context: &RequestContext,
) -> Option<Response> {
    if !kinds
        .iter()
        .any(|kind| matches!(kind, ChangeOperationType::Schema))
    {
        return None;
    }
    let outcome = ChangefeedOutcome::new(
        feed,
        request.operation_id.clone(),
        request.request_id.clone(),
        OperationState::TerminalFailure,
        Some(FailureClass::InvalidRequest),
        Some("change_kind_unsupported".to_owned()),
        RoutingOutcome::new(
            RoutingOutcomeKind::Unsupported,
            routing.range_identity,
            routing.metadata_version,
            "change_kind_unsupported",
        ),
        false,
        alopex_cluster::IdempotencyResult {
            operation_id: request.operation_id,
            request_id: request.request_id,
            first_outcome: "change_kind_unsupported".to_owned(),
            state: OperationState::TerminalFailure,
            duplicate_count: 0,
        },
        alopex_cluster::ChangefeedResult::Feed,
    )
    .ok()?;
    Some(outcome_response(outcome, context))
}

fn invalid_checkpoint_response(
    registered: &RegisteredFeed,
    request: FeedRequest,
    context: &RequestContext,
) -> Response {
    let outcome = ChangefeedOutcome::new(
        registered.feed.clone(),
        request.operation_id.clone(),
        request.request_id.clone(),
        OperationState::TerminalFailure,
        Some(FailureClass::InvalidRequest),
        Some("invalid_checkpoint".to_owned()),
        registered.routing.clone(),
        false,
        alopex_cluster::IdempotencyResult {
            operation_id: request.operation_id,
            request_id: request.request_id,
            first_outcome: "invalid_checkpoint".to_owned(),
            state: OperationState::TerminalFailure,
            duplicate_count: 0,
        },
        alopex_cluster::ChangefeedResult::Feed,
    )
    .expect("validated registered feed produces a canonical invalid checkpoint outcome");
    outcome_response(outcome, context)
}

fn denied_response(
    feed: FeedIdentity,
    routing: RoutingOutcome,
    request: FeedRequest,
    context: &RequestContext,
) -> Response {
    match ChangefeedAuthorizationDecision::Denied.denied_outcome(
        feed,
        routing,
        request.operation_id,
        request.request_id,
    ) {
        Ok(outcome) => outcome_response(outcome, context),
        Err(error) => error_response(ServerError::Internal(error.to_string()), context),
    }
}

fn coordinator_error_response(
    message: String,
    registered: &RegisteredFeed,
    context: &RequestContext,
) -> Response {
    error_response(
        ServerError::BadRequest(format!("changefeed {}: {message}", registered.feed.feed_id)),
        context,
    )
}

fn outcome_response(outcome: ChangefeedOutcome, context: &RequestContext) -> Response {
    (
        status_code(&outcome),
        Json(OutcomeResponse {
            outcome,
            correlation_id: context.correlation_id.clone(),
        }),
    )
        .into_response()
}

fn delivery_response(delivery: FeedDelivery, context: &RequestContext) -> Response {
    (
        status_code(&delivery.outcome),
        Json(DeliveryResponse {
            outcome: delivery.outcome,
            events: delivery.events,
            correlation_id: context.correlation_id.clone(),
        }),
    )
        .into_response()
}

fn status_code(outcome: &ChangefeedOutcome) -> StatusCode {
    StatusCode::from_u16(outcome.surface_status().http_status)
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

#[allow(clippy::result_large_err)] // Axum Response is returned immediately by the route handler.
fn non_empty<'a>(
    context: &RequestContext,
    value: &'a str,
    field: &str,
) -> std::result::Result<&'a str, Response> {
    if value.trim().is_empty() {
        Err(error_response(
            ServerError::BadRequest(format!("missing {field}")),
            context,
        ))
    } else {
        Ok(value)
    }
}

#[allow(clippy::result_large_err)] // Axum Response is returned immediately by the route handler.
fn feed_request(
    context: &RequestContext,
    operation_id: &str,
    request_id: &str,
) -> std::result::Result<FeedRequest, Response> {
    FeedRequest::new(operation_id, request_id)
        .map_err(|error| error_response(ServerError::BadRequest(error.to_string()), context))
}

fn operation_id(action: &str, request_id: &str) -> String {
    format!("changefeed-{action}-{request_id}")
}

fn action_name(action: ChangefeedAction) -> &'static str {
    match action {
        ChangefeedAction::Create => "create",
        ChangefeedAction::Subscribe => "subscribe",
        ChangefeedAction::Poll => "poll",
        ChangefeedAction::Stream => "stream",
        ChangefeedAction::Resume => "resume",
        ChangefeedAction::Ack => "ack",
        ChangefeedAction::Cancel => "cancel",
        ChangefeedAction::Close => "close",
        ChangefeedAction::ManageRetention => "retention",
    }
}

fn feed_id_for(request_id: &str) -> String {
    format!("changefeed-{request_id}")
}

fn redacted_feed_id(feed_id: &str) -> FeedIdentity {
    redacted_feed(
        feed_id,
        &ChangefeedTarget {
            table: None,
            range_id: "redacted".to_owned(),
        },
        RetentionWindow::unbounded(),
    )
}

fn redacted_feed(
    feed_id: &str,
    target: &ChangefeedTarget,
    retention: RetentionWindow,
) -> FeedIdentity {
    FeedIdentity::new(
        feed_id,
        RangeIdentity::new("redacted", 0, target.range_id.clone(), None, None, 0, 0),
        0,
        Placement::new(
            "redacted",
            Vec::new(),
            PlacementRole::Owner,
            PlacementReadiness::Unavailable,
            0,
        ),
        OrderingScope::Range,
        retention,
        OperationState::TerminalFailure,
    )
    .expect("fixed redacted feed identity is valid")
}

fn redacted_routing(feed: &FeedIdentity) -> RoutingOutcome {
    RoutingOutcome::new(
        RoutingOutcomeKind::Blocked,
        Some(feed.range.clone()),
        0,
        "changefeed_unauthorized",
    )
}

fn redacted_routing_for_id(feed_id: &str) -> RoutingOutcome {
    redacted_routing(&redacted_feed_id(feed_id))
}

fn changefeed_scope(scope: ChangefeedScopeConfig) -> ChangefeedScope {
    match scope {
        ChangefeedScopeConfig::Read => ChangefeedScope::Read,
        ChangefeedScopeConfig::Ack => ChangefeedScope::Ack,
        ChangefeedScopeConfig::RetentionAdmin => ChangefeedScope::RetentionAdmin,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use alopex_cluster::{
        changefeed::{ChangefeedScope, DurableCapabilityVersion, DurableProfileEvidence},
        AuthenticatedSubject,
    };

    use super::*;

    fn authorization(subject: &str) -> ChangefeedAuthorization {
        ChangefeedAuthorization {
            subject: AuthenticatedSubject::new(subject),
            tenant: "tenant-a".to_string(),
            allowed_ranges: BTreeSet::from(["range-a".into()]),
            allowed_scopes: BTreeSet::from([ChangefeedScope::Read]),
        }
    }

    fn request() -> ChangefeedAccessRequest {
        ChangefeedAccessRequest {
            action: ChangefeedAction::Poll,
            tenant: "tenant-a".to_string(),
            range_id: "range-a".into(),
        }
    }

    #[test]
    fn server_actor_must_match_authorization_subject_before_range_check() {
        let context = RequestContext {
            correlation_id: "correlation-a".to_string(),
            actor: Some("dev".to_string()),
        };
        assert!(!authorize_changefeed(&context, &authorization("other"), request()).permits());
        assert!(authorize_changefeed(&context, &authorization("dev"), request()).permits());
    }

    #[test]
    fn registry_uses_a_rejected_compiled_preflight_without_local_fallback() {
        let registry = ChangefeedRegistry::from_config(
            DurableProfileAdapter::compiled(),
            &ChangefeedServerConfig::default(),
        )
        .expect("compiled registry");
        assert!(!registry.preflight_is_ready());
    }

    #[test]
    fn config_policy_is_converted_to_the_shared_scope_model() {
        let config = ChangefeedServerConfig {
            authorizations: vec![crate::config::ChangefeedAuthorizationConfig {
                subject: "dev".to_owned(),
                tenant: "tenant-a".to_owned(),
                allowed_ranges: vec!["range-a".to_owned()],
                allowed_scopes: vec![ChangefeedScopeConfig::Read],
            }],
        };
        let registry = ChangefeedRegistry::from_config(
            DurableProfileAdapter::new(DurableProfileEvidence::complete(
                DurableCapabilityVersion::new(0, 7, 0),
            )),
            &config,
        )
        .expect("configured registry");
        assert!(registry.preflight_is_ready());
        assert!(registry
            .authorization_for(&AuthenticatedSubject::new("dev"), "tenant-a")
            .expect("policy")
            .authorize(request())
            .permits());
    }
}
