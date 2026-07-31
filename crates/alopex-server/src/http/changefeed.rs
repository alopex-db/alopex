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

/// Executes the public create lifecycle against the shared coordinator.
///
/// HTTP and gRPC call this façade directly.  Keeping authorization, target
/// resolution, Durable preflight, and canonical outcome construction here
/// prevents either transport from acquiring a private, divergent lifecycle.
pub(crate) fn create_changefeed(
    state: &ServerState,
    context: &RequestContext,
    request: CreateChangefeedRequest,
) -> Result<ChangefeedOutcome> {
    let request_id = non_empty(&request.request_id, "request_id")?;
    validate_actor(context, &request.actor)?;
    let target = create_target(&request)?;
    let subject = authenticated_subject(state, context)?;
    let operation = operation_id("create", request_id);

    let mut registry_guard = registry(state)?;
    let authorization = registry_guard.authorization_for(&subject, &request.tenant);
    let synthetic = redacted_feed(&feed_id_for(request_id), &target, request.retention.clone());
    let synthetic_routing = redacted_routing(&synthetic);
    let create_request = feed_request(&operation, request_id)?;
    let Some(authorization) = authorization else {
        return denied_outcome(synthetic, synthetic_routing, create_request);
    };
    let resolved = if target.table.is_some() {
        Some(resolve_feed(state, &target, request.retention.clone())?)
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
    if !authorize_changefeed(context, &authorization, access).permits() {
        return denied_outcome(synthetic, synthetic_routing, create_request);
    }
    if let Some(outcome) = unsupported_change_kind_outcome(
        &request.change_kinds,
        candidate_feed.clone(),
        candidate_routing.clone(),
        create_request.clone(),
    ) {
        return Ok(outcome);
    }
    if !registry_guard.preflight_is_ready() {
        return registry_guard
            .coordinator
            .create(candidate_feed, candidate_routing, create_request)
            .map_err(|error| ServerError::Internal(error.to_string()));
    }
    drop(registry_guard);

    let (feed, routing) = match resolved {
        Some(metadata) => metadata,
        None => resolve_feed(state, &target, request.retention)?,
    };
    let mut registry_guard = registry(state)?;
    let create_request = feed_request(&operation, request_id)?;
    if !authorize_changefeed(
        context,
        &authorization,
        ChangefeedAccessRequest {
            action: ChangefeedAction::Create,
            tenant: request.tenant.clone(),
            range_id: feed.range.range_id.clone(),
        },
    )
    .permits()
    {
        return denied_outcome(synthetic, synthetic_routing, create_request);
    }
    let outcome = registry_guard
        .coordinator
        .create(feed.clone(), routing.clone(), create_request)
        .map_err(|error| ServerError::Internal(error.to_string()))?;
    if outcome.failure_class.is_none() {
        registry_guard.register(feed, routing, authorization);
    }
    Ok(outcome)
}

/// Executes subscribe after generation/epoch and read-scope validation.
pub(crate) fn subscribe_changefeed(
    state: &ServerState,
    context: &RequestContext,
    feed_id: &str,
    request: SubscribeChangefeedRequest,
) -> Result<ChangefeedOutcome> {
    let request_id = non_empty(&request.request_id, "request_id")?;
    let mut registry = registry(state)?;
    let feed_request = feed_request(&operation_id("subscribe", request_id), request_id)?;
    let Some(registered) = authorized_existing(
        state,
        context,
        &registry,
        feed_id,
        ChangefeedAction::Subscribe,
        &feed_request,
    ) else {
        return denied_outcome(
            redacted_feed_id(feed_id),
            redacted_routing_for_id(feed_id),
            feed_request,
        );
    };
    registry
        .coordinator
        .subscribe(
            feed_id,
            request.expected_generation,
            request.expected_epoch,
            feed_request,
        )
        .map_err(|error| coordinator_error(error.to_string(), &registered))
}

/// Executes a bounded poll or stream delivery with the shared coordinator.
pub(crate) fn deliver_changefeed(
    state: &ServerState,
    context: &RequestContext,
    feed_id: &str,
    query: DeliveryQuery,
    action: ChangefeedAction,
) -> Result<FeedDelivery> {
    let request_id = non_empty(&query.request_id, "request_id")?;
    if query.max_events == 0 || query.deadline_epoch == 0 {
        return Err(ServerError::BadRequest(
            "max_events and deadline_epoch are required".into(),
        ));
    }
    let registry = registry(state)?;
    let feed_request = feed_request(&operation_id(action_name(action), request_id), request_id)?;
    let Some(registered) =
        authorized_existing(state, context, &registry, feed_id, action, &feed_request)
    else {
        return denied_delivery(
            redacted_feed_id(feed_id),
            redacted_routing_for_id(feed_id),
            feed_request,
        );
    };
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
    .map_err(|error| coordinator_error(error.to_string(), &registered))
}

/// Validates checkpoint binding and accepts an acknowledgement through the
/// coordinator. The coordinator deliberately reports `accepted`, never a
/// fabricated durable checkpoint.
pub(crate) fn ack_changefeed(
    state: &ServerState,
    context: &RequestContext,
    feed_id: &str,
    request: AckChangefeedRequest,
) -> Result<ChangefeedOutcome> {
    let request_id = non_empty(&request.request_id, "request_id")?;
    if request.ack_id.trim().is_empty() || request.checkpoint.trim().is_empty() {
        return Err(ServerError::BadRequest(
            "missing ack_id or checkpoint".into(),
        ));
    }
    let registry = registry(state)?;
    let feed_request = feed_request(&operation_id("ack", request_id), request_id)?;
    let Some(registered) = authorized_existing(
        state,
        context,
        &registry,
        feed_id,
        ChangefeedAction::Ack,
        &feed_request,
    ) else {
        return denied_outcome(
            redacted_feed_id(feed_id),
            redacted_routing_for_id(feed_id),
            feed_request,
        );
    };
    if alopex_cluster::changefeed::CheckpointCursor::decode_for(
        &request.checkpoint,
        &registered.feed.feed_id,
        &registered.feed.range.range_id,
    )
    .is_err()
    {
        return Ok(invalid_checkpoint_outcome(&registered, feed_request));
    }
    registry
        .coordinator
        .ack(feed_id, request.ack_id, feed_request)
        .map_err(|error| coordinator_error(error.to_string(), &registered))
}

/// Resumes strictly after the supplied checkpoint.
pub(crate) fn resume_changefeed(
    state: &ServerState,
    context: &RequestContext,
    feed_id: &str,
    request: ResumeChangefeedRequest,
) -> Result<FeedDelivery> {
    let request_id = non_empty(&request.request_id, "request_id")?;
    let mut registry = registry(state)?;
    let feed_request = feed_request(&operation_id("resume", request_id), request_id)?;
    let Some(registered) = authorized_existing(
        state,
        context,
        &registry,
        feed_id,
        ChangefeedAction::Resume,
        &feed_request,
    ) else {
        return denied_delivery(
            redacted_feed_id(feed_id),
            redacted_routing_for_id(feed_id),
            feed_request,
        );
    };
    registry
        .coordinator
        .resume(feed_id, &request.checkpoint, feed_request)
        .map_err(|error| coordinator_error(error.to_string(), &registered))
}

/// Executes cancel or close through the canonical lifecycle transition.
pub(crate) fn close_changefeed(
    state: &ServerState,
    context: &RequestContext,
    feed_id: &str,
    request: LifecycleRequest,
    action: ChangefeedAction,
) -> Result<ChangefeedOutcome> {
    let request_id = non_empty(&request.request_id, "request_id")?;
    let mut registry = registry(state)?;
    let feed_request = feed_request(&operation_id(action_name(action), request_id), request_id)?;
    let Some(registered) =
        authorized_existing(state, context, &registry, feed_id, action, &feed_request)
    else {
        return denied_outcome(
            redacted_feed_id(feed_id),
            redacted_routing_for_id(feed_id),
            feed_request,
        );
    };
    let outcome = match action {
        ChangefeedAction::Cancel => registry.coordinator.cancel(feed_id, feed_request),
        ChangefeedAction::Close => registry.coordinator.close(feed_id, feed_request),
        _ => unreachable!("close helper accepts only cancel or close"),
    };
    outcome.map_err(|error| coordinator_error(error.to_string(), &registered))
}

/// Creates a feed after actor, tenant, range, scope, Durable, and target
/// metadata checks. Rejected checks stay canonical outcomes.
pub async fn create(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<CreateChangefeedRequest>,
) -> Response {
    match create_changefeed(&state, &context, request) {
        Ok(outcome) => outcome_response(outcome, &context),
        Err(error) => error_response(error, &context),
    }
}

/// Subscribes to a feed after generation/epoch and read-scope validation.
pub async fn subscribe(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Json(request): Json<SubscribeChangefeedRequest>,
) -> Response {
    match subscribe_changefeed(&state, &context, &feed_id, request) {
        Ok(outcome) => outcome_response(outcome, &context),
        Err(error) => error_response(error, &context),
    }
}

/// Returns a bounded JSON delivery batch for one authorized feed.
pub async fn poll(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Query(query): Query<DeliveryQuery>,
) -> Response {
    match deliver_changefeed(&state, &context, &feed_id, query, ChangefeedAction::Poll) {
        Ok(delivery) => delivery_response(delivery, &context),
        Err(error) => error_response(error, &context),
    }
}

/// Returns the same canonical delivery contract as poll, encoded as JSONL.
pub async fn stream(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Query(query): Query<DeliveryQuery>,
) -> Response {
    let delivery =
        match deliver_changefeed(&state, &context, &feed_id, query, ChangefeedAction::Stream) {
            Ok(delivery) => delivery,
            Err(error) => return error_response(error, &context),
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
    match ack_changefeed(&state, &context, &feed_id, request) {
        Ok(outcome) => outcome_response(outcome, &context),
        Err(error) => error_response(error, &context),
    }
}

/// Resumes strictly after the supplied checkpoint and returns a JSON batch.
pub async fn resume(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Json(request): Json<ResumeChangefeedRequest>,
) -> Response {
    match resume_changefeed(&state, &context, &feed_id, request) {
        Ok(delivery) => delivery_response(delivery, &context),
        Err(error) => error_response(error, &context),
    }
}

/// Cancels a feed exactly through the canonical lifecycle transition.
pub async fn cancel(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Json(request): Json<LifecycleRequest>,
) -> Response {
    match close_changefeed(
        &state,
        &context,
        &feed_id,
        request,
        ChangefeedAction::Cancel,
    ) {
        Ok(outcome) => outcome_response(outcome, &context),
        Err(error) => error_response(error, &context),
    }
}

/// Closes a feed exactly through the canonical lifecycle transition.
pub async fn close(
    Extension(state): Extension<std::sync::Arc<ServerState>>,
    Extension(context): Extension<RequestContext>,
    Path(feed_id): Path<String>,
    Json(request): Json<LifecycleRequest>,
) -> Response {
    match close_changefeed(&state, &context, &feed_id, request, ChangefeedAction::Close) {
        Ok(outcome) => outcome_response(outcome, &context),
        Err(error) => error_response(error, &context),
    }
}

fn authorized_existing(
    state: &ServerState,
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

fn authenticated_subject(
    state: &ServerState,
    context: &RequestContext,
) -> Result<AuthenticatedSubject> {
    state
        .auth
        .authenticated_subject(context.actor.as_deref())
        .map_err(|_| ServerError::Unauthorized("changefeed actor is not authenticated".into()))
}

fn create_target(request: &CreateChangefeedRequest) -> Result<ChangefeedTarget> {
    match (&request.table, &request.range_id) {
        (Some(table), None) if !table.trim().is_empty() => Ok(ChangefeedTarget {
            table: Some(table.trim().to_owned()),
            range_id: "table-target".to_owned(),
        }),
        (None, Some(range_id)) if !range_id.trim().is_empty() => Ok(ChangefeedTarget {
            table: None,
            range_id: range_id.trim().to_owned(),
        }),
        _ => Err(ServerError::BadRequest(
            "exactly one of table or range_id is required".into(),
        )),
    }
}

#[derive(Clone)]
struct ChangefeedTarget {
    table: Option<String>,
    range_id: String,
}

fn resolve_feed(
    state: &ServerState,
    target: &ChangefeedTarget,
    retention: RetentionWindow,
) -> Result<(FeedIdentity, RoutingOutcome)> {
    let snapshot = state.cluster_status_snapshot()?;
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
        return Err(ServerError::CapabilityUnavailable(
            "changefeed target does not resolve to one committed range".into(),
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
    .map_err(|error| ServerError::BadRequest(error.to_string()))?;
    Ok((feed, routing))
}

fn validate_actor(context: &RequestContext, actor: &str) -> Result<()> {
    let expected = context.actor.as_deref().unwrap_or("anonymous");
    if actor == expected {
        Ok(())
    } else {
        Err(ServerError::Unauthorized(
            "changefeed actor differs from authenticated actor".into(),
        ))
    }
}

fn unsupported_change_kind_outcome(
    kinds: &[ChangeOperationType],
    feed: FeedIdentity,
    routing: RoutingOutcome,
    request: FeedRequest,
) -> Option<ChangefeedOutcome> {
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
    .expect("fixed unsupported outcome is canonical");
    Some(outcome)
}

fn invalid_checkpoint_outcome(
    registered: &RegisteredFeed,
    request: FeedRequest,
) -> ChangefeedOutcome {
    ChangefeedOutcome::new(
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
    .expect("validated registered feed produces a canonical invalid checkpoint outcome")
}

fn denied_outcome(
    feed: FeedIdentity,
    routing: RoutingOutcome,
    request: FeedRequest,
) -> Result<ChangefeedOutcome> {
    ChangefeedAuthorizationDecision::Denied
        .denied_outcome(feed, routing, request.operation_id, request.request_id)
        .map_err(|error| ServerError::Internal(error.to_string()))
}

fn denied_delivery(
    feed: FeedIdentity,
    routing: RoutingOutcome,
    request: FeedRequest,
) -> Result<FeedDelivery> {
    let outcome = denied_outcome(feed, routing, request)?;
    Ok(FeedDelivery {
        outcome,
        events: Vec::new(),
    })
}

fn coordinator_error(message: String, registered: &RegisteredFeed) -> ServerError {
    ServerError::BadRequest(format!("changefeed {}: {message}", registered.feed.feed_id))
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

fn non_empty<'a>(value: &'a str, field: &str) -> Result<&'a str> {
    if value.trim().is_empty() {
        Err(ServerError::BadRequest(format!("missing {field}")))
    } else {
        Ok(value)
    }
}

fn feed_request(operation_id: &str, request_id: &str) -> Result<FeedRequest> {
    FeedRequest::new(operation_id, request_id)
        .map_err(|error| ServerError::BadRequest(error.to_string()))
}

fn registry(state: &ServerState) -> Result<std::sync::MutexGuard<'_, ChangefeedRegistry>> {
    state
        .changefeed_registry
        .lock()
        .map_err(|_| ServerError::Internal("changefeed registry lock poisoned".into()))
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
