use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use alopex_cluster::{
    ClusterDiagnostic, ClusterMetricsSource, ClusterMetricsSummary, ClusterMode,
    ClusterStatusSnapshot, ExcludedRoutingTarget, ExcludedTargetReason, LogicalRange, LogicalShard,
    MemberMetricsSummary, MemberStatus, MembershipSource, NodeRole, NodeState,
    PlacementLifecycleState, PlacementMetadata, RawChirpsState, RetryPolicySummary,
    RoutingDecisionKind, RoutingDiagnostics, RoutingTarget, StableDiagnosticCode,
};
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyDictMethods, PyList, PyListMethods};

use crate::error;
use crate::types::results::json_value_to_py;

/// Convert a canonical changefeed outcome without redefining its result
/// schema for Python.  The JSON shape is shared with the other public
/// surfaces and remains the source of the exception `status` attribute.
pub(crate) fn changefeed_outcome_to_py(
    py: Python<'_>,
    outcome: &alopex_cluster::ChangefeedOutcome,
) -> PyResult<Py<PyDict>> {
    let document =
        serde_json::to_value(outcome).map_err(|source| error::to_py_err(source.to_string()))?;
    let value = json_value_to_py(py, &document)?;
    value
        .into_bound(py)
        .cast_into::<PyDict>()
        .map(Bound::unbind)
        .map_err(|_| error::to_py_err("changefeed outcome must serialize to a Python dict"))
}

/// Convert a poll, stream, or resume delivery while retaining both the
/// lifecycle outcome and every event envelope.
pub(crate) fn changefeed_delivery_to_py(
    py: Python<'_>,
    delivery: &alopex_cluster::changefeed::FeedDelivery,
) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("outcome", changefeed_outcome_to_py(py, &delivery.outcome)?)?;
    let events = PyList::empty(py);
    for event in &delivery.events {
        let document =
            serde_json::to_value(event).map_err(|source| error::to_py_err(source.to_string()))?;
        events.append(json_value_to_py(py, &document)?)?;
    }
    result.set_item("events", events)?;
    Ok(result.unbind())
}

/// Return the Python result for a successful canonical outcome or raise the
/// stable changefeed exception while preserving the complete status mapping.
pub(crate) fn changefeed_outcome_or_error(
    py: Python<'_>,
    outcome: &alopex_cluster::ChangefeedOutcome,
) -> PyResult<Py<PyDict>> {
    let status = changefeed_outcome_to_py(py, outcome)?;
    if let Some(code) = outcome.surface_status().python_error_code {
        let message = outcome
            .reason_code
            .as_deref()
            .unwrap_or("changefeed operation failed");
        let py_err = error::with_code(error::to_py_err(message), code);
        py_err.value(py).setattr("status", status.clone_ref(py))?;
        if let Some(failure) = status.bind(py).get_item("failure_class")? {
            py_err.value(py).setattr("failure_class", failure)?;
        } else {
            py_err.value(py).setattr("failure_class", py.None())?;
        }
        return Err(py_err);
    }
    Ok(status)
}

#[derive(Default)]
struct PyChangefeedState {
    latest_status: Option<serde_json::Value>,
    buffered_events: VecDeque<serde_json::Value>,
    generated_request_count: u64,
}

/// Synchronous, embedded-only Changefeed facade.
///
/// It delegates every lifecycle transition to the common embedded handle;
/// there is no endpoint parameter, remote client, or local-WAL fallback.
#[pyclass(name = "Changefeed")]
pub(crate) struct PyChangefeed {
    handle: Arc<alopex_embedded::Changefeed>,
    feed_id: String,
    state: Mutex<PyChangefeedState>,
}

impl PyChangefeed {
    pub(crate) fn from_created(
        handle: alopex_embedded::Changefeed,
        outcome: &alopex_cluster::ChangefeedOutcome,
    ) -> PyResult<Self> {
        let latest_status =
            serde_json::to_value(outcome).map_err(|source| error::to_py_err(source.to_string()))?;
        Ok(Self {
            handle: Arc::new(handle),
            feed_id: outcome.feed.feed_id.clone(),
            state: Mutex::new(PyChangefeedState {
                latest_status: Some(latest_status),
                ..PyChangefeedState::default()
            }),
        })
    }

    fn request(
        &self,
        action: &str,
        request_id: &str,
    ) -> PyResult<alopex_cluster::changefeed::FeedRequest> {
        alopex_cluster::changefeed::FeedRequest::new(
            format!("changefeed-{action}-{request_id}"),
            request_id,
        )
        .map_err(|source| error::to_py_err(source.to_string()))
    }

    fn generated_request_id(&self, action: &str) -> PyResult<String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| error::to_py_err("changefeed state lock poisoned"))?;
        state.generated_request_count = state.generated_request_count.saturating_add(1);
        Ok(format!(
            "{action}-{}-{}",
            self.feed_id, state.generated_request_count
        ))
    }

    fn store_outcome(&self, outcome: &alopex_cluster::ChangefeedOutcome) -> PyResult<()> {
        let latest_status =
            serde_json::to_value(outcome).map_err(|source| error::to_py_err(source.to_string()))?;
        self.state
            .lock()
            .map_err(|_| error::to_py_err("changefeed state lock poisoned"))?
            .latest_status = Some(latest_status);
        Ok(())
    }

    fn store_delivery(
        &self,
        delivery: &alopex_cluster::changefeed::FeedDelivery,
        buffer_events: bool,
    ) -> PyResult<()> {
        self.store_outcome(&delivery.outcome)?;
        if buffer_events {
            let mut state = self
                .state
                .lock()
                .map_err(|_| error::to_py_err("changefeed state lock poisoned"))?;
            for event in &delivery.events {
                state.buffered_events.push_back(
                    serde_json::to_value(event)
                        .map_err(|source| error::to_py_err(source.to_string()))?,
                );
            }
        }
        Ok(())
    }

    fn outcome_result(
        &self,
        py: Python<'_>,
        outcome: alopex_cluster::ChangefeedOutcome,
    ) -> PyResult<Py<PyDict>> {
        self.store_outcome(&outcome)?;
        changefeed_outcome_or_error(py, &outcome)
    }

    fn delivery_result(
        &self,
        py: Python<'_>,
        delivery: alopex_cluster::changefeed::FeedDelivery,
        buffer_events: bool,
    ) -> PyResult<Py<PyDict>> {
        self.store_delivery(&delivery, buffer_events)?;
        changefeed_outcome_or_error(py, &delivery.outcome)?;
        changefeed_delivery_to_py(py, &delivery)
    }

    fn next_event(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(event) = self
            .state
            .lock()
            .map_err(|_| error::to_py_err("changefeed state lock poisoned"))?
            .buffered_events
            .pop_front()
        {
            return json_value_to_py(py, &event);
        }

        let request_id = self.generated_request_id("iterator")?;
        let delivery = self
            .handle
            .stream(1, self.request("stream", &request_id)?)
            .map_err(error::embedded_err)?;
        self.store_delivery(&delivery, true)?;
        changefeed_outcome_or_error(py, &delivery.outcome)?;
        let event = self
            .state
            .lock()
            .map_err(|_| error::to_py_err("changefeed state lock poisoned"))?
            .buffered_events
            .pop_front()
            .ok_or_else(|| PyStopIteration::new_err(()))?;
        json_value_to_py(py, &event)
    }
}

#[pymethods]
impl PyChangefeed {
    #[getter]
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let status = self
            .state
            .lock()
            .map_err(|_| error::to_py_err("changefeed state lock poisoned"))?
            .latest_status
            .clone()
            .ok_or_else(|| error::to_py_err("changefeed has no lifecycle status"))?;
        json_value_to_py(py, &status)?
            .into_bound(py)
            .cast_into::<PyDict>()
            .map(Bound::unbind)
            .map_err(|_| error::to_py_err("changefeed status must be a Python dict"))
    }

    fn subscribe(
        &self,
        py: Python<'_>,
        expected_generation: u64,
        expected_epoch: u64,
        request_id: &str,
    ) -> PyResult<Py<PyDict>> {
        let outcome = self
            .handle
            .subscribe(
                expected_generation,
                expected_epoch,
                self.request("subscribe", request_id)?,
            )
            .map_err(error::embedded_err)?;
        self.outcome_result(py, outcome)
    }

    fn poll(&self, py: Python<'_>, max_events: usize, request_id: &str) -> PyResult<Py<PyDict>> {
        let delivery = self
            .handle
            .poll(max_events, self.request("poll", request_id)?)
            .map_err(error::embedded_err)?;
        self.delivery_result(py, delivery, false)
    }

    fn stream(&self, py: Python<'_>, max_events: usize, request_id: &str) -> PyResult<Py<PyDict>> {
        let delivery = self
            .handle
            .stream(max_events, self.request("stream", request_id)?)
            .map_err(error::embedded_err)?;
        self.delivery_result(py, delivery, true)
    }

    fn ack(
        &self,
        py: Python<'_>,
        ack_id: &str,
        checkpoint: &str,
        request_id: &str,
    ) -> PyResult<Py<PyDict>> {
        let outcome = self
            .handle
            .ack(ack_id, checkpoint, self.request("ack", request_id)?)
            .map_err(error::embedded_err)?;
        self.outcome_result(py, outcome)
    }

    fn resume(&self, py: Python<'_>, checkpoint: &str, request_id: &str) -> PyResult<Py<PyDict>> {
        let delivery = self
            .handle
            .resume(checkpoint, self.request("resume", request_id)?)
            .map_err(error::embedded_err)?;
        self.delivery_result(py, delivery, true)
    }

    fn cancel(&self, py: Python<'_>, request_id: &str) -> PyResult<Py<PyDict>> {
        let outcome = self
            .handle
            .cancel(self.request("cancel", request_id)?)
            .map_err(error::embedded_err)?;
        self.outcome_result(py, outcome)
    }

    fn close(&self, py: Python<'_>, request_id: &str) -> PyResult<Py<PyDict>> {
        let outcome = self
            .handle
            .close(self.request("close", request_id)?)
            .map_err(error::embedded_err)?;
        self.outcome_result(py, outcome)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.next_event(py)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        let request_id = self.generated_request_id("context-close")?;
        let outcome = self
            .handle
            .close(self.request("close", &request_id)?)
            .map_err(error::embedded_err)?;
        self.store_outcome(&outcome)?;
        // `close` has the canonical cancelled terminal state.  The explicit
        // lifecycle method exposes that state as `changefeed_cancelled`, but
        // context-manager cleanup must not mask an exception from its body or
        // turn a normal scope exit into a second exception.
        if outcome.surface_status().python_error_code != Some("changefeed_cancelled") {
            changefeed_outcome_or_error(py, &outcome)?;
        }
        Ok(false)
    }
}

pub(crate) fn cluster_status_to_py(
    py: Python<'_>,
    snapshot: &ClusterStatusSnapshot,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("schema_version", snapshot.schema_version)?;
    dict.set_item("mode", cluster_mode_name(snapshot.mode))?;
    dict.set_item("identity", cluster_identity_to_py(py, snapshot)?)?;
    dict.set_item("membership", membership_to_py(py, snapshot)?)?;
    dict.set_item("placement", placement_to_py(py, snapshot)?)?;
    dict.set_item(
        "routing_capabilities",
        routing_capabilities_to_py(py, snapshot)?,
    )?;
    dict.set_item(
        "metrics_summary",
        metrics_summary_to_py(py, &snapshot.metrics_summary)?,
    )?;
    dict.set_item(
        "cluster_control",
        embedded_cluster_control_to_py(py, snapshot)?,
    )?;
    dict.set_item("degraded", snapshot.degraded)?;
    dict.set_item("diagnostics", diagnostics_to_py(py, &snapshot.diagnostics)?)?;
    Ok(dict.unbind())
}

/// Keep the embedded diagnostic surface compatible with the HTTP status
/// control fields without creating a client or representing embedded storage
/// as multi-node control. Embedded databases always report unavailable control
/// for the default single-node snapshot.
fn embedded_cluster_control_to_py(
    py: Python<'_>,
    snapshot: &ClusterStatusSnapshot,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("available", false)?;
    dict.set_item("mode", cluster_mode_name(snapshot.mode))?;
    let reason = match snapshot.mode {
        ClusterMode::SingleNode => "single_node_mode",
        ClusterMode::ClusterAware => "embedded_local_diagnostics_only",
    };
    dict.set_item("reason", reason)?;
    dict.set_item("missing_prerequisites", PyList::empty(py))?;
    Ok(dict.unbind())
}

pub(crate) fn routing_diagnostics_to_py(
    py: Python<'_>,
    diagnostics: &RoutingDiagnostics,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("schema_version", diagnostics.schema_version)?;
    dict.set_item("update_epoch", diagnostics.update_epoch)?;
    dict.set_item("decision", routing_decision_name(diagnostics.decision))?;
    dict.set_item("reason", stable_diagnostic_code_name(diagnostics.reason))?;
    // v0.9 clients consume a stable machine-readable reason_code; retain the
    // legacy `reason` key for Python compatibility.
    dict.set_item(
        "reason_code",
        stable_diagnostic_code_name(diagnostics.reason),
    )?;
    dict.set_item("plan_id", diagnostics.plan_id.as_str())?;
    dict.set_item("roles", node_roles_to_py(py, &diagnostics.roles)?)?;
    dict.set_item("targets", routing_targets_to_py(py, &diagnostics.targets)?)?;
    dict.set_item(
        "excluded_targets",
        excluded_targets_to_py(py, &diagnostics.excluded_targets)?,
    )?;
    match &diagnostics.retry_summary {
        Some(summary) => dict.set_item("retry_summary", retry_summary_to_py(py, summary)?)?,
        None => dict.set_item("retry_summary", py.None())?,
    }
    Ok(dict.unbind())
}

fn cluster_identity_to_py(
    py: Python<'_>,
    snapshot: &ClusterStatusSnapshot,
) -> PyResult<Py<PyDict>> {
    let identity = &snapshot.identity;
    let dict = PyDict::new(py);
    dict.set_item("node_id", identity.node_id.as_str())?;
    set_optional_str(
        py,
        &dict,
        "cluster_id",
        identity.cluster_id.as_ref().map(|id| id.as_str()),
    )?;
    set_optional_str(
        py,
        &dict,
        "advertised_endpoint",
        identity
            .advertised_endpoint
            .as_ref()
            .map(|endpoint| endpoint.as_str()),
    )?;
    dict.set_item("role", node_role_name(identity.role))?;
    dict.set_item("lifecycle_state", node_state_name(identity.lifecycle_state))?;
    dict.set_item("metadata_schema_version", identity.metadata_schema_version)?;
    dict.set_item("update_epoch", identity.update_epoch)?;
    Ok(dict.unbind())
}

fn membership_to_py(py: Python<'_>, snapshot: &ClusterStatusSnapshot) -> PyResult<Py<PyDict>> {
    let membership = &snapshot.membership;
    let dict = PyDict::new(py);
    dict.set_item("schema_version", membership.schema_version)?;
    dict.set_item("update_epoch", membership.update_epoch)?;
    dict.set_item("source", membership_source_name(membership.source))?;
    dict.set_item("members", member_statuses_to_py(py, &membership.members)?)?;
    Ok(dict.unbind())
}

fn member_statuses_to_py(py: Python<'_>, members: &[MemberStatus]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for member in members {
        let dict = PyDict::new(py);
        let identity = PyDict::new(py);
        identity.set_item("node_id", member.identity.node_id.as_str())?;
        set_optional_str(
            py,
            &identity,
            "cluster_id",
            member.identity.cluster_id.as_ref().map(|id| id.as_str()),
        )?;
        set_optional_str(
            py,
            &identity,
            "advertised_endpoint",
            member
                .identity
                .advertised_endpoint
                .as_ref()
                .map(|endpoint| endpoint.as_str()),
        )?;
        identity.set_item("role", node_role_name(member.identity.role))?;
        dict.set_item("identity", identity)?;
        match member.raw_reachability_state {
            Some(state) => dict.set_item("raw_reachability_state", raw_chirps_state_name(state))?,
            None => dict.set_item("raw_reachability_state", py.None())?,
        }
        dict.set_item("derived_state", node_state_name(member.derived_state))?;
        set_optional_str(
            py,
            &dict,
            "transition_reason",
            member.transition_reason.as_deref(),
        )?;
        list.append(dict)?;
    }
    Ok(list.unbind())
}

fn placement_to_py(py: Python<'_>, snapshot: &ClusterStatusSnapshot) -> PyResult<Py<PyDict>> {
    let placement = &snapshot.placement;
    let dict = PyDict::new(py);
    dict.set_item("schema_version", placement.schema_version)?;
    dict.set_item("update_epoch", placement.update_epoch)?;
    let placements = PyList::empty(py);
    for item in &placement.placements {
        placements.append(placement_metadata_to_py(py, item)?)?;
    }
    dict.set_item("placements", placements)?;
    Ok(dict.unbind())
}

fn placement_metadata_to_py(py: Python<'_>, placement: &PlacementMetadata) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("schema_version", placement.schema_version)?;
    dict.set_item("update_epoch", placement.update_epoch)?;
    dict.set_item("table_ref", placement.table_ref.as_str())?;
    dict.set_item("table_id", placement.table_id)?;
    dict.set_item(
        "lifecycle_state",
        placement_lifecycle_state_name(placement.lifecycle_state),
    )?;
    dict.set_item("shards", logical_shards_to_py(py, &placement.shards)?)?;
    dict.set_item("ranges", logical_ranges_to_py(py, &placement.ranges)?)?;
    dict.set_item("targets", routing_targets_to_py(py, &placement.targets)?)?;
    Ok(dict.unbind())
}

fn routing_capabilities_to_py(
    py: Python<'_>,
    snapshot: &ClusterStatusSnapshot,
) -> PyResult<Py<PyDict>> {
    let capabilities = &snapshot.routing_capabilities;
    let dict = PyDict::new(py);
    dict.set_item("local_only", capabilities.local_only)?;
    dict.set_item(
        "future_distributed_execution_required",
        capabilities.future_distributed_execution_required,
    )?;
    dict.set_item(
        "scatter_gather_simulated",
        capabilities.scatter_gather_simulated,
    )?;
    Ok(dict.unbind())
}

fn metrics_summary_to_py(py: Python<'_>, summary: &ClusterMetricsSummary) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("source", cluster_metrics_source_name(summary.source))?;
    dict.set_item("members", member_metrics_to_py(py, &summary.members)?)?;
    Ok(dict.unbind())
}

fn member_metrics_to_py(py: Python<'_>, members: &[MemberMetricsSummary]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for member in members {
        let dict = PyDict::new(py);
        dict.set_item("node_id", member.node_id.as_str())?;
        dict.set_item("source", cluster_metrics_source_name(member.source))?;
        set_optional_f64(py, &dict, "latency_ms", member.latency_ms)?;
        set_optional_f64(py, &dict, "load", member.load)?;
        set_optional_u64(py, &dict, "error_count", member.error_count)?;
        list.append(dict)?;
    }
    Ok(list.unbind())
}

fn diagnostics_to_py(py: Python<'_>, diagnostics: &[ClusterDiagnostic]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for diagnostic in diagnostics {
        let dict = PyDict::new(py);
        dict.set_item("code", stable_diagnostic_code_name(diagnostic.code))?;
        dict.set_item("message", &diagnostic.message)?;
        dict.set_item("remediation", &diagnostic.remediation)?;
        dict.set_item("degraded", diagnostic.degraded)?;
        list.append(dict)?;
    }
    Ok(list.unbind())
}

fn node_roles_to_py(py: Python<'_>, roles: &[NodeRole]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for role in roles {
        list.append(node_role_name(*role))?;
    }
    Ok(list.unbind())
}

fn routing_targets_to_py(py: Python<'_>, targets: &[RoutingTarget]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for target in targets {
        list.append(routing_target_to_py(py, target)?)?;
    }
    Ok(list.unbind())
}

fn routing_target_to_py(py: Python<'_>, target: &RoutingTarget) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("node_id", target.node_id.as_str())?;
    dict.set_item("table_ref", target.table_ref.as_str())?;
    dict.set_item("table_id", target.table_id)?;
    set_optional_str(
        py,
        &dict,
        "shard_id",
        target.shard_id.as_ref().map(|id| id.as_str()),
    )?;
    set_optional_str(
        py,
        &dict,
        "range_id",
        target.range_id.as_ref().map(|id| id.as_str()),
    )?;
    Ok(dict.unbind())
}

fn excluded_targets_to_py(
    py: Python<'_>,
    excluded_targets: &[ExcludedRoutingTarget],
) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for excluded in excluded_targets {
        let dict = PyDict::new(py);
        dict.set_item("target", routing_target_to_py(py, &excluded.target)?)?;
        dict.set_item("reason", excluded_target_reason_name(excluded.reason))?;
        list.append(dict)?;
    }
    Ok(list.unbind())
}

fn retry_summary_to_py(py: Python<'_>, summary: &RetryPolicySummary) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("max_attempts", summary.max_attempts)?;
    dict.set_item("max_backoff_ms", summary.max_backoff_ms)?;
    set_optional_str(
        py,
        &dict,
        "cancellation_state",
        summary.cancellation_state.as_deref(),
    )?;
    Ok(dict.unbind())
}

fn logical_shards_to_py(py: Python<'_>, shards: &[LogicalShard]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for shard in shards {
        let dict = PyDict::new(py);
        dict.set_item("shard_id", shard.shard_id.as_str())?;
        dict.set_item(
            "target_node_ids",
            node_ids_to_py(py, &shard.target_node_ids)?,
        )?;
        list.append(dict)?;
    }
    Ok(list.unbind())
}

fn logical_ranges_to_py(py: Python<'_>, ranges: &[LogicalRange]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for range in ranges {
        let dict = PyDict::new(py);
        dict.set_item("range_id", range.range_id.as_str())?;
        set_optional_str(py, &dict, "start_bound", range.start_bound.as_deref())?;
        set_optional_str(py, &dict, "end_bound", range.end_bound.as_deref())?;
        dict.set_item(
            "target_node_ids",
            node_ids_to_py(py, &range.target_node_ids)?,
        )?;
        list.append(dict)?;
    }
    Ok(list.unbind())
}

fn node_ids_to_py(py: Python<'_>, node_ids: &[alopex_cluster::NodeId]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for node_id in node_ids {
        list.append(node_id.as_str())?;
    }
    Ok(list.unbind())
}

fn set_optional_str(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    key: &str,
    value: Option<&str>,
) -> PyResult<()> {
    match value {
        Some(value) => dict.set_item(key, value),
        None => dict.set_item(key, py.None()),
    }
}

fn set_optional_f64(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    key: &str,
    value: Option<f64>,
) -> PyResult<()> {
    match value {
        Some(value) => dict.set_item(key, value),
        None => dict.set_item(key, py.None()),
    }
}

fn set_optional_u64(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    key: &str,
    value: Option<u64>,
) -> PyResult<()> {
    match value {
        Some(value) => dict.set_item(key, value),
        None => dict.set_item(key, py.None()),
    }
}

fn cluster_mode_name(value: ClusterMode) -> &'static str {
    match value {
        ClusterMode::SingleNode => "single_node",
        ClusterMode::ClusterAware => "cluster_aware",
    }
}

fn cluster_metrics_source_name(value: ClusterMetricsSource) -> &'static str {
    match value {
        ClusterMetricsSource::LiveStatusSurface => "live_status_surface",
        ClusterMetricsSource::SimulatedHarness => "simulated_harness",
    }
}

fn node_role_name(value: NodeRole) -> &'static str {
    match value {
        NodeRole::Gateway => "gateway",
        NodeRole::Worker => "worker",
    }
}

fn node_state_name(value: NodeState) -> &'static str {
    match value {
        NodeState::Unconfigured => "unconfigured",
        NodeState::Joining => "joining",
        NodeState::Active => "active",
        NodeState::Leaving => "leaving",
        NodeState::Unreachable => "unreachable",
    }
}

fn membership_source_name(value: MembershipSource) -> &'static str {
    match value {
        MembershipSource::LocalDefault => "local_default",
        MembershipSource::Persisted => "persisted",
        MembershipSource::Chirps => "chirps",
        MembershipSource::Simulated => "simulated",
    }
}

fn raw_chirps_state_name(value: RawChirpsState) -> &'static str {
    match value {
        RawChirpsState::Alive => "alive",
        RawChirpsState::Suspect => "suspect",
        RawChirpsState::Dead => "dead",
    }
}

fn placement_lifecycle_state_name(value: PlacementLifecycleState) -> &'static str {
    match value {
        PlacementLifecycleState::Active => "active",
        PlacementLifecycleState::Stale => "stale",
        PlacementLifecycleState::Tombstoned => "tombstoned",
    }
}

fn routing_decision_name(value: RoutingDecisionKind) -> &'static str {
    match value {
        RoutingDecisionKind::LocalOnly => "local_only",
        RoutingDecisionKind::FutureDistributedExecutionRequired => {
            "future_distributed_execution_required"
        }
        RoutingDecisionKind::ScatterGatherSimulated => "scatter_gather_simulated",
    }
}

fn stable_diagnostic_code_name(value: StableDiagnosticCode) -> &'static str {
    match value {
        StableDiagnosticCode::SingleResolvedTarget => "single_resolved_target",
        StableDiagnosticCode::PlacementAbsent => "placement_absent",
        StableDiagnosticCode::PlacementStale => "placement_stale",
        StableDiagnosticCode::PlacementTargetIneligible => "placement_target_ineligible",
        StableDiagnosticCode::MixedPlacementFallback => "mixed_placement_fallback",
        StableDiagnosticCode::FutureDistributedExecutionRequired => {
            "future_distributed_execution_required"
        }
        StableDiagnosticCode::ScatterGatherSimulated => "scatter_gather_simulated",
        StableDiagnosticCode::ChirpsUnavailable => "chirps_unavailable",
        StableDiagnosticCode::MembershipSourceUnavailable => "membership_source_unavailable",
        StableDiagnosticCode::InvalidNodeIdentity => "invalid_node_identity",
        StableDiagnosticCode::ConflictingNodeIdentity => "conflicting_node_identity",
        StableDiagnosticCode::PlanningInputUnavailable => "planning_input_unavailable",
        StableDiagnosticCode::RetryScheduled => "retry_scheduled",
        StableDiagnosticCode::RetryExhausted => "retry_exhausted",
        StableDiagnosticCode::SubRequestCancelled => "sub_request_cancelled",
        StableDiagnosticCode::DuplicateRequest => "duplicate_request",
        StableDiagnosticCode::RequestConflict => "request_conflict",
        StableDiagnosticCode::StaleMetadataVersion => "stale_metadata_version",
        StableDiagnosticCode::Unauthorized => "unauthorized",
        StableDiagnosticCode::InvalidRange => "invalid_range",
        StableDiagnosticCode::RangeCoverageIncomplete => "range_coverage_incomplete",
        StableDiagnosticCode::SchemaOwnerRequired => "schema_owner_required",
        StableDiagnosticCode::OperationPending => "operation_pending",
        StableDiagnosticCode::MetadataCommitted => "metadata_committed",
    }
}

fn excluded_target_reason_name(value: ExcludedTargetReason) -> &'static str {
    match value {
        ExcludedTargetReason::MemberInactive => "member_inactive",
        ExcludedTargetReason::MemberUnknown => "member_unknown",
        ExcludedTargetReason::RoleNotWorker => "role_not_worker",
        ExcludedTargetReason::PlacementStale => "placement_stale",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use alopex_cluster::{
        changefeed::{
            ChangefeedAuthorization, ChangefeedScope, CheckpointCursor, DurableCapabilityVersion,
            DurableProfileAdapter, DurableProfileEvidence, FeedRequest,
        },
        AuthenticatedSubject, Checkpoint, FeedIdentity, OperationState, OrderingScope, Placement,
        PlacementReadiness, PlacementRole, RangeIdentity, RetentionWindow, RoutingOutcome,
        RoutingOutcomeKind, StableDiagnosticCode,
    };
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
    use pyo3::{Py, Python};

    use super::{stable_diagnostic_code_name, PyChangefeed};

    fn ready_handle() -> (
        alopex_embedded::Changefeed,
        alopex_cluster::ChangefeedOutcome,
    ) {
        let range = RangeIdentity::new("cluster-a", 7, "range-a", None, None, 4, 9);
        let feed = FeedIdentity::new(
            "feed-a",
            range.clone(),
            3,
            Placement::new(
                "node-a",
                vec![],
                PlacementRole::Owner,
                PlacementReadiness::Ready,
                11,
            ),
            OrderingScope::Range,
            RetentionWindow::unbounded(),
            OperationState::Accepted,
        )
        .unwrap();
        let routing = RoutingOutcome::new(
            RoutingOutcomeKind::SingleRange,
            Some(range.clone()),
            12,
            "placement_ready",
        );
        let authorization = ChangefeedAuthorization {
            subject: AuthenticatedSubject::new("python-test"),
            tenant: "tenant-a".to_owned(),
            allowed_ranges: BTreeSet::from([range.range_id]),
            allowed_scopes: BTreeSet::from([ChangefeedScope::Read, ChangefeedScope::Ack]),
        };
        let created = alopex_embedded::Database::new()
            .create_changefeed(
                DurableProfileAdapter::new(DurableProfileEvidence::complete(
                    DurableCapabilityVersion::new(0, 7, 0),
                )),
                authorization,
                "tenant-a",
                feed,
                routing,
                FeedRequest::new("create", "create-request").unwrap(),
            )
            .unwrap();
        (created.changefeed.unwrap(), created.outcome)
    }

    fn checkpoint() -> String {
        CheckpointCursor::new(Checkpoint::new("feed-a", "range-a", 3, 0, 0, 9, None).unwrap())
            .unwrap()
            .encode()
            .unwrap()
    }

    #[test]
    fn phase_one_metadata_diagnostic_codes_have_stable_python_names() {
        let cases = [
            (StableDiagnosticCode::DuplicateRequest, "duplicate_request"),
            (StableDiagnosticCode::RequestConflict, "request_conflict"),
            (
                StableDiagnosticCode::StaleMetadataVersion,
                "stale_metadata_version",
            ),
            (StableDiagnosticCode::Unauthorized, "unauthorized"),
            (StableDiagnosticCode::InvalidRange, "invalid_range"),
            (
                StableDiagnosticCode::RangeCoverageIncomplete,
                "range_coverage_incomplete",
            ),
            (
                StableDiagnosticCode::SchemaOwnerRequired,
                "schema_owner_required",
            ),
            (StableDiagnosticCode::OperationPending, "operation_pending"),
            (
                StableDiagnosticCode::MetadataCommitted,
                "metadata_committed",
            ),
        ];

        for (code, expected) in cases {
            assert_eq!(stable_diagnostic_code_name(code), expected);
        }
    }

    #[test]
    fn sync_changefeed_preserves_lifecycle_status_exception_and_context_contract() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let (handle, outcome) = ready_handle();
            let handle = Py::new(py, PyChangefeed::from_created(handle, &outcome).unwrap())
                .expect("Python Changefeed");
            let handle = handle.bind(py);

            let status_value = handle.getattr("status").unwrap();
            let status = status_value.cast::<PyDict>().unwrap();
            assert_eq!(
                status
                    .get_item("operation_state")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "accepted"
            );

            let subscribed_value = handle
                .call_method1("subscribe", (3_u64, 9_u64, "subscribe-request"))
                .unwrap();
            let subscribed = subscribed_value.cast::<PyDict>().unwrap();
            assert_eq!(
                subscribed
                    .get_item("operation_state")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "running"
            );

            for action in ["poll", "stream"] {
                let delivery_value = handle
                    .call_method1(action, (10_usize, format!("{action}-request")))
                    .unwrap();
                let delivery = delivery_value.cast::<PyDict>().unwrap();
                assert!(delivery.get_item("outcome").unwrap().is_some());
                assert!(delivery.get_item("events").unwrap().is_some());
            }

            let acknowledged_value = handle
                .call_method1("ack", ("ack-a", checkpoint(), "ack-request"))
                .unwrap();
            let acknowledged = acknowledged_value.cast::<PyDict>().unwrap();
            let result_value = acknowledged.get_item("result").unwrap().unwrap();
            let result_envelope = result_value.cast::<PyDict>().unwrap();
            assert_eq!(
                result_envelope
                    .get_item("result_type")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "ack"
            );
            let result_value = result_envelope.get_item("result").unwrap().unwrap();
            let result = result_value.cast::<PyDict>().unwrap();
            assert_eq!(
                result
                    .get_item("ack_state")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "accepted"
            );

            let invalid = handle
                .call_method1("ack", ("ack-b", "not-a-checkpoint", "invalid-request"))
                .expect_err("invalid checkpoint must be a classified Python error");
            assert_eq!(
                invalid
                    .value(py)
                    .getattr("code")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "changefeed_invalid_request"
            );
            let failure_status_value = invalid.value(py).getattr("status").unwrap();
            let failure_status = failure_status_value.cast::<PyDict>().unwrap();
            assert_eq!(
                failure_status
                    .get_item("reason_code")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "invalid_checkpoint"
            );

            handle
                .call_method1("resume", (checkpoint(), "resume-request"))
                .unwrap();
            let exhausted = handle
                .call_method0("__next__")
                .expect_err("empty stream must end through iterator protocol");
            assert!(exhausted.is_instance_of::<pyo3::exceptions::PyStopIteration>(py));

            handle.call_method0("__enter__").unwrap();
            let cancelled = handle
                .call_method1("cancel", ("cancel-request",))
                .expect_err("explicit cancellation must remain classified");
            assert_eq!(
                cancelled
                    .value(py)
                    .getattr("code")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "changefeed_cancelled"
            );
            handle
                .call_method1("__exit__", (py.None(), py.None(), py.None()))
                .unwrap();
            let closed_value = handle.getattr("status").unwrap();
            let closed = closed_value.cast::<PyDict>().unwrap();
            assert_eq!(
                closed
                    .get_item("operation_state")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "cancelled"
            );
        });
    }
}
