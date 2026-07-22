use alopex_cluster::{
    ClusterDiagnostic, ClusterMetricsSource, ClusterMetricsSummary, ClusterMode,
    ClusterStatusSnapshot, ExcludedRoutingTarget, ExcludedTargetReason, LogicalRange, LogicalShard,
    MemberMetricsSummary, MemberStatus, MembershipSource, NodeRole, NodeState,
    PlacementLifecycleState, PlacementMetadata, RawChirpsState, RetryPolicySummary,
    RoutingDecisionKind, RoutingDiagnostics, RoutingTarget, StableDiagnosticCode,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyDictMethods, PyList, PyListMethods};

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
    use super::stable_diagnostic_code_name;
    use alopex_cluster::StableDiagnosticCode;

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
}
