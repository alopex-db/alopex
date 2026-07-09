//! Versioned cluster metadata contracts for Alopex DB.
//!
//! This crate intentionally owns only stable data contracts at this layer. It
//! does not perform remote execution, Raft replication, or distributed
//! transactions.

use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, fmt};

/// Current schema version for v0.7 cluster metadata payloads.
pub const CLUSTER_METADATA_SCHEMA_VERSION: u32 = 1;

/// Initial epoch used by metadata snapshots before a persistent source assigns
/// an update epoch.
pub const INITIAL_UPDATE_EPOCH: u64 = 0;

pub type UpdateEpoch = u64;
pub type TableId = u32;

fn default_schema_version() -> u32 {
    CLUSTER_METADATA_SCHEMA_VERSION
}

fn default_update_epoch() -> UpdateEpoch {
    INITIAL_UPDATE_EPOCH
}

macro_rules! string_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }
    };
}

string_id!(NodeId, "Stable application-layer node identifier.");
string_id!(
    ClusterId,
    "Stable cluster identifier shared by configured nodes."
);
string_id!(
    Endpoint,
    "Advertised network endpoint stored as an opaque migration-safe string."
);
string_id!(PlanId, "Stable routing plan identifier.");
string_id!(
    TableRef,
    "Catalog-qualified table reference stored as an opaque SQL-owned string."
);
string_id!(ShardId, "Logical shard identifier.");
string_id!(RangeId, "Logical range identifier.");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Gateway,
    Worker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    Unconfigured,
    Joining,
    Active,
    Leaving,
    Unreachable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipSource {
    LocalDefault,
    Persisted,
    Chirps,
    Simulated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawChirpsState {
    Alive,
    Suspect,
    Dead,
}

impl RawChirpsState {
    pub fn derived_node_state(self) -> NodeState {
        match self {
            Self::Alive => NodeState::Active,
            Self::Suspect | Self::Dead => NodeState::Unreachable,
        }
    }

    pub fn transition_reason(self) -> &'static str {
        match self {
            Self::Alive => "chirps_alive",
            Self::Suspect => "chirps_suspect",
            Self::Dead => "chirps_dead",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterIdentity {
    pub node_id: NodeId,
    #[serde(default)]
    pub cluster_id: Option<ClusterId>,
    #[serde(default)]
    pub advertised_endpoint: Option<Endpoint>,
    pub role: NodeRole,
    pub lifecycle_state: NodeState,
    #[serde(default = "default_schema_version")]
    pub metadata_schema_version: u32,
    #[serde(default = "default_update_epoch")]
    pub update_epoch: UpdateEpoch,
}

impl ClusterIdentity {
    pub fn new(node_id: impl Into<NodeId>, role: NodeRole, lifecycle_state: NodeState) -> Self {
        Self {
            node_id: node_id.into(),
            cluster_id: None,
            advertised_endpoint: None,
            role,
            lifecycle_state,
            metadata_schema_version: CLUSTER_METADATA_SCHEMA_VERSION,
            update_epoch: INITIAL_UPDATE_EPOCH,
        }
    }

    pub fn unconfigured_single_node(node_id: impl Into<NodeId>) -> Self {
        Self::new(node_id, NodeRole::Gateway, NodeState::Unconfigured)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberIdentity {
    pub node_id: NodeId,
    #[serde(default)]
    pub cluster_id: Option<ClusterId>,
    #[serde(default)]
    pub advertised_endpoint: Option<Endpoint>,
    pub role: NodeRole,
}

impl From<&ClusterIdentity> for MemberIdentity {
    fn from(identity: &ClusterIdentity) -> Self {
        Self {
            node_id: identity.node_id.clone(),
            cluster_id: identity.cluster_id.clone(),
            advertised_endpoint: identity.advertised_endpoint.clone(),
            role: identity.role,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipView {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_update_epoch")]
    pub update_epoch: UpdateEpoch,
    pub source: MembershipSource,
    #[serde(default)]
    pub members: Vec<MemberStatus>,
}

impl MembershipView {
    pub fn new(source: MembershipSource, update_epoch: UpdateEpoch) -> Self {
        Self {
            schema_version: CLUSTER_METADATA_SCHEMA_VERSION,
            update_epoch,
            source,
            members: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberStatus {
    pub identity: MemberIdentity,
    #[serde(default)]
    pub raw_reachability_state: Option<RawChirpsState>,
    pub derived_state: NodeState,
    #[serde(default)]
    pub transition_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementLifecycleState {
    Active,
    Stale,
    Tombstoned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementMetadata {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_update_epoch")]
    pub update_epoch: UpdateEpoch,
    pub table_ref: TableRef,
    pub table_id: TableId,
    pub lifecycle_state: PlacementLifecycleState,
    #[serde(default)]
    pub shards: Vec<LogicalShard>,
    #[serde(default)]
    pub ranges: Vec<LogicalRange>,
    #[serde(default)]
    pub targets: Vec<RoutingTarget>,
}

impl PlacementMetadata {
    pub fn new(
        table_ref: impl Into<TableRef>,
        table_id: TableId,
        update_epoch: UpdateEpoch,
    ) -> Self {
        Self {
            schema_version: CLUSTER_METADATA_SCHEMA_VERSION,
            update_epoch,
            table_ref: table_ref.into(),
            table_id,
            lifecycle_state: PlacementLifecycleState::Active,
            shards: Vec::new(),
            ranges: Vec::new(),
            targets: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalShard {
    pub shard_id: ShardId,
    #[serde(default)]
    pub target_node_ids: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalRange {
    pub range_id: RangeId,
    #[serde(default)]
    pub start_bound: Option<String>,
    #[serde(default)]
    pub end_bound: Option<String>,
    #[serde(default)]
    pub target_node_ids: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RoutingTarget {
    pub node_id: NodeId,
    pub table_ref: TableRef,
    pub table_id: TableId,
    #[serde(default)]
    pub shard_id: Option<ShardId>,
    #[serde(default)]
    pub range_id: Option<RangeId>,
}

impl RoutingTarget {
    pub fn table(
        node_id: impl Into<NodeId>,
        table_ref: impl Into<TableRef>,
        table_id: TableId,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            table_ref: table_ref.into(),
            table_id,
            shard_id: None,
            range_id: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutingDecisionKind {
    LocalOnly,
    FutureDistributedExecutionRequired,
    ScatterGatherSimulated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StableDiagnosticCode {
    SingleResolvedTarget,
    PlacementAbsent,
    PlacementStale,
    PlacementTargetIneligible,
    MixedPlacementFallback,
    FutureDistributedExecutionRequired,
    ScatterGatherSimulated,
    ChirpsUnavailable,
    MembershipSourceUnavailable,
    InvalidNodeIdentity,
    ConflictingNodeIdentity,
    PlanningInputUnavailable,
    RetryScheduled,
    SubRequestCancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExcludedTargetReason {
    MemberInactive,
    MemberUnknown,
    RoleNotWorker,
    PlacementStale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExcludedRoutingTarget {
    pub target: RoutingTarget,
    pub reason: ExcludedTargetReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicySummary {
    pub max_attempts: u32,
    pub max_backoff_ms: u64,
    #[serde(default)]
    pub cancellation_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingDiagnostics {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_update_epoch")]
    pub update_epoch: UpdateEpoch,
    pub decision: RoutingDecisionKind,
    pub reason: StableDiagnosticCode,
    pub plan_id: PlanId,
    #[serde(default)]
    pub roles: Vec<NodeRole>,
    #[serde(default)]
    pub targets: Vec<RoutingTarget>,
    #[serde(default)]
    pub excluded_targets: Vec<ExcludedRoutingTarget>,
    #[serde(default)]
    pub retry_summary: Option<RetryPolicySummary>,
}

impl RoutingDiagnostics {
    pub fn new(
        decision: RoutingDecisionKind,
        reason: StableDiagnosticCode,
        plan_id: impl Into<PlanId>,
        update_epoch: UpdateEpoch,
    ) -> Self {
        Self {
            schema_version: CLUSTER_METADATA_SCHEMA_VERSION,
            update_epoch,
            decision,
            reason,
            plan_id: plan_id.into(),
            roles: Vec::new(),
            targets: Vec::new(),
            excluded_targets: Vec::new(),
            retry_summary: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterMode {
    SingleNode,
    ClusterAware,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterMetricsSource {
    LiveStatusSurface,
    SimulatedHarness,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberMetricsSummary {
    pub node_id: NodeId,
    pub source: ClusterMetricsSource,
    #[serde(default)]
    pub latency_ms: Option<f64>,
    #[serde(default)]
    pub load: Option<f64>,
    #[serde(default)]
    pub error_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterMetricsSummary {
    pub source: ClusterMetricsSource,
    #[serde(default)]
    pub members: Vec<MemberMetricsSummary>,
}

impl Default for ClusterMetricsSummary {
    fn default() -> Self {
        Self {
            source: ClusterMetricsSource::LiveStatusSurface,
            members: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingCapabilities {
    pub local_only: bool,
    pub future_distributed_execution_required: bool,
    pub scatter_gather_simulated: bool,
}

impl Default for RoutingCapabilities {
    fn default() -> Self {
        Self {
            local_only: true,
            future_distributed_execution_required: true,
            scatter_gather_simulated: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterDiagnostic {
    pub code: StableDiagnosticCode,
    pub message: String,
    pub remediation: String,
    pub degraded: bool,
}

impl ClusterDiagnostic {
    pub fn new(
        code: StableDiagnosticCode,
        message: impl Into<String>,
        remediation: impl Into<String>,
        degraded: bool,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            remediation: remediation.into(),
            degraded,
        }
    }
}

pub fn chirps_unavailable_diagnostic() -> ClusterDiagnostic {
    unavailable_membership_diagnostic(MembershipSource::Chirps)
}

pub fn node_id_from_chirps_bytes(bytes: &[u8]) -> NodeId {
    NodeId::new(lower_hex(bytes))
}

#[cfg(feature = "chirps")]
pub fn raw_chirps_state_from_status(
    status: &alopex_chirps_gossip_swim::types::Status,
) -> RawChirpsState {
    match status {
        alopex_chirps_gossip_swim::types::Status::Alive => RawChirpsState::Alive,
        alopex_chirps_gossip_swim::types::Status::Suspect => RawChirpsState::Suspect,
        alopex_chirps_gossip_swim::types::Status::Dead => RawChirpsState::Dead,
    }
}

#[cfg(feature = "chirps")]
pub fn member_status_from_chirps_peer(
    peer: &alopex_chirps_gossip_swim::types::Peer,
    cluster_id: Option<ClusterId>,
    role: NodeRole,
) -> MemberStatus {
    let raw_state = raw_chirps_state_from_status(&peer.state.status);

    MemberStatus {
        identity: MemberIdentity {
            node_id: node_id_from_chirps_bytes(peer.node_id.as_bytes()),
            cluster_id,
            advertised_endpoint: Some(Endpoint::new(peer.addr.to_string())),
            role,
        },
        raw_reachability_state: Some(raw_state),
        derived_state: raw_state.derived_node_state(),
        transition_reason: Some(raw_state.transition_reason().to_string()),
    }
}

#[cfg(feature = "chirps")]
pub fn membership_view_from_chirps(
    view: &alopex_chirps_gossip_swim::types::MembershipView,
    cluster_id: Option<ClusterId>,
    default_role: NodeRole,
    update_epoch: UpdateEpoch,
) -> MembershipView {
    let mut members = view
        .peers
        .values()
        .map(|peer| member_status_from_chirps_peer(peer, cluster_id.clone(), default_role))
        .collect::<Vec<_>>();
    members.sort_by(|left, right| left.identity.node_id.cmp(&right.identity.node_id));

    MembershipView {
        schema_version: CLUSTER_METADATA_SCHEMA_VERSION,
        update_epoch,
        source: MembershipSource::Chirps,
        members,
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }

    output
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementView {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_update_epoch")]
    pub update_epoch: UpdateEpoch,
    #[serde(default)]
    pub placements: Vec<PlacementMetadata>,
}

impl PlacementView {
    pub fn new(update_epoch: UpdateEpoch) -> Self {
        Self {
            schema_version: CLUSTER_METADATA_SCHEMA_VERSION,
            update_epoch,
            placements: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogTableRef {
    pub table_ref: TableRef,
    pub table_id: TableId,
}

impl CatalogTableRef {
    pub fn new(table_ref: impl Into<TableRef>, table_id: TableId) -> Self {
        Self {
            table_ref: table_ref.into(),
            table_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogTableSnapshot {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_update_epoch")]
    pub update_epoch: UpdateEpoch,
    #[serde(default)]
    pub tables: Vec<CatalogTableRef>,
}

impl CatalogTableSnapshot {
    pub fn new(update_epoch: UpdateEpoch) -> Self {
        Self {
            schema_version: CLUSTER_METADATA_SCHEMA_VERSION,
            update_epoch,
            tables: Vec::new(),
        }
    }

    pub fn from_tables(update_epoch: UpdateEpoch, tables: Vec<CatalogTableRef>) -> Self {
        Self {
            tables,
            ..Self::new(update_epoch)
        }
    }

    pub fn table_id_for(&self, table_ref: &TableRef) -> Option<TableId> {
        self.tables
            .iter()
            .find(|table| &table.table_ref == table_ref)
            .map(|table| table.table_id)
    }

    pub fn contains_current(&self, table_ref: &TableRef, table_id: TableId) -> bool {
        self.table_id_for(table_ref) == Some(table_id)
    }
}

/// SQL-planner-style access class for extracted table references.
///
/// This mirrors the shape of alopex-sql routing input without depending on the
/// SQL crate or parsing SQL in the cluster layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryTableReferenceAccess {
    Read,
    Write,
    Create,
    Drop,
    Metadata,
}

/// Source location for a table reference extracted before cluster routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryTableReferenceSource {
    TopLevelPlanTableName,
    LogicalPlanScan,
    LogicalPlanMutationTarget,
    LogicalPlanDdlTarget,
    LogicalPlanIndexTarget,
    TypedExprSubquery,
}

/// A table reference already extracted by the SQL planning boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryTableReference {
    pub table_ref: TableRef,
    pub access: QueryTableReferenceAccess,
    pub source: QueryTableReferenceSource,
}

impl QueryTableReference {
    pub fn new(
        table_ref: impl Into<TableRef>,
        access: QueryTableReferenceAccess,
        source: QueryTableReferenceSource,
    ) -> Self {
        Self {
            table_ref: table_ref.into(),
            access,
            source,
        }
    }

    pub fn read(table_ref: impl Into<TableRef>, source: QueryTableReferenceSource) -> Self {
        Self::new(table_ref, QueryTableReferenceAccess::Read, source)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRoutingRequest {
    pub plan_id: PlanId,
    pub catalog_snapshot: CatalogTableSnapshot,
    #[serde(default)]
    pub table_references: Vec<QueryTableReference>,
}

impl QueryRoutingRequest {
    pub fn new(
        plan_id: impl Into<PlanId>,
        catalog_snapshot: CatalogTableSnapshot,
        table_references: Vec<QueryTableReference>,
    ) -> Self {
        Self {
            plan_id: plan_id.into(),
            catalog_snapshot,
            table_references,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementCatalog {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_update_epoch")]
    pub update_epoch: UpdateEpoch,
    #[serde(default)]
    placements: Vec<PlacementMetadata>,
}

impl Default for PlacementCatalog {
    fn default() -> Self {
        Self::new(INITIAL_UPDATE_EPOCH)
    }
}

impl PlacementCatalog {
    pub fn new(update_epoch: UpdateEpoch) -> Self {
        Self {
            schema_version: CLUSTER_METADATA_SCHEMA_VERSION,
            update_epoch,
            placements: Vec::new(),
        }
    }

    pub fn from_placements(
        update_epoch: UpdateEpoch,
        mut placements: Vec<PlacementMetadata>,
    ) -> Self {
        let catalog_epoch = placements
            .iter()
            .map(|placement| placement.update_epoch)
            .max()
            .unwrap_or(update_epoch)
            .max(update_epoch);
        for placement in &mut placements {
            placement.schema_version = CLUSTER_METADATA_SCHEMA_VERSION;
        }

        Self {
            schema_version: CLUSTER_METADATA_SCHEMA_VERSION,
            update_epoch: catalog_epoch,
            placements,
        }
    }

    pub fn from_view(view: PlacementView) -> Self {
        Self::from_placements(view.update_epoch, view.placements)
    }

    pub fn placement_view(&self) -> PlacementView {
        PlacementView {
            schema_version: self.schema_version,
            update_epoch: self.update_epoch,
            placements: self.placements.clone(),
        }
    }

    pub fn placements(&self) -> &[PlacementMetadata] {
        &self.placements
    }

    pub fn active_placement_for(
        &self,
        table_ref: &TableRef,
        current_table_id: TableId,
    ) -> Option<&PlacementMetadata> {
        self.placements.iter().find(|placement| {
            &placement.table_ref == table_ref
                && placement.table_id == current_table_id
                && placement.lifecycle_state == PlacementLifecycleState::Active
        })
    }

    pub fn active_targets_for(
        &self,
        table_ref: &TableRef,
        current_table_id: TableId,
    ) -> Vec<RoutingTarget> {
        self.active_placement_for(table_ref, current_table_id)
            .map(|placement| {
                placement
                    .targets
                    .iter()
                    .filter(|target| {
                        &target.table_ref == table_ref && target.table_id == current_table_id
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn apply_table_lifecycle_effect(&mut self, effect: TableLifecycleEffect) -> PlacementView {
        match effect {
            TableLifecycleEffect::Created {
                table_ref,
                table_id,
            } => {
                self.mark_placements(
                    |placement| {
                        placement.table_ref == table_ref
                            && placement.table_id != table_id
                            && placement.lifecycle_state != PlacementLifecycleState::Tombstoned
                    },
                    PlacementLifecycleState::Stale,
                );
            }
            TableLifecycleEffect::Dropped {
                table_ref,
                table_id,
            } => {
                self.mark_placements(
                    |placement| placement.table_ref == table_ref && placement.table_id == table_id,
                    PlacementLifecycleState::Tombstoned,
                );
            }
            TableLifecycleEffect::SchemaChanged {
                table_ref,
                table_id,
            } => {
                self.mark_placements(
                    |placement| placement.table_ref == table_ref && placement.table_id == table_id,
                    PlacementLifecycleState::Stale,
                );
            }
        }

        self.placement_view()
    }

    pub fn reconcile(&mut self, catalog_snapshot: &CatalogTableSnapshot) -> PlacementView {
        let mut changed_indexes = Vec::new();

        for (index, placement) in self.placements.iter_mut().enumerate() {
            let reconciled_state = match catalog_snapshot.table_id_for(&placement.table_ref) {
                None => PlacementLifecycleState::Tombstoned,
                Some(current_table_id) if current_table_id != placement.table_id => {
                    PlacementLifecycleState::Stale
                }
                Some(_) => placement.lifecycle_state,
            };

            if placement.lifecycle_state != reconciled_state {
                placement.lifecycle_state = reconciled_state;
                changed_indexes.push(index);
            }
        }

        self.bump_changed_placements(changed_indexes);
        self.placement_view()
    }

    fn next_epoch(&mut self) -> UpdateEpoch {
        self.update_epoch = self.update_epoch.saturating_add(1);
        self.update_epoch
    }

    fn mark_placements(
        &mut self,
        predicate: impl Fn(&PlacementMetadata) -> bool,
        lifecycle_state: PlacementLifecycleState,
    ) {
        let mut changed_indexes = Vec::new();

        for (index, placement) in self.placements.iter_mut().enumerate() {
            if predicate(placement) && placement.lifecycle_state != lifecycle_state {
                placement.lifecycle_state = lifecycle_state;
                changed_indexes.push(index);
            }
        }

        self.bump_changed_placements(changed_indexes);
    }

    fn bump_changed_placements(&mut self, changed_indexes: Vec<usize>) {
        if changed_indexes.is_empty() {
            return;
        }

        let epoch = self.next_epoch();
        for index in changed_indexes {
            if let Some(placement) = self.placements.get_mut(index) {
                placement.schema_version = CLUSTER_METADATA_SCHEMA_VERSION;
                placement.update_epoch = epoch;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TargetEligibility {
    role: Option<NodeRole>,
    excluded_reason: Option<ExcludedTargetReason>,
}

#[derive(Debug)]
pub struct QueryRouter<'a> {
    placement_catalog: &'a PlacementCatalog,
    membership: &'a MembershipView,
}

impl<'a> QueryRouter<'a> {
    pub fn new(placement_catalog: &'a PlacementCatalog, membership: &'a MembershipView) -> Self {
        Self {
            placement_catalog,
            membership,
        }
    }

    pub fn route(&self, request: QueryRoutingRequest) -> RoutingDiagnostics {
        self.route_live(request)
    }

    pub fn route_live(&self, request: QueryRoutingRequest) -> RoutingDiagnostics {
        self.route_request(request, false)
    }

    pub fn simulate(&self, request: QueryRoutingRequest) -> RoutingDiagnostics {
        self.route_request(request, true)
    }

    fn route_request(&self, request: QueryRoutingRequest, simulated: bool) -> RoutingDiagnostics {
        let update_epoch = self
            .placement_catalog
            .update_epoch
            .max(self.membership.update_epoch)
            .max(request.catalog_snapshot.update_epoch);
        let mut targets = Vec::new();
        let mut excluded_targets = Vec::new();
        let mut roles = Vec::new();
        let mut fallback_reasons = Vec::new();

        for table_reference in unique_query_table_references(request.table_references) {
            let Some(current_table_id) = request
                .catalog_snapshot
                .table_id_for(&table_reference.table_ref)
            else {
                push_unique_diagnostic_reason(
                    &mut fallback_reasons,
                    StableDiagnosticCode::PlacementAbsent,
                );
                continue;
            };

            match self
                .placement_catalog
                .active_placement_for(&table_reference.table_ref, current_table_id)
            {
                Some(placement) => {
                    let mut eligible_target_found = false;
                    for target in placement.targets.iter().filter(|target| {
                        target.table_ref == table_reference.table_ref
                            && target.table_id == current_table_id
                    }) {
                        let eligibility = self.target_eligibility(&target.node_id);
                        if let Some(role) = eligibility.role {
                            push_unique_role(&mut roles, role);
                        }

                        if let Some(reason) = eligibility.excluded_reason {
                            push_unique_excluded_target(
                                &mut excluded_targets,
                                target.clone(),
                                reason,
                            );
                        } else {
                            eligible_target_found = true;
                            push_unique_target(&mut targets, target.clone());
                        }
                    }

                    if !eligible_target_found {
                        push_unique_diagnostic_reason(
                            &mut fallback_reasons,
                            StableDiagnosticCode::PlacementTargetIneligible,
                        );
                    }
                }
                None => {
                    let stale_placements = stale_placements_for(
                        self.placement_catalog,
                        &table_reference.table_ref,
                        current_table_id,
                    );
                    if stale_placements.is_empty() {
                        push_unique_diagnostic_reason(
                            &mut fallback_reasons,
                            StableDiagnosticCode::PlacementAbsent,
                        );
                    } else {
                        for placement in stale_placements {
                            for target in &placement.targets {
                                if target.table_ref == table_reference.table_ref {
                                    push_unique_excluded_target(
                                        &mut excluded_targets,
                                        target.clone(),
                                        ExcludedTargetReason::PlacementStale,
                                    );
                                }
                            }
                        }
                        push_unique_diagnostic_reason(
                            &mut fallback_reasons,
                            StableDiagnosticCode::PlacementStale,
                        );
                    }
                }
            }
        }

        sort_targets(&mut targets);
        sort_excluded_targets(&mut excluded_targets);

        let unique_target_nodes = targets
            .iter()
            .map(|target| target.node_id.clone())
            .collect::<BTreeSet<_>>();
        let reason = routing_reason(unique_target_nodes.len(), &fallback_reasons);
        let decision = match (simulated, unique_target_nodes.len()) {
            (true, count) if count > 1 => RoutingDecisionKind::ScatterGatherSimulated,
            (false, count) if count > 1 => RoutingDecisionKind::FutureDistributedExecutionRequired,
            _ => RoutingDecisionKind::LocalOnly,
        };
        let reason = match decision {
            RoutingDecisionKind::FutureDistributedExecutionRequired => {
                StableDiagnosticCode::FutureDistributedExecutionRequired
            }
            RoutingDecisionKind::ScatterGatherSimulated => {
                StableDiagnosticCode::ScatterGatherSimulated
            }
            RoutingDecisionKind::LocalOnly => reason,
        };

        let mut diagnostics =
            RoutingDiagnostics::new(decision, reason, request.plan_id, update_epoch);
        diagnostics.roles = roles;
        diagnostics.targets = targets;
        diagnostics.excluded_targets = excluded_targets;
        diagnostics
    }

    fn target_eligibility(&self, node_id: &NodeId) -> TargetEligibility {
        let Some(member) = self
            .membership
            .members
            .iter()
            .find(|member| &member.identity.node_id == node_id)
        else {
            return TargetEligibility {
                role: None,
                excluded_reason: Some(ExcludedTargetReason::MemberUnknown),
            };
        };

        if member.derived_state != NodeState::Active {
            return TargetEligibility {
                role: Some(member.identity.role),
                excluded_reason: Some(ExcludedTargetReason::MemberInactive),
            };
        }

        if member.identity.role != NodeRole::Worker {
            return TargetEligibility {
                role: Some(member.identity.role),
                excluded_reason: Some(ExcludedTargetReason::RoleNotWorker),
            };
        }

        TargetEligibility {
            role: Some(member.identity.role),
            excluded_reason: None,
        }
    }
}

fn unique_query_table_references(
    table_references: Vec<QueryTableReference>,
) -> Vec<QueryTableReference> {
    let mut seen = BTreeSet::new();
    let mut unique = Vec::new();

    for table_reference in table_references {
        let key = (
            table_reference.table_ref.clone(),
            table_reference.access,
            table_reference.source,
        );
        if seen.insert(key) {
            unique.push(table_reference);
        }
    }

    unique
}

fn stale_placements_for<'a>(
    catalog: &'a PlacementCatalog,
    table_ref: &TableRef,
    current_table_id: TableId,
) -> Vec<&'a PlacementMetadata> {
    catalog
        .placements()
        .iter()
        .filter(|placement| {
            &placement.table_ref == table_ref
                && (placement.table_id != current_table_id
                    || placement.lifecycle_state != PlacementLifecycleState::Active)
        })
        .collect()
}

fn push_unique_role(roles: &mut Vec<NodeRole>, role: NodeRole) {
    if !roles.contains(&role) {
        roles.push(role);
    }
}

fn push_unique_target(targets: &mut Vec<RoutingTarget>, target: RoutingTarget) {
    if !targets.contains(&target) {
        targets.push(target);
    }
}

fn push_unique_excluded_target(
    excluded_targets: &mut Vec<ExcludedRoutingTarget>,
    target: RoutingTarget,
    reason: ExcludedTargetReason,
) {
    let excluded = ExcludedRoutingTarget { target, reason };
    if !excluded_targets.contains(&excluded) {
        excluded_targets.push(excluded);
    }
}

fn push_unique_diagnostic_reason(
    reasons: &mut Vec<StableDiagnosticCode>,
    reason: StableDiagnosticCode,
) {
    if !reasons.contains(&reason) {
        reasons.push(reason);
    }
}

fn routing_reason(
    target_node_count: usize,
    fallback_reasons: &[StableDiagnosticCode],
) -> StableDiagnosticCode {
    if fallback_reasons.contains(&StableDiagnosticCode::PlacementTargetIneligible) {
        return StableDiagnosticCode::PlacementTargetIneligible;
    }
    if fallback_reasons.contains(&StableDiagnosticCode::PlacementStale) {
        return StableDiagnosticCode::PlacementStale;
    }
    if fallback_reasons.contains(&StableDiagnosticCode::PlacementAbsent) {
        return if target_node_count > 0 {
            StableDiagnosticCode::MixedPlacementFallback
        } else {
            StableDiagnosticCode::PlacementAbsent
        };
    }

    match target_node_count {
        0 => StableDiagnosticCode::PlanningInputUnavailable,
        1 => StableDiagnosticCode::SingleResolvedTarget,
        _ => StableDiagnosticCode::FutureDistributedExecutionRequired,
    }
}

fn sort_targets(targets: &mut [RoutingTarget]) {
    targets.sort_by(|left, right| {
        (
            &left.node_id,
            &left.table_ref,
            left.table_id,
            &left.shard_id,
            &left.range_id,
        )
            .cmp(&(
                &right.node_id,
                &right.table_ref,
                right.table_id,
                &right.shard_id,
                &right.range_id,
            ))
    });
}

fn sort_excluded_targets(excluded_targets: &mut [ExcludedRoutingTarget]) {
    excluded_targets.sort_by(|left, right| {
        (
            &left.target.node_id,
            &left.target.table_ref,
            left.target.table_id,
            &left.target.shard_id,
            &left.target.range_id,
            excluded_reason_order(left.reason),
        )
            .cmp(&(
                &right.target.node_id,
                &right.target.table_ref,
                right.target.table_id,
                &right.target.shard_id,
                &right.target.range_id,
                excluded_reason_order(right.reason),
            ))
    });
}

fn excluded_reason_order(reason: ExcludedTargetReason) -> u8 {
    match reason {
        ExcludedTargetReason::MemberInactive => 0,
        ExcludedTargetReason::MemberUnknown => 1,
        ExcludedTargetReason::RoleNotWorker => 2,
        ExcludedTargetReason::PlacementStale => 3,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableLifecycleEffect {
    Created {
        table_ref: TableRef,
        table_id: TableId,
    },
    Dropped {
        table_ref: TableRef,
        table_id: TableId,
    },
    SchemaChanged {
        table_ref: TableRef,
        table_id: TableId,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatusSnapshot {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub mode: ClusterMode,
    pub identity: ClusterIdentity,
    pub membership: MembershipView,
    pub placement: PlacementView,
    pub routing_capabilities: RoutingCapabilities,
    pub metrics_summary: ClusterMetricsSummary,
    pub degraded: bool,
    #[serde(default)]
    pub diagnostics: Vec<ClusterDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterError {
    pub code: StableDiagnosticCode,
    pub message: String,
    pub remediation: String,
}

impl ClusterError {
    pub fn new(
        code: StableDiagnosticCode,
        message: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            remediation: remediation.into(),
        }
    }
}

impl fmt::Display for ClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}: {} remediation: {}",
            self.code, self.message, self.remediation
        )
    }
}

impl Error for ClusterError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterManagerConfig {
    pub mode: ClusterMode,
    pub identity: Option<ClusterIdentity>,
    pub membership_source: MembershipSource,
    pub membership_source_available: bool,
    pub initial_membership: Option<MembershipView>,
    pub initial_placements: Vec<PlacementMetadata>,
}

impl Default for ClusterManagerConfig {
    fn default() -> Self {
        Self::single_node()
    }
}

impl ClusterManagerConfig {
    pub fn single_node() -> Self {
        Self {
            mode: ClusterMode::SingleNode,
            identity: None,
            membership_source: MembershipSource::LocalDefault,
            membership_source_available: true,
            initial_membership: None,
            initial_placements: Vec::new(),
        }
    }

    pub fn cluster_aware(identity: ClusterIdentity) -> Self {
        Self {
            mode: ClusterMode::ClusterAware,
            identity: Some(identity),
            membership_source: MembershipSource::Chirps,
            membership_source_available: true,
            initial_membership: None,
            initial_placements: Vec::new(),
        }
    }

    pub fn cluster_aware_chirps_unavailable(identity: ClusterIdentity) -> Self {
        Self {
            membership_source_available: false,
            ..Self::cluster_aware(identity)
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterManager {
    mode: ClusterMode,
    identity: ClusterIdentity,
    membership: MembershipView,
    placement: PlacementCatalog,
    routing_capabilities: RoutingCapabilities,
    metrics_summary: ClusterMetricsSummary,
    degraded: bool,
    diagnostics: Vec<ClusterDiagnostic>,
    update_epoch: UpdateEpoch,
}

impl Default for ClusterManager {
    fn default() -> Self {
        Self::new(ClusterManagerConfig::default())
            .expect("default single-node ClusterManager config is valid")
    }
}

impl ClusterManager {
    pub fn new(config: ClusterManagerConfig) -> Result<Self, ClusterError> {
        match config.mode {
            ClusterMode::SingleNode => Ok(Self::new_single_node(config)),
            ClusterMode::ClusterAware => Self::new_cluster_aware(config),
        }
    }

    pub fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }

    pub fn membership_view(&self) -> MembershipView {
        self.membership.clone()
    }

    pub fn placement_view(&self) -> PlacementView {
        self.placement.placement_view()
    }

    pub fn placement_catalog(&self) -> &PlacementCatalog {
        &self.placement
    }

    pub fn status_snapshot(&self) -> ClusterStatusSnapshot {
        ClusterStatusSnapshot {
            schema_version: CLUSTER_METADATA_SCHEMA_VERSION,
            mode: self.mode,
            identity: self.identity.clone(),
            membership: self.membership.clone(),
            placement: self.placement.placement_view(),
            routing_capabilities: self.routing_capabilities.clone(),
            metrics_summary: self.metrics_summary.clone(),
            degraded: self.degraded,
            diagnostics: self.diagnostics.clone(),
        }
    }

    pub fn join(&mut self) -> Result<ClusterStatusSnapshot, ClusterError> {
        if self.mode == ClusterMode::SingleNode {
            return Ok(self.status_snapshot());
        }

        self.transition_local(NodeState::Joining, "join_requested");
        self.transition_local(NodeState::Active, "join_completed");
        Ok(self.status_snapshot())
    }

    pub fn leave(&mut self) -> Result<ClusterStatusSnapshot, ClusterError> {
        if self.mode == ClusterMode::SingleNode {
            return Ok(self.status_snapshot());
        }

        self.transition_local(NodeState::Leaving, "leave_requested");
        Ok(self.status_snapshot())
    }

    pub fn apply_table_lifecycle_effect(
        &mut self,
        effect: TableLifecycleEffect,
    ) -> Result<PlacementView, ClusterError> {
        let view = self.placement.apply_table_lifecycle_effect(effect);
        self.update_epoch = self.update_epoch.max(view.update_epoch);
        Ok(view)
    }

    pub fn reconcile_placements(
        &mut self,
        catalog_snapshot: &CatalogTableSnapshot,
    ) -> Result<PlacementView, ClusterError> {
        let view = self.placement.reconcile(catalog_snapshot);
        self.update_epoch = self.update_epoch.max(view.update_epoch);
        Ok(view)
    }

    fn new_single_node(config: ClusterManagerConfig) -> Self {
        let identity = config
            .identity
            .unwrap_or_else(|| ClusterIdentity::unconfigured_single_node("local"));
        let update_epoch = identity.update_epoch;

        Self {
            mode: ClusterMode::SingleNode,
            identity,
            membership: MembershipView::new(MembershipSource::LocalDefault, update_epoch),
            placement: PlacementCatalog::new(update_epoch),
            routing_capabilities: RoutingCapabilities::default(),
            metrics_summary: ClusterMetricsSummary::default(),
            degraded: false,
            diagnostics: Vec::new(),
            update_epoch,
        }
    }

    fn new_cluster_aware(config: ClusterManagerConfig) -> Result<Self, ClusterError> {
        let identity = config.identity.ok_or_else(|| {
            ClusterError::new(
                StableDiagnosticCode::InvalidNodeIdentity,
                "cluster-aware mode requires a configured local node identity",
                "provide node_id, cluster_id, advertised_endpoint, role, and lifecycle_state",
            )
        })?;
        validate_cluster_identity(&identity)?;

        let update_epoch = identity.update_epoch;
        let mut membership = config
            .initial_membership
            .unwrap_or_else(|| MembershipView::new(config.membership_source, update_epoch));
        membership.source = config.membership_source;
        membership.schema_version = CLUSTER_METADATA_SCHEMA_VERSION;
        membership.update_epoch = membership.update_epoch.max(update_epoch);
        validate_membership_compatible_with_identity(&identity, &membership)?;

        let placement = PlacementCatalog::from_placements(update_epoch, config.initial_placements);

        let mut manager = Self {
            mode: ClusterMode::ClusterAware,
            identity,
            membership,
            placement,
            routing_capabilities: RoutingCapabilities::default(),
            metrics_summary: ClusterMetricsSummary::default(),
            degraded: false,
            diagnostics: Vec::new(),
            update_epoch,
        };

        manager.upsert_local_member("initialized");

        if !config.membership_source_available {
            manager.degraded = true;
            manager
                .diagnostics
                .push(unavailable_membership_diagnostic(config.membership_source));
        }

        Ok(manager)
    }

    fn next_epoch(&mut self) -> UpdateEpoch {
        self.update_epoch = self.update_epoch.saturating_add(1);
        self.update_epoch
    }

    fn transition_local(&mut self, state: NodeState, reason: &'static str) {
        let epoch = self.next_epoch();
        self.identity.lifecycle_state = state;
        self.identity.update_epoch = epoch;
        self.membership.update_epoch = epoch;
        self.upsert_local_member(reason);
    }

    fn upsert_local_member(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        let status = MemberStatus {
            identity: MemberIdentity::from(&self.identity),
            raw_reachability_state: None,
            derived_state: self.identity.lifecycle_state,
            transition_reason: Some(reason),
        };

        if let Some(existing) = self
            .membership
            .members
            .iter_mut()
            .find(|member| member.identity.node_id == self.identity.node_id)
        {
            *existing = status;
        } else {
            self.membership.members.push(status);
        }
    }
}

fn validate_cluster_identity(identity: &ClusterIdentity) -> Result<(), ClusterError> {
    if is_blank(identity.node_id.as_str()) {
        return Err(invalid_identity_error(
            "node_id must be a non-empty stable identifier",
        ));
    }

    let Some(cluster_id) = &identity.cluster_id else {
        return Err(invalid_identity_error(
            "cluster_id is required in cluster-aware mode",
        ));
    };
    if is_blank(cluster_id.as_str()) {
        return Err(invalid_identity_error("cluster_id must be non-empty"));
    }

    let Some(endpoint) = &identity.advertised_endpoint else {
        return Err(invalid_identity_error(
            "advertised_endpoint is required in cluster-aware mode",
        ));
    };
    if is_blank(endpoint.as_str()) {
        return Err(invalid_identity_error(
            "advertised_endpoint must be non-empty",
        ));
    }

    if identity.lifecycle_state == NodeState::Unconfigured {
        return Err(invalid_identity_error(
            "cluster-aware local identity cannot use the unconfigured lifecycle state",
        ));
    }

    Ok(())
}

fn validate_membership_compatible_with_identity(
    identity: &ClusterIdentity,
    membership: &MembershipView,
) -> Result<(), ClusterError> {
    for member in &membership.members {
        if member.identity.node_id == identity.node_id {
            let local_member = MemberIdentity::from(identity);
            if member.identity != local_member {
                return Err(ClusterError::new(
                    StableDiagnosticCode::ConflictingNodeIdentity,
                    "membership contains a conflicting record for the local node_id",
                    "remove the stale member record or align node_id, cluster_id, endpoint, and role",
                ));
            }
        }

        if let (Some(local_cluster), Some(member_cluster)) =
            (&identity.cluster_id, &member.identity.cluster_id)
            && local_cluster != member_cluster
        {
            return Err(ClusterError::new(
                StableDiagnosticCode::ConflictingNodeIdentity,
                "membership contains a node from a different cluster_id",
                "verify the configured cluster_id and remove metadata from the wrong cluster",
            ));
        }
    }

    Ok(())
}

fn invalid_identity_error(message: impl Into<String>) -> ClusterError {
    ClusterError::new(
        StableDiagnosticCode::InvalidNodeIdentity,
        message,
        "configure a stable node_id, cluster_id, advertised_endpoint, role, and non-unconfigured lifecycle_state",
    )
}

fn unavailable_membership_diagnostic(source: MembershipSource) -> ClusterDiagnostic {
    match source {
        MembershipSource::Chirps => ClusterDiagnostic::new(
            StableDiagnosticCode::ChirpsUnavailable,
            "Chirps membership input is unavailable; using single-node-equivalent routing fallback",
            "enable the Chirps adapter or fix membership service configuration before relying on cluster routing",
            true,
        ),
        _ => ClusterDiagnostic::new(
            StableDiagnosticCode::MembershipSourceUnavailable,
            "cluster membership input is unavailable; using single-node-equivalent routing fallback",
            "restore the configured membership source before relying on cluster routing",
            true,
        ),
    }
}

fn is_blank(value: &str) -> bool {
    value.trim().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn active_identity() -> ClusterIdentity {
        ClusterIdentity {
            cluster_id: Some(ClusterId::new("cluster-a")),
            advertised_endpoint: Some(Endpoint::new("127.0.0.1:7001")),
            ..ClusterIdentity::new("node-a", NodeRole::Worker, NodeState::Active)
        }
    }

    fn users_placement(table_id: TableId, update_epoch: UpdateEpoch) -> PlacementMetadata {
        let mut placement = PlacementMetadata::new("default.public.users", table_id, update_epoch);
        placement.targets.push(RoutingTarget::table(
            "node-a",
            "default.public.users",
            table_id,
        ));
        placement
    }

    fn placement_with_targets(
        table_ref: &str,
        table_id: TableId,
        update_epoch: UpdateEpoch,
        node_ids: &[&str],
    ) -> PlacementMetadata {
        let mut placement = PlacementMetadata::new(table_ref, table_id, update_epoch);
        placement.targets.extend(
            node_ids
                .iter()
                .map(|node_id| RoutingTarget::table(*node_id, table_ref, table_id)),
        );
        placement
    }

    fn member_status(node_id: &str, role: NodeRole, derived_state: NodeState) -> MemberStatus {
        MemberStatus {
            identity: MemberIdentity {
                node_id: NodeId::new(node_id),
                cluster_id: Some(ClusterId::new("cluster-a")),
                advertised_endpoint: Some(Endpoint::new(format!("127.0.0.1:7{}", node_id))),
                role,
            },
            raw_reachability_state: None,
            derived_state,
            transition_reason: Some("test".to_string()),
        }
    }

    fn membership_with(members: Vec<MemberStatus>) -> MembershipView {
        let mut membership = MembershipView::new(MembershipSource::Persisted, 5);
        membership.members = members;
        membership
    }

    fn read_ref(table_ref: &str, source: QueryTableReferenceSource) -> QueryTableReference {
        QueryTableReference::read(table_ref, source)
    }

    fn catalog_snapshot(tables: &[(&str, TableId)]) -> CatalogTableSnapshot {
        CatalogTableSnapshot::from_tables(
            6,
            tables
                .iter()
                .map(|(table_ref, table_id)| CatalogTableRef::new(*table_ref, *table_id))
                .collect(),
        )
    }

    fn routing_request(
        plan_id: &str,
        tables: &[(&str, TableId)],
        table_references: Vec<QueryTableReference>,
    ) -> QueryRoutingRequest {
        QueryRoutingRequest::new(plan_id, catalog_snapshot(tables), table_references)
    }

    #[test]
    fn node_role_serializes_as_stable_snake_case() {
        assert_eq!(
            serde_json::to_string(&NodeRole::Gateway).unwrap(),
            "\"gateway\""
        );
        assert_eq!(
            serde_json::to_string(&NodeRole::Worker).unwrap(),
            "\"worker\""
        );
        assert_eq!(
            serde_json::from_str::<NodeRole>("\"worker\"").unwrap(),
            NodeRole::Worker
        );
    }

    #[test]
    fn node_state_serializes_as_stable_snake_case() {
        let states = [
            (NodeState::Unconfigured, "unconfigured"),
            (NodeState::Joining, "joining"),
            (NodeState::Active, "active"),
            (NodeState::Leaving, "leaving"),
            (NodeState::Unreachable, "unreachable"),
        ];

        for (state, wire_name) in states {
            assert_eq!(
                serde_json::to_string(&state).unwrap(),
                format!("\"{wire_name}\"")
            );
            assert_eq!(
                serde_json::from_str::<NodeState>(&format!("\"{wire_name}\"")).unwrap(),
                state
            );
        }
    }

    #[test]
    fn stable_diagnostic_codes_serialize_as_contract_strings() {
        let diagnostics = [
            (
                StableDiagnosticCode::SingleResolvedTarget,
                "single_resolved_target",
            ),
            (StableDiagnosticCode::PlacementAbsent, "placement_absent"),
            (StableDiagnosticCode::PlacementStale, "placement_stale"),
            (
                StableDiagnosticCode::PlacementTargetIneligible,
                "placement_target_ineligible",
            ),
            (
                StableDiagnosticCode::FutureDistributedExecutionRequired,
                "future_distributed_execution_required",
            ),
            (
                StableDiagnosticCode::ChirpsUnavailable,
                "chirps_unavailable",
            ),
            (
                StableDiagnosticCode::MembershipSourceUnavailable,
                "membership_source_unavailable",
            ),
            (
                StableDiagnosticCode::ConflictingNodeIdentity,
                "conflicting_node_identity",
            ),
        ];

        for (diagnostic, wire_name) in diagnostics {
            assert_eq!(
                serde_json::to_string(&diagnostic).unwrap(),
                format!("\"{wire_name}\"")
            );
            assert_eq!(
                serde_json::from_str::<StableDiagnosticCode>(&format!("\"{wire_name}\"")).unwrap(),
                diagnostic
            );
        }
    }

    #[test]
    fn routing_diagnostics_roundtrip_preserves_version_epoch_and_targets() {
        let mut diagnostics = RoutingDiagnostics::new(
            RoutingDecisionKind::LocalOnly,
            StableDiagnosticCode::PlacementAbsent,
            "plan-42",
            7,
        );
        diagnostics.roles.push(NodeRole::Gateway);
        diagnostics
            .targets
            .push(RoutingTarget::table("node-a", "default.public.users", 12));

        let encoded = serde_json::to_string(&diagnostics).unwrap();
        assert!(encoded.contains("\"schema_version\":1"));
        assert!(encoded.contains("\"update_epoch\":7"));
        assert!(encoded.contains("\"decision\":\"local_only\""));
        assert!(encoded.contains("\"reason\":\"placement_absent\""));

        let decoded: RoutingDiagnostics = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, diagnostics);
    }

    #[test]
    fn query_router_returns_mixed_fallback_for_partial_missing_placement() {
        let catalog = PlacementCatalog::from_placements(
            3,
            vec![placement_with_targets(
                "default.public.orders",
                8,
                3,
                &["node-a"],
            )],
        );
        let membership = membership_with(vec![member_status(
            "node-a",
            NodeRole::Worker,
            NodeState::Active,
        )]);
        let router = QueryRouter::new(&catalog, &membership);

        let diagnostics = router.route(routing_request(
            "plan-missing",
            &[("default.public.orders", 8)],
            vec![
                read_ref(
                    "default.public.users",
                    QueryTableReferenceSource::LogicalPlanScan,
                ),
                read_ref(
                    "default.public.orders",
                    QueryTableReferenceSource::LogicalPlanScan,
                ),
            ],
        ));

        assert_eq!(diagnostics.decision, RoutingDecisionKind::LocalOnly);
        assert_eq!(
            diagnostics.reason,
            StableDiagnosticCode::MixedPlacementFallback
        );
        assert_eq!(diagnostics.targets.len(), 1);
        assert!(diagnostics.excluded_targets.is_empty());
    }

    #[test]
    fn query_router_returns_stale_placement_with_excluded_stale_targets() {
        let mut placement = placement_with_targets("default.public.users", 7, 3, &["node-a"]);
        placement.lifecycle_state = PlacementLifecycleState::Stale;
        let catalog = PlacementCatalog::from_placements(3, vec![placement]);
        let membership = membership_with(vec![member_status(
            "node-a",
            NodeRole::Worker,
            NodeState::Active,
        )]);
        let router = QueryRouter::new(&catalog, &membership);

        let diagnostics = router.route(routing_request(
            "plan-stale",
            &[("default.public.users", 7)],
            vec![read_ref(
                "default.public.users",
                QueryTableReferenceSource::LogicalPlanScan,
            )],
        ));

        assert_eq!(diagnostics.reason, StableDiagnosticCode::PlacementStale);
        assert!(diagnostics.targets.is_empty());
        assert_eq!(diagnostics.excluded_targets.len(), 1);
        assert_eq!(
            diagnostics.excluded_targets[0].reason,
            ExcludedTargetReason::PlacementStale
        );
    }

    #[test]
    fn query_router_resolves_single_eligible_target() {
        let catalog = PlacementCatalog::from_placements(3, vec![users_placement(7, 3)]);
        let membership = membership_with(vec![member_status(
            "node-a",
            NodeRole::Worker,
            NodeState::Active,
        )]);
        let router = QueryRouter::new(&catalog, &membership);

        let diagnostics = router.route(routing_request(
            "plan-single",
            &[("default.public.users", 7)],
            vec![read_ref(
                "default.public.users",
                QueryTableReferenceSource::LogicalPlanScan,
            )],
        ));

        assert_eq!(diagnostics.decision, RoutingDecisionKind::LocalOnly);
        assert_eq!(
            diagnostics.reason,
            StableDiagnosticCode::SingleResolvedTarget
        );
        assert_eq!(diagnostics.targets.len(), 1);
        assert_eq!(diagnostics.targets[0].node_id, NodeId::new("node-a"));
        assert!(diagnostics.excluded_targets.is_empty());
    }

    #[test]
    fn query_router_marks_multi_node_targets_as_future_distributed() {
        let catalog = PlacementCatalog::from_placements(
            3,
            vec![placement_with_targets(
                "default.public.users",
                7,
                3,
                &["node-a", "node-b"],
            )],
        );
        let membership = membership_with(vec![
            member_status("node-a", NodeRole::Worker, NodeState::Active),
            member_status("node-b", NodeRole::Worker, NodeState::Active),
        ]);
        let router = QueryRouter::new(&catalog, &membership);

        let diagnostics = router.route(routing_request(
            "plan-distributed",
            &[("default.public.users", 7)],
            vec![read_ref(
                "default.public.users",
                QueryTableReferenceSource::LogicalPlanScan,
            )],
        ));

        assert_eq!(
            diagnostics.decision,
            RoutingDecisionKind::FutureDistributedExecutionRequired
        );
        assert_eq!(
            diagnostics.reason,
            StableDiagnosticCode::FutureDistributedExecutionRequired
        );
        assert_eq!(diagnostics.targets.len(), 2);
        assert!(diagnostics.excluded_targets.is_empty());
    }

    #[test]
    fn query_router_simulates_scatter_gather_for_multi_node_targets() {
        let catalog = PlacementCatalog::from_placements(
            3,
            vec![placement_with_targets(
                "default.public.users",
                7,
                3,
                &["node-a", "node-b"],
            )],
        );
        let membership = membership_with(vec![
            member_status("node-a", NodeRole::Worker, NodeState::Active),
            member_status("node-b", NodeRole::Worker, NodeState::Active),
        ]);
        let router = QueryRouter::new(&catalog, &membership);

        let diagnostics = router.simulate(routing_request(
            "plan-simulated",
            &[("default.public.users", 7)],
            vec![read_ref(
                "default.public.users",
                QueryTableReferenceSource::LogicalPlanScan,
            )],
        ));

        assert_eq!(
            diagnostics.decision,
            RoutingDecisionKind::ScatterGatherSimulated
        );
        assert_eq!(
            diagnostics.reason,
            StableDiagnosticCode::ScatterGatherSimulated
        );
        assert_eq!(diagnostics.targets.len(), 2);
    }

    #[test]
    fn query_router_excludes_role_unknown_and_inactive_targets() {
        let catalog = PlacementCatalog::from_placements(
            3,
            vec![placement_with_targets(
                "default.public.users",
                7,
                3,
                &["node-a", "node-gateway", "node-missing", "node-down"],
            )],
        );
        let membership = membership_with(vec![
            member_status("node-a", NodeRole::Worker, NodeState::Active),
            member_status("node-gateway", NodeRole::Gateway, NodeState::Active),
            member_status("node-down", NodeRole::Worker, NodeState::Unreachable),
        ]);
        let router = QueryRouter::new(&catalog, &membership);

        let diagnostics = router.route(routing_request(
            "plan-exclusions",
            &[("default.public.users", 7)],
            vec![read_ref(
                "default.public.users",
                QueryTableReferenceSource::LogicalPlanScan,
            )],
        ));

        assert_eq!(
            diagnostics.reason,
            StableDiagnosticCode::SingleResolvedTarget
        );
        assert_eq!(diagnostics.targets.len(), 1);
        assert_eq!(diagnostics.excluded_targets.len(), 3);
        assert!(diagnostics.excluded_targets.iter().any(|excluded| {
            excluded.target.node_id == NodeId::new("node-gateway")
                && excluded.reason == ExcludedTargetReason::RoleNotWorker
        }));
        assert!(diagnostics.excluded_targets.iter().any(|excluded| {
            excluded.target.node_id == NodeId::new("node-missing")
                && excluded.reason == ExcludedTargetReason::MemberUnknown
        }));
        assert!(diagnostics.excluded_targets.iter().any(|excluded| {
            excluded.target.node_id == NodeId::new("node-down")
                && excluded.reason == ExcludedTargetReason::MemberInactive
        }));
    }

    #[test]
    fn query_router_returns_placement_target_ineligible_when_no_target_can_run() {
        let catalog = PlacementCatalog::from_placements(
            3,
            vec![placement_with_targets(
                "default.public.users",
                7,
                3,
                &["node-gateway"],
            )],
        );
        let membership = membership_with(vec![member_status(
            "node-gateway",
            NodeRole::Gateway,
            NodeState::Active,
        )]);
        let router = QueryRouter::new(&catalog, &membership);

        let diagnostics = router.route(routing_request(
            "plan-ineligible",
            &[("default.public.users", 7)],
            vec![read_ref(
                "default.public.users",
                QueryTableReferenceSource::LogicalPlanScan,
            )],
        ));

        assert_eq!(
            diagnostics.reason,
            StableDiagnosticCode::PlacementTargetIneligible
        );
        assert!(diagnostics.targets.is_empty());
        assert_eq!(diagnostics.excluded_targets.len(), 1);
        assert_eq!(
            diagnostics.excluded_targets[0].reason,
            ExcludedTargetReason::RoleNotWorker
        );
    }

    #[test]
    fn query_router_composes_join_and_subquery_style_table_references() {
        let catalog = PlacementCatalog::from_placements(
            3,
            vec![
                placement_with_targets("default.public.users", 7, 3, &["node-a"]),
                placement_with_targets("default.public.orders", 8, 3, &["node-a"]),
                placement_with_targets("default.public.audit_log", 9, 3, &["node-b"]),
            ],
        );
        let membership = membership_with(vec![
            member_status("node-a", NodeRole::Worker, NodeState::Active),
            member_status("node-b", NodeRole::Worker, NodeState::Active),
        ]);
        let router = QueryRouter::new(&catalog, &membership);

        let diagnostics = router.route(routing_request(
            "plan-join-subquery",
            &[
                ("default.public.users", 7),
                ("default.public.orders", 8),
                ("default.public.audit_log", 9),
            ],
            vec![
                read_ref(
                    "default.public.users",
                    QueryTableReferenceSource::LogicalPlanScan,
                ),
                read_ref(
                    "default.public.orders",
                    QueryTableReferenceSource::LogicalPlanScan,
                ),
                read_ref(
                    "default.public.audit_log",
                    QueryTableReferenceSource::TypedExprSubquery,
                ),
            ],
        ));

        assert_eq!(
            diagnostics.decision,
            RoutingDecisionKind::FutureDistributedExecutionRequired
        );
        assert_eq!(diagnostics.targets.len(), 3);
        for table_ref in [
            "default.public.users",
            "default.public.orders",
            "default.public.audit_log",
        ] {
            assert!(
                diagnostics
                    .targets
                    .iter()
                    .any(|target| target.table_ref == TableRef::new(table_ref)),
                "missing target for {table_ref}"
            );
        }
    }

    #[test]
    fn membership_view_roundtrip_preserves_raw_and_derived_states() {
        let identity = ClusterIdentity {
            cluster_id: Some(ClusterId::new("cluster-a")),
            advertised_endpoint: Some(Endpoint::new("127.0.0.1:7001")),
            update_epoch: 3,
            ..ClusterIdentity::new("node-a", NodeRole::Worker, NodeState::Active)
        };
        let mut view = MembershipView::new(MembershipSource::Chirps, 4);
        view.members.push(MemberStatus {
            identity: MemberIdentity::from(&identity),
            raw_reachability_state: Some(RawChirpsState::Alive),
            derived_state: NodeState::Active,
            transition_reason: Some("chirps_alive".to_string()),
        });

        let encoded = serde_json::to_string(&view).unwrap();
        assert!(encoded.contains("\"source\":\"chirps\""));
        assert!(encoded.contains("\"raw_reachability_state\":\"alive\""));
        assert!(encoded.contains("\"derived_state\":\"active\""));

        let decoded: MembershipView = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, view);
    }

    #[test]
    fn raw_chirps_state_derives_application_node_state() {
        assert_eq!(
            RawChirpsState::Alive.derived_node_state(),
            NodeState::Active
        );
        assert_eq!(
            RawChirpsState::Suspect.derived_node_state(),
            NodeState::Unreachable
        );
        assert_eq!(
            RawChirpsState::Dead.derived_node_state(),
            NodeState::Unreachable
        );
        assert_eq!(RawChirpsState::Alive.transition_reason(), "chirps_alive");
    }

    #[test]
    fn chirps_bytes_are_stable_lower_hex_node_ids() {
        let node_id = node_id_from_chirps_bytes(&[
            0x00, 0x01, 0x0a, 0x0f, 0x10, 0x1f, 0x20, 0x7f, 0x80, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
            0xf0, 0xff,
        ]);

        assert_eq!(node_id.as_str(), "00010a0f101f207f80aabbccddeef0ff");
    }

    #[test]
    fn chirps_unavailable_helper_returns_degraded_cluster_diagnostic() {
        let diagnostic = chirps_unavailable_diagnostic();

        assert_eq!(diagnostic.code, StableDiagnosticCode::ChirpsUnavailable);
        assert!(diagnostic.degraded);
        assert!(diagnostic.message.contains("Chirps membership input"));
    }

    #[cfg(feature = "chirps")]
    #[test]
    fn chirps_adapter_maps_raw_status_values() {
        use alopex_chirps_gossip_swim::types::Status;

        assert_eq!(
            raw_chirps_state_from_status(&Status::Alive),
            RawChirpsState::Alive
        );
        assert_eq!(
            raw_chirps_state_from_status(&Status::Suspect),
            RawChirpsState::Suspect
        );
        assert_eq!(
            raw_chirps_state_from_status(&Status::Dead),
            RawChirpsState::Dead
        );
    }

    #[test]
    fn default_single_node_manager_is_non_degraded_v06_fallback() {
        let manager = ClusterManager::new(ClusterManagerConfig::single_node()).unwrap();
        let snapshot = manager.status_snapshot();

        assert_eq!(snapshot.schema_version, CLUSTER_METADATA_SCHEMA_VERSION);
        assert_eq!(snapshot.mode, ClusterMode::SingleNode);
        assert_eq!(snapshot.identity.lifecycle_state, NodeState::Unconfigured);
        assert_eq!(snapshot.membership.source, MembershipSource::LocalDefault);
        assert!(snapshot.membership.members.is_empty());
        assert!(snapshot.placement.placements.is_empty());
        assert!(snapshot.routing_capabilities.local_only);
        assert!(!snapshot.degraded);
        assert!(snapshot.diagnostics.is_empty());
    }

    #[test]
    fn cluster_aware_with_unavailable_chirps_is_degraded_but_initialized() {
        let manager = ClusterManager::new(ClusterManagerConfig::cluster_aware_chirps_unavailable(
            active_identity(),
        ))
        .unwrap();
        let snapshot = manager.status_snapshot();

        assert_eq!(snapshot.mode, ClusterMode::ClusterAware);
        assert!(snapshot.degraded);
        assert_eq!(snapshot.diagnostics.len(), 1);
        assert_eq!(
            snapshot.diagnostics[0].code,
            StableDiagnosticCode::ChirpsUnavailable
        );
        assert_eq!(snapshot.membership.source, MembershipSource::Chirps);
        assert_eq!(snapshot.membership.members.len(), 1);
        assert_eq!(
            snapshot.membership.members[0].derived_state,
            NodeState::Active
        );
        assert_eq!(
            snapshot.membership.members[0].transition_reason.as_deref(),
            Some("initialized")
        );
    }

    #[test]
    fn invalid_cluster_aware_identity_fails_fast_with_remediation() {
        let invalid = ClusterIdentity {
            cluster_id: Some(ClusterId::new("cluster-a")),
            advertised_endpoint: Some(Endpoint::new("127.0.0.1:7001")),
            ..ClusterIdentity::new("", NodeRole::Gateway, NodeState::Active)
        };

        let err = ClusterManager::new(ClusterManagerConfig::cluster_aware(invalid)).unwrap_err();

        assert_eq!(err.code, StableDiagnosticCode::InvalidNodeIdentity);
        assert!(err.message.contains("node_id"));
        assert!(err.remediation.contains("stable node_id"));
    }

    #[test]
    fn conflicting_local_identity_in_membership_fails_fast() {
        let identity = active_identity();
        let mut membership = MembershipView::new(MembershipSource::Persisted, 2);
        membership.members.push(MemberStatus {
            identity: MemberIdentity {
                node_id: identity.node_id.clone(),
                cluster_id: identity.cluster_id.clone(),
                advertised_endpoint: Some(Endpoint::new("127.0.0.1:7999")),
                role: identity.role,
            },
            raw_reachability_state: None,
            derived_state: NodeState::Active,
            transition_reason: Some("persisted".to_string()),
        });

        let mut config = ClusterManagerConfig::cluster_aware(identity);
        config.membership_source = MembershipSource::Persisted;
        config.initial_membership = Some(membership);

        let err = ClusterManager::new(config).unwrap_err();

        assert_eq!(err.code, StableDiagnosticCode::ConflictingNodeIdentity);
        assert!(err.remediation.contains("stale member record"));
    }

    #[test]
    fn join_and_leave_update_local_lifecycle_and_membership_without_duplicates() {
        let identity = ClusterIdentity {
            lifecycle_state: NodeState::Joining,
            ..active_identity()
        };
        let mut manager =
            ClusterManager::new(ClusterManagerConfig::cluster_aware(identity)).unwrap();
        assert_eq!(manager.membership_view().members.len(), 1);

        let joined = manager.join().unwrap();
        assert_eq!(joined.identity.lifecycle_state, NodeState::Active);
        assert_eq!(joined.membership.members.len(), 1);
        assert_eq!(
            joined.membership.members[0].derived_state,
            NodeState::Active
        );
        assert_eq!(
            joined.membership.members[0].transition_reason.as_deref(),
            Some("join_completed")
        );

        let joined_again = manager.join().unwrap();
        assert_eq!(joined_again.membership.members.len(), 1);
        assert_eq!(
            joined_again.membership.members[0].derived_state,
            NodeState::Active
        );

        let left = manager.leave().unwrap();
        assert_eq!(left.identity.lifecycle_state, NodeState::Leaving);
        assert_eq!(left.membership.members.len(), 1);
        assert_eq!(left.membership.members[0].derived_state, NodeState::Leaving);
        assert_eq!(
            left.membership.members[0].transition_reason.as_deref(),
            Some("leave_requested")
        );
    }

    #[test]
    fn placement_catalog_returns_absent_when_no_current_placement_exists() {
        let catalog = PlacementCatalog::new(0);
        let table_ref = TableRef::new("default.public.users");

        assert!(catalog.active_placement_for(&table_ref, 7).is_none());
        assert!(catalog.active_targets_for(&table_ref, 7).is_empty());
    }

    #[test]
    fn placement_catalog_returns_present_active_placement_for_matching_table_id() {
        let catalog = PlacementCatalog::from_placements(1, vec![users_placement(7, 1)]);
        let table_ref = TableRef::new("default.public.users");

        let placement = catalog.active_placement_for(&table_ref, 7).unwrap();
        assert_eq!(placement.table_id, 7);
        assert_eq!(placement.lifecycle_state, PlacementLifecycleState::Active);
        assert_eq!(catalog.active_targets_for(&table_ref, 7).len(), 1);
    }

    #[test]
    fn placement_catalog_ignores_mismatched_table_id_and_reconciles_to_stale() {
        let mut catalog = PlacementCatalog::from_placements(1, vec![users_placement(7, 1)]);
        let table_ref = TableRef::new("default.public.users");

        assert!(catalog.active_placement_for(&table_ref, 8).is_none());

        let reconciled = catalog.reconcile(&CatalogTableSnapshot::from_tables(
            2,
            vec![CatalogTableRef::new("default.public.users", 8)],
        ));

        assert_eq!(
            reconciled.placements[0].lifecycle_state,
            PlacementLifecycleState::Stale
        );
        assert!(catalog.active_placement_for(&table_ref, 7).is_none());
    }

    #[test]
    fn placement_catalog_reconciles_absent_catalog_table_to_tombstone() {
        let mut catalog = PlacementCatalog::from_placements(1, vec![users_placement(7, 1)]);
        let table_ref = TableRef::new("default.public.users");

        let reconciled = catalog.reconcile(&CatalogTableSnapshot::new(2));

        assert_eq!(
            reconciled.placements[0].lifecycle_state,
            PlacementLifecycleState::Tombstoned
        );
        assert!(catalog.active_placement_for(&table_ref, 7).is_none());
    }

    #[test]
    fn placement_catalog_schema_change_marks_matching_table_id_stale_idempotently() {
        let mut catalog = PlacementCatalog::from_placements(1, vec![users_placement(7, 1)]);
        let first = catalog.apply_table_lifecycle_effect(TableLifecycleEffect::SchemaChanged {
            table_ref: TableRef::new("default.public.users"),
            table_id: 7,
        });
        let first_epoch = first.update_epoch;

        assert_eq!(
            first.placements[0].lifecycle_state,
            PlacementLifecycleState::Stale
        );
        assert!(
            catalog
                .active_placement_for(&TableRef::new("default.public.users"), 7)
                .is_none()
        );

        let repeated = catalog.apply_table_lifecycle_effect(TableLifecycleEffect::SchemaChanged {
            table_ref: TableRef::new("default.public.users"),
            table_id: 7,
        });

        assert_eq!(repeated.update_epoch, first_epoch);
        assert_eq!(repeated.placements.len(), 1);
        assert_eq!(
            repeated.placements[0].lifecycle_state,
            PlacementLifecycleState::Stale
        );
    }

    #[test]
    fn placement_catalog_drop_tombstones_matching_table_id_idempotently() {
        let mut catalog = PlacementCatalog::from_placements(1, vec![users_placement(7, 1)]);
        let first = catalog.apply_table_lifecycle_effect(TableLifecycleEffect::Dropped {
            table_ref: TableRef::new("default.public.users"),
            table_id: 7,
        });
        let first_epoch = first.update_epoch;

        assert_eq!(
            first.placements[0].lifecycle_state,
            PlacementLifecycleState::Tombstoned
        );

        let repeated = catalog.apply_table_lifecycle_effect(TableLifecycleEffect::Dropped {
            table_ref: TableRef::new("default.public.users"),
            table_id: 7,
        });

        assert_eq!(repeated.update_epoch, first_epoch);
        assert_eq!(repeated.placements.len(), 1);
        assert_eq!(
            repeated.placements[0].lifecycle_state,
            PlacementLifecycleState::Tombstoned
        );
    }

    #[test]
    fn placement_catalog_created_effect_stales_older_same_table_ref_without_revival() {
        let mut old = users_placement(7, 1);
        old.lifecycle_state = PlacementLifecycleState::Active;
        let mut already_tombstoned = users_placement(6, 1);
        already_tombstoned.lifecycle_state = PlacementLifecycleState::Tombstoned;
        let mut catalog = PlacementCatalog::from_placements(1, vec![old, already_tombstoned]);

        let view = catalog.apply_table_lifecycle_effect(TableLifecycleEffect::Created {
            table_ref: TableRef::new("default.public.users"),
            table_id: 8,
        });

        assert_eq!(
            view.placements[0].lifecycle_state,
            PlacementLifecycleState::Stale
        );
        assert_eq!(
            view.placements[1].lifecycle_state,
            PlacementLifecycleState::Tombstoned
        );
        assert!(
            catalog
                .active_placement_for(&TableRef::new("default.public.users"), 7)
                .is_none()
        );
    }

    #[test]
    fn table_lifecycle_effect_marks_existing_placement_without_reintroducing_targets() {
        let mut config = ClusterManagerConfig::cluster_aware(active_identity());
        config.initial_placements.push(users_placement(7, 1));
        let mut manager = ClusterManager::new(config).unwrap();

        let dropped = manager
            .apply_table_lifecycle_effect(TableLifecycleEffect::Dropped {
                table_ref: TableRef::new("default.public.users"),
                table_id: 7,
            })
            .unwrap();
        assert_eq!(
            dropped.placements[0].lifecycle_state,
            PlacementLifecycleState::Tombstoned
        );

        let dropped_again = manager
            .apply_table_lifecycle_effect(TableLifecycleEffect::Dropped {
                table_ref: TableRef::new("default.public.users"),
                table_id: 7,
            })
            .unwrap();
        assert_eq!(dropped_again.placements.len(), 1);
        assert_eq!(
            dropped_again.placements[0].lifecycle_state,
            PlacementLifecycleState::Tombstoned
        );
    }

    #[test]
    fn cluster_manager_reconciles_placements_through_catalog_snapshot() {
        let mut config = ClusterManagerConfig::cluster_aware(active_identity());
        config.initial_placements.push(users_placement(7, 1));
        let mut manager = ClusterManager::new(config).unwrap();

        let view = manager
            .reconcile_placements(&CatalogTableSnapshot::from_tables(
                2,
                vec![CatalogTableRef::new("default.public.users", 8)],
            ))
            .unwrap();

        assert_eq!(
            view.placements[0].lifecycle_state,
            PlacementLifecycleState::Stale
        );
        assert!(
            manager
                .placement_catalog()
                .active_placement_for(&TableRef::new("default.public.users"), 7)
                .is_none()
        );
    }

    #[test]
    fn catalog_table_snapshot_tracks_current_table_id_without_generation_counter() {
        let snapshot = CatalogTableSnapshot::from_tables(
            4,
            vec![CatalogTableRef::new("default.public.users", 7)],
        );
        let table_ref = TableRef::new("default.public.users");

        assert_eq!(snapshot.table_id_for(&table_ref), Some(7));
        assert!(snapshot.contains_current(&table_ref, 7));
        assert!(!snapshot.contains_current(&table_ref, 8));
    }

    #[test]
    fn status_snapshot_and_lifecycle_effect_use_stable_wire_names() {
        let manager = ClusterManager::new(ClusterManagerConfig::cluster_aware_chirps_unavailable(
            active_identity(),
        ))
        .unwrap();

        let encoded = serde_json::to_string(&manager.status_snapshot()).unwrap();
        assert!(encoded.contains("\"mode\":\"cluster_aware\""));
        assert!(encoded.contains("\"code\":\"chirps_unavailable\""));
        assert!(encoded.contains("\"degraded\":true"));

        let effect = TableLifecycleEffect::SchemaChanged {
            table_ref: TableRef::new("default.public.users"),
            table_id: 7,
        };
        let encoded_effect = serde_json::to_string(&effect).unwrap();
        assert!(encoded_effect.contains("schema_changed"));
        assert!(!encoded_effect.contains("SchemaChanged"));
    }
}
