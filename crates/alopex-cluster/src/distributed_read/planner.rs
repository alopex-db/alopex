//! Committed-metadata route planning for fenced distributed reads.

use std::collections::BTreeSet;

use alopex_core::{CanonicalRowKey, ReadAtPoint, RowKeyRange};
use alopex_sql::distributed_read::RemoteReadDescriptor;
use alopex_sql::{RangeReadSnapshot, StorageRangeConstraint};
use serde::{Deserialize, Serialize};

use crate::{
    ClusterReadConsistency, ClusterReadPoint, ClusterReadPointAuthority, ClusterReadPolicy,
    CommittedMetadata, NodeId, RangeId, RangeReplicaDirectory, RangeReplicaReadWatermark,
    RangeReplicaReadinessState, RangeRoutingDefinition, ReadConsistencyMode, ReadPointFailure,
    ReadPointRequest, RequestId, SchemaManifestId, TableId,
};

/// The caller's requested distributed consistency, before policy resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadModeRequest {
    /// Use the committed cluster read policy.
    Inherit,
    /// Require the current common committed prefix.
    Strong,
    /// Permit a common retained prefix no older than the supplied bound.
    Stale { max_age_ms: u64 },
}

/// Input required to plan one already-classified remote read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRoutePlanRequest {
    /// Closed SQL descriptor accepted before entering the cluster layer.
    pub descriptor: RemoteReadDescriptor,
    /// Resolved logical table identity from the catalog snapshot.
    pub table_id: TableId,
    /// Stable execution identifier shared by subsequent worker sessions.
    pub request_id: RequestId,
    /// Requested mode reported to callers alongside the effective mode.
    pub requested_mode: ReadModeRequest,
    /// Policy-owned stale budget used only when `requested_mode` is inherit.
    pub inherited_stale_max_age_ms: u64,
}

/// The remote-only route kind selected from committed metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteDecision {
    RemoteSingleRange,
    RemoteMultiRange,
}

/// The immutable fence shared by every target in one distributed read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadFence {
    pub metadata_version: u64,
    pub read_point: ClusterReadPoint,
    pub effective_consistency: ReadConsistencyMode,
}

/// One physical range and all strictly fence-compatible replica candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTarget {
    pub constraint: StorageRangeConstraint,
    pub candidates: Vec<NodeId>,
}

impl RangeTarget {
    /// Returns a replacement only when it was prevalidated against this exact
    /// range constraint and the plan's shared read fence.
    pub fn compatible_failover(&self, failed: &NodeId) -> Option<&NodeId> {
        self.candidates
            .iter()
            .find(|candidate| *candidate != failed)
    }
}

/// Complete, remote-only dispatch plan for one logical table read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedReadPlan {
    pub descriptor: RemoteReadDescriptor,
    pub request_id: RequestId,
    pub requested_mode: ReadModeRequest,
    pub decision: RouteDecision,
    pub fence: ReadFence,
    pub ranges: Vec<RangeTarget>,
}

impl DistributedReadPlan {
    /// Finds a failover target proven compatible with the existing fence.
    pub fn compatible_failover(&self, range_id: &RangeId, failed: &NodeId) -> Option<&NodeId> {
        self.ranges
            .iter()
            .find(|range| range.constraint.range_id() == range_id.as_str())
            .and_then(|range| range.compatible_failover(failed))
    }
}

/// Classified pre-dispatch routing failures.  None represents local fallback.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadRoutePlanningError {
    #[error("requested read mode is not permitted by committed policy")]
    ModeNotPermitted,
    #[error("no committed ranges cover table {0}")]
    NoRangesForTable(u32),
    #[error("committed ranges do not completely and non-overlappingly cover table {0}")]
    IncompleteOrOverlappingCoverage(u32),
    #[error("active schema manifest is unavailable for a distributed read")]
    MissingActiveSchemaManifest,
    #[error("range {range_id} has no replica compatible with the immutable read fence")]
    NoCompatibleTarget { range_id: String },
    #[error("cannot issue a fenced read point: {0}")]
    ReadPoint(#[from] ReadPointFailure),
    #[error("invalid committed range metadata: {0}")]
    InvalidRangeMetadata(String),
    #[error("could not construct a storage range constraint: {0}")]
    StorageConstraint(String),
}

impl ReadRoutePlanningError {
    /// Stable machine-readable detail shared by HTTP, gRPC, CLI and Python.
    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::ModeNotPermitted => "read_mode_not_permitted",
            Self::NoRangesForTable(_) => "range_not_configured",
            Self::IncompleteOrOverlappingCoverage(_) => "range_coverage_incomplete",
            Self::MissingActiveSchemaManifest => "schema_manifest_unavailable",
            Self::NoCompatibleTarget { .. } => "replica_not_ready",
            Self::ReadPoint(_) => "read_point_unavailable",
            Self::InvalidRangeMetadata(_) => "invalid_range_metadata",
            Self::StorageConstraint(_) => "storage_constraint_invalid",
        }
    }
}

/// Builds remote dispatch plans exclusively from immutable committed evidence.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReadRoutePlanner {
    read_point_authority: ClusterReadPointAuthority,
}

impl ReadRoutePlanner {
    pub fn new(read_point_authority: ClusterReadPointAuthority) -> Self {
        Self {
            read_point_authority,
        }
    }

    /// Returns the canonical committed-snapshot classification used by
    /// status and routing adapters before a remote read is dispatched.
    pub fn routing_outcome(
        &self,
        metadata: &CommittedMetadata,
        table_id: TableId,
    ) -> crate::RoutingOutcome {
        crate::CommittedMetadataProjector
            .project(metadata, &std::collections::BTreeMap::new())
            .routing_outcome(table_id)
    }

    /// Plans every physical range for a logical table or returns a classified
    /// pre-dispatch failure.  This function never returns a local decision.
    pub fn plan(
        &self,
        metadata: &CommittedMetadata,
        request: ReadRoutePlanRequest,
        watermarks: &[RangeReplicaReadWatermark],
        now_ms: u64,
    ) -> Result<DistributedReadPlan, ReadRoutePlanningError> {
        let effective_consistency = resolve_mode(
            metadata.read_policy(),
            request.requested_mode,
            request.inherited_stale_max_age_ms,
        )?;
        let ranges = table_ranges(metadata, request.table_id)?;
        let active_manifest = metadata
            .schema()
            .active_manifest
            .clone()
            .ok_or(ReadRoutePlanningError::MissingActiveSchemaManifest)?;
        let range_ids = ranges
            .iter()
            .map(|(definition, _)| definition.range_id.clone())
            .collect::<BTreeSet<_>>();
        let read_point = self.read_point_authority.issue(
            metadata,
            &ReadPointRequest {
                ranges: range_ids,
                consistency: effective_consistency,
            },
            watermarks,
            now_ms,
        )?;
        let fence = ReadFence {
            metadata_version: metadata.state_version(),
            read_point: read_point.clone(),
            effective_consistency,
        };
        let directory = RangeReplicaDirectory::from_committed(metadata);
        let targets = ranges
            .into_iter()
            .map(|(definition, row_key_range)| {
                range_target(
                    metadata,
                    &directory,
                    &read_point,
                    &active_manifest,
                    definition,
                    row_key_range,
                    watermarks,
                    effective_consistency,
                    now_ms,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let decision = if targets.len() == 1 {
            RouteDecision::RemoteSingleRange
        } else {
            RouteDecision::RemoteMultiRange
        };
        Ok(DistributedReadPlan {
            descriptor: request.descriptor,
            request_id: request.request_id,
            requested_mode: request.requested_mode,
            decision,
            fence,
            ranges: targets,
        })
    }
}

fn resolve_mode(
    policy: &ClusterReadPolicy,
    requested: ReadModeRequest,
    inherited_stale_max_age_ms: u64,
) -> Result<ReadConsistencyMode, ReadRoutePlanningError> {
    let permits_strong = matches!(
        policy.default_consistency,
        ClusterReadConsistency::Leader | ClusterReadConsistency::Quorum
    ) || policy.permitted_overrides.iter().any(|mode| {
        matches!(
            mode,
            ClusterReadConsistency::Leader | ClusterReadConsistency::Quorum
        )
    });
    let permits_stale = policy.default_consistency == ClusterReadConsistency::BoundedStaleness
        || policy
            .permitted_overrides
            .contains(&ClusterReadConsistency::BoundedStaleness);
    match requested {
        ReadModeRequest::Inherit => Ok(match policy.default_consistency {
            ClusterReadConsistency::Leader | ClusterReadConsistency::Quorum => {
                ReadConsistencyMode::Strong
            }
            ClusterReadConsistency::BoundedStaleness => ReadConsistencyMode::Stale {
                max_age_ms: inherited_stale_max_age_ms,
            },
        }),
        ReadModeRequest::Strong if permits_strong => Ok(ReadConsistencyMode::Strong),
        ReadModeRequest::Stale { max_age_ms } if permits_stale => {
            Ok(ReadConsistencyMode::Stale { max_age_ms })
        }
        _ => Err(ReadRoutePlanningError::ModeNotPermitted),
    }
}

fn table_ranges(
    metadata: &CommittedMetadata,
    table_id: TableId,
) -> Result<Vec<(&RangeRoutingDefinition, RowKeyRange)>, ReadRoutePlanningError> {
    let mut ranges = metadata
        .ranges()
        .values()
        .filter(|definition| definition.table_id == table_id)
        .map(|definition| range_interval(definition).map(|interval| (definition, interval)))
        .collect::<Result<Vec<_>, _>>()?;
    if ranges.is_empty() {
        return Err(ReadRoutePlanningError::NoRangesForTable(table_id));
    }
    ranges.sort_by(|(_, left), (_, right)| {
        left.encoded_bounds()
            .lower_inclusive
            .cmp(&right.encoded_bounds().lower_inclusive)
    });
    let full = RowKeyRange::full_table(table_id).encoded_bounds();
    let mut expected_lower = full.lower_inclusive;
    for (_, range) in &ranges {
        let bounds = range.encoded_bounds();
        if bounds.lower_inclusive != expected_lower {
            return Err(ReadRoutePlanningError::IncompleteOrOverlappingCoverage(
                table_id,
            ));
        }
        expected_lower = bounds.upper_exclusive;
    }
    if expected_lower != full.upper_exclusive {
        return Err(ReadRoutePlanningError::IncompleteOrOverlappingCoverage(
            table_id,
        ));
    }
    Ok(ranges)
}

fn range_interval(
    definition: &RangeRoutingDefinition,
) -> Result<RowKeyRange, ReadRoutePlanningError> {
    let decode = |bound: &Option<Vec<u8>>| {
        bound
            .as_deref()
            .map(CanonicalRowKey::decode)
            .transpose()
            .map_err(|error| ReadRoutePlanningError::InvalidRangeMetadata(error.to_string()))
    };
    RowKeyRange::from_keys(
        decode(&definition.lower_inclusive)?,
        decode(&definition.upper_exclusive)?,
        definition.table_id,
    )
    .map_err(|error| ReadRoutePlanningError::InvalidRangeMetadata(error.to_string()))
}

#[allow(clippy::too_many_arguments)]
fn range_target(
    metadata: &CommittedMetadata,
    directory: &RangeReplicaDirectory,
    read_point: &ClusterReadPoint,
    active_manifest: &SchemaManifestId,
    definition: &RangeRoutingDefinition,
    row_key_range: RowKeyRange,
    watermarks: &[RangeReplicaReadWatermark],
    consistency: ReadConsistencyMode,
    now_ms: u64,
) -> Result<RangeTarget, ReadRoutePlanningError> {
    let index_epoch = *read_point
        .index_epochs
        .get(&definition.range_id)
        .ok_or_else(|| ReadRoutePlanningError::NoCompatibleTarget {
            range_id: definition.range_id.as_str().to_string(),
        })?;
    let snapshot = RangeReadSnapshot::new(
        ReadAtPoint::new(
            read_point.data_epoch,
            read_point.metadata_version,
            metadata.state_version(),
            index_epoch,
        ),
        active_manifest.as_str(),
    )
    .map_err(|error| ReadRoutePlanningError::StorageConstraint(error.to_string()))?;
    let constraint = StorageRangeConstraint::new(
        definition.range_id.as_str(),
        definition.generation,
        row_key_range,
        snapshot,
    )
    .map_err(|error| ReadRoutePlanningError::StorageConstraint(error.to_string()))?;
    let mut candidates = watermarks
        .iter()
        .filter(|watermark| {
            watermark.range_id == definition.range_id
                && watermark.generation == definition.generation
                && watermark.schema_manifest_id.as_ref() == Some(active_manifest)
                && watermark.index_epoch == index_epoch
                && watermark.applied_through_epoch >= read_point.data_epoch
                && watermark.retained_from_epoch <= read_point.data_epoch
                && directory.entries().iter().any(|entry| {
                    entry.range_id == definition.range_id
                        && entry.node_id == watermark.node_id
                        && entry.state == RangeReplicaReadinessState::Ready
                })
                && metadata
                    .range_replicas()
                    .get(&definition.range_id)
                    .and_then(|replicas| replicas.get(&watermark.node_id))
                    .is_some_and(|evidence| {
                        evidence.generation == definition.generation
                            && evidence.schema_manifest_id.as_ref() == Some(active_manifest)
                            && evidence.index_epoch == index_epoch
                            && evidence.data_epoch >= read_point.data_epoch
                    })
                && match consistency {
                    ReadConsistencyMode::Strong => watermark.current,
                    ReadConsistencyMode::Stale { max_age_ms } => {
                        now_ms.saturating_sub(watermark.observed_at_ms) <= max_age_ms
                    }
                }
        })
        .map(|watermark| watermark.node_id.clone())
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.dedup();
    if candidates.is_empty() {
        return Err(ReadRoutePlanningError::NoCompatibleTarget {
            range_id: definition.range_id.as_str().to_string(),
        });
    }
    Ok(RangeTarget {
        constraint,
        candidates,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClusterId, RangeCoverageProof, RangeReplicaEvidence, RangeReplicaLifecycle,
        SchemaRolloutState, TableRef,
    };
    use alopex_sql::distributed_read::{RemoteReadOperators, RemoteReadShape};

    fn descriptor() -> RemoteReadDescriptor {
        RemoteReadDescriptor {
            catalog_version: "v0.8".into(),
            table: "users".into(),
            shape: RemoteReadShape::Rows,
            operators: RemoteReadOperators::default(),
        }
    }

    fn range(id: &str, lower: Option<u64>, upper: Option<u64>) -> RangeRoutingDefinition {
        RangeRoutingDefinition {
            range_id: RangeId::new(id),
            table_ref: TableRef::new("default.public.users"),
            table_id: 7,
            lower_inclusive: lower.map(|row_id| CanonicalRowKey::new(7, row_id).encode()),
            upper_exclusive: upper.map(|row_id| CanonicalRowKey::new(7, row_id).encode()),
            generation: 3,
        }
    }

    fn fixture() -> (CommittedMetadata, Vec<RangeReplicaReadWatermark>) {
        let mut metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        let manifest = SchemaManifestId::new("schema-1");
        *metadata.schema_mut_for_apply() = SchemaRolloutState {
            active_manifest: Some(manifest.clone()),
            ..SchemaRolloutState::default()
        };
        let ranges = [
            range("range-a", None, Some(50)),
            range("range-b", Some(50), None),
        ];
        let mut watermarks = Vec::new();
        for definition in ranges {
            metadata.record_range_for_apply(definition.clone());
            for node in [NodeId::new("node-a"), NodeId::new("node-b")] {
                metadata.record_replica_for_apply(RangeReplicaEvidence {
                    range_id: definition.range_id.clone(),
                    node_id: node.clone(),
                    generation: 3,
                    schema_manifest_id: Some(manifest.clone()),
                    data_epoch: 12,
                    index_epoch: 8,
                    lifecycle: RangeReplicaLifecycle::Ready,
                    coverage: Some(RangeCoverageProof {
                        generation: 3,
                        lower_inclusive: definition.lower_inclusive.clone(),
                        upper_exclusive: definition.upper_exclusive.clone(),
                        data_epoch: 12,
                        index_epoch: 8,
                        content_hash: format!("{}-{node:?}", definition.range_id.as_str()),
                    }),
                });
                watermarks.push(RangeReplicaReadWatermark {
                    range_id: definition.range_id.clone(),
                    node_id: node,
                    generation: 3,
                    applied_through_epoch: 12,
                    retained_from_epoch: 10,
                    schema_manifest_id: Some(manifest.clone()),
                    index_epoch: 8,
                    observed_at_ms: 1_000,
                    current: true,
                });
            }
        }
        (metadata, watermarks)
    }

    fn request(mode: ReadModeRequest) -> ReadRoutePlanRequest {
        ReadRoutePlanRequest {
            descriptor: descriptor(),
            table_id: 7,
            request_id: RequestId::new("read-1"),
            requested_mode: mode,
            inherited_stale_max_age_ms: 100,
        }
    }

    #[test]
    fn plans_complete_multi_range_read_with_only_same_fence_candidates() {
        let (metadata, watermarks) = fixture();
        let plan = ReadRoutePlanner::default()
            .plan(
                &metadata,
                request(ReadModeRequest::Inherit),
                &watermarks,
                1_000,
            )
            .unwrap();

        assert_eq!(plan.decision, RouteDecision::RemoteMultiRange);
        assert_eq!(plan.fence.read_point.data_epoch, 12);
        assert_eq!(plan.ranges.len(), 2);
        assert!(plan.ranges.iter().all(|range| range.candidates.len() == 2));
        assert!(
            plan.ranges
                .iter()
                .all(|range| range.constraint.snapshot().read_at().data_epoch
                    == plan.fence.read_point.data_epoch)
        );
    }

    #[test]
    fn failover_never_uses_a_candidate_outside_the_existing_fence() {
        let (metadata, mut watermarks) = fixture();
        let mut stale = watermarks[1].clone();
        stale.index_epoch = 9;
        stale.node_id = NodeId::new("node-incompatible");
        watermarks.push(stale);
        let plan = ReadRoutePlanner::default()
            .plan(
                &metadata,
                request(ReadModeRequest::Strong),
                &watermarks,
                1_000,
            )
            .unwrap();

        assert_eq!(
            plan.compatible_failover(&RangeId::new("range-a"), &NodeId::new("node-a")),
            Some(&NodeId::new("node-b"))
        );
        assert!(
            !plan.ranges[0]
                .candidates
                .contains(&NodeId::new("node-incompatible"))
        );
    }

    #[test]
    fn incomplete_coverage_is_rejected_before_read_point_or_dispatch() {
        let (mut metadata, watermarks) = fixture();
        metadata.record_range_for_apply(range("range-gap", Some(60), None));

        assert!(matches!(
            ReadRoutePlanner::default().plan(
                &metadata,
                request(ReadModeRequest::Strong),
                &watermarks,
                1_000,
            ),
            Err(ReadRoutePlanningError::IncompleteOrOverlappingCoverage(7))
        ));
    }

    #[test]
    fn policy_rejection_has_no_local_route_result() {
        let (metadata, watermarks) = fixture();
        let request = request(ReadModeRequest::Stale { max_age_ms: 100 });
        assert!(matches!(
            ReadRoutePlanner::default().plan(&metadata, request, &watermarks, 1_000),
            Err(ReadRoutePlanningError::ModeNotPermitted)
        ));
    }
}
