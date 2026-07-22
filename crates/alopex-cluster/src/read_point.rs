//! Cluster read-point admission from committed metadata and journal watermarks.
//!
//! A node-local transaction timestamp is never accepted here.  The caller must
//! supply a watermark produced after verified journal apply; missing evidence
//! is an unavailable result rather than an optimistic local fallback.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    CommittedMetadata, NodeId, RangeId, RangeReplicaDirectory, RangeReplicaReadinessState,
    SchemaManifestId,
};

/// Requested distributed read consistency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistencyMode {
    /// Every selected watermark must prove the current committed journal prefix.
    Strong,
    /// A common retained prefix may be used if it is no older than this bound.
    Stale { max_age_ms: u64 },
}

/// Immutable journal-apply evidence for one physical range replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeReplicaReadWatermark {
    /// Logical range covered by this replica.
    pub range_id: RangeId,
    /// Replica that authenticated and applied the journal prefix.
    pub node_id: NodeId,
    /// Immutable range generation.
    pub generation: u64,
    /// Global user-data epoch applied through this replica.
    pub applied_through_epoch: u64,
    /// Oldest global epoch still retained by this replica.
    pub retained_from_epoch: u64,
    /// Schema manifest visible at the applied prefix.
    pub schema_manifest_id: Option<SchemaManifestId>,
    /// Index definition epoch visible at the applied prefix.
    pub index_epoch: u64,
    /// Wall-clock evidence time, used only for bounded-stale freshness.
    pub observed_at_ms: u64,
    /// Whether the authority proved this prefix current for strong admission.
    pub current: bool,
}

/// Input used to issue one common read point across logical ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPointRequest {
    /// Ranges that must share the same data epoch.
    pub ranges: BTreeSet<RangeId>,
    /// Requested consistency contract.
    pub consistency: ReadConsistencyMode,
}

/// Fenced common read point approved for dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterReadPoint {
    /// Shared retained global user-data epoch.
    pub data_epoch: u64,
    /// Committed metadata state used for range selection.
    pub metadata_version: u64,
    /// Active schema manifest at the fence.
    pub schema_manifest_id: Option<SchemaManifestId>,
    /// Per-range immutable generations at the fence.
    pub range_generations: BTreeMap<RangeId, u64>,
    /// Per-range index epochs at the fence.
    pub index_epochs: BTreeMap<RangeId, u64>,
    /// Effective consistency mode.
    pub consistency: ReadConsistencyMode,
}

/// A classified reason no safe common point can be issued.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadPointFailure {
    /// No target range was requested.
    #[error("read point requires at least one range")]
    EmptyRangeSet,
    /// Metadata no longer contains a requested range.
    #[error("range is unavailable in committed metadata: {0}")]
    RangeUnavailable(String),
    /// No ready replica has compatible verified journal evidence.
    #[error("read point unavailable for range {range_id}: {reason}")]
    Unavailable { range_id: String, reason: String },
    /// No global epoch is retained by all requested ranges.
    #[error("no common retained read epoch")]
    NoCommonEpoch,
}

/// Issues only read points justified by committed metadata and supplied
/// journal/apply evidence.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClusterReadPointAuthority;

impl ClusterReadPointAuthority {
    /// Selects the newest common retained epoch. `now_ms` is used only to
    /// evaluate an explicit bounded-stale request, never as a data epoch.
    pub fn issue(
        &self,
        metadata: &CommittedMetadata,
        request: &ReadPointRequest,
        watermarks: &[RangeReplicaReadWatermark],
        now_ms: u64,
    ) -> Result<ClusterReadPoint, ReadPointFailure> {
        if request.ranges.is_empty() {
            return Err(ReadPointFailure::EmptyRangeSet);
        }
        let directory = RangeReplicaDirectory::from_committed(metadata);
        let active_manifest = metadata.schema().active_manifest.as_ref();
        let mut candidates = Vec::new();
        let mut generations = BTreeMap::new();
        let mut indexes = BTreeMap::new();

        for range_id in &request.ranges {
            let definition = metadata
                .ranges()
                .get(range_id)
                .ok_or_else(|| ReadPointFailure::RangeUnavailable(range_id.as_str().to_string()))?;
            let compatible: Vec<_> = watermarks
                .iter()
                .filter(|watermark| {
                    watermark.range_id == *range_id
                        && watermark.generation == definition.generation
                        && watermark.schema_manifest_id.as_ref() == active_manifest
                        && directory.entries().iter().any(|entry| {
                            entry.range_id == *range_id
                                && entry.node_id == watermark.node_id
                                && entry.state == RangeReplicaReadinessState::Ready
                        })
                        && metadata
                            .range_replicas()
                            .get(range_id)
                            .and_then(|replicas| replicas.get(&watermark.node_id))
                            .is_some_and(|evidence| {
                                evidence.data_epoch >= watermark.applied_through_epoch
                                    && evidence.index_epoch == watermark.index_epoch
                            })
                        && match request.consistency {
                            ReadConsistencyMode::Strong => watermark.current,
                            ReadConsistencyMode::Stale { max_age_ms } => {
                                now_ms.saturating_sub(watermark.observed_at_ms) <= max_age_ms
                            }
                        }
                })
                .collect();
            let Some(best) = compatible
                .iter()
                .max_by_key(|watermark| watermark.applied_through_epoch)
            else {
                return Err(ReadPointFailure::Unavailable {
                    range_id: range_id.as_str().to_string(),
                    reason: "no ready replica has a matching retained journal watermark".into(),
                });
            };
            candidates.push((*best).clone());
            generations.insert(range_id.clone(), definition.generation);
            indexes.insert(range_id.clone(), best.index_epoch);
        }

        let epoch = candidates
            .iter()
            .map(|candidate| candidate.applied_through_epoch)
            .min()
            .ok_or(ReadPointFailure::NoCommonEpoch)?;
        if candidates
            .iter()
            .any(|candidate| candidate.retained_from_epoch > epoch)
        {
            return Err(ReadPointFailure::NoCommonEpoch);
        }
        Ok(ClusterReadPoint {
            data_epoch: epoch,
            metadata_version: metadata.state_version(),
            schema_manifest_id: active_manifest.cloned(),
            range_generations: generations,
            index_epochs: indexes,
            consistency: request.consistency,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClusterId, RangeCoverageProof, RangeReplicaEvidence, RangeReplicaLifecycle,
        RangeRoutingDefinition, TableRef,
    };

    fn fixture() -> (CommittedMetadata, RangeId, NodeId) {
        let range_id = RangeId::new("range-a");
        let node_id = NodeId::new("node-a");
        let mut metadata = CommittedMetadata::new(ClusterId::new("cluster-a"));
        metadata.record_range_for_apply(RangeRoutingDefinition {
            range_id: range_id.clone(),
            table_ref: TableRef::new("default.public.users"),
            table_id: 7,
            lower_inclusive: None,
            upper_exclusive: None,
            generation: 3,
        });
        metadata.record_replica_for_apply(RangeReplicaEvidence {
            range_id: range_id.clone(),
            node_id: node_id.clone(),
            generation: 3,
            schema_manifest_id: None,
            data_epoch: 12,
            index_epoch: 8,
            lifecycle: RangeReplicaLifecycle::Ready,
            coverage: Some(RangeCoverageProof {
                generation: 3,
                lower_inclusive: None,
                upper_exclusive: None,
                data_epoch: 12,
                index_epoch: 8,
                content_hash: "verified".into(),
            }),
        });
        (metadata, range_id, node_id)
    }

    fn watermark(range_id: RangeId, node_id: NodeId) -> RangeReplicaReadWatermark {
        RangeReplicaReadWatermark {
            range_id,
            node_id,
            generation: 3,
            applied_through_epoch: 12,
            retained_from_epoch: 10,
            schema_manifest_id: None,
            index_epoch: 8,
            observed_at_ms: 1_000,
            current: true,
        }
    }

    #[test]
    fn issues_a_common_strong_point_only_from_ready_verified_evidence() {
        let (metadata, range_id, node_id) = fixture();
        let request = ReadPointRequest {
            ranges: [range_id.clone()].into(),
            consistency: ReadConsistencyMode::Strong,
        };
        let point = ClusterReadPointAuthority
            .issue(
                &metadata,
                &request,
                &[watermark(range_id.clone(), node_id)],
                1_000,
            )
            .unwrap();
        assert_eq!(point.data_epoch, 12);
        assert_eq!(point.range_generations[&range_id], 3);
        assert_eq!(point.index_epochs[&range_id], 8);
    }

    #[test]
    fn missing_or_unretained_watermark_is_classified_before_dispatch() {
        let (metadata, range_id, node_id) = fixture();
        let request = ReadPointRequest {
            ranges: [range_id.clone()].into(),
            consistency: ReadConsistencyMode::Strong,
        };
        assert!(matches!(
            ClusterReadPointAuthority.issue(&metadata, &request, &[], 1_000),
            Err(ReadPointFailure::Unavailable { .. })
        ));
        let mut expired = watermark(range_id, node_id);
        expired.retained_from_epoch = 13;
        assert!(matches!(
            ClusterReadPointAuthority.issue(&metadata, &request, &[expired], 1_000),
            Err(ReadPointFailure::NoCommonEpoch)
        ));
    }
}
