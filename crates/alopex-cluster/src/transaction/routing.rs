use std::collections::BTreeMap;

use alopex_core::CanonicalRowKey;

use crate::{
    CommittedMetadata, FailureClass, Placement, PlacementReadiness, PlacementRole, RangeDirectory,
    RangeDirectoryError, RangeIdentity, RangeReplicaDirectory, RoutingOutcome, RoutingOutcomeKind,
    TableId, TransactionParticipant,
};

/// A transaction-routing target accepted before any participant is opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionRouteTarget {
    /// Route every supplied canonical primary-row key independently.
    Keys(Vec<Vec<u8>>),
    /// Enlist every committed range for one logical table.
    Table(TableId),
}

/// Input used to freeze an immutable transaction routing view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionRouteRequest {
    pub expected_metadata_version: u64,
    pub target: TransactionRouteTarget,
}

/// The committed routing snapshot used by all later participant operations.
///
/// This value has no mutation API.  A coordinator must call
/// [`Self::ensure_current`] before accepting a newly discovered write target;
/// it must not re-resolve it against a newer placement in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionRoutePlan {
    pub metadata_version: u64,
    pub participants: Vec<TransactionParticipant>,
    pub routing: RoutingOutcome,
}

impl TransactionRoutePlan {
    pub fn ensure_current(
        &self,
        metadata: &CommittedMetadata,
    ) -> Result<(), TransactionRoutingError> {
        if metadata.state_version() == self.metadata_version {
            Ok(())
        } else {
            Err(TransactionRoutingError::RoutingViewChanged {
                planned_version: self.metadata_version,
                current_version: metadata.state_version(),
            })
        }
    }
}

/// Deterministic pre-write routing failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransactionRoutingError {
    #[error("expected committed metadata version {expected}, got {actual}")]
    StaleMetadata { expected: u64, actual: u64 },
    #[error(
        "transaction routing view changed from metadata version {planned_version} to {current_version}"
    )]
    RoutingViewChanged {
        planned_version: u64,
        current_version: u64,
    },
    #[error("committed range directory is invalid: {0}")]
    RangeDirectory(#[from] RangeDirectoryError),
    #[error("transaction route needs at least one key")]
    EmptyKeyTarget,
    #[error("canonical transaction key is invalid")]
    InvalidKey,
    #[error("no committed range covers transaction key for table {table_id}")]
    KeyUnmapped { table_id: TableId },
    #[error("no committed ranges cover table {table_id}")]
    TableUnmapped { table_id: TableId },
    #[error("range {range_id} has no complete committed ready placement")]
    IncompletePlacement { range_id: String },
}

impl TransactionRoutingError {
    pub const fn failure_class(&self) -> FailureClass {
        match self {
            Self::StaleMetadata { .. } => FailureClass::StaleMetadata,
            Self::RoutingViewChanged { .. } => FailureClass::EpochMismatch,
            Self::RangeDirectory(RangeDirectoryError::Gap { .. }) => FailureClass::Gap,
            Self::RangeDirectory(RangeDirectoryError::Overlap { .. }) => FailureClass::Overlap,
            Self::RangeDirectory(_) => FailureClass::StaleMetadata,
            Self::EmptyKeyTarget | Self::InvalidKey => FailureClass::InvalidRequest,
            Self::KeyUnmapped { .. } | Self::TableUnmapped { .. } => FailureClass::Gap,
            Self::IncompletePlacement { .. } => FailureClass::PrerequisiteMissing,
        }
    }

    pub const fn reason_code(&self) -> &'static str {
        match self {
            Self::StaleMetadata { .. } => "metadata_version_mismatch",
            Self::RoutingViewChanged { .. } => "transaction_routing_view_changed",
            Self::RangeDirectory(RangeDirectoryError::Gap { .. }) => "range_gap",
            Self::RangeDirectory(RangeDirectoryError::Overlap { .. }) => "range_overlap",
            Self::RangeDirectory(_) => "invalid_range_directory",
            Self::EmptyKeyTarget => "empty_transaction_key_target",
            Self::InvalidKey => "invalid_transaction_key",
            Self::KeyUnmapped { .. } => "range_not_configured",
            Self::TableUnmapped { .. } => "table_range_not_configured",
            Self::IncompletePlacement { .. } => "range_placement_incomplete",
        }
    }
}

/// Freezes transaction participants from one immutable committed metadata view.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransactionRoutingPlanner;

impl TransactionRoutingPlanner {
    pub fn plan(
        &self,
        metadata: &CommittedMetadata,
        request: &TransactionRouteRequest,
    ) -> Result<TransactionRoutePlan, TransactionRoutingError> {
        if metadata.state_version() != request.expected_metadata_version {
            return Err(TransactionRoutingError::StaleMetadata {
                expected: request.expected_metadata_version,
                actual: metadata.state_version(),
            });
        }
        let directory = RangeDirectory::from_committed(metadata)?;
        let definitions = resolve_definitions(&directory, &request.target)?;
        let replica_directory = RangeReplicaDirectory::from_committed(metadata);
        let participants = definitions
            .into_iter()
            .map(|definition| participant(metadata, &replica_directory, definition))
            .collect::<Result<Vec<_>, _>>()?;
        let routing = match participants.as_slice() {
            [only] => RoutingOutcome::new(
                RoutingOutcomeKind::SingleRange,
                Some(only.range.clone()),
                metadata.state_version(),
                "transaction_single_range_route",
            ),
            _ => RoutingOutcome::new(
                RoutingOutcomeKind::MultiRange,
                None,
                metadata.state_version(),
                "transaction_multi_range_route",
            ),
        };
        Ok(TransactionRoutePlan {
            metadata_version: metadata.state_version(),
            participants,
            routing,
        })
    }
}

fn resolve_definitions<'a>(
    directory: &'a RangeDirectory,
    target: &TransactionRouteTarget,
) -> Result<Vec<&'a crate::RangeRoutingDefinition>, TransactionRoutingError> {
    let mut selected = BTreeMap::new();
    match target {
        TransactionRouteTarget::Keys(keys) => {
            if keys.is_empty() {
                return Err(TransactionRoutingError::EmptyKeyTarget);
            }
            for key in keys {
                let decoded = CanonicalRowKey::decode(key)
                    .map_err(|_| TransactionRoutingError::InvalidKey)?;
                let definition =
                    directory
                        .lookup(key)
                        .ok_or(TransactionRoutingError::KeyUnmapped {
                            table_id: decoded.table_id(),
                        })?;
                if definition.table_id != decoded.table_id() {
                    return Err(TransactionRoutingError::KeyUnmapped {
                        table_id: decoded.table_id(),
                    });
                }
                selected.insert(definition.range_id.clone(), definition);
            }
        }
        TransactionRouteTarget::Table(table_id) => {
            for definition in directory
                .ranges()
                .values()
                .filter(|range| range.table_id == *table_id)
            {
                selected.insert(definition.range_id.clone(), definition);
            }
            if selected.is_empty() {
                return Err(TransactionRoutingError::TableUnmapped {
                    table_id: *table_id,
                });
            }
        }
    }
    Ok(selected.into_values().collect())
}

fn participant(
    metadata: &CommittedMetadata,
    replicas: &RangeReplicaDirectory,
    definition: &crate::RangeRoutingDefinition,
) -> Result<TransactionParticipant, TransactionRoutingError> {
    let ready = replicas.routing_eligible(&definition.range_id);
    let Some(owner) = ready.first() else {
        return Err(TransactionRoutingError::IncompletePlacement {
            range_id: definition.range_id.as_str().to_string(),
        });
    };
    // `RangeReplicaDirectory` is derived only from committed readiness
    // evidence and is ordered by committed node identity.  The first ready
    // node is therefore the stable dispatch owner for this immutable plan;
    // it is not inferred from liveness or from an uncommitted placement.
    let replica_nodes = ready
        .iter()
        .skip(1)
        .map(|entry| entry.node_id.clone())
        .collect();
    let data_epoch = metadata
        .range_replicas()
        .get(&definition.range_id)
        .and_then(|evidence| evidence.get(&owner.node_id).map(|item| item.data_epoch))
        .ok_or_else(|| TransactionRoutingError::IncompletePlacement {
            range_id: definition.range_id.as_str().to_string(),
        })?;
    Ok(TransactionParticipant {
        range: RangeIdentity::new(
            metadata.cluster_id().clone(),
            definition.table_id,
            definition.range_id.clone(),
            definition.lower_inclusive.clone(),
            definition.upper_exclusive.clone(),
            1,
            data_epoch,
        ),
        range_generation: definition.generation,
        placement: Placement::new(
            owner.node_id.clone(),
            replica_nodes,
            PlacementRole::Owner,
            PlacementReadiness::Ready,
            metadata.state_version(),
        ),
    })
}

#[cfg(test)]
mod tests {
    use alopex_core::CanonicalRowKey;

    use super::{
        TransactionRouteRequest, TransactionRouteTarget, TransactionRoutingError,
        TransactionRoutingPlanner,
    };
    use crate::{
        AuthorizationScope, ClusterId, CommittedMetadata, MetadataActor, MetadataCommand,
        MetadataCommandEnvelope, RangeCoverageProof, RangeId, RangeReplicaEvidence,
        RangeReplicaLifecycle, RangeRoutingDefinition, TableRef,
    };

    fn definition(
        range_id: &str,
        lower: Option<u64>,
        upper: Option<u64>,
    ) -> RangeRoutingDefinition {
        RangeRoutingDefinition {
            range_id: RangeId::from(range_id),
            table_ref: TableRef::from("default.public.users"),
            table_id: 7,
            lower_inclusive: lower.map(|key| CanonicalRowKey::new(7, key).encode()),
            upper_exclusive: upper.map(|key| CanonicalRowKey::new(7, key).encode()),
            generation: 1,
        }
    }

    fn ready(definition: &RangeRoutingDefinition, node: &str, epoch: u64) -> RangeReplicaEvidence {
        RangeReplicaEvidence {
            range_id: definition.range_id.clone(),
            node_id: node.into(),
            generation: definition.generation,
            schema_manifest_id: None,
            data_epoch: epoch,
            index_epoch: epoch,
            lifecycle: RangeReplicaLifecycle::Ready,
            coverage: Some(RangeCoverageProof {
                generation: definition.generation,
                lower_inclusive: definition.lower_inclusive.clone(),
                upper_exclusive: definition.upper_exclusive.clone(),
                data_epoch: epoch,
                index_epoch: epoch,
                content_hash: format!("{node}-{epoch}"),
            }),
        }
    }

    fn metadata(with_second_ready_replica: bool) -> CommittedMetadata {
        let mut metadata = CommittedMetadata::new(ClusterId::from("cluster-a"));
        let left = definition("left", None, Some(10));
        let right = definition("right", Some(10), None);
        metadata.record_range_for_apply(left.clone());
        metadata.record_range_for_apply(right.clone());
        metadata.record_replica_for_apply(ready(&left, "node-a", 11));
        if with_second_ready_replica {
            metadata.record_replica_for_apply(ready(&right, "node-b", 11));
        }
        metadata
    }

    #[test]
    fn keys_freeze_a_deduplicated_multi_range_committed_view() {
        let metadata = metadata(true);
        let plan = TransactionRoutingPlanner
            .plan(
                &metadata,
                &TransactionRouteRequest {
                    expected_metadata_version: 0,
                    target: TransactionRouteTarget::Keys(vec![
                        CanonicalRowKey::new(7, 1).encode(),
                        CanonicalRowKey::new(7, 11).encode(),
                        CanonicalRowKey::new(7, 12).encode(),
                    ]),
                },
            )
            .expect("committed ready ranges resolve before a write");
        assert_eq!(plan.participants.len(), 2);
        assert_eq!(plan.routing.kind, crate::RoutingOutcomeKind::MultiRange);
        plan.ensure_current(&metadata)
            .expect("same metadata view remains current");
    }

    #[test]
    fn stale_or_incomplete_metadata_is_rejected_before_enlistment() {
        let metadata = metadata(false);
        let stale = TransactionRoutingPlanner.plan(
            &metadata,
            &TransactionRouteRequest {
                expected_metadata_version: 1,
                target: TransactionRouteTarget::Table(7),
            },
        );
        assert!(matches!(
            stale,
            Err(TransactionRoutingError::StaleMetadata { .. })
        ));

        let incomplete = TransactionRoutingPlanner.plan(
            &metadata,
            &TransactionRouteRequest {
                expected_metadata_version: 0,
                target: TransactionRouteTarget::Table(7),
            },
        );
        assert!(matches!(
            incomplete,
            Err(TransactionRoutingError::IncompletePlacement { range_id }) if range_id == "right"
        ));
    }

    #[test]
    fn committed_range_movement_invalidates_the_frozen_plan_before_a_new_write() {
        let metadata = metadata(true);
        let plan = TransactionRoutingPlanner
            .plan(
                &metadata,
                &TransactionRouteRequest {
                    expected_metadata_version: 0,
                    target: TransactionRouteTarget::Table(7),
                },
            )
            .expect("initial committed range view is routable");
        let moved = metadata.apply_validated_for_consensus(&MetadataCommandEnvelope {
            request_id: "move-right".into(),
            request_fingerprint: "move-right-generation-2".to_string(),
            actor: MetadataActor::authorized_for("controller", AuthorizationScope::Range),
            expected_version: Some(0),
            command: MetadataCommand::UpdateRange {
                definition: RangeRoutingDefinition {
                    generation: 2,
                    ..definition("right", Some(10), None)
                },
            },
        });

        assert!(matches!(
            plan.ensure_current(&moved),
            Err(TransactionRoutingError::RoutingViewChanged {
                planned_version: 0,
                current_version: 1,
            })
        ));
    }
}
