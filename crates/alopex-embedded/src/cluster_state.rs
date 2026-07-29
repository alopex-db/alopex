use alopex_cluster::crdt::{
    CrdtCounterError, CrdtCounterProjection, CrdtOperationEnvelope, CrdtOperationKind, CrdtOutcome,
    CrdtSetError, CrdtSetProjection,
};
use alopex_cluster::{
    CatalogTableRef, CatalogTableSnapshot, ClusterManager, ClusterStatusSnapshot, PlanId,
    QueryRouter, QueryRoutingRequest, QueryTableReference, QueryTableReferenceAccess,
    QueryTableReferenceSource, RoutingDecisionKind, RoutingDiagnostics, StableDiagnosticCode,
    TableRef,
};
use alopex_cluster::{
    FailureClass, IdempotencyResult, OperationState, RoutingOutcome, RoutingOutcomeKind,
};
use alopex_core::kv::any::{AnyKVManager, AnyKVTransaction};
use alopex_core::kv::{AnyKV, KVStore};
use alopex_core::TxnMode;
use alopex_sql::catalog::{Catalog, TableMetadata};
use alopex_sql::planner::{plan_statement_for_routing, TableReferenceAccess, TableReferenceSource};
use alopex_sql::Statement;

/// A borrowing KV-store adapter lets the CRDT projection share the exact
/// embedded `AnyKV` transaction/WAL boundary without cloning a disk-backed
/// store or creating an independent persistence path.
struct EmbeddedCrdtStore<'store>(&'store AnyKV);

impl<'store> KVStore for EmbeddedCrdtStore<'store> {
    type Transaction<'txn>
        = AnyKVTransaction<'txn>
    where
        Self: 'txn;
    type Manager<'txn>
        = AnyKVManager<'txn>
    where
        Self: 'txn;

    fn txn_manager(&self) -> Self::Manager<'_> {
        self.0.txn_manager()
    }

    fn begin(&self, mode: TxnMode) -> alopex_core::Result<Self::Transaction<'_>> {
        self.0.begin(mode)
    }
}

pub(crate) struct EmbeddedClusterState {
    manager: ClusterManager,
    latest_routing_diagnostics: RoutingDiagnostics,
}

impl Default for EmbeddedClusterState {
    fn default() -> Self {
        let manager = ClusterManager::default();
        let mut latest_routing_diagnostics = RoutingDiagnostics::new(
            RoutingDecisionKind::LocalOnly,
            StableDiagnosticCode::PlanningInputUnavailable,
            "embedded.initial",
            manager.identity().update_epoch,
        );
        latest_routing_diagnostics
            .roles
            .push(manager.identity().role);
        Self {
            manager,
            latest_routing_diagnostics,
        }
    }
}

impl EmbeddedClusterState {
    /// Applies a Counter create only through the shared CRDT projection and
    /// returns its canonical common outcome. The standalone embedded database
    /// is intentionally local-only, so it does not assert a replica-quorum
    /// result or make a Chirps capability claim.
    pub(crate) fn create_counter(
        &mut self,
        store: &AnyKV,
        envelope: CrdtOperationEnvelope,
    ) -> CrdtOutcome {
        if envelope.operation != CrdtOperationKind::CounterCreate {
            return self.counter_rejection(
                &envelope,
                FailureClass::InvalidRequest,
                "counter_create_envelope_required",
            );
        }

        let projection = CrdtCounterProjection::new(EmbeddedCrdtStore(store));
        match projection.apply(&envelope, envelope.state_epoch) {
            Ok(result) => {
                let common = envelope.common_fields(
                    result.ledger.first_state,
                    result.ledger.first_failure_class,
                    self.counter_routing(
                        &envelope,
                        RoutingOutcomeKind::LocalOnly,
                        "embedded_local_only",
                    ),
                    false,
                    result.ledger.idempotency_result(),
                );
                CrdtOutcome::counter(common, result.value)
            }
            Err(error) => self.counter_projection_failure(&envelope, error),
        }
    }

    /// Applies a Counter increment through the same durable projection and
    /// local-only outcome boundary as Counter create.
    pub(crate) fn increment_counter(
        &mut self,
        store: &AnyKV,
        envelope: CrdtOperationEnvelope,
    ) -> CrdtOutcome {
        if envelope.operation != CrdtOperationKind::CounterIncrement {
            return self.counter_rejection(
                &envelope,
                FailureClass::InvalidRequest,
                "counter_increment_envelope_required",
            );
        }
        let projection = CrdtCounterProjection::new(EmbeddedCrdtStore(store));
        match projection.apply(&envelope, envelope.state_epoch) {
            Ok(result) => {
                let common = envelope.common_fields(
                    result.ledger.first_state,
                    result.ledger.first_failure_class,
                    self.counter_routing(
                        &envelope,
                        RoutingOutcomeKind::LocalOnly,
                        "embedded_local_only",
                    ),
                    false,
                    result.ledger.idempotency_result(),
                );
                CrdtOutcome::counter(common, result.value)
            }
            Err(error) => self.counter_projection_failure(&envelope, error),
        }
    }

    /// Applies a Counter decrement through the same durable projection and
    /// local-only outcome boundary as Counter increment.
    pub(crate) fn decrement_counter(
        &mut self,
        store: &AnyKV,
        envelope: CrdtOperationEnvelope,
    ) -> CrdtOutcome {
        if envelope.operation != CrdtOperationKind::CounterDecrement {
            return self.counter_rejection(
                &envelope,
                FailureClass::InvalidRequest,
                "counter_decrement_envelope_required",
            );
        }
        let projection = CrdtCounterProjection::new(EmbeddedCrdtStore(store));
        match projection.apply(&envelope, envelope.state_epoch) {
            Ok(result) => {
                let common = envelope.common_fields(
                    result.ledger.first_state,
                    result.ledger.first_failure_class,
                    self.counter_routing(
                        &envelope,
                        RoutingOutcomeKind::LocalOnly,
                        "embedded_local_only",
                    ),
                    false,
                    result.ledger.idempotency_result(),
                );
                CrdtOutcome::counter(common, result.value)
            }
            Err(error) => self.counter_projection_failure(&envelope, error),
        }
    }

    /// Creates a Set through the shared durable projection and returns the
    /// canonical local-only outcome for the standalone embedded database.
    pub(crate) fn create_set(
        &mut self,
        store: &AnyKV,
        envelope: CrdtOperationEnvelope,
    ) -> CrdtOutcome {
        if envelope.operation != CrdtOperationKind::SetCreate {
            return self.set_rejection(
                &envelope,
                FailureClass::InvalidRequest,
                "set_create_envelope_required",
            );
        }

        let projection = CrdtSetProjection::new(EmbeddedCrdtStore(store));
        match projection.apply(&envelope, envelope.state_epoch) {
            Ok(result) => {
                let common = envelope.common_fields(
                    result.ledger.first_state,
                    result.ledger.first_failure_class,
                    self.set_routing(
                        &envelope,
                        RoutingOutcomeKind::LocalOnly,
                        "embedded_local_only",
                    ),
                    false,
                    result.ledger.idempotency_result(),
                );
                CrdtOutcome::set(common, result.value)
            }
            Err(error) => self.set_projection_failure(&envelope, error),
        }
    }

    /// Reads a Counter projection without admitting a new ledger record.
    ///
    /// The standalone embedded database is intentionally local-only. A read
    /// therefore reports the same canonical identity/routing outcome as a
    /// local mutation while preserving the projection's read-only boundary.
    pub(crate) fn read_counter(
        &self,
        store: &AnyKV,
        envelope: CrdtOperationEnvelope,
    ) -> CrdtOutcome {
        if envelope.operation != CrdtOperationKind::CounterRead {
            return self.counter_rejection(
                &envelope,
                FailureClass::InvalidRequest,
                "counter_read_envelope_required",
            );
        }

        let projection = CrdtCounterProjection::new(EmbeddedCrdtStore(store));
        match projection.read(&envelope) {
            Ok(value) => {
                let common = envelope.common_fields(
                    OperationState::Committed,
                    None,
                    self.counter_routing(
                        &envelope,
                        RoutingOutcomeKind::LocalOnly,
                        "embedded_local_only",
                    ),
                    false,
                    IdempotencyResult {
                        operation_id: envelope.operation_id.clone(),
                        request_id: envelope.request_id.clone(),
                        first_outcome: "counter_read".to_string(),
                        state: OperationState::Committed,
                        duplicate_count: 0,
                    },
                );
                CrdtOutcome::counter(common, value)
            }
            Err(CrdtCounterError::MissingProjection { .. }) => self.counter_rejection(
                &envelope,
                FailureClass::PrerequisiteMissing,
                "counter_not_found",
            ),
            Err(error) => self.counter_projection_failure(&envelope, error),
        }
    }

    pub(crate) fn status_snapshot(&self, catalog_epoch: u64) -> ClusterStatusSnapshot {
        let mut snapshot = self.manager.status_snapshot();
        let epoch = snapshot
            .identity
            .update_epoch
            .max(snapshot.membership.update_epoch)
            .max(snapshot.placement.update_epoch)
            .max(catalog_epoch);
        snapshot.identity.update_epoch = epoch;
        snapshot.membership.update_epoch = epoch;
        snapshot.placement.update_epoch = epoch;
        snapshot
    }

    pub(crate) fn routing_diagnostics(&self, catalog_epoch: u64) -> RoutingDiagnostics {
        let mut diagnostics = self.latest_routing_diagnostics.clone();
        diagnostics.update_epoch = diagnostics.update_epoch.max(catalog_epoch);
        diagnostics
    }

    pub(crate) fn record_routing<C: Catalog + ?Sized>(
        &mut self,
        catalog: &C,
        statement: &Statement,
        statement_index: usize,
        catalog_epoch: u64,
    ) {
        let plan_id = format!("embedded.statement.{statement_index}");
        let Ok(planned) = plan_statement_for_routing(catalog, statement) else {
            self.latest_routing_diagnostics = self.unavailable_diagnostics(plan_id, catalog_epoch);
            return;
        };

        let catalog_snapshot = catalog_snapshot(catalog, catalog_epoch);
        let table_references = planned
            .table_references()
            .iter()
            .map(|reference| {
                QueryTableReference::new(
                    resolve_table_ref(&reference.table_name, &catalog_snapshot),
                    query_access(reference.access),
                    query_source(reference.source),
                )
            })
            .collect();
        let request = QueryRoutingRequest::new(plan_id, catalog_snapshot, table_references);
        let membership = self.manager.membership_view();
        self.latest_routing_diagnostics =
            QueryRouter::new(self.manager.placement_catalog(), &membership).route(request);
    }

    fn unavailable_diagnostics(&self, plan_id: String, catalog_epoch: u64) -> RoutingDiagnostics {
        let mut diagnostics = RoutingDiagnostics::new(
            RoutingDecisionKind::LocalOnly,
            StableDiagnosticCode::PlanningInputUnavailable,
            PlanId::new(plan_id),
            self.manager.identity().update_epoch.max(catalog_epoch),
        );
        diagnostics.roles.push(self.manager.identity().role);
        diagnostics
    }

    fn counter_projection_failure(
        &self,
        envelope: &CrdtOperationEnvelope,
        error: CrdtCounterError,
    ) -> CrdtOutcome {
        let (failure_class, reason) = match error {
            CrdtCounterError::AlreadyExists { .. } => {
                (FailureClass::Conflict, "counter_already_exists")
            }
            CrdtCounterError::InvalidCounterPayload | CrdtCounterError::WrongOperation { .. } => {
                (FailureClass::InvalidRequest, "counter_payload_invalid")
            }
            CrdtCounterError::ArithmeticOverflow => {
                (FailureClass::InvalidRequest, "counter_value_out_of_range")
            }
            CrdtCounterError::MissingProjection { .. }
            | CrdtCounterError::Ledger(_)
            | CrdtCounterError::Storage(_)
            | CrdtCounterError::Encode(_)
            | CrdtCounterError::Decode(_) => (FailureClass::Internal, "counter_projection_failed"),
        };
        self.counter_rejection(envelope, failure_class, reason)
    }

    fn counter_rejection(
        &self,
        envelope: &CrdtOperationEnvelope,
        failure_class: FailureClass,
        reason: &str,
    ) -> CrdtOutcome {
        let common = envelope.common_fields(
            OperationState::Rejected,
            Some(failure_class),
            self.counter_routing(envelope, RoutingOutcomeKind::Blocked, reason),
            false,
            IdempotencyResult {
                operation_id: envelope.operation_id.clone(),
                request_id: envelope.request_id.clone(),
                first_outcome: reason.to_string(),
                state: OperationState::Rejected,
                duplicate_count: 0,
            },
        );
        CrdtOutcome::counter_unavailable(common, reason)
    }

    fn counter_routing(
        &self,
        envelope: &CrdtOperationEnvelope,
        kind: RoutingOutcomeKind,
        reason: impl Into<String>,
    ) -> RoutingOutcome {
        RoutingOutcome::new(
            kind,
            Some(envelope.range.clone()),
            self.manager.identity().update_epoch,
            reason,
        )
    }

    fn set_projection_failure(
        &self,
        envelope: &CrdtOperationEnvelope,
        error: CrdtSetError,
    ) -> CrdtOutcome {
        let (failure_class, reason) = match error {
            CrdtSetError::AlreadyExists { .. } => (FailureClass::Conflict, "set_already_exists"),
            CrdtSetError::InvalidSetPayload
            | CrdtSetError::NonCanonicalMember
            | CrdtSetError::NonCanonicalOperationId
            | CrdtSetError::WrongOperation { .. } => {
                (FailureClass::InvalidRequest, "set_payload_invalid")
            }
            CrdtSetError::ResourceLimit { .. } => (FailureClass::InvalidRequest, "resource_limit"),
            CrdtSetError::EpochMismatch { .. } => (FailureClass::EpochMismatch, "epoch_mismatch"),
            CrdtSetError::MissingProjection { .. }
            | CrdtSetError::Ledger(_)
            | CrdtSetError::Storage(_)
            | CrdtSetError::Encode(_)
            | CrdtSetError::Decode(_) => (FailureClass::Internal, "set_projection_failed"),
        };
        self.set_rejection(envelope, failure_class, reason)
    }

    fn set_rejection(
        &self,
        envelope: &CrdtOperationEnvelope,
        failure_class: FailureClass,
        reason: &str,
    ) -> CrdtOutcome {
        let common = envelope.common_fields(
            OperationState::Rejected,
            Some(failure_class),
            self.set_routing(envelope, RoutingOutcomeKind::Blocked, reason),
            false,
            IdempotencyResult {
                operation_id: envelope.operation_id.clone(),
                request_id: envelope.request_id.clone(),
                first_outcome: reason.to_string(),
                state: OperationState::Rejected,
                duplicate_count: 0,
            },
        );
        CrdtOutcome::set_unavailable(common, reason)
    }

    fn set_routing(
        &self,
        envelope: &CrdtOperationEnvelope,
        kind: RoutingOutcomeKind,
        reason: impl Into<String>,
    ) -> RoutingOutcome {
        RoutingOutcome::new(
            kind,
            Some(envelope.range.clone()),
            self.manager.identity().update_epoch,
            reason,
        )
    }
}

fn catalog_snapshot<C: Catalog + ?Sized>(catalog: &C, update_epoch: u64) -> CatalogTableSnapshot {
    CatalogTableSnapshot::from_tables(
        update_epoch,
        catalog
            .list_tables()
            .iter()
            .map(|table| CatalogTableRef::new(table_fqn(table), table.table_id))
            .collect(),
    )
}

fn resolve_table_ref(table_name: &str, snapshot: &CatalogTableSnapshot) -> TableRef {
    snapshot
        .tables
        .iter()
        .find(|table| {
            table.table_ref.as_str() == table_name
                || table.table_ref.as_str().rsplit('.').next() == Some(table_name)
        })
        .map(|table| table.table_ref.clone())
        .unwrap_or_else(|| TableRef::new(format!("default.default.{table_name}")))
}

fn table_fqn(table: &TableMetadata) -> String {
    format!(
        "{}.{}.{}",
        table.catalog_name, table.namespace_name, table.name
    )
}

fn query_access(access: TableReferenceAccess) -> QueryTableReferenceAccess {
    match access {
        TableReferenceAccess::Read => QueryTableReferenceAccess::Read,
        TableReferenceAccess::Write => QueryTableReferenceAccess::Write,
        TableReferenceAccess::Create => QueryTableReferenceAccess::Create,
        TableReferenceAccess::Drop => QueryTableReferenceAccess::Drop,
        TableReferenceAccess::Metadata => QueryTableReferenceAccess::Metadata,
    }
}

fn query_source(source: TableReferenceSource) -> QueryTableReferenceSource {
    match source {
        TableReferenceSource::TopLevelPlanTableName => {
            QueryTableReferenceSource::TopLevelPlanTableName
        }
        TableReferenceSource::LogicalPlanScan => QueryTableReferenceSource::LogicalPlanScan,
        TableReferenceSource::LogicalPlanMutationTarget => {
            QueryTableReferenceSource::LogicalPlanMutationTarget
        }
        TableReferenceSource::LogicalPlanDdlTarget => {
            QueryTableReferenceSource::LogicalPlanDdlTarget
        }
        TableReferenceSource::LogicalPlanIndexTarget => {
            QueryTableReferenceSource::LogicalPlanIndexTarget
        }
        TableReferenceSource::TypedExprSubquery => QueryTableReferenceSource::TypedExprSubquery,
    }
}
