use alopex_cluster::{
    CatalogTableRef, CatalogTableSnapshot, ClusterManager, ClusterStatusSnapshot, PlanId,
    QueryRouter, QueryRoutingRequest, QueryTableReference, QueryTableReferenceAccess,
    QueryTableReferenceSource, RoutingDecisionKind, RoutingDiagnostics, StableDiagnosticCode,
    TableRef,
};
use alopex_sql::catalog::{Catalog, TableMetadata};
use alopex_sql::planner::{plan_statement_for_routing, TableReferenceAccess, TableReferenceSource};
use alopex_sql::Statement;

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
