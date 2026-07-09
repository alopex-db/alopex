use alopex_core::kv::memory::MemoryKV;
use alopex_sql::ast::ddl::{ColumnDef, CreateTable, DataType};
use alopex_sql::ast::dml::{FromItem, JoinType, Select, SelectItem};
use alopex_sql::catalog::persistent::TableFqn;
use alopex_sql::catalog::{
    Catalog, CatalogOverlay, ColumnMetadata, MemoryCatalog, PersistentCatalog, TableMetadata,
    TxnCatalogView,
};
use alopex_sql::planner::{
    PlanningDiagnosticSeverity, TableReferenceAccess, TableReferenceSource, plan_sql_for_routing,
    plan_statement_for_routing,
};
use alopex_sql::{ResolvedType, Span, Statement, StatementKind};

fn table(name: &str) -> TableMetadata {
    TableMetadata::new(name, vec![ColumnMetadata::new("id", ResolvedType::Integer)])
}

fn select_from(name: &str) -> Statement {
    Statement {
        kind: StatementKind::Select(Select {
            distinct: false,
            projection: vec![SelectItem::Wildcard {
                span: Span::empty(),
            }],
            from: vec![FromItem::Table {
                name: name.to_string(),
                alias: None,
                span: Span::empty(),
            }],
            selection: None,
            group_by: None,
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            span: Span::empty(),
        }),
        span: Span::empty(),
    }
}

#[test]
fn plan_statement_for_routing_returns_plan_kind_and_table_reference() {
    let mut catalog = MemoryCatalog::new();
    catalog.create_table(table("users")).unwrap();

    let planned = plan_statement_for_routing(&catalog, &select_from("users")).unwrap();

    assert_eq!(planned.plan.operation_name(), "SELECT");
    assert!(matches!(planned.statement_kind(), StatementKind::Select(_)));
    assert!(planned.diagnostics().is_empty());
    assert_eq!(planned.table_references().len(), 1);
    assert_eq!(planned.table_references()[0].table_name, "users");
    assert_eq!(
        planned.table_references()[0].access,
        TableReferenceAccess::Read
    );
    assert_eq!(
        planned.table_references()[0].source,
        TableReferenceSource::TopLevelPlanTableName
    );
}

#[test]
fn plan_statement_for_routing_marks_create_table_access() {
    let catalog = MemoryCatalog::new();
    let stmt = Statement {
        kind: StatementKind::CreateTable(CreateTable {
            if_not_exists: false,
            name: "events".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                constraints: Vec::new(),
                span: Span::empty(),
            }],
            constraints: Vec::new(),
            with_options: Vec::new(),
            span: Span::empty(),
        }),
        span: Span::empty(),
    };

    let planned = plan_statement_for_routing(&catalog, &stmt).unwrap();

    assert_eq!(planned.plan.operation_name(), "CREATE TABLE");
    assert!(matches!(
        planned.statement_kind(),
        StatementKind::CreateTable(_)
    ));
    assert_eq!(planned.table_references().len(), 1);
    assert_eq!(planned.table_references()[0].table_name, "events");
    assert_eq!(
        planned.table_references()[0].access,
        TableReferenceAccess::Create
    );
}

#[test]
fn join_plan_has_diagnostic_attachment_point_for_future_extractor() {
    let mut catalog = MemoryCatalog::new();
    catalog.create_table(table("users")).unwrap();
    catalog.create_table(table("orders")).unwrap();

    let stmt = Statement {
        kind: StatementKind::Select(Select {
            distinct: false,
            projection: vec![SelectItem::Wildcard {
                span: Span::empty(),
            }],
            from: vec![FromItem::Join {
                left: Box::new(FromItem::Table {
                    name: "users".to_string(),
                    alias: None,
                    span: Span::empty(),
                }),
                right: Box::new(FromItem::Table {
                    name: "orders".to_string(),
                    alias: None,
                    span: Span::empty(),
                }),
                join_type: JoinType::Inner,
                condition: None,
                using: None,
                span: Span::empty(),
            }],
            selection: None,
            group_by: None,
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            span: Span::empty(),
        }),
        span: Span::empty(),
    };

    let planned = plan_statement_for_routing(&catalog, &stmt).unwrap();

    assert!(planned.table_references().is_empty());
    assert_eq!(planned.diagnostics().len(), 1);
    assert_eq!(planned.diagnostics()[0].code, "ALOPEX-PLAN-ROUTE-002");
    assert_eq!(
        planned.diagnostics()[0].severity,
        PlanningDiagnosticSeverity::Warning
    );
}

#[test]
fn planning_boundary_uses_transaction_catalog_view_overlay() {
    let store = std::sync::Arc::new(MemoryKV::new());
    let catalog = PersistentCatalog::load(store).unwrap();
    let mut overlay = CatalogOverlay::new();
    overlay.add_table(
        TableFqn::new("default", "default", "pending_users"),
        table("pending_users").with_table_id(42),
    );
    let view = TxnCatalogView::new(&catalog, &overlay);

    let planned = plan_statement_for_routing(&view, &select_from("pending_users")).unwrap();

    assert_eq!(planned.table_references().len(), 1);
    assert_eq!(planned.table_references()[0].table_name, "pending_users");
    assert_eq!(
        planned.table_references()[0].access,
        TableReferenceAccess::Read
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn plan_sql_for_routing_parses_and_plans_without_execution() {
    let mut catalog = MemoryCatalog::new();
    catalog.create_table(table("users")).unwrap();

    let planned = plan_sql_for_routing(&catalog, "SELECT * FROM users").unwrap();

    assert_eq!(planned.len(), 1);
    assert_eq!(planned[0].table_references()[0].table_name, "users");
}
