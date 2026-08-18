use alopex_core::kv::memory::MemoryKV;
use alopex_sql::ast::ddl::{ColumnDef, CreateTable, DataType};
use alopex_sql::ast::dml::{FromItem, JoinType, LITERAL_TABLE, Select, SelectItem};
use alopex_sql::ast::expr::BinaryOp;
use alopex_sql::catalog::persistent::TableFqn;
use alopex_sql::catalog::{
    Catalog, CatalogOverlay, ColumnMetadata, MemoryCatalog, PersistentCatalog, TableMetadata,
    TxnCatalogView,
};
use alopex_sql::planner::{
    LogicalPlan, PlanningDiagnosticSeverity, ProjectedColumn, Projection, TableReferenceAccess,
    TableReferenceExtractor, TableReferenceSource, TypedExpr, TypedExprKind, plan_sql_for_routing,
    plan_statement_for_routing,
};
use alopex_sql::{ResolvedType, Span, Statement, StatementKind};

fn table(name: &str) -> TableMetadata {
    TableMetadata::new(name, vec![ColumnMetadata::new("id", ResolvedType::Integer)])
}

fn select_from(name: &str) -> Statement {
    Statement {
        kind: StatementKind::Select(Select {
            with: None,
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
            windows: vec![],
            qualify: None,
            set_operations: vec![],
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
        TableReferenceSource::LogicalPlanScan
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
fn join_plan_extracts_both_table_references() {
    let mut catalog = MemoryCatalog::new();
    catalog.create_table(table("users")).unwrap();
    catalog.create_table(table("orders")).unwrap();

    let stmt = Statement {
        kind: StatementKind::Select(Select {
            with: None,
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
                natural: false,
                span: Span::empty(),
            }],
            selection: None,
            group_by: None,
            having: None,
            windows: vec![],
            qualify: None,
            set_operations: vec![],
            order_by: Vec::new(),
            limit: None,
            offset: None,
            span: Span::empty(),
        }),
        span: Span::empty(),
    };

    let planned = plan_statement_for_routing(&catalog, &stmt).unwrap();

    assert!(planned.diagnostics().is_empty());
    assert_eq!(planned.table_references().len(), 2);
    assert!(
        planned
            .table_references()
            .iter()
            .any(|reference| reference.table_name == "users"
                && reference.access == TableReferenceAccess::Read
                && reference.source == TableReferenceSource::LogicalPlanScan)
    );
    assert!(
        planned
            .table_references()
            .iter()
            .any(|reference| reference.table_name == "orders"
                && reference.access == TableReferenceAccess::Read
                && reference.source == TableReferenceSource::LogicalPlanScan)
    );
}

#[test]
fn table_reference_extractor_covers_projection_filter_and_typed_subqueries() {
    let extractor = TableReferenceExtractor::new();
    let scalar = TypedExpr::new(
        TypedExprKind::ScalarSubquery(Box::new(scan_plan("invoices"))),
        ResolvedType::Integer,
        Span::empty(),
    );
    let in_subquery = TypedExpr::new(
        TypedExprKind::InSubquery {
            expr: Box::new(column_ref("users", "id")),
            subquery: Box::new(scan_plan("orders")),
            negated: false,
        },
        ResolvedType::Boolean,
        Span::empty(),
    );
    let exists = TypedExpr::new(
        TypedExprKind::Exists {
            subquery: Box::new(scan_plan("shipments")),
            negated: false,
        },
        ResolvedType::Boolean,
        Span::empty(),
    );
    let quantified = TypedExpr::new(
        TypedExprKind::Quantified {
            expr: Box::new(column_ref("users", "id")),
            op: BinaryOp::Lt,
            quantifier: alopex_sql::planner::typed_expr::Quantifier::All,
            subquery: Box::new(scan_plan("payments")),
        },
        ResolvedType::Boolean,
        Span::empty(),
    );
    let predicate = TypedExpr::binary_op(
        in_subquery,
        BinaryOp::And,
        TypedExpr::binary_op(
            exists,
            BinaryOp::And,
            quantified,
            ResolvedType::Boolean,
            Span::empty(),
        ),
        ResolvedType::Boolean,
        Span::empty(),
    );
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::scan(
            "users".to_string(),
            Projection::Columns(vec![ProjectedColumn::new(scalar)]),
        )),
        predicate,
    };
    let mut diagnostics = Vec::new();

    let references =
        extractor.extract_from_logical_plan(&plan, TableReferenceAccess::Read, &mut diagnostics);

    assert!(diagnostics.is_empty());
    for table in ["users", "invoices", "orders", "shipments", "payments"] {
        assert!(
            references
                .iter()
                .any(|reference| reference.table_name == table),
            "missing reference for {table}"
        );
    }
    assert!(
        references
            .iter()
            .any(|reference| reference.table_name == "users"
                && reference.source == TableReferenceSource::LogicalPlanScan)
    );
    for table in ["invoices", "orders", "shipments", "payments"] {
        assert!(
            references
                .iter()
                .any(|reference| reference.table_name == table
                    && reference.source == TableReferenceSource::TypedExprSubquery)
        );
    }
}

#[test]
fn table_reference_extractor_covers_join_condition_subquery() {
    let extractor = TableReferenceExtractor::new();
    let plan = LogicalPlan::join(
        scan_plan("users"),
        scan_plan("orders"),
        alopex_sql::planner::JoinType::Inner,
        Some(TypedExpr::new(
            TypedExprKind::Exists {
                subquery: Box::new(scan_plan("audit_log")),
                negated: false,
            },
            ResolvedType::Boolean,
            Span::empty(),
        )),
        None,
    );
    let mut diagnostics = Vec::new();

    let references =
        extractor.extract_from_logical_plan(&plan, TableReferenceAccess::Read, &mut diagnostics);

    assert!(diagnostics.is_empty());
    for table in ["users", "orders", "audit_log"] {
        assert!(
            references
                .iter()
                .any(|reference| reference.table_name == table),
            "missing reference for {table}"
        );
    }
    assert!(
        references
            .iter()
            .any(|reference| reference.table_name == "audit_log"
                && reference.source == TableReferenceSource::TypedExprSubquery)
    );
}

#[test]
fn table_reference_extractor_reports_no_physical_table_reference() {
    let extractor = TableReferenceExtractor::new();
    let mut diagnostics = Vec::new();

    let references = extractor.extract_from_logical_plan(
        &LogicalPlan::scan(LITERAL_TABLE.to_string(), Projection::All(Vec::new())),
        TableReferenceAccess::Read,
        &mut diagnostics,
    );

    assert!(references.is_empty());
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, "ALOPEX-PLAN-ROUTE-001");
    assert_eq!(diagnostics[0].severity, PlanningDiagnosticSeverity::Info);
}

#[test]
fn table_reference_extractor_reports_unsupported_drop_index_target() {
    let extractor = TableReferenceExtractor::new();
    let mut diagnostics = Vec::new();

    let references = extractor.extract_from_logical_plan(
        &LogicalPlan::drop_index("idx_orders_user_id".to_string(), false),
        TableReferenceAccess::Metadata,
        &mut diagnostics,
    );

    assert!(references.is_empty());
    let drop_index_diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "ALOPEX-PLAN-ROUTE-003")
        .expect("DROP INDEX target diagnostic");
    assert_eq!(
        drop_index_diagnostic.severity,
        PlanningDiagnosticSeverity::Warning
    );
    assert!(drop_index_diagnostic.message.contains("idx_orders_user_id"));
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

fn scan_plan(name: &str) -> LogicalPlan {
    LogicalPlan::scan(name.to_string(), Projection::All(vec!["id".to_string()]))
}

fn column_ref(table: &str, column: &str) -> TypedExpr {
    TypedExpr::column_ref(
        table.to_string(),
        column.to_string(),
        0,
        ResolvedType::Integer,
        Span::empty(),
    )
}
