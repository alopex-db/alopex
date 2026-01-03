use alopex_core::kv::memory::MemoryKV;
use alopex_sql::Span;
use alopex_sql::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
use alopex_sql::executor::{ExecutionConfig, ExecutionResult, Executor, ExecutorError};
use alopex_sql::planner::logical_plan::LogicalPlan;
use alopex_sql::planner::typed_expr::{Projection, TypedAssignment, TypedExpr, TypedExprKind};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::{Catalog, Compression, StorageType};
use std::sync::{Arc, RwLock};

fn create_executor() -> (
    Executor<MemoryKV, MemoryCatalog>,
    Arc<RwLock<MemoryCatalog>>,
) {
    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let executor = Executor::new(store, Arc::clone(&catalog));
    (executor, catalog)
}

fn literal(kind: TypedExprKind, ty: ResolvedType) -> TypedExpr {
    TypedExpr {
        kind,
        resolved_type: ty,
        span: Span::default(),
    }
}

#[test]
fn executor_end_to_end_manual_plans() {
    let (mut executor, _catalog) = create_executor();
    let config = ExecutionConfig::default();

    // CREATE TABLE
    let table = TableMetadata::new(
        "users",
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ColumnMetadata::new("age", ResolvedType::Integer),
        ],
    )
    .with_primary_key(vec!["id".into()]);
    executor
        .execute(
            LogicalPlan::CreateTable {
                table,
                if_not_exists: false,
                with_options: vec![],
            },
            &config,
        )
        .unwrap();

    // INSERT rows
    executor
        .execute(
            LogicalPlan::Insert {
                table: "users".into(),
                columns: vec!["id".into(), "name".into(), "age".into()],
                values: vec![
                    vec![
                        literal(
                            TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number(
                                "1".into(),
                            )),
                            ResolvedType::Integer,
                        ),
                        literal(
                            TypedExprKind::Literal(alopex_sql::ast::expr::Literal::String(
                                "alice".into(),
                            )),
                            ResolvedType::Text,
                        ),
                        literal(
                            TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number(
                                "30".into(),
                            )),
                            ResolvedType::Integer,
                        ),
                    ],
                    vec![
                        literal(
                            TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number(
                                "2".into(),
                            )),
                            ResolvedType::Integer,
                        ),
                        literal(
                            TypedExprKind::Literal(alopex_sql::ast::expr::Literal::String(
                                "bob".into(),
                            )),
                            ResolvedType::Text,
                        ),
                        literal(
                            TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number(
                                "25".into(),
                            )),
                            ResolvedType::Integer,
                        ),
                    ],
                ],
            },
            &config,
        )
        .unwrap();

    // UPDATE name for id = 2
    executor
        .execute(
            LogicalPlan::Update {
                table: "users".into(),
                assignments: vec![TypedAssignment {
                    column: "name".into(),
                    column_index: 1,
                    value: literal(
                        TypedExprKind::Literal(alopex_sql::ast::expr::Literal::String(
                            "bob2".into(),
                        )),
                        ResolvedType::Text,
                    ),
                }],
                filter: Some(TypedExpr {
                    kind: TypedExprKind::BinaryOp {
                        left: Box::new(TypedExpr {
                            kind: TypedExprKind::ColumnRef {
                                table: "users".into(),
                                column: "id".into(),
                                column_index: 0,
                            },
                            resolved_type: ResolvedType::Integer,
                            span: Span::default(),
                        }),
                        op: alopex_sql::ast::expr::BinaryOp::Eq,
                        right: Box::new(literal(
                            TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number(
                                "2".into(),
                            )),
                            ResolvedType::Integer,
                        )),
                    },
                    resolved_type: ResolvedType::Boolean,
                    span: Span::default(),
                }),
            },
            &config,
        )
        .unwrap();

    // DELETE id = 1
    executor
        .execute(
            LogicalPlan::Delete {
                table: "users".into(),
                filter: Some(TypedExpr {
                    kind: TypedExprKind::BinaryOp {
                        left: Box::new(TypedExpr {
                            kind: TypedExprKind::ColumnRef {
                                table: "users".into(),
                                column: "id".into(),
                                column_index: 0,
                            },
                            resolved_type: ResolvedType::Integer,
                            span: Span::default(),
                        }),
                        op: alopex_sql::ast::expr::BinaryOp::Eq,
                        right: Box::new(literal(
                            TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number(
                                "1".into(),
                            )),
                            ResolvedType::Integer,
                        )),
                    },
                    resolved_type: ResolvedType::Boolean,
                    span: Span::default(),
                }),
            },
            &config,
        )
        .unwrap();

    // SELECT remaining row ordered by age desc limit 1
    let scan = LogicalPlan::scan(
        "users".into(),
        Projection::All(vec!["id".into(), "name".into(), "age".into()]),
    );
    let sort = LogicalPlan::sort(
        scan,
        vec![alopex_sql::planner::typed_expr::SortExpr {
            expr: TypedExpr {
                kind: TypedExprKind::ColumnRef {
                    table: "users".into(),
                    column: "age".into(),
                    column_index: 2,
                },
                resolved_type: ResolvedType::Integer,
                span: Span::default(),
            },
            asc: false,
            nulls_first: false,
        }],
    );
    let limit = LogicalPlan::limit(sort, Some(1), None);
    let result = executor.execute(limit, &config).unwrap();

    match result {
        ExecutionResult::Query(q) => {
            assert_eq!(q.rows.len(), 1);
            assert_eq!(
                q.rows[0],
                vec![
                    alopex_sql::storage::SqlValue::Integer(2),
                    alopex_sql::storage::SqlValue::Text("bob2".into()),
                    alopex_sql::storage::SqlValue::Integer(25)
                ]
            );
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[test]
fn create_table_with_options_applies_storage() {
    let (mut executor, catalog) = create_executor();
    let config = ExecutionConfig::default();

    let table = TableMetadata::new(
        "col_tbl",
        vec![ColumnMetadata::new("id", ResolvedType::Integer)],
    );

    executor
        .execute(
            LogicalPlan::CreateTable {
                table,
                if_not_exists: false,
                with_options: vec![
                    ("storage".into(), " columnar ".into()),
                    ("compression".into(), " none ".into()),
                    ("row_group_size".into(), "2000".into()),
                ],
            },
            &config,
        )
        .unwrap();

    let stored = catalog
        .read()
        .unwrap()
        .get_table("col_tbl")
        .unwrap()
        .clone();
    assert_eq!(stored.storage_options.storage_type, StorageType::Columnar);
    assert_eq!(stored.storage_options.compression, Compression::None);
    assert_eq!(stored.storage_options.row_group_size, 2_000);
}

#[test]
fn create_table_with_duplicate_option_errors() {
    let (mut executor, _catalog) = create_executor();
    let config = ExecutionConfig::default();

    let table = TableMetadata::new(
        "dup_tbl",
        vec![ColumnMetadata::new("id", ResolvedType::Integer)],
    );

    let err = executor
        .execute(
            LogicalPlan::CreateTable {
                table,
                if_not_exists: false,
                with_options: vec![
                    ("storage".into(), "row".into()),
                    ("storage".into(), "columnar".into()),
                ],
            },
            &config,
        )
        .unwrap_err();

    assert!(matches!(err, ExecutorError::DuplicateOption(_)));
}
