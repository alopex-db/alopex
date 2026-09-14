use alopex_core::kv::memory::MemoryKV;
use alopex_sql::Span;
use alopex_sql::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn btree_index_answers_equality_and_range_filters() {
    use alopex_sql::ast::ddl::IndexMethod;
    use alopex_sql::storage::SqlValue;

    let (mut executor, _catalog) = create_executor();
    executor
        .execute(LogicalPlan::CreateTable {
            table: TableMetadata::new(
                "items",
                vec![
                    ColumnMetadata::new("id", ResolvedType::Integer)
                        .with_primary_key(true)
                        .with_not_null(true),
                    ColumnMetadata::new("score", ResolvedType::Integer),
                ],
            )
            .with_primary_key(vec!["id".into()]),
            if_not_exists: false,
            with_options: vec![],
        })
        .unwrap();

    let number = |value: i32| {
        literal(
            TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number(value.to_string())),
            ResolvedType::Integer,
        )
    };
    executor
        .execute(LogicalPlan::Insert {
            table: "items".into(),
            columns: vec!["id".into(), "score".into()],
            values: vec![
                vec![number(1), number(10)],
                vec![number(2), number(20)],
                vec![number(3), number(30)],
            ],
            conflict: None,
            returning: None,
        })
        .unwrap();
    executor
        .execute(LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "idx_items_score",
                "items",
                vec!["score".into()],
            )
            .with_method(IndexMethod::BTree),
            if_not_exists: false,
        })
        .unwrap();

    let predicate = |op, value| TypedExpr {
        kind: TypedExprKind::BinaryOp {
            left: Box::new(TypedExpr {
                kind: TypedExprKind::ColumnRef {
                    table: "items".into(),
                    column: "score".into(),
                    column_index: 1,
                },
                resolved_type: ResolvedType::Integer,
                span: Span::default(),
            }),
            op,
            right: Box::new(number(value)),
        },
        resolved_type: ResolvedType::Boolean,
        span: Span::default(),
    };
    let scan = || {
        LogicalPlan::scan(
            "items".into(),
            Projection::All(vec!["id".into(), "score".into()]),
        )
    };
    let mut query_rows = |plan| match executor.execute(plan).unwrap() {
        ExecutionResult::Query(query) => query.rows,
        other => panic!("unexpected result {other:?}"),
    };

    assert_eq!(
        query_rows(LogicalPlan::filter(
            scan(),
            predicate(alopex_sql::ast::expr::BinaryOp::Eq, 20),
        )),
        vec![vec![SqlValue::Integer(2), SqlValue::Integer(20)]]
    );
    assert_eq!(
        query_rows(LogicalPlan::filter(
            scan(),
            predicate(alopex_sql::ast::expr::BinaryOp::GtEq, 20),
        )),
        vec![
            vec![SqlValue::Integer(2), SqlValue::Integer(20)],
            vec![SqlValue::Integer(3), SqlValue::Integer(30)],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn executor_end_to_end_manual_plans() {
    let (mut executor, _catalog) = create_executor();

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
        .execute(LogicalPlan::CreateTable {
            table,
            if_not_exists: false,
            with_options: vec![],
        })
        .unwrap();

    // INSERT rows
    executor
        .execute(LogicalPlan::Insert {
            table: "users".into(),
            columns: vec!["id".into(), "name".into(), "age".into()],
            values: vec![
                vec![
                    literal(
                        TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number("1".into())),
                        ResolvedType::Integer,
                    ),
                    literal(
                        TypedExprKind::Literal(alopex_sql::ast::expr::Literal::String(
                            "alice".into(),
                        )),
                        ResolvedType::Text,
                    ),
                    literal(
                        TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number("30".into())),
                        ResolvedType::Integer,
                    ),
                ],
                vec![
                    literal(
                        TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number("2".into())),
                        ResolvedType::Integer,
                    ),
                    literal(
                        TypedExprKind::Literal(alopex_sql::ast::expr::Literal::String(
                            "bob".into(),
                        )),
                        ResolvedType::Text,
                    ),
                    literal(
                        TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number("25".into())),
                        ResolvedType::Integer,
                    ),
                ],
            ],
            conflict: None,
            returning: None,
        })
        .unwrap();

    // UPDATE name for id = 2
    executor
        .execute(LogicalPlan::Update {
            table: "users".into(),
            assignments: vec![TypedAssignment {
                column: "name".into(),
                column_index: 1,
                value: literal(
                    TypedExprKind::Literal(alopex_sql::ast::expr::Literal::String("bob2".into())),
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
                        TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number("2".into())),
                        ResolvedType::Integer,
                    )),
                },
                resolved_type: ResolvedType::Boolean,
                span: Span::default(),
            }),
            join_source: None,
            returning: None,
        })
        .unwrap();

    // DELETE id = 1
    executor
        .execute(LogicalPlan::Delete {
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
                        TypedExprKind::Literal(alopex_sql::ast::expr::Literal::Number("1".into())),
                        ResolvedType::Integer,
                    )),
                },
                resolved_type: ResolvedType::Boolean,
                span: Span::default(),
            }),
            join_source: None,
            returning: None,
        })
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
    let result = executor.execute(limit).unwrap();

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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn create_table_with_options_applies_storage() {
    let (mut executor, catalog) = create_executor();

    let table = TableMetadata::new(
        "col_tbl",
        vec![ColumnMetadata::new("id", ResolvedType::Integer)],
    );

    executor
        .execute(LogicalPlan::CreateTable {
            table,
            if_not_exists: false,
            with_options: vec![
                ("storage".into(), " columnar ".into()),
                ("compression".into(), " none ".into()),
                ("row_group_size".into(), "2000".into()),
            ],
        })
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

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn create_table_with_duplicate_option_errors() {
    let (mut executor, _catalog) = create_executor();

    let table = TableMetadata::new(
        "dup_tbl",
        vec![ColumnMetadata::new("id", ResolvedType::Integer)],
    );

    let err = executor
        .execute(LogicalPlan::CreateTable {
            table,
            if_not_exists: false,
            with_options: vec![
                ("storage".into(), "row".into()),
                ("storage".into(), "columnar".into()),
            ],
        })
        .unwrap_err();

    assert!(matches!(err, ExecutorError::DuplicateOption(_)));
}
