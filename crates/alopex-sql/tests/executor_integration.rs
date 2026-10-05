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
fn columnar_dml_rejects_every_write_entrypoint() {
    use alopex_sql::ast::expr::Literal;
    use alopex_sql::planner::{MergeActionPlan, MergeClausePlan};

    let number = || {
        literal(
            TypedExprKind::Literal(Literal::Number("1".into())),
            ResolvedType::Integer,
        )
    };
    for operation in ["INSERT", "INSERT SELECT", "UPDATE", "DELETE", "MERGE"] {
        let (mut executor, _) = create_executor();
        for (name, storage) in [("target", "columnar"), ("source", "row")] {
            executor
                .execute(LogicalPlan::CreateTable {
                    table: TableMetadata::new(
                        name,
                        vec![ColumnMetadata::new("id", ResolvedType::Integer)],
                    ),
                    if_not_exists: false,
                    with_options: vec![("storage".into(), storage.into())],
                })
                .unwrap();
        }
        // A row-store write remains supported and supplies a nonempty MERGE source.
        executor
            .execute(LogicalPlan::Insert {
                table: "source".into(),
                columns: vec!["id".into()],
                values: vec![vec![number()]],
                conflict: None,
                returning: None,
            })
            .unwrap();
        let plan = match operation {
            "INSERT" => LogicalPlan::Insert {
                table: "target".into(),
                columns: vec!["id".into()],
                values: vec![vec![number()]],
                conflict: None,
                returning: None,
            },
            "INSERT SELECT" => LogicalPlan::InsertSelect {
                table: "target".into(),
                columns: vec!["id".into()],
                source: Box::new(LogicalPlan::Values {
                    rows: vec![vec![number()]],
                    schema: vec![ColumnMetadata::new("id", ResolvedType::Integer)],
                }),
                conflict: None,
                returning: None,
            },
            "UPDATE" => LogicalPlan::Update {
                table: "target".into(),
                assignments: vec![TypedAssignment {
                    column: "id".into(),
                    column_index: 0,
                    value: number(),
                }],
                filter: None,
                join_source: None,
                returning: None,
            },
            "DELETE" => LogicalPlan::Delete {
                table: "target".into(),
                filter: None,
                join_source: None,
                returning: None,
            },
            "MERGE" => LogicalPlan::Merge {
                target: "target".into(),
                source: "source".into(),
                on: literal(
                    TypedExprKind::Literal(Literal::Boolean(true)),
                    ResolvedType::Boolean,
                ),
                clauses: vec![MergeClausePlan {
                    matched: false,
                    condition: None,
                    action: MergeActionPlan::Insert {
                        columns: vec!["id".into()],
                        values: vec![number()],
                    },
                }],
            },
            _ => unreachable!(),
        };
        let error = executor.execute(plan).expect_err(operation);
        assert!(
            matches!(error, ExecutorError::UnsupportedOperation(ref message)
            if message.starts_with(operation.split_whitespace().next().unwrap())
                && message.contains("columnar") && message.contains("COPY")),
            "{operation}: {error}"
        );
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn columnar_sql_dml_preserves_copied_rows() {
    use alopex_sql::dialect::AlopexDialect;
    use alopex_sql::parser::Parser;
    use alopex_sql::planner::Planner;
    use alopex_sql::storage::SqlValue;
    use std::io::Write;

    let (mut executor, catalog) = create_executor();
    let mut run = |sql: &str| {
        let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap();
        executor.execute(plan)
    };
    run("CREATE TABLE columnar_target (id INT PRIMARY KEY, value INT) WITH (storage='columnar')")
        .unwrap();
    run("CREATE TABLE row_source (id INT PRIMARY KEY, value INT)").unwrap();
    run("INSERT INTO row_source VALUES (1, 20), (2, 30)").unwrap();
    let mut csv = tempfile::NamedTempFile::new().unwrap();
    writeln!(csv, "id,value\n1,10").unwrap();
    assert_eq!(
        run(&format!(
            "COPY columnar_target FROM '{}' WITH (FORMAT CSV, HEADER TRUE)",
            csv.path().display()
        ))
        .unwrap(),
        ExecutionResult::RowsAffected(1)
    );
    for sql in [
        "INSERT INTO columnar_target VALUES (2, 30)",
        "INSERT INTO columnar_target SELECT id, value FROM row_source",
        "INSERT INTO columnar_target VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET value = excluded.value",
        "INSERT INTO columnar_target VALUES (1, 20) ON CONFLICT (id) DO NOTHING",
        "UPDATE columnar_target SET value = 20 WHERE id = 1",
        "DELETE FROM columnar_target WHERE id = 1",
        "MERGE INTO columnar_target USING row_source ON columnar_target.id = row_source.id WHEN MATCHED THEN UPDATE SET value = row_source.value",
    ] {
        let error = run(sql).expect_err(sql);
        assert!(
            matches!(error, ExecutorError::UnsupportedOperation(ref message)
            if message.starts_with(sql.split_whitespace().next().unwrap())
                && message.contains("columnar") && message.contains("COPY")),
            "{sql}: {error}"
        );
        let ExecutionResult::Query(result) = run("SELECT id, value FROM columnar_target").unwrap()
        else {
            panic!("query result expected")
        };
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::Integer(1), SqlValue::Integer(10)]]
        );
    }
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
