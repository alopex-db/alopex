use alopex_core::kv::memory::MemoryKV;
use alopex_sql::Span;
use alopex_sql::catalog::{ColumnMetadata, MemoryCatalog, TableMetadata};
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::planner::logical_plan::LogicalPlan;
use alopex_sql::planner::typed_expr::{Projection, TypedAssignment, TypedExpr, TypedExprKind};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::{Catalog, Compression, ExplainFormat, StorageType};
use std::sync::{Arc, RwLock};

mod issue575_columnar_source {
    use super::*;
    use alopex_core::kv::{KVStore, KVTransaction};
    use alopex_core::types::TxnMode;
    use alopex_sql::catalog::{CatalogOverlay, PersistentCatalog};
    use alopex_sql::storage::{SqlValue, TxnBridge};
    use alopex_sql::{AlopexDialect, Parser, Planner};
    use std::io::Write;

    type Engine = Executor<MemoryKV, PersistentCatalog<MemoryKV>>;
    type Metadata = Arc<RwLock<PersistentCatalog<MemoryKV>>>;

    fn plan(catalog: &Metadata, sql: &str) -> LogicalPlan {
        let statements = Parser::parse_sql(&AlopexDialect, sql).unwrap();
        assert_eq!(statements.len(), 1);
        Planner::new(&*catalog.read().unwrap())
            .plan(&statements[0])
            .unwrap()
    }

    fn run(engine: &mut Engine, catalog: &Metadata, sql: &str) -> ExecutionResult {
        engine.execute(plan(catalog, sql)).unwrap()
    }

    fn values(result: ExecutionResult) -> Vec<Vec<SqlValue>> {
        let ExecutionResult::Query(result) = result else {
            panic!("expected query rows")
        };
        result.rows
    }

    fn expected(rows: &[(i32, i32)]) -> Vec<Vec<SqlValue>> {
        rows.iter()
            .map(|&(id, value)| vec![SqlValue::Integer(id), SqlValue::Integer(value)])
            .collect()
    }

    fn setup() -> (Arc<MemoryKV>, Metadata, Engine) {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
        let mut engine = Executor::new(store.clone(), catalog.clone());
        for table in ["target", "row_target"] {
            run(
                &mut engine,
                &catalog,
                &format!("CREATE TABLE {table} (id INT PRIMARY KEY, value INT)"),
            );
            run(
                &mut engine,
                &catalog,
                &format!("INSERT INTO {table} VALUES (1,10),(2,20)"),
            );
        }
        run(
            &mut engine,
            &catalog,
            "CREATE TABLE row_source (id INT, value INT)",
        );
        run(
            &mut engine,
            &catalog,
            "INSERT INTO row_source VALUES (1,100),(3,300)",
        );
        for table in ["column_source", "empty_source"] {
            run(
                &mut engine,
                &catalog,
                &format!(
                    "CREATE TABLE {table} (id INT, value INT) WITH (storage='columnar', row_group_size=1000)"
                ),
            );
        }
        // Two real COPY operations exercise different persisted segments.
        for csv in ["1,100\n", "3,300\n"] {
            let mut file = tempfile::NamedTempFile::new().unwrap();
            file.write_all(csv.as_bytes()).unwrap();
            assert_eq!(
                run(
                    &mut engine,
                    &catalog,
                    &format!(
                        "COPY column_source FROM '{}' WITH (FORMAT CSV)",
                        file.path().display()
                    )
                ),
                ExecutionResult::RowsAffected(1)
            );
        }
        assert_eq!(
            values(run(
                &mut engine,
                &catalog,
                "SELECT id,value FROM column_source ORDER BY id"
            )),
            expected(&[(1, 100), (3, 300)])
        );
        (store, catalog, engine)
    }

    fn statement(merge: bool, target: &str, source: &str) -> String {
        if merge {
            format!(
                "MERGE INTO {target} USING {source} ON {target}.id = {source}.id WHEN MATCHED THEN UPDATE SET value = {source}.value WHEN NOT MATCHED THEN INSERT (id,value) VALUES ({source}.id,{source}.value)"
            )
        } else {
            format!(
                "UPDATE {target} SET value = {source}.value FROM {source} WHERE {target}.id = {source}.id"
            )
        }
    }

    fn result_rows(merge: bool) -> Vec<Vec<SqlValue>> {
        if merge {
            expected(&[(1, 100), (2, 20), (3, 300)])
        } else {
            expected(&[(1, 100), (2, 20)])
        }
    }

    fn check_committed(merge: bool) {
        let (_, catalog, mut engine) = setup();
        // Same-responsibility row-storage control runs before the columnar assertion.
        assert_eq!(
            run(
                &mut engine,
                &catalog,
                &statement(merge, "row_target", "row_source")
            ),
            ExecutionResult::RowsAffected(if merge { 2 } else { 1 })
        );
        assert_eq!(
            values(run(
                &mut engine,
                &catalog,
                "SELECT id,value FROM row_target ORDER BY id"
            )),
            result_rows(merge)
        );
        assert_eq!(
            run(
                &mut engine,
                &catalog,
                &statement(merge, "target", "empty_source")
            ),
            ExecutionResult::RowsAffected(0)
        );
        assert_eq!(
            values(run(
                &mut engine,
                &catalog,
                "SELECT id,value FROM target ORDER BY id"
            )),
            expected(&[(1, 10), (2, 20)])
        );
        assert_eq!(
            run(
                &mut engine,
                &catalog,
                &statement(merge, "target", "column_source")
            ),
            ExecutionResult::RowsAffected(if merge { 2 } else { 1 })
        );
        assert_eq!(
            values(run(
                &mut engine,
                &catalog,
                "SELECT id,value FROM target ORDER BY id"
            )),
            result_rows(merge)
        );
        assert_eq!(
            values(run(
                &mut engine,
                &catalog,
                "SELECT id,value FROM column_source ORDER BY id"
            )),
            expected(&[(1, 100), (3, 300)])
        );
    }

    fn snapshot(store: &MemoryKV) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
        txn.scan_prefix(&[]).unwrap().collect()
    }

    fn check_rollback(merge: bool) {
        let (store, catalog, mut engine) = setup();
        let before = snapshot(&store);
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        let mut overlay = CatalogOverlay::new();
        {
            let mut borrowed =
                TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
            assert_eq!(
                engine
                    .execute_in_txn(
                        plan(&catalog, &statement(merge, "target", "column_source")),
                        &mut borrowed
                    )
                    .unwrap(),
                ExecutionResult::RowsAffected(if merge { 2 } else { 1 })
            );
            assert_eq!(
                values(
                    engine
                        .execute_in_txn(
                            plan(&catalog, "SELECT id,value FROM target ORDER BY id"),
                            &mut borrowed
                        )
                        .unwrap()
                ),
                result_rows(merge)
            );
            assert_eq!(
                values(
                    engine
                        .execute_in_txn(
                            plan(&catalog, "SELECT id,value FROM column_source ORDER BY id"),
                            &mut borrowed
                        )
                        .unwrap()
                ),
                expected(&[(1, 100), (3, 300)])
            );
        }
        txn.rollback_self().unwrap();
        assert_eq!(
            snapshot(&store),
            before,
            "rollback must restore every KV entry, including source segments and catalog"
        );
        assert_eq!(
            values(run(
                &mut engine,
                &catalog,
                "SELECT id,value FROM target ORDER BY id"
            )),
            expected(&[(1, 10), (2, 20)])
        );
    }

    #[test]
    #[cfg_attr(not(feature = "lane_ci"), ignore)]
    fn merge_reads_columnar_source() {
        check_committed(true);
    }

    #[test]
    #[cfg_attr(not(feature = "lane_ci"), ignore)]
    fn update_from_reads_columnar_source() {
        check_committed(false);
    }

    #[test]
    #[cfg_attr(not(feature = "lane_ci"), ignore)]
    fn update_from_columnar_source_evaluates_scalar_subquery() {
        let (_, catalog, mut engine) = setup();
        assert_eq!(
            run(
                &mut engine,
                &catalog,
                "UPDATE target SET value = column_source.value FROM column_source \
                 WHERE target.id = column_source.id AND column_source.value = \
                 (SELECT value FROM row_source WHERE id = 1)",
            ),
            ExecutionResult::RowsAffected(1)
        );
        assert_eq!(
            values(run(
                &mut engine,
                &catalog,
                "SELECT id,value FROM target ORDER BY id",
            )),
            expected(&[(1, 100), (2, 20)])
        );
        assert_eq!(
            values(run(
                &mut engine,
                &catalog,
                "SELECT id,value FROM column_source ORDER BY id",
            )),
            expected(&[(1, 100), (3, 300)])
        );
    }

    #[test]
    #[cfg_attr(not(feature = "lane_ci"), ignore)]
    fn merge_columnar_source_rollback_restores_all_bytes() {
        check_rollback(true);
    }

    #[test]
    #[cfg_attr(not(feature = "lane_ci"), ignore)]
    fn update_from_columnar_source_rollback_restores_all_bytes() {
        check_rollback(false);
    }
}

#[path = "support/btree_read_counts.rs"]
mod btree_read_counts;

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
fn explain_reports_the_selected_btree_access_path() {
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
            values: vec![vec![number(1), number(10)], vec![number(2), number(20)]],
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

    let predicate = TypedExpr {
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
            op: alopex_sql::ast::expr::BinaryOp::Eq,
            right: Box::new(number(20)),
        },
        resolved_type: ResolvedType::Boolean,
        span: Span::default(),
    };
    let input = LogicalPlan::filter(
        LogicalPlan::scan("items".into(), Projection::All(vec!["id".into()])),
        predicate,
    );
    let input = LogicalPlan::limit(
        LogicalPlan::sort(
            input,
            vec![alopex_sql::planner::typed_expr::SortExpr::desc(TypedExpr {
                kind: TypedExprKind::ColumnRef {
                    table: "items".into(),
                    column: "score".into(),
                    column_index: 1,
                },
                resolved_type: ResolvedType::Integer,
                span: Span::default(),
            })],
        ),
        Some(1),
        None,
    );
    let result = executor
        .execute(LogicalPlan::Explain {
            analyze: false,
            format: ExplainFormat::Text,
            input: Box::new(input.clone()),
        })
        .unwrap();
    let ExecutionResult::Query(result) = result else {
        panic!("EXPLAIN must return a query result");
    };
    let plan = match &result.rows[0][0] {
        alopex_sql::storage::SqlValue::Text(plan) => plan,
        value => panic!("unexpected EXPLAIN value {value:?}"),
    };
    assert!(plan.contains("IndexScan index=idx_items_score"), "{plan}");

    let result = executor
        .execute(LogicalPlan::Explain {
            analyze: false,
            format: ExplainFormat::Json,
            input: Box::new(input.clone()),
        })
        .unwrap();
    let ExecutionResult::Query(result) = result else {
        panic!("EXPLAIN (FORMAT JSON) must return a query result");
    };
    let plan = match &result.rows[0][0] {
        alopex_sql::storage::SqlValue::Text(plan) => plan,
        value => panic!("unexpected EXPLAIN JSON value {value:?}"),
    };
    assert!(plan.contains("\"node\":\"IndexScan\""), "{plan}");
    assert!(plan.contains("\"index\":\"idx_items_score\""), "{plan}");

    // Each scan occurrence owns its access path, including self-joins where
    // matching by table name would incorrectly mark an unfiltered scan.
    let scan = || LogicalPlan::scan("items".into(), Projection::All(vec!["id".into()]));
    let join = |left, right| {
        LogicalPlan::join(
            left,
            right,
            alopex_sql::planner::JoinType::Cross,
            None,
            None,
        )
    };
    let lateral = |left, right| LogicalPlan::LateralJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type: alopex_sql::planner::JoinType::Cross,
        condition: None,
        right_schema: vec![ColumnMetadata::new("id", ResolvedType::Integer)],
    };
    let cases = [
        (
            LogicalPlan::project(input.clone(), Projection::All(vec!["id".into()])),
            vec![true],
        ),
        (
            LogicalPlan::aggregate(
                input.clone(),
                vec![],
                vec![],
                None,
                Projection::All(vec!["id".into()]),
            ),
            vec![true],
        ),
        (
            LogicalPlan::Window {
                input: Box::new(input.clone()),
                windows: vec![],
            },
            vec![true],
        ),
        (join(scan(), input.clone()), vec![false, true]),
        (join(input.clone(), scan()), vec![true, false]),
        (join(input.clone(), input.clone()), vec![true, true]),
        // LATERAL supplies an outer row even when its predicate is constant.
        (lateral(input.clone(), input.clone()), vec![true, false]),
        (
            lateral(scan(), lateral(input.clone(), input)),
            vec![false, false, false],
        ),
    ];
    fn scan_paths(node: &serde_json::Value, paths: &mut Vec<bool>) {
        match node["node"].as_str() {
            Some("Scan") => paths.push(false),
            Some("IndexScan") => {
                assert_eq!(node["index"], "idx_items_score");
                paths.push(true);
            }
            _ => {}
        }
        for child in node["children"].as_array().unwrap() {
            scan_paths(child, paths);
        }
    }
    for (input, expected) in cases {
        for format in [ExplainFormat::Text, ExplainFormat::Json] {
            let ExecutionResult::Query(result) = executor
                .execute(LogicalPlan::Explain {
                    analyze: false,
                    format,
                    input: Box::new(input.clone()),
                })
                .unwrap()
            else {
                panic!("EXPLAIN must return a query result");
            };
            let SqlValue::Text(plan) = &result.rows[0][0] else {
                panic!("EXPLAIN must return text");
            };
            let actual = match format {
                ExplainFormat::Text => plan
                    .lines()
                    .filter_map(|line| {
                        let line = line.trim_start();
                        if line.starts_with("IndexScan ") {
                            assert!(
                                line.starts_with("IndexScan index=idx_items_score table=items")
                            );
                            Some(true)
                        } else if line.starts_with("Scan ") {
                            Some(false)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
                ExplainFormat::Json => {
                    let document: serde_json::Value = serde_json::from_str(plan).unwrap();
                    let mut logical = Vec::new();
                    scan_paths(&document["logical_plan"], &mut logical);
                    assert_eq!(logical, vec![false; expected.len()]);
                    let mut physical = Vec::new();
                    scan_paths(&document["physical_plan"]["root"], &mut physical);
                    if expected.len() > 1 {
                        assert!(document["physical_plan"]["access_path"].is_null());
                    }
                    physical
                }
            };
            assert_eq!(actual, expected, "{plan}");
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn btree_cross_type_literals_preserve_scan_results_and_ordered_limits() {
    use alopex_sql::ast::expr::{BinaryOp, Literal};
    use alopex_sql::catalog::IndexMetadata;
    use alopex_sql::planner::typed_expr::SortExpr;

    // The same public executor plans run before and after CREATE INDEX. Each
    // case also checks EXPLAIN so a fallback cannot advertise an index lookup.
    for column_type in [ResolvedType::Integer, ResolvedType::Double] {
        let (mut executor, _) = create_executor();
        executor
            .execute(LogicalPlan::CreateTable {
                table: TableMetadata::new(
                    "numbers",
                    vec![ColumnMetadata::new("n", column_type.clone())],
                ),
                if_not_exists: false,
                with_options: vec![],
            })
            .unwrap();
        executor
            .execute(LogicalPlan::Insert {
                table: "numbers".into(),
                columns: vec!["n".into()],
                values: (1..=4)
                    .map(|n| {
                        vec![literal(
                            TypedExprKind::Literal(Literal::Number(n.to_string())),
                            column_type.clone(),
                        )]
                    })
                    .collect(),
                conflict: None,
                returning: None,
            })
            .unwrap();
        let column = TypedExpr {
            kind: TypedExprKind::ColumnRef {
                table: "numbers".into(),
                column: "n".into(),
                column_index: 0,
            },
            resolved_type: column_type.clone(),
            span: Span::default(),
        };
        let mut cases = Vec::new();
        for (op, number, expected_count) in [
            (BinaryOp::Eq, "3.0", 1),
            (BinaryOp::Gt, "2.5", 2),
            (BinaryOp::Lt, "2.5", 2),
            (BinaryOp::GtEq, "3.0", 2),
            (BinaryOp::LtEq, "3.0", 3),
        ] {
            // DOUBLE n = 3 uses an INTEGER literal; the other cases exercise
            // INTEGER comparisons against fractional/integral DOUBLE literals.
            let (number, literal_type) = if column_type == ResolvedType::Double {
                if number == "2.5" {
                    continue;
                }
                ("3", ResolvedType::Integer)
            } else {
                (number, ResolvedType::Double)
            };
            for reversed in [false, true] {
                let value = literal(
                    TypedExprKind::Literal(Literal::Number(number.into())),
                    literal_type.clone(),
                );
                let (left, right, op) = if reversed {
                    let op = match op {
                        BinaryOp::Gt => BinaryOp::Lt,
                        BinaryOp::GtEq => BinaryOp::LtEq,
                        BinaryOp::Lt => BinaryOp::Gt,
                        BinaryOp::LtEq => BinaryOp::GtEq,
                        op => op,
                    };
                    (value, column.clone(), op)
                } else {
                    (column.clone(), value, op)
                };
                let plan = LogicalPlan::filter(
                    LogicalPlan::scan("numbers".into(), Projection::All(vec!["n".into()])),
                    literal(
                        TypedExprKind::BinaryOp {
                            left: Box::new(left),
                            op,
                            right: Box::new(right),
                        },
                        ResolvedType::Boolean,
                    ),
                );
                cases.push((plan.clone(), expected_count));
                cases.push((
                    LogicalPlan::limit(
                        LogicalPlan::sort(plan, vec![SortExpr::desc(column.clone())]),
                        Some(1),
                        Some(0),
                    ),
                    1,
                ));
            }
        }
        let baseline: Vec<_> = cases
            .iter()
            .map(|(plan, expected_count)| {
                let ExecutionResult::Query(result) = executor.execute(plan.clone()).unwrap() else {
                    panic!("expected rows")
                };
                assert_eq!(result.rows.len(), *expected_count);
                result.rows
            })
            .collect();
        executor
            .execute(LogicalPlan::CreateIndex {
                index: IndexMetadata::new(0, "idx_numbers_n", "numbers", vec!["n".into()]),
                if_not_exists: false,
            })
            .unwrap();
        for ((plan, _), expected) in cases.into_iter().zip(baseline) {
            let ExecutionResult::Query(result) = executor.execute(plan.clone()).unwrap() else {
                panic!("expected rows")
            };
            assert_eq!(
                result.rows, expected,
                "column type {column_type:?}, plan {plan:?}"
            );
            for format in [ExplainFormat::Text, ExplainFormat::Json] {
                let ExecutionResult::Query(result) = executor
                    .execute(LogicalPlan::Explain {
                        analyze: false,
                        format,
                        input: Box::new(plan.clone()),
                    })
                    .unwrap()
                else {
                    panic!("expected EXPLAIN")
                };
                let alopex_sql::storage::SqlValue::Text(explain) = &result.rows[0][0] else {
                    panic!("expected plan text")
                };
                assert!(!explain.contains("IndexScan"), "{explain}");
            }
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn btree_same_type_boundaries_preserve_scan_comparison_semantics() {
    use alopex_sql::ast::expr::{BinaryOp, Literal};
    use alopex_sql::catalog::IndexMetadata;

    for (column_type, values, boundary, equality_count) in [
        (ResolvedType::Float, vec!["-0.0", "0.0", "1.0"], "0.0", 2),
        (ResolvedType::Double, vec!["-0.0", "0.0", "1.0"], "0.0", 2),
        (
            ResolvedType::BigInt,
            vec!["9007199254740992", "9007199254740993", "9007199254740994"],
            "9007199254740992",
            2,
        ),
        (ResolvedType::Text, vec!["a", "a\0b", "b"], "a", 1),
    ] {
        let (mut executor, _) = create_executor();
        let value = |text: &str| {
            literal(
                TypedExprKind::Literal(if column_type == ResolvedType::Text {
                    Literal::String(text.into())
                } else {
                    Literal::Number(text.into())
                }),
                column_type.clone(),
            )
        };
        executor
            .execute(LogicalPlan::CreateTable {
                table: TableMetadata::new(
                    "boundaries",
                    vec![ColumnMetadata::new("n", column_type.clone())],
                ),
                if_not_exists: false,
                with_options: vec![],
            })
            .unwrap();
        executor
            .execute(LogicalPlan::Insert {
                table: "boundaries".into(),
                columns: vec!["n".into()],
                values: values.into_iter().map(|n| vec![value(n)]).collect(),
                conflict: None,
                returning: None,
            })
            .unwrap();
        let column = literal(
            TypedExprKind::ColumnRef {
                table: "boundaries".into(),
                column: "n".into(),
                column_index: 0,
            },
            column_type.clone(),
        );
        let mut cases = Vec::new();
        for op in [
            BinaryOp::Eq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
        ] {
            for reversed in [false, true] {
                let (left, right) = if reversed {
                    (value(boundary), column.clone())
                } else {
                    (column.clone(), value(boundary))
                };
                let plan = LogicalPlan::filter(
                    LogicalPlan::scan("boundaries".into(), Projection::All(vec!["n".into()])),
                    literal(
                        TypedExprKind::BinaryOp {
                            left: Box::new(left),
                            op,
                            right: Box::new(right),
                        },
                        ResolvedType::Boolean,
                    ),
                );
                let ExecutionResult::Query(result) = executor.execute(plan.clone()).unwrap() else {
                    panic!("expected baseline rows")
                };
                if op == BinaryOp::Eq {
                    assert_eq!(result.rows.len(), equality_count);
                }
                cases.push((
                    plan,
                    result.rows,
                    column_type == ResolvedType::Text && op == BinaryOp::Eq,
                ));
            }
        }
        executor
            .execute(LogicalPlan::CreateIndex {
                index: IndexMetadata::new(0, "idx_boundaries_n", "boundaries", vec!["n".into()]),
                if_not_exists: false,
            })
            .unwrap();
        for (plan, expected, uses_index) in cases {
            let ExecutionResult::Query(result) = executor.execute(plan.clone()).unwrap() else {
                panic!("expected indexed rows")
            };
            assert_eq!(result.rows, expected, "{column_type:?}: {plan:?}");
            let ExecutionResult::Query(result) = executor
                .execute(LogicalPlan::Explain {
                    analyze: false,
                    format: ExplainFormat::Text,
                    input: Box::new(plan),
                })
                .unwrap()
            else {
                panic!("expected EXPLAIN")
            };
            let alopex_sql::storage::SqlValue::Text(explain) = &result.rows[0][0] else {
                panic!("expected plan text")
            };
            assert_eq!(explain.contains("IndexScan"), uses_index, "{explain}");
        }
    }
}
