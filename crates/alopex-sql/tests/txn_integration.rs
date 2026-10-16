use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::types::TxnMode;
use alopex_core::vector::hnsw::HnswIndex;

use alopex_sql::Span;
use alopex_sql::ast::ddl::{IndexMethod, TableConstraint, VectorMetric};
use alopex_sql::ast::expr::Literal;
use alopex_sql::catalog::{
    Catalog, CatalogOverlay, PersistentCatalog, TableMetadata, TxnCatalogView,
};
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::planner::logical_plan::LogicalPlan;
use alopex_sql::planner::typed_expr::{Projection, TypedExpr, TypedExprKind};
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::SqlValue;
use alopex_sql::storage::{SqlTxn as _, TxnBridge};

type PersistentCatalogHandle = Arc<RwLock<PersistentCatalog<MemoryKV>>>;
type PersistentExecutor = Executor<MemoryKV, PersistentCatalog<MemoryKV>>;

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn overlay_replaces_base_index_in_transaction_view() {
    use alopex_sql::catalog::persistent::{IndexFqn, TableFqn};
    use alopex_sql::catalog::{ColumnMetadata, IndexMetadata};

    let store = Arc::new(MemoryKV::new());
    let mut catalog = PersistentCatalog::new(store);
    let mut table = TableMetadata::new(
        "items",
        vec![
            ColumnMetadata::new("obsolete", ResolvedType::Integer),
            ColumnMetadata::new("id", ResolvedType::Integer),
        ],
    )
    .with_table_id(1);
    let mut index = IndexMetadata::new(1, "idx_items_id", "items", vec!["id".into()])
        .with_column_indices(vec![1]);
    let mut base = CatalogOverlay::new();
    base.add_table(TableFqn::from(&table), table.clone());
    base.add_index(IndexFqn::from(&index), index.clone());
    catalog.apply_overlay(base);

    table.columns.remove(0);
    index.column_indices = vec![0];
    let mut overlay = CatalogOverlay::new();
    overlay.add_table(TableFqn::from(&table), table);
    overlay.add_index(IndexFqn::from(&index), index);

    let view = TxnCatalogView::new(&catalog, &overlay);
    let indexes = view.get_indexes_for_table("items");
    assert_eq!(
        indexes.len(),
        1,
        "an overlay replacement must hide the base index"
    );
    assert_eq!(indexes[0].name, "idx_items_id");
    assert_eq!(indexes[0].column_indices, vec![0]);
    assert_eq!(
        catalog.get_indexes_for_table("items")[0].column_indices,
        vec![1],
        "the transaction overlay must not change the base catalog"
    );
}

fn executor_with_persistent_catalog(
    store: Arc<MemoryKV>,
) -> (PersistentExecutor, PersistentCatalogHandle) {
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
    (Executor::new(store, catalog.clone()), catalog)
}

fn execute_sql_persistent(
    executor: &mut PersistentExecutor,
    catalog: &PersistentCatalogHandle,
    sql: &str,
) -> Result<Vec<ExecutionResult>, ExecutorError> {
    let store = catalog.read().unwrap().store().clone();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
    let mut results = Vec::new();
    for statement in Parser::parse_sql(&AlopexDialect, sql).expect("parse SQL") {
        let plan = {
            let guard = catalog.read().unwrap();
            let (_, overlay) = borrowed.split_parts();
            let view = TxnCatalogView::new(&*guard, &*overlay);
            Planner::new(&view).plan(&statement).expect("plan SQL")
        };
        results.push(executor.execute_in_txn(plan, &mut borrowed)?);
    }
    drop(borrowed);
    txn.commit_self().unwrap();
    catalog.write().unwrap().apply_overlay(overlay);
    Ok(results)
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistent_unique_creation_checks_sparse_rows_methods_and_repeated_names() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store);
    let mut run = |sql: &str| execute_sql_persistent(&mut executor, &catalog, sql);

    run("CREATE TABLE sparse (id INT, code INT)").unwrap();
    // Preserve the 2,048-row gap without exceeding the parser's per-statement
    // MessagePack collection budget while constructing the fixture.
    for first in (1..=2050).step_by(128) {
        let values = (first..=(first + 127).min(2050))
            .map(|id| format!("({id}, 7)"))
            .collect::<Vec<_>>()
            .join(",");
        run(&format!("INSERT INTO sparse VALUES {values}")).unwrap();
    }
    run("DELETE FROM sparse WHERE id <= 2048").unwrap();
    let error = run("CREATE UNIQUE INDEX sparse_unique ON sparse (code)").unwrap_err();
    assert!(
        matches!(
            error,
            ExecutorError::ConstraintViolation(
                alopex_sql::executor::ConstraintViolation::Unique { .. }
            )
        ),
        "{error}"
    );
    assert!(!catalog.read().unwrap().index_exists("sparse_unique"));
    let rows = run("SELECT id, code FROM sparse ORDER BY id").unwrap();
    let ExecutionResult::Query(query) = &rows[0] else {
        panic!("expected sparse rows");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(2049), SqlValue::Integer(7)],
            vec![SqlValue::Integer(2050), SqlValue::Integer(7)],
        ]
    );
    run("CREATE INDEX sparse_unique ON sparse (code)").unwrap();

    run("CREATE TABLE vectors (id INT, embedding VECTOR(2, L2));
         INSERT INTO vectors VALUES (1, [1.0, 0.0]), (2, [1.0, 0.0]);")
    .unwrap();
    let error =
        run("CREATE UNIQUE INDEX vector_unique ON vectors (embedding) USING HNSW").unwrap_err();
    assert!(
        matches!(
            error,
            ExecutorError::InvalidOperation { ref operation, ref reason }
                if operation == "CREATE UNIQUE INDEX" && reason.contains("HNSW")
        ),
        "{error}"
    );
    assert!(!catalog.read().unwrap().index_exists("vector_unique"));
    let rows = run("SELECT id FROM vectors ORDER BY id").unwrap();
    let ExecutionResult::Query(query) = &rows[0] else {
        panic!("expected unchanged vector rows");
    };
    assert_eq!(
        query.rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)],]
    );

    let error = run(
        "CREATE TABLE repeated (a INT, CONSTRAINT duplicate_name UNIQUE (a),
         CONSTRAINT duplicate_name UNIQUE (a))",
    )
    .unwrap_err();
    assert!(
        matches!(
            error, ExecutorError::IndexAlreadyExists(ref name) if name == "duplicate_name"
        ),
        "{error}"
    );
    let guard = catalog.read().unwrap();
    assert!(!guard.table_exists("repeated"));
    assert!(!guard.index_exists("duplicate_name"));
}

fn wrap_external<'a, 'b>(
    txn: &'a mut <MemoryKV as KVStore>::Transaction<'b>,
    mode: TxnMode,
    overlay: &'a mut CatalogOverlay,
) -> alopex_sql::storage::BorrowedSqlTransaction<'a, 'b, 'a, MemoryKV> {
    TxnBridge::<MemoryKV>::wrap_external(txn, mode, overlay)
}

fn vector_table() -> TableMetadata {
    TableMetadata::new(
        "items",
        vec![
            alopex_sql::catalog::ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            alopex_sql::catalog::ColumnMetadata::new(
                "embedding",
                ResolvedType::Vector {
                    dimension: 3,
                    metric: VectorMetric::Cosine,
                },
            )
            .with_not_null(true),
        ],
    )
    .with_primary_key(vec!["id".to_string()])
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn external_columnar_ctas_plan_rejects_before_overlay_mutation_and_preserves_noop() {
    use alopex_core::kv::KVTransaction;
    use alopex_sql::dialect::AlopexDialect;
    use alopex_sql::parser::Parser;
    use alopex_sql::planner::Planner;
    use alopex_sql::storage::SqlValue;
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());
    let statement = Parser::parse_sql(&AlopexDialect, "CREATE TABLE copied AS SELECT 1 AS id")
        .unwrap()
        .remove(0);
    let row_plan = Planner::new(&*catalog.read().unwrap())
        .plan(&statement)
        .unwrap();
    let mut columnar_plan = row_plan.clone();
    let LogicalPlan::CreateTableAs {
        with_options,
        if_not_exists,
        ..
    } = &mut columnar_plan
    else {
        panic!("expected CTAS plan");
    };
    with_options.push(("storage".into(), "columnar".into()));
    *if_not_exists = true;
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
    assert!(
        matches!(executor.execute_in_txn(columnar_plan.clone(), &mut borrowed),
        Err(ExecutorError::UnsupportedOperation(message)) if message.contains("columnar"))
    );
    drop(borrowed);
    assert!(!TxnCatalogView::new(&*catalog.read().unwrap(), &overlay).table_exists("copied"));
    txn.commit_self().unwrap();
    assert!(
        !PersistentCatalog::load(store.clone())
            .unwrap()
            .table_exists("copied")
    );

    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
    executor.execute_in_txn(row_plan, &mut borrowed).unwrap();
    assert_eq!(
        executor
            .execute_in_txn(columnar_plan, &mut borrowed)
            .unwrap(),
        ExecutionResult::Success
    );
    drop(borrowed);
    txn.commit_self().unwrap();
    catalog.write().unwrap().apply_overlay(overlay);
    let reloaded = PersistentCatalog::load(store).unwrap();
    assert!(reloaded.table_exists("copied"));
    let statement = Parser::parse_sql(&AlopexDialect, "SELECT id FROM copied")
        .unwrap()
        .remove(0);
    let plan = Planner::new(&reloaded).plan(&statement).unwrap();
    let ExecutionResult::Query(query) = executor.execute(plan).unwrap() else {
        panic!("expected preserved rows");
    };
    assert_eq!(query.rows, vec![vec![SqlValue::Integer(1)]]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn explain_observes_index_creation_and_removal_in_the_catalog_overlay() {
    use alopex_sql::storage::SqlValue;
    use alopex_sql::{AlopexDialect, Parser, Planner};

    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
    // The table and index both remain uncommitted throughout this sequence.
    // Planning and EXPLAIN must use the same transaction-local catalog view.
    for (sql, expected_index) in [
        ("CREATE TABLE items (score INT)", None),
        ("INSERT INTO items VALUES (7), (9)", None),
        ("CREATE INDEX idx_items_score ON items (score)", None),
        (
            "EXPLAIN SELECT score FROM items WHERE score = 7",
            Some(true),
        ),
        (
            "EXPLAIN ANALYZE SELECT score FROM items WHERE score = 7",
            Some(true),
        ),
        ("DROP INDEX idx_items_score", None),
        (
            "EXPLAIN SELECT score FROM items WHERE score = 7",
            Some(false),
        ),
        (
            "EXPLAIN ANALYZE SELECT score FROM items WHERE score = 7",
            Some(false),
        ),
    ] {
        let statements = Parser::parse_sql(&AlopexDialect, sql).unwrap();
        let plan = {
            let guard = catalog.read().unwrap();
            let (_, overlay) = borrowed.split_parts();
            let view = TxnCatalogView::new(&*guard, &*overlay);
            Planner::new(&view).plan(&statements[0]).unwrap()
        };
        let result = executor.execute_in_txn(plan, &mut borrowed).unwrap();
        if let Some(expected_index) = expected_index {
            let ExecutionResult::Query(result) = result else {
                panic!("expected EXPLAIN query result: {sql}");
            };
            let SqlValue::Text(plan) = &result.rows[0][0] else {
                panic!("expected EXPLAIN text: {sql}");
            };
            assert_eq!(
                plan.contains("IndexScan index=idx_items_score"),
                expected_index,
                "{sql}: {plan}"
            );
            if !expected_index {
                assert!(plan.contains("Scan table=items"), "{plan}");
            }
        }
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn wrap_external_preserves_mode() {
    let store = MemoryKV::new();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();

    let borrowed = wrap_external(&mut txn, TxnMode::ReadOnly, &mut overlay);
    assert_eq!(borrowed.mode(), TxnMode::ReadOnly);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn execute_in_txn_readonly_rejects_dml() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, _catalog) = executor_with_persistent_catalog(store.clone());

    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed = wrap_external(&mut txn, TxnMode::ReadOnly, &mut overlay);

    let plan = LogicalPlan::DropTable {
        name: "users".to_string(),
        if_exists: true,
    };

    let err = executor.execute_in_txn(plan, &mut borrowed).unwrap_err();
    assert!(matches!(err, ExecutorError::ReadOnlyTransaction { .. }));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn execute_in_txn_readonly_allows_select() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());

    // ベースカタログにテーブルを投入（ReadOnly で SELECT を通す目的）。
    {
        let mut catalog = catalog.write().expect("catalog lock poisoned");
        let table = TableMetadata::new(
            "users",
            vec![alopex_sql::catalog::ColumnMetadata::new(
                "id",
                ResolvedType::Integer,
            )],
        )
        .with_table_id(1);
        catalog.create_table(table).unwrap();
    }

    let mut txn = store.begin(TxnMode::ReadOnly).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed = wrap_external(&mut txn, TxnMode::ReadOnly, &mut overlay);

    let plan = LogicalPlan::Scan {
        table: "users".to_string(),
        projection: Projection::All(vec!["id".to_string()]),
    };

    let result = executor.execute_in_txn(plan, &mut borrowed).unwrap();
    assert!(matches!(result, ExecutionResult::Query(_)));
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn overlay_visible_in_same_txn() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());
    let mut overlay = CatalogOverlay::new();

    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();

    // CREATE TABLE（コミット前なのでベースには反映されない）
    {
        let plan = LogicalPlan::CreateTable {
            table: TableMetadata::new(
                "users",
                vec![
                    alopex_sql::catalog::ColumnMetadata::new("id", ResolvedType::Integer)
                        .with_primary_key(true),
                ]
                .into_iter()
                .collect(),
            )
            .with_primary_key(vec!["id".to_string()]),
            if_not_exists: false,
            with_options: vec![],
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }

    {
        let catalog = catalog.read().expect("catalog lock poisoned");
        assert!(!catalog.table_exists("users"));
        let view = TxnCatalogView::new(&*catalog, &overlay);
        assert!(view.table_exists("users"));
    }

    // 同一トランザクション内で DROP TABLE が通る（オーバーレイで可視）
    {
        let plan = LogicalPlan::DropTable {
            name: "users".to_string(),
            if_exists: false,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistent_create_table_registers_unique_constraint_indexes() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());
    let mut overlay = CatalogOverlay::new();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut table = TableMetadata::new(
        "users",
        vec![
            alopex_sql::catalog::ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true),
            alopex_sql::catalog::ColumnMetadata::new("email", ResolvedType::Text),
        ],
    )
    .with_primary_key(vec!["id".to_string()]);
    table.constraints.push(TableConstraint::Unique {
        name: Some("users_email_key".to_string()),
        columns: vec!["email".to_string()],
        span: Span::default(),
    });

    let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
    executor
        .execute_in_txn(
            LogicalPlan::CreateTable {
                table,
                if_not_exists: false,
                with_options: vec![],
            },
            &mut borrowed,
        )
        .unwrap();
    drop(borrowed);

    let catalog = catalog.read().expect("catalog lock poisoned");
    let view = TxnCatalogView::new(&*catalog, &overlay);
    let index = view.get_index("users_email_key").expect("unique index");
    assert!(index.unique);
    assert_eq!(index.columns, vec!["email"]);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistent_constraint_index_name_collisions_preserve_existing_indexes() {
    for previously_committed in [false, true] {
        let store = Arc::new(MemoryKV::new());
        let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());
        let setup = "CREATE TABLE original (code TEXT, CONSTRAINT shared_key UNIQUE (code));
                     INSERT INTO original VALUES ('same');";
        if previously_committed {
            execute_sql_persistent(&mut executor, &catalog, setup).unwrap();
        }

        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        let mut overlay = CatalogOverlay::new();
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        if !previously_committed {
            for statement in Parser::parse_sql(&AlopexDialect, setup).unwrap() {
                let plan = {
                    let guard = catalog.read().unwrap();
                    let (_, overlay) = borrowed.split_parts();
                    Planner::new(&TxnCatalogView::new(&*guard, overlay))
                        .plan(&statement)
                        .unwrap()
                };
                executor.execute_in_txn(plan, &mut borrowed).unwrap();
            }
        }
        let statement = Parser::parse_sql(
            &AlopexDialect,
            "CREATE TABLE attempted (code TEXT, CONSTRAINT shared_key UNIQUE (code))",
        )
        .unwrap()
        .remove(0);
        let plan = {
            let guard = catalog.read().unwrap();
            let (_, overlay) = borrowed.split_parts();
            Planner::new(&TxnCatalogView::new(&*guard, overlay))
                .plan(&statement)
                .unwrap()
        };
        assert!(matches!(
            executor.execute_in_txn(plan, &mut borrowed),
            Err(ExecutorError::IndexAlreadyExists(name)) if name == "shared_key"
        ));
        drop(borrowed);
        assert!(
            !TxnCatalogView::new(&*catalog.read().unwrap(), &overlay).table_exists("attempted")
        );
        // Commit deliberately: rejection must precede all catalog writes, even
        // when the caller retains earlier successful work in the transaction.
        txn.commit_self().unwrap();
        drop(executor);
        drop(catalog);

        let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store.clone()).unwrap()));
        {
            let guard = catalog.read().unwrap();
            assert!(!guard.table_exists("attempted"));
            let index = guard.get_index("shared_key").unwrap();
            assert_eq!(index.table, "original");
            assert!(index.unique);
        }
        let mut executor = Executor::new(store, catalog.clone());
        assert!(matches!(
            execute_sql_persistent(
                &mut executor,
                &catalog,
                "INSERT INTO original VALUES ('same')"
            ),
            Err(ExecutorError::ConstraintViolation(
                alopex_sql::executor::ConstraintViolation::Unique { .. }
            ))
        ));
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistent_duplicate_constraint_index_names_leave_no_partial_table() {
    for second_column in ["a", "b"] {
        let store = Arc::new(MemoryKV::new());
        let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());
        let statement = Parser::parse_sql(
            &AlopexDialect,
            &format!(
                "CREATE TABLE attempted (a TEXT, b TEXT,
         CONSTRAINT shared_key UNIQUE (a), CONSTRAINT shared_key UNIQUE ({second_column}))"
            ),
        )
        .unwrap()
        .remove(0);
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap();
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        let mut overlay = CatalogOverlay::new();
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        assert!(matches!(
            executor.execute_in_txn(plan, &mut borrowed),
            Err(ExecutorError::IndexAlreadyExists(name)) if name == "shared_key"
        ));
        drop(borrowed);
        assert!(
            !TxnCatalogView::new(&*catalog.read().unwrap(), &overlay).table_exists("attempted")
        );
        txn.commit_self().unwrap();
        drop(executor);
        drop(catalog);
        let catalog = PersistentCatalog::load(store).unwrap();
        assert!(!catalog.table_exists("attempted"));
        assert!(!catalog.index_exists("shared_key"));
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn persistent_unique_constraints_survive_catalog_reload() {
    let directory = tempfile::tempdir().unwrap();
    let wal = directory.path().join("unique.wal");
    let store = Arc::new(MemoryKV::open(&wal).unwrap());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());
    execute_sql_persistent(
        &mut executor,
        &catalog,
        "
        CREATE TABLE users (id INT PRIMARY KEY, code TEXT UNIQUE, token TEXT);
        INSERT INTO users (id, code, token) VALUES (1, 'a', 'x'), (2, NULL, 'y');
        CREATE UNIQUE INDEX users_token_key ON users (token);
        CREATE TABLE pairs (id INT PRIMARY KEY, a TEXT, b INT, note TEXT, UNIQUE (a, b));
        INSERT INTO pairs VALUES (1, 'key', 2, 'first'), (2, 'key', NULL, 'null');
        ",
    )
    .expect("create persistent unique constraints");
    drop(executor);
    drop(catalog);
    store.flush().unwrap();
    drop(store);

    let store = Arc::new(MemoryKV::open(&wal).unwrap());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::load(store).unwrap()));
    let mut executor = Executor::new(catalog.read().unwrap().store().clone(), catalog.clone());

    for sql in [
        "INSERT INTO users (id, code, token) VALUES (3, 'a', 'z');",
        "INSERT INTO users (id, code, token) VALUES (3, 'b', 'x');",
        "UPDATE users SET code = 'a' WHERE id = 2;",
        "INSERT INTO pairs VALUES (3, 'key', 2, 'duplicate');",
        "UPDATE pairs SET b = 2 WHERE id = 2;",
    ] {
        let error = execute_sql_persistent(&mut executor, &catalog, sql).unwrap_err();
        assert!(
            matches!(
                error,
                ExecutorError::ConstraintViolation(
                    alopex_sql::executor::ConstraintViolation::Unique { value: Some(_), .. }
                )
            ),
            "expected UNIQUE violation for {sql}: {error}"
        );
    }

    let results = execute_sql_persistent(
        &mut executor,
        &catalog,
        "
        INSERT INTO users (id, code, token) VALUES (3, 'a', 'z') ON CONFLICT (code) DO NOTHING;
        INSERT INTO users (id, code, token) VALUES (3, NULL, NULL);
        SELECT id, code, token FROM users ORDER BY id;
        INSERT INTO pairs VALUES (3, 'key', NULL, 'null');
        INSERT INTO pairs VALUES (4, 'key', 2, 'updated')
            ON CONFLICT (a, b) DO UPDATE SET note = EXCLUDED.note;
        SELECT id, a, b, note FROM pairs ORDER BY id;
        ",
    )
    .expect("execute persistent unique checks");
    assert_eq!(results[0], ExecutionResult::RowsAffected(0));
    assert_eq!(results[1], ExecutionResult::RowsAffected(1));
    let ExecutionResult::Query(query) = &results[2] else {
        panic!("expected users query");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![
                SqlValue::Integer(1),
                SqlValue::Text("a".into()),
                SqlValue::Text("x".into())
            ],
            vec![
                SqlValue::Integer(2),
                SqlValue::Null,
                SqlValue::Text("y".into())
            ],
            vec![SqlValue::Integer(3), SqlValue::Null, SqlValue::Null],
        ]
    );
    assert_eq!(results[3], ExecutionResult::RowsAffected(1));
    assert_eq!(results[4], ExecutionResult::RowsAffected(1));
    let ExecutionResult::Query(query) = &results[5] else {
        panic!("expected pairs query");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![
                SqlValue::Integer(1),
                SqlValue::Text("key".into()),
                SqlValue::Integer(2),
                SqlValue::Text("updated".into())
            ],
            vec![
                SqlValue::Integer(2),
                SqlValue::Text("key".into()),
                SqlValue::Null,
                SqlValue::Text("null".into())
            ],
            vec![
                SqlValue::Integer(3),
                SqlValue::Text("key".into()),
                SqlValue::Null,
                SqlValue::Text("null".into())
            ],
        ]
    );
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn drop_table_in_txn_ignores_non_default_namespace() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());

    {
        let mut catalog = catalog.write().expect("catalog lock poisoned");
        let mut table = TableMetadata::new(
            "users",
            vec![
                alopex_sql::catalog::ColumnMetadata::new("id", ResolvedType::Integer)
                    .with_primary_key(true),
                alopex_sql::catalog::ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_primary_key(vec!["id".to_string()]);
        table.catalog_name = "main".to_string();
        table.namespace_name = "analytics".to_string();
        catalog.create_table(table).unwrap();
    }

    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();

    {
        let plan = LogicalPlan::DropTable {
            name: "users".to_string(),
            if_exists: true,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        let result = executor.execute_in_txn(plan, &mut borrowed).unwrap();
        assert!(matches!(result, ExecutionResult::Success));
    }

    {
        let catalog = catalog.read().expect("catalog lock poisoned");
        assert!(catalog.table_exists("users"));
    }

    {
        let plan = LogicalPlan::DropTable {
            name: "users".to_string(),
            if_exists: false,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        let err = executor.execute_in_txn(plan, &mut borrowed).unwrap_err();
        assert!(matches!(err, ExecutorError::TableNotFound(_)));
    }

    {
        let catalog = catalog.read().expect("catalog lock poisoned");
        assert!(catalog.table_exists("users"));
    }

    drop(txn);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn drop_index_in_txn_ignores_non_default_namespace() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());

    {
        let mut catalog = catalog.write().expect("catalog lock poisoned");
        let mut table = TableMetadata::new(
            "users",
            vec![
                alopex_sql::catalog::ColumnMetadata::new("id", ResolvedType::Integer)
                    .with_primary_key(true),
                alopex_sql::catalog::ColumnMetadata::new("name", ResolvedType::Text),
            ],
        )
        .with_primary_key(vec!["id".to_string()]);
        table.catalog_name = "main".to_string();
        table.namespace_name = "analytics".to_string();
        catalog.create_table(table).unwrap();

        let mut index = alopex_sql::catalog::IndexMetadata::new(
            1,
            "idx_users_name",
            "users",
            vec!["name".to_string()],
        );
        index.catalog_name = "main".to_string();
        index.namespace_name = "analytics".to_string();
        catalog.create_index(index).unwrap();
    }

    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();

    {
        let plan = LogicalPlan::DropIndex {
            name: "idx_users_name".to_string(),
            if_exists: true,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        let result = executor.execute_in_txn(plan, &mut borrowed).unwrap();
        assert!(matches!(result, ExecutionResult::Success));
    }

    {
        let catalog = catalog.read().expect("catalog lock poisoned");
        assert!(catalog.index_exists("idx_users_name"));
    }

    {
        let plan = LogicalPlan::DropIndex {
            name: "idx_users_name".to_string(),
            if_exists: false,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        let err = executor.execute_in_txn(plan, &mut borrowed).unwrap_err();
        assert!(matches!(err, ExecutorError::IndexNotFound(_)));
    }

    {
        let catalog = catalog.read().expect("catalog lock poisoned");
        assert!(catalog.index_exists("idx_users_name"));
    }

    drop(txn);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn hnsw_flush_on_success() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, _catalog) = executor_with_persistent_catalog(store.clone());
    let mut overlay = CatalogOverlay::new();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();

    // CREATE TABLE
    {
        let plan = LogicalPlan::CreateTable {
            table: vector_table(),
            if_not_exists: false,
            with_options: vec![],
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }

    // CREATE INDEX (HNSW)
    {
        let plan = LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "idx_items_embedding",
                "items",
                vec!["embedding".to_string()],
            )
            .with_method(IndexMethod::Hnsw),
            if_not_exists: false,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }

    // INSERT（HNSW は staged -> execute_in_txn が flush する）
    {
        let id = TypedExpr::literal(
            Literal::Number("1".to_string()),
            ResolvedType::Integer,
            Span::default(),
        );
        let embedding = TypedExpr {
            kind: TypedExprKind::VectorLiteral(vec![0.1, 0.2, 0.3]),
            resolved_type: ResolvedType::Vector {
                dimension: 3,
                metric: VectorMetric::Cosine,
            },
            span: Span::default(),
        };
        let plan = LogicalPlan::Insert {
            table: "items".to_string(),
            columns: vec!["id".to_string(), "embedding".to_string()],
            values: vec![vec![id, embedding]],
            conflict: None,
            returning: None,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }

    // flush が効いていれば、同一トランザクション内でも HNSW インデックスがロードできる。
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    let (results, _) = index.search(&[0.1f32, 0.2, 0.3], 1, None).unwrap();
    assert_eq!(results.len(), 1);
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn hnsw_abandon_on_drop() {
    let store = Arc::new(MemoryKV::new());
    let (mut executor, catalog) = executor_with_persistent_catalog(store.clone());
    let mut overlay = CatalogOverlay::new();
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();

    // CREATE TABLE + HNSW INDEX（ベースとなるインデックスを作成）
    {
        let plan = LogicalPlan::CreateTable {
            table: vector_table(),
            if_not_exists: false,
            with_options: vec![],
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }
    {
        let plan = LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "idx_items_embedding",
                "items",
                vec!["embedding".to_string()],
            )
            .with_method(IndexMethod::Hnsw),
            if_not_exists: false,
        };
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        executor.execute_in_txn(plan, &mut borrowed).unwrap();
    }

    // dirty 状態だけ作って Drop させる（flush しない）。
    {
        let mut borrowed = wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        let (mut sql_txn, overlay) = borrowed.split_parts();
        let catalog_guard = catalog.read().expect("catalog lock poisoned");
        let table_view = TxnCatalogView::new(&*catalog_guard, &*overlay);
        let index = table_view.get_index("idx_items_embedding").unwrap().clone();

        // staged だけ作る（Drop により rollback される/少なくとも残らないことを期待）
        let entry = sql_txn.hnsw_entry_mut(&index.name).unwrap();
        entry
            .index
            .upsert_staged(
                &1u64.to_be_bytes(),
                &[0.1f32, 0.2, 0.3],
                &[],
                &mut entry.state,
            )
            .unwrap();
        entry.dirty = true;
    }

    // Drop 後もインデックスが壊れずロードできる（rollback が安全に働くことの確認）。
    let index = HnswIndex::load("idx_items_embedding", &mut txn).unwrap();
    let (results, _) = index.search(&[0.1f32, 0.2, 0.3], 1, None).unwrap();
    assert!(results.is_empty());
}
