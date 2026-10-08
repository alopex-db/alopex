use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::SqlValue;

#[path = "support/target_max_guard.rs"]
mod target_max_guard;

fn execute_sql(sql: &str) -> Result<Vec<ExecutionResult>, ExecutorError> {
    let dialect = AlopexDialect;
    let statements = Parser::parse_sql(&dialect, sql).expect("parse sql");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());
    let mut results = Vec::new();
    for stmt in statements {
        let guard = catalog.read().unwrap();
        let plan = Planner::new(&*guard).plan(&stmt)?;
        drop(guard);
        results.push(executor.execute(plan)?);
    }
    Ok(results)
}

fn last_query(sql: &str) -> alopex_sql::executor::QueryResult {
    execute_sql(sql)
        .expect("execute sql")
        .into_iter()
        .rev()
        .find_map(|result| match result {
            ExecutionResult::Query(query) => Some(query),
            _ => None,
        })
        .expect("query result")
}

fn dml_statement_fixture(count: i32) -> String {
    let mut sql = String::from("CREATE TABLE hw (id INTEGER PRIMARY KEY, v INTEGER);");
    // Keep each INSERT inside the parser's MessagePack collection budget.
    for first in (1..=count).step_by(128) {
        let values = (first..=(first + 127).min(count))
            .map(|id| format!("({id},{id})"))
            .collect::<Vec<_>>()
            .join(",");
        sql.push_str(&format!("INSERT INTO hw VALUES {values};"));
    }
    sql
}

#[test]
fn dml_statement_distinct_subqueries_inside_case_keep_distinct_results() {
    let query = last_query(&format!(
        "{} UPDATE hw SET v=CASE WHEN id<=512 \
         THEN CAST((SELECT MAX(v) FROM hw)+1 AS INTEGER) \
         ELSE (SELECT MIN(v) FROM hw)+1 END; SELECT id,v FROM hw ORDER BY id;",
        dml_statement_fixture(600)
    ));
    assert_eq!(query.rows.len(), 600);
    for (row, id) in query.rows.iter().zip(1..=600) {
        assert_eq!(
            row,
            &[
                SqlValue::Integer(id),
                SqlValue::Integer(if id <= 512 { 601 } else { 2 })
            ]
        );
    }
}

#[test]
fn dml_statement_executor_joined_plans_read_before_all_batches() {
    for (sql, is_update) in [
        (
            "UPDATE hw SET v=source.v+1 FROM source WHERE source.id=1",
            true,
        ),
        ("DELETE FROM hw USING source WHERE source.id=1", false),
    ] {
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = Executor::new(Arc::new(MemoryKV::new()), catalog.clone());
        let setup = format!(
            "{} CREATE TABLE source(id INTEGER PRIMARY KEY, v INTEGER);",
            dml_statement_fixture(600)
        );
        for statement in Parser::parse_sql(&AlopexDialect, &setup).unwrap() {
            let plan = Planner::new(&*catalog.read().unwrap())
                .plan(&statement)
                .unwrap();
            executor.execute(plan).unwrap();
        }
        let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
        let mut plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap();
        // This tests the public LogicalPlan/Executor contract, not SQL alias or
        // joined scalar-subquery planning. Both tables have identical schemas;
        // reuse the typed source indexes with the target as the physical source.
        let (alopex_sql::LogicalPlan::Update { join_source, .. }
        | alopex_sql::LogicalPlan::Delete { join_source, .. }) = &mut plan
        else {
            panic!("expected joined DML plan")
        };
        join_source.as_mut().unwrap().table = "hw".into();
        let result = executor.execute(plan).unwrap();
        // UPDATE reports changed rows: id=2 already has its final value 2.
        let expected_changes = if is_update { 599 } else { 600 };
        assert_eq!(
            result,
            ExecutionResult::RowsAffected(expected_changes),
            "{sql}"
        );
        let statement = Parser::parse_sql(&AlopexDialect, "SELECT id,v FROM hw ORDER BY id")
            .unwrap()
            .remove(0);
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap();
        let ExecutionResult::Query(query) = executor.execute(plan).unwrap() else {
            panic!("expected query")
        };
        if is_update {
            assert_eq!(query.rows.len(), 600);
            for (row, id) in query.rows.iter().zip(1..=600) {
                assert_eq!(row, &[SqlValue::Integer(id), SqlValue::Integer(2)]);
            }
        } else {
            assert!(query.rows.is_empty());
        }
    }
}

#[test]
fn dml_statement_auto_transaction_rolls_back_late_apply_error() {
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::new(MemoryKV::new()), catalog.clone());
    let mut run = |sql: &str| -> Result<Vec<ExecutionResult>, ExecutorError> {
        Parser::parse_sql(&AlopexDialect, sql)
            .unwrap()
            .iter()
            .map(|statement| {
                let plan = Planner::new(&*catalog.read().unwrap()).plan(statement)?;
                executor.execute(plan)
            })
            .collect()
    };
    run(&dml_statement_fixture(600)).unwrap();
    // Batch one can insert ids 1001..1512. Batch two conflicts with id 1001.
    let error = run(
        "UPDATE hw SET id=CASE WHEN id<=512 THEN id+1000 ELSE 1001 END, \
        v=(SELECT MAX(v) FROM hw)",
    )
    .unwrap_err();
    assert!(
        matches!(error, ExecutorError::ConstraintViolation(_)),
        "{error}"
    );
    let results = run("SELECT id,v FROM hw ORDER BY id").unwrap();
    let ExecutionResult::Query(query) = &results[0] else {
        panic!("expected rows")
    };
    assert_eq!(query.rows.len(), 600);
    for (row, id) in query.rows.iter().zip(1..=600) {
        assert_eq!(row, &vec![SqlValue::Integer(id); 2]);
    }
}

#[test]
fn dml_statement_scalar_update_reads_before_all_batches() {
    for count in [600, 1025] {
        let query = last_query(&format!(
            "{} UPDATE hw SET v=(SELECT MAX(v) FROM hw)+1; SELECT id,v FROM hw ORDER BY id;",
            dml_statement_fixture(count)
        ));
        assert_eq!(query.rows.len(), count as usize);
        for (row, id) in query.rows.iter().zip(1..=count) {
            assert_eq!(row, &[SqlValue::Integer(id), SqlValue::Integer(count + 1)]);
        }
    }
}

#[test]
fn dml_statement_delete_membership_reads_before_all_batches() {
    for count in [600, 1025] {
        let query = last_query(&format!(
            "{} DELETE FROM hw WHERE id<=512 OR v IN \
             (SELECT id+512 FROM hw WHERE id<={}); SELECT id FROM hw ORDER BY id;",
            dml_statement_fixture(count),
            count - 512
        ));
        assert_eq!(query.rows.len(), 0, "row count {count}");
    }
}

#[test]
fn dml_statement_correlated_update_and_delete_read_original_rows() {
    let query = last_query(&format!(
        "{} UPDATE hw SET v=(SELECT MAX(s.v) FROM hw s WHERE s.id<=hw.id)+1000; \
         SELECT id,v FROM hw ORDER BY id;",
        dml_statement_fixture(600)
    ));
    assert_eq!(query.rows.len(), 600);
    for (row, id) in query.rows.iter().zip(1..=600) {
        assert_eq!(row, &[SqlValue::Integer(id), SqlValue::Integer(id + 1000)]);
    }
    let query = last_query(&format!(
        "{} DELETE FROM hw WHERE EXISTS \
         (SELECT 1 FROM hw s WHERE s.id=1 AND hw.id>=s.id); SELECT id FROM hw;",
        dml_statement_fixture(600)
    ));
    assert!(query.rows.is_empty());
}

#[test]
fn dml_statement_sees_previous_uncommitted_statement_and_resets_cache() {
    use alopex_core::kv::{KVStore, KVTransaction};
    use alopex_core::types::TxnMode;
    use alopex_sql::catalog::{CatalogOverlay, PersistentCatalog, TxnCatalogView};
    use alopex_sql::storage::TxnBridge;

    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed =
        TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
    let sql = format!(
        "{} UPDATE hw SET v=v+1000; \
         UPDATE hw SET v=(SELECT MAX(v) FROM hw)+1; \
         SELECT DISTINCT v FROM hw; \
         UPDATE hw SET v=(SELECT MAX(v) FROM hw)+1; SELECT DISTINCT v FROM hw;",
        dml_statement_fixture(600)
    );
    let mut observed = Vec::new();
    for statement in Parser::parse_sql(&AlopexDialect, &sql).unwrap() {
        let plan = {
            let guard = catalog.read().unwrap();
            let (_, overlay) = borrowed.split_parts();
            Planner::new(&TxnCatalogView::new(&*guard, overlay))
                .plan(&statement)
                .unwrap()
        };
        if let ExecutionResult::Query(result) =
            executor.execute_in_txn(plan, &mut borrowed).unwrap()
        {
            observed.push(result.rows);
        }
    }
    assert_eq!(
        observed,
        vec![
            vec![vec![SqlValue::Integer(1601)]],
            vec![vec![SqlValue::Integer(1602)]],
        ]
    );
    drop(borrowed);
    txn.rollback_self().unwrap();
}

#[test]
fn dml_statement_returning_preserves_updated_and_deleted_rows() {
    let updated = last_query(&format!(
        "{} UPDATE hw SET v=(SELECT MAX(v) FROM hw)+1 RETURNING id,v;",
        dml_statement_fixture(600)
    ));
    assert_eq!(updated.rows.len(), 600);
    for (row, id) in updated.rows.iter().zip(1..=600) {
        assert_eq!(row, &[SqlValue::Integer(id), SqlValue::Integer(601)]);
    }
    let deleted = last_query(&format!(
        "{} DELETE FROM hw WHERE id<=512 OR v IN \
         (SELECT id+512 FROM hw WHERE id<=88) RETURNING id,v;",
        dml_statement_fixture(600)
    ));
    assert_eq!(deleted.rows.len(), 600);
    for (row, id) in deleted.rows.iter().zip(1..=600) {
        assert_eq!(row, &[SqlValue::Integer(id), SqlValue::Integer(id)]);
    }
}

#[test]
fn dml_statement_late_scalar_error_preserves_prior_uncommitted_writes() {
    use alopex_core::kv::{KVStore, KVTransaction};
    use alopex_core::types::TxnMode;
    use alopex_sql::catalog::{CatalogOverlay, PersistentCatalog, TxnCatalogView};
    use alopex_sql::storage::TxnBridge;

    let store = Arc::new(MemoryKV::new());
    let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed =
        TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
    let mut run = |sql: &str| -> Result<Vec<ExecutionResult>, ExecutorError> {
        let mut results = Vec::new();
        for statement in Parser::parse_sql(&AlopexDialect, sql).unwrap() {
            let plan = {
                let guard = catalog.read().unwrap();
                let (_, overlay) = borrowed.split_parts();
                Planner::new(&TxnCatalogView::new(&*guard, overlay))
                    .plan(&statement)
                    .unwrap()
            };
            results.push(executor.execute_in_txn(plan, &mut borrowed)?);
        }
        Ok(results)
    };
    run(&format!(
        "{} UPDATE hw SET v=v+1000;",
        dml_statement_fixture(600)
    ))
    .unwrap();
    let error = run("UPDATE hw SET v=(SELECT s.v FROM hw s \
         WHERE s.id=hw.id OR (hw.id>512 AND s.id=1))+1;")
    .unwrap_err();
    assert!(matches!(error, ExecutorError::InvalidOperation { .. }));
    let rows = run("SELECT id,v FROM hw ORDER BY id").unwrap();
    let ExecutionResult::Query(query) = &rows[0] else {
        panic!("expected original rows");
    };
    assert_eq!(query.rows.len(), 600);
    for (row, id) in query.rows.iter().zip(1..=600) {
        assert_eq!(row, &[SqlValue::Integer(id), SqlValue::Integer(id + 1000)]);
    }
    drop(run);
    drop(borrowed);
    txn.rollback_self().unwrap();
}

fn joined_subquery_setup(statement: &str) -> String {
    format!(
        "CREATE TABLE target (id INT PRIMARY KEY, v INT);
         CREATE TABLE source (id INT PRIMARY KEY, v INT);
         CREATE TABLE lookup (id INT PRIMARY KEY, bonus INT);
         INSERT INTO target VALUES (1, 10), (2, 20);
         INSERT INTO source VALUES (1, 100), (2, 200);
         INSERT INTO lookup VALUES (1, 10), (2, 20);
         {statement};
         SELECT id, v FROM target ORDER BY id;"
    )
}

#[test]
fn composition_target_max_joined_update_terminates() {
    composition_assert_target_max("UPDATE target SET v=v+1 FROM source s WHERE ", true, false);
}

#[test]
fn composition_target_max_joined_delete_terminates() {
    composition_assert_target_max("DELETE FROM target USING source s WHERE ", true, true);
}

#[test]
fn composition_target_max_subquery_update_terminates() {
    composition_assert_target_max(
        "UPDATE target SET v=v+(SELECT n FROM lookup) WHERE ",
        false,
        false,
    );
}

#[test]
fn composition_target_max_subquery_delete_terminates() {
    composition_assert_target_max(
        "DELETE FROM target WHERE (SELECT n FROM lookup)=1 AND ",
        false,
        true,
    );
}

#[test]
fn composition_target_max_plain_update_match_and_no_match() {
    composition_assert_target_max("UPDATE target SET v=v+1 WHERE ", false, false);
}

#[test]
fn composition_target_max_plain_delete_match_and_no_match() {
    composition_assert_target_max("DELETE FROM target WHERE ", false, true);
}

fn composition_assert_target_max(action: &str, joined: bool, delete: bool) {
    use alopex_core::{KVStore, KVTransaction, TxnMode};
    use alopex_sql::catalog::Catalog;
    use alopex_sql::storage::{KeyEncoder, StorageError, TableStorage};

    let mut repeated = Vec::new();
    for matched in [true, false] {
        let store = Arc::new(target_max_guard::GuardedStore::default());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = Executor::new(store.clone(), catalog.clone());
        for statement in Parser::parse_sql(&AlopexDialect,
            "CREATE TABLE target (id INT, v INT); CREATE TABLE source (sid INT);
             INSERT INTO source VALUES (1); CREATE TABLE lookup (n INT); INSERT INTO lookup VALUES (1);"
        ).unwrap() {
            let plan = Planner::new(&*catalog.read().unwrap()).plan(&statement).unwrap();
            executor.execute(plan).unwrap();
        }
        let table = catalog.read().unwrap().get_table("target").unwrap().clone();
        let original = vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(2), SqlValue::Integer(20)],
        ];
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        {
            let mut storage = TableStorage::new(&mut txn, &table);
            storage.insert(1, &original[0]).unwrap();
            storage.insert(u64::MAX, &original[1]).unwrap();
        }
        txn.commit_self().unwrap();
        let predicate = if matched {
            "target.id>0"
        } else {
            "target.id<0"
        };
        let sql = format!(
            "{action}{}{predicate}",
            if joined { "s.sid=1 AND " } else { "" }
        );
        let stmt = Parser::parse_sql(&AlopexDialect, &sql).unwrap().remove(0);
        let plan = Planner::new(&*catalog.read().unwrap()).plan(&stmt).unwrap();
        store.arm(KeyEncoder::row_key(table.table_id, u64::MAX));
        let result = executor.execute(plan);
        let scans = store.disarm();
        let succeeded = result.is_ok();
        match result {
            Ok(ExecutionResult::RowsAffected(count)) => {
                assert_eq!(count, if matched { 2 } else { 0 })
            }
            Ok(other) => panic!("unexpected result: {other:?}"),
            Err(error) => {
                assert!(
                    matches!(&error,
                        ExecutorError::Storage(StorageError::KvError(alopex_core::Error::Io(io)))
                            if io.kind() == std::io::ErrorKind::Other && io.to_string() == target_max_guard::MARKER
                    ),
                    "unexpected RED cause: {error}"
                );
                assert_eq!(scans, 2, "the bounded guard must own the error");
                repeated.push(format!("matched={matched}: {error}"));
            }
        }
        let statement = Parser::parse_sql(&AlopexDialect, "SELECT id,v FROM target ORDER BY id")
            .unwrap()
            .remove(0);
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap();
        let ExecutionResult::Query(query) = executor.execute(plan).unwrap() else {
            panic!("expected rows")
        };
        let expected = if !succeeded || !matched {
            original
        } else if delete {
            Vec::new()
        } else {
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(11)],
                vec![SqlValue::Integer(2), SqlValue::Integer(21)],
            ]
        };
        assert_eq!(query.rows, expected, "{sql}");
        eprintln!(
            "target_max action={action:?} matched={matched} succeeded={succeeded} max_start_scans={scans}; row oracle passed"
        );
    }
    // Both match and no-match execute before reporting the finite RED.
    assert!(repeated.is_empty(), "{action}: {repeated:?}");
}

#[test]
fn composition_snapshot_joined_update_alias_reads_original_rows() {
    for count in [600, 1025] {
        let query = last_query(&format!(
            "{} CREATE TABLE source (id INT); INSERT INTO source VALUES (1);
             UPDATE hw SET v=0 FROM source AS s
             WHERE hw.id>=s.id AND (SELECT p.v FROM hw p WHERE p.id=1 AND hw.id>=p.id)>0;
             SELECT id,v FROM hw ORDER BY id;",
            dml_statement_fixture(count)
        ));
        assert_eq!(
            query.rows,
            (1..=count)
                .map(|id| vec![SqlValue::Integer(id), SqlValue::Integer(0)])
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn composition_snapshot_joined_delete_alias_reads_original_rows() {
    for count in [600, 1025] {
        let query = last_query(&format!(
            "{} CREATE TABLE source (id INT); INSERT INTO source VALUES (1);
             DELETE FROM hw USING source s
             WHERE hw.id>=s.id AND (SELECT p.v FROM hw p WHERE p.id=1 AND hw.id>=p.id)>0;
             SELECT id,v FROM hw ORDER BY id;",
            dml_statement_fixture(count)
        ));
        assert!(query.rows.is_empty(), "count {count}: {:?}", query.rows);
    }
}

#[test]
fn composition_late_joined_update_preserves_borrowed_writes() {
    composition_assert_late_error("UPDATE hw SET v=0 FROM source AS s WHERE hw.id>=s.id AND ");
}

#[test]
fn composition_late_joined_delete_preserves_borrowed_writes() {
    composition_assert_late_error("DELETE FROM hw USING source s WHERE hw.id>=s.id AND ");
}

#[test]
fn composition_late_merge_insert_preserves_borrowed_writes() {
    composition_assert_late_error("MERGE");
}

fn composition_assert_late_error(action: &str) {
    use alopex_core::kv::{KVStore, KVTransaction};
    use alopex_core::types::TxnMode;
    use alopex_sql::catalog::{CatalogOverlay, PersistentCatalog, TxnCatalogView};
    use alopex_sql::storage::TxnBridge;

    for (count, boundary) in [(600, 512), (1025, 1024)] {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
        let mut executor = Executor::new(store.clone(), catalog.clone());
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        let mut overlay = CatalogOverlay::new();
        let mut borrowed =
            TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        let mut run = |sql: &str| -> Result<Vec<ExecutionResult>, ExecutorError> {
            let mut results = Vec::new();
            for statement in Parser::parse_sql(&AlopexDialect, sql).unwrap() {
                let plan = {
                    let guard = catalog.read().unwrap();
                    let (_, overlay) = borrowed.split_parts();
                    Planner::new(&TxnCatalogView::new(&*guard, overlay)).plan(&statement)?
                };
                results.push(executor.execute_in_txn(plan, &mut borrowed)?);
            }
            Ok(results)
        };
        run(&format!(
            "{} UPDATE hw SET v=v+1000;
             CREATE TABLE source (id INT); INSERT INTO source VALUES (1);
             CREATE TABLE lookup (n INT); INSERT INTO lookup VALUES (1),(2);
             CREATE TABLE sink (id INT PRIMARY KEY, v INT); INSERT INTO sink VALUES (0,42);",
            dml_statement_fixture(count)
        ))
        .unwrap();
        let scalar = format!("(SELECT n FROM lookup WHERE n=1 OR hw.id>{boundary})");
        let statement = if action == "MERGE" {
            format!(
                "MERGE INTO sink USING hw ON sink.id=hw.id WHEN NOT MATCHED THEN INSERT (id,v) VALUES (hw.id,{scalar})"
            )
        } else {
            format!("{action}{scalar}>0")
        };
        let error = run(&statement).unwrap_err();
        assert!(
            matches!(&error, ExecutorError::InvalidOperation { operation, reason }
            if operation == "execute_scalar_subquery" && reason == "scalar subquery returned multiple rows"),
            "count {count}: {error}"
        );
        let results =
            run("SELECT id,v FROM hw ORDER BY id; SELECT id,v FROM sink ORDER BY id;").unwrap();
        let ExecutionResult::Query(hw) = &results[0] else {
            panic!("expected hw")
        };
        assert_eq!(
            hw.rows,
            (1..=count)
                .map(|id| vec![SqlValue::Integer(id), SqlValue::Integer(id + 1000)])
                .collect::<Vec<_>>()
        );
        let ExecutionResult::Query(sink) = &results[1] else {
            panic!("expected sink")
        };
        assert_eq!(
            sink.rows,
            vec![vec![SqlValue::Integer(0), SqlValue::Integer(42)]]
        );
        drop(run);
        drop(borrowed);
        txn.rollback_self().unwrap();
    }
}

#[test]
fn joined_subquery_update_scalar_assignment() {
    let result = last_query(&joined_subquery_setup(
        "UPDATE target SET v = source.v + (SELECT MAX(bonus) FROM lookup)
         FROM source WHERE target.id = source.id",
    ));
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(120)],
            vec![SqlValue::Integer(2), SqlValue::Integer(220)],
        ]
    );
}

#[test]
fn joined_subquery_update_correlated_assignment() {
    let result = last_query(&joined_subquery_setup(
        "UPDATE target SET v = source.v +
            (SELECT bonus FROM lookup WHERE lookup.id = target.id AND lookup.id = source.id)
         FROM source WHERE target.id = source.id",
    ));
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(110)],
            vec![SqlValue::Integer(2), SqlValue::Integer(220)],
        ]
    );
}

#[test]
fn joined_subquery_update_scalar_condition() {
    let result = last_query(&joined_subquery_setup(
        "UPDATE target SET v = source.v FROM source
         WHERE target.id = source.id AND target.id = (SELECT MAX(id) FROM lookup)",
    ));
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(2), SqlValue::Integer(200)],
        ]
    );
}

#[test]
fn joined_subquery_update_in_condition() {
    let result = last_query(&joined_subquery_setup(
        "UPDATE target SET v = source.v FROM source
         WHERE target.id = source.id AND source.id IN (SELECT id FROM lookup WHERE bonus = 20)",
    ));
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(2), SqlValue::Integer(200)],
        ]
    );
}

#[test]
fn joined_subquery_delete_scalar_condition() {
    let result = last_query(&joined_subquery_setup(
        "DELETE FROM target USING source
         WHERE target.id = source.id AND source.id = (SELECT MAX(id) FROM lookup)",
    ));
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(10)]]
    );
}

#[test]
fn joined_subquery_delete_correlated_in_condition() {
    let result = last_query(&joined_subquery_setup(
        "DELETE FROM target USING source WHERE target.id = source.id
         AND target.id IN (SELECT id FROM lookup WHERE lookup.id = source.id AND bonus = 20)",
    ));
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(10)]]
    );
}

#[test]
fn joined_subquery_merge_scalar_update_and_insert() {
    let result = last_query(&joined_subquery_setup(
        "INSERT INTO source VALUES (3, 300);
         INSERT INTO lookup VALUES (3, 30);
         MERGE INTO target USING source ON target.id = source.id
         WHEN MATCHED THEN UPDATE SET v = source.v + (SELECT MAX(bonus) FROM lookup)
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, (SELECT MAX(bonus) FROM lookup))",
    ));
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(130)],
            vec![SqlValue::Integer(2), SqlValue::Integer(230)],
            vec![SqlValue::Integer(3), SqlValue::Integer(30)],
        ]
    );
}

#[test]
fn joined_subquery_merge_correlated_on_and_clause() {
    let result = last_query(&joined_subquery_setup(
        "MERGE INTO target USING source ON target.id = source.id
         AND source.id IN (SELECT id FROM lookup WHERE lookup.id = target.id)
         WHEN MATCHED AND (SELECT bonus FROM lookup WHERE lookup.id = source.id) > 15
         THEN UPDATE SET v = source.v + (SELECT bonus FROM lookup WHERE lookup.id = source.id)",
    ));
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(2), SqlValue::Integer(220)],
        ]
    );
}

#[test]
fn joined_subquery_rejects_multiple_columns_with_actual_arity() {
    for statement in [
        "UPDATE target SET v = (SELECT id, bonus FROM lookup) FROM source WHERE target.id = source.id",
        "UPDATE target SET v = 1 FROM source WHERE target.id = (SELECT id, bonus FROM lookup)",
        "DELETE FROM target USING source WHERE target.id IN (SELECT id, bonus FROM lookup)",
        "MERGE INTO target USING source ON target.id = (SELECT id, bonus FROM lookup) WHEN MATCHED THEN UPDATE SET v = source.v",
    ] {
        let error = execute_sql(&joined_subquery_setup(statement)).unwrap_err();
        assert!(
            matches!(error,
                ExecutorError::Planner(alopex_sql::planner::PlannerError::TypeMismatch { ref expected, ref found, .. })
                    if expected == "one-column subquery" && found == "2 columns"
            ),
            "{statement}: {error}"
        );
    }
}

#[test]
fn joined_condition_update_rejects_non_boolean_scalar() {
    let error = execute_sql(&joined_subquery_setup(
        "UPDATE target SET v = source.v FROM source WHERE (SELECT MAX(id) FROM lookup)",
    ))
    .unwrap_err();
    assert!(
        matches!(error,
            ExecutorError::Planner(alopex_sql::planner::PlannerError::TypeMismatch { ref expected, ref found, .. })
                if expected == "Boolean" && found == "INTEGER"
        ),
        "{error}"
    );
}

#[test]
fn joined_condition_delete_rejects_non_boolean_scalar() {
    let error = execute_sql(&joined_subquery_setup(
        "DELETE FROM target USING source WHERE (SELECT MAX(id) FROM lookup)",
    ))
    .unwrap_err();
    assert!(
        matches!(error,
            ExecutorError::Planner(alopex_sql::planner::PlannerError::TypeMismatch { ref expected, ref found, .. })
                if expected == "Boolean" && found == "INTEGER"
        ),
        "{error}"
    );
}

#[test]
fn joined_condition_plain_boolean_controls() {
    assert_eq!(
        last_query(&joined_subquery_setup(
            "UPDATE target SET v = source.v FROM source WHERE target.id = source.id AND source.id = 2",
        )).rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(2), SqlValue::Integer(200)],
        ]
    );
    assert_eq!(
        last_query(&joined_subquery_setup(
            "DELETE FROM target USING source WHERE target.id = source.id AND source.id = 2",
        ))
        .rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(10)]]
    );
}

#[test]
fn joined_source_batch_late_match_and_no_match() {
    let values = (1..=514)
        .map(|id| format!("({id}, {id})"))
        .collect::<Vec<_>>()
        .join(",");
    for statement in [
        "UPDATE target SET v = source.v FROM source WHERE source.id >= 513 AND target.id = (SELECT MAX(id) FROM lookup)",
        "DELETE FROM target USING source WHERE source.id >= 513 AND target.id = (SELECT MAX(id) FROM lookup)",
    ] {
        let rows = last_query(&format!(
            "CREATE TABLE target (id INT PRIMARY KEY, v INT);
             CREATE TABLE source (id INT PRIMARY KEY, v INT);
             CREATE TABLE lookup (id INT);
             INSERT INTO target VALUES (1,10),(2,20);
             INSERT INTO source VALUES {values};
             INSERT INTO lookup VALUES (2);
             {statement}; SELECT id,v FROM target ORDER BY id;"
        ))
        .rows;
        let expected = if statement.starts_with("UPDATE") {
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(10)],
                vec![SqlValue::Integer(2), SqlValue::Integer(513)],
            ]
        } else {
            vec![vec![SqlValue::Integer(1), SqlValue::Integer(10)]]
        };
        assert_eq!(rows, expected, "{statement}");
    }
}

#[test]
fn joined_source_internal_row_id_max_terminates() {
    use alopex_core::{KVStore, KVTransaction, TxnMode};
    use alopex_sql::catalog::Catalog;
    use alopex_sql::storage::TableStorage;

    for action in [
        "UPDATE target SET v = source.v FROM source",
        "DELETE FROM target USING source",
    ] {
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let store = Arc::new(MemoryKV::new());
        let mut executor = Executor::new(store.clone(), catalog.clone());
        for stmt in Parser::parse_sql(
            &AlopexDialect,
            "CREATE TABLE target (id INT, v INT); CREATE TABLE source (id INT, v INT);
             INSERT INTO target VALUES (1,10),(2,20);",
        )
        .unwrap()
        {
            let plan = Planner::new(&*catalog.read().unwrap()).plan(&stmt).unwrap();
            executor.execute(plan).unwrap();
        }
        let source = catalog.read().unwrap().get_table("source").unwrap().clone();
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        TableStorage::new(&mut txn, &source)
            .insert(u64::MAX, &[SqlValue::Integer(2), SqlValue::Integer(200)])
            .unwrap();
        txn.commit_self().unwrap();
        for stmt in Parser::parse_sql(
            &AlopexDialect,
            &format!("{action} WHERE target.id = source.id; SELECT id,v FROM target ORDER BY id;"),
        )
        .unwrap()
        {
            let plan = Planner::new(&*catalog.read().unwrap()).plan(&stmt).unwrap();
            if let ExecutionResult::Query(query) = executor.execute(plan).unwrap() {
                let expected = if action.starts_with("UPDATE") {
                    vec![
                        vec![SqlValue::Integer(1), SqlValue::Integer(10)],
                        vec![SqlValue::Integer(2), SqlValue::Integer(200)],
                    ]
                } else {
                    vec![vec![SqlValue::Integer(1), SqlValue::Integer(10)]]
                };
                assert_eq!(query.rows, expected, "{action}");
            }
        }
    }
}

fn setup_sql(select: &str) -> String {
    format!(
        r#"
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);
        INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
        INSERT INTO orders (id, user_id, total) VALUES (10, 1, 50), (11, 1, 75), (12, 2, 20);
        {select};
        "#
    )
}

#[test]
fn scalar_and_correlated_exists_subqueries_execute() {
    let query = last_query(&setup_sql(
        "SELECT users.name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users ORDER BY users.id",
    ));
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Text("alice".into()), SqlValue::BigInt(2)],
            vec![SqlValue::Text("bob".into()), SqlValue::BigInt(1)],
            vec![SqlValue::Text("carol".into()), SqlValue::BigInt(0)],
        ]
    );

    let exists = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY users.id",
    ));
    assert_eq!(
        exists.rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}

#[test]
fn in_any_all_and_derived_subqueries_execute() {
    let in_query = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE users.id IN (SELECT orders.user_id FROM orders) ORDER BY users.id",
    ));
    assert_eq!(
        in_query.rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );

    let any_query = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE users.id = ANY (SELECT orders.user_id FROM orders) ORDER BY users.id",
    ));
    assert_eq!(any_query.rows, in_query.rows);

    let all_query = last_query(&setup_sql(
        "SELECT users.name FROM users WHERE users.id < ALL (SELECT orders.user_id FROM orders) ORDER BY users.id",
    ));
    assert!(all_query.rows.is_empty());

    let derived = last_query(&setup_sql(
        "SELECT active_users.name FROM (SELECT users.id, users.name FROM users WHERE users.id < 3) AS active_users ORDER BY active_users.id",
    ));
    assert_eq!(
        derived.rows,
        vec![
            vec![SqlValue::Text("alice".into())],
            vec![SqlValue::Text("bob".into())],
        ]
    );
}

#[test]
fn update_and_delete_subqueries_execute() {
    let results = execute_sql(
        "CREATE TABLE p (id INT PRIMARY KEY, cat TEXT); \
         CREATE TABLE o (id INT PRIMARY KEY); \
         INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c'); \
         INSERT INTO o VALUES (1), (3); \
         UPDATE p SET cat = 'z' WHERE id IN (SELECT id FROM o); \
         SELECT id, cat FROM p ORDER BY id; \
         DELETE FROM p WHERE EXISTS (SELECT 1 FROM o WHERE o.id = p.id); \
         SELECT id FROM p ORDER BY id;",
    )
    .expect("execute DML subqueries");

    assert!(matches!(results[4], ExecutionResult::RowsAffected(2)));
    let ExecutionResult::Query(updated) = &results[5] else {
        panic!("UPDATE verification must return rows");
    };
    assert_eq!(
        updated.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("z".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("b".into())],
            vec![SqlValue::Integer(3), SqlValue::Text("z".into())],
        ]
    );
    assert!(matches!(results[6], ExecutionResult::RowsAffected(2)));
    let ExecutionResult::Query(remaining) = &results[7] else {
        panic!("DELETE verification must return rows");
    };
    assert_eq!(remaining.rows, vec![vec![SqlValue::Integer(2)]]);
}

#[test]
fn scalar_subquery_rejects_multiple_rows() {
    let err = execute_sql(&setup_sql(
        "SELECT (SELECT orders.total FROM orders) AS total FROM users",
    ))
    .unwrap_err();
    assert!(err.to_string().contains("multiple rows"));
}

#[test]
fn update_correlated_assignment_and_delete_in_subquery_execute() {
    let query = last_query(
        "CREATE TABLE p (id INT PRIMARY KEY, cat TEXT);
         CREATE TABLE o (id INT PRIMARY KEY, cat TEXT);
         INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c');
         INSERT INTO o VALUES (1, 'changed'), (3, 'remove');
         UPDATE p SET cat = (SELECT o.cat FROM o WHERE o.id = p.id)
             WHERE EXISTS (SELECT 1 FROM o WHERE o.id = p.id);
         DELETE FROM p WHERE id IN (SELECT id FROM o WHERE cat = 'remove');
         SELECT id, cat FROM p ORDER BY id;",
    );
    assert_eq!(
        query.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Text("changed".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("b".into())],
        ]
    );
}

#[test]
fn local_subquery_columns_shadow_outer_scope_across_all_forms() {
    let setup = r#"
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        CREATE TABLE b (id INT PRIMARY KEY, y INT);
        INSERT INTO a (id, x) VALUES (1, 'one'), (2, 'two');
        INSERT INTO b (id, y) VALUES (1, 1), (2, 2);
    "#;

    for (predicate, expected) in [
        ("id > (SELECT MIN(id) FROM b)", 2),
        ("id IN (SELECT id FROM b WHERE b.id = 2)", 2),
        ("id NOT IN (SELECT id FROM b WHERE b.id = 1)", 2),
        ("id = ANY (SELECT id FROM b WHERE b.id = 2)", 2),
        ("id > ALL (SELECT id FROM b WHERE b.id = 1)", 2),
    ] {
        let query = last_query(&format!("{setup} SELECT id FROM a WHERE {predicate};"));
        assert_eq!(query.rows, vec![vec![SqlValue::Integer(expected)]]);
    }

    let self_reference = last_query(
        r#"
        CREATE TABLE t (v INT);
        INSERT INTO t VALUES (1), (3);
        SELECT v FROM t WHERE v > (SELECT AVG(v) FROM t);
        "#,
    );
    assert_eq!(self_reference.rows, vec![vec![SqlValue::Integer(3)]]);
}

#[test]
fn correlated_and_non_overlapping_subquery_names_remain_valid() {
    let exists = last_query(
        r#"
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        CREATE TABLE b (id INT PRIMARY KEY, y INT);
        INSERT INTO a VALUES (1, 'one'), (2, 'two');
        INSERT INTO b VALUES (1, 10);
        SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id);
        "#,
    );
    assert_eq!(exists.rows, vec![vec![SqlValue::Integer(1)]]);

    let not_exists = last_query(
        r#"
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        CREATE TABLE b (id INT PRIMARY KEY, y INT);
        INSERT INTO a VALUES (1, 'one'), (2, 'two');
        INSERT INTO b VALUES (1, 10);
        SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.id);
        "#,
    );
    assert_eq!(not_exists.rows, vec![vec![SqlValue::Integer(2)]]);

    let scalar = last_query(
        r#"
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        CREATE TABLE b (id INT PRIMARY KEY, y INT);
        INSERT INTO a VALUES (1, 'one'), (2, 'two');
        INSERT INTO b VALUES (1, 10), (2, 20);
        SELECT a.id, (SELECT y FROM b WHERE b.id = a.id) AS y FROM a ORDER BY a.id;
        "#,
    );
    assert_eq!(
        scalar.rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(2), SqlValue::Integer(20)],
        ]
    );

    let explicit_self_reference = last_query(
        r#"
        CREATE TABLE t (v INT);
        INSERT INTO t VALUES (1), (3);
        SELECT o.v FROM t AS o WHERE o.v > (SELECT AVG(i.v) FROM t AS i);
        "#,
    );
    assert_eq!(
        explicit_self_reference.rows,
        vec![vec![SqlValue::Integer(3)]]
    );

    let derived = last_query(
        r#"
        CREATE TABLE a (id INT PRIMARY KEY, x TEXT);
        INSERT INTO a VALUES (1, 'one'), (2, 'two');
        SELECT d.id FROM (SELECT id FROM a) AS d ORDER BY d.id;
        "#,
    );
    assert_eq!(
        derived.rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
    );

    let unique_inner_name = last_query(
        r#"
        CREATE TABLE a (id INT PRIMARY KEY);
        CREATE TABLE b (id INT PRIMARY KEY, y INT);
        INSERT INTO a VALUES (2);
        INSERT INTO b VALUES (1, 1);
        SELECT id FROM a WHERE id > (SELECT MIN(y) FROM b);
        "#,
    );
    assert_eq!(unique_inner_name.rows, vec![vec![SqlValue::Integer(2)]]);
}

#[test]
fn a_derived_table_does_not_see_the_enclosing_query_without_lateral() {
    // Standard SQL evaluates a derived table independently of the query it sits
    // in, so `o` is not in scope inside it; only LATERAL would make it visible.
    // Resolving `o.v` here builds a correlated reference the user never wrote.
    let error = execute_sql(
        r#"
        CREATE TABLE t (v INT);
        CREATE TABLE u (w INT);
        INSERT INTO t VALUES (1);
        INSERT INTO u VALUES (1);
        SELECT o.v FROM t AS o
        WHERE o.v = (SELECT d.w FROM (SELECT u.w FROM u WHERE u.w = o.v) AS d);
        "#,
    )
    .expect_err("a derived table must not resolve a name from the enclosing query");

    let message = error.to_string();
    assert!(
        message.contains("'o'"),
        "the error should name the unresolvable qualifier, got: {message}"
    );

    // The same shape stays legal one level up: a scalar subquery *is* allowed to
    // correlate, so only the derived-table boundary changed.
    let correlated = last_query(
        r#"
        CREATE TABLE t (v INT);
        CREATE TABLE u (w INT);
        INSERT INTO t VALUES (1), (2);
        INSERT INTO u VALUES (1);
        SELECT o.v FROM t AS o WHERE o.v = (SELECT u.w FROM u WHERE u.w = o.v);
        "#,
    );
    assert_eq!(correlated.rows, vec![vec![SqlValue::Integer(1)]]);
}

#[test]
fn double_quoted_identifier_resolves_to_the_column_value() {
    let query = last_query(
        r#"
        CREATE TABLE t (s TEXT);
        INSERT INTO t VALUES ('hello world');
        SELECT "s" FROM t;
        "#,
    );

    assert_eq!(query.rows, vec![vec![SqlValue::Text("hello world".into())]]);
}

/// PostgreSQL-style identifiers fold only when they are unquoted: a delimited
/// identifier keeps its case while bare spellings resolve as lowercase.
#[test]
fn quoted_identifiers_preserve_case_while_unquoted_identifiers_fold() {
    let query = last_query(
        r#"
        CREATE TABLE t ("Col" INT, PLAIN INT);
        INSERT INTO t ("Col", PLAIN) VALUES (10, 20);
        SELECT "Col", plain, PLAIN FROM t;
        "#,
    );
    assert_eq!(
        query.rows,
        vec![vec![
            SqlValue::Integer(10),
            SqlValue::Integer(20),
            SqlValue::Integer(20),
        ]]
    );

    let err = execute_sql(
        r#"
        CREATE TABLE t ("Col" INT);
        INSERT INTO t ("Col") VALUES (10);
        SELECT col FROM t;
        "#,
    )
    .expect_err("unquoted col must not resolve the case-sensitive quoted column");
    assert!(
        err.to_string().contains("ALOPEX-C003"),
        "expected C003 for an unquoted case mismatch, got: {err}"
    );
}

/// Error positions must point into the SQL the caller wrote. Quoted identifiers
/// are normalised before parsing, and dropping the quote characters shifted
/// every later column by two per identifier, so diagnostics pointed at the
/// wrong place in exactly the queries that use quoting.
#[test]
fn error_spans_survive_quoted_identifier_normalisation() {
    let sql = "SELECT \"Quoted\", missing FROM t";
    let column = sql.find("missing").expect("locate the offending column") + 1;

    let statements =
        Parser::parse_sql(&AlopexDialect, "CREATE TABLE t (\"Quoted\" INT)").expect("parse create");
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let store = Arc::new(MemoryKV::new());
    let mut executor = Executor::new(store, catalog.clone());
    for statement in statements {
        let guard = catalog.read().expect("catalog lock");
        let plan = Planner::new(&*guard).plan(&statement).expect("plan create");
        drop(guard);
        executor.execute(plan).expect("execute create");
    }

    let statement = Parser::parse_sql(&AlopexDialect, sql)
        .expect("parse select")
        .remove(0);
    let guard = catalog.read().expect("catalog lock");
    let error = Planner::new(&*guard)
        .plan(&statement)
        .expect_err("unknown column must fail");

    let rendered = error.to_string();
    assert!(
        rendered.contains(&format!("column {column}")),
        "error should point at column {column} of the original SQL: {rendered}"
    );
}

/// Scope resolution has to hold at more than one level. A three-level query
/// exercises the inner-first rule twice, and the middle level must not become a
/// candidate for a name the innermost one already defines.
#[test]
fn three_level_nesting_resolves_each_name_in_its_own_scope() {
    let rows = last_query(
        "
        CREATE TABLE outer_t (id INT PRIMARY KEY, v INT);
        CREATE TABLE mid_t (id INT PRIMARY KEY, v INT);
        CREATE TABLE inner_t (id INT PRIMARY KEY, v INT);
        INSERT INTO outer_t (id, v) VALUES (1, 10), (2, 20);
        INSERT INTO mid_t (id, v) VALUES (1, 5);
        INSERT INTO inner_t (id, v) VALUES (1, 1);
        SELECT id FROM outer_t
         WHERE v > (SELECT MAX(v) FROM mid_t
                     WHERE v > (SELECT MAX(v) FROM inner_t))
         ORDER BY id;
        ",
    )
    .rows;
    // MAX(inner_t.v) = 1, so the middle level yields MAX(mid_t.v) = 5 and both
    // outer rows exceed it. Binding v to an enclosing scope would change this.
    assert_eq!(
        rows,
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
    );
}

/// EXISTS introduces a scope like any other subquery: a name defined inside it
/// shadows the outer one, and only a qualified reference reaches outward.
#[test]
fn exists_subquery_shadows_the_outer_name_it_redefines() {
    let rows = last_query(
        "
        CREATE TABLE lhs (id INT PRIMARY KEY, tag TEXT);
        CREATE TABLE rhs (id INT PRIMARY KEY, tag TEXT);
        INSERT INTO lhs (id, tag) VALUES (1, 'keep'), (2, 'drop');
        INSERT INTO rhs (id, tag) VALUES (9, 'keep');
        SELECT id FROM lhs
         WHERE EXISTS (SELECT 1 FROM rhs WHERE tag = lhs.tag)
         ORDER BY id;
        ",
    )
    .rows;
    // Unqualified tag inside EXISTS is rhs.tag, so only the row whose tag the
    // right side also carries survives. Resolving it to lhs.tag would keep both.
    assert_eq!(rows, vec![vec![SqlValue::Integer(1)]]);
}

/// A name that genuinely appears in more than one visible relation must be
/// rejected rather than bound to whichever one comes first.
#[test]
fn a_genuinely_ambiguous_name_is_rejected() {
    let error = execute_sql(
        "
        CREATE TABLE left_t (shared INT PRIMARY KEY, l TEXT);
        CREATE TABLE right_t (shared INT PRIMARY KEY, r TEXT);
        SELECT shared FROM left_t JOIN right_t ON left_t.shared = right_t.shared;
        ",
    )
    .expect_err("shared is ambiguous across both inputs");
    let rendered = error.to_string();
    assert!(
        rendered.contains("ambiguous"),
        "expected an ambiguity error, got: {rendered}"
    );
}

#[test]
fn resolution_is_identical_either_side_of_the_column_index_threshold() {
    // Scoped tables switch from scanning their column list to a hash index once
    // they exceed a width threshold. Both paths must resolve names the same way,
    // otherwise a schema crossing that width silently changes behaviour.
    for width in [8usize, 31, 32, 33, 64] {
        let columns = (0..width)
            .map(|i| format!("c{i} INT"))
            .collect::<Vec<_>>()
            .join(", ");
        let values = (0..width)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let last = width - 1;

        // A name is found wherever it sits in the schema.
        let query = last_query(&format!(
            "CREATE TABLE w ({columns});
             INSERT INTO w VALUES ({values});
             SELECT c0, c{last} FROM w;"
        ));
        assert_eq!(
            query.rows,
            vec![vec![SqlValue::Integer(0), SqlValue::Integer(last as i32),]],
            "width {width} resolved the wrong columns"
        );

        // A name that does not exist is still an error, not a stray match.
        let missing = execute_sql(&format!(
            "CREATE TABLE w ({columns});
             SELECT c{width} FROM w;"
        ))
        .expect_err("an absent column must be rejected");
        assert!(
            missing.to_string().contains(&format!("c{width}")),
            "width {width} gave an unhelpful error: {missing}"
        );

        // An ambiguous unqualified name is rejected at every width.
        let ambiguous = execute_sql(&format!(
            "CREATE TABLE l ({columns});
             CREATE TABLE r ({columns});
             SELECT c0 FROM l JOIN r ON l.c0 = r.c0;"
        ))
        .expect_err("an ambiguous column must be rejected");
        assert!(
            ambiguous.to_string().contains("c0"),
            "width {width} gave an unhelpful ambiguity error: {ambiguous}"
        );
    }
}
