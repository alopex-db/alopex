//! Parent-FK batch behavior through the real borrowed transaction API.
use std::sync::{Arc, RwLock};

use alopex_core::kv::{KVStore, KVTransaction, memory::MemoryKV};
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{CatalogOverlay, PersistentCatalog, TxnCatalogView};
use alopex_sql::storage::BorrowedSqlTransaction;
use alopex_sql::{AlopexDialect, ExecutionResult, Executor, Parser, Planner, SqlValue, TxnBridge};

struct Fixture {
    store: Arc<MemoryKV>,
    catalog: Arc<RwLock<PersistentCatalog<MemoryKV>>>,
    executor: Executor<MemoryKV, PersistentCatalog<MemoryKV>>,
}

impl Fixture {
    fn new() -> Self {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(PersistentCatalog::new(store.clone())));
        let executor = Executor::new(store.clone(), catalog.clone());
        Self {
            store,
            catalog,
            executor,
        }
    }

    fn execute(&mut self, sql: &str) -> ExecutionResult {
        let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
        let plan = Planner::new(&*self.catalog.read().unwrap())
            .plan(&statement)
            .unwrap();
        self.executor.execute(plan).unwrap()
    }

    fn borrowed(
        &mut self,
        txn: &mut BorrowedSqlTransaction<'_, '_, '_, MemoryKV>,
        sql: &str,
    ) -> alopex_sql::executor::Result<ExecutionResult> {
        let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
        let plan = {
            let (_, overlay) = txn.split_parts();
            let catalog = self.catalog.read().unwrap();
            Planner::new(&TxnCatalogView::new(&*catalog, overlay))
                .plan(&statement)
                .unwrap()
        };
        self.executor.execute_in_txn(plan, txn)
    }
}

fn rows(result: ExecutionResult) -> Vec<Vec<SqlValue>> {
    let ExecutionResult::Query(result) = result else {
        panic!("expected query")
    };
    result.rows
}

fn ints(values: &[i32]) -> Vec<SqlValue> {
    values.iter().copied().map(SqlValue::Integer).collect()
}

fn affected(result: ExecutionResult, count: u64) {
    assert!(matches!(result, ExecutionResult::RowsAffected(actual) if actual == count));
}

fn transaction(test: impl FnOnce(&mut Fixture, &mut BorrowedSqlTransaction<'_, '_, '_, MemoryKV>)) {
    let mut fixture = Fixture::new();
    let store = fixture.store.clone();
    let mut transaction = store.begin(TxnMode::ReadWrite).unwrap();
    let mut overlay = CatalogOverlay::new();
    let mut borrowed =
        TxnBridge::<MemoryKV>::wrap_external(&mut transaction, TxnMode::ReadWrite, &mut overlay);
    test(&mut fixture, &mut borrowed);
    drop(borrowed);
    transaction.rollback_self().unwrap();
}

#[test]
fn cascade_updates_all_600_rows_across_batch_boundary() {
    transaction(|f, txn| {
        for sql in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id) ON UPDATE CASCADE)",
        ] {
            f.borrowed(txn, sql).unwrap();
        }
        for start in (1..=600).step_by(100) {
            let parent = (start..start + 100)
                .map(|id| format!("({id})"))
                .collect::<Vec<_>>()
                .join(",");
            let child = (start..start + 100)
                .map(|id| format!("({id},{id})"))
                .collect::<Vec<_>>()
                .join(",");
            f.borrowed(txn, &format!("INSERT INTO p VALUES {parent}"))
                .unwrap();
            f.borrowed(txn, &format!("INSERT INTO c VALUES {child}"))
                .unwrap();
        }
        affected(f.borrowed(txn, "UPDATE p SET id=id+1000").unwrap(), 600);
        assert_eq!(
            rows(f.borrowed(txn, "SELECT id FROM p ORDER BY id").unwrap()),
            (1001..=1600).map(|id| ints(&[id])).collect::<Vec<_>>()
        );
        assert_eq!(
            rows(f.borrowed(txn, "SELECT id,pid FROM c ORDER BY id").unwrap()),
            (1..=600)
                .map(|id| ints(&[id, id + 1000]))
                .collect::<Vec<_>>()
        );
    });
}

#[test]
fn recursive_cascade_visits_multiple_children_and_constraints() {
    transaction(|f, txn| {
        for sql in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY REFERENCES p(id) ON UPDATE CASCADE,other INTEGER REFERENCES p(id) ON UPDATE CASCADE)",
            "CREATE TABLE g(id INTEGER REFERENCES c(id) ON UPDATE CASCADE)",
            "CREATE TABLE sibling(id INTEGER REFERENCES p(id) ON UPDATE CASCADE)",
            "INSERT INTO p VALUES(10)",
            "INSERT INTO c VALUES(10,10)",
            "INSERT INTO g VALUES(10)",
            "INSERT INTO sibling VALUES(10)",
        ] {
            f.borrowed(txn, sql).unwrap();
        }
        affected(f.borrowed(txn, "UPDATE p SET id=20").unwrap(), 1);
        assert_eq!(
            rows(f.borrowed(txn, "SELECT id,other FROM c").unwrap()),
            vec![ints(&[20, 20])]
        );
        for table in ["p", "g", "sibling"] {
            assert_eq!(
                rows(f.borrowed(txn, &format!("SELECT id FROM {table}")).unwrap()),
                vec![ints(&[20])]
            );
        }
    });
}

#[test]
fn unchanged_parent_key_does_not_modify_children() {
    transaction(|f, txn| {
        for sql in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY,v INTEGER)",
            "CREATE TABLE c(pid INTEGER REFERENCES p(id) ON UPDATE RESTRICT)",
        ] {
            f.borrowed(txn, sql).unwrap();
        }
        for start in (1..=600).step_by(100) {
            let parent = (start..start + 100)
                .map(|id| format!("({id},10)"))
                .collect::<Vec<_>>()
                .join(",");
            let child = (start..start + 100)
                .map(|id| format!("({id})"))
                .collect::<Vec<_>>()
                .join(",");
            f.borrowed(txn, &format!("INSERT INTO p VALUES {parent}"))
                .unwrap();
            f.borrowed(txn, &format!("INSERT INTO c VALUES {child}"))
                .unwrap();
        }
        affected(f.borrowed(txn, "UPDATE p SET v=20").unwrap(), 600);
        affected(f.borrowed(txn, "UPDATE p SET v=20").unwrap(), 0);
        assert_eq!(
            rows(f.borrowed(txn, "SELECT id,v FROM p ORDER BY id").unwrap()),
            (1..=600).map(|id| ints(&[id, 20])).collect::<Vec<_>>()
        );
        assert_eq!(
            rows(f.borrowed(txn, "SELECT pid FROM c ORDER BY pid").unwrap()),
            (1..=600).map(|id| ints(&[id])).collect::<Vec<_>>()
        );
    });
}

#[test]
fn composite_cascade_preserves_null_nonmatches() {
    transaction(|f, txn| {
        for sql in [
            "CREATE TABLE p(a INTEGER,b INTEGER,PRIMARY KEY(a,b))",
            "CREATE TABLE c(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER,FOREIGN KEY(a,b) REFERENCES p(a,b) ON UPDATE CASCADE)",
            "INSERT INTO p VALUES(1,10)",
            "INSERT INTO c VALUES(1,1,10),(2,NULL,10),(3,1,NULL)",
        ] {
            f.borrowed(txn, sql).unwrap();
        }
        affected(f.borrowed(txn, "UPDATE p SET b=20").unwrap(), 1);
        assert_eq!(
            rows(f.borrowed(txn, "SELECT id,a,b FROM c ORDER BY id").unwrap()),
            vec![
                ints(&[1, 1, 20]),
                vec![SqlValue::Integer(2), SqlValue::Null, SqlValue::Integer(10)],
                vec![SqlValue::Integer(3), SqlValue::Integer(1), SqlValue::Null]
            ]
        );
    });
}

#[test]
fn set_null_and_self_reference_preserve_actions() {
    transaction(|f, txn| {
        for sql in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY,manager INTEGER REFERENCES p(id) ON UPDATE CASCADE ON DELETE SET NULL)",
            "CREATE TABLE c(pid INTEGER REFERENCES p(id) ON UPDATE SET NULL)",
            "INSERT INTO p VALUES(1,NULL),(2,1)",
            "INSERT INTO c VALUES(1)",
        ] {
            f.borrowed(txn, sql).unwrap();
        }
        affected(f.borrowed(txn, "UPDATE p SET id=3 WHERE id=1").unwrap(), 1);
        assert_eq!(
            rows(f.borrowed(txn, "SELECT manager FROM p WHERE id=2").unwrap()),
            vec![ints(&[3])]
        );
        assert_eq!(
            rows(f.borrowed(txn, "SELECT pid FROM c").unwrap()),
            vec![vec![SqlValue::Null]]
        );
        affected(f.borrowed(txn, "DELETE FROM p WHERE id=3").unwrap(), 1);
        assert_eq!(
            rows(f.borrowed(txn, "SELECT manager FROM p WHERE id=2").unwrap()),
            vec![vec![SqlValue::Null]]
        );
    });
}

#[test]
fn late_restrict_and_no_action_fail_then_explicit_rollback_restores_rows() {
    for action in ["RESTRICT", "NO ACTION"] {
        let mut f = Fixture::new();
        f.execute("CREATE TABLE p(id INTEGER PRIMARY KEY)");
        f.execute(&format!(
            "CREATE TABLE c(pid INTEGER REFERENCES p(id) ON UPDATE {action})"
        ));
        for start in (1..=600).step_by(100) {
            let values = (start..start + 100)
                .map(|id| format!("({id})"))
                .collect::<Vec<_>>()
                .join(",");
            f.execute(&format!("INSERT INTO p VALUES {values}"));
        }
        f.execute("INSERT INTO c VALUES(600)");
        let store = f.store.clone();
        let mut txn = store.begin(TxnMode::ReadWrite).unwrap();
        let mut overlay = CatalogOverlay::new();
        let mut borrowed =
            TxnBridge::<MemoryKV>::wrap_external(&mut txn, TxnMode::ReadWrite, &mut overlay);
        let error = f
            .borrowed(&mut borrowed, "UPDATE p SET id=id+1000")
            .unwrap_err();
        assert!(
            matches!(
                error,
                alopex_sql::executor::ExecutorError::ConstraintViolation(
                    alopex_sql::executor::ConstraintViolation::ForeignKey { .. }
                )
            ),
            "{error:?}"
        );
        drop(borrowed);
        txn.rollback_self().unwrap();
        assert_eq!(
            rows(f.execute("SELECT id FROM p ORDER BY id")),
            (1..=600).map(|id| ints(&[id])).collect::<Vec<_>>()
        );
        assert_eq!(rows(f.execute("SELECT pid FROM c")), vec![ints(&[600])]);
    }
}

#[test]
fn preceding_transaction_ddl_and_rows_are_visible() {
    transaction(|f, txn| {
        for sql in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY)",
            "INSERT INTO p VALUES(1)",
            "CREATE TABLE c(pid INTEGER REFERENCES p(id) ON UPDATE CASCADE)",
            "INSERT INTO c VALUES(1)",
            "UPDATE p SET id=2",
        ] {
            f.borrowed(txn, sql).unwrap();
        }
        assert_eq!(
            rows(f.borrowed(txn, "SELECT pid FROM c").unwrap()),
            vec![ints(&[2])]
        );
    });
}

#[test]
fn recursive_update_depth_has_a_bounded_boundary() {
    for depth in [64, 65] {
        transaction(|f, txn| {
            f.borrowed(txn, "CREATE TABLE p0(id INTEGER PRIMARY KEY)")
                .unwrap();
            f.borrowed(txn, "INSERT INTO p0 VALUES(1)").unwrap();
            for level in 1..=depth {
                f.borrowed(txn, &format!("CREATE TABLE p{level}(id INTEGER PRIMARY KEY REFERENCES p{}(id) ON UPDATE CASCADE)", level-1)).unwrap();
                f.borrowed(txn, &format!("INSERT INTO p{level} VALUES(1)"))
                    .unwrap();
            }
            let result = f.borrowed(txn, "UPDATE p0 SET id=2");
            if depth == 64 {
                affected(result.unwrap(), 1);
                for level in 0..=depth {
                    assert_eq!(
                        rows(
                            f.borrowed(txn, &format!("SELECT id FROM p{level}"))
                                .unwrap()
                        ),
                        vec![ints(&[2])]
                    );
                }
            } else {
                let error = result.unwrap_err();
                assert!(
                    matches!(&error, alopex_sql::executor::ExecutorError::InvalidOperation { operation, reason } if operation == "FOREIGN KEY CASCADE" && reason == "cascade depth exceeds 64"),
                    "{error:?}"
                );
            }
        });
    }
}
