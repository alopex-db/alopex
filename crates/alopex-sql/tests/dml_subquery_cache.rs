//! Observe real source-table reads; the adapter delegates all data operations.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use alopex_core::Result as CoreResult;
use alopex_core::kv::memory::{MemoryKV, MemoryTransaction};
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::txn::TxnManager;
use alopex_core::types::{Key, TxnId, TxnMode, Value};
use alopex_sql::catalog::{Catalog, MemoryCatalog};
use alopex_sql::executor::{ExecutionResult, Executor};
use alopex_sql::planner::typed_expr::TypedExprKind;
use alopex_sql::storage::SqlValue;
use alopex_sql::{AlopexDialect, LogicalPlan, Parser, Planner, Span};

#[derive(Default)]
struct CountingStore {
    inner: MemoryKV,
    scans: Arc<Mutex<HashMap<u32, usize>>>,
    gets: Arc<Mutex<HashMap<u32, usize>>>,
}

struct CountingTransaction<'a> {
    inner: MemoryTransaction<'a>,
    scans: Arc<Mutex<HashMap<u32, usize>>>,
    gets: Arc<Mutex<HashMap<u32, usize>>>,
}

impl CountingTransaction<'_> {
    fn observe(&self, key: &[u8]) {
        if key.len() >= 5 && key[0] == 1 {
            let table = u32::from_be_bytes(key[1..5].try_into().unwrap());
            *self.scans.lock().unwrap().entry(table).or_default() += 1;
        }
    }
}

impl KVStore for CountingStore {
    type Transaction<'a> = CountingTransaction<'a>;
    type Manager<'a> = &'a Self;
    fn txn_manager(&self) -> Self::Manager<'_> {
        self
    }
    fn begin(&self, mode: TxnMode) -> CoreResult<Self::Transaction<'_>> {
        Ok(CountingTransaction {
            inner: self.inner.begin(mode)?,
            scans: self.scans.clone(),
            gets: self.gets.clone(),
        })
    }
}

impl<'a> TxnManager<'a, CountingTransaction<'a>> for &'a CountingStore {
    fn begin(&'a self, mode: TxnMode) -> CoreResult<CountingTransaction<'a>> {
        KVStore::begin(*self, mode)
    }
    fn commit(&'a self, transaction: CountingTransaction<'a>) -> CoreResult<()> {
        transaction.commit_self()
    }
    fn rollback(&'a self, transaction: CountingTransaction<'a>) -> CoreResult<()> {
        transaction.rollback_self()
    }
}

impl<'a> KVTransaction<'a> for CountingTransaction<'a> {
    fn id(&self) -> TxnId {
        self.inner.id()
    }
    fn mode(&self) -> TxnMode {
        self.inner.mode()
    }
    fn get(&mut self, key: &Key) -> CoreResult<Option<Value>> {
        if key.len() >= 5 && key[0] == 1 {
            let table = u32::from_be_bytes(key[1..5].try_into().unwrap());
            *self.gets.lock().unwrap().entry(table).or_default() += 1;
        }
        self.inner.get(key)
    }
    fn put(&mut self, key: Key, value: Value) -> CoreResult<()> {
        self.inner.put(key, value)
    }
    fn delete(&mut self, key: Key) -> CoreResult<()> {
        self.inner.delete(key)
    }
    fn scan_prefix(
        &mut self,
        prefix: &[u8],
    ) -> CoreResult<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.observe(prefix);
        self.inner.scan_prefix(prefix)
    }
    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> CoreResult<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.observe(start);
        self.inner.scan_range(start, end)
    }
    fn scan_from(
        &mut self,
        start: &[u8],
    ) -> CoreResult<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        self.observe(start);
        self.inner.scan_from(start)
    }
    fn commit_self(self) -> CoreResult<()> {
        self.inner.commit_self()
    }
    fn rollback_self(self) -> CoreResult<()> {
        self.inner.rollback_self()
    }
}

struct Fixture {
    store: Arc<CountingStore>,
    catalog: Arc<RwLock<MemoryCatalog>>,
    executor: Executor<CountingStore, MemoryCatalog>,
}

impl Fixture {
    fn new() -> Self {
        let store = Arc::new(CountingStore::default());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let executor = Executor::new(store.clone(), catalog.clone());
        let mut fixture = Self {
            store,
            catalog,
            executor,
        };
        fixture.run("CREATE TABLE hw (id INTEGER PRIMARY KEY, v INTEGER)");
        fixture.run("CREATE TABLE source (id INTEGER, v INTEGER)");
        fixture.run("CREATE TABLE other (v INTEGER)");
        fixture.run("INSERT INTO source VALUES (1,10),(2,20)");
        fixture.run("INSERT INTO other VALUES (3)");
        for first in (1..=600).step_by(128) {
            let rows = (first..=(first + 127).min(600))
                .map(|id| format!("({id},{id})"))
                .collect::<Vec<_>>()
                .join(",");
            fixture.run(&format!("INSERT INTO hw VALUES {rows}"));
        }
        fixture.store.scans.lock().unwrap().clear();
        fixture.store.gets.lock().unwrap().clear();
        fixture
    }
    fn plan(&self, sql: &str) -> LogicalPlan {
        let statements = Parser::parse_sql(&AlopexDialect, sql).unwrap();
        assert_eq!(statements.len(), 1);
        Planner::new(&*self.catalog.read().unwrap())
            .plan(&statements[0])
            .unwrap()
    }
    fn run(&mut self, sql: &str) -> ExecutionResult {
        self.executor.execute(self.plan(sql)).unwrap()
    }
    fn scans(&self, table: &str) -> usize {
        let id = self
            .catalog
            .read()
            .unwrap()
            .get_table(table)
            .unwrap()
            .table_id;
        self.store
            .scans
            .lock()
            .unwrap()
            .get(&id)
            .copied()
            .unwrap_or(0)
    }
    fn values(&mut self) -> Vec<SqlValue> {
        let ExecutionResult::Query(query) = self.run("SELECT v FROM hw ORDER BY id") else {
            panic!("expected query")
        };
        query
            .rows
            .into_iter()
            .map(|mut row| row.remove(0))
            .collect()
    }
}

#[test]
fn composition_joined_scalar_condition_cache_resets() {
    assert_composed_cache(
        "UPDATE hw SET v=hw.v+1 FROM source s WHERE hw.id>=s.id AND (SELECT MAX(v) FROM other)>0",
        false,
        false,
    );
}

#[test]
fn composition_joined_in_condition_cache_resets() {
    assert_composed_cache(
        "UPDATE hw SET v=hw.v+1 FROM source s WHERE hw.id>=s.id AND s.id IN (SELECT v-v+1 FROM other)",
        false,
        false,
    );
}

#[test]
fn composition_merge_on_cache_resets() {
    assert_composed_cache(
        "MERGE INTO hw USING source ON hw.id=source.id AND (SELECT MAX(v) FROM other)>0 WHEN MATCHED THEN UPDATE SET v=hw.v+1",
        true,
        false,
    );
}

#[test]
fn composition_delete_scalar_condition_cache_resets() {
    assert_composed_delete_cache(
        "DELETE FROM hw USING source s WHERE hw.id>=s.id AND hw.id<=(SELECT MAX(v) FROM other)",
        true,
    );
}

#[test]
fn composition_delete_in_condition_cache_resets() {
    assert_composed_delete_cache(
        "DELETE FROM hw USING source s WHERE hw.id>=s.id AND hw.id IN (SELECT v FROM other)",
        false,
    );
}

fn assert_composed_delete_cache(sql: &str, threshold: bool) {
    let mut fixture = Fixture::new();
    for round in 1..=2 {
        fixture.store.scans.lock().unwrap().clear();
        fixture.run(sql);
        assert_eq!(fixture.scans("other"), 1, "round {round}: {sql}");
        let expected = (1..=600)
            .filter(|id| {
                if threshold {
                    *id > round + 2
                } else {
                    *id < 3 || *id > round + 2
                }
            })
            .map(SqlValue::Integer)
            .collect::<Vec<_>>();
        assert_eq!(fixture.values(), expected, "round {round}: {sql}");
        fixture.run("UPDATE other SET v=v+1");
    }
}

#[test]
fn composition_merge_when_cache_resets() {
    assert_composed_cache(
        "MERGE INTO hw USING source ON hw.id=source.id WHEN MATCHED AND (SELECT MAX(v) FROM other)>0 THEN UPDATE SET v=hw.v+1",
        true,
        false,
    );
}

#[test]
fn composition_merge_update_value_cache_resets() {
    assert_composed_cache(
        "MERGE INTO hw USING source ON hw.id=source.id WHEN MATCHED THEN UPDATE SET v=(SELECT MAX(v) FROM other)",
        true,
        true,
    );
}

fn assert_composed_cache(sql: &str, only_first_two: bool, scalar_value: bool) {
    let mut fixture = Fixture::new();
    for round in 1..=2 {
        fixture.store.scans.lock().unwrap().clear();
        fixture.run(sql);
        // Count the independent subquery table, not the joined source scans.
        assert_eq!(fixture.scans("other"), 1, "round {round}: {sql}");
        let expected = (1..=600)
            .map(|id| {
                SqlValue::Integer(if !only_first_two || id <= 2 {
                    if scalar_value { round + 2 } else { id + round }
                } else {
                    id
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(fixture.values(), expected, "round {round}: {sql}");
        fixture.run("UPDATE other SET v=v+1");
    }
}

#[test]
fn composition_merge_insert_value_cache_resets() {
    let mut fixture = Fixture::new();
    fixture.run("UPDATE source SET id=id+600");
    for round in 1..=2 {
        fixture.store.scans.lock().unwrap().clear();
        fixture.run("MERGE INTO hw USING source ON hw.id=source.id WHEN NOT MATCHED THEN INSERT (id,v) VALUES (source.id,(SELECT MAX(v) FROM other))");
        assert_eq!(fixture.scans("other"), 1, "round {round}");
        let ExecutionResult::Query(query) =
            fixture.run("SELECT id,v FROM hw WHERE id>600 ORDER BY id")
        else {
            panic!("expected query")
        };
        assert_eq!(
            query.rows,
            vec![
                vec![SqlValue::Integer(601), SqlValue::Integer(round + 2)],
                vec![SqlValue::Integer(602), SqlValue::Integer(round + 2)],
            ]
        );
        fixture.run("DELETE FROM hw WHERE id>600");
        fixture.run("UPDATE other SET v=v+1");
    }
}

#[test]
fn ordinary_select_with_subquery_keeps_bounded_ordered_limit_reads() {
    let mut fixture = Fixture::new();
    let result =
        fixture.run("SELECT (SELECT MAX(v) FROM source) FROM hw WHERE id>=1 ORDER BY id LIMIT 1");
    let ExecutionResult::Query(query) = result else {
        panic!("expected query")
    };
    assert_eq!(query.rows, vec![vec![SqlValue::Integer(20)]]);
    let table_id = fixture
        .catalog
        .read()
        .unwrap()
        .get_table("hw")
        .unwrap()
        .table_id;
    assert_eq!(fixture.store.gets.lock().unwrap().get(&table_id), Some(&1));
    assert_eq!(fixture.scans("hw"), 0);
}

#[test]
fn independent_scalar_and_membership_scan_once_per_statement() {
    let mut fixture = Fixture::new();
    fixture.run("UPDATE hw SET v=(SELECT MAX(v) FROM source)+1");
    assert_eq!(fixture.scans("source"), 1);
    assert_eq!(fixture.values(), vec![SqlValue::Integer(21); 600]);
    fixture.run("UPDATE source SET v=v+100");
    fixture.store.scans.lock().unwrap().clear();
    fixture.run("UPDATE hw SET v=(SELECT MAX(v) FROM source)+1");
    assert_eq!(
        fixture.scans("source"),
        1,
        "cache must reset between statements"
    );
    assert_eq!(fixture.values(), vec![SqlValue::Integer(121); 600]);
    fixture.store.scans.lock().unwrap().clear();
    fixture.run("DELETE FROM hw WHERE v IN (SELECT v+1 FROM source)");
    assert_eq!(fixture.scans("source"), 1);
    assert!(fixture.values().is_empty());
}

#[test]
fn distinct_plans_with_default_spans_never_share_a_result() {
    let mut fixture = Fixture::new();
    let mut plan =
        fixture.plan("UPDATE hw SET v=(SELECT MAX(v) FROM source)+(SELECT MIN(v) FROM source)");
    let LogicalPlan::Update { assignments, .. } = &mut plan else {
        panic!("expected UPDATE")
    };
    let TypedExprKind::BinaryOp { left, right, .. } = &mut assignments[0].value.kind else {
        panic!("expected addition")
    };
    left.span = Span::default();
    right.span = Span::default();
    fixture.executor.execute(plan).unwrap();
    assert_eq!(
        fixture.scans("source"),
        2,
        "two occurrences each execute exactly once"
    );
    assert_eq!(fixture.values(), vec![SqlValue::Integer(30); 600]);
}

#[test]
fn nested_independent_query_and_correlated_parent_keep_separate_lifetimes() {
    let mut fixture = Fixture::new();
    fixture.run("UPDATE hw SET v=(SELECT (SELECT MAX(v) FROM source))");
    assert_eq!(fixture.scans("source"), 1);
    assert_eq!(fixture.values(), vec![SqlValue::Integer(20); 600]);
    fixture.store.scans.lock().unwrap().clear();
    fixture.run("UPDATE hw SET v=(SELECT MAX(s.v)+(SELECT MIN(v) FROM other) FROM source s WHERE s.id<=hw.id)");
    assert_eq!(
        fixture.scans("source"),
        600,
        "correlated parent must read each row"
    );
    assert_eq!(
        fixture.scans("other"),
        1,
        "independent child survives execution-plan clones"
    );
    let values = fixture.values();
    assert_eq!(values[0], SqlValue::Integer(13));
    assert_eq!(values[1..], vec![SqlValue::Integer(23); 599]);
}

#[test]
fn volatile_queries_are_not_cached_and_unused_case_is_not_eager() {
    let mut fixture = Fixture::new();
    fixture.run("UPDATE hw SET v=CAST((SELECT random() FROM other)*100 AS INTEGER)");
    assert_eq!(
        fixture.scans("other"),
        600,
        "volatile expression must not become a constant"
    );
    fixture.store.scans.lock().unwrap().clear();
    fixture.run("UPDATE hw SET v=CASE WHEN id<0 THEN (SELECT v FROM source) ELSE 7 END");
    assert_eq!(
        fixture.scans("source"),
        0,
        "unused multi-row scalar must not run"
    );
    assert_eq!(fixture.values(), vec![SqlValue::Integer(7); 600]);
}
