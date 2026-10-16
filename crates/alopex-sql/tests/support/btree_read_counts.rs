use super::*;
use alopex_core::error::Result as CoreResult;
use alopex_core::kv::memory::MemoryTransaction;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::txn::TxnManager;
use alopex_core::types::{Key, TxnId, TxnMode, Value};
use alopex_sql::ast::expr::{BinaryOp, Literal};
use alopex_sql::storage::{KeyEncoder, SqlValue};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct RowReads {
    gets: AtomicUsize,
    scans: AtomicUsize,
}

impl RowReads {
    fn reset(&self) {
        self.gets.store(0, Ordering::Relaxed);
        self.scans.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (usize, usize) {
        (
            self.gets.load(Ordering::Relaxed),
            self.scans.load(Ordering::Relaxed),
        )
    }
}

#[derive(Default)]
struct CountingKV {
    inner: MemoryKV,
    reads: RowReads,
}

struct CountingTransaction<'a> {
    inner: MemoryTransaction<'a>,
    reads: &'a RowReads,
}

impl KVStore for CountingKV {
    type Transaction<'a> = CountingTransaction<'a>;
    type Manager<'a> = &'a Self;

    fn txn_manager(&self) -> Self::Manager<'_> {
        self
    }

    fn begin(&self, mode: TxnMode) -> CoreResult<Self::Transaction<'_>> {
        Ok(CountingTransaction {
            inner: self.inner.begin(mode)?,
            reads: &self.reads,
        })
    }
}

impl<'a> TxnManager<'a, CountingTransaction<'a>> for &'a CountingKV {
    fn begin(&'a self, mode: TxnMode) -> CoreResult<CountingTransaction<'a>> {
        KVStore::begin(*self, mode)
    }

    fn commit(&'a self, txn: CountingTransaction<'a>) -> CoreResult<()> {
        txn.commit_self()
    }

    fn rollback(&'a self, txn: CountingTransaction<'a>) -> CoreResult<()> {
        txn.rollback_self()
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
        let value = self.inner.get(key)?;
        if value.is_some() && KeyEncoder::decode_row_key(key).is_ok() {
            self.reads.gets.fetch_add(1, Ordering::Relaxed);
        }
        Ok(value)
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
        let reads = self.reads;
        Ok(Box::new(self.inner.scan_prefix(prefix)?.inspect(
            move |(key, _)| {
                if KeyEncoder::decode_row_key(key).is_ok() {
                    reads.scans.fetch_add(1, Ordering::Relaxed);
                }
            },
        )))
    }

    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> CoreResult<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        let reads = self.reads;
        Ok(Box::new(self.inner.scan_range(start, end)?.inspect(
            move |(key, _)| {
                if KeyEncoder::decode_row_key(key).is_ok() {
                    reads.scans.fetch_add(1, Ordering::Relaxed);
                }
            },
        )))
    }

    fn scan_from(
        &mut self,
        start: &[u8],
    ) -> CoreResult<Box<dyn Iterator<Item = (Key, Value)> + '_>> {
        let reads = self.reads;
        Ok(Box::new(self.inner.scan_from(start)?.inspect(
            move |(key, _)| {
                if KeyEncoder::decode_row_key(key).is_ok() {
                    reads.scans.fetch_add(1, Ordering::Relaxed);
                }
            },
        )))
    }

    fn commit_self(self) -> CoreResult<()> {
        self.inner.commit_self()
    }

    fn journal_pending_writes(&self) -> Option<Vec<alopex_core::kv::JournalPendingWrite>> {
        self.inner.journal_pending_writes()
    }

    fn rollback_self(self) -> CoreResult<()> {
        self.inner.rollback_self()
    }
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn btree_access_path_reads_only_matching_table_rows() {
    // This measures rows delivered across the SQL/KV boundary in a single-table
    // fixture. It excludes index/metadata keys, backend-internal traversal,
    // setup writes, timing, and claims about storage-engine complexity.
    let store = Arc::new(CountingKV::default());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::clone(&store), catalog);
    executor
        .execute(LogicalPlan::CreateTable {
            table: TableMetadata::new(
                "items",
                vec![ColumnMetadata::new("score", ResolvedType::Integer)],
            ),
            if_not_exists: false,
            with_options: vec![],
        })
        .unwrap();
    let number =
        |value: &str, ty| literal(TypedExprKind::Literal(Literal::Number(value.into())), ty);
    executor
        .execute(LogicalPlan::Insert {
            table: "items".into(),
            columns: vec!["score".into()],
            values: (0..256)
                .map(|value| vec![number(&value.to_string(), ResolvedType::Integer)])
                .collect(),
            conflict: None,
            returning: None,
        })
        .unwrap();
    let query = |op, value| {
        LogicalPlan::filter(
            LogicalPlan::scan("items".into(), Projection::All(vec!["score".into()])),
            TypedExpr {
                kind: TypedExprKind::BinaryOp {
                    left: Box::new(TypedExpr {
                        kind: TypedExprKind::ColumnRef {
                            table: "items".into(),
                            column: "score".into(),
                            column_index: 0,
                        },
                        resolved_type: ResolvedType::Integer,
                        span: Span::default(),
                    }),
                    op,
                    right: Box::new(value),
                },
                resolved_type: ResolvedType::Boolean,
                span: Span::default(),
            },
        )
    };
    let equality = query(BinaryOp::Eq, number("253", ResolvedType::Integer));
    let range = query(BinaryOp::GtEq, number("253", ResolvedType::Integer));
    let fallback = query(BinaryOp::Eq, number("253.0", ResolvedType::Double));
    let run = |executor: &mut Executor<CountingKV, MemoryCatalog>, plan| {
        store.reads.reset();
        let ExecutionResult::Query(result) = executor.execute(plan).unwrap() else {
            panic!("expected query result");
        };
        (result.rows, store.reads.snapshot())
    };
    let (baseline_equal, counts) = run(&mut executor, equality.clone());
    assert_eq!(baseline_equal, vec![vec![SqlValue::Integer(253)]]);
    assert_eq!(counts, (0, 256), "unindexed equality (get, scan)");
    let (baseline_range, counts) = run(&mut executor, range.clone());
    assert_eq!(
        baseline_range,
        (253..256)
            .map(|value| vec![SqlValue::Integer(value)])
            .collect::<Vec<_>>()
    );
    assert_eq!(counts, (0, 256), "unindexed range (get, scan)");
    executor
        .execute(LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "idx_items_score",
                "items",
                vec!["score".into()],
            )
            .with_method(alopex_sql::ast::ddl::IndexMethod::BTree),
            if_not_exists: false,
        })
        .unwrap();
    let (rows, counts) = run(&mut executor, equality);
    assert_eq!(rows, baseline_equal);
    assert_eq!(counts, (1, 0), "indexed equality (get, scan)");
    let (rows, counts) = run(&mut executor, range);
    assert_eq!(rows, baseline_range);
    assert_eq!(counts, (3, 0), "indexed range (get, scan)");
    let (rows, counts) = run(&mut executor, fallback);
    assert_eq!(rows, baseline_equal);
    assert_eq!(counts, (0, 256), "cross-type fallback (get, scan)");
}
