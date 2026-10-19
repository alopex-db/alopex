use super::*;
use alopex_core::error::Result as CoreResult;
use alopex_core::kv::memory::MemoryTransaction;
use alopex_core::kv::{KVStore, KVTransaction};
use alopex_core::txn::TxnManager;
use alopex_core::types::{Key, TxnId, TxnMode, Value};
use alopex_sql::ast::expr::{BinaryOp, Literal};
use alopex_sql::storage::{KeyEncoder, SqlValue};
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn btree_retained_rows_respect_default_and_large_policy_ceiling() {
    const PAYLOAD_BYTES: usize = 20 * 1024;
    const INDEX_BYTE_CEILING: usize = 8 * 1024 * 1024;
    let store = Arc::new(CountingKV::default());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::clone(&store), Arc::clone(&catalog));
    executor
        .execute(LogicalPlan::CreateTable {
            table: TableMetadata::new(
                "wide_items",
                vec![
                    ColumnMetadata::new("score", ResolvedType::Integer),
                    ColumnMetadata::new("payload", ResolvedType::Text),
                ],
            ),
            if_not_exists: false,
            with_options: vec![],
        })
        .unwrap();
    let number = |value: i32| {
        literal(
            TypedExprKind::Literal(Literal::Number(value.to_string())),
            ResolvedType::Integer,
        )
    };
    let payload = "x".repeat(PAYLOAD_BYTES);
    executor
        .execute(LogicalPlan::Insert {
            table: "wide_items".into(),
            columns: vec!["score".into(), "payload".into()],
            values: (0..512)
                .map(|row| {
                    vec![
                        number(row / 256 + 1),
                        literal(
                            TypedExprKind::Literal(Literal::String(payload.clone())),
                            ResolvedType::Text,
                        ),
                    ]
                })
                .collect(),
            conflict: None,
            returning: None,
        })
        .unwrap();
    executor
        .execute(LogicalPlan::CreateIndex {
            index: alopex_sql::catalog::IndexMetadata::new(
                0,
                "idx_wide_items_score",
                "wide_items",
                vec!["score".into()],
            ),
            if_not_exists: false,
        })
        .unwrap();
    let bridge = alopex_sql::storage::TxnBridge::new(Arc::clone(&store));
    let mut read = bridge.begin_read().unwrap();
    for limit in [None, Some(16 * 1024 * 1024)] {
        let policy = limit.map(|bytes| {
            alopex_sql::executor::MemoryPolicy::new(
                Some(bytes),
                alopex_sql::executor::SpillPolicy::FailFast,
            )
        });
        // Both matches are below the 1,024-ID cap. The equality retains about
        // 5 MiB; the range would retain over 10 MiB unless the byte cap applies.
        for (op, expected_rows) in [(BinaryOp::Eq, 256), (BinaryOp::GtEq, 512)] {
            let plan = LogicalPlan::filter(
                LogicalPlan::scan(
                    "wide_items".into(),
                    Projection::All(vec!["score".into(), "payload".into()]),
                ),
                literal(
                    TypedExprKind::BinaryOp {
                        left: Box::new(literal(
                            TypedExprKind::ColumnRef {
                                table: "wide_items".into(),
                                column: "score".into(),
                                column_index: 0,
                            },
                            ResolvedType::Integer,
                        )),
                        op,
                        right: Box::new(number(1)),
                    },
                    ResolvedType::Boolean,
                ),
            );
            store.reads.reset();
            let mut result = alopex_sql::executor::query::execute_query_streaming_with_policy(
                &mut read,
                &*catalog.read().unwrap(),
                plan,
                policy.as_ref(),
            )
            .unwrap();
            let mut count = 0;
            while let Some(row) = result.next_row().unwrap() {
                assert_eq!(row[0], SqlValue::Integer(count / 256 + 1));
                assert!(matches!(&row[1], SqlValue::Text(text) if text == &payload));
                count += 1;
            }
            assert_eq!(count, expected_rows, "limit={limit:?}, op={op:?}");
            let (gets, scans) = store.reads.snapshot();
            if op == BinaryOp::Eq {
                assert_eq!((gets, scans), (256, 0), "limit={limit:?}");
            } else {
                // Include at most one fetched row that crosses the byte cap.
                assert!((1..=INDEX_BYTE_CEILING / PAYLOAD_BYTES + 1).contains(&gets));
                assert_eq!(scans, 512, "limit={limit:?}: must stream the table");
            }
        }
    }
}

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
    let mut executor = Executor::new(Arc::clone(&store), Arc::clone(&catalog));
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

    let bridge = alopex_sql::storage::TxnBridge::new(Arc::clone(&store));
    let mut read = bridge.begin_read().unwrap();
    for (budget, indexed) in [
        (64, false),
        (
            (std::mem::size_of::<alopex_sql::executor::Row>() + std::mem::size_of::<u64>() + 4)
                as u64,
            true,
        ),
        (
            (std::mem::size_of::<alopex_sql::executor::Row>() + std::mem::size_of::<u64>() + 4)
                as u64
                - 4,
            false,
        ),
    ] {
        let predicate = if budget == 64 {
            query(BinaryOp::GtEq, number("247", ResolvedType::Integer))
        } else {
            query(BinaryOp::Eq, number("253", ResolvedType::Integer))
        };
        let policy = alopex_sql::executor::MemoryPolicy::new(
            Some(budget),
            alopex_sql::executor::SpillPolicy::FailFast,
        );
        store.reads.reset();
        let mut rows = alopex_sql::executor::query::execute_query_streaming_with_policy(
            &mut read,
            &*catalog.read().unwrap(),
            predicate,
            Some(&policy),
        )
        .unwrap();
        assert!(rows.next_row().unwrap().is_some());
        let (gets, scans) = store.reads.snapshot();
        if indexed {
            assert_eq!((gets, scans), (1, 0), "budget {budget}");
        } else {
            assert!(scans > 0, "budget {budget} must stream table rows");
            assert_eq!(gets, usize::from(budget != 64), "budget {budget}");
        }
    }
    drop(read);

    // A broad range and a heavily duplicated equality must stop collecting
    // index candidates and use the streaming table scan instead.
    executor
        .execute(LogicalPlan::Insert {
            table: "items".into(),
            columns: vec!["score".into()],
            values: (0..1023)
                .map(|_| vec![number("253", ResolvedType::Integer)])
                .collect(),
            conflict: None,
            returning: None,
        })
        .unwrap();
    let (rows, counts) = run(
        &mut executor,
        query(BinaryOp::Eq, number("253", ResolvedType::Integer)),
    );
    assert_eq!(rows.len(), 1024);
    assert_eq!(counts, (1024, 0), "candidate cap remains indexable");
    executor
        .execute(LogicalPlan::Insert {
            table: "items".into(),
            columns: vec!["score".into()],
            values: (0..2)
                .map(|_| vec![number("253", ResolvedType::Integer)])
                .collect(),
            conflict: None,
            returning: None,
        })
        .unwrap();
    for (plan, expected_rows) in [
        (
            query(BinaryOp::GtEq, number("0", ResolvedType::Integer)),
            1281,
        ),
        (
            query(BinaryOp::Eq, number("253", ResolvedType::Integer)),
            1026,
        ),
    ] {
        let (rows, counts) = run(&mut executor, plan);
        assert_eq!(rows.len(), expected_rows);
        assert_eq!(counts, (0, 1281), "broad predicate must scan table");
    }
}

#[test]
#[ignore = "manual SQL/row-read measurement; requires ALOPEX_BTREE_MEASUREMENT_OUTPUT"]
fn btree_budget_access_path_measurement() {
    use alopex_sql::{AlopexDialect, Parser, Planner};
    use std::time::Instant;

    const ROWS: i32 = 4000;
    const SAMPLES: usize = 11;
    const REPEATS: usize = 20;
    let indexed = std::env::var("ALOPEX_BTREE_MEASUREMENT_INDEXED").unwrap() == "1";
    let store = Arc::new(CountingKV::default());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(store.clone(), catalog.clone());
    let mut sql = |text: &str| {
        let statement = Parser::parse_sql(&AlopexDialect, text).unwrap().remove(0);
        let plan = Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap();
        executor.execute(plan).unwrap()
    };
    sql("CREATE TABLE measured (id INTEGER PRIMARY KEY, score INTEGER, bucket INTEGER)");
    for batch in 0..8 {
        let values = (batch * 500..(batch + 1) * 500)
            .map(|id| format!("({id},{id},{})", id % 2))
            .collect::<Vec<_>>()
            .join(",");
        sql(&format!("INSERT INTO measured VALUES {values}"));
    }
    if indexed {
        sql("CREATE INDEX measured_score ON measured (score)");
        sql("CREATE INDEX measured_bucket ON measured (bucket)");
    }
    let mut cases = Vec::new();
    for (name, predicate, ids, indexed_reads) in [
        ("point", "score = 2000", vec![2000], (1, 0)),
        (
            "candidate_cap",
            "score < 1024",
            (0..1024).collect(),
            (1024, 0),
        ),
        (
            "broad",
            "score >= 0",
            (0..ROWS).collect(),
            (0, ROWS as usize),
        ),
        (
            "duplicates",
            "bucket = 0",
            (0..ROWS).step_by(2).collect(),
            (0, ROWS as usize),
        ),
    ] {
        let expected: Vec<Vec<SqlValue>> = ids
            .into_iter()
            .map(|id| vec![SqlValue::Integer(id)])
            .collect();
        let query = format!("SELECT id FROM measured WHERE {predicate} ORDER BY id");
        let expected_reads = if indexed {
            indexed_reads
        } else {
            (0, ROWS as usize)
        };
        let mut samples = Vec::new();
        for sample in 0..SAMPLES + 2 {
            let mut elapsed = 0;
            for _ in 0..REPEATS {
                store.reads.reset();
                let started = Instant::now();
                let ExecutionResult::Query(result) = sql(&query) else {
                    panic!("query expected")
                };
                elapsed += started.elapsed().as_nanos();
                assert_eq!(result.rows, expected, "{name}");
                assert_eq!(
                    store.reads.snapshot(),
                    expected_reads,
                    "{name}: runtime path changed"
                );
            }
            if sample >= 2 {
                samples.push(elapsed);
            }
        }
        let mut ordered = samples.clone();
        ordered.sort_unstable();
        cases.push(serde_json::json!({
            "case": name, "sql": query, "result_rows": expected.len(),
            "row_gets_per_query": expected_reads.0, "row_scans_per_query": expected_reads.1,
            "samples_ns": samples, "median_ns_per_query": ordered[SAMPLES / 2] / REPEATS as u128,
        }));
    }
    let output = std::env::var("ALOPEX_BTREE_MEASUREMENT_OUTPUT").unwrap();
    let data = serde_json::json!({
        "schema": "alopex.btree-budget-measurement/v1", "indexed": indexed,
        "source_commit": std::env::var("ALOPEX_BTREE_MEASUREMENT_SOURCE_COMMIT").unwrap(),
        "rows": ROWS, "samples": SAMPLES, "warmup_samples": 2,
        "repeats_per_sample": REPEATS, "cases": cases,
    });
    std::fs::write(output, serde_json::to_vec_pretty(&data).unwrap()).unwrap();
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn knn_cost_boundary_changes_table_reads_without_changing_nearest_ids() {
    // Reuse the SQL/KV row counter: this counts delivered table rows, not
    // graph traversal, index metadata, or backend-internal operations.
    let store = Arc::new(CountingKV::default());
    let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
    let mut executor = Executor::new(Arc::clone(&store), Arc::clone(&catalog));
    let run = |executor: &mut Executor<CountingKV, MemoryCatalog>, sql: &str| {
        let statement =
            alopex_sql::parser::Parser::parse_sql(&alopex_sql::dialect::AlopexDialect, sql)
                .unwrap()
                .pop()
                .unwrap();
        let plan = alopex_sql::planner::Planner::new(&*catalog.read().unwrap())
            .plan(&statement)
            .unwrap();
        executor.execute(plan).unwrap()
    };
    run(
        &mut executor,
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(128, L2))",
    );
    let tail = ", 0".repeat(127);
    for start in (1..=8_192).step_by(256) {
        let values = (start..start + 256)
            .map(|id| format!("({id}, [{id}{tail}])"))
            .collect::<Vec<_>>()
            .join(",");
        run(&mut executor, &format!("INSERT INTO items VALUES {values}"));
    }
    run(
        &mut executor,
        "CREATE INDEX idx_items_embedding ON items (embedding) USING HNSW WITH (ef_search=64)",
    );
    let query = format!(
        "SELECT id FROM items ORDER BY vector_distance(embedding, [0{tail}], 'l2') ASC LIMIT 10"
    );
    let expected = (1..=10)
        .map(|id| vec![SqlValue::Integer(id)])
        .collect::<Vec<_>>();
    for (rows, path, counts) in [
        (8_192, "ExactKnnScan", (0, 8_192)),
        (8_193, "HnswSearch index=idx_items_embedding", (10, 0)),
    ] {
        if rows == 8_193 {
            run(
                &mut executor,
                &format!("INSERT INTO items VALUES (8193, [8193{tail}])"),
            );
        }
        let ExecutionResult::Query(plan) = run(&mut executor, &format!("EXPLAIN ANALYZE {query}"))
        else {
            panic!("expected explain result");
        };
        let SqlValue::Text(plan) = &plan.rows[0][0] else {
            panic!("expected plan text");
        };
        assert!(plan.contains(path), "{plan}");
        if rows == 8_193 {
            assert!(plan.contains("fallback=none"), "{plan}");
            assert!(!plan.contains("nodes_visited=0"), "{plan}");
        }
        store.reads.reset();
        let ExecutionResult::Query(result) = run(&mut executor, &query) else {
            panic!("expected query result");
        };
        assert_eq!(result.rows, expected, "rows={rows}");
        assert_eq!(store.reads.snapshot(), counts, "rows={rows} (get, scan)");
    }
    store.reads.reset();
    let ExecutionResult::Query(result) =
        run(&mut executor, &format!("{query} WITH (enable_hnsw=false)"))
    else {
        panic!("expected exact query result");
    };
    assert_eq!(result.rows, expected);
    assert_eq!(store.reads.snapshot(), (0, 8_193));
}
