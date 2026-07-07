//! Parse and execution performance benchmarks for the Nim-backed SQL parser
//! and the JOIN / subquery execution paths.
//!
//! Phase 8.1 of `nim-sql-parser-migration`:
//! 1. Parse throughput for representative SQL (SELECT+WHERE, JOIN, subquery).
//! 2. INNER / LEFT JOIN execution over two populated tables.
//! 3. IN / EXISTS / scalar subquery execution.
//!
//! Note: SQL parsing is routed through the Nim FFI bridge, so running this
//! benchmark requires the Nim shared library to be discoverable via
//! `NIM_SQL_PARSER_LIB_DIR`.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::{Catalog, ColumnMetadata, MemoryCatalog, TableMetadata};
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::Executor;
use alopex_sql::planner::Planner;
use alopex_sql::planner::types::ResolvedType;
use alopex_sql::storage::{SqlValue, TxnBridge};
use alopex_sql::{LogicalPlan, Parser};
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};

/// Representative SQL statements exercised by the parse benchmark.
const SELECT_WHERE_SQL: &str = "SELECT users.id, users.name FROM users WHERE users.id > 100 AND users.name <> 'x' ORDER BY users.id LIMIT 50";
const INNER_JOIN_SQL: &str = "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id WHERE orders.total > 10 ORDER BY orders.id";
const SUBQUERY_SQL: &str = "SELECT users.name FROM users WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.total > 20) ORDER BY users.id";

/// Benchmark harness that owns an executor with two populated tables
/// (`users` and `orders`) suitable for JOIN and subquery execution.
struct BenchHarness {
    executor: Executor<MemoryKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl BenchHarness {
    fn new(user_count: usize, order_count: usize) -> Self {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = Executor::new(Arc::clone(&store), Arc::clone(&catalog));

        let users = TableMetadata::new(
            "users",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer),
                ColumnMetadata::new("name", ResolvedType::Text),
            ],
        );
        executor
            .execute(LogicalPlan::CreateTable {
                table: users,
                if_not_exists: false,
                with_options: vec![],
            })
            .unwrap();

        let orders = TableMetadata::new(
            "orders",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer),
                ColumnMetadata::new("user_id", ResolvedType::Integer),
                ColumnMetadata::new("total", ResolvedType::Integer),
            ],
        );
        executor
            .execute(LogicalPlan::CreateTable {
                table: orders,
                if_not_exists: false,
                with_options: vec![],
            })
            .unwrap();

        let users_meta = catalog.read().unwrap().get_table("users").cloned().unwrap();
        let orders_meta = catalog
            .read()
            .unwrap()
            .get_table("orders")
            .cloned()
            .unwrap();

        let bridge = TxnBridge::new(Arc::clone(&store));
        let mut txn = bridge.begin_write().unwrap();

        txn.with_table(&users_meta, |storage| {
            for i in 0..user_count {
                let row = vec![
                    SqlValue::Integer(i as i32),
                    SqlValue::Text(format!("user{i}")),
                ];
                storage.insert(i as u64, &row)?;
            }
            Ok(())
        })
        .unwrap();

        txn.with_table(&orders_meta, |storage| {
            for i in 0..order_count {
                // Every order references an existing user so equi-joins match.
                let user_id = (i % user_count.max(1)) as i32;
                let total = (i % 200) as i32;
                let row = vec![
                    SqlValue::Integer(i as i32),
                    SqlValue::Integer(user_id),
                    SqlValue::Integer(total),
                ];
                storage.insert(i as u64, &row)?;
            }
            Ok(())
        })
        .unwrap();

        txn.commit().unwrap();

        Self { executor, catalog }
    }

    fn plan(&self, sql: &str) -> LogicalPlan {
        let dialect = AlopexDialect;
        let stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);
        let guard = self.catalog.read().unwrap();
        Planner::new(&*guard).plan(&stmt).unwrap()
    }
}

/// Benchmark 1: raw parse throughput for representative SQL shapes.
fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse");
    group.measurement_time(Duration::from_secs(2));

    let dialect = AlopexDialect;
    let cases = [
        ("select_where", SELECT_WHERE_SQL),
        ("inner_join", INNER_JOIN_SQL),
        ("subquery_in", SUBQUERY_SQL),
    ];

    for (name, sql) in cases {
        group.bench_function(name, |b| {
            b.iter(|| {
                let statements = Parser::parse_sql(&dialect, black_box(sql)).unwrap();
                black_box(statements);
            })
        });
    }

    group.finish();
}

/// Benchmark 2: INNER / LEFT JOIN execution over populated tables.
fn bench_join_exec(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_exec");
    group.measurement_time(Duration::from_secs(2));

    for (user_count, order_count) in [(200usize, 1_000usize), (1_000usize, 5_000usize)] {
        let label = format!("{user_count}x{order_count}");

        group.bench_function(format!("inner_join_{label}"), |b| {
            b.iter_batched(
                || {
                    let harness = BenchHarness::new(user_count, order_count);
                    let plan = harness.plan(
                        "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.id",
                    );
                    (harness, plan)
                },
                |(mut harness, plan)| {
                    let result = harness.executor.execute(plan).unwrap();
                    black_box(result);
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("left_join_{label}"), |b| {
            b.iter_batched(
                || {
                    let harness = BenchHarness::new(user_count, order_count);
                    let plan = harness.plan(
                        "SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id, orders.id",
                    );
                    (harness, plan)
                },
                |(mut harness, plan)| {
                    let result = harness.executor.execute(plan).unwrap();
                    black_box(result);
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

/// Benchmark 3: IN / EXISTS / scalar subquery execution.
fn bench_subquery_exec(c: &mut Criterion) {
    let mut group = c.benchmark_group("subquery_exec");
    group.measurement_time(Duration::from_secs(2));

    for (user_count, order_count) in [(200usize, 1_000usize), (1_000usize, 5_000usize)] {
        let label = format!("{user_count}x{order_count}");

        group.bench_function(format!("in_subquery_{label}"), |b| {
            b.iter_batched(
                || {
                    let harness = BenchHarness::new(user_count, order_count);
                    let plan = harness.plan(
                        "SELECT users.name FROM users WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.total > 20) ORDER BY users.id",
                    );
                    (harness, plan)
                },
                |(mut harness, plan)| {
                    let result = harness.executor.execute(plan).unwrap();
                    black_box(result);
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("exists_subquery_{label}"), |b| {
            b.iter_batched(
                || {
                    let harness = BenchHarness::new(user_count, order_count);
                    let plan = harness.plan(
                        "SELECT users.name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY users.id",
                    );
                    (harness, plan)
                },
                |(mut harness, plan)| {
                    let result = harness.executor.execute(plan).unwrap();
                    black_box(result);
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("scalar_subquery_{label}"), |b| {
            b.iter_batched(
                || {
                    let harness = BenchHarness::new(user_count, order_count);
                    let plan = harness.plan(
                        "SELECT users.name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users ORDER BY users.id",
                    );
                    (harness, plan)
                },
                |(mut harness, plan)| {
                    let result = harness.executor.execute(plan).unwrap();
                    black_box(result);
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

criterion_group! {
    name = parser_exec_benches;
    config = Criterion::default().measurement_time(Duration::from_secs(2));
    targets = bench_parse, bench_join_exec, bench_subquery_exec
}
criterion_main!(parser_exec_benches);
