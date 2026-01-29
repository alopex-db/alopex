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

struct BenchHarness {
    executor: Executor<MemoryKV, MemoryCatalog>,
    catalog: Arc<RwLock<MemoryCatalog>>,
}

impl BenchHarness {
    fn new(row_count: usize) -> Self {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let mut executor = Executor::new(Arc::clone(&store), Arc::clone(&catalog));

        let table = TableMetadata::new(
            "sales",
            vec![
                ColumnMetadata::new("category", ResolvedType::Text),
                ColumnMetadata::new("region", ResolvedType::Text),
                ColumnMetadata::new("amount", ResolvedType::Integer),
            ],
        );
        executor
            .execute(LogicalPlan::CreateTable {
                table: table.clone(),
                if_not_exists: false,
                with_options: vec![],
            })
            .unwrap();

        let table_meta = catalog.read().unwrap().get_table("sales").cloned().unwrap();
        let bridge = TxnBridge::new(Arc::clone(&store));
        let mut txn = bridge.begin_write().unwrap();
        txn.with_table(&table_meta, |storage| {
            for i in 0..row_count {
                let category = format!("c{}", i % 100);
                let region = format!("r{}", i % 10);
                let amount = (i % 1000) as i32;
                let row = vec![
                    SqlValue::Text(category),
                    SqlValue::Text(region),
                    SqlValue::Integer(amount),
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

fn bench_group_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by");
    group.measurement_time(Duration::from_secs(2));

    for size in [100_000usize, 1_000_000usize] {
        group.bench_function(format!("count_{size}"), |b| {
            b.iter_batched(
                || {
                    let harness = BenchHarness::new(size);
                    let plan =
                        harness.plan("SELECT category, COUNT(*) FROM sales GROUP BY category");
                    (harness, plan)
                },
                |(mut harness, plan)| {
                    let result = harness.executor.execute(plan).unwrap();
                    black_box(result);
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("sum_multi_column_{size}"), |b| {
            b.iter_batched(
                || {
                    let harness = BenchHarness::new(size);
                    let plan = harness.plan(
                        "SELECT category, region, SUM(amount) FROM sales GROUP BY category, region",
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

        group.bench_function(format!("avg_{size}"), |b| {
            b.iter_batched(
                || {
                    let harness = BenchHarness::new(size);
                    let plan =
                        harness.plan("SELECT category, AVG(amount) FROM sales GROUP BY category");
                    (harness, plan)
                },
                |(mut harness, plan)| {
                    let result = harness.executor.execute(plan).unwrap();
                    black_box(result);
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("min_max_{size}"), |b| {
            b.iter_batched(
                || {
                    let harness = BenchHarness::new(size);
                    let plan = harness.plan(
                        "SELECT category, MIN(amount), MAX(amount) FROM sales GROUP BY category",
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
}

criterion_group! {
    name = group_by_benches;
    config = Criterion::default().measurement_time(Duration::from_secs(2));
    targets = bench_group_by
}
criterion_main!(group_by_benches);
