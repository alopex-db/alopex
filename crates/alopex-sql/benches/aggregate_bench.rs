use std::fmt::Write;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionConfig, ExecutionResult, Executor, ExecutorError};
use alopex_sql::parser::Parser;
use alopex_sql::planner::{LogicalPlan, Planner};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

struct Harness {
    dialect: AlopexDialect,
    catalog: Arc<RwLock<MemoryCatalog>>,
    executor: Executor<MemoryKV, MemoryCatalog>,
}

impl Harness {
    fn new() -> Self {
        let store = Arc::new(MemoryKV::new());
        let catalog = Arc::new(RwLock::new(MemoryCatalog::new()));
        let executor = Executor::new(store, catalog.clone());
        Self {
            dialect: AlopexDialect,
            catalog,
            executor,
        }
    }

    fn plan(&self, sql: &str) -> LogicalPlan {
        let stmts = Parser::parse_sql(&self.dialect, sql).expect("parse");
        assert_eq!(stmts.len(), 1, "multi-stmt not supported in bench");
        let stmt = &stmts[0];
        let guard = self.catalog.read().expect("catalog lock");
        let planner = Planner::new(&*guard);
        planner.plan(stmt).expect("plan")
    }

    fn exec_plan(
        &mut self,
        plan: LogicalPlan,
        config: &ExecutionConfig,
    ) -> Result<ExecutionResult, ExecutorError> {
        self.executor.execute(plan, config)
    }

    fn exec_sql(&mut self, sql: &str) {
        let plan = self.plan(sql);
        let config = ExecutionConfig::default();
        self.exec_plan(plan, &config).expect("execute");
    }
}

fn setup_aggregate_dataset(rows: usize, groups: usize) -> Harness {
    let mut harness = Harness::new();
    harness.exec_sql("CREATE TABLE agg (group_id INT);");
    load_rows(&mut harness, rows, groups);
    harness
}

fn load_rows(harness: &mut Harness, rows: usize, groups: usize) {
    let batch_size = 1000usize;
    let mut inserted = 0usize;
    while inserted < rows {
        let upper = (inserted + batch_size).min(rows);
        let mut values = String::new();
        for i in inserted..upper {
            if i > inserted {
                values.push(',');
            }
            let group_id = (i % groups) as i32;
            write!(&mut values, "({})", group_id).expect("write values");
        }
        let insert = format!("INSERT INTO agg (group_id) VALUES {};", values);
        harness.exec_sql(&insert);
        inserted = upper;
        if inserted.is_multiple_of(200_000) {
            eprintln!("inserted {} rows", inserted);
        }
    }
}

fn bench_aggregate_1m_1k(c: &mut Criterion) {
    const ROWS: usize = 1_000_000;
    const GROUPS: usize = 1_000;
    let mut harness = setup_aggregate_dataset(ROWS, GROUPS);
    let plan = harness.plan("SELECT group_id, COUNT(*) FROM agg GROUP BY group_id");
    let config = ExecutionConfig::default();
    c.bench_function("aggregate_1m_rows_1k_groups", |b| {
        b.iter(|| {
            let result = harness.exec_plan(plan.clone(), &config).expect("execute");
            black_box(result);
        })
    });
}

fn bench_aggregate_group_limit(c: &mut Criterion) {
    const ROWS: usize = 1_000_000;
    const GROUPS: usize = 1_000;
    let mut harness = setup_aggregate_dataset(ROWS, GROUPS);
    let plan = harness.plan("SELECT group_id, COUNT(*) FROM agg GROUP BY group_id");
    let config = ExecutionConfig { max_groups: 100 };
    c.bench_function("aggregate_group_limit_exceeded", |b| {
        b.iter(|| {
            let result = harness.exec_plan(plan.clone(), &config);
            match result {
                Err(ExecutorError::TooManyGroups { .. }) => {}
                other => panic!("expected TooManyGroups error, got {other:?}"),
            }
        })
    });
}

criterion_group! {
    name = aggregate_bench;
    config = Criterion::default()
        .measurement_time(Duration::from_millis(500))
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1));
    targets = bench_aggregate_1m_1k, bench_aggregate_group_limit
}
criterion_main!(aggregate_bench);
