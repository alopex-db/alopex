use std::sync::{Arc, RwLock};

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::{ExecutionConfig, ExecutionResult, Executor};
use alopex_sql::parser::Parser;
use alopex_sql::planner::LogicalPlan;
use alopex_sql::planner::Planner;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

#[derive(Clone, Copy)]
enum Variant {
    RowIdDirect,
    NoRowId,
}

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
        let cat = self.catalog.read().expect("catalog lock");
        let planner = Planner::new(&*cat);
        planner.plan(stmt).expect("plan")
    }

    fn exec_sql(&mut self, sql: &str) -> ExecutionResult {
        let plan = self.plan(sql);
        let config = ExecutionConfig::default();
        self.executor.execute(plan, &config).expect("execute")
    }
}

/// 小規模データで Columnar + KNN を通すセットアップ。
fn setup_dataset(variant: Variant) -> (Harness, Vec<Vec<f32>>) {
    let mut harness = Harness::new();
    let create = match variant {
        Variant::RowIdDirect => {
            "CREATE TABLE items (row_id BIGINT, id BIGINT, ts BIGINT, cat TEXT, metric1 DOUBLE, metric2 DOUBLE, embedding VECTOR(8)) WITH (storage='columnar', row_group_size=1024);"
        }
        Variant::NoRowId => {
            "CREATE TABLE items (id BIGINT, ts BIGINT, cat TEXT, metric1 DOUBLE, metric2 DOUBLE, embedding VECTOR(8)) WITH (storage='columnar', row_group_size=1024);"
        }
    };
    harness.exec_sql(create);

    let mut vectors = Vec::new();
    // 200 件ほどの小さなデータセットでベクトル検索のロジック差分を比較する。
    for i in 0..200u64 {
        let base = i as f64;
        let vec_vals: Vec<f64> = (0..8).map(|d| base + d as f64 * 0.01).collect();
        let cat = format!("C{:03}", i % 20);
        let insert = match variant {
            Variant::RowIdDirect => format!(
                "INSERT INTO items (row_id, id, ts, cat, metric1, metric2, embedding) VALUES ({}, {}, {}, '{}', {}, {}, [{}]);",
                i,
                i,
                1 + i as i64,
                cat,
                base * 0.001,
                base * 0.002,
                vec_to_literal(&vec_vals)
            ),
            Variant::NoRowId => format!(
                "INSERT INTO items (id, ts, cat, metric1, metric2, embedding) VALUES ({}, {}, '{}', {}, {}, [{}]);",
                i,
                1 + i as i64,
                cat,
                base * 0.001,
                base * 0.002,
                vec_to_literal(&vec_vals)
            ),
        };
        harness.exec_sql(&insert);
        vectors.push(vec_vals.iter().map(|v| *v as f32).collect());
    }

    (harness, vectors)
}

/// 50万行のデータセット（RowID あり）で Columnar + KNN を通す。
fn setup_dataset_large_rowid(rows: usize) -> Harness {
    let mut harness = Harness::new();
    let create = "CREATE TABLE items (row_id BIGINT, id BIGINT, ts BIGINT, cat TEXT, metric1 DOUBLE, metric2 DOUBLE, embedding VECTOR(8)) WITH (storage='columnar', row_group_size=65536);";
    harness.exec_sql(create);

    load_large_rows(
        &mut harness,
        rows,
        |i, cat, vec_literal, base| {
            format!(
                "({}, {}, {}, '{}', {}, {}, [{}])",
                i, // row_id
                i,
                1 + i as i64,
                cat,
                base * 0.001,
                base * 0.002,
                vec_literal
            )
        },
        "INSERT INTO items (row_id, id, ts, cat, metric1, metric2, embedding) VALUES {};",
    );

    harness
}

fn load_large_rows<F>(harness: &mut Harness, rows: usize, row_builder: F, insert_tmpl: &str)
where
    F: Fn(usize, String, String, f64) -> String,
{
    let batch_size = 1000usize;
    let mut inserted = 0usize;
    while inserted < rows {
        let upper = (inserted + batch_size).min(rows);
        let mut values = Vec::with_capacity(upper - inserted);
        for i in inserted..upper {
            let base = i as f64;
            let vec_vals: Vec<f64> = (0..8).map(|d| base + d as f64 * 0.01).collect();
            let cat = format!("C{:03}", i % 1000);
            values.push(row_builder(i, cat, vec_to_literal(&vec_vals), base));
        }
        let insert = insert_tmpl.replace("{}", &values.join(","));
        harness.exec_sql(&insert);
        inserted = upper;
        if inserted.is_multiple_of(100_000) {
            eprintln!("inserted {} rows", inserted);
        }
    }
}

fn vec_to_literal(values: &[f64]) -> String {
    values
        .iter()
        .map(|v| format!("{:.6}", v))
        .collect::<Vec<_>>()
        .join(",")
}

fn bench_knn_executor_no_rowid(c: &mut Criterion) {
    c.bench_function("knn_executor_columnar_no_rowid", |b| {
        b.iter_batched(
            || {
                let (h, _vecs) = setup_dataset(Variant::NoRowId);
                let query = "SELECT id, vector_similarity(embedding, [0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5], 'cosine') AS score FROM items ORDER BY 2 DESC LIMIT 5;";
                (h, query.to_string())
            },
            |mut state| {
                let _ = state.0.exec_sql(&state.1);
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_knn_executor_rowid(c: &mut Criterion) {
    c.bench_function("knn_executor_columnar_rowid", |b| {
        b.iter_batched(
            || {
                let (h, _vecs) = setup_dataset(Variant::RowIdDirect);
                let query = "SELECT row_id, vector_similarity(embedding, [0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5], 'cosine') AS score FROM items ORDER BY 2 DESC LIMIT 5;";
                (h, query.to_string())
            },
            |mut state| {
                let _ = state.0.exec_sql(&state.1);
            },
            BatchSize::SmallInput,
        )
    });
}

/// 50万行データセット（RowID あり）で KNN executor を計測（データ構築は計測外で一度だけ行う）。
fn bench_knn_executor_large_rowid(c: &mut Criterion) {
    const ROWS: usize = 500_000;
    let mut harness = setup_dataset_large_rowid(ROWS);
    let query = "SELECT row_id, id, vector_similarity(embedding, [0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5], 'cosine') AS score FROM items ORDER BY 3 DESC LIMIT 10;";
    c.bench_function("knn_columnar_500k_rowid", |b| {
        b.iter(|| {
            let _ = harness.exec_sql(query);
        })
    });
}

criterion_group!(
    benches,
    bench_knn_executor_no_rowid,
    bench_knn_executor_rowid,
    bench_knn_executor_large_rowid
);
criterion_main!(benches);
