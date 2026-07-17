//! Throughput baseline for registry-dispatched scalar evaluation.

use alopex_sql::executor::evaluator::registry::scalar_registry;
use alopex_sql::storage::SqlValue;
use criterion::{Criterion, black_box, criterion_group, criterion_main};

const ROWS: usize = 1_000_000;

fn bench_scalar_registry(c: &mut Criterion) {
    let values: Vec<_> = (0..ROWS)
        .map(|value| SqlValue::Integer((value % 10_000) as i32 - 5_000))
        .collect();
    let abs = scalar_registry().get("abs").expect("abs is registered");
    let upper = scalar_registry().get("upper").expect("upper is registered");
    let mut group = c.benchmark_group("scalar_registry");

    group.bench_function("abs_1m", |b| {
        b.iter(|| {
            let mut total = 0i64;
            for value in &values {
                if let Ok(SqlValue::Integer(result)) = (abs.eval)(std::slice::from_ref(value)) {
                    total += i64::from(result);
                }
            }
            black_box(total)
        })
    });

    let text_values: Vec<_> = (0..ROWS)
        .map(|value| SqlValue::Text(format!("row-{value}")))
        .collect();
    group.bench_function("upper_1m", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for value in &text_values {
                if let Ok(SqlValue::Text(result)) = (upper.eval)(std::slice::from_ref(value)) {
                    total += result.len();
                }
            }
            black_box(total)
        })
    });
}

criterion_group!(benches, bench_scalar_registry);
criterion_main!(benches);
