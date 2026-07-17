//! Throughput baseline for registry-dispatched scalar evaluation.

use std::time::Duration;

use alopex_sql::executor::evaluator::registry::scalar_registry;
use alopex_sql::storage::SqlValue;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use rusqlite::Connection;

const ROWS: usize = 1_000_000;

fn bench_scalar_registry(c: &mut Criterion) {
    let values: Vec<_> = (0..ROWS)
        .map(|value| SqlValue::Integer((value % 10_000) as i32 - 5_000))
        .collect();
    let mut group = c.benchmark_group("scalar_registry");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(ROWS as u64));

    let abs = scalar_registry().get("abs").expect("abs is registered");
    group.bench_function("alopex_abs_1m", |b| {
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

    let sqlite = Connection::open_in_memory().expect("open SQLite benchmark connection");
    let mut sqlite_abs = sqlite
        .prepare("SELECT abs(?1)")
        .expect("prepare SQLite abs");
    group.bench_function("sqlite_abs_1m", |b| {
        b.iter(|| {
            let mut total = 0i64;
            for value in &values {
                let input = match value {
                    SqlValue::Integer(value) => i64::from(*value),
                    _ => unreachable!("benchmark input is integer"),
                };
                let result: i64 = sqlite_abs
                    .query_row([input], |row| row.get(0))
                    .expect("SQLite abs result");
                total += result;
            }
            black_box(total)
        })
    });

    let text_values: Vec<_> = (0..ROWS)
        .map(|value| SqlValue::Text(format!("row-{value}")))
        .collect();
    let upper = scalar_registry().get("upper").expect("upper is registered");
    group.bench_function("alopex_upper_1m", |b| {
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

    let mut sqlite_upper = sqlite
        .prepare("SELECT upper(?1)")
        .expect("prepare SQLite upper");
    group.bench_function("sqlite_upper_1m", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for value in &text_values {
                let input = match value {
                    SqlValue::Text(value) => value,
                    _ => unreachable!("benchmark input is text"),
                };
                let result: String = sqlite_upper
                    .query_row([input], |row| row.get(0))
                    .expect("SQLite upper result");
                total += result.len();
            }
            black_box(total)
        })
    });

    drop(sqlite_upper);
    drop(sqlite_abs);
    drop(sqlite);

    let hash_values: Vec<_> = (0..ROWS)
        .map(|value| SqlValue::Text(format!("row-{value}")))
        .collect();
    let md5 = scalar_registry().get("md5").expect("md5 is registered");
    group.bench_function("alopex_v074_md5_1m", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for value in &hash_values {
                if let Ok(SqlValue::Text(result)) = (md5.eval)(std::slice::from_ref(value)) {
                    total += result.len();
                }
            }
            black_box(total)
        })
    });

    let sha256 = scalar_registry()
        .get("sha256")
        .expect("sha256 is registered");
    group.bench_function("alopex_v074_sha256_1m", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for value in &hash_values {
                if let Ok(SqlValue::Blob(result)) = (sha256.eval)(std::slice::from_ref(value)) {
                    total += result.len();
                }
            }
            black_box(total)
        })
    });

    let encoded_values: Vec<_> = (0..ROWS)
        .map(|value| SqlValue::Blob(format!("row-{value}").into_bytes()))
        .collect();
    let encode = scalar_registry()
        .get("encode")
        .expect("encode is registered");
    let base64 = SqlValue::Text("base64".to_string());
    group.bench_function("alopex_v074_encode_base64_1m", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for value in &encoded_values {
                if let Ok(SqlValue::Text(result)) = (encode.eval)(&[value.clone(), base64.clone()])
                {
                    total += result.len();
                }
            }
            black_box(total)
        })
    });
}

criterion_group!(benches, bench_scalar_registry);
criterion_main!(benches);
