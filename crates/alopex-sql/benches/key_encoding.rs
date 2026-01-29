use std::cmp::Ordering;

use alopex_sql::storage::{KeyEncoder, SqlValue};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn assert_monotonic<K: AsRef<[u8]>, T: std::fmt::Debug>(
    mut values: Vec<T>,
    mut encode: impl FnMut(&T, u64) -> K,
    cmp: impl Fn(&T, &T) -> Ordering,
    label: &str,
) {
    values.sort_by(|a, b| cmp(a, b));
    let mut prev: Option<(K, &T)> = None;
    for (i, v) in values.iter().enumerate() {
        let key = encode(v, i as u64);
        if let Some((prev_key, prev_v)) = prev {
            let ordering = cmp(prev_v, v);
            let key_ord = prev_key.as_ref().cmp(key.as_ref());
            assert!(
                ordering == key_ord || (ordering == Ordering::Equal && key_ord == Ordering::Less),
                "{label}: ordering mismatch prev={prev_v:?} curr={v:?}"
            );
        }
        prev = Some((key, v));
    }
}

fn bench_integers(c: &mut Criterion) {
    let ints: Vec<i32> = (-128..=127).collect();
    c.bench_function("key_encoding/integer_order", |b| {
        b.iter(|| {
            assert_monotonic(
                ints.clone(),
                |v, row_id| KeyEncoder::index_key(1, &SqlValue::Integer(*v), row_id).unwrap(),
                |a, b| a.cmp(b),
                "integer",
            );
        })
    });
}

fn bench_bigints(c: &mut Criterion) {
    let values = vec![i64::MIN, -1, 0, 1, 123, i64::MAX];
    c.bench_function("key_encoding/bigint_order", |b| {
        b.iter(|| {
            assert_monotonic(
                values.clone(),
                |v, row_id| KeyEncoder::index_key(1, &SqlValue::BigInt(*v), row_id).unwrap(),
                |a, b| a.cmp(b),
                "bigint",
            );
        })
    });
}

fn bench_floats(c: &mut Criterion) {
    let values = vec![
        -f32::INFINITY,
        -1000.0,
        -1.5,
        -0.0,
        0.0,
        1.5,
        1000.0,
        f32::INFINITY,
        f32::NAN,
    ];
    c.bench_function("key_encoding/float_order", |b| {
        b.iter(|| {
            assert_monotonic(
                values.clone(),
                |v, row_id| KeyEncoder::index_key(1, &SqlValue::Float(*v), row_id).unwrap(),
                |a, b| a.total_cmp(b),
                "float",
            );
        })
    });
}

fn bench_doubles(c: &mut Criterion) {
    let values = vec![
        -f64::INFINITY,
        -1000.0,
        -1.5,
        -0.0,
        0.0,
        1.5,
        1000.0,
        f64::INFINITY,
        f64::NAN,
    ];
    c.bench_function("key_encoding/double_order", |b| {
        b.iter(|| {
            assert_monotonic(
                values.clone(),
                |v, row_id| KeyEncoder::index_key(1, &SqlValue::Double(*v), row_id).unwrap(),
                |a, b| a.total_cmp(b),
                "double",
            );
        })
    });
}

fn bench_texts(c: &mut Criterion) {
    let values = vec!["", "a", "aa", "b", "é", "あ", "あい", "🍣"];
    c.bench_function("key_encoding/text_order", |b| {
        b.iter(|| {
            assert_monotonic(
                values.clone(),
                |v, row_id| {
                    KeyEncoder::index_key(1, &SqlValue::Text(v.to_string()), row_id).unwrap()
                },
                |a, b| a.cmp(b),
                "text",
            );
        })
    });
}

fn bench_blobs(c: &mut Criterion) {
    let values = vec![vec![], vec![0], vec![0, 1], vec![1], vec![1, 0], vec![0xFF]];
    c.bench_function("key_encoding/blob_order", |b| {
        b.iter(|| {
            assert_monotonic(
                values.clone(),
                |v, row_id| KeyEncoder::index_key(1, &SqlValue::Blob(v.clone()), row_id).unwrap(),
                |a, b| match a.len().cmp(&b.len()) {
                    Ordering::Equal => a.cmp(b),
                    other => other,
                },
                "blob",
            );
        })
    });
}

fn bench_booleans(c: &mut Criterion) {
    let values = vec![false, true];
    c.bench_function("key_encoding/boolean_order", |b| {
        b.iter(|| {
            assert_monotonic(
                values.clone(),
                |v, row_id| KeyEncoder::index_key(1, &SqlValue::Boolean(*v), row_id).unwrap(),
                |a, b| a.cmp(b),
                "boolean",
            );
        })
    });
}

fn bench_timestamps(c: &mut Criterion) {
    let values = vec![-5, -1, 0, 1, 10, i64::MAX];
    c.bench_function("key_encoding/timestamp_order", |b| {
        b.iter(|| {
            assert_monotonic(
                values.clone(),
                |v, row_id| KeyEncoder::index_key(1, &SqlValue::Timestamp(*v), row_id).unwrap(),
                |a, b| a.cmp(b),
                "timestamp",
            );
        })
    });
}

fn bench_composite(c: &mut Criterion) {
    let mut values = [(0, 0), (0, 1), (1, 0), (1, 2), (2, 0), (2, 2)];
    c.bench_function("key_encoding/composite_order", |b| {
        b.iter(|| {
            values.sort();
            let mut prev: Option<(Vec<u8>, (i32, i32))> = None;
            for (i, (a, b)) in values.iter().enumerate() {
                let key = KeyEncoder::composite_index_key(
                    9,
                    &[SqlValue::Integer(*a), SqlValue::Integer(*b)],
                    i as u64,
                )
                .unwrap();
                if let Some((prev_key, prev_v)) = prev {
                    let ord = prev_v.cmp(&(*a, *b));
                    let key_ord = prev_key.cmp(&key);
                    assert!(
                        ord == key_ord || (ord == Ordering::Equal && key_ord == Ordering::Less),
                        "composite ordering mismatch prev={prev_v:?} curr=({a},{b})"
                    );
                }
                prev = Some((key, (*a, *b)));
            }
        })
    });
}

pub fn criterion_benchmark(c: &mut Criterion) {
    bench_integers(c);
    bench_bigints(c);
    bench_floats(c);
    bench_doubles(c);
    bench_texts(c);
    bench_blobs(c);
    bench_booleans(c);
    bench_timestamps(c);
    bench_composite(c);
    // small encode loop to keep Criterion measuring encode latency
    c.bench_function("key_encoding/encode_hot_path", |b| {
        b.iter(|| {
            let key = KeyEncoder::index_key(1, &SqlValue::Integer(black_box(123)), 42).unwrap();
            black_box(key);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
