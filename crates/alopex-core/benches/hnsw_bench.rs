use alopex_core::vector::hnsw::{HnswConfig, HnswIndex};
use alopex_core::vector::simd::select_kernel;
use alopex_core::vector::Metric;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn config(dim: usize) -> HnswConfig {
    HnswConfig::default()
        .with_dimension(dim)
        .with_metric(Metric::L2)
        .with_m(16)
        .with_ef_construction(200)
}

fn generate_dataset(count: usize, dim: usize) -> Vec<(Vec<u8>, Vec<f32>)> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..count)
        .map(|i| {
            let key = i.to_be_bytes().to_vec();
            let vec = (0..dim).map(|_| rng.gen::<f32>()).collect::<Vec<f32>>();
            (key, vec)
        })
        .collect()
}

fn build_index(name: &str, data: &[(Vec<u8>, Vec<f32>)], dim: usize) -> HnswIndex {
    let mut index = HnswIndex::create(name, config(dim)).expect("設定は固定なので失敗しない");
    for (k, v) in data {
        index.upsert(k, v, &[]).unwrap();
    }
    index
}

fn bench_insert_throughput(c: &mut Criterion) {
    let dim = 128;
    for &n in &[1_000usize, 10_000, 100_000] {
        let data = generate_dataset(n, dim);
        let name = format!("hnsw_insert_{}_vectors", n);
        c.bench_function(&name, |b| {
            b.iter_batched(
                || {
                    (
                        HnswIndex::create("bench", config(dim)).unwrap(),
                        data.clone(),
                    )
                },
                |(mut index, dataset)| {
                    for (k, v) in dataset {
                        index.upsert(&k, &v, &[]).unwrap();
                    }
                    black_box(index)
                },
                BatchSize::SmallInput,
            )
        });
    }
}

fn bench_search_latency(c: &mut Criterion) {
    let dim = 128;
    let data = generate_dataset(20_000, dim);
    let queries: Vec<Vec<f32>> = data.iter().take(50).map(|(_, v)| v.clone()).collect();
    let index = build_index("search", &data, dim);

    c.bench_function("hnsw_search_top10_20k", |b| {
        b.iter(|| {
            for q in &queries {
                let _ = black_box(index.search(q, 10, Some(64)).unwrap());
            }
        })
    });
}

fn bench_recall_at_10(c: &mut Criterion) {
    let dim = 64;
    let data = generate_dataset(5_000, dim);
    let queries: Vec<Vec<f32>> = data
        .iter()
        .skip(100)
        .take(20)
        .map(|(_, v)| v.clone())
        .collect();
    let index = build_index("recall", &data, dim);
    let exact = exact_top_k(&data, &queries, 10);

    c.bench_function("hnsw_recall@10_5k", |b| {
        b.iter(|| {
            let recall = recall_at_k(&index, &queries, &exact, 10);
            black_box(recall)
        })
    });
}

fn bench_compaction(c: &mut Criterion) {
    let dim = 64;
    let data = generate_dataset(10_000, dim);

    c.bench_function("hnsw_compact_after_delete_10k", |b| {
        b.iter_batched(
            || {
                let mut index = build_index("compact", &data, dim);
                for (i, (k, _)) in data.iter().enumerate() {
                    if i % 3 == 0 {
                        let _ = index.delete(k);
                    }
                }
                index
            },
            |mut index| {
                let _ = black_box(index.compact().unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

fn recall_at_k(index: &HnswIndex, queries: &[Vec<f32>], exact: &[Vec<Vec<u8>>], k: usize) -> f64 {
    let mut hit = 0usize;
    for (q, truth) in queries.iter().zip(exact.iter()) {
        let (results, _) = index.search(q, k, Some(64)).unwrap();
        let got: Vec<Vec<u8>> = results.into_iter().map(|r| r.key).collect();
        for target in truth {
            if got.contains(target) {
                hit += 1;
            }
        }
    }
    hit as f64 / (queries.len() * k) as f64
}

fn exact_top_k(data: &[(Vec<u8>, Vec<f32>)], queries: &[Vec<f32>], k: usize) -> Vec<Vec<Vec<u8>>> {
    let kernel = select_kernel();
    queries
        .iter()
        .map(|q| {
            let mut scored: Vec<(f32, Vec<u8>)> = data
                .iter()
                .map(|(k, v)| (kernel.l2(q, v), k.clone()))
                .collect();
            scored.sort_by(|a, b| b.0.total_cmp(&a.0));
            scored
                .into_iter()
                .take(k)
                .map(|(_, k)| k)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn criterion_benchmark() -> Criterion {
    Criterion::default().sample_size(20)
}

criterion_group!(
    name = benches;
    config = criterion_benchmark();
    targets = bench_insert_throughput, bench_search_latency, bench_recall_at_10, bench_compaction
);
criterion_main!(benches);
