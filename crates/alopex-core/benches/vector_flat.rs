use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, BatchSize};
use rand::Rng;
use alopex_core::{search_flat, Metric, ScoredItem};

fn bench_search_similar(c: &mut Criterion) {
    let dims = 128;
    let n = 10_000usize;
    let mut rng = rand::thread_rng();
    let mut data: Vec<(Vec<u8>, Vec<f32>)> = Vec::with_capacity(n);
    for i in 0..n {
        let mut v = Vec::with_capacity(dims);
        for _ in 0..dims {
            v.push(rng.gen::<f32>());
        }
        data.push((i.to_le_bytes().to_vec(), v));
    }
    let query: Vec<f32> = (0..dims).map(|_| rng.gen::<f32>()).collect();

    c.bench_with_input(BenchmarkId::new("search_flat_cosine", n), &n, |b, _| {
        b.iter_batched(
            || query.clone(),
            |q| run_search(&q, Metric::Cosine, &data),
            BatchSize::SmallInput,
        )
    });

    c.bench_with_input(BenchmarkId::new("search_flat_l2", n), &n, |b, _| {
        b.iter_batched(
            || query.clone(),
            |q| run_search(&q, Metric::L2, &data),
            BatchSize::SmallInput,
        )
    });
}

fn run_search(query: &[f32], metric: Metric, data: &[(Vec<u8>, Vec<f32>)]) -> Vec<ScoredItem> {
    let candidates = data
        .iter()
        .map(|(k, v)| alopex_core::vector::flat::Candidate {
            key: k,
            vector: v.as_slice(),
        });
    search_flat(query, metric, 10, candidates, None::<fn(&_) -> bool>).unwrap()
}

criterion_group!(benches, bench_search_similar);
criterion_main!(benches);
