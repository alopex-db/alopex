use alopex_core::vector::simd::select_kernel;
use alopex_core::vector::Metric;
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_flat_search(c: &mut Criterion) {
    let dim = 128;
    let n = 10_000; // small default to keep CI fast; adjust locally for full runs.
    let query: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.01).collect();
    let vectors: Vec<f32> = (0..(dim * n)).map(|i| (i % 7) as f32 * 0.1).collect();
    let mut scores = vec![0.0f32; n];
    let kernel = select_kernel();

    c.bench_function("flat_search_inner_product", |b| {
        b.iter(|| kernel.batch_score(Metric::InnerProduct, &query, &vectors, dim, &mut scores))
    });
}

criterion_group!(benches, bench_flat_search);
criterion_main!(benches);
