//! Build-only HNSW scale measurement.
//!
//! Input format: little-endian `u64` count, little-endian `u64` dimension,
//! followed by `count * dimension` little-endian `f32` values.

use alopex_core::vector::hnsw::{HnswConfig, HnswIndex};
use alopex_core::vector::Metric;
use std::env;
use std::fs;
use std::time::Instant;

fn read_u64(bytes: &[u8], offset: &mut usize) -> u64 {
    let end = *offset + 8;
    let value = u64::from_le_bytes(bytes[*offset..end].try_into().unwrap());
    *offset = end;
    value
}

fn main() {
    let mut args = env::args().skip(1);
    let path = args
        .next()
        .expect("usage: hnsw_build_scale <vectors.bin> <n>");
    let requested_n: usize = args
        .next()
        .expect("usage: hnsw_build_scale <vectors.bin> <n>")
        .parse()
        .expect("N must be an integer");
    let bytes = fs::read(path).expect("read vectors.bin");
    let mut offset = 0;
    let count = read_u64(&bytes, &mut offset) as usize;
    let dimension = read_u64(&bytes, &mut offset) as usize;
    let n = requested_n.min(count);
    let expected = offset + count * dimension * std::mem::size_of::<f32>();
    assert_eq!(bytes.len(), expected, "invalid vectors.bin length");

    let config = HnswConfig::default()
        .with_dimension(dimension)
        .with_metric(Metric::Cosine)
        .with_m(16)
        .with_ef_construction(200);
    let mut index = HnswIndex::create("hnsw-scale", config).expect("create HNSW index");
    let started = Instant::now();
    for id in 0..n {
        let end = offset + dimension * std::mem::size_of::<f32>();
        let vector: Vec<f32> = bytes[offset..end]
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        index
            .upsert(&(id as u64).to_be_bytes(), &vector, &[])
            .expect("insert vector");
        offset = end;
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let stats = index.stats();
    println!(
        "{{\"n\":{n},\"dimension\":{dimension},\"build_ms\":{elapsed_ms:.3},\"node_count\":{},\"memory_bytes\":{},\"avg_edges_per_node\":{:.6}}}",
        stats.node_count, stats.memory_bytes, stats.avg_edges_per_node
    );
}
