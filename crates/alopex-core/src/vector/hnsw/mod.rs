//! Hierarchical Navigable Small World (HNSW) vector index module.

mod graph;
mod storage;
mod types;

pub use types::{HnswConfig, HnswSearchResult, HnswStats, InsertStats, SearchStats};

/// Public entrypoint for the HNSW index.
///
/// Full functionality is implemented in later phases of the specification.
#[derive(Debug, Default)]
pub struct HnswIndex {
    _private: (),
}
