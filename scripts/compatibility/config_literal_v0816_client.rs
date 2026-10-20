// Unchanged six-field client shared by exact 0.8.16 and the candidate graph.
use alopex_embedded::{EmbeddedConfig, StorageMode};

fn main() {
    let _ = EmbeddedConfig {
        path: None,
        storage_mode: StorageMode::InMemory,
        memory_limit: None,
        segment_config: Default::default(),
        hnsw_cache_bytes: Some(17),
        hnsw_cache_entries: Some(2),
    };
}
