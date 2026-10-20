// Exact 0.8.15 public struct-literal client. Compiling this is the test.
fn legacy_config_literal() -> alopex_embedded::EmbeddedConfig {
    alopex_embedded::EmbeddedConfig {
        path: None,
        storage_mode: alopex_embedded::StorageMode::InMemory,
        memory_limit: None,
        segment_config: Default::default(),
    }
}
