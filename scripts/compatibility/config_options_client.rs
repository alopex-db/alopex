use alopex_embedded::{Database, DatabaseOptions};

fn main() {
    let options = DatabaseOptions::new()
        .with_segment_config(Default::default())
        .with_hnsw_cache_limits(17, Some(2));
    let _ = options.clone();
    let _: fn(&std::path::Path, DatabaseOptions) -> alopex_embedded::Result<Database> =
        Database::open_with_options;
}
