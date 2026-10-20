// Unchanged client shared by the exact 0.8.15, 0.8.16 and candidate graphs.
use alopex_embedded::{Database, DatabaseOptions, EmbeddedConfig};

fn main() {
    let _ = EmbeddedConfig::in_memory();
    let _ = EmbeddedConfig::in_memory_with_limit(1024);
    let _ = EmbeddedConfig::disk("example.db".into()).with_segment_config(Default::default());
    let options = DatabaseOptions::in_memory().with_memory_limit(1024);
    let _ = (options.memory_mode(), options.memory_limit());
    let _: fn(EmbeddedConfig) -> alopex_embedded::Result<Database> = Database::open_with_config;
    let _: fn(DatabaseOptions) -> alopex_embedded::Result<Database> =
        Database::open_in_memory_with_options;
}
