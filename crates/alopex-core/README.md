# alopex-core

Core storage engine for [Alopex DB](https://github.com/alopex-db/alopex) - a unified database engine that scales from embedded to distributed.

## Features

- **LSM-Tree Storage**: High-throughput key-value storage with write-ahead logging
- **Columnar Storage**: Efficient analytical queries with column-oriented data layout
- **Vector Index**: Native vector similarity search with HNSW and flat index support
- **Multiple Compression**: Snappy (default), Zstd, LZ4 compression options
- **Memory Mapping**: Efficient file I/O with mmap support
- **WASM Support**: Optional WebAssembly target with IndexedDB backend

## Installation

```toml
[dependencies]
alopex-core = "0.3"
```

### Optional Features

```toml
[dependencies]
alopex-core = { version = "0.3", features = ["compression-zstd", "compression-lz4"] }
```

| Feature | Description |
|---------|-------------|
| `compression-zstd` | Zstandard compression support |
| `compression-lz4` | LZ4 compression support |
| `checksum-xxh64` | XXHash64 checksum |
| `wasm-indexeddb` | WebAssembly with IndexedDB backend |
| `memory-profiling` | Memory profiling tools |

## Quick Start

```rust
use alopex_core::lsm::LsmTree;
use alopex_core::config::LsmConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create LSM-Tree with default config
    let config = LsmConfig::default();
    let tree = LsmTree::open("./data", config)?;

    // Write data
    tree.put(b"key1", b"value1")?;
    tree.put(b"key2", b"value2")?;

    // Read data
    if let Some(value) = tree.get(b"key1")? {
        println!("Found: {:?}", value);
    }

    // Flush to disk
    tree.flush()?;

    Ok(())
}
```

## Architecture

```
alopex-core
├── lsm/          # LSM-Tree implementation
│   ├── memtable  # In-memory sorted table
│   ├── sstable   # Sorted string table (on-disk)
│   └── wal       # Write-ahead log
├── columnar/     # Columnar storage engine
├── vector/       # Vector index (HNSW, Flat)
└── kv/           # Key-value abstraction layer
```

## License

Apache-2.0
