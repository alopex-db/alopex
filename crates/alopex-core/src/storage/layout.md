# SST Layout (v0.1 stub)

MemoryKV::flush currently emits two artifacts:
- `<wal>.sst`: sorted KV SSTable (header/body/footer with checksum)
- `<wal>.vec`: placeholder vector segment (see `storage::flush::write_empty_vector_segment`)

SSTable file layout:
- Header (14 bytes): magic `ALXS` (4), version `u16 = 1` (2), reserved `u16 = 0` (2), entry_count `u64` (8)
- Body: repeated records `(key_len u32, value_len u32, key bytes, value bytes)` in **sorted key order**
- Footer (16 bytes): magic `SSTF` (4), entry_count `u64` (8), crc32 `u32` over the body region

Indexing/reopen behavior:
- `SstableReader::open` validates header/footer, recomputes the body checksum, and builds an in-memory index of `(key, offset, lens)`.
- `MemoryTxnManager::load_sstable` loads the index and hydrates MemTable with version bumps, so reads remain memory-first while still serving SST hits through `sstable_get`.
- WAL replay runs after SST load to overlay newer committed writes.

Notes for future page layout:
- Single-file, sequential write; no compression/bloom/compaction in v0.1.
- Page sizing/alignment is intentionally omitted; future versions can wrap this SST inside the unified `.alopex` container (`storage::format`) with section metadata.
