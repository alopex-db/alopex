# Borrowed SSTable point lookup (#712)

## Contract and identity

The completed #780 profiles showed repeated full-block entry allocations under SSTableReader::get_cached, both for individual persisted gets and for the visible-value lookup inside owned range scans. The point lookup now traverses borrowed keys/values in the decompressed block and copies only its selected entry. Parsing still visits the complete block, including entries after a successful match, and rejects invalid entry types, truncated fields/data and entry-count mismatch. Snapshot timestamp, sequence ordering, tombstones and block-spanning versions keep the existing selection semantics. Full scans still collect all entries. No new cache, disk format or public API is introduced.

- Runtime implementation: `751b734701e6fe0a4092c53a11025c5f66bf47ba`; tests-before source `e5f8a89b`.
- Base: #780 `3ee2150d347d79a7151219035607c0514404000c`. Retained baseline native source `36c445466b705f15223d534b2ca4a252e09baafc`.
- One after Python extension build in the shared target. Exact native/source/package/parser/harness hashes are in `sstable-point-lookup.json`; every non-native Python package file and harness hash matches the baseline.
- This is development verification on a stacked branch. Candidate composition/CI, published0.8.16/reference comparison and full #712/release acceptance remain pending.

## Focused development verification

- Before and after: `cargo test --offline --locked -p alopex-core --features compression-lz4,compression-zstd --lib lsm::sstable::tests`: 12 passed on each side.
- Five added tests: None/LZ4/Zstd version/cache matrices, six malformed-tail variants after a match (and invisible match), and a 24-case reference-model property with at most six versions. Fixtures use a few keys/bytes and 64/256-byte blocks, including cross-block versions, same-time sequence ties, tombstones, empty values and misses.
- `... --lib owned_lsm_`: 4 passed; `... --lib lsm::scan_tests`: 7 passed. After total: 23 unique tests, including restart, rollback, savepoints, hidden tombstone work budgets, snapshot overlays and early-drop read sets.
- Four prelisted mutants over key/value bounds, exact remaining length, count validation and snapshot time were all caught; no survivor/unviable/timeout. cargo-mutants 27.1.0 includes unrelated test struct-field mutants despite regex filtering; a prelisted slice shard selected only the intended four. No whole-file/workspace mutation run.
- Scoped core Clippy (`--features compression-lz4,compression-zstd --lib --tests -- -D warnings`), fmt and diff checks passed. Its initial failure identified an inherited test-only `mem::replace(..., MemTable::new())` warning; the equivalent `mem::take` fix is included and that scan test passed.
- Initial test attempts failed before execution: the existing vendor manifest lacked static_library; explicit SHA-verified published parser selection resolved it. A newly added test initially named unsupported SSTable Snappy; it was corrected to the actual None/LZ4/Zstd modes. These are separate invalid attempts, not failed product assertions.
- Coupling was not run: production changes are private helpers in one file, with no new module/API edges; the second file has the equivalent test-only lint fix. Miri/Kani were not run because no unsafe path was introduced; malformed lengths and a reference model exercise the parser boundary. No broad workspace tests, load tests or performance loops were added to ordinary CI.

## Manual comparison

Same persisted SST workload and immutable harness as #780: seed 7, 10-byte keys, 32-byte values; 3/10,000 keys; three trials × eight cells with three warmups and 21 measurements each (504 measurements, 72 warmups). Every case verifies bytes/type/order and all state after close/reopen. Point samples comprise 100 separate read transactions; range samples fully consume min(rows, 1,000) pairs. Values below are medians of three trial medians in milliseconds, not per-request latency.

The before measurements are retained evidence from the preceding source, not a rebuilt freshness run or temporally interleaved AB/BA experiment. Before measuring, the diagnostic band was fixed at max(5% of baseline median, 3 × pooled baseline MAD). Every increase is retained; being inside the band does not prove noise.

| Keys / operation | Before ms | After ms | Change | Diagnostic band ms |
|---|---:|---:|---:|---:|
| 10000-begin-rollback | 0.042791 | 0.043708 | +2.14% | 0.004998 |
| 10000-random-get-hit | 0.327167 | 0.135333 | -58.63% | 0.021873 |
| 10000-random-get-miss | 0.065333 | 0.065334 | +0.00% | 0.005625 |
| 10000-range-scan | 2.565416 | 0.932833 | -63.64% | 0.128271 |
| 3-begin-rollback | 0.045209 | 0.044959 | -0.55% | 0.002260 |
| 3-random-get-hit | 0.116334 | 0.097834 | -15.90% | 0.008751 |
| 3-random-get-miss | 0.065250 | 0.064417 | -1.28% | 0.003627 |
| 3-range-scan | 0.018083 | 0.018791 | +3.92% | 0.002124 |

Separate four-second hit/range profiles completed; cProfile covers public operations, while native samples also include validation/setup. They are diagnostic evidence rather than an exclusive-time attribution or additional timed trials.

| Profile | Iterations | Peak sampled RSS bytes |
|---|---:|---:|
| random-get-hit | 21,649 | 79,839,232 |
| range-scan | 3,559 | 84,738,048 |

## Cleanup and continuation

- Host peak 18,926,907,392 bytes; after cleanup 18,828,042,240 bytes, below the 40 GB stop and 50 GB ceiling.
- All 26 timing/profile processes ended and all 26 fixture directories were removed. The downloaded parser archive, extracted parser, native Python package/target exports and owned core test executable were removed after hashes/results were recorded. Shared Cargo dependencies and compact evidence remain.
- Next: recheck candidate and dependency CI for a safe dependency-order integration. Full #712 workload acceptance and composed-candidate/reference measurements remain required, followed by the remaining v0.8.17 qualification/release/main integration stages.
