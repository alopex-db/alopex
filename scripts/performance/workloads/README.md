# Pinned diagnostic workloads

`dataframe_phases_v0816.py` is copied byte-for-byte from `alopex-db/docs` commit
`017e52996ad4130c6a52064153c4f8d93d08e123`,
`scripts/performance/dataframe_phases_v0816.py`.
SHA256: `d0303975720a43755dae5628338b0a0d6bb099d81d0b43d1b8fc8688259af0e2`.

It preserves the #712 four phase boundaries, twelve expressions, complete
materialization contract, and full typed/NULL/ordered Python oracle. The original
version-gated runner remains unchanged. `dataframe_profile.py` adapts the API
provider for explicitly hashed current-source builds; its results are internal
diagnostics, not published v0.8.16 or reference-product comparisons.

`polars_entrypoints_v0816.py` is a byte-identical copy from the same docs commit,
at `scripts/performance/polars_entrypoints_v0816.py` (SHA256
`c14806d4dd79579e4519f0465e1e9a384bbd38dae60dcbabfc15b141d75e3d15`).
`lazy_file_profile.py` preserves its scan/plan/load/full-materialization timing
boundary and typed/NULL/ordered oracle while adapting the current native API.
CSV uses twelve expressions; numeric Parquet uses eleven. The known Parquet
string failures remain excluded. Fixture generation uses the original writer
with Polars 1.43.2 outside measurement; the same immutable files serve both builds.

`sql_streaming_v0816.py` preserves the complete `_sql_streaming` function extracted
from Alopex commit `d9ff8783b8d18a63d167204e55902ea6cd90dcd1` after applying the
docs run-20261021 fairness patch (SHA256
`9b2dde147fce3a1ce820c65fb91627595d75832820cce34526f7abb575abd68b`).
The extracted function SHA256 is
`30f7355ea08824ddcf726047fba7024b738c1866e7e8d0e29142464918eda56c`.
It is a source reference, not a standalone runner. Tests compare the original
consumer bodies against `sql_stream_profile.py` using their ASTs. The original
lane counts fully consumed Python rows / Arrow batches with full-value checks
before and after the case. A separate lane materializes Python rows on both
sides and validates every timed result; it does not replace the original metric.

`csv_streaming_v0816.py` preserves `_streaming` and `_write_csv` from the same
corrected run-20261021 source as the SQL streaming reference. The native consumer
bodies are checked by AST comparison. `csv_stream_profile.py` keeps plan/collect
observations separate from complete 8,192-row-chunk consumption. Extra full
collect/stream typed-value, NULL and order oracles run outside the original timers.
The reconstructed 34,250-row CSV matches the original manifest SHA256
`eec24557dcbdf71821b2e7afad1a7083dfb3256b71b57adf2e7ec387e1fd20ff`.

`sqlite_composite_v0816.py` freezes nine SQL helper function bodies from the
same corrected original source (whole-file SHA256
`7ba8a8e9da92987d2f0dbdc6ef3d98f2a12e5c4dc087651ae279a1dd6fbf1800`).
Development extraction verifies each function's AST against that source. The
`*-original` diagnostic lane retains the original memory DB, SQL operation
sequence, discarded query returns and unspecified SQLite foreign-key setting.
It must not be presented as proof of equal constraint enforcement.

`sqlite_composite_profile.py` adds a separate `*-checked` lane: the same five
operation groups, an explicit alias on the ordered SELECT, retained SELECT
results checked after timing, and SQLite foreign keys enabled and verified.
Negative FK/CHECK and intermediate savepoint state are checked outside timing.
Every timed sample is followed by full typed fixture/rollback validation. The
original and checked timing boundaries differ and their results stay separate.
Five phase clocks run only in separate observations, outside the primary timing
samples. These manual drivers and their supervisor reject ordinary CI.
