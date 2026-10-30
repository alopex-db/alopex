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
