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
