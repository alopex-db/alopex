# Manual HNSW scale diagnostics

The following existing diagnostics are always ignored by ordinary test runs,
including `--features lane_ci` and `--all-features`:

| Case | Fixture |
| --- | --- |
| `shuffled_bulk_preserves_tie_aware_recall` | 9,600 rows × 128 dimensions |
| `duplicate_saturated_bulk_preserves_tie_aware_recall` | 16,000 rows × 128 dimensions |
| `cost_fixture_recall_survives_upper_layer_local_minima` | Up to 40,000 rows × 128 dimensions |

The first two also collect construction time and resource observations through
the existing worker. The configurable `bulk_order_quality_cost_worker` was
already permanently ignored and remains so. The bounded 997-row periodic
cosine correctness regression and smaller graph/format tests retain their
existing lane selection.

To run one scale diagnostic deliberately, select its full test name with
`--exact --ignored`. For example:

```sh
cargo test --locked -p alopex-core --lib \
  vector::hnsw::tests::storage_tests::shuffled_bulk_preserves_tie_aware_recall \
  -- --exact --ignored
```

This is a manual invocation pattern, not an additional CI command. Follow the
repository's per-host inventory, serialized build, bounded storage and cleanup
requirements before execution. Record the source identity, selected scale,
limits and raw result. Run each diagnostic separately; do not use a broad
`--include-ignored` invocation in normal CI.

The fixture data, quality assertions and diagnostic implementation are unchanged.
These cases remain available for dedicated acceptance. Excluding them from
normal CI does not establish HNSW performance acceptance or complete #681/#690.
