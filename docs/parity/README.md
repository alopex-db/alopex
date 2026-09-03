# v0.8.11 reference compatibility

These ledgers separate externally verified behavior, Alopex extensions, known
divergences, and APIs that have not been implemented. A feature being present is
not by itself a claim of drop-in compatibility.

| Surface | Canonical reference | Immutable revision | Ledger |
|---|---|---|---|
| DataFrame | Polars 1.43.2 | `ae588a9f2c91171f45bace43a99fb7b80b90847b` | `polars-v1.43.2.json` |
| HNSW parameters/search | hnswlib 0.8.0 | `3f3429661187e4c24a490a0f148fc6bc89042b3d` | `hnsw-v0.8.11.json` |
| HNSW database semantics | pgvector 0.8.0 | `2627c5ff775ae6d7aef0c430121ccf857842d2f2` | `hnsw-v0.8.11.json` |
| SQL base | SQLite 3.46.1 | `f3d536d37825302e31ed0eddd811c689f38f85a3` | `sql-v0.8.11.json` |
| PostgreSQL-derived SQL | PostgreSQL 16.14 | `0d1c00c624fa7367d4a895f44381887757289682` | `sql-v0.8.11.json` |
| Analytical SQL | DuckDB 1.4.0 / DataFusion 50.0.0 | `b8a06e4a22672e254cd0baa68a3dbed2eb51c56e` / `d0a0c5a7d5867da949161b6065642d15293806de` | `sql-v0.8.11.json` |

Polars 1.43.2 is the v0.8.11 canonical version because the release was current
when the ledger was introduced and provides `LazyFrame.collect_batches`. To
update it, change the exact version and commit in the ledger, the pinned install
in `alopex-py.yml`, the live differential assertion, and the performance
contract in one pull request. Then run:

```bash
python scripts/validate_v0811_ledgers.py
python -m unittest scripts.test_validate_v0811_ledgers
pytest crates/alopex-py/tests/test_polars_1432_reference.py
```

The HNSW ledger claims algorithm and observable-contract conformance only where
the row links a pinned oracle. Alopex-specific statistics, metadata, and
compaction remain extensions. The SQL ledger similarly records SQLite as the
base dialect and selects PostgreSQL, DuckDB, or DataFusion per feature; it does
not claim wholesale compatibility with any one database.
