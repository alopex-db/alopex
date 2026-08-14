# v0.8.5 embedded-local demo coverage

`demo_embedded_v085.sh` is the single Rust Embedded API walkthrough used by
post-release verification. It builds against local path dependencies during
development and against exact-version crates.io packages in the release
verifier.

| Scenario | Observable capability | Requirements/design source |
| --- | --- | --- |
| `EMB-01-storage-durability` | memory options/limits, snapshot, clone, clear, persist, file URI reopen and flush | inherited local compatibility; Phase 1 R5 |
| `EMB-02-kv-transactions` | KV CRUD, prefix scan, commit, rollback | Phase 2 R1 local Transaction compatibility |
| `EMB-03-persisted-transaction-manager` | named transaction metadata, staged reads, commit and rollback | Phase 2 R1 local multi-operation workflow |
| `EMB-04-local-sql-matrix` | DDL/DML, SELECT clauses, JOIN, subquery, aggregates, scalar/hash/encoding, TIMESTAMP, Vector SQL and PRAGMA | Phase 2 R1 complete local SQL baseline |
| `EMB-05-catalog-cluster-diagnostics` | catalog/namespace/table/index observation, cache invalidation, single-node cluster status and routing diagnostics | Phase 1 R1/R2 local diagnostic boundary |
| `EMB-06-owned-and-sql-streams` | callback/iterator/owned SQL streams, owned transaction commit/rollback and preflight rejection | Phase 4 R1/R2 Rust backend contract |
| `EMB-07-dataframe-columnar` | SQL-to-DataFrame with FLOAT-to-Arrow-Float32 preservation, columnar projection/scan/stats/index, and fail-closed legacy-to-V08 streaming boundary | Phase 3 R1/R2; v0.8.5 #93 |
| `EMB-08-vector-hnsw` | flat Vector API, metric semantics, HNSW lifecycle and lower-is-closer public distance | inherited Vector SQL/API and v0.8.5 fix |
| `EMB-09-large-values` | chunked blob and typed-payload round trips | inherited embedded storage surface |
| `EMB-10-fail-closed-boundaries` | read-only write rejection, dimension mismatch, fenced-read unavailability, S3 feature boundary and unsupported stream preflight | Phase 1/2 prerequisite and structured rejection contracts |

The script covers every roadmap-observable capability that is callable through
the default Rust `alopex-embedded` package. The following are intentionally not
fabricated as Embedded API successes:

- authoritative multi-node metadata management and distributed reads require a
  compatible external cluster foundation; the demo instead proves single-node
  diagnostics and fenced-read rejection without local fallback;
- bounded CSV/Parquet `LazyFrame` execution belongs to `alopex-dataframe`, and
  Python sync/async/DataFrame bindings belong to `alopex`; their existing
  release demos remain separate;
- a V08 columnar stream consumes an externally provisioned chunked segment.
  Phase 3 explicitly excludes a public V08 segment writer, so the Embedded demo
  proves the required `requires_v08_chunked_layout` rejection for legacy V2;
- S3 is outside the default v0.8 local package profile and requires the `s3`
  Cargo feature and external credentials.
