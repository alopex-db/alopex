# Changelog

All notable changes to this project will be documented in this file.

## [0.7.1]

Bugfix release. All fixes were discovered by the new mode-parity verification
suite, which is included in this release.

### Fixed
- `alopex-cli`'s `reqwest` dependency is now unified on 0.13 (it was pinned
  at 0.12, incompatible with `object_store` 0.14's `reqwest ^0.13`
  requirement). The two independent, semver-incompatible dependency trees
  this produced duplicated the entire hyper/rustls/tokio stack and could
  exhaust disk space during release builds.
- Subqueries now execute on the CLI/streaming query path. Previously,
  WHERE-clause subqueries failed with `ALOPEX-E999` (unsupported expression)
  and scalar subqueries silently returned empty results on the streaming
  path, while the embedded API and HTTP returned correct results (#23, #24).
- `NOT IN (subquery)` now follows SQL three-valued logic: when the subquery
  result contains NULL (or the probe value is NULL), the predicate evaluates
  to UNKNOWN instead of raising a type error, on both streaming and
  non-streaming paths.
- gRPC `ExecuteSql` now delegates to the same non-streaming SQL execution
  path as HTTP, so both server surfaces return identical results and errors
  for the same SQL, including DML/DDL execution and routing gates (#25).
  Response buffering was trimmed with incremental response-size checks.
- CLI `sql` now emits one result block per statement for multi-statement
  input instead of silently dropping all but the last result (#26). All
  statements run in one auto-commit transaction; a mid-batch failure rolls
  back the whole batch and exits non-zero.
- CLI streaming JSON output no longer leaves invalid partial JSON on stdout
  when a local or remote stream fails mid-result.
- Multi-row `INSERT ... VALUES (...), (...)` without an explicit column list
  no longer fails with `ALOPEX-P001` (Nim parser AST ambiguity between the
  column list and the first value row) and no longer desyncs the Nim FFI
  boundary for subsequent statements on the same thread (#40).
- Windows: the Nim SQL parser DLL no longer fails to load for `alopex-py`
  (`ImportError: DLL load failed`) — the MinGW runtime is now statically
  linked into the DLL, and `ALOPEX_DLL_DIR` resolves the dependency
  explicitly instead of relying on PATH search (Python 3.8+ does not use
  PATH for extension-module dependency resolution).

### Security
- Updated dependencies to resolve 15 of 17 RustSec advisories found in the
  v0.7.0 dependency tree: `pyo3` 0.24.2 → 0.29.0 (RUSTSEC-2026-0176,
  RUSTSEC-2026-0177), `rustls` 0.21 → 0.23 chain including `axum`/`axum-server`
  and `alopex-cli`'s `reqwest` client (RUSTSEC-2026-0104, RUSTSEC-2026-0099,
  RUSTSEC-2026-0098; the native-tls/openssl chain was also removed from
  `alopex-cli`, which had been silently linked despite the `rustls-tls`
  feature being requested), plus semver-compatible patch bumps for `bytes`,
  `crossbeam-epoch`, `lz4_flex`, `quinn-proto`, `rustls-webpki`, and `time`.
  `object_store` was updated 0.11 → 0.14, but the two remaining advisories
  (RUSTSEC-2026-0194, RUSTSEC-2026-0195, both in the transitive `quick-xml`
  dependency, not used directly by alopex code) are blocked on an upstream
  `object_store` release not yet on crates.io; tracked in #42 and
  suppressed in CI via `rustsec/audit-check`'s `ignore` input until then.

### Added
- `Database.execute_sql` / `Transaction.execute_sql` in `alopex-py`:
  SELECT returns `list[dict]` (column order preserved), DML returns the
  affected-row count, DDL returns `None`. Positional `?` parameters are
  bound client-side with quote/comment-aware substitution (#27).
- `Database::execute_sql_multi` in `alopex-embedded`: executes all statements
  in one transaction and returns one `ExecutionResult` per statement.
- Mode-parity verification & demo suite (`scripts/parity/`): a shared SQL
  corpus with hand-calculated expected results, executed across the
  embedded API (in-memory / file), CLI, HTTP, gRPC, and now cluster-aware
  server surfaces to verify the mode-parity invariants (same data
  directory, same SQL, same results), plus a pinned verification container
  and a five-act demo (SF-CLUSTER, the fifth act, is no longer skipped).
- v0.7 feature demo suite (`scripts/demo/v07/`): cluster status
  cross-surface verification (HTTP/CLI, membership lifecycle, degraded
  fallback), routing transparency (live `local_only` decisions plus the
  simulated scatter-gather harness contract), and DataFrame P3
  (`str`/`dt`/`list` namespaces, `explode`/`implode`) with hand-calculated
  expected results and determinism checks.
- `_alopex.pyi` now declares `Database.cluster_status()` and
  `Database.routing_diagnostics()`, which were implemented but had no type
  stubs.

### Changed
- CLI `sql --output json` always emits an array of per-statement result sets;
  a single statement yields a 1-element array. DDL/DML statements contribute a
  `status`/`message` result set (omitted with `--quiet`). Remote (`--server`)
  output uses the same array-of-result-sets shape.

### Breaking Changes
- CLI `sql --output json` output shape changed: previously a single result set
  was emitted as an array of row objects; it is now always an array of
  per-statement result sets (one extra level of nesting, even for a single
  statement). Consumers parsing the old shape must unwrap the outer array.

## [0.7.0]

### Added
- Cluster-aware foundation through the `alopex-cluster` boundary, including
  stable node identity, membership lifecycle, placement metadata, routing
  diagnostics, and status schema contracts.
- Server admin cluster status surfaces for status, health, metrics, join, and
  leave operations while preserving `single_node` as the default mode.
- CLI and Python cluster status projections with cross-surface fixture checks
  against the Server status schema.
- Query Router planning foundation for `local_only`,
  `future_distributed_execution_required`, and simulated scatter-gather routing
  decisions with retry/backoff, idempotency, cancellation, and diagnostics
  coverage in the release gate.
- DataFrame P3 namespace primitives for string, datetime, and list operations
  in `alopex-dataframe`.
- Python `DataFrame` wrapper APIs for P3 namespace operations, `explode`,
  `implode`, default constructor/from_columns compatibility, and schema aliases
  for default DataFrame contracts.
- v0.7 release gate (`scripts/release/v07_gate.sh`) covering the v0.6
  baseline, cluster-aware tests, Server/CLI/Python status consistency,
  DataFrame P3, compatibility regressions, workflow contracts, and release
  binary smoke build.

### Changed
- Default Embedded and Server behavior remains single-node compatible with
  v0.6. Cluster-aware metadata and routing behavior are opt-in through
  cluster-aware configuration.
- Live Server SQL routing now records local routing diagnostics. Queries that
  would require production distributed execution return a stable future-work
  diagnostic instead of attempting partial remote execution.
- Release readiness is separated from release completion: the release branch is
  merged to `main` before tagging, the `v0.7.0` tag points to that merged `main`
  commit, and the release is complete only after artifacts and branch cleanup
  are verified.

### Compatibility
- Existing v0.6 Embedded, Server, SQL, DataFrame, and Python default behavior is
  covered by compatibility regressions.
- v0.7 metadata initialization and upgrade paths are designed to be idempotent
  and safe to retry.
- The v0.7 cluster status schema and routing diagnostics are forward
  compatibility contracts for v0.8 Metadata Raft and v0.9 Multi-Raft,
  distributed transaction, and Changefeed work.

### Not Included
- Production remote scatter-gather execution.
- Raft-backed metadata consensus or Raft DDL distribution.
- Distributed transactions, Multi-Raft placement, and Changefeed execution.
- The alopex-py Client/Transaction/ConnectionPool API, which remains on the
  independent alopex-py client release track.

## [0.4.0]

### Added
- Polars Unity Catalog API for Python bindings.
- Catalog API entry point: `Catalog`.
- Catalog metadata classes: `CatalogInfo`, `NamespaceInfo`, `TableInfo`, `ColumnInfo`.
- Catalog methods:
  - `list_catalogs`, `list_namespaces`, `list_tables`, `get_table_info`
  - `create_catalog`, `delete_catalog`, `create_namespace`, `delete_namespace`
  - `create_table`, `delete_table`, `scan_table`, `write_table`

### Breaking Changes
- None.

## [0.4.2]

### Added
- TUI as default mode for interactive terminals; automatically activates in TTY without `--tui`.
- TUI support for all non-SQL commands (KV, Vector, HNSW, Columnar, Server, Profile).
- Unified TUI renderer for consistent Column/Row display across commands.
- Admin TUI navigation with data lifecycle operations (create/update/delete/archive/restore/backup/export).
- Lifecycle subcommands (`archive`, `restore`, `backup`, `export`) with placeholder implementations.
- mTLS certificate validation: expiration and CN/SAN host matching enforcement.
- `--insecure` flag for explicit HTTP opt-in with warning.
- SELECT literal-only queries support (e.g., `SELECT 1`, `SELECT 1, 'ok'`) without FROM clause.

### Changed
- Default UI mode is now TUI in TTY; `--batch` or `--output` forces batch mode.
- Non-TTY environments fall back to batch output with warning message.
- Admin TUI uses rainfrog-inspired layout with left navigation and main panel.

### Fixed
- HNSW persistence after inserts.
- Columnar compression defaults to zstd.

## [0.4.1]

### Added
- Streaming SQL over HTTP with JSON array output and backpressure-aware buffering.
- Server connection profiles with HTTPS enforcement and authentication (token/basic/mTLS).
- TUI status bar showing connection, row count, and processing status; improved keybindings.
- Server admin commands for status/metrics/health/compaction trigger with updated schema fields.

### Changed
- SQL server endpoint updated to `/api/sql/query`.
- Output formats aligned to table/json/csv/tsv/quiet (jsonl removed from documented support).
- TUI paging behavior now uses half-screen paging for Ctrl+U/Ctrl+D.

## [0.3.4]

### Added
- CLI profile management commands (create/list/show/delete/set-default).
- Batch mode support (`--batch`, TTY detection, `ALOPEX_MODE=batch`).
- KVS transaction commands (begin/get/put/delete/commit/rollback).
- Columnar ingest (Parquet/CSV) and index management (minmax/bloom).
- Streaming-friendly output for jsonl/csv/tsv.
- File format version compatibility checks.
- Shell completions (bash/zsh/fish/pwsh).
