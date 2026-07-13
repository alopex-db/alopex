# Changelog

All notable changes to this project will be documented in this file.

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
