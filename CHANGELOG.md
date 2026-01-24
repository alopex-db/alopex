# Changelog

All notable changes to this project will be documented in this file.

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
