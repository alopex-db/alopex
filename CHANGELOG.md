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
