# Portable SQL metadata

Alopex v0.8.11 exposes catalog metadata through SQL query results rather than
requiring SQLite-specific PRAGMA calls. The same schemas are returned by Rust,
Python, CLI JSON/table output, and DataFrame queries.

## Quick start

```sql
SHOW TABLES;
SHOW INDEXES;
SHOW INDEXES FROM "Order Items";
DESCRIBE "Order Items";

SELECT table_name, table_type FROM information_schema.tables;
SELECT table_name, column_name, ordinal_position
FROM information_schema.columns
ORDER BY table_name, ordinal_position;
SELECT table_name, index_name, column_name
FROM information_schema.indexes
ORDER BY table_name, index_name, ordinal_position;
```

The runnable batch is
[`scripts/demo/v08/demo_metadata.sql`](../scripts/demo/v08/demo_metadata.sql).

## Exact result schemas

| Surface | Columns in order |
| --- | --- |
| `SHOW TABLES` | `table_name TEXT` |
| `SHOW INDEXES [FROM table]` | `table_name TEXT`, `index_name TEXT`, `is_unique BOOLEAN`, `index_type TEXT` |
| `DESCRIBE` / `DESC` | `column_name TEXT`, `column_type TEXT`, `null TEXT`, `key TEXT`, `default TEXT`, `extra TEXT` |
| `information_schema.tables` | `table_catalog TEXT`, `table_schema TEXT`, `table_name TEXT`, `table_type TEXT` |
| `information_schema.columns` | `table_catalog TEXT`, `table_schema TEXT`, `table_name TEXT`, `column_name TEXT`, `ordinal_position BIGINT`, `column_default TEXT`, `is_nullable TEXT`, `data_type TEXT` |
| `information_schema.indexes` | `table_catalog TEXT`, `table_schema TEXT`, `table_name TEXT`, `index_name TEXT`, `is_unique BOOLEAN`, `index_type TEXT`, `ordinal_position BIGINT`, `column_name TEXT` |

Rows are ordered by catalog, schema, table, index, and ordinal position where
those keys apply. `null` and `is_nullable` use `YES`/`NO`; `key` uses `PRI`,
`UNI`, or the empty string. Literal defaults use canonical SQL text. A default
without a canonical literal representation is reported as NULL until the AST
provides a shared SQL formatter.

## Visibility and lifetime

Metadata shows every object visible in the current catalog view. A transaction
sees its own staged CREATE/DROP operations; rollback removes those staged
changes. Quoted names preserve spelling, spaces, and keyword text.

`CREATE TEMP TABLE` and `CREATE TEMPORARY TABLE` create database-handle-scoped
objects. They are visible to later transactions using the same handle and have
`table_type = 'LOCAL TEMPORARY'`. Reopening a persistent database hides them.
A retained internal catalog tombstone prevents their table IDs from being
reused for durable data.

Alopex v0.8.11 has no users, roles, grants, views, or row-level catalog
permissions. Consequently the visibility boundary is the current database
handle and transaction snapshot; the metadata layer does not invent a second
authorization system.

## Distributed reads and compatibility

Metadata is materialized from one coordinator-local catalog snapshot during
planning. Distributed-read validation already classifies the resulting
`Values` plan as local-only, so a remote request fails as a whole instead of
returning stale or partially merged rows. No partial-success metadata response
exists in v0.8.11.

Parser contract `0.25.0` adds relational constraints, advanced DML, COPY,
sequences, identity columns, and SERIAL types to the existing `CreateTable`
surface.
Older durable table and index records remain readable because existing enum
discriminants are unchanged and the new table type is appended. Metadata is a
query surface only; no durable catalog migration is required for ordinary
tables.

## Release evidence

The v0.8 verifier runs the owning Rust workspace and Python suites through
`crates/alopex-tools/v08/verify-v08-surfaces.sh`. Exact-schema coverage lives in
the embedded metadata, DataFrame, CLI multi-statement, Nim/Rust parser, and
Python SQL tests; the verifier therefore exercises the public surfaces without
duplicating their assertions.
