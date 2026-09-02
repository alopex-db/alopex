# SQL schema evolution

Alopex v0.8.11 supports persistent dynamic views, transactional table changes,
and transactional truncation. The implementation reuses the normal planner,
catalog overlay, row codec, type coercion, and index cleanup paths.

## Supported statements

| Statement | Supported behavior |
| --- | --- |
| `CREATE VIEW [IF NOT EXISTS] name [(columns)] AS SELECT ...` | Stores the SELECT definition and resolves it on every query; nested views are supported. |
| `DROP VIEW [IF EXISTS] name` | Drops an unreferenced view. |
| `ALTER TABLE [IF EXISTS] name ADD [COLUMN] [IF NOT EXISTS] column type [DEFAULT value] [NOT NULL]` | Migrates existing rows using the same default evaluator as INSERT. Key constraints must use CREATE INDEX. |
| `ALTER TABLE name DROP [COLUMN] [IF EXISTS] column` | Rejects the last column and indexed columns. |
| `ALTER TABLE name RENAME [COLUMN old TO new \| TO new_table]` | Renames catalog metadata and associated index metadata. |
| `ALTER TABLE name ALTER [COLUMN] column ...` | Supports `TYPE`/`SET DATA TYPE`, `SET/DROP DEFAULT`, and `SET/DROP NOT NULL`. |
| `TRUNCATE [TABLE] name` | Removes rows, sequences, and B-tree/FTS/HNSW index state without dropping metadata. |

`CREATE OR REPLACE VIEW`, materialized views, `CASCADE`, and compound ALTER
actions are not accepted in v0.8.11. A view is read-only and cannot be the
target of INSERT, UPDATE, DELETE, ALTER TABLE, or TRUNCATE.

## Dependencies and planning

View definitions store their direct relation dependencies. DROP VIEW, DROP
TABLE, and ALTER TABLE use `RESTRICT` behavior and report dependent views;
dropping dependents from the leaves inward is required. A view can reference
only objects visible when it is created, so self-reference and dependency
cycles fail during normal name resolution.

Views are dynamic rather than materialized. Each query deserializes and plans
the stored SELECT through the existing planner. Prepared statements also parse
and plan on each execution, so a renamed or removed column cannot use a stale
plan. Generic Alopex SQL does not own continuous aggregates; their separate
Skulk lifecycle is unchanged.

## Transactions, recovery, and compatibility

Row rewrites, catalog metadata, and index metadata use one KV transaction.
Explicit rollback and process interruption discard the catalog overlay and row
changes together; commit publishes them together. Persistent databases reload
the committed definition and migrated rows on reopen. Multiple writers retain
the existing single-writer/conflict behavior.

Backups remain physical database backups: restore the complete database and
open it with v0.8.11 or newer. Older binaries fail closed when they encounter
the new VIEW catalog variant; they must not be used to write a database after
v0.8.11 creates a view. Tables changed only by ALTER TABLE/TRUNCATE retain the
existing table metadata envelope.

The parser wire contract is `0.25.0`. A parser asset with another contract is
rejected before MessagePack decoding.

## Verification

Public embedded tests cover dynamic and nested views, dependency restriction,
prepared-statement replanning, row migration, rollback, truncation, rename,
and reopen. `FM-SQL-SCHEMA-001` model-checks commit, rollback/crash, and reopen
states for partial schema visibility.
