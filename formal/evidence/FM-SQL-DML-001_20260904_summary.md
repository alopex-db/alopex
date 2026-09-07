# Evidence: FM-SQL-DML-001

## Date

2026-09-04 (Asia/Tokyo)

## Working Tree

Uncommitted v0.8.11 completion worktree.

## Command

`tlc formal/tla/sql/AdvancedDmlAtomicity.tla`

## Result

TLC 2026.05.12.170007 completed with no error after the independent review strengthened row identity and retry coverage: 87 generated states, 49 distinct states, depth 8, and 0 states left on the queue.

## Invariants

`TypeOK`, `RejectedOrConflictedDoesNotPublish`, `OnlyCommitPublishes`.

## Counterexample

None.

## Follow-up

The owning Rust atomicity tests and PostgreSQL 16.14 / DuckDB 1.4.0 live differential remain the implementation evidence.
