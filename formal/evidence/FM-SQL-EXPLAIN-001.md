# FM-SQL-EXPLAIN-001 evidence

TLC 2.20-compatible build `2026.05.12.170007` checked
`ExplainLifecycle.cfg` with one worker on 2026-09-01.

- Result: no error found
- States: 9 generated, 9 distinct, 0 left on queue
- Complete-state-graph depth: 3
- Checked invariants: `TypeOK`, `PlainNeverWrites`, `FailureIsRolledBack`
- Deadlocks: disabled because terminal lifecycle states intentionally have no successor
