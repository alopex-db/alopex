# FM-SQL-SCHEMA-001 evidence

TLC 2.20-compatible build `2026.05.12.170007` checked
`SchemaEvolutionLifecycle.cfg` with one worker on 2026-09-02.

- Result: no error found
- States: 161 generated, 39 distinct, 0 left on queue
- Complete-state-graph depth: 7
- Checked invariants: `TypeOK`, `IdleHasNoPartialMigration`, `ViewKeepsDependency`
- Deadlocks: disabled because stable idle and transaction lifecycle states are intentional
