# FM-SQL-METADATA-001 evidence

TLC 2.20-compatible build `2026.05.12.170007` checked
`MetadataVisibility.cfg` with one worker on 2026-09-02.

- Result: no error found
- States: 85 generated, 24 distinct, 0 left on queue
- Complete-state-graph depth: 8
- Checked invariants: `TypeOK`, `TemporaryIsNotDurable`, `IdleHasNoOverlay`, `DistributedFailsClosed`
- Deadlocks: disabled because terminal and idempotent lifecycle states are intentional
