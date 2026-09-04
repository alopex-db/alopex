# FM-SQL-PREP-001: prepared statement lifecycle

## Scope

`PreparedStatement` and `PreparedSessionStatement` define the modeled lifecycle. The model covers one-based positional binding, reset, repeated execution, schema changes, and finalization. A prepared execution reparses the rendered SQL, so it records the schema current at that execution instead of retaining a cached plan.

## Invariants

- Execution is enabled only after every positional parameter is bound.
- Finalization clears bindings and is irreversible.
- Every execution refers to a known schema version.

## Artifacts

- Model: `formal/tla/sql/PreparedStatementLifecycle.tla`
- TLC configuration: `formal/tla/sql/PreparedStatementLifecycle.cfg`
- Implementation mapping: `formal/scenarios/FM-SQL-PREP-001.md`
- Verification evidence: `formal/evidence/FM-SQL-PREP-001.md`
