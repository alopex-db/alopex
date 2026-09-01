# FM-SQL-TXN-001: SQL transaction lifecycle

## Scope

`SqlSession` and its owned embedded transaction define the modeled lifecycle. The model covers atomic local `COMMIT`, `ROLLBACK`, statement failure, savepoint recovery, disconnect or process termination before a terminal statement, process termination after an acknowledged terminal statement, and restart.

The model deliberately excludes termination during the internal execution of `COMMIT`. That interval has an ambiguous client outcome and belongs to retry/idempotency design rather than the v0.8.11 local transaction contract.

## Invariants

- Only durable writes are visible after every transition and restart.
- Every acknowledged committed write is durable.
- Rolled-back, disconnected, or pre-commit terminated writes never become durable.
- An idle session or terminated process owns no staged writes.
- A failed session can recover through rollback or rollback-to-savepoint, but cannot commit.

## Artifacts

- Model: `formal/tla/sql/SqlTransactionLifecycle.tla`
- TLC configuration: `formal/tla/sql/SqlTransactionLifecycle.cfg`
- Implementation mapping: `formal/scenarios/FM-SQL-TXN-001.md`
- Verification evidence: `formal/evidence/FM-SQL-TXN-001.md`
