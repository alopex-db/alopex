# FM-SQL-TXN-001: SQL transaction lifecycle

## Scope

`SqlSession` and its owned embedded transaction define the modeled lifecycle. The model covers successful and failed local `COMMIT`/`ROLLBACK`, statement failure, savepoint recovery, commit-barrier ordering, post-commit read success/failure, disconnect or process termination, and restart.

The model deliberately excludes termination during the internal execution of `COMMIT`. That interval has an ambiguous client outcome and belongs to retry/idempotency design rather than the v0.8.11 local transaction contract.

## Invariants

- Only durable writes are visible after every transition and restart.
- Every acknowledged committed write is durable.
- Rolled-back, disconnected, or pre-commit terminated writes never become durable.
- An idle session or terminated process owns no staged writes.
- A failed session can recover through rollback or rollback-to-savepoint, but cannot commit.
- A post-commit read can run only after a successful commit barrier and cannot change durable state.
- A failed commit does not acknowledge or publish staged writes; a failed rollback returns the session to idle without publishing them.

## Artifacts

- Model: `formal/tla/sql/SqlTransactionLifecycle.tla`
- TLC configuration: `formal/tla/sql/SqlTransactionLifecycle.cfg`
- Implementation mapping: `formal/scenarios/FM-SQL-TXN-001.md`
- Verification evidence: `formal/evidence/FM-SQL-TXN-001.md`

## Verification

- Local/CI command: `tlc formal/tla/sql/SqlTransactionLifecycle.tla`
- Implementation failure fixture: `cargo run --manifest-path crates/alopex-tools/Cargo.toml --bin verify-sql-transaction-failures`
- Status: active
