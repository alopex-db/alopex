# FM-SQL-TXN-001 implementation scenarios

## Coverage map

| Model transition or invariant | Implementation evidence |
| --- | --- |
| `Begin`, `Commit`, `Rollback`, invalid control transition | `sql_session::tests::control_transition_table_is_deterministic` and `invalid_transition_is_typed_and_statement_failure_requires_rollback` |
| `StatementFail`, failed commit rejection | `sql_session::tests::owned_transaction_cannot_commit_after_a_statement_failure` |
| `RollbackToSavepoint` | `sql_session::tests::rollback_to_savepoint_recovers_failed_state_and_catalog_overlay` |
| Concurrent commit failure and recovery | `sql_session::tests::terminal_commit_failure_does_not_wedge_the_session` and `verify-sql-transaction-failures` |
| Disconnect before commit | `verify-sql-transaction-failures` drops an active `SqlSession`, reopens the file, and checks absence |
| Process termination before commit | `verify-sql-transaction-failures` kills the active child and checks absence after restart |
| Process termination after acknowledged commit | `verify-sql-transaction-failures` kills the child after `COMMIT` returns and checks visibility after restart |
| Rollback followed by process termination | `verify-sql-transaction-failures` kills the child after `ROLLBACK` and checks absence after restart |
| Public release reuse | `scripts/release/verify-release/run.sh` builds and runs the fixture against the exact crates.io version |

## Observable contract

The fixture exits successfully only when every mapped scenario passes. TLC separately checks every reachable state of the bounded model against the inventory invariants.
