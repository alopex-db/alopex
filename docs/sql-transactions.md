# SQL Transactions

`Database::sql_session()` executes one SQL statement per call. Outside an
explicit transaction, statements use auto-commit. `BEGIN` and
`START TRANSACTION` enter an explicit read-write transaction; `COMMIT` and
`ROLLBACK` return the session to auto-commit mode.

## Session states

| State | Allowed transaction controls |
| --- | --- |
| Idle | `BEGIN`, `START TRANSACTION` |
| Active | `SET TRANSACTION` before work; `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE SAVEPOINT` |
| Failed | `ROLLBACK`, `ROLLBACK TO SAVEPOINT` |

A failed ordinary statement changes an explicit transaction to `Failed`.
`ROLLBACK TO SAVEPOINT` returns it to `Active` only when the named savepoint
exists and rollback succeeds. A top-level `ROLLBACK` always ends the explicit
transaction, including when the backend reports that a failed commit already
closed its storage transaction.

## Savepoint stack

Savepoint names are compared case-insensitively. Duplicate names are allowed;
the newest matching name shadows older entries.

- `SAVEPOINT name` appends a snapshot of staged KV writes and catalog changes.
- `ROLLBACK TO SAVEPOINT name` restores the newest matching snapshot, keeps
  that savepoint, and discards its descendants.
- `RELEASE SAVEPOINT name` keeps current writes but discards the newest
  matching savepoint and its descendants.
- A missing name returns `Error::SavepointNotFound` without changing session
  state or transaction contents.
- Top-level `COMMIT` and `ROLLBACK` discard the complete savepoint stack.

## Transaction characteristics

`BEGIN` and `START TRANSACTION` accept `ISOLATION LEVEL ...` and `READ ONLY` or
`READ WRITE`. A bare `BEGIN` may be followed by one `SET TRANSACTION` before
any query, mutation, or savepoint operation. Characteristics specified on the
start statement are locked immediately.

Alopex maps SQL `REPEATABLE READ` to its engine-native start-time snapshot and
optimistic commit validation. A transaction does not observe commits made
after its snapshot. A new transaction observes those commits. Alopex does not
claim predicate-lock serializability, so `READ UNCOMMITTED`, `READ COMMITTED`,
and `SERIALIZABLE` return `UnsupportedSqlTransactionIsolation` instead of
falling back silently.

`READ ONLY` rejects DDL and DML before executor dispatch with `TxnReadOnly`.
Queries remain allowed, and the rejection does not fail the transaction.
`READ WRITE` is the default. These local snapshot semantics do not establish
the replica freshness or version evidence tracked by issue #226.
