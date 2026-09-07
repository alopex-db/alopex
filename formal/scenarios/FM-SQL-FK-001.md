# FM-SQL-FK-001 implementation scenarios

## Fixed starting state

Two read-write transactions start while parent `1` exists and no child references it. One transaction stages `DELETE FROM parents WHERE id = 1`; the other stages `INSERT INTO children VALUES (1)`.

## Coverage map

| Model transition or invariant | Implementation evidence |
| --- | --- |
| `CommitInsert`, `RejectDeleteConflict` | `concurrent_parent_delete_and_child_insert_cannot_both_commit` |
| `CommitDelete`, `RejectInsertConflict` | `concurrent_parent_delete_then_child_insert_cannot_both_commit` |
| `ReferentialIntegrity` | Both tests query the committed parent and child tables after the second commit fails. |
| `ConflictDoesNotPublish` | Both tests assert that the failed transaction's staged mutation is absent. |

## Implementation mapping

The SQL constraint executor validates the parent or child relation in each transaction snapshot. The local range-change journal stages one shared epoch update with every SQL data commit. KV read/write conflict detection rejects the second epoch update, so the failed transaction cannot publish its staged row mutation.
