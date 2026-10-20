# HNSW TRUNCATE transactions

The v0.8.17 change for #677 clears transaction-local graph identities before
HNSW index creation or deletion writes graph keys. Pending graph changes are
flushed first, including changes to other indexes in the same transaction.
Subsequent statements load the new graph from that transaction's KV state.
This uses the existing transaction contract and does not change the public API
or database format.

After `TRUNCATE` followed by `INSERT` and `COMMIT`, HNSW statistics and queries
reflect the replacement rows, including after reopening the database. A full
rollback or rollback to a pre-TRUNCATE savepoint restores the original rows and
graph. `node_count` counts live nodes; `deleted_count` reports tombstones
separately when DELETE follows TRUNCATE.

This change does not automatically rebuild an index already persisted with old
nodes by an affected binary. Reopening that database is not a repair. If the
authoritative table rows and vector values are intact, rebuild the affected
index using its original column, metric and options. For the two-dimensional
test table `items(id, e)` and index `ih`, the verified transaction is:

```python
tx = db.begin(TxnMode.READ_WRITE)
tx.execute_sql("DROP INDEX ih")
tx.execute_sql("CREATE INDEX ih ON items (e) USING HNSW")
tx.commit()
```

Use the actual index definition for an application database. TRUNCATE deletes
table rows and is not an index-repair command.

The regression tests use two-dimensional vectors and at most two table rows
at a time. COMMIT/reopen, ROLLBACK, mixed DELETE/UPDATE, empty COMMIT, savepoints,
and independent staged index writes are separate functional cases. The bounded
manual witness is `scripts/repro/v0816-followups/f1_truncate_hnsw_txn.py`;
the former 3000-row reproduction is not a normal-CI requirement.
