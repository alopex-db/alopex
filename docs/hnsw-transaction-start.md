# HNSW cache at transaction startup

Explicit borrowed `Database::begin` and owned
`Database::begin_owned_embedded_transaction` try to acquire the HNSW commit gate
without waiting. If the read gate is available, they create the backend
transaction and capture shared cached graphs while holding it, preserving the
pairing of the backend read point and graph versions.

If a writer holds the gate, or the platform's reader/writer lock refuses another
reader while a writer is waiting, startup uses an empty transaction graph cache.
The first HNSW query loads from that transaction's own backend snapshot. It does
not take a graph from the global cache after the transaction has started.

The fallback samples the cache epoch before creating the backend transaction.
A commit overlapping startup therefore invalidates later publication of a graph
loaded by that transaction. Sampling after backend creation would allow an old
snapshot to be mistaken for the new epoch. Publication still uses the existing
commit gate and epoch recheck.

This removes waiting on the HNSW commit gate at explicit startup. Backend
snapshot creation, range-change journal capture, catalog locks and subsequent
SQL execution retain their own synchronization. This is not a guarantee that
all calls to `begin` complete without waiting. Global cold-cache publication may
still wait at transaction completion. A poisoned HNSW gate still fails loudly.

## Focused verification

`cargo test --locked -p alopex-embedded --lib hnsw_begin_tests` covers borrowed
and owned APIs, read-only and read-write transactions, and memory and persistent
LSM stores. Each database has two two-dimensional vectors. The writer gate is
held until startup reports completion; it is always released before a failed
assertion. A five-second receive timeout is a deadlock guard, not a performance
threshold. After a concurrent vector update, the transaction must see its old
snapshot, a fresh reader must see the update, and the old graph must not be
republished. Separate cases check warm cache reuse, epoch ordering and backend
error propagation.

The dedicated Windows native contract runner includes the same eleven cases.
There is no scale or throughput workload in these tests.
