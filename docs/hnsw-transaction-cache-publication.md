# Transaction-local HNSW cache publication

Related: #681; depends on the MemoryKV snapshot correction in #748 / #750.

Explicit borrowed and owned SQL transactions retain a graph loaded by a kNN
read in the shared database cache after a successful commit or rollback. The
next transaction can reuse it without loading the persisted graph again.
`rollback_in_place` on the borrowed API follows the same rule. This covers both
read-only transactions and read-write transactions that performed no writes.

Publication requires all of the following:

- The backend can enumerate its complete final write set, and that set is empty.
  Even unrelated KV writes conservatively prevent publication.
- The graph was loaded in this transaction and never requested for mutation.
  Flushing a mutation clears its dirty flag but does not restore eligibility.
- The transaction ends successfully, and no intervening commit has changed the
  cache epoch since the transaction began. A successful no-op commit advances
  the epoch itself; publication checks that expected next epoch again while
  holding the publication gate.
- The current shared-cache byte and entry limits permit insertion.

The owned session closes access before observing the final write set, including
on rollback. Concurrent handles cannot add writes between this observation and
the backend's terminal operation. An unsupported visitor, failed terminal
operation or stale epoch publishes nothing. An entry already in the shared cache
is retained without replacement. Implicit drop does not publish a graph.

The two-row, two-dimensional integration cases in `hnsw_cold_publication.rs`
cover both transaction APIs and modes, commit/rollback, reuse by a second
transaction, stale snapshots, flushed writes, unrelated writes, repeated
termination and disabled/oversized caches. The Windows native runner executes
these same 21 cases. Scale and performance measurements remain in the dedicated
manual runner introduced by #749; this change does not add a load test to CI.

This is one part of #681. Whole-graph copying and saving on writes, begin-gate
contention, savepoint reuse and cache statistics remain separate work. The
post-change scale measurement and candidate acceptance are not claimed here.
