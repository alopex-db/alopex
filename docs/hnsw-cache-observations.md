# HNSW cache admission counters and warning lifetime

The cache statistics distinguish capacity decisions from committed data changes:

| Event | `oversize_skips` | `evictions` | `invalidations` |
| --- | --- | --- | --- |
| A graph cannot fit, with no retained version | +1 | unchanged | unchanged |
| A graph cannot fit and replaces a retained version | +1 | +1 | unchanged |
| Limits or LRU pressure remove a retained graph | unchanged | +1 per graph | unchanged |
| Committed changes invalidate a retained graph | unchanged | unchanged | +1 per graph |
| Committed changes name an unretained graph | unchanged | unchanged | unchanged |

Byte and entry accounting follows actual retained entries. Repeated admission
failures increment `oversize_skips` every time, but do not repeatedly count an
eviction after the previous entry has gone. Queries remain available when the
cache cannot retain a graph.

The capacity warning is emitted once per index name during an uninterrupted
capacity-rejection period. Successful retention or committed invalidation of
that name ends the period. An actual byte/entry limit change resets all warning
suppression, as does broad committed invalidation. Reapplying identical limits,
invalid limit arguments and failed commits do not reset suppression. Selective
invalidation also clears warning state for an index that has no retained graph;
unrelated names keep their suppression and retained entries.

The focused Rust tests use one-row, two-dimensional graphs and capture actual
warning events. They cover repeated rejection, rejected replacement, successful
retention, unchanged/changed/invalid limits, selective/broad invalidation and
failed commits. These are functional tests; no timing or load threshold is added
to normal CI.

This addresses the cache-observation item in #681. It does not complete the
remaining first-write copying, transaction-start, savepoint, dedicated scale
comparison, candidate acceptance or release qualification work tracked by #690.
