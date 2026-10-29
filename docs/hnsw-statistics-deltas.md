# Incremental HNSW statistics

HNSW index updates, staged commits, rollback and owned graph forks previously
recomputed statistics by scanning every node. Graphs now retain aggregate
active-node, edge, level and payload-byte totals. Each changed slot records its
first contribution before mutation; refresh subtracts that contribution and
adds the slot's current contribution. Multiple writes before a refresh keep
one before-image per slot. Logical deletion contributes zero active payload;
revival and slot reuse restore the new contribution.

All production node mutation paths mark before-images before editing: insert,
upsert and reverse-edge removal, logical delete, compaction, reciprocal edge
insertion and pruning. Statistics tracking is separate from persistence's dirty
slot set, so refreshing statistics cannot discard pending durable writes.
Graph forks and rollback snapshots copy the aggregate and pending deltas with
their graph version. A clean fork refreshes no node slots. An uninitialized
graph, including a graph just loaded from storage, performs one initial scan.

The existing public statistics fields and persistence format are unchanged.
`memory_bytes` keeps the existing logical estimate of active payloads and slot
capacity; it is not unique physical RSS and does not account for every allocator
or cache bookkeeping object. `stats()` retains its existing staged-update
visibility behavior. The previous full-scan implementation remains a test-only
reference model.

The delta refresh visits changed slots and their neighbor lists, plus the level
histogram. It does not make the entire first write constant time: copy-on-write
directory/shard detach, graph search, reverse-edge scans and compaction have
their own costs. The graph-level diagnostic statistics helper remains separate.
Dedicated 100k-row × 384-dimension before/after measurements remain manual work;
no scale or time-threshold test is added to normal CI.

## Verification scope

Small reference-model traces compare every public field against the full scan
through updates, bursts, delete/revive, compaction, slot reuse and independent
forks. Separate cases verify save/load, staged commit and rollback. A structural
check verifies that a clean fork and repeated refresh visit zero node slots;
it does not infer latency from a small fixture. The new four cases are also
included in the Windows native contract runner. Existing small graph, format,
dirty-write and copy-on-write regressions remain part of focused verification.
