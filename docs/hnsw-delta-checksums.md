# HNSW delta checksums and key mappings

Related: #681; extends the changed-slot persistence in #752.

A normal staged flush now updates both the format-v1 checksum and key mappings
from the changed slots. It does not serialize all unchanged nodes or scan and
rewrite the entire key index. The graph still uses format version 1:

`checksum = CRC32(header with checksum = 0) + sum(CRC32(serialized node))`

All arithmetic is wrapping u32 arithmetic. The previous header yields the node
checksum sum. For each changed slot, persistence subtracts the old node CRC and
adds the new node CRC. It then adds the new header CRC. Before-images come from
the transaction snapshot already retained for rollback; no additional full
snapshot is introduced by this change.

The incremental path requires a known persisted header matching the target KV
transaction, and a clean before-image with the same header. A snapshot containing
unsaved mutations is not accepted. First persistence, a changed target, a prior
flush rolled back by KV, direct mutations without a matching staged snapshot,
and a full save performed while staging use the existing full-save fallback.
Direct compaction without a staged before-image also takes this fallback.
An unchanged flush needs no node serialization or node/key scan.

The before-image remains stable across a partially failed flush. Retrying in
the same KV transaction therefore computes the same checksum even when some
new node records have already been buffered. Dirty tracking and its baseline
are updated only after all writes, including the header, succeed.

Removed key mappings are identified from changed slots' before-images. A key
removed and reinserted into a different slot in the same staged group retains
its current mapping. Routine delta flushes do not scrub unrelated orphan key
records; the full-save fallback retains the complete prefix replacement.

Tests compare every stored KV byte and reopened graph with full format-v1
serialization. Two bounded property tests cover up to eight keys and sixteen
operations, both flushing each operation and grouping operations before a
flush. Additional cases cover rollback, partial retry, slot reuse, a mismatched
before-image and key movement. A 32-node graph verifies work counts: test-only
instrumentation counts node serialization and rejects KV scans on the valid
incremental path. There are no elapsed-time thresholds or load tests in CI.

Whole-graph forks and rollback snapshot copies, aggregate statistics calculation,
and other #681 begin/savepoint/statistics work remain. The dedicated post-fix
100k-row measurement follows the remaining write-path changes; this document
does not claim a measured speedup or completion of #681.
