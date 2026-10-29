# HNSW incremental node writes

Related: #681. This change removes the unconditional rewrite of every HNSW
node on each staged INSERT or UPDATE. It preserves format version 1.

The graph tracks changed slots, including reverse edges, neighbor pruning,
logical deletion, physical removal and slot reuse. Incremental persistence
unions those slots with the transaction's addressed nodes. It deduplicates
writes and deletes removed slots without scanning the entire slot array.

Dirty slots are cleared only after all synchronous writes, including the final
header, succeed. A failed save keeps them for retry. Rolling back staged graph
changes restores the graph's dirty set along with its snapshot.

A successful flush into a KV transaction is not yet a durable commit. The graph
therefore remembers the header written by its previous save or read by load.
If the target transaction sees a different header or no known baseline, the
next flush uses a full save. This covers a rollback after a successful flush,
first persistence and saving the handle into another store. It uses the existing
format's checksum contract; it does not introduce a new cryptographic identity.
Async full-save plans do not reset this synchronous baseline and safely take
the full-save fallback on the next staged flush if needed.

Validation uses a 32-node, two-dimensional graph to show that one insertion
writes reverse-edge changes while leaving untouched node records alone. Other
cases use at most eight nodes. A 32-case property test compares complete KV
bytes and reopen results against a full format-v1 save across at most eight
keys and sixteen mutations. Separate cases cover rollback, failure before
writing, failure after two buffered writes, compaction, slot reuse, batched
insertion and a rolled-back flush. The same nine cases run on Windows.

This is one stage of #681. Full graph cloning for forks and rollback snapshots,
full checksum calculation, key-index synchronization and statistics calculation
remain. This change does not claim to remove all O(N) work or establish a new
100k-row timing result. The dedicated post-fix measurement follows the remaining
write-path work; normal CI receives no load or performance test.
