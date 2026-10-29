# HNSW graph snapshots with copy-on-write storage

An owned index fork and the transaction before-image share node directories,
node pages, node payloads, key-map shards and free-list pages. Writing one node
makes its payload private. A graph snapshot no longer copies every vector,
metadata buffer, adjacency list and external key up front.

The private containers use only standard-library ownership:

- Node and free-list directories share 128-slot pages. First write after a
  snapshot copies the directory's page references and the addressed page.
- Each node payload has an independent `Arc` ownership boundary. Copying a page
  copies node handles; it does not copy every payload in that page.
- The key map has 256 lazily allocated shards, with a random shard hash shared
  across snapshots. Writes detach the shard directory and the affected shard.
- Update and compaction inspect reverse edges before requesting mutable access,
  avoiding detachment of every page and node during their scans.

The key shard hash is process-local and is never serialized. Node IDs, free-slot
reuse, key mappings, and format-v1 serialized nodes/checksums retain their
existing meaning. Recovery reconstructs the private containers from the same
stored nodes. Transaction rollback restores its shared before-image; later
writes to either graph detach from other readers and snapshots.

This is not an O(1) first-write claim. The first page write can copy O(N/128)
page references, and a key write copies the keys in its hash shard. The expected
shard size is N/256, with randomized hashing rather than a worst-case bound.
Statistics collection and reverse-edge discovery still scan graph state.
Dirty-slot and metadata bookkeeping also remain part of a snapshot. The public
cache `memory_bytes` value remains an estimate of logical graph ownership;
it is not a measurement of physical memory uniquely retained by one reader.
The dedicated 100,000-row/384-dimension before/after comparison in #681 remains
separate acceptance work.

Functional verification uses reference-model edits across a 128-slot page
boundary, 257 small keys to guarantee a shard collision, pointer sharing and
clone counts, and existing bounded HNSW save/reopen/rollback cases. These tests
have no timing, RSS or throughput threshold. Bulk/scale diagnostics remain
outside this focused run and outside normal CI additions.
