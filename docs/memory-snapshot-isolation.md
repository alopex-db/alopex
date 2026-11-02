# MemoryKV snapshot versions

MemoryKV transactions read the committed state at their start version, overlaid
with their own pending writes. Updating, deleting or reinserting a key in another
transaction does not change that view. Borrowed and owned transactions use the
same version selection for point reads and prefix/range/from scans.

Each transaction registers its start version while holding the map lock. A commit
keeps an overwritten value only when an active snapshot can select it. Deletion
markers also preserve conflict detection for older writers. Transaction completion
and drop reclaim versions that no remaining reader can select. Reclamation visits
the keys with history or deletion markers, rather than copying the database.

The capacity check includes historical values, version identifiers, deletion
markers and the duplicate keys in the reclamation index, in addition to current
key/value bytes. These are logical storage bytes under the existing MemoryKV
accounting convention, not a measurement of allocator overhead or process RSS.
The committing transaction does not retain its own old values merely to complete
its commit. A capacity failure occurs before the WAL or visible map is changed.

Recovery still materializes SSTable values into the map and applies committed WAL
changes. Reads use that authoritative map; an older SSTable must not resurrect a
key deleted after the last flush. Flushing writes only the current live values.

`MemoryTxnManager::clear_all()` remains an administrative in-memory reset, including the
concurrent crash-simulation use. It switches generations and clears accounting;
old transactions and owned cursors return `TxnClosed` instead of observing the new
generation or publishing old staged writes. Dropping them cannot alter the new
generation's reader counts. Ordinary SQL `DELETE`/`TRUNCATE` use transactional
changes and preserve old readers' rows.

Reset invalidates the old cursor gate and wakes its waiters. A guard from the old
generation cannot decrement the new generation's reader count or release its
writer. The bounded reset regression retains the old cursor while a new writer
finishes, and fails within five seconds if the stale gate blocks progress.

The focused regression suite uses one to three keys. Its reference model has at
most eight mutations and three live generations per API. The embedded suite uses
two rows with two-dimensional vectors. Windows runs these same bounded cases;
the manual 100,000-row HNSW performance workload remains outside normal CI.

Implementation and acceptance are tracked in [#748](https://github.com/alopex-db/alopex/issues/748),
with the release process in [#690](https://github.com/alopex-db/alopex/issues/690).
