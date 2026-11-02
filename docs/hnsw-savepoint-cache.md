# SQL HNSW graphs at savepoints

Owned embedded transactions, including SQL sessions and Python transaction
handles, retain the SQL HNSW entries present at each savepoint. Each entry has
an independent graph handle backed by copy-on-write pages and payloads. Its
staged undo state, dirty flag, cache origin, last access and mutation history
are preserved. Sharing the mutable graph handle itself would allow later
writes to change the saved version.

`ROLLBACK TO` restores core KV and catalog state first. Direct HNSW graphs keep
their existing reload-and-fail-closed recovery. Only after that succeeds are
SQL graph entries restored from the selected savepoint. The restored entries
are forked again because that savepoint remains usable for another rollback.
Graphs first loaded after the savepoint are not restored; descendants are
removed by the existing savepoint lifecycle. `RELEASE` removes the retained
versions with the corresponding savepoints.

Current cache limits are applied when capturing and restoring SQL entries.
Already captured snapshots retain their graph versions until the savepoint is
released, successfully committed/rolled back, or dropped; lowering a cache limit does not rewrite
existing snapshots. Copy-on-write storage reduces duplicate payload retention,
but savepoints are not free: maps, metadata, undo sets and changed pages consume
space. `fork_owned` still computes graph statistics, so this change does not
claim constant-time savepoint creation or rollback. Statistics scans remain a
separate #681 follow-up.

Mutation history is essential at rollback. A graph changed before the savepoint
still includes uncommitted data, even when its dirty flag has been cleared by
flushing to the transaction. It must remain ineligible for global publication
on transaction rollback. An unmodified cold load remains eligible under the
existing epoch guard.

Focused cases use two rows and two dimensions per index. They cover warm/cold
Memory and LSM stores, nested and repeated rollback, index drop, TRUNCATE, NULL
updates, failed SQL, publication eligibility, lowered limits and graphs first
loaded after the savepoint. These are functionality tests, not scale tests.
