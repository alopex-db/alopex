# Atomic WAL overflow: format and recovery contract

Status: #682 provides the standalone codecs, low-level WAL v3 APIs and explicit
native `LsmKV::open_with_overflow` integration. Default `open_with_config` keeps
WAL v2 and the v0.1 container writer. Windows durable publication, dedicated public
capacity errors and full public acceptance remain follow-up work. This is not release acceptance for #682.

## Commit boundary

Use a bounded immutable spool plus a small ring reference. A sealed spool is only prepared data. It becomes committed only when its reference is appended and the WAL is synced. Keep snapshot writer gate → commit lock → WAL lock. Checkpoint prior commits before allocating this transaction's LSN.

Reserve space before starting the spool: account for committed spools, temporary spools, framing, and any format-activation temporary. Enforce transaction encoded-byte, write-set memory, fragment-buffer and total-spool-disk budgets independently. Existing public config structs keep their fields; introduce options/builders with private fields and expose the same contract in Python.

Use checked size accounting when a put/delete enters or replaces a write-set item. A known limit violation must not wait until commit or after expensive index work. The encoding stream must avoid cloning the entire write set or concatenating all encoded bytes. Keys/values may cross physical fragments, so a fragment limit is not a hidden value-size limit.

## Spool identity and framing

A spool header contains magic, exact format version, zero reserved bits, a nonzero 128-bit transaction ID, nonzero commit LSN, exact payload length, fragment payload bound, fragment count and header CRC. All integer fields have explicit little-endian widths. Ring references use a fixed descriptor, never a pathname from a file.

Each fragment carries ordinal, payload length and CRC covering its ordinal/length/payload. The expected fragment count and final-fragment length derive from the exact payload length. Reject empty intermediate fragments, reorder, duplicate, omission and extra trailing bytes. The seal's whole-stream CRC binds the header identity/count/length and every complete fragment. The reference binds the expected identity and checksum; valid standalone spool bytes alone cannot authorize replay. CRC32 detects accidental corruption; it does not authenticate a file against an adversary who can rewrite both it and the reference.

The standalone codec uses this little-endian format, version 1:

| Record | Bytes and fields |
| --- | --- |
| Header (56 bytes) | `ALPXOVFL` (8), version u16, zero flags u16, transaction ID (16), LSN u64, payload length u64, fragment bound u32, fragment count u32, CRC32 over preceding 52 bytes |
| Fragment | ordinal u32, payload length u32, CRC32 over ordinal/length/payload, then payload |
| Seal (16 bytes) | `ALPXSEAL` (8), CRC32 over the header and all complete fragments, CRC32 over preceding 12 seal bytes |

Payload length and identity/LSN must be nonzero. Fragment count must fit u32. The caller
supplies separate payload/file/buffer budgets; the codec also caps a fragment at
1 MiB. File reservation is `56 + payload + 12 * ceil(payload / fragment) + 16`,
computed with checked arithmetic. The descriptor stores identity, payload length,
fragment bound and whole-stream CRC, independently of the spool bytes. Wire
encoding of the ring reference is specified below.

The v3 ring reference is exactly 53 bytes: LSN u64, record length u32 (41),
entry tag u8 (3), reference version u16 (1), zero flags u16, transaction ID
(16), payload length u64, fragment bound u32, spool whole-stream CRC u32,
and record CRC u32 over the preceding 49 bytes. All fields are little-endian.
Spool filenames derive from transaction ID and LSN; the record carries no path.

Native `create_overflow`/`open_overflow` explicitly opt in to v3. `append_spool`
requires an exclusively owned, durably created directory and increasing LSNs.
It preflights independent per-spool and total-directory budgets plus the 53-byte
ring reservation, then syncs the spool, renames it, syncs the directory, appends
the reference and forces WAL sync even under `NoSync`. Uncertain reference I/O
retains the spool and prevents further writer mutation until recovery/reopen.

The writer/activation primitives require real directory sync. Windows currently
returns `Unsupported` before creating or modifying files, because the existing
container directory-sync helper is a no-op there. Explicit v3 read/recovery is
available. Implement and verify Windows durable publication before enabling
database overflow on that platform; the no-op is not durability evidence.
Directory budgets count logical file lengths (including framing and temporary
files); filesystem block/metadata overhead needs separate host headroom. The
inline limit bounds encoded record bytes; decoded key/value and operation-vector
overhead and the caller's private recovery state must also fit its memory budget.

`activate` requires all old WAL handles closed and the container fence already
durable. It reserves a separate full-file temporary budget, stream-copies a v2
WAL and changes only its segment versions/CRCs before sync/rename/directory sync.
It preserves the ring and checkpoint metadata. This primitive does not fence
the container itself and must not be used alone to activate a database.

`recover_overflow` scans with a fixed reference buffer, a separately bounded
inline-record buffer and one bounded spool fragment. It delivers inline records
or begin/fragment/end events into private caller state. Any error requires that
state to be discarded. Missing/corrupt referenced spools are hard errors; torn
ring tails return an explicit truncation position. Covered checkpoint LSNs do
not require their spool files. Orphans and temporary files still count against
disk limits and are not deleted by these primitives. Existing default v2
readers reject v3; even an explicitly opened v3 reader rejects legacy replay.

The shared inline decoder grows the batch only after each operation is validated;
it never reserves memory from the untrusted operation count. It validates key/value
lengths without unchecked cursor addition and rejects overflowing varints. Tiny
valid-CRC corrupt records exercise these checks; hostile lengths never require
allocating their declared sizes.

`write_spool` reads exactly the declared payload, rejects extra input and does not
return a descriptor for incomplete output. It does not fsync or publish a file.
`verify_spool` validates the remaining stream through EOF without delivering data.
`read_verified_spool` verifies first, seeks to its starting position, then validates
again while delivering one fragment at a time. The caller must position a complete
spool at its header; a key/value decoder must not assume fragment boundaries match
operation boundaries.

Lengths are validated against independently supplied limits before allocation. Verification holds one fragment buffer, not a whole-batch Vec. A first pass validates every fragment and the seal. A second pass on the same exclusively owned immutable file decodes operations into a private recovery MemTable. Do not publish the database until every committed reference has completed. On second-pass I/O failure, fail open and discard private state.

## Durable states and recovery

### Operation payload (explicit v1)

The native `wal::operations` codec supplies the operation stream used by the
explicit LSM overflow path. Its layout is independent of physical spool fragments:

- `ALPXOPS1` (8 bytes), then a little-endian `u64` operation count.
- For each operation: tag (`0` Put, `1` Delete), key length (`u64` LE), value
  length (`u64` LE), key bytes, value bytes. Delete requires a zero value length.
  Empty Put values and empty keys are supported. The empty stream has count zero.
- Exact length and count are required; unknown tags, future versions, trailing
  bytes and incomplete records are errors.

`OperationStream` borrows the ordered write set and preflights the full payload
and individual key/value limits before any spool I/O. Reading uses only a fixed
17-byte scratch header, without cloning the batch. `OperationDecoder` accepts
arbitrary fragments, checks lengths against independent limits and the remaining
payload before accumulating an operation, and never reserves memory from a count.
It holds one operation at a time, plus allocation capacity and the owned operation
delivered to the sink; it does not bound the sink's total retained memory. The LSM
integration separately charges the private recovery MemTable and incrementally
checks the live public transaction write set before retention (see below).

Every sink operation is provisional until `finish()` and the enclosing WAL/spool
recovery succeed. After any decoder or sink error, the decoder stays failed and
the caller discards the entire private sink. A completed operation or spool is
not permission to publish a partial database.

| State | Durable evidence | Reopen behavior |
| --- | --- | --- |
| Old format | Existing v1/v2 WAL/container | Read according to that version, preserving checkpoint watermark |
| Format activated | New reader requirement durable on WAL and container/sidecar entrypoints | Old software must explicitly reject before decoding new records |
| Preparing | Temporary spool, no reference | Uncommitted; reclaim under exclusive database ownership |
| Prepared | Complete synced spool and directory entry, no reference | Still uncommitted; reclaim |
| Committed | Durable ring reference | Validate complete spool and replay once, in LSN order |
| Checkpointed | All MVCC versions and checkpoint watermark durable | References at or below watermark need not replay; spool becomes reclaimable |
| Reclaimed | WAL start and spool directory updates durable | Retain the checkpoint state and account for any failed cleanup |

Write the spool to a unique temporary name, sync its contents, rename to a derived immutable name, and sync its directory before recording the ring reference. Preserve a spool if reference append/sync has an uncertain outcome; stop further commits on that writer until reopen resolves the WAL. Do not delete data that may have been acknowledged by durable media.

A referenced missing, corrupt or unsupported spool is a hard recovery error. The current ring reader's recoverable torn-tail result must not swallow such a failure. Unreferenced files are garbage only after a complete reference inventory and successful format validation under the exclusive owner lock. Never delete arbitrary neighboring files.

After checkpoint metadata is durable, reclaim only spools whose LSN is covered by that watermark. A failed checkpoint or failed directory sync must retain them. A failed unlink remains counted against the disk budget and reported in statistics. Apply the same rule to container convergence and sidecar removal.

## Format activation and compatibility

Do not merely increment WAL_FORMAT_VERSION: current migration would route every v2 WAL through a legacy rewrite and reset checkpoint metadata. Retain explicit v1/v2 readers, preserve the watermark, and test upgrade with existing SSTables and remaining WAL entries.

A new entry kind in a v2 file or an ignored flag cannot fence old readers. Before the first overflow commit, atomically publish and sync a version/capability that the old WAL reader rejects, and fence the formal container entrypoint as well. The current container manifest rejects a newer manifest version but does not reject an unsupported wal_format_version by itself. Design activation so each crash point leaves either the intact readable old state or an explicit new-reader requirement, never an old reader silently opening stale container data.

Keep non-overflow stores in the old format until activation. No downgrade of a database with new committed references. Document whether convergence retains the new requirement, and expose the format transition before the first write. Existing durability guarantees must not be weakened to enable overflow.

## Explicit LSM connection

`LsmKV::open_with_overflow(path, config, LsmOverflowConfig)` is an explicit native
opt-in. Existing `LsmKVConfig` fields are unchanged; overflow options have private
fields and budget builders. The owner lock precedes all restore/activation mutation. Before directory
creation, open records the first existing ancestors; new sidecar/container parent
entries are synced child-first up to those ancestors before v3 activation.
Windows returns Unsupported before opening or changing database files.

For a sidecar, activation first atomically stream-copies its valid container and
patches only the header version to **0.2.0**, syncs the file, renames and syncs the
parent. Only then may WAL v2 be atomically upgraded to v3. An absent/zero-byte
container can be initialized for an empty database; a legacy nonempty database
without a valid container must first be converged with its existing v2 reader.
An interrupted fence can leave a fenced v2 WAL; retry with explicit budgets is
supported. Existing fence/activation temp files are not overwritten.

`FileVersion::CURRENT` remains 0.1.0 for ordinary writers; `MAX_SUPPORTED` is
0.2.0 for readers. Old readers reject 0.2.0 before restoring a container, and
reject v3 when the live sidecar takes priority. Current default LSM open also
requires explicit overflow options for a fenced container. Converge preserves
both the 0.2 header and WAL v3 manifest; container-only rehydrate recreates v3.
The manifest WAL version may not exceed its container fence. There is no downgrade.

Each nonempty opt-in commit streams its borrowed write set into one spool and
force-syncs one 53-byte WAL reference before publishing the MemTable changes.
Snapshot gate → commit lock → WAL lock ordering is retained. Transaction IDs are
`ALPXLSM1` plus the little-endian LSN: database ownership and monotonic LSNs provide
local uniqueness; these IDs are not random or globally unique. Ring pressure can
checkpoint prior commits, without splitting the pending transaction.

Replay holds a private MemTable until every inline record, spool and decoder
finish succeeds. Its budget charges key/value bytes plus 128 bytes per operation,
including MVCC versions, and rejects before insertion. This is deterministic
accounting, **not a bound on process RSS**; allocator, fragment/decoder, inline
buffer and original write-set overhead require separate headroom. A commit above
the replay budget is rejected before WAL changes; accumulated pending charges
trigger a checkpoint before the next commit would exceed that budget. The counter
resumes from recovered MVCC versions and resets only after successful reclamation.

An uncertain reference write poisons the WAL. Checkpoint and converge inspect
that health before publishing metadata, including converge on Drop. Otherwise a
checkpoint could incorrectly cover the uncertain commit while its values were
absent from memory. Reopen resolves that outcome through the durable reference.

After durable checkpoint metadata publication and successful WAL reclamation,
checkpoint/converge collect canonical sealed spool names whose LSN is at or below
the checkpoint, and canonical abandoned `.tmp` preparations. Collection runs under
the owner/commit locks. Unknown names and later sealed files remain charged; the
collector never follows symlinks or recursively deletes directories. A cleanup
failure does not change the outcome of an already durable checkpoint: it increments
`gc_failures`, retains any undeleted files, and can be retried by checkpoint. A
failed inventory is an error, never zero usage. Directory pressure checkpoints
previous commits before reserving the next spool. A single spool larger than the
directory budget is rejected before that checkpoint.

Rust `DatabaseOptions::with_wal_overflow(limits, wal)` and Python
`Database.open(path, wal_overflow=WalOverflowOptions(...))` enable the same explicit
local-disk path, including SQL transactions. Reopen requires the opt-in again.
Defaults and the published `EmbeddedConfig` fields remain unchanged; memory mode
rejects overflow options. `wal_overflow_stats()` reports logical directory bytes,
replay charge, their limits, and cleanup failures since open. These counts do not
represent filesystem allocation or process RSS. Individually oversized Put/Delete
operations are rejected before entering either borrowed or owned write sets.
Borrowed and owned transactions also check the entire live write set before each
Put/Delete is retained: encoded payload (one stream header), framed spool size,
single-spool directory reservation, and key/value plus 128 bytes per entry. An
O(log n) lookup and constant-size delta replace the previous key's charge; an
oversized overwrite leaves both the data and counters unchanged. Savepoints save
and restore the counters together with the map; rollback clears them. The commit
stream independently rechecks the final map, including index/journal writes that
enter through Put/Delete. These logical limits do not include cloned savepoint
maps, caller buffers or allocator overhead and do not promise a process RSS cap.
A dedicated public capacity-error code remains outstanding.

After complete v3 replay and successful activation/SST discovery, open reconciles
canonical spool identities with every live post-checkpoint WAL reference. It
collects unreferenced sealed files and abandoned canonical tmp files, preserves
referenced and unknown-name files, and validates the whole directory before any
deletion. The temporary sorted identity index grows on demand, with at most
`replay_bytes / 128` entries (24 bytes per identity, bounded geometric allocation);
this also bounds foreign empty streams. Directory scanning is streaming, with
binary searches rather than a WAL rescan per file. A referenced-spool error or
index-budget error aborts before collection. A torn/CRC-invalid ring tail is
repaired durably but defers collection for that open to retain uncertain evidence;
a subsequent complete recovery may reclaim it. Cleanup failure increments
`gc_failures` and leaves undeleted bytes charged; a later open retries. Existing
directory budget checks still precede replay: cleanup does not bypass an
already-exceeded recovery budget.

### Windows publication prerequisites

Windows writes are still rejected before I/O, including the public Database and
Python opt-in. `WalOverflowOptions.writes_supported()` reports this explicitly.
Read-only low-level v3 recovery remains available. Supporting Windows requires a
single reviewed publication protocol for spool sealing, container fencing,
checkpoint metadata, SST publication, WAL activation and newly created ancestors.
The current Windows container directory-sync helper is a no-op; removing the
opt-in refusal alone would incorrectly assert durability across those boundaries.

Microsoft documents `MOVEFILE_WRITE_THROUGH` for [MoveFileEx](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)
and [directory moves](https://learn.microsoft.com/en-us/windows/win32/fileio/moving-directories).
That is a candidate building block, not proof that ordinary `create_dir_all`, file
replacement and checkpoint publication already satisfy this protocol. Before
activation, validate each boundary on Windows with tiny fixtures, including nested
new directories, replaced files, open-handle sharing, injected publication failures
and process termination. No Windows host validation is claimed by Unix tests.
Power-loss guarantees must be justified separately from process-crash tests.

The portable read-only recovery tests define the same 3-byte/two-fragment fixture
for all native targets, including Windows: successful replay without file
mutation, missing referenced spool, and truncated referenced spool without prefix
delivery. They construct a v3 fixture through private test helpers; they do not
enable or qualify a production Windows writer. The test directory contains a
Unicode component and all fixture writer handles close before reader open.
The current ordinary CI runs Rust library tests on Linux/macOS; its Windows build
job only builds release binaries. Windows execution of these fixtures therefore
remains pending a measured, bounded Windows test host/job. Merely adding a native
`#[test]` does not establish Windows runtime evidence.

The remaining writer decision must cover both renames and directory creation.
[FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
requires a writable file handle; [File Caching](https://learn.microsoft.com/en-us/windows/win32/fileio/file-caching)
separately describes cached metadata and explicit flushing. This is insufficient
evidence to treat the existing no-op directory helper as durable. Do not enable
Windows by merely replacing the spool rename or removing the platform guard.

## Delivery and acceptance

1. Versioned spool framing, bounded encoder/validator and corruption fixtures. Keep live v2 writer behavior unchanged. Review the exact layout and validation limits before integration.
2. Explicit activation, ring commit references, streaming writer/recovery, budget accounting and checkpoint/converge reclamation.
3. Rust/Python options/statistics/errors, early write-set limits, public SQL rows/index/journal recovery and failure/cancellation acceptance.

Ordinary CI uses a few keys, a ring of a few hundred bytes, and two or three fragments. Split missing/truncated/CRC/ordinal/version/limit cases. Inject short writes, read errors and sync failures at individual boundaries. Test a committed reference with a broken spool separately from an uncommitted truncated tail. Include old-format reopen and rejected new-format reads with the legacy reader. Do not add load or performance execution to CI.

Manual comparison uses identical API, data shape, durability and source identities. Report input preparation, index building, commit time, peak memory, ring bytes and spool bytes separately. Retain compact evidence and delete generated outputs under the per-host budget.
