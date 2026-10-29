# Atomic WAL overflow: format and recovery contract

Status: #682 provides the standalone codecs, low-level WAL v3 APIs and explicit
native `LsmKV::open_with_overflow` integration. Default `open_with_config` keeps
WAL v2 and the v0.1 container writer. The explicit public Windows path now uses
the existing write-through file/directory publication and recovery pipeline.
Native Windows public-API qualification is still pending; the historical
internal-only results below do not establish that qualification. This is not
release acceptance for #682.

The public-path change also selects the small Database/SQL/Python overflow,
budget and cancellation cases on Windows. LSM lifecycle, inline recovery and
publication-boundary tests enter through `open_with_overflow` on both platforms.
Invalid budgets must still fail before creating ancestors or touching an
existing file. Ordinary builds contain no failure-injection controls; the
manual instrumented Database/SQL/Python harnesses are now Windows-capable,
but their Windows results and independent legacy-reader checks remain required
before this change is accepted. No load/performance test is added to ordinary CI.

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
ring reservation, then syncs and durably publishes the spool (rename plus
directory sync on Unix; write-through move on Windows), appends the reference
and forces WAL sync even under `NoSync`. Uncertain reference I/O
retains the spool and prevents further writer mutation until recovery/reopen.

The writer/activation primitives require durable file publication. Windows uses
synced closed files and same-volume `MoveFileExW(MOVEFILE_WRITE_THROUGH)` in
already durable, exclusively owned parent directories. Missing ancestors are
published parent-first through the shared directory publication helper. The
legacy container directory-sync no-op is not used as durability evidence.
Explicit v3 read/recovery uses the same public opt-in on both native platforms;
Windows public-API qualification remains a separate integration requirement.
Directory budgets count logical file lengths (including framing and temporary
files); filesystem block/metadata overhead needs separate host headroom. The
inline limit bounds encoded record bytes; decoded key/value and operation-vector
overhead and the caller's private recovery state must also fit its memory budget.

`activate` requires all old WAL handles closed and the container fence already
durable. It reserves a separate full-file temporary budget, stream-copies a v2
WAL and changes only its segment versions/CRCs before sync and durable publication.
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
| Committed | Durable inline batch or ring reference | Replay the inline batch, or validate its complete spool, once in LSN order |
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
On Windows, the shared publication helper creates those ancestors parent-first
using write-through moves. Invalid budgets still fail before filesystem effects.

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

Small opt-in commits can use the existing v3 inline batch format. The operation
stream must be at most 1 KiB, fit the configured inline recovery limit, and fit
the current free ring while leaving room for one 53-byte reference. The stream
length conservatively bounds the varint batch encoding within this size range.
This bounds the extra codec copies and avoids spool file/directory I/O for small
writes. All existing operation/spool/replay budgets still apply. Inline commits
sync the WAL before publishing MemTable changes, including NoSync/BatchSync
configurations; uncertain append/sync errors poison the writer until recovery.
Replay charges inline versions exactly as it charges referenced operations.

Other nonempty opt-in commits stream their borrowed write set into one spool and
force-sync one 53-byte WAL reference before publishing the MemTable changes.
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

An uncertain inline or reference write poisons the WAL. Checkpoint and converge inspect
that health before publishing metadata, including converge on Drop. Otherwise a
checkpoint could incorrectly cover the uncertain commit while its values were
absent from memory. Reopen resolves that outcome through the WAL records.

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
Capacity limits return `alopex_core::Error::WalCapacityExceeded { reason }`.
Downstream exhaustive matches on the public core `Error` enum need a new arm.
The variant is the Rust contract; the diagnostic `reason` is human-readable and
must not be parsed. This covers operation key/value/payload, spool, directory,
replay, activation/fence and WAL-ring limits. SQL preserves the core source and
reports `ALOPEX-S004`; Python `AlopexError.code` uses the same code for direct KV,
open and SQL failures. Actual filesystem failures, including OS disk-full, remain
`Io`; malformed configuration/data remains `InvalidFormat`. Allocation failure
remains `Io(OutOfMemory)`. A caller must not assume retrying the same input without
changing its budget can succeed. Failed puts preserve the retained write set;
SQL statement failures follow the existing transaction rollback contract.

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

The following implementation notes record staged qualification through #739.
References below to a gated public API describe those earlier source revisions.
The current public-path change and its outstanding qualification are stated at
the start of this document; earlier internal results are not substituted for it.

Windows LSM writes are still rejected before I/O, including the public Database
and Python opt-in. `WalOverflowOptions.writes_supported()` reports this explicitly.
Low-level WAL v3 create/open/append/activation now use write-through file moves.
They require pre-existing durable parent/spool directories and exclusive ownership.
Creation stages one complete synced WAL in `.overflow-create`, closes its handle,
and publishes without replacement; a stale temp blocks creation without deletion.
Spool sealing and v2-to-v3 activation close source/output handles before replacing
the destination. Failed publication never acknowledges a commit. These primitives
do not create directories or qualify the public database writer. Supporting Windows
LSM requires a single reviewed publication protocol for spool sealing, container fencing,
checkpoint metadata, SST publication, WAL activation and newly created ancestors.
The current Windows container directory-sync helper is a no-op; removing the
opt-in refusal alone would incorrectly assert durability across those boundaries.

Microsoft documents `MOVEFILE_WRITE_THROUGH` for [MoveFileEx](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)
and [directory moves](https://learn.microsoft.com/en-us/windows/win32/fileio/moving-directories).
The low-level WAL uses that building block with `MOVEFILE_REPLACE_EXISTING` only
for spool/upgrade publication, never for fresh WAL creation. No cross-volume copy
is allowed. Paths use the canonical parent plus a UTF-16 filename, preserving
Unicode/long paths and rejecting embedded NUL. The same private storage helper
also publishes LSM SSTables, checkpoint metadata and finalized containers. Each
caller syncs the complete file (including metadata), closes its handle and then
publishes; Unix callers retain their parent-directory sync boundaries. This does
not prove that ordinary `create_dir_all`, container fencing/retry or sidecar
pruning satisfy the complete overflow protocol. Before
activation, validate each boundary on Windows with tiny fixtures, including nested
new directories, replaced files, open-handle sharing, injected publication failures
and process termination. No Windows host validation is claimed by Unix tests.

The container fence and restored SSTables also use this publication helper.
After an interrupted fence publication, Windows repeats the reserved copy and
write-through replacement even when the visible header is already v3. The copy
budget therefore applies to retries as well. It never flushes a read-only file
handle (Windows requires write access for
[FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)).
Unix retains the existing file/parent sync shortcut for an already fenced file.
These existing-directory file operations do not remove the public Windows
overflow guard.
Power-loss guarantees must be justified separately from process-crash tests.

Windows LSM directory creation now stages each missing empty directory beside
its destination and publishes it with a non-replacing write-through move, parent
first, before writing children. The first existing ancestor is assumed durable.
Lock-parent creation, sidecar/SST creation and rehydration use this helper. A
stale staging directory blocks creation and is not removed; a failed publication
cleans only the empty staging directory created by that call. If a move became
visible before an error, the published parent is retained and children are not
created until retry. This does not flush arbitrary pre-existing directory entries.

Windows sidecar pruning retires `lsm.wal` to `lsm.wal.pruned` with a
non-replacing write-through move before deleting metadata or SSTs. A sharing
failure or existing retirement name leaves the live WAL and data intact. Once
retirement succeeds, remaining debris is classified as dead even if subsequent
deletion fails. Reopen must remove the entire dead sidecar before rehydrating;
cleanup failure propagates instead of publishing a new WAL beside stale SSTs.
The existing sidecar owner lock covers this sequence. This adds no format or
new live-sidecar marker: the presence of `lsm.wal` remains the deciding artifact.
Unix keeps its unlink/parent-sync prune boundary. Internal Windows activation
and GC use the protocol below; public opt-in remains Unsupported pending full
qualification. Native execution and power-loss qualification are separate.

The portable read-only recovery tests define the same 3-byte/two-fragment fixture
for all native targets, including Windows: successful replay without file
mutation, missing referenced spool, and truncated referenced spool without prefix
delivery. They construct a v3 fixture through private test helpers; they do not
enable or qualify a production Windows writer. The test directory contains a
Unicode component and all fixture writer handles close before reader open.
The Windows build job runs these three reader tests, 27 shared low-level writer
and recovery tests, and six Windows publication/LSM-refusal cases after its
existing release build, reusing the same target directory. Seven additional native
LSM publication cases cover a 24-byte checkpoint replacement and a failure
injected after sync, one key/value flushed to an SST and reopened with no WAL replay, a
3-byte container replacement, one-key container-to-SST restoration, Windows sharing failures, and public LSM overflow
refusal without creating ancestors or changing existing files. Windows LSM
overflow remains Unsupported until all publication boundaries are implemented
and qualified; successful legacy-LSM file tests do not remove that guard.
Seven separate fence cases exercise an exact copy budget, one byte below the
required budget, failures before/after publication, stale temporary ownership,
and Windows retry sharing/budget failures. The native Windows harness requires
83 passing cases in total: the previous 63 plus seventeen overflow garbage cases
and three internal overflow lifecycle cases. The earlier 63 include eight directory
publication cases and five legacy LSM lifecycle cases (nested commit/prune/reopen,
cleanup error propagation, and three Windows prune failure/retry cases). Python additionally verifies that an unsupported
overflow open neither creates nested ancestors nor modifies an existing
3-byte file, and uses one KV pair and one SQL row in separate Windows tests for
nested creation, pruning and reopen. These checks do not prove crash or power-loss durability.
Fixtures contain
3-byte payloads or a 65-byte payload just beyond a 64-byte ring, not workloads. It
requires nonzero expected test counts and saves SHA/tree, raw logs and native
allocation measurements. The watchdog measures before/during/after Cargo, stops
at 40 GB host use or 20 GB additional use, and kills the owned process tree on
failure. Windows inventory uses `FILE_STANDARD_INFO.AllocationSize`; inability
to measure fails the job. Cache restore is disabled for this Windows job and
build outputs are removed after CLI artifact upload. This is a native functional
check on an isolated hosted runner, not a load or power-loss test. Only an actual
successful run on the exact source qualifies as Windows runtime evidence.

The remaining writer decision must cover both renames and directory creation.
[FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
requires a writable file handle; [File Caching](https://learn.microsoft.com/en-us/windows/win32/fileio/file-caching)
separately describes cached metadata and explicit flushing. This is insufficient
evidence to treat the existing no-op directory helper as durable. Do not enable
Windows LSM by merely replacing the spool rename or removing the platform guard.

### Windows internal activation and spool retirement

Internal activation now uses the parent-first directory publication described
above; it checks the resulting directory paths rather than treating a skipped
directory flush as success. This internal path is exercised separately from the
public API, whose Windows `Unsupported` guard remains enabled.

Only the exclusive owner, after a durable checkpoint/WAL reclamation or complete
recovery reconciliation, can retire unreferenced canonical spool/temp files:

1. Publish a non-replacing, write-through move to
   `.gc-<16 lowercase hex reserved bytes>-<original canonical name>`.
2. Truncate the retired file to zero, sync it, and close the write handle.
3. Publish a non-replacing, write-through move to the same name with reservation
   zero, then remove that empty entry. A prior canonical zero-reservation entry
   may be removed before this move only if it is a regular, zero-length file;
   this lets GC finish after an uncommitted LSN is reused. Occupied or non-regular
   destinations still block publication and remain untouched.

Directory accounting charges the greater of actual length and the encoded
reservation. A failed truncate or flush keeps the prior reservation. A failed
zero-reservation move may leave either name visible: the old name stays charged,
while the zero-reservation name refers only to already synced empty data. A retry
can finish either state. If a crash restores the old reservation name, accounting
remains conservative. A remaining zero-reservation file is empty and can be
removed by a later attempt. Recovery recognizes canonical retirement names
because their original files were proven unreferenced before the retirement
move; it preserves unknown names and continues to charge their actual bytes.
Malformed names and retirement files larger than their recorded reservation
are refused by the Windows remover. No payload copy or whole-directory file list
is required. These are logical byte reservations, not allocated-block or RSS
limits. Unix retains its unlink plus strict directory sync and understands the
retirement accounting when opening files moved from Windows.

Tiny tests cover each interruption boundary, non-replacing publication failure,
reservation parsing/accounting, Windows sharing failure, and the internal LSM
activation/commit/checkpoint/recovery sequence with one key and a 40-byte value.
Portable state-machine tests do not establish power-loss durability. Native
Complete public API qualification and process/power-loss boundaries remain
separate work. Internal Windows tests do not qualify the public API.

### Interrupted activation and fence copies

Under the exclusive database owner lock, LSM open reconciles an existing
`.overflow-upgrade` WAL copy or `.overflow-fence` container copy before retrying
publication. The authoritative file remains the source of truth. Recovery only
removes a regular staging file whose contents are a prefix of that source, allowing
bytes from the permitted v2/v3 WAL header (including its CRC) or container-version
rewrite. This handles interrupted copies and partially rewritten headers, as well
as an old redundant copy left after publication. Staging data is never promoted.

Comparison uses three fixed 1024-byte buffers. Both file lengths must fit the
configured activation/fence copy budget; excess returns `WalCapacityExceeded`.
Staging longer than the source, foreign payload bytes, non-regular paths and
aliases of the authoritative file are refused and preserved. Opened file identities
are compared before truncation, including hard links. Filesystem errors propagate;
an unavailable inventory is not interpreted as an absent file. Low-level WAL
`activate`/`create` retain their existing refusal of a pre-existing staging path;
automatic reconciliation belongs to the locked LSM owner.

After the complete comparison succeeds, recovery truncates staging to zero,
flushes and closes it, then unlinks it. Unix also syncs the parent directory.
On Windows, a deletion interrupted after the data flush may leave an empty prefix
for the next open to reclaim. A sharing failure is reported and can be retried
after the blocking handle closes. Recovery does not allocate another full copy
or recursively delete paths. Unknown staging requires operator investigation;
callers must not delete it automatically merely because open failed.

The public Rust `LsmKV::open_with_overflow` path on supported platforms exercises
this reconciliation on reopen and preserves committed values. The public
Windows overflow guard remains enabled: internal recovery coverage alone does
not establish the Database/SQL/Python failure, cancellation and legacy-reader
contracts. Native Windows CI checks the shared recovery helper, WAL header
reconciliation, fence retry and internal lifecycle with small independent fixtures;
it does not run load or performance tests.

## Public API failure and cancellation scope

On supported platforms, the public embedded Rust and Python regression cases
stage one 65-byte value (one byte beyond the 64-byte ring payload), then explicitly
roll back or drop the transaction. They compare spool statistics before and after
the cancellation and reopen the database to check the previous committed value
and absence of the pending key. Python also checks a caller exception in a
transaction context. These cancellations occur before synchronous commit begins.

The Python SQL-stream case stages a second row in a two-row table with primary
and unique keys. Cancelling the transaction's SELECT stream makes commit fail
with `stream_abort_required`. After rollback and reopen, only the prior row exists,
and both keys from the cancelled row can be reused. This covers the owned session
boundary rather than treating a successful standalone SELECT cancel as write
transaction acceptance.

Missing spool-directory inventory is an I/O error. A foreign regular file at
that path is a format error and remains untouched. Public commit fails in either
case, and restoring the original directory permits reopen with the previous
commit intact. Rust asserts the distinct error variants; Python exposes the
existing generic `ALOPEX-PY999` for both, rather than the capacity code
`ALOPEX-S004`. Existing separate capacity cases exercise failed KV and SQL writes.
This does not inject failures after a spool or WAL reference becomes durable.

`scripts/compatibility/wal_overflow.py` separately exercises a published old
Python writer, candidate opt-in upgrade, old-reader rejection of the fenced
container without byte changes, and candidate reopen. It uses one old key and
one new 65-byte value, records native module and file hashes, and checks the old
bytes before candidate open. The baseline run uses published 0.8.16, not a
candidate writer emitting an old-looking header. This closed-container case
does not establish recovery of an independently produced live legacy WAL.

The separate manual `scripts/compatibility/live_wal.py` driver uses a published
0.8.16 core client to commit one key and exit without destructors. Its fixture is
only a 112-byte v2 WAL (64-byte ring payload), with no SST or checkpoint. A
separately built candidate client verifies recovery through both default and
explicit overflow LSM opens, writes one new 65-byte value, and reads both values
in two new processes. The actual old reader must reject v3 without changing data
file bytes. Receipts bind binary, client, lockfile and data-file hashes. This
native LSM scenario is separate from the Python closed-container check and is
not a claim about failure in the middle of publication or power-loss recovery.

Public Windows overflow remains unsupported. These results do not make a
synchronous commit interruptible. Process termination at publication boundaries,
power loss and public Windows acceptance remain distinct work.
The API tests do not certify a cancellation token for synchronous commit or
replace the format-level short-write, CRC and fsync-failure tests. None of these
functional cases is a load or performance test.

## Public LSM persistence-boundary acceptance

On supported non-Windows platforms, twelve small independent cases in
`lsm::overflow::publication_acceptance` call public `LsmKV` open, begin, put,
commit and scan APIs. Each starts with one committed key and stages two more
keys, including one 65-byte value, against a 64-byte ring payload. `EveryWrite`
remains enabled. Each boundary is exercised once with a returned I/O error and
once with subprocess exit without unwinding or destructors:

| Boundary | Recovery expectation |
| --- | --- |
| Spool written, before sync | Previous key only; temporary spool is removed |
| Spool synced, before publication | Previous key only; temporary spool is removed |
| Spool published, before reference attempt | Previous key only; unreferenced spool is removed |
| Reference attempt begins, before append | Previous key only; uncertain writer rejects reuse; reopen removes unreferenced spool |
| Reference appended and synced by `EveryWrite` | Both pending keys recover together, although the caller has received no success |
| Additional reference force-sync completed, before returning | Both pending keys recover together, although the caller has received no success |

After a returned error the pending keys have not entered the running store's
memtable. Once reference publication is uncertain, a second write must fail
with the explicit reopen-required error. Two fresh opens check exact key/value
sets, recovery record count, spool cleanup/retention and stable replay accounting.
An error after durable reference publication does **not** promise rollback.

The boundary hooks and process-exit action exist only in `cfg(test)` builds;
there is no runtime environment switch or new normal-build public API. The
ignored child entry is invoked explicitly by each parent case and is not an
unexecuted acceptance scenario. Each child has a 30-second deadline. There are
no volume, timing or performance assertions, and no timing-based fault races.

This qualifies the public native LSM layer, not the embedded Database/SQL/Python
layers or Windows public overflow. An error injected next to a sync call is
not an actual filesystem short write or fsync failure. Process exit does not
simulate lost kernel caches or power failure. Those failure modes and lower-level
codec corruption tests retain separate acceptance evidence.

## Delivery and acceptance

1. Versioned spool framing, bounded encoder/validator and corruption fixtures. Keep live v2 writer behavior unchanged. Review the exact layout and validation limits before integration.
2. Explicit activation, ring commit references, streaming writer/recovery, budget accounting and checkpoint/converge reclamation.
3. Rust/Python options/statistics/errors, early write-set limits, public SQL rows/index/journal recovery and failure/cancellation acceptance.

Ordinary CI uses a few keys, a ring of a few hundred bytes, and two or three fragments. Split missing/truncated/CRC/ordinal/version/limit cases. Inject short writes, read errors and sync failures at individual boundaries. Test a committed reference with a broken spool separately from an uncommitted truncated tail. Include old-format reopen and rejected new-format reads with the legacy reader. Do not add load or performance execution to CI.

Manual comparison uses identical API, data shape, durability and source identities. Report input preparation, index building, commit time, peak memory, ring bytes and spool bytes separately. Retain compact evidence and delete generated outputs under the per-host budget.
