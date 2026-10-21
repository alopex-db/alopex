# Atomic WAL overflow: format and recovery contract

Status: the first stage for #682 implements the standalone `lsm::wal::spool`
codec described below. The current writer remains WAL v2 and still rejects a
single entry larger than its ring. Overflow commits are not enabled. Activation,
commit references, database recovery and public configuration below are the
contract for the remaining integration stages.

## Commit boundary

Use a bounded immutable spool plus a small ring reference. A sealed spool is only prepared data. It becomes committed only when its reference is appended and the WAL is synced. Keep snapshot writer gate → commit lock → WAL lock. Checkpoint prior commits before allocating this transaction's LSN.

Reserve space before starting the spool: account for committed spools, temporary spools, framing, and any format-activation temporary. Enforce transaction encoded-byte, write-set memory, fragment-buffer and total-spool-disk budgets independently. Existing public config structs keep their fields; introduce options/builders with private fields and expose the same contract in Python.

Use checked size accounting when a put/delete enters or replaces a write-set item. A known limit violation must not wait until commit or after expensive index work. The encoding stream must avoid cloning the entire write set or concatenating all encoded bytes. Keys/values may cross physical fragments, so a fragment limit is not a hidden value-size limit.

## Spool identity and framing

A spool header contains magic, exact format version, zero reserved bits, a random 128-bit transaction ID, nonzero commit LSN, exact payload length, fragment payload bound, fragment count and header CRC. All integer fields have explicit little-endian widths. Ring references use a fixed descriptor, never a pathname from a file.

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
encoding of the future ring reference is not part of this stage.

`write_spool` reads exactly the declared payload, rejects extra input and does not
return a descriptor for incomplete output. It does not fsync or publish a file.
`verify_spool` validates the remaining stream through EOF without delivering data.
`read_verified_spool` verifies first, seeks to its starting position, then validates
again while delivering one fragment at a time. The caller must position a complete
spool at its header; a key/value decoder must not assume fragment boundaries match
operation boundaries.

Lengths are validated against independently supplied limits before allocation. Verification holds one fragment buffer, not a whole-batch Vec. A first pass validates every fragment and the seal. A second pass on the same exclusively owned immutable file decodes operations into a private recovery MemTable. Do not publish the database until every committed reference has completed. On second-pass I/O failure, fail open and discard private state.

## Durable states and recovery

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

## Delivery and acceptance

1. Versioned spool framing, bounded encoder/validator and corruption fixtures. Keep live v2 writer behavior unchanged. Review the exact layout and validation limits before integration.
2. Explicit activation, ring commit references, streaming writer/recovery, budget accounting and checkpoint/converge reclamation.
3. Rust/Python options/statistics/errors, early write-set limits, public SQL rows/index/journal recovery and failure/cancellation acceptance.

Ordinary CI uses a few keys, a ring of a few hundred bytes, and two or three fragments. Split missing/truncated/CRC/ordinal/version/limit cases. Inject short writes, read errors and sync failures at individual boundaries. Test a committed reference with a broken spool separately from an uncommitted truncated tail. Include old-format reopen and rejected new-format reads with the legacy reader. Do not add load or performance execution to CI.

Manual comparison uses identical API, data shape, durability and source identities. Report input preparation, index building, commit time, peak memory, ring bytes and spool bytes separately. Retain compact evidence and delete generated outputs under the per-host budget.
