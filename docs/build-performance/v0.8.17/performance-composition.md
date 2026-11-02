# Performance and snapshot composition for v0.8.17

This composes the adopted #774 and #776–#790 performance stack onto #792's
MemoryKV/HNSW and Windows public-overflow integration. Product source
`80cfd8a77dc1e674b0ca9955cb131df34b037f4d` / tree `1b8efadd0a656b148b6c87d4311ef76782f6581a`. Candidate acceptance is pending.

## Reconciled behavior

The two branches conflicted in `MergedScanIter::advance_data`: the performance
branch used the former `(value, version)` representation, while #748/#750 retained
historical values and deletion markers. The composition uses `VersionedValue::at`
to select the reader's version, skips visible tombstones, and clones key/value only
after bounds and visibility checks. Borrowed peek comparisons and conditional
read-set key copies remain. Read tracking uses the selected historical version.
The existing unsafe guard bridge is unchanged.

The #790 scan model deliberately described the former behavior where later
overwrites hide a key. That expectation is no longer valid after #748. Its oracle
now uses the initial snapshot plus local writes, independently of future updates
or deletions. The 24-case property stays bounded (16 seed / 8 later / 12 pending
entries, two-byte keys and one-byte values); the deterministic fixture uses at
most 26 keys. No timing, load or performance test was added to normal CI.

Of 77 base-relative files, 75 match #790's blobs exactly. The other files are
`memory.rs` above and `lsm/mod.rs`, whose performance patch is unchanged after
normalizing diff offsets and which retains #791's Windows support. Existing
HNSW large diagnostics remain manual-only. Original performance reports remain
bound to their original sources, not relabeled as composed-candidate results.

## Development verification

- Core memory unit tests: 27 passed, including the updated reference model.
- Snapshot overwrite/delete/reinsert/GC/capacity/reset/recovery integration: 20 passed.
- Public Database HNSW transaction snapshots: 5 passed, including NULL-update and
  TRUNCATE commit with old readers. Total: 52 named cases, zero failed/ignored.
- Changed-line cargo-mutants: 8 generated, 3 caught, 5 compile-unviable due to
  denied unused/dead-code warnings; zero missed/timeouts. Unviable is not detection.
- Two manual negative controls (future-version visibility and current-version
  read tracking) each fail the fixed model as expected. Source hashes were restored;
  both model cases pass again after restoration.
- Core library/tests Clippy `-D warnings`, workspace rustfmt and diff checks passed.
- Scoped KV coupling ran before mutation: summary critical0/high1/medium4, two
  existing trait/implementation cycles. The hotspot renderer marks cycle entries
  Critical; it is not the same as the summary count. Existing KV abstraction and
  version ownership are retained; optional private module extraction remains a
  future cleanup. Runtime order, implicit meaning, macros/cfg and type-resolution
  blind spots are retained in JSON. File-mode analysis initially failed with ENOENT;
  the KV-directory report succeeded. This is not a new workspace-wide analysis.
- Miri is unavailable in the installed toolchain; it was not executed. No new
  unsafe code. No new Kani run; bounded reference properties cover the changed path.

The initial parser setup expected installation sidecars absent from the release
archive and stopped before any build. Sidecars were then derived from the verified
BUILD_IDENTITY and library hashes. Public v0.8.16 contract0.28 parser is an explicit
development override, not v0.8.17 package qualification. The initial formatting
check requested one model-expression wrap; the corrected source passed.

## Resources and remaining work

Serialized shared-target validation, debug/incremental disabled, jobs2, declared
additional bound3GB. Observed host peak 19,397,812,224 bytes;
after cleanup 19,348,492,288 bytes. Owned validation executables and
parser/archive were reclaimed after recording identities. Compact logs and shared
Cargo dependencies are retained for active release work; existing worktrees remain.

No new performance run was performed. HNSW index-build +9.87%, memory-scan ordered
phase +11.40%, and other adverse observations in the individual reports remain
unresolved; no universal speedup or no-regression claim is made. Required PR and
candidate CI, other adopted fixes, frozen-candidate guards/comparisons, RC/package
qualification, publication, postrelease verification and main merge remain under
#690. Next: integrate #791 then #792 after their required CI, retarget this PR and
verify the exact candidate composition. Owner: Codex.

Machine-readable source, outcomes and cleanup: `performance-composition.json`.
Compact local evidence: `/private/tmp/alopex-performance-composition`.
