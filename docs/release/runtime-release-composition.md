# Runtime and publication composition for v0.8.17

This composes #747, #754, #762, #770 (including #769), #772, #763
(including #764–#766), and #775 (including #767–#768) onto #793.
The tested product source is `5a29eb71dcfe4adecd0ab3ec138239c59cb90ae5`,
tree `42492114021991618ca949549a6853135c75a923`.
Candidate acceptance remains pending under #690.

## Preserved contracts

The composition includes bound kNN options and parser-source guards, HNSW
capacity-eviction/warning observations, VECTOR Parquet interchange, LSM
write-local before-images, and the manual public-API cost harness. Publication
changes include independent optional Intel qualification, primary target policy,
Core Latest identity checks, immutable report pairs, publication-failure records,
and bounded public-verification cleanup.

All seven pinned source heads are ancestors of the product commit. Of 80
base-relative files, 71 match a contributing source blob exactly. Seven other
files retain exactly the contributing source's added/deleted lines after removing
diff offsets: CI, LSM, embedded facade, SQL bridge, and three documentation files.
This retains the previously integrated Windows, MemoryKV snapshot and HNSW
behavior. Source identities and per-file results are in the adjacent JSON.

The only merge conflict was the public verification workflow's artifact paths.
Keep both Latest identity and failure/storage evidence: original JSON/Markdown,
`latest-release.json`, `availability.log`, vector-scenario log, and storage JSONL.
The new regression executes the real cleanup shell against tiny fixtures, checks
the real upload globs, and verifies all six files' original bytes survive.
No additional production Rust logic was introduced by conflict resolution.

The subsequent acceptance inventory found that the selective Windows runner did
not execute #770's before-image regressions. It now adds the existing borrowed
journal (4), owned journal (3), and public no-scan journal (1) cases, requiring
zero failures/ignored cases. The runner grows from 23 groups / 236 cases to
26 groups / 244 cases. These tests already passed on macOS in the source-bound
checks below; native Windows execution of the added groups remains pending.
Fixtures use at most three keys; the existing property has 24 cases with at most
16 operations. This routing change adds no new workload or production logic.

## Development verification

| Check | Result |
| --- | --- |
| Release Python contracts | 203 discovered: 201 passed, 2 host-specific skips |
| Parser wire/source contracts | 17 passed |
| Core journal / aggregate capacity / overwrite-savepoint | 12 / 4 / 1 passed |
| Public LSM before-images / WAL SQL journal | 1 / 2 passed |
| Embedded cache contracts | 30 passed |
| Cold publication / savepoint / transaction / two-index cache | 21 / 13 / 5 / 6 passed |
| Bound kNN / VECTOR Parquet public round trips | 6 / 4 passed |
| SQL COPY bulk contracts, including VECTOR conversion | 52 passed |
| Change-scope, release join, reporting shell contracts | All passed |
| Rustfmt, diff whitespace, four changed workflow YAML parses | All passed |

Rust total: 157 passed, zero failed/ignored. VECTOR public fixtures contain only
three rows, with 3/384 dimensions. The release suite skips native Windows path
aliases and native Linux x86_64 Docker parser builds on this Apple Silicon Mac.
Actionlint is unavailable. These host checks remain required in CI.

Two negative controls independently remove the Latest or storage artifact path;
the new test fails for each as expected. Exact workflow bytes were restored and
all four cleanup tests passed again. Those intentional failures are retained in
the receipt, separately from ordinary test outcomes.

Target: release workflow cleanup/upload boundary and composed runtime contracts.
Mutation check: two focused workflow negative controls caught; no new Rust
mutation run. The source-bound mutation/coupling results in the contributing PRs
are retained, not relabeled as composed-candidate evidence. No new Rust branching
or cross-module behavior was added during conflict resolution.
Survivors: none in the two controls; no claim about unrun mutations.
Strengthening: tiny cleanup-and-upload behavior regression plus focused native
integration checks. No new Kani/Miri run; Miri is unavailable in the installed
toolchain and this composition adds no unsafe code.

The verified v0.8.16 contract0.28 parser was used as an explicit development
override, with sidecars derived from the archive's BUILD_IDENTITY and library
hashes. This is not v0.8.17 package qualification. No performance/load test or
publication command ran. Existing performance reports keep their original
source/host identities and unresolved adverse observations.

## Resources and handoff

Builds were serialized in the shared Cargo target with debug/incremental disabled,
jobs2, and a declared additional-storage ceiling of 3 GB. The watchdog measured
before/during/after each command. Exact peak and cleanup measurements, commands,
log hashes and removed-executable identities are in `runtime-release-composition.json`.
Eleven native executables and the parser archive/extraction were removed. Compact
logs and shared Cargo dependencies remain for active release work, owned by Codex.
No worktree was created and preexisting user files/worktrees were preserved.

This is a Draft integration handoff, not candidate or release acceptance.
Required CI on #791–#793 and this composition, candidate integration and guards,
the remaining issue acceptances/performance comparisons, RC/package qualification,
publication, postrelease verification, and main merge remain open under #690.
The optional Intel release still requires its native qualification; local workflow
contracts do not supply it. The live failure-publication path also remains pending.

Next action: Codex checks required CI on #791, then integrates/retargets the
composition chain in order and verifies the exact resulting candidate.
Compact local evidence: `/private/tmp/alopex-release-composition`.
