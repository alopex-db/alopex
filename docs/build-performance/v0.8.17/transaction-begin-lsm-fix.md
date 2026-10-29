# LSM transaction begin: write-local journal development evidence

This is development evidence for #684, following the
[baseline investigation](transaction-begin-baseline.md). It is not candidate or
release acceptance. The product change is `90b8b0b560ae79fd6bfd78a3a3e588ae3422a7ad`,
stacked on #769 (`b5ad2854`); the product baseline is candidate `5fe5cbc1`.

## Change and retained contracts

Borrowed and owned LSM transactions now provide the existing
`journal_pending_writes` capability. Each modified key retains its value at the
transaction start timestamp, captured before the first write. SQL journal
capture and staging therefore use the modified keys instead of scanning all SQL
rows and indexes. Repeated writes preserve the original; restoring it produces
no journal delta. Rollback frees originals, and owned savepoint rollback frees
originals for discarded writes without copying them into each savepoint.

Persisted-source errors propagate before staging a write or publishing its
capacity counters. There is no fallback from a failed lookup to an absent value.
The WAL format and aggregate encoded/replay limits are unchanged. Before-image
buffers are separate from those logical WAL counters, like savepoint copies and
allocator overhead. Retained original keys/values grow with modified data, not
the full SQL keyspace; this does not establish an RSS bound. First writes gain a
snapshot lookup, and mutation-heavy/HNSW performance still needs acceptance.

## Focused semantic checks

Twenty focused tests passed on macOS arm64:

- Twelve `journal_` core tests, including seven new borrowed/owned tests and the
  new persisted-error/overflow-capacity regression. A 24-case property checks
  first-before/final-after values against a three-key reference model.
- Four aggregate capacity boundary tests, plus one overwrite/delete/savepoint
  capacity restoration test, now asserting exact pending journal values.
- One SQL integration test covers four borrowed/owned net-change/no-op variants
  and panics if journal capture or staging calls a scan. Reopen preserves the
  changed row and the untouched row.
- Two existing SQL journal overflow tests verify exact aggregate capacity,
  rejection/rollback and recovery of row, index and journal data.

Fixtures contain only a few keys/rows, use small WAL rings, and are reclaimed by
RAII. The tests cover start-snapshot originals, interleaved updates/conflicts,
read-your-writes, read-only refusal, tombstones, repeated writes, savepoints,
rollback, persisted-source errors and reopen. No performance/load execution was
added to normal CI.

Commands used the shared target, test profile, incremental/debug off and build
jobs 2:

```sh
cargo test --offline --profile test -p alopex-core --lib journal_ -- --test-threads=1
cargo test --offline --profile test -p alopex-core --lib aggregate_ -- --test-threads=1
cargo test --offline --profile test -p alopex-core --lib overwrite_delete_and_savepoint_restore_capacity -- --test-threads=1
cargo test --offline --profile test -p alopex-embedded --test lsm_journal_before_images --test wal_overflow_journal -- --test-threads=1
cargo clippy --offline --profile test -p alopex-core --lib --tests -- -D warnings
cargo clippy --offline --profile test -p alopex-embedded --test lsm_journal_before_images --test wal_overflow_journal -- -D warnings
cargo fmt --all -- --check
```

The published v0.8.16 contract 0.28.0 parser was used through the explicit
development override, with archive SHA-256
`8ac1f708efdfdbf6c85a65aebd34a368de41e798d9441a2785abad011cec038d`.
This is not parser/package release qualification.

## Test-gap and design checks

Scoped cargo-mutants changed only the two before-image capture helpers and the
two journal exporters: **10 caught, 0 missed, 0 unviable, 0 timeout**. The final
run passed its unmutated baseline and every caught result was an actual test
assertion failure. Mutation used the product diff, one in-place worker and
`--lib` for both build/test phases; source restoration matched the saved diff.

Cargo-coupling ran on core with `--changed-since main --impact-depth 4 --json
--no-git --jobs 2`. Existing `lsm` and `kv::any` hotspots remain; the existing KV
trait boundary carries the journal view to SQL. Borrowed/owned bookkeeping is
duplicated for their ownership/lifetime routes and both have contract tests.
Main-relative impact includes prior candidate changes, so the score is not an
incremental regression measurement. Static analysis misses runtime ordering,
macro/inactive-cfg expansion and temporal co-change. No broader refactor is part
of this fix. No unsafe change required Miri; Kani was not run.

Failed/invalid attempts remain in local evidence: initial parser configuration
failure before tests; a test reopening its DB before dropping the borrowed
handle (corrected, then all 20 passed); rejected mutation CLI options, an
overbroad mutation build stopped before completion, and duplicated Cargo flags
that prevented test startup. Those mutation attempts are excluded from the ten
caught results.

## Native before/after measurement

The unchanged native `begin_profile` example (SHA-256
`506b058597fd0918cf99fc3266d97be3a704e20b8d909ab64c544c7263920edf`)
ran 48 serial cases: borrowed/owned, memory/disk, RO/RW, noop/query, and
0/100/1,000 rows. Each case has one first-after-open and five warm samples, for
288 samples. All cases validated every row/value and reclaimed their fixture.
No INSERT occurs inside the measured begin/query/rollback interval.

Warm disk RW noop begin medians, milliseconds:

| API | Rows | Before | After |
| --- | ---: | ---: | ---: |
| Borrowed | 0 | 0.001833 | 0.001500 |
| Borrowed | 100 | 0.059500 | 0.001000 |
| Borrowed | 1,000 | 0.480708 | 0.000708 |
| Owned | 0 | 0.002333 | 0.002209 |
| Owned | 100 | 0.120750 | 0.001416 |
| Owned | 1,000 | 0.896000 | 0.000917 |

At 1,000 rows, reductions were 99.853% and 99.898%. Both pass the predeclared
criterion: at least 10% lower median and an absolute difference greater than
`3 * max(MAD_before, MAD_after)`. The 100-row cases also pass; neither empty-DB
case passes both conditions, so no empty-DB improvement is claimed.

The host, test optimization profile, parser and runtime thread limits match the
baseline. This is a bounded diagnostic, not throughput or large-DB acceptance.
First-after-open is not a cold OS page cache. The maximum sampled RSS was
6,979,584 bytes, but 100-ms sampling misses short-process peaks and cannot prove
memory acceptance. Per-case limits were 90 seconds and 2 GiB sampled RSS, with
2 GB additional host storage and the 40 GB host stop boundary. There were no
concurrent product builds during timing.

Raw samples, MADs, binary hashes, comparison results, storage measurements and
failed attempts are retained in `/private/tmp/alopex-684-fix`; the baseline is
in `/private/tmp/alopex-684`. Owned executables/parser downloads are reclaimed
after recording; shared dependencies remain for active release work.

## Remaining acceptance

The [source-matched public Python comparison](transaction-begin-python-comparison.md)
now confirms the begin improvement and public transaction regressions. The
[HNSW/resource comparison](transaction-begin-hnsw-resources.md) and
[first-scalar-write comparison](transaction-first-write-comparison.md) add
public Python evidence, including bounded replication of two initial timing
flags. Native borrowed/owned index/resource coverage, OS CI and candidate
integration remain. These bounded measurements do not establish large-transaction
or HNSW vector-mutation throughput. The public Python 0.8.16 baseline must not be
compared directly with these native candidate results. #684 and the v0.8.17
release remain open.
