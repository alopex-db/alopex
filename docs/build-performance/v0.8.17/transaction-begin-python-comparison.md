# Public Python source-matched LSM begin comparison

This extends the [native LSM fix evidence](transaction-begin-lsm-fix.md) for
#684 / #770. It verifies the public Python entrypoint on both product sources;
it does not qualify release packages or complete #684 acceptance.

## Source and measurement identity

| Item | Before | After |
| --- | --- | --- |
| Build source | `b5ad28540edcd8aacb91ff0d2d3dae8f4fd873bf` | `e6c64f3b505ae54a14e3a6803c551a19e0c7a203` |
| Product equivalence | candidate `5fe5cbc1` | fix `90b8b0b5` |
| Extension SHA-256 | `8a661828287119d0e692b7213aebf62321f8babf70135546a44b72273a142ae9` | `116853261d4d8e89e6ceba855fcc66fbe53645e07ed50a429bd960bc8df4404d` |

Both are locally built Alopex 0.8.17 wheels, CPython 3.13.16, on the same macOS
arm64 host. Maturin 1.14.1 used the dev profile with `opt-level=1`, debug and
incremental compilation disabled, and two build jobs. The shared target was
reused serially. The published v0.8.16 contract 0.28.0 parser was an explicit
development override; the parser archive hash is recorded in the native report.
There is no comparison of a published 0.8.16 release build against a different
candidate profile.

The exact corrected worker and runner were identical at both measurement
sources. Only those diagnostic Python files were changed during the detached
baseline checkout; product sources were verified clean. Installed extension
bytes matched each wheel, and every process reported the expected extension
hash. The original branch was restored before building the fixed product.

## Handle lifetime correction

The worker previously retained the seed transaction until the first timed
begin. It now destroys that handle before reopen/measurement, and uses the
transaction context manager so a failed commit is not followed by a second
rollback of an already closed transaction. Failure still releases the handle
before database close. Timed transactions continue to be destroyed after their
rollback interval and before the next begin.

Three fake-handle tests pass without loading a native database. They cover an
empty fixture, a seeded memory/disk fixture, and seed failure. The original
worker fails the seeded cases and error case under the same tests. The earlier
published-wheel baseline's 16 nonempty first-after-open samples are excluded;
its 120 warm Python samples remain valid. See the corrected counts in the
[baseline report](transaction-begin-baseline.md). Raw samples are retained.

## Public semantics

The same three new public disk tests passed before and after the change:

- Restoring a row to its original value, updating/deleting rows, reusing a
  deleted unique-index value, and reopening with the exact final rows/indexes.
- Savepoint rollback/release, read-your-writes, full rollback and reopen.
- Start-snapshot visibility across an interleaved commit, explicit-read conflict
  `ALOPEX-PY104`, preservation of the winning update and absence of the rejected
  pending key.

Each fixture has at most three rows. The public legacy open uses the default
ring geometry, so each case owns a `TemporaryDirectory` and closes/reclaims it
immediately rather than retaining all pytest temporary DBs until session end.
These are ordinary correctness tests, not performance assertions.

```sh
python -m unittest discover -s scripts/performance/tests -p test_begin_profile.py -v
python -m pytest crates/alopex-py/tests/test_lsm_journal.py -q
```

The baseline ran an identical copy of the new public test file outside the
checkout; the fixed product ran its committed copy.

## Timing result

Each source ran 24 serial processes: 0/100/1,000 rows, memory/disk, RO/RW and
noop/query. Each process recorded one first-after-open and five warm samples,
separately timing begin, query and rollback. That is **48 cases / 288 samples**
across the two sources. Every case checked all final rows and reclaimed its
fixture. No INSERT occurs in a timed interval, and no performance execution was
added to ordinary CI.

Warm disk RW noop begin medians, milliseconds:

| Rows | Before | After | Improvement criterion |
| ---: | ---: | ---: | --- |
| 0 | 0.002458 | 0.001416 | Not met: noise bound |
| 100 | 0.109209 | 0.001125 | Met |
| 1,000 | 0.881083 | 0.001167 | Met |

The 1,000-row median decreased by **99.868%**. The predeclared criterion was at
least 10% improvement with an absolute difference above
`3 * max(MAD_before, MAD_after)`. No empty-database improvement is claimed.
First-after-open does not mean an OS-cold page cache. Full phase samples and
MADs are in `/private/tmp/alopex-684-python/comparison.json`.

Per-case safeguards remained 90 seconds, sampled RSS below 2 GiB and at most
2 GB additional host filesystem usage, with the 40 GB host stop boundary.
Sampling RSS every 100 ms may miss short-process peaks; these values do not
prove phase-specific memory improvement.

## Invalid attempts and remaining acceptance

The initial parser sidecar used the wrong required line order; the build stopped
before any tests. Correcting the sidecar format preserved checksum validation.
An initial Maturin `--profile test` invocation packaged a Mach-O test executable
instead of a loadable extension. Its missing-init-symbol warning and import
failure were investigated, and its wheel was excluded and removed. Both
qualified wheels were then built with the same dev/opt-level-1 configuration,
imported successfully and passed the public tests. Logs and identities of the
failed attempts remain separate from accepted results.

The [HNSW/resource follow-up](transaction-begin-hnsw-resources.md) now records
public Python index counts 0/1/2, autocommit and start-phase memory/disk scaling.
Native index/resource coverage, first-write overhead, OS CI and candidate
integration remain. Reuse the two recorded source-bound wheels for
those immediate comparisons; they are development artifacts, not release assets.
The temporary venv, parser downloads and owned intermediate binaries are
reclaimed after recording, while compact logs and the two needed wheels remain.
Do not rebuild these products merely to refresh their dates.
