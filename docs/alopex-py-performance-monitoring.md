# Alopex Python Performance Monitoring

This runbook describes the advisory performance lifecycle introduced for
[Issue #76](https://github.com/alopex-db/alopex/issues/76). It covers the three
Polars wrapper overhead metrics only. Rust stress baselines use a separate
mechanism documented in [perf-baselines.md](perf-baselines.md).

## Current decision

- Performance values never determine the result of normal CI or a release.
- `pytest-benchmark` JSON and xUnit XML are raw run artifacts. Their monitored
  values must agree before a canonical record is accepted.
- `performance-history/history.json` on the `performance-history` branch is the
  canonical cross-run history.
- Markdown, xUnit XML, comparison JSON, and release reports are derived evidence.
- Only records with identical environment and workload fingerprints are compared.
- One outlier is recorded. An issue is created or updated only after two
  consecutive distribution outliers in the same series.
- Criterion and competitor benchmarks belong to
  [Issue #45](https://github.com/alopex-db/alopex/issues/45); they are not mixed
  into this Python overhead distribution.

## Invariants and ownership

The measurement test owns numeric fact production and functional assertions.
The collector owns schema validation and environment identity. The analyzer
owns distribution comparison. The workflow owns serialization, persistence,
and advisory issue creation. Rendered Markdown never becomes machine input.

| Path | Current role | Target role | Action | Delete when | Proof |
|---|---|---|---|---|---|
| `crates/alopex-py/tests/benchmarks/test_performance.py` | Measures three overheads and checks correctness | Emit numeric benchmark/xUnit properties; keep correctness assertions | shrink | Old compound strings and `min()` aggregation are absent | Targeted pytest plus JSON inspection |
| `.github/workflows/alopex-py.yml` | Runs all benchmarks in ordinary CI | Retain an advisory artifact job outside `ci-success`; run the three correctness assertions in the required Polars lane | shrink | Performance values are absent from assertions | Harness contract and Polars tests |
| `.github/workflows/alopex-performance.yml` | Not previously present | Trusted, serialized measurement composition root | replace | A successor persists the same schema and reports | Manual workflow run |
| `scripts/performance/alopex_py_metrics.py` | Not previously present | Normalize, compare, retain, and render canonical data | replace | A versioned migration covers all history | Unit and replay tests |
| `performance-history/history.json` | Ephemeral Actions artifact only | Canonical, idempotent history | move | A migrated store is verified before branch removal | Rerun the same commit and inspect one record |
| `performance-history/index.md` | Not previously present | Regenerable commit trend table | replace | It can be regenerated from `history.json` | `record` command |
| `performance-history/releases/{v*,alopex-py-v*}.{json,md}` | Not previously present | Tag-SHA performance report | replace | A replacement release view is generated from canonical JSON | Tag-triggered workflow |

## Measurement profile

The dedicated workflow runs on one `ubuntu-24.04` GitHub-hosted runner and is
globally serialized by the `alopex-performance-history` concurrency group. The
queue retains up to 100 pending runs so rapid pushes do not silently replace an
older pending measurement. The workload is pinned to one CPU with `taskset`, and
common numerical-library thread pools are limited to one thread. Python, Rust,
the Python dependencies, Nim, and the Nim packages are pinned. These settings
are mirrored in `scripts/performance/profile-v1.json`, checked against the
workflow, and included in the workload signature.

CPU model, kernel, governor, turbo setting, dependency versions, `Cargo.lock`
hash, and workload hash are recorded. The workload hash covers the benchmark
and native-parser build inputs, but not the analyzer/renderer. `Cargo.lock` and
the installed `alopex` version are provenance for the subject under test, not
environment identity; including either would reset the baseline at the exact
application changes the monitor is meant to compare. Governor and turbo cannot
always be controlled on a hosted runner; an unavailable value is recorded
explicitly. This is why the output remains advisory even after environmental
isolation.

The measurement job has read-only repository permission. A second job receives
the measurement artifact and holds write permissions for trusted history and
issue updates. Manual runs on non-main branches produce artifacts but cannot
persist history or notify an issue.

## Artifacts and history

Each successful run produces:

- `benchmark.json`: raw pytest-benchmark output;
- `performance-junit.xml`: xUnit 1 properties cross-checked against the JSON;
- `current.json`: normalized numeric record;
- `comparison.json`: distribution comparison and notification decision;
- `report.md`: human-facing advisory summary.

Trusted `main`, scheduled, manual-on-main, `v*`, and `alopex-py-v*` runs update
the `performance-history` branch. A record is keyed by commit, environment
fingerprint, and workload signature, so rerunning the same source is idempotent.
The latest 50 records per comparable series are retained.

## Regression and notification policy

The analyzer requires at least five prior samples from the same environment and
workload. It compares the current value to the historical median using a
one-sided robust z-score based on median absolute deviation. If the historical
MAD is zero, a value above the historical median is an outlier. Requiring two
consecutive outliers prevents one tiny plateau deviation from creating an issue
while still detecting two equal degraded measurements.

The first outlier is visible in the report but does not create an issue. If the
latest prior sample and current sample are outliers for the same metric, the
trusted workflow creates an issue. Later occurrences update that issue using an
environment/workload marker. Notification errors and measured regressions do
not fail normal CI or a release.

The workflow intentionally has no `pull_request` trigger. Untrusted fork code
must never run with `contents: write` or `issues: write`. Regression notification
therefore happens after a trusted main or scheduled measurement.

## Release reports

Core `v*` and Python `alopex-py-v*` tags measure that exact `GITHUB_SHA` and
write both Markdown and JSON to `performance-history/releases/<tag>.*`. If any
setup, build, collection, or comparison stage fails, the write job records an
explicit non-blocking missing measurement instead. Re-running the tag workflow
regenerates the same canonical record or missing-measurement report.

## Local validation and replay

Run the harness unit and workflow-contract tests without building Rust:

```bash
python -m pytest scripts/performance/tests/test_alopex_py_metrics.py
```

After creating `benchmark.json`, replay the lifecycle with:

```bash
python scripts/performance/alopex_py_metrics.py collect \
  --benchmark-json benchmark.json \
  --junit-xml performance-junit.xml \
  --output current.json \
  --commit "$(git rev-parse HEAD)" \
  --ref refs/heads/main \
  --profile local-single-core-v1 \
  --lock-file Cargo.lock \
  --workload-file crates/alopex-py/tests/benchmarks/test_performance.py \
  --workload-file scripts/performance/profile-v1.json \
  --workload-file scripts/build-nim-parser.sh \
  --workload-file crates/alopex-sql/nim-sql-parser/nim_sql_parser.nimble

python scripts/performance/alopex_py_metrics.py evaluate \
  --current current.json \
  --history history.json \
  --output-json comparison.json \
  --output-markdown report.md

python scripts/performance/alopex_py_metrics.py record \
  --current current.json \
  --history history.json \
  --index index.md
```

Generated virtual environments, Cargo `target/`, and replay files are caches or
derived evidence and may be deleted after validation. Do not delete
`performance-history/history.json` without a verified migration.

## Operational prerequisites and rollback

GitHub Actions must allow `GITHUB_TOKEN` write access for the workflow to create
the history branch and advisory issues. The branch is created automatically on
the first successful trusted run.

To roll back measurement execution, disable the dedicated workflow without
deleting the history branch. To roll back a schema change, restore the previous
collector and replay the canonical JSON. Markdown and release views can always
be regenerated and require no backup of their own.
