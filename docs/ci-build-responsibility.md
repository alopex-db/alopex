# CI build responsibility and metrics

## Purpose and current decision

Issue [#196](https://github.com/alopex-db/alopex/issues/196) reduces build feedback time by assigning every expensive build/test signature to one owner. Compatibility coverage is preserved, but version-labelled gates no longer call each other and the final CI gate only joins owner statuses.

The production rule is:

> One `(toolchain, target, profile, features, package/target set)` signature has one execution owner in a workflow graph.

Owner result JSON is the machine-facing evidence. GitHub Job Summary and uploaded Cargo timing HTML are human-facing views; neither is an input to a later build.

## Ownership

| Owner | Responsibility | Does not own |
|---|---|---|
| `test` matrix | macOS stable and Linux beta compatibility | stable Linux current suite, Windows current suite |
| `build` matrix | cross-platform release compilation, including Windows | workspace test behavior |
| `v08-release-gate` | stable Linux full workspace and Python surfaces | v0.6/v0.7 historical contracts, Windows workspace |
| `compatibility: historical-parser` | one exact parser build/test and verified artifact | Cargo package tests |
| `compatibility: historical-contract` | independent v0.6 and v0.7 scheduled/manual contracts | PR critical path, nested version gates |
| `compatibility: current-windows-full` | scheduled/manual full Windows workspace compatibility | PR critical path |
| `coverage` | coverage-instrumented build and tests | reuse as an uninstrumented correctness artifact |
| `security-audit` | blocking duplicate-version policy followed by RustSec advisory audit | compilation or test execution |
| `release.yml` | package, sign, publish, and public delivery verdict | repeating implementation tests |
| `ci-success` | status-only join of required owners | build or test execution |

`cargo test --workspace` already runs workspace library doctests. A second explicit `cargo test --doc --workspace` is therefore forbidden unless Cargo semantics or selectors change and a contract test documents the new gap.

The compatibility workflow has two disjoint trigger paths. Push/pull-request events run only the native and advisory WASM compatibility jobs; schedule/manual events run only the historical parser, v0.6/v0.7 contract jobs, and full Windows current compatibility. Adding a trigger must preserve that separation so a scheduled run cannot silently multiply the regular matrix.

## Metrics artifact

`scripts/ci/run_with_metrics.py` wraps one owner command and always writes schema `alopex-ci-build-owner-result-v1` with:

- owner and exact command;
- UTC start/completion time and wall seconds;
- exit status;
- target path and apparent file bytes.

The wrapper exits with the wrapped command's status, so measurement can never convert a failed build into success. Current implementation and historical owners upload JSON for 14 days; Cargo timing HTML is uploaded when Cargo produced it.

## Lifecycle

| Path/artifact | Current role | Target role | Action | Delete when | Proof |
|---|---|---|---|---|---|
| `.github/workflows/ci.yml:test` | all stable OS plus Linux beta | complementary macOS stable/Linux beta lanes | shrink | Linux current and Windows responsibilities have explicit owners | workflow contract tests |
| `v08-release-gate` | current suite plus nested v0.7 | stable Linux full owner | replace | old nested step and duplicate Windows test owner are absent | workflow contract tests and owner JSON |
| Windows current validation | full workspace repeated on every PR | PR release compilation plus one weekly/manual full workspace owner | move | Windows remains in the build matrix and the scheduled catch-all is wired | workflow contract tests and owner JSON |
| `scripts/release/v07_gate.sh` | v0.7, nested v0.6, and delivery smoke | independent historical v0.7 behavior | shrink | scheduled owner is wired | workflow contract-only gate |
| `compatibility.yml` parser build | repeated per historical gate | one checksum-verified artifact producer | replace | both consumers verify the artifact | scheduled/manual workflow |
| `target/cargo-timings` | local transient output | optional diagnostic artifact | keep | workflow artifact upload completes | Actions artifact inventory |
| `artifacts/build-metrics/*.json` | absent | canonical per-owner result | create | retention expires after 14 days | schema/unit tests and artifact upload |
| job-local Cargo targets | runner-local build output | ephemeral owner cache | keep | job and metrics inventory finish | runner cleanup and JSON `target_bytes` |

## Failure, rollback, and operations

- Missing or malformed parser artifacts fail before either historical gate runs.
- v0.6 and v0.7 execute in separate matrix jobs; one failure does not hide the other result.
- Owner metrics are uploaded with `if: always()`, while the wrapper preserves the real command exit status.
- If production CI loses required coverage, revert the ownership change as one unit; do not reintroduce version-gate recursion piecemeal.
- Local cleanup follows the repository `AGENTS.md`: inventory first, preserve active builds and current useful artifacts, and reduce regenerable output even below the 50 GiB hard limit.

## Validation

Run before merging changes to this ownership model:

```bash
python -m unittest discover -s scripts/release/tests
bash scripts/release/v07_gate.sh --workflow-contract-only
bash -n scripts/release/v06_gate.sh scripts/release/v07_gate.sh \
  crates/alopex-tools/v08/verify-v08-surfaces.sh
cargo metadata --no-deps
```

Production effectiveness is not established by local contracts alone. Record the first real CI run, then retain rolling-20 wall time, runner-minutes, cache behavior, and artifact-byte evidence in #196.
