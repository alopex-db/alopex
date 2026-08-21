# Issue #196 local responsibility experiment

This is a local-only experiment for decomposing CI by verification
responsibility. It does not modify or trigger the repository's existing CI,
tests, release gates, or source code. It is not a replacement workflow and
must not be merged into `main` as production CI.

## Outcome

The current graph nests temporal release gates:

```text
v08 release gate -> v07 gate -> v06 gate
                 -> v08 surface verifier
```

Those gates own bundles of current behavior, historical compatibility,
delivery, and performance checks. The same current-implementation build work
can therefore be owned more than once, behind different target boundaries.
The inventory also found `cargo clean --profile dev` inside the v0.7 release
gate, coupling delivery verification to cache destruction.

The target graph removes version-gate nesting. Each observed Cargo signature
has exactly one canonical owner:

- `current-quality`: formatting, lint, and compile-quality feedback;
- `current-implementation`: current behavior and active feature combinations;
- `historical-compatibility`: persisted-data and API compatibility contracts;
- `delivery-contract`: artifacts and public surfaces delivered to users;
- `exhaustive-performance`: expensive checks outside merge-gate feedback.

The final gate depends on those owner statuses and runs no build or test
command. Performance work is not silently deleted; it remains an explicit
owner with a different scheduling responsibility.

## Measured evidence

The containerized `act` run on 2026-08-21 used `origin/main` at
`bd4479cca5ede2668956fd523f0b8fa1c03c3557`, Cargo 1.96.0, and Rust 1.96.0.
The A/B probe used the identical existing command in both graphs:

```text
cargo test -p alopex-dataframe --tests --features lane_ci --locked --timings
```

Graph A assigned that current-implementation responsibility to two version
gate copies with separate target directories. Graph B assigned it once to the
canonical owner. Each graph was measured cold and warm three times; graph
order alternated by sample. The values below are sums of owner command time,
excluding result serialization and directory-size instrumentation.

| Graph | Cold samples (s) | Cold median | Warm samples (s) | Warm median |
| --- | --- | ---: | --- | ---: |
| Nested version ownership | 1117.349, 1512.720, 1398.277 | 1398.277 | 4.643, 755.827, 5.482 | 5.482 |
| Single responsibility owner | 546.720, 614.596, 765.613 | 614.596 | 3.298, 2.356, 2.060 | 2.356 |

The cold median decreased by 56.05%. This is not a reduction obtained by
skipping a test: Graph A runs the same existing suite twice under duplicate
ownership, while Graph B runs that suite once under its canonical owner. The
test bodies complete in well under a second in representative output; cold
compilation and linking dominate the elapsed time. Graph A also retained about
1.35 GB across two targets during a sample, compared with about 674 MB for
Graph B's one target.

Nested Graph A sample 2 unexpectedly rebuilt during its warm phase and took
755.827 seconds; samples 1 and 3 reused their targets normally. The experiment
records this anomaly rather than filtering it out. Its exact invalidation cause
was not established before the per-sample target was intentionally deleted,
so it is evidence of build-boundary lifecycle instability, not a claimed root
cause. The cold conclusion does not depend on that warm outlier.

The static inventory assigned all 35 observed Cargo signatures exactly once:

| Owner | Signatures |
| --- | ---: |
| Current quality | 4 |
| Current implementation | 11 |
| Historical compatibility | 5 |
| Delivery contract | 13 |
| Exhaustive performance | 2 |

It found one exact duplicate group and three near-duplicate groups. The exact
duplicate is `cargo fmt --all -- --check` in both the existing CI workflow and
the v0.7 gate. The large near-duplicate test group spans 28 commands across
the existing CI and release/surface scripts. Near-duplicate does not mean the
commands are interchangeable; it identifies places where ownership and build
boundaries must be reviewed instead of accumulated in a final gate.

All experiment jobs passed. Existing historical release-contract tests (13)
and existing CI workflow-contract tests (11) ran without modification. The
final gate joined six upstream experiment statuses in 4.577 seconds and did
not invoke verification.

## Isolation contract

- The experiment branch is based directly on `origin/main`.
- Only this document, the dedicated workflow, and
  `scripts/issue196_experiment/` may differ from `origin/main`; the final
  verifier found 14 changed paths, all allowlisted.
- The workflow has only `workflow_dispatch`; it has no push, pull-request, or
  schedule trigger and is selected explicitly with `act -W`.
- A synthetic Git repository is created from `origin/main`, then only
  allowlisted experiment files are overlaid. The `act` controller sees that
  snapshot read-only and cannot see the real worktree or its Git metadata.
- Existing tests are invoked without editing them.
- Cargo targets and the experiment Cargo home live below
  `/tmp/alopex-issue-196-runtime`, not in the repository's normal `target`.
- The host Cargo registry index is mounted read-only. Only registry archives
  are copied into task-owned state; credentials, configuration, and extracted
  source caches are neither copied nor mounted. Missing locked archives are
  downloaded in a one-shot container and checked against `Cargo.lock` SHA-256
  values before use. The measured workflow then runs Cargo offline.
- The runner labels and removes only its own containers and workflow-specific
  volumes. It preserves a pre-existing shared `act-toolcache` volume.

## Artifact lifecycle

| Artifact or path | Owner | Lifecycle |
| --- | --- | --- |
| Existing CI, release scripts, tests, and source | Existing repository | Read-only input; keep unchanged |
| Responsibility manifest and validators | This experiment | Canonical experiment contract; review before any production design |
| Inventory, raw A/B observations, and summary JSON | This experiment | Ephemeral evidence below task-owned `/tmp`; capture, then delete |
| Cargo home and per-sample targets | This experiment | Ephemeral; separate per graph/sample and delete after measurement |
| Final gate | Status join | Consumes owner results only; owns no build artifacts |

No production migration, rollback, or deletion is authorized by this
experiment. A future production change must map every real command to an
owner, preserve historical and delivery contracts, define cache ownership,
and prove the complete rolling acceptance window before removing an obsolete
gate.

## Local execution

The runner pins `act` v0.2.88 by SHA-256 and runs the binary as the entrypoint
of the existing `docker.io/library/rust:1.96-bookworm` image. A task-specific
rootless Podman API socket allows the controller to create job containers.
Neither a controller image nor a project image is built.

```bash
ISSUE196_CARGO_SEED_DIR=/path/to/cargo-home \
  bash scripts/issue196_experiment/run_containerized_act.sh
```

The runtime is `/tmp/alopex-issue-196-runtime`; the pinned `act` archive cache
is `/tmp/alopex-issue-196-cache`. The service startup has a bounded 60-second
wait with a liveness check. After evidence is captured, delete only those two
task-owned paths.

## What this does not prove

This experiment proves that the representative duplicate responsibility can
be owned upstream once and that the final gate can be status-only. It does not
prove that the reported 5–6 hour production path is already eliminated, that
the full production graph has been migrated, or that issue #196's rolling
performance acceptance criteria are met. Those claims require a separate,
reviewed production design and complete CI-equivalent evidence—not this
non-merge local route.
