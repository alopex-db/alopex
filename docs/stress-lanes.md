# Stress Lanes

This document defines the stress-test lanes, how they are selected, and how to run them.

## Lane definitions

| Lane | Feature flag | Purpose | Typical schedule |
| --- | --- | --- | --- |
| smoke | `lane_smoke` | Minimal, fast checks for local sanity | PR / local |
| ci | `lane_ci` | Standard CI coverage | PR / main |
| nightly | `lane_nightly` | Longer-running stress scenarios | nightly |
| weekly | `lane_weekly` | Very long or heavy workloads | weekly |
| soak | `lane_soak` | Multi-hour burn-in / resource monitoring | nightly / weekly |
| perf | `lane_perf` | Performance baseline + regression checks | nightly |
| fuzz | `lane_fuzz` | Long fuzz runs (24h targets) | nightly / weekly |
| sanitizer | `lane_sanitizer` | TSAN/ASAN/LSAN/MSAN via `-Zsanitizer` | nightly / release gate |

## Lane selection rules

Lane selection is resolved in the following order:

1) `STRESS_LANE` environment variable
2) Enabled lane feature flags (`--features lane_*`)
3) Default: `ci`

Examples:

```bash
# Run only nightly lane tests
STRESS_LANE=nightly cargo test -p alopex-core --tests --features lane_nightly

# Run multiple lanes explicitly
STRESS_LANE=ci,nightly cargo test -p alopex-core --tests --features lane_ci,lane_nightly

# Run all lanes
STRESS_LANE=all cargo test -p alopex-core --tests --features lane_smoke,lane_ci,lane_nightly,lane_weekly,lane_soak,lane_perf,lane_fuzz,lane_sanitizer
```

## Typical commands

```bash
# CI lane
cargo test -p alopex-core --tests --features lane_ci

# Nightly lane
cargo test -p alopex-core --tests --features lane_nightly

# Weekly lane
cargo test -p alopex-core --tests --features lane_weekly

# Soak lane
cargo test -p alopex-core --tests --features lane_soak

# Perf lane
STRESS_BASELINE_REQUIRED=true \
STRESS_BASELINE_DIR=target/stress-baselines \
STRESS_REPORT_DIR=target/stress-reports/perf \
STRESS_ARTIFACTS_DIR=target/stress-artifacts/perf \
cargo test -p alopex-core --features lane_perf --test stress_perf_baseline

# Fuzz lane (cargo fuzz targets)
cd crates/alopex-sql/fuzz
cargo fuzz run sql_parser -- -max_total_time=86400
cd ../../alopex-dataframe/fuzz
cargo fuzz run dataframe_conversion -- -max_total_time=86400
# Run both SQL and DataFrame targets for 24h.

# Sanitizer lane (nightly + script)
./scripts/run-sanitizer-lane.sh asan
```

## Lane-specific environment variables

Soak lane:

- `STRESS_SOAK_DURATION_SECS`
- `STRESS_WEEKLY_DURATION_SECS`
- `STRESS_SOAK_MAX_RSS_MB`
- `STRESS_SOAK_MAX_DB_MB`
- `STRESS_SOAK_CHECK_INTERVAL_SECS`
- `STRESS_SOAK_BATCH_SIZE`
- `STRESS_SOAK_VALUE_SIZE`
- `STRESS_SOAK_KEY_SPACE`

Fuzz lane:

Fuzz control is passed via libFuzzer args (e.g. `-max_total_time=86400`, `-seed=...`).
The current fuzz targets do not consume `STRESS_FUZZ_DURATION_SECS`, `STRESS_SEED`,
or `STRESS_REPLAY_SEED`.

Perf lane:

- `STRESS_BASELINE_REQUIRED`
- `STRESS_BASELINE_DIR`
- `STRESS_BASELINE_UPDATE`
- `STRESS_BASELINE_MARGIN_PCT`

Sanitizer lane:

- `STRESS_REPORT_DIR`
- `STRESS_ARTIFACTS_DIR`
- `RUSTFLAGS`

## Common environment variables

- `STRESS_LANE`: Override lane selection (e.g., `ci`, `nightly`, `all`).
- `STRESS_SEED` / `STRESS_REPLAY_SEED`: Fix RNG seed for deterministic replay.
- `STRESS_REPLAY`: Enable deterministic mode (`1` / `true`).
- `STRESS_STORAGE_MODE`: `memory`, `disk`, or `both` (default).
- `STRESS_REPORT_DIR`: Metrics report output directory.
- `STRESS_ARTIFACTS_DIR`: Artifacts root directory.
