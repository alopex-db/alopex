# Performance Baselines

This document describes how perf baselines are generated, stored, and enforced.

## Baseline storage

Baselines are stored as JSON files in:

- `STRESS_BASELINE_DIR` (default: `target/stress-baselines`)

Each test writes a `<test-name>.json` file with metric values.

## Update workflow

To generate or refresh baselines:

```bash
STRESS_BASELINE_UPDATE=1 \
STRESS_BASELINE_DIR=target/stress-baselines \
STRESS_REPORT_DIR=target/stress-reports/perf \
STRESS_ARTIFACTS_DIR=target/stress-artifacts/perf \
cargo test -p alopex-core --features lane_perf --test stress_perf_baseline -- --test-threads=1
```

## Enforcement

Set `STRESS_BASELINE_REQUIRED=true` to require baselines during perf runs. The default regression
margin is 20% and can be changed with `STRESS_BASELINE_MARGIN_PCT` (e.g., `0.15`).

## Metrics tracked

### perf_mixed_point_latency

- `throughput_ops_sec` (higher is better)
- `p50_latency_ns` (lower is better)
- `p95_latency_ns` (lower is better)
- `p99_latency_ns` (lower is better)

### perf_sequential_throughput_and_amplification

- `throughput_ops_sec` (higher is better)
- `write_amplification` (lower is better)
- `space_amplification` (lower is better)

## Regression checks

Baseline comparisons are performed with:

- Higher-is-better metrics must be >= `baseline * (1 - margin)`
- Lower-is-better metrics must be <= `baseline * (1 + margin)`

Violations are written to `checks/perf_*.json` before failing the test.

## CI usage

The perf lane downloads the latest `perf-baselines` artifact and runs only on
`schedule`/`workflow_dispatch`. This keeps PR CI lightweight while still enforcing
nightly regression checks.
