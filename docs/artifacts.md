# Artifacts

This document describes the artifact layout, metadata, and replay guidance for stress tests.

## Output roots

Artifacts are written to `artifacts/` when `STRESS_ARTIFACTS_DIR` is unset, and disabled only when set to an empty string.
Metrics reports are written when `STRESS_REPORT_DIR` is set (default: `target/stress-reports`).

## Layout

```
artifacts/<lane>/<test-name>/<timestamp>/
  run.json
  command.txt
  metrics.json
  logs/
    harness.log
    chaos_timeline.log
  checks/
    summary.json
    kv_storage_consistency_*.json
    sql_storage_consistency_*.json
    dataframe_consistency.json
    durability_*.json
    linearizability.json
    serializability.json
    perf_*.json
```

The `<timestamp>` format is `YYYYMMDDTHHMMSSZ`.

## run.json

`run.json` captures a configuration snapshot for reproducibility:

- `test_name`, `lane`, `seed`, `replay`
- `topology`, `binary_version`, `storage_mode`
- `execution_model`, `concurrency`, `scenario_timeout_ms`, `operation_timeout_ms`
- `metrics_interval_ms`, `warmup_ops`, `slo`
- `started_at`, `git_sha`
- `system` (OS, arch, kernel, CPU, mem, disk, compiler)
- `env` (filtered environment variables)

## command.txt

`command.txt` includes a ready-to-run command with lane/seed:

```
STRESS_LANE=<lane> STRESS_SEED=<seed> cargo test -p <package> --tests --features lane_<lane>
```

## checks/

The `checks/` directory stores detailed verification outputs. Examples:

- `kv_storage_consistency_*.json`
- `sql_storage_consistency_*.json`
- `dataframe_consistency.json`
- `durability_crash_loop.json`, `durability_wal_integrity.json`
- `linearizability.json`, `serializability.json`
- `perf_mixed_point_latency.json`, `perf_sequential_throughput_and_amplification.json`

## logs/

- `harness.log`: summary line with lane/seed/throughput.
- `chaos_timeline.log`: chaos injection timeline (chaos matrix runs).

## Replay workflow

1) Read `command.txt` to get the exact command.
2) Set `STRESS_REPLAY=1` (or `STRESS_SEED`/`STRESS_REPLAY_SEED`) to force deterministic mode.
3) Re-run the test in the same lane and storage mode.

Example:

```bash
STRESS_REPLAY=1 STRESS_SEED=12345 cargo test -p alopex-core --tests --features lane_nightly
```
