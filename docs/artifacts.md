# Stress artifacts

The harness uses `STRESS_EVIDENCE_MODE=public` by default. Set
`STRESS_EVIDENCE_MODE=debug` explicitly for local diagnosis. Debug mode fails
when `CI` is set. `STRESS_ARTIFACTS_DIR` selects the root (default `artifacts/`);
an empty value disables harness artifacts. Stage 2 jobs set a dedicated public
root and reject missing records.

Each run lives at `<root>/<lane>/<test-name>/<YYYYMMDDTHHMMSSZ>-<index>/`.
The index is reserved atomically, so repeated invocations in the same second
cannot overwrite an earlier measurement.

## Public mode

Public runs contain only `run.json`, `metrics.json`, and `checks/summary.json`.
Performance cases also write `checks/perf_*.json`. No command, log, host
inventory, environment snapshot, crash input, or free-text error is recorded.

- `run.json` has schema version 1, fixed test and lane identifiers, seed, crate
  version, execution model, concurrency, timeouts, interval, warmup count, SLO
  configuration, replay flag, and a validated commit SHA when available.
- `metrics.json` has schema version 1, duration and operation counts, finite
  throughput and outlier ratio, and optional latency percentiles in nanoseconds.
- `checks/summary.json` has schema version 1, a fixed result code, watchdog
  outcome, SLO outcome, skipped/success flags, and duration.
- Performance checks retain finite current and baseline numeric values, a
  pass flag per metric, and the overall comparison result. The Stage 2
  publication step supplies and checks the pinned baseline identity.

See [stress-evidence-modes.md](stress-evidence-modes.md) for the acceptance
contract.

## Local debug mode

The same run directory additionally contains `command.txt`, `logs/harness.log`,
and detailed `checks/*.json` files. Chaos runs may write a timeline log.
`STRESS_REPORT_DIR` (default `target/stress-reports`) receives the legacy JSON
and Markdown metrics reports in debug mode only. These outputs are local
diagnostics and are not accepted by the Stage 2 publisher.
