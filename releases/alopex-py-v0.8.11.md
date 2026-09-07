# Alopex Python v0.8.11 Performance Report

Commit: `cec643d3a4feefb8e082309ff6159e1676888212`
Measured at: 2026-09-07T12:56:55.509267+00:00
Profile: `gha-ubuntu-24.04-x64-single-core-v1`
CPU: AMD EPYC 7763 64-Core Processor
Environment fingerprint: `5085be655afe9b76`
Workload signature: `597abb463084432c55db143a75c98ce3f203c44e8b7b3db2adeb300a894565cf`

# Alopex Python Performance Advisory

Overall status: **insufficient_history**
Issue notification ready: **false**

> Advisory only: performance regressions create or update an issue; they do not fail CI.

| Metric | Current | Baseline median | MAD | Samples | Status |
|---|---:|---:|---:|---:|---|
| `read_overhead_pct` | 0.3520 % | — | — | 0 | insufficient_history |
| `scan_overhead_ms` | 0.0061 ms | — | — | 0 | insufficient_history |
| `write_overhead_pct` | 0.0509 % | — | — | 0 | insufficient_history |
