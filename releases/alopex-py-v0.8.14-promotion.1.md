# Alopex Python v0.8.14-promotion.1 Performance Report

Commit: `e18e6b5779cd1871072ba0d0d5dd2bee64b46998`
Measured at: 2026-09-24T08:09:57.505056+00:00
Profile: `gha-ubuntu-24.04-x64-single-core-v1`
CPU: AMD EPYC 7763 64-Core Processor
Environment fingerprint: `5085be655afe9b76`
Workload signature: `597abb463084432c55db143a75c98ce3f203c44e8b7b3db2adeb300a894565cf`

# Alopex Python Performance Advisory

Overall status: **regression**
Issue notification ready: **false**

> Advisory only: performance regressions create or update an issue; they do not fail CI.

| Metric | Current | Baseline median | MAD | Samples | Status |
|---|---:|---:|---:|---:|---|
| `read_overhead_pct` | 0.4832 % | 0.4024 | 0.0524 | 10 | stable |
| `scan_overhead_ms` | 0.0048 ms | 0.0010 | 0.0005 | 10 | regression |
| `write_overhead_pct` | 0.6690 % | 0.3338 | 0.0979 | 10 | stable |
