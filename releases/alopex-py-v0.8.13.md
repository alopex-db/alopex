# Alopex Python v0.8.13 Performance Report

Commit: `04fb191a390b768acd08919aea6004b86d002ac0`
Measured at: 2026-09-17T14:53:16.254153+00:00
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
| `read_overhead_pct` | 0.3398 % | 0.4024 | 0.0435 | 8 | stable |
| `scan_overhead_ms` | 0.0070 ms | 0.0010 | 0.0004 | 8 | regression |
| `write_overhead_pct` | 0.3470 % | 0.3540 | 0.1483 | 8 | stable |
