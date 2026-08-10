# Alopex Python v0.8.4 Performance Report

Commit: `9a0cea1d24e7672f59cae72d9218b9cc698d9162`
Measured at: 2026-08-10T16:57:02.641759+00:00
Profile: `gha-ubuntu-24.04-x64-single-core-v1`
CPU: AMD EPYC 7763 64-Core Processor
Environment fingerprint: `cc9dcf2a54b6c574`
Workload signature: `1ec4da38777426b8d73dfab354b76795fd8c08cdcf37e2a113e8d2fb4d8b0a9b`

# Alopex Python Performance Advisory

Overall status: **insufficient_history**
Issue notification ready: **false**

> Advisory only: performance regressions create or update an issue; they do not fail CI.

| Metric | Current | Baseline median | MAD | Samples | Status |
|---|---:|---:|---:|---:|---|
| `read_overhead_pct` | 0.4649 % | — | — | 0 | insufficient_history |
| `scan_overhead_ms` | 0.0061 ms | — | — | 0 | insufficient_history |
| `write_overhead_pct` | 0.2429 % | — | — | 0 | insufficient_history |
