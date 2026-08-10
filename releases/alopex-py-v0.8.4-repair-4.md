# Alopex Python v0.8.4-repair-4 Performance Report

Commit: `5ade7d73724d01c6b73044995bee1b1f69f751f4`
Measured at: 2026-08-10T20:23:53.990293+00:00
Profile: `gha-ubuntu-24.04-x64-single-core-v1`
CPU: AMD EPYC 9V74 80-Core Processor
Environment fingerprint: `aad91bff6b2e74ff`
Workload signature: `1ec4da38777426b8d73dfab354b76795fd8c08cdcf37e2a113e8d2fb4d8b0a9b`

# Alopex Python Performance Advisory

Overall status: **insufficient_history**
Issue notification ready: **false**

> Advisory only: performance regressions create or update an issue; they do not fail CI.

| Metric | Current | Baseline median | MAD | Samples | Status |
|---|---:|---:|---:|---:|---|
| `read_overhead_pct` | 0.2945 % | — | — | 2 | insufficient_history |
| `scan_overhead_ms` | 0.0016 ms | — | — | 2 | insufficient_history |
| `write_overhead_pct` | 0.4009 % | — | — | 2 | insufficient_history |
