# Alopex Python v0.8.4-repair-5 Performance Report

Commit: `09831a3836fb8d1c5bd0ad4da7c7ab8adb516faa`
Measured at: 2026-08-10T20:57:38.730734+00:00
Profile: `gha-ubuntu-24.04-x64-single-core-v1`
CPU: INTEL(R) XEON(R) PLATINUM 8573C
Environment fingerprint: `8f2f02d3b3c0fe88`
Workload signature: `1ec4da38777426b8d73dfab354b76795fd8c08cdcf37e2a113e8d2fb4d8b0a9b`

# Alopex Python Performance Advisory

Overall status: **insufficient_history**
Issue notification ready: **false**

> Advisory only: performance regressions create or update an issue; they do not fail CI.

| Metric | Current | Baseline median | MAD | Samples | Status |
|---|---:|---:|---:|---:|---|
| `read_overhead_pct` | 0.3824 % | — | — | 1 | insufficient_history |
| `scan_overhead_ms` | 0.0013 ms | — | — | 1 | insufficient_history |
| `write_overhead_pct` | 1.0439 % | — | — | 1 | insufficient_history |
