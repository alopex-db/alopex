# Alopex Python v0.8.4-repair Performance Report

Commit: `761d6214694c229df46a63933ac438ad7f700dcd`
Measured at: 2026-08-10T18:41:21.832175+00:00
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
| `read_overhead_pct` | 0.3475 % | — | — | 2 | insufficient_history |
| `scan_overhead_ms` | 0.0019 ms | — | — | 2 | insufficient_history |
| `write_overhead_pct` | 0.2125 % | — | — | 2 | insufficient_history |
