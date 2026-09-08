# Alopex Python v0.8.11 Performance Report

Commit: `d287f17eb65757f373b6905fca2d42cded9b4650`
Measured at: 2026-09-08T05:16:04.849910+00:00
Profile: `gha-ubuntu-24.04-x64-single-core-v1`
CPU: AMD EPYC 9V74 80-Core Processor
Environment fingerprint: `a948901164b4352b`
Workload signature: `597abb463084432c55db143a75c98ce3f203c44e8b7b3db2adeb300a894565cf`

# Alopex Python Performance Advisory

Overall status: **insufficient_history**
Issue notification ready: **false**

> Advisory only: performance regressions create or update an issue; they do not fail CI.

| Metric | Current | Baseline median | MAD | Samples | Status |
|---|---:|---:|---:|---:|---|
| `read_overhead_pct` | 0.5521 % | — | — | 2 | insufficient_history |
| `scan_overhead_ms` | 0.0006 ms | — | — | 2 | insufficient_history |
| `write_overhead_pct` | 0.4452 % | — | — | 2 | insufficient_history |
