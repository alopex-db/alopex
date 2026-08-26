# Alopex Python v0.8.8-repair.1 Performance Report

Commit: `f76ea118096defdff4b29236472b446acb5ca020`
Measured at: 2026-08-26T03:51:15.749228+00:00
Profile: `gha-ubuntu-24.04-x64-single-core-v1`
CPU: Intel(R) Xeon(R) 6973P-C
Environment fingerprint: `56afee25cf7ab058`
Workload signature: `3660808adb3c32cd261030686d14ee61055f3f242f133adf366172fa1ae07d17`

# Alopex Python Performance Advisory

Overall status: **insufficient_history**
Issue notification ready: **false**

> Advisory only: performance regressions create or update an issue; they do not fail CI.

| Metric | Current | Baseline median | MAD | Samples | Status |
|---|---:|---:|---:|---:|---|
| `read_overhead_pct` | 0.7164 % | — | — | 0 | insufficient_history |
| `scan_overhead_ms` | -0.0031 ms | — | — | 0 | insufficient_history |
| `write_overhead_pct` | 1.1084 % | — | — | 0 | insufficient_history |
