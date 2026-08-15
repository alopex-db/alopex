# Alopex Python v0.8.5-repair-1 Performance Report

Commit: `f53a65d191201bfe5774409d31d1a5c808bcec27`
Measured at: 2026-08-15T01:55:42.450273+00:00
Profile: `gha-ubuntu-24.04-x64-single-core-v1`
CPU: INTEL(R) XEON(R) PLATINUM 8573C
Environment fingerprint: `579e8febe7deb59c`
Workload signature: `4f278ef6c10700e4cb878107f99c2eb944f98f31bbe9ea60a9c88a95ffe8fdf1`

# Alopex Python Performance Advisory

Overall status: **insufficient_history**
Issue notification ready: **false**

> Advisory only: performance regressions create or update an issue; they do not fail CI.

| Metric | Current | Baseline median | MAD | Samples | Status |
|---|---:|---:|---:|---:|---|
| `read_overhead_pct` | 0.7911 % | — | — | 0 | insufficient_history |
| `scan_overhead_ms` | 0.0011 ms | — | — | 0 | insufficient_history |
| `write_overhead_pct` | 0.4168 % | — | — | 0 | insufficient_history |
