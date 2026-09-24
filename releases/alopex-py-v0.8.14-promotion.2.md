# Alopex Python v0.8.14-promotion.2 Performance Report

Commit: `075fba0578c4ca614a59f05eb0f8dedd72f753da`
Measured at: 2026-09-24T08:31:27.101894+00:00
Profile: `gha-ubuntu-24.04-x64-single-core-v1`
CPU: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
Environment fingerprint: `d56d34aaac792ebd`
Workload signature: `597abb463084432c55db143a75c98ce3f203c44e8b7b3db2adeb300a894565cf`

# Alopex Python Performance Advisory

Overall status: **stable**
Issue notification ready: **false**

> Advisory only: performance regressions create or update an issue; they do not fail CI.

| Metric | Current | Baseline median | MAD | Samples | Status |
|---|---:|---:|---:|---:|---|
| `read_overhead_pct` | 0.5228 % | 0.6816 | 0.0956 | 5 | stable |
| `scan_overhead_ms` | 0.0010 ms | 0.0009 | 0.0001 | 5 | stable |
| `write_overhead_pct` | 0.5507 % | 0.8288 | 0.0552 | 5 | stable |
