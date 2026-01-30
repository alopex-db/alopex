# Chaos Matrix

This document describes the chaos matrix scenarios, scales, and timelines.

## Overview

The chaos matrix exercises multi-node failures with randomized short-interval injections. It combines:

- Network faults: partition, latency, and packet loss
- Node failures: kill and restart
- Zone outages: take down all nodes in a zone, optional restart
- Disk-full injection: emulate ENOSPC behavior

The matrix test name is `chaos_matrix_short_interval_n{scale}` and is executed across a set of
cluster sizes to validate scaling behavior.

## Scales

Scales are configured via:

- `STRESS_CHAOS_MATRIX_SCALES` (comma-separated, default: `3,5,7`)

Each scale value sets `ChaosMatrixConfig.nodes` for that run. A minimum of 3 nodes is enforced.

## Injection timeline

The test writes a timeline log at:

```
artifacts/<lane>/<test-name>/<timestamp>/logs/chaos_timeline.log
```

This file contains one line per injection step with timestamps and target details. It is only
written when `STRESS_ARTIFACTS_DIR` is enabled.

## Configuration

The chaos matrix uses the following environment variables:

- `STRESS_CHAOS_MATRIX_SCALES`: Cluster sizes to run (e.g., `3,5,7`).
- `STRESS_CHAOS_MATRIX_NODES`: Default node count (overridden per scale).
- `STRESS_CHAOS_MATRIX_ZONES`: Zone count.
- `STRESS_CHAOS_MATRIX_STEPS`: Number of workload steps.
- `STRESS_CHAOS_MATRIX_INJECT_MS`: Injection interval in milliseconds.
- `STRESS_CHAOS_MATRIX_MAX_LATENCY_MS`: Max injected link latency.
- `STRESS_CHAOS_MATRIX_LOSS_RATE`: Packet loss probability.
- `STRESS_CHAOS_MATRIX_PARTITION_RATE`: Partition probability.
- `STRESS_CHAOS_MATRIX_ZONE_OUTAGE_RATE`: Zone outage probability.
- `STRESS_CHAOS_MATRIX_ZONE_RESTART_RATE`: Restart probability after zone outage.
- `STRESS_CHAOS_MATRIX_KILL_RATE`: Single-node kill probability.
- `STRESS_CHAOS_MATRIX_RESTART_RATE`: Restart probability after kill.
- `STRESS_CHAOS_MATRIX_DISK_FULL_RATE`: Disk-full injection probability.

## Long-running chaos mode

The `chaos_long_running` scenario can be extended with:

- `STRESS_TEST_LONG_RUNNING=1`

This increases batch counts and duration for deeper burn-in coverage.
