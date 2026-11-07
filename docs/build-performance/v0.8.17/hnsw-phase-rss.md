# HNSW first-write phase RSS observation

The manual #681 runner accepts `--phase-kernel-rss` on Linux. It resets the
child's RSS high-water mark while the child is waiting at each start handshake,
then reads the mark at the matching end handshake. The existing operation
timers and sampled RSS observations are preserved.

```sh
python3 scripts/performance/issue681_manual.py "$BINARY" \
  --output "$PRIVATE_OUTPUT" --rows "$ROWS" --dimensions "$DIMENSIONS" \
  --timeout "$TIMEOUT_SECONDS" --rss-limit "$RSS_LIMIT_BYTES" --phase-kernel-rss
```

The binary must have the existing `issue681_hnsw_first_write` event protocol.
The runner records its hash, source identity and observer-module hash. The
existing manual execution, storage, time and RSS limits still apply. No load
workload is added to normal CI.

## Measurement contract

- The observer opens the child process directory once and checks its start
  ticks. A reused PID path cannot redirect an existing descriptor to a new
  process. The process must belong to the runner's UID.
- `clear_refs=5` resets the process high-water mark to its current RSS. It does
  not change cgroups, capabilities, host settings or page access bits.
- Each record retains values before reset, after reset and at the phase end.
  Missing counters, rejected resets, identity changes and incomplete phase
  coverage fail the run. A reported phase peak at the RSS limit also fails.
  The phase value is the maximum of the reset and end `VmHWM` readings. Linux
  combines a stored high-water value with approximate RSS accounting; the
  reported value can decrease during ordinary page release. Both raw readings
  and `kernel_high_water_decreased` are retained. The preceding phase's value
  before reset is excluded from this phase's maximum.
  `incomplete_kernel_phase` preserves the last attempted phase and every
  available reset/end counter even when validation fails. An unavailable end
  reading remains absent. This evidence survives child cleanup.
- Both sampled and kernel modes require all nine start/end pairs in order,
  followed by exactly one `complete` handshake and a zero process exit.
  The runner drains stdout through EOF and saves each raw event before parsing
  or reading counters, so a rejected event remains available for diagnosis.
  SIGTERM enters the failure path, stops and waits for the workload process
  group, and retains the interruption in the private report.
- The peak is the **kernel-reported process RSS high-water mark over the
  handshake interval**, including end-event serialization. It is not an exact
  allocation peak or an HNSW-only memory value. Kernel RSS accounting accuracy
  applies; sampled RSS values remain sampled lower bounds. The workload must
  not reset its own counters: this observer is for source-identified cooperative
  workloads, and cannot distinguish another reset from accounting variation.
- Resetting the high-water mark affects subsequent process lifetime reporting.
  With this option, `children_lifetime_peak_rss_bytes` is null. The raw
  `RUSAGE_CHILDREN` result is retained under a separate name and is not used as
  a lifetime peak; it also includes monitor utilities.

Small synthetic-file tests verify parsing, phase order, identity checks and
failure handling. Tiny child-process tests exercise the runner's handshakes,
missing/duplicate events, observer failure, kernel limit, timeout, nonzero exit
and child reaping. Their storage and kernel counters are injected; they do not
qualify product performance. Linux container execution must additionally verify
actual reset behavior before using this mode for #681 acceptance.

References: [clear_refs](https://man7.org/linux/man-pages/man5/proc_pid_clear_refs.5.html),
[process status and RSS accuracy](https://man7.org/linux/man-pages/man5/proc_pid_status.5.html),
[process-directory descriptor lifetime](https://www.kernel.org/doc/html/latest/filesystems/proc.html).

The small [release/madvise diagnostic](https://github.com/alopex-db/alopex/actions/runs/42424344878)
reproduced a 212,992-byte decrease in both cases with an 8 MiB allocation and
no workload-side reset. The old monotonicity check rejected both valid releases.
An explicitly injected reset was recorded separately. This demonstrates the
invalid monotonicity assumption, not the exact kernel cause of the earlier
100,000-row observation. See Linux's [status snapshot implementation](https://github.com/torvalds/linux/blob/v6.8/fs/proc/task_mmu.c#L30-L50)
and [RSS/high-water accounting](https://github.com/torvalds/linux/blob/v6.8/include/linux/mm.h#L2393-L2466).
