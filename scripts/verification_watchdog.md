The READ_PARQUET verification entrypoint uses the host-monitoring watchdog:

```sh
rtk proxy python3 -B scripts/run_issue577_verification.py UNIQUE_RUN_ID issue577_
```

Resource boundaries are separate:

| Boundary and owner | Purpose and use | What the number means |
| --- | --- | --- |
| Caller `MemoryPolicy`, enforced by READ_PARQUET preflight and page reader | Reject declared reservations before page decoding under a finite caller budget; preserve the existing unlimited path when the limit is `None`. | The shared reservation includes row/value slots, compressed and uncompressed page bytes, dictionary slots, and DELTA expansion capacity across columns, pages, and row groups. This conservative reservation is not measured process RAM or proof that all metadata and native scratch allocations are bounded. |
| Allocation observer, owned by the integration-test executable | Detect regressions at the SQL execution boundary and check normal results under the same explicitly supplied budget. | The 64 KiB and 128 KiB cases are deliberately small fixture policies. `maximum_request` is one requested heap allocation; `rust_live_peak_bytes` is the peak sum of tracked live heap allocations on the executing thread. Neither is RSS. Planning, pre-existing allocations, other threads, stack storage, allocator overhead, and C internals are outside the ledger. |
| Verification watchdog and runner | Protect the machine while building and executing tests; stop the owned process tree if host reserves or unit limits are lost. | The unit's 4 GiB allowance and measured peak include setup/build/test. An exhausted allowance makes that run invalid; it cannot establish a product allocation failure. Cargo compilation and executed product assertions have separate result fields. |

The product reservation decides whether the reader may proceed; the test ledger
checks the selected execution scenarios; the watchdog decides whether the host
can safely run the verification. None of these owners may substitute its result
for another owner's result. Remaining coverage gaps belong to issue #577.
Ledger exhaustion invalidates the allocation measurement. Missing or zero-test
results are incomplete; a build failure leaves product tests unexecuted.
An OS unit exit of zero is insufficient without the child completion receipt;
the runner returns 125 when an interrupted unit leaves that receipt absent.
The receipt names environment RAM `verification_unit_memory_peak_bytes`.
The fixture budgets keep normal controls within the limit while wider values
exceed the combined budget even though every individual page fits. Repeating
the same layouts at 64 KiB and 128 KiB checks the caller-supplied limit rather
than a hard-coded threshold; these values prescribe no deployment sizing.

The finite-policy ZSTD reader also reserves its native context before allocating
an 8-byte-aligned, Rust-owned static workspace. The reservation includes alignment
rounding and is returned after decoding, including error paths. The fixed bundled
ZSTD 1.5.6 API guarantees that this static context cannot malloc or resize itself.
Normal ZSTD fixtures therefore allow the measured context size in addition to
their page/result allowance; the context-minus-one case must reject before allocation.
The workspace is now visible to the Rust allocation ledger. Older Rust-only
peaks excluded this native cost and are not total-memory comparison baselines.
This reader targets the [Parquet ZSTD format](https://github.com/apache/parquet-format/blob/master/Compression.md#zstd)
defined by RFC 8878. Static contexts reject older legacy frames; the unlimited
reader retains its existing decoding path. Experimental static-context bindings
are validated against the locked bundled library; substituted system libraries
are outside this verification.

Use a fresh run ID. The watchdog refuses to overwrite its previous event stream.
The watchdog writes its start record before observing the host or starting the unit.
The watchdog monitors Windows physical memory, conservative commit headroom and
whole-host CPU usage, and separately checks WSL available memory. Windows APIs are
documented in [GlobalMemoryStatusEx](https://learn.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-globalmemorystatusex)
and [GetSystemTimes](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getsystemtimes).

The watchdog reserves the greater of 8 GiB or 15% of Windows physical RAM, 4 GiB
of commit headroom, and 8 GiB of WSL available RAM. Before starting, the watchdog
also reserves the entire 4 GiB verification memory allowance. During execution,
the watchdog stops the unit if any reserve is lost, if host CPU usage stays at
90% or higher for 10 seconds, or if host observations are unavailable or older
than 5 seconds. The watchdog refuses to start on an already busy host.
The observer may take up to 30 seconds to initialize before the unit starts.
The watchdog starts no test without an observation; subsequent reads retain
the 5-second deadline.
The verification unit separately enforces 4 GiB RAM, no swap, one CPU equivalent,
128 tasks and a 600-second deadline. The watchdog stops the entire unit, including
its child processes. A failed cleanup is a failed run, not a successful stop.
The unit's OS watchdog receives heartbeats only while the parent event stream
continues to advance. The parent heartbeat allows 20 seconds for the 5-second
observer read, the 8-second unit query, and recording IO. The host observation
deadline and resource thresholds remain unchanged. Heartbeat notifications do
not wait for a systemd acknowledgement. If the parent stops recording for
20 seconds, heartbeats cease and the OS watchdog terminates the unit after its 4-second deadline.
The unit log records whether the parent heartbeat expired or notification failed.
The unit uses SIGTERM and a short abort timeout, rather than creating a core dump.

The watchdog fsyncs start, host/unit samples, stop decisions and final events to
`target/verification/issue577-focused/UNIQUE_RUN_ID-events.jsonl`. The watchdog
also saves a final `UNIQUE_RUN_ID-watch.json`, a unit log and the existing focused
verification receipt. An absent final event means the run was incomplete; saved
events remain evidence after a PC shutdown. The Windows observer exits when its
input closes, with a 10-second heartbeat deadline as a fallback.

The watchdog cannot prevent Windows Update, loss of power, or every hardware
failure. CPU monitoring is not temperature monitoring. The watchdog does not
change host update settings or system-wide limits.

Focused behavior checks use only short-lived, small Python processes and injected
observations, never actual host resource exhaustion:

```sh
rtk proxy python3 -B -m unittest discover -s scripts -p test_verification_watchdog.py
```

The checks own launch rejection, pressure/monitor-loss/interruption stops,
descendant cleanup, successful/nonzero exits, launch failure and deadline expiry.
Their dependencies are the Python standard library and the existing systemd user
manager. Their temporary-output budget is 10 MiB; the checks remove their own
temporary directories and units. Historical probes are retained as evidence,
while the existing entrypoint uses this watchdog as its sole normal launch path.

| Owner/file | Responsibility and lifecycle | Verification |
| --- | --- | --- |
| `run_issue577_verification.py` | The existing runner keeps target selection and owned build-output cleanup; its former launch loop delegates to the watchdog. | One focused READ_PARQUET test through the entrypoint. |
| `verification_watchdog.py` | The watchdog owns policy decisions, unit stops, durable records and observer cleanup. | Small success, failure, pressure, interruption and heartbeat cases. |
| `host_resources.py`, `windows_host_observer.ps1` | The observer owns host facts and exits on input closure or missing heartbeat. | Real host sampling and delayed-reader freshness. |
| Per-run events, receipts and logs | The run artifacts retain both failed and passing runs for review. | Readback, source/log digests and explicit final status. |
| Legacy local entrypoint | The legacy entrypoint redirects to the tracked runner; it retains no separate launch implementation. | Its uncontrolled-launch refusal check. |
