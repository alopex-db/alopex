The READ_PARQUET comparison controller uses the host-monitoring watchdog:

```sh
rtk proxy python3 -B scripts/performance/issue577_matrix.py PROFILE FIXTURES BUNDLE OUTPUT
```

The bundle must contain a fresh inventory baseline for the execution host and
explicit filesystem counters in `storage.filesystems`, each with `path` and
`used_at_baseline`. The baseline includes project worktrees, build outputs,
known external temporary directories and caches. WSL and native Windows project
outputs count toward the same physical host budget. Another machine has its own
budget. The controller requires the worktree filesystem and all Windows drive
mounts exposed under `/mnt`; missing coverage and old scalar-only bundles fail
before a case starts.

`output_budget.py` checks constant-cost filesystem counters during admission and
execution. The estimate adds positive growth on each volume to the inventory;
a shrink on one volume cannot hide growth on another. Backing-volume overlap
may conservatively overestimate growth. The controller stops at 40,000,000,000 B,
keeps at least 16 GiB free on every monitored volume, and enforces the declared
per-run output allowance. The 50 GiB policy remains an emergency maximum. The
monitor performs no recursive scans or deletion.

The historical `run_issue577_verification.py` launcher now exits 125 before
launching a child because its repository-only inventory omitted host outputs.
Callers must use the host-budgeted controller or supply the same inventory,
resource policy, bounded command and owned-output cleanup to `run_guarded`.

Resource boundaries are separate:

| Boundary and owner | Purpose and use | What the number means |
| --- | --- | --- |
| Caller `MemoryPolicy`, enforced by READ_PARQUET preflight and page reader | Reject declared reservations before page decoding under a finite caller budget; preserve the existing unlimited path when the limit is `None`. | The shared reservation includes retained File KV declared slots/payload, row/value slots, compressed and uncompressed page bytes, dictionary slots, DELTA expansion capacity across columns, pages, and row groups, and ZSTD/Brotli decoding workspace. This conservative reservation is not measured process RAM or proof that all metadata and codec allocations are bounded. |
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

The finite-policy Brotli reader reserves all three fixed Huffman tables before
entering the pinned 4.0.3 decoder. Its custom allocators charge live byte, integer,
and Huffman buffers to the same reader budget and return charges after freeing
storage. A shared rejection flag prevents later dependent allocations from
resuming after any refused request. The aggregate-workspace fixture uses explicit
6 MiB and 12 MiB policies: the default 4 MiB window fits individually, but its sum
with a 2 MiB page exceeds the smaller policy. These are test conditions, not a
product default or deployment sizing recommendation.

Footer preflight checks raw footer bytes plus cumulative Thrift list slots and
string/binary payloads before the normal metadata decoder runs. It returns the
footer offset and the File KV declaration reservation separately. When the footer
has no `ARROW:schema` key, the finite SQL reader skips unused Arrow root metadata:
CopySchema exposes field names/types and rows, so ordinary KV need not be cloned
into an Arrow HashMap. The Parquet KV copy remains reserved with rows/pages/codecs.
When the key is present, including a missing value, the reader keeps the existing
Arrow schema decoding path and its conservative double-copy KV declaration.
The unlimited reader and planning helpers retain their existing paths.
Column KV declarations do not receive an Arrow copy charge. Metadata fixtures
use 64 KiB and 256 KiB policies and one/512-row controls for retained overlap;
513 short KV pairs check the previously unbudgeted Map construction peak.
The SQL materializer releases exhausted input iterators before projecting rows
and transfers the values buffer for full-column output. Projection consumes
owned rows rather than retaining every input value while cloning the complete
result. Computed projections use the existing expression kernel; outer-row and
subquery evaluation retain their transaction-aware path. The 512-row control checks this execution boundary,
including output values and column names, rather than compilation memory.
This increment does not establish bounds for other retained metadata, schema
conversion, `ARROW:schema` expansion, or Map storage in that embedded-schema path.
These remaining allocations still prevent a whole-reader memory-bound claim.

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
the 5-second deadline. Each request has an increasing identifier that the host
echoes only after receiving it and sampling resources. The observer ignores
responses to previous requests and measures the round trip with the guest's
monotonic clock. Windows Stopwatch and WSL clock drift cannot accumulate into
an observation-age error; missing or late correlated responses still stop the run.
The verification unit separately enforces 4 GiB RAM, no swap, one CPU equivalent,
128 tasks, with no elapsed execution-time limit.

The Windows observer remains available
while its request pipe is open and exits on EOF; it has no idle deadline.
The caller still requires a fresh correlated observation before continuing.

For a generic verification command, launch
`python3 -B scripts/verification_watchdog.py --unit-child EVENTS COMMAND [ARGS...]`
inside a `Type=notify`, `NotifyAccess=all` unit with the existing OS watchdog.
The wrapper renews that watchdog only while the parent's event stream advances
and propagates the command's exit status.

For a native Windows command, that unit runs
`python3 -B scripts/windows_job_bridge.py SPEC.json`. The bridge keeps a control
pipe open to `windows_job_runner.ps1`. The specification contains the executable,
an argument array, working directory, environment overlay, command log and final
receipt filenames. Logs and receipts must be new files. The caller still owns
the host inventory, storage allowance and `run_guarded` admission policy.

The Windows guardian creates the command suspended, assigns it to a Job, then
resumes it. The Job limits aggregate committed memory to 4 GiB, CPU to one logical
processor equivalent, and active processes to 128. The guardian reads the limits
back before starting the command. A normal root exit also terminates descendants.
EOF or a lost bridge heartbeat terminates the Job; closing the guardian's last
Job handle gives kernel-enforced termination if the guardian itself exits.
The command receives NUL input and cannot inherit the controlling input pipe.
The guardian flushes its final receipt before publishing it by atomic rename.
The heartbeat and cleanup deadlines do not impose a total build/test runtime.

The focused Windows lifecycle target exercises real, small processes:

```sh
rtk proxy python3 -B scripts/windows_job_checks.py UNIQUE_OUTPUT_DIR
```

The caller runs this target inside the same host-budgeted watchdog unit. The
six cases cover normal/nonzero exits, descendant cleanup, control EOF, bridge
loss and guardian loss. The target checks process creation time as well as PID.
The target requires existing Windows PowerShell/.NET, WSL interop and Python's
standard library; it needs no Windows Python installation. The target retains
compact specifications and receipts as evidence and leaves no running fixtures.

The watchdog stops the entire unit, including
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
input closes.

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
while current comparison commands use this watchdog for controlled launches.

| Owner/file | Responsibility and lifecycle | Verification |
| --- | --- | --- |
| `run_issue577_verification.py` | Reject historical invocations before launch. | CLI exit 125. |
| `output_budget.py`, `performance/issue577_matrix.py` | Monitor per-host filesystem growth and reject incomplete coverage before a measured case. | Cross-volume growth, missing coverage, reserve stops and matrix lifecycle. |
| `verification_watchdog.py` | The watchdog owns policy decisions, unit stops, durable records and observer cleanup. | Small success, failure, pressure, interruption and heartbeat cases. |
| `host_resources.py`, `windows_host_observer.ps1` | The observer owns host facts and exits on input closure. | Real host sampling and delayed-reader freshness. |
| `windows_job_bridge.py`, `windows_job_runner.ps1` | The bridge owns the control pipe; the guardian owns the native Windows Job and atomic final receipt. | Real Windows exit, EOF, bridge-loss and guardian-loss cases in `windows_job_checks.py`. |
| Per-run events, receipts and logs | The run artifacts retain both failed and passing runs for review. | Readback, source/log digests and explicit final status. |
