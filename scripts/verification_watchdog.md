The READ_PARQUET verification entrypoint uses the host-monitoring watchdog:

```sh
rtk proxy python3 -B scripts/run_issue577_verification.py UNIQUE_RUN_ID issue577_
```

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
