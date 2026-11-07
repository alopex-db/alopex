#!/usr/bin/env python3
"""Bounded local #681 measurement, explicitly excluded from normal CI.

Run a prebuilt issue681_hnsw_first_write example on macOS/Linux. Phase handshakes
capture boundary RSS before work resumes; intermediate peaks are sampled.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import resource
import signal
import subprocess
import sys
import threading
import time

from phase_rss import PhaseRss

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/compatibility"))
import storage

PHASES = ("seed", "index_build", "cold_transaction_1", "cold_transaction_2",
          "warm", "writer_begin", "first_insert", "second_insert", "rollback")
EXPECTED_EVENTS = tuple((phase, kind) for phase in PHASES for kind in ("start", "end")) + (("complete", None),)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--dimensions", type=int, default=384)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--rss-limit", type=int, default=8_000_000_000)
    parser.add_argument("--phase-kernel-rss", action="store_true",
                        help="record Linux process RSS high-water marks between phase handshakes")
    args = parser.parse_args()
    if os.environ.get("CI") or not (1 <= args.rows <= 100_000 and 1 <= args.dimensions <= 384):
        parser.error("manual local run only; rows<=100000, dimensions<=384")
    if not (1 <= args.timeout <= 1800 and 1 <= args.rss_limit <= 8_000_000_000):
        parser.error("timeout<=1800 seconds and RSS<=8 GB required")
    if sys.platform not in ("darwin", "linux"):
        parser.error("this process monitor supports macOS/Linux only")
    if args.phase_kernel_rss and sys.platform != "linux":
        parser.error("phase kernel RSS requires Linux proc counters")
    args.output.mkdir(parents=True, exist_ok=False)
    inventory = storage.inventory([args.output])
    if inventory["allocated_bytes"] + 1_000_000_000 >= storage.STOP_BYTES:
        raise RuntimeError("insufficient measured host storage budget")
    binary = args.binary.resolve(strict=True)
    identity = {"source":subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
                "binary_sha256":hashlib.sha256(binary.read_bytes()).hexdigest(),
                "example_source_sha256":hashlib.sha256((ROOT / "crates/alopex-embedded/examples/issue681_hnsw_first_write.rs").read_bytes()).hexdigest(),
                "runner_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "phase_rss_source_sha256":hashlib.sha256(Path(__file__).with_name("phase_rss.py").read_bytes()).hexdigest(),
                "rows":args.rows,"dimensions":args.dimensions,"rss_limit":args.rss_limit,
                "phase_kernel_rss":args.phase_kernel_rss,
                "timeout_seconds":args.timeout,"initial_inventory":inventory}
    (args.output / "identity.json").write_text(json.dumps(identity,indent=2))
    events = queue.Queue()
    phases = {}
    kernel_phases = []
    kernel_observer = None
    phase = "startup"
    outcome = "failed"
    failure = None
    event_count = 0
    stream_closed = False
    started = time.monotonic()
    last_inventory = started
    def rss(pid):
        value = subprocess.check_output(["ps","-o","rss=","-p",str(pid)],text=True).strip()
        if not value.isdigit():
            raise RuntimeError("process RSS measurement unavailable")
        return int(value) * 1024
    with (args.output / "stderr.log").open("w") as stderr, (args.output / "events.jsonl").open("w") as event_log, (args.output / "rss.jsonl").open("w") as rss_log:
        process = subprocess.Popen([str(binary),str(args.rows),str(args.dimensions)],cwd=ROOT,
            env=dict(os.environ,ALOPEX_MANUAL_681="1",RAYON_NUM_THREADS="2",OMP_NUM_THREADS="2"),
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True,start_new_session=True)
        def read_events():
            try:
                for line in process.stdout:
                    events.put(line)
            finally:
                events.put(None)
        reader = threading.Thread(target=read_events,daemon=True)
        reader.start()
        try:
            if args.phase_kernel_rss:
                kernel_observer = PhaseRss(process.pid)
            while not stream_closed or process.poll() is None:
                if time.monotonic()-started > args.timeout:
                    raise RuntimeError("manual workload timeout")
                try:
                    line = events.get(timeout=0.1)
                    if line is None:
                        stream_closed = True
                except queue.Empty:
                    line = None
                event = None
                if line is not None:
                    # Preserve the offending event even if parsing or observation fails.
                    event_log.write(line)
                    event_log.flush()
                    event = json.loads(line)
                    if (not isinstance(event, dict) or not isinstance(event.get("data"), dict) or
                            event_count >= len(EXPECTED_EVENTS) or
                            (event.get("phase"), event["data"].get("event")) != EXPECTED_EVENTS[event_count]):
                        raise RuntimeError("invalid manual workload phase sequence")
                    event_count += 1
                if event:
                    phase = event["phase"]
                    if kernel_observer is not None:
                        if event["data"].get("event") == "start":
                            kernel_observer.begin(phase)
                        elif event["data"].get("event") == "end":
                            kernel_phases.append(kernel_observer.finish(phase))
                            if kernel_phases[-1]["kernel_reported_peak_rss_bytes"] >= args.rss_limit:
                                raise RuntimeError("manual workload kernel RSS limit exceeded")
                        elif phase != "complete":
                            raise RuntimeError("invalid kernel RSS phase event")
                    print(line.strip(),flush=True)
                if process.poll() is None:
                    try:
                        current = rss(process.pid)
                    except subprocess.CalledProcessError:
                        if process.poll() is not None:
                            continue
                        raise
                    phases[phase] = max(phases.get(phase,0),current)
                    rss_log.write(json.dumps({"seconds":time.monotonic()-started,"phase":phase,"rss_bytes":current})+"\n")
                    rss_log.flush()
                    if current >= args.rss_limit:
                        raise RuntimeError("manual workload RSS limit exceeded")
                if time.monotonic()-last_inventory >= 30:
                    inventory = storage.inventory([args.output])
                    with (args.output / "storage.jsonl").open("a") as log:
                        log.write(json.dumps(inventory)+"\n")
                    if inventory["allocated_bytes"] >= storage.STOP_BYTES:
                        raise RuntimeError("host storage stop threshold reached")
                    last_inventory = time.monotonic()
                if event:
                    process.stdin.write("continue\n")
                    process.stdin.flush()
            code = process.wait()
            if code != 0:
                raise RuntimeError(f"manual workload exited {code}")
            if event_count != len(EXPECTED_EVENTS):
                raise RuntimeError("incomplete manual workload phase coverage")
            if kernel_observer is not None:
                if kernel_observer.active is not None or [row["phase"] for row in kernel_phases] != list(PHASES):
                    raise RuntimeError("incomplete kernel RSS phase coverage")
            outcome = "passed"
        except Exception as error:
            failure = str(error)
        finally:
            if process.poll() is None:
                os.killpg(process.pid,signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGKILL)
                    process.wait()
            reader.join(timeout=2)
            process.stdin.close()
            process.stdout.close()
            if kernel_observer is not None:
                kernel_observer.close()
    peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    report = {"outcome":outcome,"failure":failure,"phase_sampled_peak_rss_bytes":phases,
              "children_lifetime_peak_rss_bytes":None if args.phase_kernel_rss else (peak if sys.platform=="darwin" else peak*1024),
              "children_rusage_reported_maxrss_bytes":peak if sys.platform=="darwin" else peak*1024,
              "phase_kernel_rss":kernel_phases,
              "kernel_rss_reset":args.phase_kernel_rss,
              "peak_scope":"RUSAGE_CHILDREN includes workload and monitor utilities; when kernel reset is enabled its reported value is not a lifetime peak. Sampled phase values remain lower bounds. Kernel phase values cover the handshake window and have kernel RSS accounting accuracy.",
              "final_inventory":storage.inventory([args.output])}
    (args.output / "summary.json").write_text(json.dumps(report,indent=2))
    print(json.dumps({**{k:v for k,v in report.items() if k != "final_inventory"},
                      "final_host_bytes":report["final_inventory"]["allocated_bytes"]}),flush=True)
    return 0 if outcome == "passed" else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise InterruptedError("manual workload interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
