"""Run the real manual runner against tiny Linux child fixtures, without mocks."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/compatibility"))
import storage

PHASES = ("seed", "index_build", "cold_transaction_1", "cold_transaction_2",
          "warm", "writer_begin", "first_insert", "second_insert", "rollback")


def run():
    if sys.platform != "linux" or os.getuid() == 0:
        raise RuntimeError("normal Linux user required")
    os.umask(0o077)
    work = Path(os.environ["RUNNER_TEMP"]) / "alopex-phase-runner"
    export = ROOT / "phase-runner-export"
    initial = storage.check(extra=[work, export])["allocated_bytes"]
    limit = initial + 64_000_000
    if limit >= storage.STOP_BYTES:
        raise RuntimeError("probe storage allowance unavailable")
    work.mkdir(exist_ok=False)
    export.mkdir(exist_ok=False)
    rows = []
    failure = None
    try:
        for case in ("complete", "missing_complete", "invalid_event", "timeout", "interrupt", "release", "madvise", "transient"):
            storage.check(extra=[work, export], limit=limit)
            events = [{"phase": phase, "data": {"event": kind}}
                      for phase in PHASES for kind in ("start", "end")]
            if case != "missing_complete":
                events.append({"phase": "complete", "data": {}})
            if case == "invalid_event":
                events = [{"phase": "rollback", "data": {"event": "end"}}]
            case_root = work / case
            case_root.mkdir()
            pid_file = case_root / "child.pid"
            fixture = case_root / "fixture"
            fixture.write_text(f"#!{sys.executable}\nimport json,mmap,os,sys,time\n"
                               f"open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
                               f"events={events!r}\n"
                               f"case={case!r}\n"
                               "allocation=None\n"
                               "for event in events:\n"
                               " print(json.dumps(event),flush=True)\n"
                               " if sys.stdin.readline().strip() != 'continue': sys.exit(9)\n"
                               " if event['phase']=='seed' and event['data'].get('event')=='start' and case in ('release','madvise','counter_reset_fault'):\n"
                               "  allocation=mmap.mmap(-1,8*1024*1024,flags=mmap.MAP_PRIVATE|mmap.MAP_ANONYMOUS)\n"
                               "  for offset in range(0,len(allocation),mmap.PAGESIZE): allocation[offset]=1\n"
                               " if event['phase']=='rollback' and event['data'].get('event')=='start' and allocation is not None:\n"
                               "  if case=='madvise': allocation.madvise(mmap.MADV_DONTNEED)\n"
                               "  else: allocation.close()\n"
                               " if event['phase']=='index_build' and event['data'].get('event')=='start' and case=='transient':\n"
                               "  transient=mmap.mmap(-1,8*1024*1024,flags=mmap.MAP_PRIVATE|mmap.MAP_ANONYMOUS)\n"
                               "  for offset in range(0,len(transient),mmap.PAGESIZE): transient[offset]=1\n"
                               "  transient.close()\n"
                               + ("time.sleep(20)\n" if case in ("timeout", "interrupt") else ""))
            fixture.chmod(0o700)
            output = case_root / "result"
            command = [sys.executable, "-B", str(ROOT / "scripts/performance/issue681_manual.py"),
                       str(fixture), "--output", str(output), "--rows", "1", "--dimensions", "1",
                       "--timeout", "3", "--phase-kernel-rss"]
            env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
            env.pop("CI", None)
            with (case_root / "runner.log").open("wb") as log:
                child = subprocess.Popen(command, env=env, stdout=log, stderr=log, start_new_session=True)
                try:
                    deadline = time.monotonic() + 15
                    interrupt_sent = False
                    while child.poll() is None:
                        if case == "interrupt" and not interrupt_sent:
                            event_log = output / "events.jsonl"
                            if event_log.exists() and len(event_log.read_text().splitlines()) == 19:
                                child.send_signal(signal.SIGTERM)
                                interrupt_sent = True
                        if time.monotonic() >= deadline:
                            raise RuntimeError("runner deadline")
                        try:
                            child.wait(timeout=1)
                        except subprocess.TimeoutExpired:
                            pass
                        storage.check(extra=[work, export], limit=limit)
                finally:
                    if child.poll() is None:
                        os.killpg(child.pid, signal.SIGTERM)
                        try:
                            child.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            os.killpg(child.pid, signal.SIGKILL)
                            child.wait(timeout=5)
            report = json.loads((output / "summary.json").read_text())
            events_saved = (output / "events.jsonl").read_text().splitlines()
            observed_decrease = any(row["kernel_high_water_decreased"] for row in report["phase_kernel_rss"])
            if case in ("complete", "release", "madvise", "transient"):
                assert child.returncode == 0 and report["outcome"] == "passed"
                assert [r["phase"] for r in report["phase_kernel_rss"]] == list(PHASES)
                assert all(r["kernel_reported_peak_rss_bytes"] > 0 for r in report["phase_kernel_rss"])
                assert len(events_saved) == 19
            else:
                assert child.returncode == 1 and report["outcome"] == "failed"
                expected = {"missing_complete": "incomplete manual workload phase coverage",
                            "invalid_event": "invalid manual workload phase sequence",
                            "timeout": "manual workload timeout",
                            "interrupt": "manual workload interrupted"}[case]
                assert report["failure"] == expected
                assert len(events_saved) == {"missing_complete": 18, "invalid_event": 1, "timeout": 19, "interrupt": 19}[case]
            if case == "transient":
                row = next(row for row in report["phase_kernel_rss"] if row["phase"] == "index_build")
                assert row["kernel_reported_peak_rss_bytes"] > row["after_reset"]["high_water_bytes"] + 4*1024*1024
                assert row["kernel_reported_peak_rss_bytes"] > row["after"]["rss_bytes"] + 4*1024*1024
            for row in report["phase_kernel_rss"]:
                assert row["kernel_reported_peak_rss_bytes"] == max(row["after_reset"]["high_water_bytes"], row["after"]["high_water_bytes"])
            assert report["children_lifetime_peak_rss_bytes"] is None and report["kernel_rss_reset"]
            assert not Path("/proc", pid_file.read_text()).exists(), "workload was not reaped"
            rows.append({"case": case, "status": "passed", "kernel_phases": len(report["phase_kernel_rss"]),
                         "child_reaped": True, "saved_events": len(events_saved), "observed_decrease": observed_decrease, "fault_injected": False})
    except BaseException as error:
        failure = type(error).__name__
    finally:
        archive = work / "evidence.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            for path in sorted(work.iterdir()):
                if path.is_dir():
                    tar.add(path, arcname=path.name)
        subprocess.run(["openssl", "cms", "-encrypt", "-aes-256-cbc", "-binary", "-outform", "DER",
                        "-in", str(archive), "-out", str(export / "private-evidence.cms"),
                        str(ROOT / ".github/ops/native-evidence-cert.pem")],
                       check=True, capture_output=True, timeout=20)
        storage.check(extra=[work, export], limit=limit)
        hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in (
            "scripts/performance/issue681_manual.py", "scripts/performance/phase_rss.py",
            "scripts/compatibility/storage.py", "scripts/ops/hosted_phase_runner_probe.py")}
        shutil.rmtree(work)
        storage.check(extra=[export], limit=limit)
        (export / "result.json").write_text(json.dumps({
            "schema": "alopex.phase-runner-runtime/v3", "status": "passed" if failure is None else "failed",
            "error_type": failure, "controller": os.environ["GITHUB_SHA"], "sources": hashes,
            "cases": rows, "fixture_cleanup": True, "counter_source": "actual Linux proc",
            "storage_source": "actual storage.inventory with outer VM guard", "performance_acceptance": False}, indent=2))
    return 0 if failure is None else 1


if __name__ == "__main__":
    try:
        status = run()
    except BaseException:
        status = 1
    print(json.dumps({"status": "passed" if status == 0 else "failed"}))
    raise SystemExit(status)
