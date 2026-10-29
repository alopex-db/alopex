#!/usr/bin/env python3
"""Bounded serial macOS supervisor for public SQL diagnosis, outside CI."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/compatibility"))
from storage import check


def events(path):
    # The child writes complete lines; ignore only an in-flight trailing line.
    lines = path.read_text().splitlines(keepends=True)
    return [json.loads(line) for line in lines if line.endswith("\n")]


def stop_group(child):
    """Stop owned descendants even if the direct child already exited."""
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        child.wait(timeout=10)
        return
    deadline = time.monotonic()+10
    while time.monotonic() < deadline:
        child.poll()  # Reap the direct child before probing the process group.
        try:
            os.killpg(child.pid, 0)
        except ProcessLookupError:
            break
        time.sleep(.1)
    else:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    child.wait(timeout=10)


def main(*, driver_name="sql_delete_profile.py", row_choices=(3, 1000, 10000),
         default_rows=(3, 1000, 10000), operations=("noop", "query", "delete"),
         profile_operations=("delete",), row_count=lambda operation, rows: rows,
         scope="internal diagnostic; profile sample covers operation/restore/validation, not isolated DELETE"):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, nargs="+", choices=row_choices, default=default_rows)
    parser.add_argument("--operations", nargs="+", choices=operations, default=operations)
    parser.add_argument("--surfaces", nargs="+", choices=["embedded", "server"], default=["embedded", "server"])
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    if sys.platform != "darwin" or os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS"):
        parser.error("manual macOS diagnosis only; never run as CI")
    if args.profile and not set(args.operations).issubset(profile_operations):
        parser.error("profile operations must be selected from "+", ".join(profile_operations))
    args.output.mkdir(parents=True, exist_ok=False)
    initial = check(args.output / "storage.jsonl")["allocated_bytes"]
    if initial+2_000_000_000 >= 40_000_000_000:
        raise RuntimeError("insufficient measured host budget")
    initial_used = shutil.disk_usage(ROOT).used
    identity = dict(source_sha=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                    server_sha256=hashlib.sha256(args.server.read_bytes()).hexdigest(),
                    supervisor_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    driver_sha256=hashlib.sha256((ROOT / "scripts/performance" / driver_name).read_bytes()).hexdigest(),
                    entrypoint_sha256=hashlib.sha256(Path(sys.argv[0]).read_bytes()).hexdigest(), scope=scope)
    (args.output / "identity.json").write_text(json.dumps(identity, indent=2)+"\n")
    records = []
    seen = set()
    for rows in args.rows:
        for surface in args.surfaces:
            for operation in args.operations:
                fixture_rows = row_count(operation, rows)
                name = f"{surface}-{fixture_rows}-{operation}"
                if name in seen:
                    continue
                seen.add(name)
                path = args.output / (name+".jsonl")
                record = dict(case=name, surface=surface, rows=fixture_rows, operation=operation, profile=args.profile)
                with tempfile.TemporaryDirectory(prefix="alopex-712-case-") as tmp:
                    work = Path(tmp) / "work"
                    command = [str(args.python), "-B", str(ROOT / "scripts/performance" / driver_name), "--manual",
                               "--surface", surface, "--rows", str(fixture_rows), "--operation", operation,
                               "--work", str(work), "--server-bin", str(args.server)]
                    if args.profile:
                        command.append("--profile")
                    env = dict(os.environ, RAYON_NUM_THREADS="2", TOKIO_WORKER_THREADS="2", OMP_NUM_THREADS="2",
                               OPENBLAS_NUM_THREADS="1", PYTHONDONTWRITEBYTECODE="1", TMPDIR=tmp)
                    sampler = None
                    failure = None
                    peak_rss = {}
                    start = time.monotonic()
                    with path.open("w") as out, (args.output / (name+".stderr")).open("w") as err:
                        child = subprocess.Popen(command, stdout=out, stderr=err, env=env, start_new_session=True)
                        try:
                            while child.poll() is None:
                                if time.monotonic()-start > 180:
                                    raise RuntimeError("180-second case deadline")
                                growth = max(0, shutil.disk_usage(ROOT).used-initial_used)
                                if growth > 2_000_000_000 or initial+growth >= 40_000_000_000:
                                    raise RuntimeError("host storage stop")
                                fixture_bytes = sum(p.stat().st_size for p in work.rglob("*") if p.is_file()) if work.exists() else 0
                                if fixture_bytes > 2_000_000_000:
                                    raise RuntimeError("2 GB logical fixture stop")
                                observed = events(path)
                                pids = {"client": child.pid}
                                for event in observed:
                                    if event.get("server_pid"):
                                        pids["server"] = event["server_pid"]
                                for role, pid in pids.items():
                                    read = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
                                    if read.returncode:
                                        if child.poll() is not None or any(e["event"] == "server_stopped" for e in events(path)):
                                            continue
                                        raise RuntimeError("cannot measure live "+role+" RSS: "+read.stderr)
                                    rss = int(read.stdout.strip())*1024
                                    peak_rss[role] = max(rss, peak_rss.get(role, 0))
                                    if rss >= 2*1024**3:
                                        raise RuntimeError(role+" 2 GiB RSS stop")
                                if args.profile and sampler is None and any(e["event"] == "ready" for e in observed):
                                    sampler = subprocess.Popen(["sample", str(pids.get("server", child.pid)), "3", "1", "-file",
                                                               str(args.output / (name+".sample.txt"))], stdout=err, stderr=err)
                                time.sleep(.1)
                            if child.returncode:
                                raise RuntimeError(f"child exit {child.returncode}")
                            observed = events(path)
                            complete = [e for e in observed if e["event"] == "complete"]
                            if len(complete) != 1:
                                raise RuntimeError("missing completed verification")
                            if surface == "server" and not any(e["event"] == "server_stopped" for e in observed):
                                raise RuntimeError("missing server shutdown receipt")
                            measured = [e for e in observed if e["event"] == "sample" and e["phase"] == "measure"]
                            if not args.profile and (len(measured) != 21 or complete[0]["iterations"] != 24):
                                raise RuntimeError("missing measured samples")
                            if sampler:
                                sampler.wait(timeout=10)
                                if sampler.returncode:
                                    raise RuntimeError("stack sampler failed")
                            record.update(result=complete[0], status="passed")
                        except BaseException as exc:
                            failure = str(exc)
                            record.update(status="failed", error=failure)
                        finally:
                            stop_group(child)
                            if sampler and sampler.poll() is None:
                                sampler.kill()
                                sampler.wait(timeout=10)
                    # Keep compact server logs before removing all data and config.
                    for log in ("server.stdout", "server.stderr", "client.profile.txt"):
                        if (work / log).exists():
                            shutil.copyfile(work / log, args.output / (name+"."+log))
                    record.update(exit_code=child.returncode, elapsed_seconds=time.monotonic()-start,
                                  peak_sampled_rss_bytes=peak_rss)
                record["fixture_removed"] = not Path(tmp).exists()
                records.append(record)
                (args.output / "results.json").write_text(json.dumps(records, indent=2)+"\n")
                check(args.output / "storage.jsonl")
                print(json.dumps(record), flush=True)
                if failure:
                    raise RuntimeError(failure)


if __name__ == "__main__":
    main()
