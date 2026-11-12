"""Read-only #682 schedstat capability probe; never qualifies a later job."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

PUBLIC_CODES = {"SCHEDSTAT_ADVANCING", "SCHEDSTAT_ZERO", "SCHEDSTAT_MISSING",
                "SCHEDSTAT_STATIC", "SCHEDSTAT_UNREADABLE", "SCHEDSTAT_DISABLED",
                "SCHEDSTAT_WAIT_UNPROVEN", "SCHEDSTAT_AFFINITY_UNAVAILABLE",
                "PROBE_FAILED"}


def read_schedstat(path: Path) -> dict:
    try:
        if not path.is_file():
            return {"status": "missing"}
        if path.stat().st_size > 128:
            return {"status": "unreadable"}
        raw = path.read_text().strip()
        fields = raw.split()
        if len(fields) != 3 or any(not field.isdecimal() for field in fields):
            return {"status": "unreadable", "raw": raw}
        return {"status": "read", "raw": raw, "values": [int(field) for field in fields]}
    except OSError:
        return {"status": "unreadable"}


def classify(before: dict, after: dict) -> str:
    if "missing" in (before["status"], after["status"]):
        return "missing"
    if before["status"] != "read" or after["status"] != "read":
        return "unreadable"
    old, new = before["values"], after["values"]
    if not any(old) and not any(new):
        return "zero"
    if any(new[index] > old[index] for index in range(3)) and all(
            new[index] >= old[index] for index in range(3)):
        return "advancing"
    return "static"


def counter_deltas(before: dict, after: dict) -> dict | None:
    if before["status"] != "read" or after["status"] != "read":
        return None
    differences = [new - old for old, new in zip(before["values"], after["values"])]
    if any(value < 0 for value in differences):
        return None
    return {"runtime_ns": differences[0], "runqueue_wait_ns": differences[1],
            "timeslices": differences[2]}


def sample(proc: Path, sibling_pid: int, advance) -> dict:
    paths = {"self": proc / "self/schedstat",
             "sibling": proc / str(sibling_pid) / "schedstat"}
    before = {name: read_schedstat(path) for name, path in paths.items()}
    advance()
    after = {name: read_schedstat(path) for name, path in paths.items()}
    return {name: {"before": before[name], "after": after[name],
                   "classification": classify(before[name], after[name]),
                   "deltas": counter_deltas(before[name], after[name])}
            for name in paths}


def capability_code(setting: dict, samples: dict, affinity: str = "pinned") -> str:
    if affinity != "pinned":
        return "SCHEDSTAT_AFFINITY_UNAVAILABLE"
    states = {item["classification"] for item in samples.values()}
    if "missing" in states or setting["status"] == "missing":
        return "SCHEDSTAT_MISSING"
    if "unreadable" in states or setting["status"] != "read":
        return "SCHEDSTAT_UNREADABLE"
    if setting["value"] != "1":
        return "SCHEDSTAT_DISABLED"
    if "zero" in states:
        return "SCHEDSTAT_ZERO"
    if states == {"advancing"}:
        if all(item.get("deltas") is not None and
               item["deltas"]["runqueue_wait_ns"] > 0 for item in samples.values()):
            return "SCHEDSTAT_ADVANCING"
        return "SCHEDSTAT_WAIT_UNPROVEN"
    return "SCHEDSTAT_STATIC"


def kernel_setting(proc: Path) -> dict:
    path = proc / "sys/kernel/sched_schedstats"
    try:
        if not path.is_file():
            return {"status": "missing"}
        if path.stat().st_size > 16:
            return {"status": "unreadable"}
        value = path.read_text().strip()
        return {"status": "read", "value": value} if value in ("0", "1") else {
            "status": "unreadable", "value": value}
    except OSError:
        return {"status": "unreadable"}


def collect(proc: Path = Path("/proc")) -> dict:
    facts = {"schema": "alopex.schedstat-capability/v1", "qualification": "not_assessed",
             "setting": kernel_setting(proc), "samples": {}, "affinity": "unavailable"}
    getter = getattr(os, "sched_getaffinity", None)
    setter = getattr(os, "sched_setaffinity", None)
    if not callable(getter) or not callable(setter):
        return facts
    try:
        previous = getter(0)
        if not previous:
            return facts
    except OSError:
        return facts
    selected = min(previous)
    try:
        setter(0, {selected})
    except OSError:
        return facts
    child = None
    try:
        child = subprocess.Popen(
            [sys.executable, "-B", "-c",
             "import time; end=time.monotonic()+0.8\nwhile time.monotonic()<end: pass"],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def advance():
            end = time.monotonic() + 0.2
            while time.monotonic() < end:
                pass

        facts["affinity"] = "pinned"
        facts["samples"] = sample(proc, child.pid, advance)
        return facts
    finally:
        try:
            if child is not None:
                child.terminate()
                try:
                    child.wait(timeout=0.5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=0.5)
        finally:
            setter(0, previous)


def main() -> int:
    export = Path("native-inventory-export")
    private = None
    code = "PROBE_FAILED"
    try:
        if os.environ.get("ISSUE_NUMBER") != "682":
            raise ValueError("wrong issue")
        private = Path(os.environ["RUNNER_TEMP"]) / "alopex-682-schedstat.private.json"
        facts = collect()
        code = capability_code(facts["setting"], facts["samples"], facts["affinity"])
        private.write_text(json.dumps(facts, sort_keys=True) + "\n")
        private.chmod(0o600)
        export.mkdir(mode=0o700, exist_ok=True)
        encrypted = export / "schedstat-private-evidence.cms"
        result = subprocess.run(
            ["openssl", "cms", "-encrypt", "-binary", "-aes256", "-outform", "DER",
             "-in", str(private), "-out", str(encrypted), ".github/ops/native-evidence-cert.pem"],
            stdin=subprocess.DEVNULL, capture_output=True, timeout=30, check=False)
        if result.returncode or not encrypted.is_file():
            raise RuntimeError("encryption_failed")
    except BaseException:
        code = "PROBE_FAILED"
    finally:
        if private is not None:
            try:
                private.unlink(missing_ok=True)
            except OSError:
                code = "PROBE_FAILED"
    assert code in PUBLIC_CODES
    public = {"schema": "alopex.schedstat-capability-public/v1",
              "case": "SCHEDSTAT_CAPABILITY", "code": code,
              "qualification": "not_assessed"}
    try:
        if export.is_dir():
            (export / "schedstat-public.json").write_text(json.dumps(public, sort_keys=True) + "\n")
    except OSError:
        code = "PROBE_FAILED"
        public["code"] = code
    print(json.dumps(public, sort_keys=True))
    return 1 if code == "PROBE_FAILED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
