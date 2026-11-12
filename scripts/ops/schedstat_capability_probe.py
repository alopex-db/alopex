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


def sample(proc: Path, sibling_pid: int, advance) -> dict:
    paths = {"self": proc / "self/schedstat",
             "sibling": proc / str(sibling_pid) / "schedstat"}
    before = {name: read_schedstat(path) for name, path in paths.items()}
    advance()
    after = {name: read_schedstat(path) for name, path in paths.items()}
    return {name: {"before": before[name], "after": after[name],
                   "classification": classify(before[name], after[name])}
            for name in paths}


def capability_code(setting: dict, samples: dict) -> str:
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
        return "SCHEDSTAT_ADVANCING"
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
    child = subprocess.Popen(
        [sys.executable, "-B", "-c",
         "import time; end=time.monotonic()+1.0\nwhile time.monotonic()<end: pass"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        def advance():
            end = time.monotonic() + 0.2
            while time.monotonic() < end:
                pass

        return {"schema": "alopex.schedstat-capability/v1", "qualification": "not_assessed",
                "setting": kernel_setting(proc), "samples": sample(proc, child.pid, advance)}
    finally:
        child.terminate()
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=2)


def main() -> int:
    export = Path("native-inventory-export")
    private = None
    code = "PROBE_FAILED"
    try:
        if os.environ.get("ISSUE_NUMBER") != "682":
            raise ValueError("wrong issue")
        private = Path(os.environ["RUNNER_TEMP"]) / "alopex-682-schedstat.private.json"
        facts = collect()
        code = capability_code(facts["setting"], facts["samples"])
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
