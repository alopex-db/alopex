"""Private CPU, counter and storage admission for a selected #681 runner.

This does not construct or approve the selected security environment.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import platform
import re
import selectors
import signal
import stat
import subprocess
import sys
import time

import issue681_public as case
import issue681_public_pair as pair
from phase_rss import PhaseRss

SCHEMA = "alopex.issue681-public-execution/v1"
WINDOW_SECONDS = 3
MIN_IDLE_BASIS_POINTS = 9_900
LEDGER_SCHEMA = "alopex.issue681-private-admission/v1"
LEDGER_FIELDS = {"schema", "kind", "rows", "trial", "arm",
                 "allocated_bytes", "idle_window_seconds",
                 "min_idle_basis_points", "status"}


def require(value):
    if not value:
        raise ValueError("PUBLIC_ADMIT")


def cpu_ticks(status, selected):
    """Return total and strict idle ticks for exactly the selected CPUs."""
    found = {}
    for line in status.splitlines():
        fields = line.split()
        if not fields or not re.fullmatch(r"cpu[0-9]+", fields[0]):
            continue
        number = int(fields[0][3:])
        if number not in selected:
            continue
        require(number not in found and len(fields) >= 9 and
                all(value.isdecimal() for value in fields[1:9]))
        counters = [int(value) for value in fields[1:9]]
        found[number] = (sum(counters), counters[3])
    require(set(found) == set(selected))
    return found


def quiet(before, after, selected):
    require(bool(selected) and len(set(selected)) == len(selected))
    first = cpu_ticks(before, selected)
    last = cpu_ticks(after, selected)
    minimum = 10_000
    for number in selected:
        total = last[number][0] - first[number][0]
        idle = last[number][1] - first[number][1]
        require(total > 0 and 0 <= idle <= total and
                idle * 10_000 >= MIN_IDLE_BASIS_POINTS * total)
        minimum = min(minimum, idle * 10_000 // total)
    return minimum


def _unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def verify_ledger(directory, additional_bytes):
    """Check the exact private admission schedule before one pair export."""
    try:
        require(isinstance(directory, Path) and directory.is_absolute() and
                directory.is_dir() and not directory.is_symlink() and
                directory.stat().st_uid == os.geteuid() and
                stat.S_IMODE(directory.stat().st_mode) == 0o700 and
                set(directory.iterdir()) == {directory / "admission.jsonl"} and
                type(additional_bytes) is int and 0 < additional_bytes < case.STOP_BYTES)
        path = directory / "admission.jsonl"
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
                    info.st_nlink == 1 and stat.S_IMODE(info.st_mode) == 0o600 and
                    0 < info.st_size <= 20_000)
            raw = stream.read(20_001)
            require(len(raw) == info.st_size)
        lines = raw.splitlines(keepends=True)
        require(len(lines) == 34 and all(line.endswith(b"\n") for line in lines))
        expected = [("preflight", None, None, None)]
        last_rows = None
        for rows, trial, arm in pair._order():
            if rows != last_rows:
                expected.append(("stage", rows, None, None))
                last_rows = rows
            expected.append(("case", rows, trial, arm))
        for line, (kind, rows, trial, arm) in zip(lines, expected):
            value = json.loads(line, object_pairs_hook=_unique)
            require(type(value) is dict and set(value) == LEDGER_FIELDS and
                    value["schema"] == LEDGER_SCHEMA and value["kind"] == kind and
                    value["rows"] == rows and value["trial"] == trial and
                    value["arm"] == arm and value["status"] == "passed" and
                    value["idle_window_seconds"] == WINDOW_SECONDS and
                    type(value["min_idle_basis_points"]) is int and
                    MIN_IDLE_BASIS_POINTS <= value["min_idle_basis_points"] <= 10_000 and
                    type(value["allocated_bytes"]) is int and
                    0 <= value["allocated_bytes"] and
                    value["allocated_bytes"] + additional_bytes <= case.STOP_BYTES and
                    line == (json.dumps(value, sort_keys=True,
                                        separators=(",", ":")) + "\n").encode())
        return True
    except Exception:
        raise ValueError("PUBLIC_ADMIT") from None


def child_counter_probe():
    """Use a normal child process to test the same VmHWM reset/read path."""
    process = subprocess.Popen(
        [sys.executable, "-B", "-c",
         "import sys,time; print('ready',flush=True); time.sleep(10)"],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, start_new_session=True)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    counter = None
    try:
        require(selector.select(5) and process.stdout.readline() == b"ready\n")
        counter = PhaseRss(process.pid)
        counter.begin("admission")
        observed = counter.finish("admission")
        require(observed["kernel_reported_peak_rss_bytes"] > 0 and
                observed["after"]["high_water_bytes"] > 0)
    finally:
        if counter is not None:
            counter.close()
        selector.close()
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        process.stdout.close()


class Admission:
    def __init__(self, profile, *, roots, log_dir):
        require(type(profile) is dict and set(profile) == {
            "schema", "runner_kind", "cpu_count", "additional_bytes"} and
            profile["schema"] == SCHEMA and
            type(profile["runner_kind"]) is str and
            re.fullmatch(r"[a-z][a-z0-9_-]{2,31}", profile["runner_kind"]) and
            type(profile["cpu_count"]) is int and profile["cpu_count"] == 4 and
            type(profile["additional_bytes"]) is int and
            0 < profile["additional_bytes"] < case.STOP_BYTES and
            type(roots) is tuple and bool(roots) and
            all(isinstance(root, Path) and root.is_absolute() and
                root.exists() and not root.is_symlink() for root in roots) and
            isinstance(log_dir, Path) and log_dir.is_absolute())
        self.profile = profile
        self.roots = roots
        self.log_dir = log_dir
        self.log = None
        self.previous = None
        self.selected = None

    def _capacity(self):
        result = case.storage.inventory(self.roots)
        used = result["allocated_bytes"]
        require(type(used) is int and 0 <= used < case.STOP_BYTES and
                used + self.profile["additional_bytes"] <= case.STOP_BYTES)
        return used

    def _quiet_window(self):
        before = Path("/proc/stat").read_text()
        started = time.monotonic()
        time.sleep(WINDOW_SECONDS)
        require(time.monotonic() - started >= WINDOW_SECONDS)
        after = Path("/proc/stat").read_text()
        return quiet(before, after, self.selected)

    def _record(self, kind, used, idle, *, rows=None, trial=None, arm=None):
        value = {"schema": LEDGER_SCHEMA,
                 "kind": kind, "rows": rows, "trial": trial, "arm": arm,
                 "allocated_bytes": used,
                 "idle_window_seconds": WINDOW_SECONDS,
                 "min_idle_basis_points": idle, "status": "passed"}
        raw = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        require(self.log is not None and os.write(self.log, raw) == len(raw))
        os.fsync(self.log)

    def __enter__(self):
        try:
            require(self.log_dir.is_dir() and not self.log_dir.is_symlink() and
                    self.log_dir.stat().st_uid == os.geteuid() and
                    stat.S_IMODE(self.log_dir.stat().st_mode) == 0o700 and
                    not any(self.log_dir.iterdir()))
            self.log = os.open(self.log_dir / "admission.jsonl",
                               os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                               getattr(os, "O_NOFOLLOW", 0), 0o600)
            os.fchmod(self.log, 0o600)
            require(sys.platform == "linux" and platform.machine() == "x86_64" and
                    os.geteuid() != 0 and
                    os.environ.get("RUNNER_KIND") == self.profile["runner_kind"])
            self.previous = os.sched_getaffinity(0)
            require(len(self.previous) >= 4)
            self.selected = tuple(sorted(self.previous)[:4])
            os.sched_setaffinity(0, self.selected)
            require(os.sched_getaffinity(0) == set(self.selected))
            child_counter_probe()
            used = self._capacity()
            idle = self._quiet_window()
            self._record("preflight", used, idle)
            return self
        except Exception:
            self.__exit__(None, None, None)
            raise ValueError("PUBLIC_ADMIT") from None

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if self.log is not None:
                os.close(self.log)
                self.log = None
        finally:
            if self.previous is not None:
                os.sched_setaffinity(0, self.previous)
                self.previous = None

    def admit_stage(self, rows):
        try:
            require(self.previous is not None and rows in (1_000, 10_000, 100_000))
            used = self._capacity()
            idle = self._quiet_window()
            self._record("stage", used, idle, rows=rows)
            return True
        except Exception:
            raise ValueError("PUBLIC_ADMIT") from None

    def admit_case(self, rows, trial, arm):
        try:
            require(self.previous is not None and rows in (1_000, 10_000, 100_000)
                    and type(trial) is int and 0 <= trial < 5 and
                    arm in ("before", "candidate"))
            used = self._capacity()
            idle = self._quiet_window()
            self._record("case", used, idle, rows=rows, trial=trial, arm=arm)
            return True
        except Exception:
            raise ValueError("PUBLIC_ADMIT") from None
