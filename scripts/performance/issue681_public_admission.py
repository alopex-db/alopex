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


def eligible_cpus(before, after, allowed):
    """Select only CPUs with a valid strict-idle three-second sample."""
    require(bool(allowed) and len(set(allowed)) == len(allowed))
    first = cpu_ticks(before, allowed)
    last = cpu_ticks(after, allowed)
    eligible = []
    for number in sorted(allowed):
        total = last[number][0] - first[number][0]
        idle = last[number][1] - first[number][1]
        require(total > 0 and 0 <= idle <= total)
        if idle * 10_000 >= MIN_IDLE_BASIS_POINTS * total:
            eligible.append(number)
    return tuple(eligible)


def parse_cpuset(raw):
    value = raw.strip()
    require(bool(value) and re.fullmatch(r"[0-9,-]+", value))
    result = set()
    for item in value.split(","):
        limits = item.split("-")
        require(len(limits) in (1, 2) and all(part.isdecimal() for part in limits))
        start = int(limits[0])
        end = int(limits[-1])
        require(0 <= start <= end < 4096 and end - start < 4096)
        selected = set(range(start, end + 1))
        require(not result.intersection(selected))
        result.update(selected)
    return result


def resolve_cgroup(root, relative, pid):
    """Bind a visible cgroup directory to this process, even under a remount."""
    parts = [] if relative == "/" else relative.split("/")[1:]
    require(isinstance(root, Path) and root.is_absolute() and root.is_dir() and
            not root.is_symlink() and (root / "cgroup.controllers").is_file() and
            type(relative) is str and relative.startswith("/") and
            all(part not in (".", "..", "") for part in parts) and
            type(pid) is int and pid > 0)
    members = []
    for index in range(len(parts) + 1):
        candidate = root.joinpath(*parts[index:])
        if not candidate.is_dir() or candidate.is_symlink():
            continue
        entry = candidate / "cgroup.procs"
        if not entry.is_file() or entry.is_symlink():
            continue
        with entry.open("rb") as stream:
            raw = stream.read(1_000_001)
        require(len(raw) <= 1_000_000)
        values = raw.splitlines()
        require(all(value.isdigit() for value in values))
        if str(pid).encode() in values:
            members.append(candidate)
    require(len(members) == 1)
    return members[0]


def cgroup_allocation(root, relative, selected, *, pid=None):
    """Check inherited cpuset, visible quota ancestors and current throttle."""
    require(bool(selected))
    current = resolve_cgroup(root, relative, os.getpid() if pid is None else pid)
    directory = current
    while not (directory / "cpuset.cpus.effective").is_file():
        require(directory != root)
        directory = directory.parent
        require(directory == root or root in directory.parents)
    effective = parse_cpuset((directory / "cpuset.cpus.effective").read_text())
    require(set(selected).issubset(effective))
    directory = current
    while True:
        fields = (directory / "cpu.max").read_text().split()
        require(len(fields) == 2 and fields[1].isdecimal() and int(fields[1]) > 0)
        if fields[0] != "max":
            require(fields[0].isdecimal() and
                    int(fields[0]) >= len(selected) * int(fields[1]))
        if directory == root:
            break
        directory = directory.parent
        require(directory == root or root in directory.parents)
    counters = {}
    for line in (current / "cpu.stat").read_text().splitlines():
        parts = line.split()
        require(len(parts) == 2 and parts[0] not in counters and
                parts[1].isdecimal())
        counters[parts[0]] = int(parts[1])
    require("nr_throttled" in counters)
    mapped = current.relative_to(root)
    return "/" + ("/".join(mapped.parts) if mapped.parts else ""), counters["nr_throttled"]


def current_allocation(selected):
    lines = Path("/proc/self/cgroup").read_text().splitlines()
    require(len(lines) == 1 and lines[0].startswith("0::"))
    relative = lines[0][3:]
    mounts = [line for line in Path("/proc/self/mountinfo").read_text().splitlines()
              if " - cgroup2 " in line and
              len(line.split()) > 5 and line.split()[4] == "/sys/fs/cgroup"]
    require(len(mounts) == 1)
    return cgroup_allocation(Path("/sys/fs/cgroup"), relative, selected)


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
        require(len(lines) == 64 and all(line.endswith(b"\n") for line in lines))
        expected = [("preflight", None, None, None)]
        last_rows = None
        for rows, trial, arm in pair._order():
            if rows != last_rows:
                expected.append(("stage", rows, None, None))
                last_rows = rows
            expected.append(("case", rows, trial, arm))
            expected.append(("case_complete", rows, trial, arm))
        for line, (kind, rows, trial, arm) in zip(lines, expected):
            value = json.loads(line, object_pairs_hook=_unique)
            require(type(value) is dict and set(value) == LEDGER_FIELDS and
                    value["schema"] == LEDGER_SCHEMA and value["kind"] == kind and
                    value["rows"] == rows and value["trial"] == trial and
                    value["arm"] == arm and value["status"] == "passed" and
                    ((kind == "case_complete" and
                      value["idle_window_seconds"] == 0 and
                      value["min_idle_basis_points"] is None) or
                     (kind != "case_complete" and
                      value["idle_window_seconds"] == WINDOW_SECONDS and
                      type(value["min_idle_basis_points"]) is int and
                      MIN_IDLE_BASIS_POINTS <= value["min_idle_basis_points"] <= 10_000)) and
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
        self.cgroup = None
        self.pending = None

    def _allocation(self):
        require(self.selected is not None and
                os.sched_getaffinity(0) == set(self.selected))
        path, throttled = current_allocation(self.selected)
        require(self.cgroup is None or path == self.cgroup)
        self.cgroup = path
        return throttled

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

    def _select_cpus(self):
        before = Path("/proc/stat").read_text()
        started = time.monotonic()
        time.sleep(WINDOW_SECONDS)
        require(time.monotonic() - started >= WINDOW_SECONDS)
        after = Path("/proc/stat").read_text()
        eligible = eligible_cpus(before, after, tuple(self.previous))
        require(len(eligible) >= 4)
        return eligible[:4]

    def _record(self, kind, used, idle, *, rows=None, trial=None, arm=None):
        value = {"schema": LEDGER_SCHEMA,
                 "kind": kind, "rows": rows, "trial": trial, "arm": arm,
                 "allocated_bytes": used,
                 "idle_window_seconds": 0 if kind == "case_complete" else WINDOW_SECONDS,
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
            self.selected = self._select_cpus()
            os.sched_setaffinity(0, self.selected)
            require(os.sched_getaffinity(0) == set(self.selected))
            self._allocation()
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
            require(self.pending is None)
            self._allocation()
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
                    arm in ("before", "candidate") and self.pending is None)
            throttled = self._allocation()
            used = self._capacity()
            idle = self._quiet_window()
            self._record("case", used, idle, rows=rows, trial=trial, arm=arm)
            self.pending = (rows, trial, arm, throttled)
            return True
        except Exception:
            raise ValueError("PUBLIC_ADMIT") from None

    def complete_case(self, rows, trial, arm):
        try:
            require(self.pending is not None and
                    self.pending[:3] == (rows, trial, arm) and
                    self._allocation() == self.pending[3])
            used = self._capacity()
            self._record("case_complete", used, None,
                         rows=rows, trial=trial, arm=arm)
            self.pending = None
            return True
        except Exception:
            raise ValueError("PUBLIC_ADMIT") from None
