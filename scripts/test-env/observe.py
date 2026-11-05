"""Conservative procfs observations of a stable server tree.

Any PID allocation during the interval invalidates the sample, including a
short-lived child absent from both endpoint snapshots. This is qualification
of tiny SQL cases, not a general-purpose profiler of changing process trees.
"""
from __future__ import annotations

import os
from pathlib import Path

from common import digest, require


def process(pid, proc=Path("/proc")):
    root = proc / str(pid)

    def stat():
        fields = (root / "stat").read_text().rsplit(")", 1)[1].split()
        require(fields[0] != "Z", "MEASUREMENT")
        return {"start_ticks": int(fields[19]), "user_ticks": int(fields[11]),
                "system_ticks": int(fields[12]), "reaped_user_ticks": int(fields[13]),
                "reaped_system_ticks": int(fields[14])}

    first = stat()
    status = dict(line.split(":", 1) for line in (root / "status").read_text().splitlines())
    io = dict(line.split(":", 1) for line in (root / "io").read_text().splitlines())
    children, tids = set(), set()
    for task in (root / "task").iterdir():
        tids.add(int(task.name))
        children.update(map(int, (task / "children").read_text().split()))
    require(status["VmRSS"].split()[1] == "kB", "MEASUREMENT")
    identity = digest(root / "exe")
    last = stat()
    require(first["start_ticks"] == last["start_ticks"], "MEASUREMENT")
    return dict(last, pid=pid, exe_sha256=identity, threads=sorted(tids), children=sorted(children),
                rss_bytes=int(status["VmRSS"].split()[0]) * 1024,
                **{k: int(io[k]) for k in ("read_bytes", "write_bytes", "cancelled_write_bytes")})


def tree(pid, maximum, proc=Path("/proc")):
    todo, rows = [pid], {}
    while todo:
        child = todo.pop()
        if child in rows:
            continue
        require(len(rows) < maximum, "MEASUREMENT")
        rows[child] = process(child, proc)
        todo.extend(rows[child]["children"])
    return rows


def delta(before, after, ticks):
    require(before.keys() == after.keys(), "MEASUREMENT")
    result = []
    for pid, first in before.items():
        last = after[pid]
        for key in ("start_ticks", "exe_sha256", "threads", "children",
                    "reaped_user_ticks", "reaped_system_ticks"):
            require(first[key] == last[key], "MEASUREMENT")
        row = {"pid": pid, "start_ticks": first["start_ticks"], "exe_sha256": first["exe_sha256"],
               "rss_before_bytes": first["rss_bytes"], "rss_after_bytes": last["rss_bytes"]}
        for key in ("user_ticks", "system_ticks", "read_bytes", "write_bytes", "cancelled_write_bytes"):
            difference = last[key] - first[key]
            require(difference >= 0, "MEASUREMENT")
            row[key] = difference
        row["cpu_seconds"] = (row["user_ticks"] + row["system_ticks"]) / ticks
        result.append(row)
    return result


class Observation:
    def __init__(self, server_pid, maximum, required_backend=None, harness=None):
        self.server_pid, self.maximum, self.backend = server_pid, maximum, required_backend
        self.harness = harness

    def snapshot(self):
        allocation = Path("/proc/sys/kernel/ns_last_pid").read_text().strip()
        server = tree(self.server_pid, self.maximum)
        client = {os.getpid(): process(os.getpid())}
        require(self.backend is None or self.backend in server, "MEASUREMENT")
        require(all(pid not in server for pid in client), "MEASUREMENT")
        require(allocation == Path("/proc/sys/kernel/ns_last_pid").read_text().strip(), "MEASUREMENT")
        return {"allocation": allocation, "server": server, "client": client}

    def begin(self):
        self.before = self.snapshot()
        if self.harness is not None:
            self.harness_before = self.harness.begin()

    def end(self):
        qualified = self.harness.end(self.harness_before) if self.harness is not None else None
        after = self.snapshot()
        require(self.before["allocation"] == after["allocation"], "MEASUREMENT")
        ticks = os.sysconf("SC_CLK_TCK")
        server = delta(self.before["server"], after["server"], ticks)
        client = delta(self.before["client"], after["client"], ticks)
        if qualified is not None:
            return {"qualified_harness": qualified, "server": server, "client": client,
                    "coverage": "stable_pid_namespace_allocation",
                    "server_identities": [{"pid": row["pid"], "start_ticks": row["start_ticks"],
                                           "exe_sha256": row["exe_sha256"]} for row in server]}
        return {"server": server, "client": client, "scope": "stable_process_tree_boundary_observations"}
