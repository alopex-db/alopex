"""Reduce one bounded private pair log to source-bound SQL stage records.

The caller supplies the complete on/off client inventories from the same pair.
This module has no server or container side effects; the independent auditor
must repeat the join when it reads the sealed evidence.
"""
from __future__ import annotations

import re
import os
import stat
import uuid
from pathlib import Path

from .adapter import CASES, POSTCHECK, SETUP

TARGET = "alopex_sql_private_timing"
READ_STAGES = frozenset(("transaction_setup", "route_plan", "execute_multi",
                         "completion_and_commit", "result_map", "execute_and_map",
                         "response_build"))
SESSION_STAGES = (READ_STAGES - {"transaction_setup", "completion_and_commit"}) | {
    "session_setup", "session_completion"}
MAX_LOG_BYTES = 8_000_000
MAX_DURATION_NS = 300_000_000_000
TIMING = re.compile(
    r"\bINFO (?:http_request\{[^{}\r\n]*\}: )?alopex_sql_private_timing: sql diagnostic stage "
    r"trace_id=([^\s]+) stage=\"?([^\s\"]+)\"? duration_ns=([^\s]+)\s*$"
)


def _canonical_uuid(value):
    if type(value) is not str:
        raise ValueError("TRACE_JOIN")
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError("TRACE_JOIN") from error
    if str(parsed) != value or parsed.version != 4:
        raise ValueError("TRACE_JOIN")
    return value


def _inventory(case, mode, trial, clients):
    if type(clients) is not list:
        raise ValueError("TRACE_JOIN")
    counts = {"setup": SETUP[case] if mode == "on" else 0,
              "precheck": 1, "warmup": 3, "measured": 21,
              "postcheck": POSTCHECK[case]}
    if len(clients) != sum(counts.values()):
        raise ValueError("TRACE_JOIN")
    seen = set()
    traces = {}
    for row in clients:
        if type(row) is not dict or row.get("case") != case or row.get("mode") != mode or \
                row.get("trial") != trial or row.get("cell_id") != f"{case}-{trial}-{mode}":
            raise ValueError("TRACE_JOIN")
        phase, sample = row.get("phase"), row.get("sample")
        if phase not in counts or type(sample) is not int or not 0 <= sample < counts[phase] or \
                (phase, sample) in seen:
            raise ValueError("TRACE_JOIN")
        seen.add((phase, sample))
        if mode == "off":
            if "trace_id" in row:
                raise ValueError("TRACE_OFF")
        else:
            trace_id = _canonical_uuid(row.get("trace_id"))
            if trace_id in traces:
                raise ValueError("TRACE_JOIN")
            traces[trace_id] = (SESSION_STAGES if case == "insert-rollback" and
                                phase in ("precheck", "warmup", "measured") else READ_STAGES)
    return traces


def parse_server_timing(path, case, trial, on_clients, off_clients):
    """Return only trace_id, stage and duration_ns for one complete on/off pair.

    Rejects malformed target lines, unknown or duplicate UUID/stage pairs,
    route-inappropriate or incomplete stage sets, and an oversized log.
    """
    if case not in CASES or type(trial) is not int or trial not in range(3):
        raise ValueError("TRACE_JOIN")
    path = Path(path)
    required = _inventory(case, "on", trial, on_clients)
    _inventory(case, "off", trial, off_clients)
    if len(required) != len(on_clients):
        raise ValueError("TRACE_JOIN")
    max_events = len(required) * 7
    seen = {trace_id: set() for trace_id in required}
    events = []
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        raise ValueError("TRACE_BOUND") from error
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid() or \
                stat.S_IMODE(before.st_mode) != 0o600 or before.st_size > MAX_LOG_BYTES:
            raise ValueError("TRACE_BOUND")
        raw = stream.read(MAX_LOG_BYTES + 1)
        after = os.fstat(stream.fileno())
        if len(raw) > MAX_LOG_BYTES or len(raw) != before.st_size or \
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != \
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
            raise ValueError("TRACE_BOUND")
    for line in raw.decode("utf-8", errors="replace").splitlines():
        if TARGET not in line:
            continue
        match = TIMING.search(line)
        if match is None:
            raise ValueError("TRACE_FORMAT")
        trace_id, stage, duration = match.groups()
        try:
            trace_id = _canonical_uuid(trace_id)
        except ValueError as error:
            raise ValueError("TRACE_FORMAT") from error
        if trace_id not in required:
            raise ValueError("TRACE_JOIN")
        if stage not in required[trace_id] or stage in seen[trace_id] or \
                len(events) >= max_events or not duration.isascii() or not duration.isdecimal():
            raise ValueError("TRACE_FORMAT")
        elapsed = int(duration)
        if elapsed > MAX_DURATION_NS:
            raise ValueError("TRACE_FORMAT")
        seen[trace_id].add(stage)
        events.append({"trace_id": trace_id, "stage": stage,
                       "duration_ns": elapsed})
    if any(seen[trace_id] != stages for trace_id, stages in required.items()):
        raise ValueError("TRACE_INCOMPLETE")
    return events
