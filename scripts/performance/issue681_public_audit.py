"""Independently rejoin private #681 observations to one public case.

Only a fixed verdict leaves this module. Raw events and kernel snapshots stay in
an owner-only directory and are never part of a public upload.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat

import issue681_public as case
import issue681_public_pair as pair

AUDIT_SCHEMA = "alopex.issue681-private-audit/v1"
SNAPSHOT_FIELDS = {"rss_bytes", "high_water_bytes"}
OBSERVATION_FIELDS = {"phase", "before_reset", "after_reset", "after",
                      "kernel_reported_peak_rss_bytes",
                      "kernel_high_water_decreased", "aggregation", "scope",
                      "counter"}
RECEIPT_FIELDS = {"schema", "source_commit", "binary_sha256",
                  "runner_sha256", "rows", "dimensions", "trial", "finish",
                  "child_exit", "events", "phases", "public_sha256"}


def require(value):
    if not value:
        raise ValueError("PRIVATE_AUDIT")


def _unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def _read(directory, name, maximum):
    require(isinstance(directory, Path) and directory.is_absolute() and
            directory.is_dir() and not directory.is_symlink())
    info = directory.stat()
    require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700 and
            set(directory.iterdir()) == {directory / name})
    fd = os.open(directory / name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600 and
                0 < info.st_size <= maximum)
        value = stream.read(maximum + 1)
        require(len(value) == info.st_size)
    return value


def _snapshot(value):
    require(type(value) is dict and set(value) == SNAPSHOT_FIELDS and
            all(case._uint(value[key], 8_000_000_000) for key in SNAPSHOT_FIELDS) and
            value["high_water_bytes"] >= value["rss_bytes"])


def _observation(value, phase):
    require(type(value) is dict and set(value) == OBSERVATION_FIELDS and
            value["phase"] == phase and
            value["aggregation"] == "maximum of reset and end kernel high-water readings" and
            value["scope"] == "child process between phase handshakes, including end-event serialization" and
            value["counter"] == "Linux VmHWM after clear_refs=5; kernel RSS accounting accuracy applies")
    for name in ("before_reset", "after_reset", "after"):
        _snapshot(value[name])
    require(value["kernel_reported_peak_rss_bytes"] == max(
        value["after_reset"]["high_water_bytes"],
        value["after"]["high_water_bytes"]) and
        type(value["kernel_high_water_decreased"]) is bool and
        value["kernel_high_water_decreased"] == (
            value["after"]["high_water_bytes"] <
            value["after_reset"]["high_water_bytes"]))
    return value["kernel_reported_peak_rss_bytes"]


def _project(phase, data, observation):
    require(type(data) is dict and data.get("event") == "end")
    elapsed = data.get("elapsed_ns")
    require((phase == "warm" and elapsed is None) or
            (phase != "warm" and case._uint(elapsed, 3_600_000_000_000) and
             elapsed > 0))
    peak = _observation(observation, phase)
    stats = data.get("stats")
    if phase in ("cold_transaction_1", "cold_transaction_2", "warm",
                 "first_insert", "second_insert"):
        require(type(stats) is dict)
    if stats is not None:
        require(type(stats) is dict and case._uint(stats.get("hits"), 1_000_000_000)
                and case._uint(stats.get("misses"), 1_000_000_000))
    graph_bytes = data.get("graph_bytes")
    require((phase == "warm" and case._uint(graph_bytes, 8_000_000_000) and
             graph_bytes > 0) or (phase != "warm" and graph_bytes is None))
    return {"phase": phase, "elapsed_ns": elapsed,
            "kernel_peak_rss_bytes": peak,
            "cache_hits": None if stats is None else stats["hits"],
            "cache_misses": None if stats is None else stats["misses"],
            "graph_bytes": graph_bytes}


def _audit(public_dir, private_dir, binary, pin, rows, trial, finish):
    pair._pin(pin)
    require(type(rows) is int and rows in pair.SCALES and
            type(trial) is int and trial in range(5) and
            finish in ("commit", "rollback") and
            isinstance(binary, Path) and binary.is_absolute() and
            binary.is_file() and not binary.is_symlink() and
            hashlib.sha256(binary.read_bytes()).hexdigest() == pin["binary_sha256"])
    public_raw = _read(public_dir, "public.json", 16_384)
    audit_raw = _read(private_dir, "audit.jsonl", 200_000)
    report = json.loads(public_raw, object_pairs_hook=_unique)
    pair._case(report, "arm", rows, trial, {"arm": pin}, finish)
    require(public_raw == case._canonical(report))
    lines = audit_raw.splitlines(keepends=True)
    require(len(lines) == 20 and all(line.endswith(b"\n") for line in lines))
    records = [json.loads(line, object_pairs_hook=_unique) for line in lines]
    phases = (*case.PHASES, finish)
    expected = tuple((phase, kind) for phase in phases
                     for kind in ("start", "end")) + (("complete", None),)
    projected = []
    for sequence, (record, (phase, kind)) in enumerate(zip(records[:19], expected)):
        require(type(record) is dict and set(record) == {
            "sequence", "event", "observation"} and
            type(record["sequence"]) is int and record["sequence"] == sequence)
        event = record["event"]
        require(type(event) is dict and set(event) == {"phase", "data"} and
                event["phase"] == phase and type(event["data"]) is dict)
        data = event["data"]
        if phase == "complete":
            require(record["observation"] is None and
                    data.get("finish") == finish and
                    data.get("result_validated") is True)
            stats = data.get("stats")
            require(type(stats) is dict and
                    case._uint(stats.get("hits"), 1_000_000_000) and
                    case._uint(stats.get("misses"), 1_000_000_000))
        elif kind == "start":
            require(data.get("event") == "start" and record["observation"] is None)
        else:
            projected.append(_project(phase, data, record["observation"]))
    require(projected == report["phases"])
    last = records[19]
    require(type(last) is dict and set(last) == {"receipt"})
    receipt = last["receipt"]
    require(type(receipt) is dict and set(receipt) == RECEIPT_FIELDS and
            receipt["schema"] == AUDIT_SCHEMA and
            receipt["source_commit"] == pin["source_commit"] and
            receipt["binary_sha256"] == pin["binary_sha256"] and
            receipt["runner_sha256"] == hashlib.sha256(
                Path(case.__file__).read_bytes()).hexdigest() and
            receipt["rows"] == rows and receipt["dimensions"] == 384 and
            receipt["trial"] == trial and receipt["finish"] == finish and
            receipt["child_exit"] == 0 and receipt["events"] == 19 and
            receipt["phases"] == 9 and
            receipt["public_sha256"] == hashlib.sha256(public_raw).hexdigest())
    return report


def audit_case(public_dir, private_dir, binary, pin, rows, trial, finish):
    """Return an exact public report or a fixed audit failure, never private text."""
    try:
        return _audit(public_dir, private_dir, binary, pin, rows, trial, finish)
    except Exception:
        raise ValueError("PRIVATE_AUDIT") from None
