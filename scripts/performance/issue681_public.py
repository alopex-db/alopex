#!/usr/bin/env python3
"""Collect #681 phase measurements as fixed public records on Linux.

The diagnostic runner is a separate entry point. This runner never prints or
exports child events, stderr, process identity, host inventory or exceptions.
An independently reviewed pair controller must bind the source/binary pins and
compare completed trials before treating a run as release evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import signal
import stat
import subprocess
import sys
import threading
import time

from phase_rss import PhaseRss

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/compatibility"))
import storage

SCHEMA = "alopex.issue681-public-phase/v1"
PHASES = ("seed", "index_build", "cold_transaction_1", "cold_transaction_2",
          "warm", "writer_begin", "first_insert", "second_insert")
ERRORS = frozenset({"PUBLIC_INPUT", "PUBLIC_CAPACITY", "PUBLIC_PROTOCOL",
                    "PUBLIC_COUNTER", "PUBLIC_TIMEOUT", "PUBLIC_CHILD"})
MAX_EVENT_BYTES = 4096
STOP_BYTES = 40_000_000_000


class PublicFailure(Exception):
    def __init__(self, code):
        if code not in ERRORS:
            raise ValueError("PUBLIC_PROTOCOL")
        super().__init__(code)
        self.code = code


def require(value, code="PUBLIC_PROTOCOL"):
    if not value:
        raise PublicFailure(code)


def _uint(value, maximum):
    return type(value) is int and 0 <= value <= maximum


def _canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


def _record(phase, data, counter):
    """Project one checked phase end directly into an allowlisted public row."""
    require(type(data) is dict and data.get("event") == "end")
    elapsed = data.get("elapsed_ns")
    require((elapsed is None and phase == "warm") or
            (phase != "warm" and
             _uint(elapsed, 3_600_000_000_000) and elapsed > 0))
    require(type(counter) is dict and counter.get("phase") == phase and
            _uint(counter.get("kernel_reported_peak_rss_bytes"), 8_000_000_000),
            "PUBLIC_COUNTER")
    stats = data.get("stats")
    if phase in ("cold_transaction_1", "cold_transaction_2", "warm",
                 "first_insert", "second_insert"):
        require(type(stats) is dict)
    if stats is not None:
        require(type(stats) is dict and _uint(stats.get("hits"), 1_000_000_000) and
                _uint(stats.get("misses"), 1_000_000_000))
    graph_bytes = data.get("graph_bytes")
    require((phase == "warm" and _uint(graph_bytes, 8_000_000_000) and
             graph_bytes > 0) or (phase != "warm" and graph_bytes is None))
    return {"phase": phase, "elapsed_ns": elapsed,
            "kernel_peak_rss_bytes": counter["kernel_reported_peak_rss_bytes"],
            "cache_hits": None if stats is None else stats["hits"],
            "cache_misses": None if stats is None else stats["misses"],
            "graph_bytes": graph_bytes}


def _event(line, expected_phase, expected_kind):
    require(type(line) is str and 0 < len(line.encode("utf-8")) <= MAX_EVENT_BYTES and
            line.endswith("\n"))
    try:
        value = json.loads(line)
    except (ValueError, UnicodeError):
        raise PublicFailure("PUBLIC_PROTOCOL") from None
    require(type(value) is dict and set(value) == {"phase", "data"} and
            value["phase"] == expected_phase and type(value["data"]) is dict)
    data = value["data"]
    if expected_phase == "complete":
        stats = data.get("stats")
        require(data.get("result_validated") is True and
                type(stats) is dict and _uint(stats.get("hits"), 1_000_000_000) and
                _uint(stats.get("misses"), 1_000_000_000))
    else:
        require(data.get("event") == expected_kind)
    return data


def _identity(binary, source_commit, binary_sha256):
    require(type(source_commit) is str and re.fullmatch(r"[0-9a-f]{40}", source_commit) and
            type(binary_sha256) is str and re.fullmatch(r"[0-9a-f]{64}", binary_sha256),
            "PUBLIC_INPUT")
    require(isinstance(binary, Path) and binary.is_absolute() and not binary.is_symlink() and
            binary.is_file(), "PUBLIC_INPUT")
    require(hashlib.sha256(binary.read_bytes()).hexdigest() == binary_sha256,
            "PUBLIC_INPUT")


def _directory(directory):
    require(isinstance(directory, Path) and directory.is_absolute() and
            directory.is_dir() and not directory.is_symlink(), "PUBLIC_INPUT")
    info = directory.stat()
    require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700 and
            not any(directory.iterdir()), "PUBLIC_INPUT")


def _write(directory, report):
    body = _canonical(report)
    require(len(body) <= 16_384, "PUBLIC_PROTOCOL")
    path = directory / "public.json"
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        require(path.read_bytes() == body)
        return path
    except Exception:
        path.unlink(missing_ok=True)
        raise PublicFailure("PUBLIC_PROTOCOL") from None


def _inventory(directory, extra=()):
    try:
        require(type(extra) is tuple and
                all(isinstance(root, Path) and root.is_absolute() and
                    root.exists() and not root.is_symlink() for root in extra),
                "PUBLIC_CAPACITY")
        value = storage.inventory([directory, *extra])
        require(type(value) is dict and _uint(value.get("allocated_bytes"), STOP_BYTES - 1)
                and value["allocated_bytes"] + 1_000_000_000 < STOP_BYTES,
                "PUBLIC_CAPACITY")
    except PublicFailure:
        raise
    except Exception:
        raise PublicFailure("PUBLIC_CAPACITY") from None


def run_case(*, binary, directory, private_dir, source_commit, binary_sha256, rows, dimensions,
             trial, finish, timeout, rss_limit, inventory_roots=(),
             counter_factory=PhaseRss):
    """Run one bounded case; make typed rows at each phase end, never from a raw log."""
    _directory(directory)
    _directory(private_dir)
    require(directory != private_dir and directory not in private_dir.parents and
            private_dir not in directory.parents, "PUBLIC_INPUT")
    records = []
    events = queue.Queue(maxsize=32)
    stop_reader = threading.Event()
    process = None
    observer = None
    reader = None
    audit_stream = None
    code = "PUBLIC_PROTOCOL"
    started = time.monotonic()
    last_inventory = started
    try:
        require(sys.platform == "linux" and rows in (1_000, 10_000, 100_000) and
                dimensions == 384 and _uint(trial, 4) and
                finish in ("commit", "rollback") and
                _uint(timeout, 1800) and timeout > 0 and
                _uint(rss_limit, 8_000_000_000) and rss_limit > 0,
                "PUBLIC_INPUT")
        _identity(binary, source_commit, binary_sha256)
        _inventory(directory, inventory_roots)
        fd = os.open(private_dir / "audit.jsonl", os.O_WRONLY | os.O_CREAT |
                     os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
        audit_stream = os.fdopen(fd, "wb")
        phases = (*PHASES, finish)
        expected = tuple((phase, kind) for phase in phases for kind in ("start", "end")) + (
            ("complete", None),)
        process = subprocess.Popen([str(binary), str(rows), str(dimensions), finish],
                                   cwd=ROOT, env=dict(os.environ, ALOPEX_MANUAL_681="1",
                                                      RAYON_NUM_THREADS="2", OMP_NUM_THREADS="2"),
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL, text=True,
                                   start_new_session=True)
        try:
            observer = counter_factory(process.pid)
        except Exception:
            raise PublicFailure("PUBLIC_COUNTER") from None

        def read_child():
            def deliver(value):
                while not stop_reader.is_set():
                    try:
                        events.put(value, timeout=0.1)
                        return
                    except queue.Full:
                        pass
            try:
                for line in iter(lambda: process.stdout.readline(MAX_EVENT_BYTES + 1), ""):
                    if stop_reader.is_set():
                        break
                    deliver(line)
            finally:
                deliver(None)

        reader = threading.Thread(target=read_child, daemon=True)
        reader.start()
        for sequence, (phase, kind) in enumerate(expected):
            while True:
                now = time.monotonic()
                if now - started > timeout:
                    raise PublicFailure("PUBLIC_TIMEOUT")
                if now - last_inventory >= 30:
                    _inventory(directory, inventory_roots)
                    last_inventory = now
                try:
                    line = events.get(timeout=0.1)
                    break
                except queue.Empty:
                    if process.poll() is not None and not reader.is_alive() and events.empty():
                        raise PublicFailure("PUBLIC_CHILD")
            require(line is not None, "PUBLIC_CHILD")
            data = _event(line, phase, kind)
            observed = None
            if phase == "complete":
                require(observer.active is None and data.get("finish") == finish)
            elif kind == "start":
                try:
                    observer.begin(phase)
                except Exception:
                    raise PublicFailure("PUBLIC_COUNTER") from None
            else:
                try:
                    observed = observer.finish(phase)
                except Exception:
                    raise PublicFailure("PUBLIC_COUNTER") from None
                row = _record(phase, data, observed)
                require(row["kernel_peak_rss_bytes"] < rss_limit,
                        "PUBLIC_COUNTER")
                records.append(row)
            audit_stream.write(_canonical({"sequence": sequence,
                                           "event": json.loads(line),
                                           "observation": observed}))
            audit_stream.flush()
            process.stdin.write("continue\n")
            process.stdin.flush()
        require(process.wait(timeout=5) == 0 and events.get(timeout=1) is None,
                "PUBLIC_CHILD")
        reader.join(timeout=2)
        require(len(records) == len(phases) and not reader.is_alive() and
                observer.active is None)
        _inventory(directory, inventory_roots)
        report = {"schema": SCHEMA, "status": "measured",
                  "source_commit": source_commit, "binary_sha256": binary_sha256,
                  "rows": rows, "dimensions": dimensions, "trial": trial,
                  "finish": finish, "phases": records,
                  "result_validated": True,
                  "rss_scope": "Linux VmHWM reset at phase handshakes"}
        audit_stream.write(_canonical({"receipt": {
            "schema": "alopex.issue681-private-audit/v1",
            "source_commit": source_commit, "binary_sha256": binary_sha256,
            "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "rows": rows, "dimensions": dimensions, "trial": trial,
            "finish": finish, "child_exit": 0, "events": len(expected),
            "phases": len(records),
            "public_sha256": hashlib.sha256(_canonical(report)).hexdigest()}}))
        audit_stream.flush()
        os.fsync(audit_stream.fileno())
        _write(directory, report)
        return report
    except PublicFailure as error:
        code = error.code
    except Exception:
        code = "PUBLIC_CHILD"
    finally:
        if audit_stream is not None:
            audit_stream.close()
        if process is not None:
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
            process.stdin.close()
            process.stdout.close()
        if observer is not None:
            observer.close()
        if reader is not None:
            stop_reader.set()
            reader.join(timeout=2)
    failed = {"schema": SCHEMA, "status": "failed", "error_code": code,
              "completed_phases": len(records)}
    _write(directory, failed)
    return failed


def main(argv=None):
    class FixedErrorParser(argparse.ArgumentParser):
        def error(self, message):
            raise PublicFailure("PUBLIC_INPUT")

    parser = FixedErrorParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--private", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--trial", type=int, required=True)
    parser.add_argument("--finish", choices=("commit", "rollback"), required=True)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--rss-limit", type=int, default=8_000_000_000)
    parser.add_argument("--inventory-root", type=Path, action="append", required=True)
    try:
        args = parser.parse_args(argv)
        report = run_case(binary=args.binary, directory=args.output,
                          private_dir=args.private,
                          source_commit=args.source_commit,
                          binary_sha256=args.binary_sha256, rows=args.rows,
                          dimensions=384, trial=args.trial, finish=args.finish,
                          timeout=args.timeout, rss_limit=args.rss_limit,
                          inventory_roots=tuple(args.inventory_root))
        return 0 if report["status"] == "measured" else 1
    except Exception:
        # Setup errors have no admitted public export directory. The workflow
        # log receives one fixed code; never print exception text or arguments.
        print("PUBLIC_INPUT")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
