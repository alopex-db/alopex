"""Validate and compare typed #681 public phase records, without debug input.

An independent same-job receipt/identity audit is required by ``write``. The
caller must collect all staged runs in their prescribed order and supply that
audit; this module alone does not authorize a release performance verdict.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import statistics

import issue681_public as case

SCHEMA = "alopex.issue681-public-pair/v1"
SCALES = (1_000, 10_000, 100_000)
ARMS = ("before", "candidate")
CASE_FIELDS = {"schema", "status", "source_commit", "binary_sha256", "rows",
               "dimensions", "trial", "finish", "phases", "rss_scope",
               "result_validated"}
PHASE_FIELDS = {"phase", "elapsed_ns", "kernel_peak_rss_bytes", "cache_hits",
                "cache_misses", "graph_bytes"}
PAIR_FIELDS = {"schema", "status", "pins", "finish", "cases", "summary",
               "performance_acceptance"}
RSS_SCOPE = "Linux VmHWM reset at phase handshakes"


def require(value):
    if not value:
        raise ValueError("PUBLIC_PAIR")


def _pin(pin):
    require(type(pin) is dict and set(pin) == {
        "source_commit", "source_tree", "binary_sha256", "example_sha256",
        "parser_sha256", "assets_digest", "profile", "platform",
        "parser_contract", "parser_record_sha256", "cargo_lock_sha256",
        "rust_version", "version"})
    require(all(type(pin[name]) is str and re.fullmatch(r"[0-9a-f]{40}", pin[name])
                for name in ("source_commit", "source_tree")) and
            all(type(pin[name]) is str and re.fullmatch(r"[0-9a-f]{64}", pin[name])
                for name in ("binary_sha256", "example_sha256", "parser_sha256",
                             "assets_digest", "parser_record_sha256",
                             "cargo_lock_sha256")) and
            pin["profile"] == "release" and pin["platform"] == "linux/amd64" and
            pin["parser_contract"] == "0.28.0" and
            pin["version"] == "0.8.17" and
            type(pin["rust_version"]) is str and
            re.fullmatch(r"1\.[0-9]{1,3}\.[0-9]{1,3}", pin["rust_version"]))


def _pins(pins):
    require(type(pins) is dict and set(pins) == set(ARMS))
    for arm in ARMS:
        _pin(pins[arm])
    require(pins["before"] != pins["candidate"] and
            pins["before"]["source_commit"] != pins["candidate"]["source_commit"])
    for name in ("example_sha256", "parser_sha256", "profile", "platform",
                 "parser_contract", "parser_record_sha256", "rust_version",
                 "version"):
        require(pins["before"][name] == pins["candidate"][name])


def _phase(row, expected):
    require(type(row) is dict and set(row) == PHASE_FIELDS and
            row["phase"] == expected and
            case._uint(row["kernel_peak_rss_bytes"], 8_000_000_000))
    elapsed = row["elapsed_ns"]
    require((expected == "warm" and elapsed is None) or
            (expected != "warm" and
             case._uint(elapsed, 3_600_000_000_000) and elapsed > 0))
    has_stats = expected in ("cold_transaction_1", "cold_transaction_2", "warm",
                             "first_insert", "second_insert")
    require((has_stats and case._uint(row["cache_hits"], 1_000_000_000) and
             case._uint(row["cache_misses"], 1_000_000_000)) or
            (not has_stats and row["cache_hits"] is None and
             row["cache_misses"] is None))
    require((expected == "warm" and case._uint(row["graph_bytes"], 8_000_000_000)
             and row["graph_bytes"] > 0) or
            (expected != "warm" and row["graph_bytes"] is None))


def _case(report, arm, rows, trial, pins, finish):
    require(type(report) is dict and set(report) == CASE_FIELDS and
            report["schema"] == case.SCHEMA and report["status"] == "measured" and
            report["source_commit"] == pins[arm]["source_commit"] and
            report["binary_sha256"] == pins[arm]["binary_sha256"] and
            type(report["rows"]) is int and report["rows"] == rows and
            type(report["dimensions"]) is int and report["dimensions"] == 384 and
            type(report["trial"]) is int and report["trial"] == trial and
            report["finish"] == finish and report["result_validated"] is True and
            report["rss_scope"] == RSS_SCOPE and
            type(report["phases"]) is list and len(report["phases"]) == 9)
    for value, phase in zip(report["phases"], (*case.PHASES, finish)):
        _phase(value, phase)


def _order():
    for rows in SCALES:
        for trial in range(5):
            for arm in (ARMS if trial % 2 == 0 else reversed(ARMS)):
                yield rows, trial, arm


def _summary(reports, finish):
    summaries = []
    by_key = {(row["report"]["rows"], row["report"]["trial"], row["arm"]):
              row["report"]
              for row in reports}
    for rows in SCALES:
        for phase_index, phase in enumerate((*case.PHASES, finish)):
            for metric in ("elapsed_ns", "kernel_peak_rss_bytes"):
                if metric == "elapsed_ns" and phase == "warm":
                    continue
                distributions = {}
                medians = {}
                mads = {}
                for arm in ARMS:
                    values = [by_key[(rows, trial, arm)]["phases"][phase_index][metric]
                              for trial in range(5)]
                    median = statistics.median(values)
                    distributions[arm] = values
                    medians[arm] = median
                    mads[arm] = statistics.median(abs(value - median) for value in values)
                difference = medians["candidate"] - medians["before"]
                bound = 3 * max(mads.values())
                summaries.append({"rows": rows, "phase": phase, "metric": metric,
                                  "before": distributions["before"],
                                  "candidate": distributions["candidate"],
                                  "before_median": medians["before"],
                                  "candidate_median": medians["candidate"],
                                  "before_mad": mads["before"],
                                  "candidate_mad": mads["candidate"],
                                  "candidate_minus_before": difference,
                                  "three_mad_bound": bound,
                                  "increase_beyond_bound": difference > bound})
    return summaries


def assemble(reports, pins, finish):
    """Require all 30 ordered trial records and derive every published number."""
    _pins(pins)
    require(finish in ("commit", "rollback"))
    require(type(reports) is list and len(reports) == 30)
    cases = []
    for report, (rows, trial, arm) in zip(reports, _order()):
        _case(report, arm, rows, trial, pins, finish)
        cases.append({"arm": arm, "report": report})
    result = {"schema": SCHEMA, "status": "measured", "pins": pins,
              "finish": finish, "cases": cases, "summary": _summary(cases, finish),
              "performance_acceptance": False}
    require(len(case._canonical(result)) <= 500_000)
    return result


def _unique(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value)
        value[key] = item
    return value


def verify(directory, *, pins, independent_audit):
    """Recompute one canonical public export after a separate receipt audit."""
    require(isinstance(directory, Path) and directory.is_absolute() and
            directory.is_dir() and not directory.is_symlink())
    info = directory.stat()
    require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700 and
            set(directory.iterdir()) == {directory / "public.json"})
    path = directory / "public.json"
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                    info.st_uid == os.geteuid() and
                    stat.S_IMODE(info.st_mode) == 0o600 and
                    0 < info.st_size <= 500_000)
            raw = stream.read(500_001)
            require(len(raw) == info.st_size)
    except OSError:
        raise ValueError("PUBLIC_PAIR") from None
    try:
        report = json.loads(raw, object_pairs_hook=_unique)
    except (ValueError, UnicodeError):
        raise ValueError("PUBLIC_PAIR") from None
    require(type(report) is dict and set(report) == PAIR_FIELDS and
            report["schema"] == SCHEMA and report["status"] == "measured" and
            report["performance_acceptance"] is False and
            report["pins"] == pins and type(report["cases"]) is list and
            all(type(row) is dict and set(row) == {"arm", "report"}
                for row in report["cases"]))
    expected = assemble([row["report"] for row in report["cases"]], pins,
                        report["finish"])
    require(report == expected and raw == case._canonical(expected) and
            callable(independent_audit))
    try:
        audited = independent_audit(expected) is True
    except Exception:
        audited = False
    require(audited)
    return report


def write(reports, pins, directory, *, finish, independent_audit):
    """Write only after exact public recomputation and private receipt review."""
    require(callable(independent_audit) and isinstance(directory, Path) and
            directory.is_absolute() and directory.is_dir() and
            not directory.is_symlink() and not any(directory.iterdir()))
    info = directory.stat()
    require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700)
    result = assemble(reports, pins, finish)
    try:
        audited = independent_audit(result) is True
    except Exception:
        audited = False
    require(audited)
    raw = case._canonical(result)
    require(len(raw) <= 500_000)
    path = directory / "public.json"
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        return verify(directory, pins=pins, independent_audit=independent_audit)
    except Exception:
        (directory / "public.json").unlink(missing_ok=True)
        raise ValueError("PUBLIC_PAIR") from None
