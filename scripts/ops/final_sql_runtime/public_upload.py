"""Final SQL upload boundary: one canonical public JSON file, no debug material."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import stat
import statistics

from . import audit_bridge, matrix, public_export, worker

MAX_BYTES = 2_000_000
SUCCESS_FIELDS = {"schema", "status", "completed_runs", "preflights", "subjects",
                  "operations", "summary", "verified_samples", "performance_acceptance"}


def require(ok):
    if not ok:
        raise ValueError("PUBLIC_OUTPUT")


def _owned_directory(directory):
    require(isinstance(directory, Path) and directory.is_absolute())
    info = directory.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
            stat.S_IMODE(info.st_mode) == 0o700)


def _destination(directory, *, empty):
    _owned_directory(directory)
    require(set(directory.iterdir()) == (set() if empty else {directory / "public.json"}))


def _canonical(report):
    try:
        body = (json.dumps(report, sort_keys=True, separators=(",", ":"),
                           allow_nan=False) + "\n").encode()
    except (TypeError, ValueError, OverflowError):
        raise ValueError("PUBLIC_OUTPUT") from None
    require(0 < len(body) <= MAX_BYTES)
    return body


def _success(report, pins):
    require(type(report) is dict and set(report) == SUCCESS_FIELDS and
            report["schema"] == public_export.SCHEMA and
            report["status"] == "measured" and report["completed_runs"] == 52 and
            report["verified_samples"] == 1008 and
            report["performance_acceptance"] is False and
            type(pins) is dict and set(pins) == set(matrix.SUBJECTS))
    require(type(report["preflights"]) is list and len(report["preflights"]) == 4)
    for subject, row in zip(matrix.SUBJECTS, report["preflights"]):
        require(type(row) is dict and row == {
            "subject": subject, "result_checksum": worker.result_digest("select-all")})
    require(type(report["subjects"]) is list and len(report["subjects"]) == 4)
    for subject, row in zip(matrix.SUBJECTS, report["subjects"]):
        pin = pins[subject]
        source = pin["source"]
        require(type(row) is dict and row == {
            "subject": subject, "source_commit": source["commit"],
            "source_tree": source["tree"], "assets_digest": pin["assets_digest"]})
    rows = report["operations"]
    require(type(rows) is list and len(rows) == 1008)
    cells = []
    for offset, planned in enumerate(matrix.plan()):
        selected = rows[offset * 21:(offset + 1) * 21]
        require(len(selected) == 21)
        for sample, row in enumerate(selected):
            require(type(row) is dict and set(row) == set(worker.PUBLIC_FIELDS) and
                    all(row.get(field) == planned[field] for field in
                        ("case", "trial", "subject")) and row.get("sample") == sample)
            resource = {"schema": "alopex.sql-operation-resources/v1",
                        "status": "observed", "coverage": worker.PUBLIC_COVERAGE,
                        "client": {name: row[f"client_{name}"] for name in worker.COUNTERS},
                        "server_processes": [
                            {name: row[f"server_{name}"] for name in worker.COUNTERS}]}
            require(row == worker.public_record(planned, sample,
                                                row["elapsed_seconds"], resource))
        cells.append(dict(planned, status="passed",
                          result_checksum=worker.result_digest(planned["case"]),
                          trial_median_seconds=statistics.median(
                              row["elapsed_seconds"] for row in selected)))
    expected_summary = matrix.summarize(cells)
    require(type(report["summary"]) is dict and report["summary"] == expected_summary)
    require(all(type(row["elapsed_seconds"]) in (int, float) and
                math.isfinite(row["elapsed_seconds"]) for row in rows))


def _fresh_report(private_result, pins, independent_evidence):
    require(type(private_result) is dict and
            set(private_result) == {"qualification", "operation", "audit",
                                    "public", "performance_acceptance"} and
            private_result["performance_acceptance"] is False and
            callable(independent_evidence))
    operation = private_result["operation"]
    evidence = independent_evidence(operation)
    require(type(evidence) is dict and set(evidence) == {
        "collections", "receipts", "preflight_collections", "preflight_receipts",
        "authenticate_receipt"} and callable(evidence["authenticate_receipt"]))
    audited = audit_bridge.audit_completed(
        operation, evidence["collections"], evidence["receipts"], pins,
        evidence["authenticate_receipt"],
        preflight_collections=evidence["preflight_collections"],
        preflight_receipts=evidence["preflight_receipts"])
    require(audited == private_result["audit"])
    report = public_export.completed(operation, audited, pins)
    require(report == private_result["public"])
    return report


def _write(report, directory, pins, private_result, independent_evidence):
    _destination(directory, empty=True)
    path = directory / "public.json"
    created = False
    try:
        require(type(report) is dict)
        _success(report, pins)
        body = _canonical(report)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        created = True
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        require(verify(directory, pins=pins, private_result=private_result,
                       independent_evidence=independent_evidence) == report)
    except Exception:
        if created:
            try:
                path.unlink()
            except OSError:
                pass
        raise ValueError("PUBLIC_OUTPUT") from None


def prepare_success(controller_result, pins, directory, independent_evidence):
    """Rebuild from audited private data in the same process before writing."""
    try:
        report = _fresh_report(controller_result, pins, independent_evidence)
        _write(report, directory, pins, controller_result, independent_evidence)
        return report
    except Exception:
        raise ValueError("PUBLIC_OUTPUT") from None


def failure_log(failure):
    """Fixed failed-job log line, with no upload artifact or claimed run count."""
    from .controller import ControllerFailure
    require(isinstance(failure, ControllerFailure))
    status = failure.public_status
    require(type(status) is dict and status.get("status") == "failed" and
            status.get("error_code") in public_export.ERROR_CODES)
    return _canonical({"schema": public_export.SCHEMA, "status": "failed",
                       "error_code": status["error_code"],
                       "completed_runs": "unknown",
                       "performance_acceptance": False})


def verify(directory, *, pins, private_result, independent_evidence):
    """Rejoin audited private evidence and recheck the exact public file."""
    owned = False
    try:
        _owned_directory(directory)
        owned = True
        _destination(directory, empty=False)
        fd = os.open(directory / "public.json", os.O_RDONLY |
                     getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                    info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600 and
                    0 < info.st_size <= MAX_BYTES)
            body = stream.read(MAX_BYTES + 1)
            require(len(body) == info.st_size)
        def unique(pairs):
            result = {}
            for key, value in pairs:
                require(key not in result)
                result[key] = value
            return result
        report = json.loads(body, object_pairs_hook=unique)
        require(type(report) is dict and body == _canonical(report))
        _success(report, pins)
        require(report == _fresh_report(private_result, pins, independent_evidence))
        return report
    except Exception:
        if owned:
            try:
                (directory / "public.json").unlink(missing_ok=True)
            except OSError:
                pass
        raise ValueError("PUBLIC_OUTPUT") from None
