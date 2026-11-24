"""Public-only final SQL result, assembled after independent private audit.

Use case: a release operator exports the fixed 52-run comparison after the
private audit has authenticated all four preflights and 48 cells. Only typed
numeric operations, fixed coordinates, source hashes and derived comparisons
cross the publication boundary. Any missing or altered row fails closed.
"""
from __future__ import annotations

import json
import hashlib
import math
import statistics

from . import matrix, worker

SCHEMA = "alopex.public-sql-comparison/v1"
ERROR_CODES = frozenset({"PUBLIC_INCOMPLETE", "PUBLIC_MISMATCH", "PRIVATE_AUDIT_FAILED"})


def require(ok, code):
    if not ok:
        raise ValueError(code)


def failed(code, completed_runs):
    """Fixed failure status; no exception text enters a public artifact."""
    require(code in ERROR_CODES and type(completed_runs) is int and
            0 <= completed_runs <= 52, "PUBLIC_STATUS")
    return {"schema": SCHEMA, "status": "failed", "error_code": code,
            "completed_runs": completed_runs, "performance_acceptance": False}


def _same_comparison(public, private):
    if type(public) is dict:
        return type(private) is dict and set(public) == set(private) and all(
            _same_comparison(value, private[key]) for key, value in public.items())
    if type(public) is list:
        return type(private) is list and len(public) == len(private) and all(
            _same_comparison(left, right) for left, right in zip(public, private))
    if type(public) in (int, float):
        return (type(private) in (int, float) and math.isfinite(public) and
                math.isfinite(private) and math.isclose(public, private, rel_tol=1e-12,
                                                       abs_tol=1e-12))
    return type(public) is type(private) and public == private


def completed(operation_result, audited, pins):
    require(type(audited) is dict and audited.get("verified_cells") == 48 and
            audited.get("verified_samples") == 1008 and
            audited.get("performance_acceptance") is False,
            "PRIVATE_AUDIT_FAILED")
    require(type(operation_result) is dict and type(operation_result.get("evidence")) is dict and
            type(pins) is dict and set(pins) == set(matrix.SUBJECTS), "PUBLIC_INCOMPLETE")
    evidence = operation_result["evidence"]
    preflights = evidence.get("preflights")
    require(type(preflights) is list and len(preflights) == 4 and
            [item.get("subject") for item in preflights] == list(matrix.SUBJECTS) and
            all(item.get("result_checksum") == worker.result_digest("select-all")
                for item in preflights), "PUBLIC_INCOMPLETE")
    rows = evidence.get("public_operations")
    operated = evidence.get("cells")
    require(type(rows) is list and len(rows) == 1008 and
            type(operated) is list and len(operated) == 48, "PUBLIC_INCOMPLETE")
    require(audited.get("public_sha256") == hashlib.sha256(json.dumps(
        rows, separators=(",", ":"), allow_nan=False).encode()).hexdigest(),
        "PUBLIC_MISMATCH")
    cells = []
    for offset, planned in enumerate(matrix.plan()):
        old = operated[offset]
        require(type(old) is dict and
                all(old.get(name) == planned[name] for name in
                    ("id", "case", "trial", "subject", "engine")) and
                old.get("status") == "passed" and
                old.get("result_checksum") == worker.result_digest(planned["case"]),
                "PUBLIC_MISMATCH")
        selected = rows[offset * 21:(offset + 1) * 21]
        require(len(selected) == 21, "PUBLIC_INCOMPLETE")
        for sample, row in enumerate(selected):
            require(type(row) is dict and tuple(row) == worker.PUBLIC_FIELDS and
                    all(row.get(name) == planned[name] for name in
                        ("case", "trial", "subject")) and row.get("sample") == sample and
                    row == worker.public_record(planned, sample, row["elapsed_seconds"],
                         {"schema": "alopex.sql-operation-resources/v1", "status": "observed",
                          "coverage": worker.PUBLIC_COVERAGE,
                          "client": {name: row[f"client_{name}"] for name in worker.COUNTERS},
                          "server_processes": [{name: row[f"server_{name}"] for name in worker.COUNTERS}]}),
                    "PUBLIC_MISMATCH")
        median = statistics.median(row["elapsed_seconds"] for row in selected)
        require(type(old.get("trial_median_seconds")) in (int, float) and
                math.isclose(median, old["trial_median_seconds"], rel_tol=1e-12),
                "PUBLIC_MISMATCH")
        cells.append(dict(planned, status="passed",
                          result_checksum=worker.result_digest(planned["case"]),
                          trial_median_seconds=median))
    summary = matrix.summarize(cells)
    require(_same_comparison(summary, operation_result.get("summary")),
            "PUBLIC_MISMATCH")
    identities = []
    for subject in matrix.SUBJECTS:
        pin = pins[subject]
        source = pin.get("source")
        require(type(source) is dict and set(source) == {"commit", "tree", "profile"} and
                matrix._hex(source.get("commit"), 40) and
                matrix._hex(source.get("tree"), 40) and
                source.get("profile") == "release" and
                matrix._hex(pin.get("assets_digest"), 64), "PUBLIC_IDENTITY")
        identities.append({"subject": subject, "source_commit": source["commit"],
                           "source_tree": source["tree"],
                           "assets_digest": pin["assets_digest"]})
    report = {"schema": SCHEMA, "status": "measured", "completed_runs": 52,
              "preflights": [{"subject": item["subject"],
                              "result_checksum": item["result_checksum"]}
                             for item in preflights],
              "subjects": identities, "operations": rows, "summary": summary,
              "verified_samples": 1008, "performance_acceptance": False}
    encoded = json.dumps(report, separators=(",", ":"), allow_nan=False)
    require(len(encoded) < 2_000_000 and all(math.isfinite(row["elapsed_seconds"])
            for row in rows), "PUBLIC_SIZE")
    return report
