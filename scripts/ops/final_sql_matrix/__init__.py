"""Private, offline contract for the fixed #712 SQL matrix.

Callers supply authenticated subject pins and observations. Nothing here starts
servers, acquires assets, or qualifies a live measurement environment.
"""
from __future__ import annotations

import hashlib
import json
import math
import statistics

SUBJECTS = ("published", "before", "candidate", "postgresql")
CASES = ("aggregate", "ordered", "select-all", "insert-rollback")
TRIALS = range(3)
WARMUPS = 3
SAMPLES = 21
COUNTERS = ("user_cpu_seconds", "system_cpu_seconds", "rss_before_bytes",
            "rss_after_bytes", "read_bytes", "write_bytes", "cancelled_write_bytes")
SQL = {
    "aggregate": "SELECT COUNT(*) AS count FROM bench WHERE enabled = true",
    "ordered": "SELECT id, COALESCE(value, 0) AS value FROM bench WHERE id >= 0 ORDER BY id LIMIT 16",
    "select-all": "SELECT id, value FROM bench WHERE enabled = true",
    "insert-rollback": "INSERT INTO perf_items VALUES (3, 'returning') RETURNING id",
}
ROLLBACK_SQL = "SELECT id, value FROM perf_items ORDER BY id"


def require(ok, code):
    if not ok:
        raise ValueError(code)


def digest(value):
    # Match the pinned v0816 worker's json.dumps(expected, sort_keys=True).
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def expected(case):
    require(case in CASES, "CASE")
    if case == "aggregate":
        return [{"count": 17_125}]
    if case == "ordered":
        return [{"id": i, "value": float(i)} for i in range(16)]
    if case == "select-all":
        return [{"id": i, "value": float(i)} for i in range(0, 34_250, 2)]
    return [{"id": 3}]


def plan():
    return [dict(case=case, trial=trial, subject=subject,
                 engine="postgresql" if subject == "postgresql" else "alopex-server")
            for case in CASES for trial in TRIALS
            for subject in (SUBJECTS if trial % 2 == 0 else SUBJECTS[::-1])]


def _positive(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def _hex(value):
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _resource(observation, resource_digest):
    require(type(observation) is dict and observation.get("schema") == "alopex.sql-operation-resources/v1"
            and observation.get("status") == "observed"
            and observation.get("coverage") == "stable_pid_namespace_allocation"
            and observation.get("manifest_sha256") == resource_digest, "RESOURCE_COVERAGE")
    client, servers = observation.get("client"), observation.get("server_processes")
    require(type(client) is dict and type(servers) is list and 1 <= len(servers) <= 64
            and all(type(p) is dict for p in servers), "RESOURCE_SCOPE")
    identities = []
    for process in (client, *servers):
        require(type(process.get("pid")) is int and process["pid"] > 0
                and type(process.get("start_ticks")) is int and process["start_ticks"] > 0
                and all(type(process.get(key)) in (int, float) and
                        math.isfinite(process[key]) and process[key] >= 0 for key in COUNTERS),
                "RESOURCE_FIELDS")
        identities.append((process["pid"], process["start_ticks"]))
    require(len(set(identities)) == len(identities) and
            len({pid for pid, _ in identities}) == len(identities), "RESOURCE_DUPLICATE")


def audit_cell(cell, pin):
    """Audit one offline record against an independently authenticated pin.

    Raw rows have timing and resource observations, not SQL result rows. Exact
    value/order and rollback checks depend on the independently pinned worker
    and harness whose bytes the evidence carries. This does not authenticate
    execution or prove those in-process checks ran; the runtime attestor must.
    """
    require(type(cell) is dict and type(pin) is dict, "CELL")
    case, subject, trial = cell.get("case"), cell.get("subject"), cell.get("trial")
    require(case in CASES and subject in SUBJECTS and type(trial) is int and trial in TRIALS, "COORDINATE")
    require(cell.get("engine") == ("postgresql" if subject == "postgresql" else "alopex-server")
            and cell.get("status") == "passed" and cell.get("cleanup") == "deleted", "CELL_STATUS")
    require(all(_hex(pin.get(key)) for key in ("assets_digest", "resource_digest",
                                              "worker_sha256", "harness_sha256"))
            and type(pin.get("source")) is dict and pin["source"], "PIN")
    require(type(cell.get("worker_bytes")) is bytes and type(cell.get("harness_bytes")) is bytes
            and hashlib.sha256(cell["worker_bytes"]).hexdigest() == pin["worker_sha256"]
            and hashlib.sha256(cell["harness_bytes"]).hexdigest() == pin["harness_sha256"],
            "VERIFIER_IDENTITY")
    require(cell.get("source") == pin["source"] and cell.get("assets_digest") == pin["assets_digest"], "IDENTITY")
    result = cell.get("measurement")
    require(type(result) is dict and result.get("case") == case and result.get("trial") == trial
            and result.get("engine") == cell["engine"] and result.get("server_client") is True
            and type(result.get("subject_identity")) is dict
            and result["subject_identity"].get("source") == pin["source"], "MEASUREMENT_IDENTITY")
    require(result.get("sql") == SQL[case] and type(result.get("warmups")) is int
            and result["warmups"] == WARMUPS and type(result.get("samples")) is int
            and result["samples"] == SAMPLES and type(result.get("fixture_rows")) is int
            and result["fixture_rows"] ==
            (1 if case == "insert-rollback" else 34_250), "SQL_BOUNDARY")
    require(type(cell.get("raw_bytes")) is bytes, "RAW_BYTES")
    raw_bytes = cell["raw_bytes"]
    require(result.get("raw_sha256") == hashlib.sha256(raw_bytes).hexdigest(), "RAW_DIGEST")
    try:
        raw = [json.loads(line) for line in raw_bytes.splitlines()]
    except (ValueError, TypeError) as error:
        raise ValueError("RAW_JSON") from error
    require(len(raw) == SAMPLES, "SAMPLES")
    seconds = []
    for index, sample in enumerate(raw):
        require(type(sample) is dict and type(sample.get("sample")) is int
                and sample["sample"] == index and _positive(sample.get("seconds")), "SAMPLE_SEQUENCE")
        _resource(sample.get("resources"), pin["resource_digest"])
        seconds.append(sample["seconds"])
    require(result.get("result_sha256") == digest(expected(case)), "RESULT_CHECKSUM")
    median = statistics.median(seconds)
    require(_positive(cell.get("trial_median_seconds")) and
            math.isclose(cell["trial_median_seconds"], median, rel_tol=1e-12)
            and _positive(result.get("latency_p50_ms"))
            and math.isclose(result["latency_p50_ms"], median * 1000, rel_tol=1e-12), "MEDIAN")
    return median


def compare(before, after):
    require(type(before) is list and type(after) is list and len(before) == len(after) == 3
            and all(_positive(v) for v in before + after), "TRIAL_MEDIANS")
    b, c = statistics.median(before), statistics.median(after)
    db = statistics.median(abs(v - b) for v in before)
    dc = statistics.median(abs(v - c) for v in after)
    noise = 3 * max(db, dc)
    ratio = c / b
    status = "pending_noise" if noise > .05 * b else "regression" if ratio > 1.05 else "within_tolerance"
    return {"baseline_median": b, "candidate_median": c, "noise": noise,
            "latency_ratio": ratio, "status": status,
            "improvement_proven": status != "pending_noise" and (b-c)/b >= .05
            and b-c > noise and all(a < z for z, a in zip(before, after))}


def audit_matrix(cells, pins):
    """Return an offline comparison; performance acceptance is always separate."""
    require(type(pins) is dict and set(pins) == set(SUBJECTS), "SUBJECTS")
    require(type(cells) is list and len(cells) == 48, "CELL_COUNT")
    expected_keys = {(r["case"], r["trial"], r["subject"]) for r in plan()}
    keys = [(c.get("case"), c.get("trial"), c.get("subject")) for c in cells if type(c) is dict]
    require(len(keys) == 48 and len(set(keys)) == 48 and set(keys) == expected_keys, "MATRIX_COVERAGE")
    medians = {key: audit_cell(cell, pins[cell["subject"]]) for key, cell in zip(keys, cells)}
    comparisons = []
    for case in CASES:
        require(len({c["measurement"]["result_sha256"] for c in cells if c["case"] == case}) == 1,
                "CROSS_SUBJECT_RESULT")
        trials = {subject: [medians[case, trial, subject] for trial in TRIALS] for subject in SUBJECTS}
        candidate = statistics.median(trials["candidate"])
        reference = statistics.median(trials["postgresql"])
        comparisons.append({"case": case, "status": "measured", "trials": trials,
                            "comparisons": {name: compare(trials[name], trials["candidate"])
                                            for name in ("before", "published")},
                            "reference_latency_ratio": candidate / reference,
                            "reference_throughput_ratio": reference / candidate,
                            "reference_goal_met": candidate <= reference})
    return {"schema": "alopex.native-sql-four-case-comparison/v1", "cases": comparisons,
            "verified_cells": 48, "verified_samples": 1008, "performance_acceptance": False}
