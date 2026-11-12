"""Independent, fail-closed join auditor for private #712 trace records."""
from __future__ import annotations

import statistics

from diagnostic import CASES, CLIENT_FIELDS, PHASES, STAGES, canonical_uuid, cell_order


def require(condition, code):
    if not condition:
        raise ValueError(code)


def nonnegative_int(value):
    return type(value) is int and value >= 0


def audit(cells, clients, servers, *, expected_oracle, tolerance_ns=0):
    """Accept only allowlisted records; caller verifies sealed artifact/file hashes.

    `cells` contains 12 independently verified frozen raw sample summaries:
    case, trial, mode, result_sha256, raw_seconds (21 items), raw_sha256.
    """
    require(nonnegative_int(tolerance_ns), "invalid_timer_tolerance")
    expected = {(case, trial, mode) for case in CASES for trial in range(3) for mode in ("on", "off")}
    by_cell = {}
    for cell in cells:
        require(type(cell) is dict and set(cell) == {"case", "trial", "mode", "result_sha256",
                                                    "raw_seconds", "raw_sha256"}, "cell_fields")
        key = (cell["case"], cell["trial"], cell["mode"])
        require(key in expected and key not in by_cell, "cell_inventory")
        require(cell["result_sha256"] == expected_oracle[cell["case"]], "oracle_identity")
        require(type(cell["raw_sha256"]) is str and len(cell["raw_sha256"]) == 64 and
                all(c in "0123456789abcdef" for c in cell["raw_sha256"]), "raw_identity")
        raw = cell["raw_seconds"]
        require(type(raw) is list and len(raw) == 21 and
                all(type(x) in (int, float) and x > 0 and x < float("inf") for x in raw), "raw_samples")
        by_cell[key] = cell
    require(set(by_cell) == expected, "cell_inventory")

    by_id = {}
    counts = {key: {phase: set() for phase in PHASES} for key in expected}
    for row in clients:
        require(type(row) is dict, "client_fields")
        key = (row.get("case"), row.get("trial"), row.get("mode"))
        require(key in expected and row.get("cell_id") == f"{key[0]}-{key[1]}-{key[2]}", "client_cell")
        phase, sample = row.get("phase"), row.get("sample")
        require(phase in PHASES and nonnegative_int(sample), "client_phase")
        require(sample not in counts[key][phase], "duplicate_phase_sample")
        counts[key][phase].add(sample)
        require(row.get("result_sha256") == expected_oracle[key[0]] if phase != "setup" else True,
                "oracle_identity")
        require(nonnegative_int(row.get("outer_ns")) if phase != "setup" else True, "outer_timer")
        if key[2] == "off":
            require(set(row) == {"cell_id", "case", "trial", "mode", "phase", "sample",
                                 "outer_ns", "result_sha256"} if phase != "setup" else
                    {"cell_id", "case", "trial", "mode", "phase", "sample"}, "off_trace")
            continue
        base = {"cell_id", "case", "trial", "mode", "phase", "sample", "trace_id", *CLIENT_FIELDS}
        if phase != "setup":
            base |= {"outer_ns", "wrapper_ns", "result_sha256"}
        require(set(row) == base, "client_fields")
        trace_id = canonical_uuid(row["trace_id"])
        require(trace_id not in by_id, "duplicate_uuid")
        require(all(nonnegative_int(row[name]) for name in CLIENT_FIELDS), "client_duration")
        require(row["total_ns"] == sum(row[name] for name in
                ("bind_ns", "transport_ns", "parse_ns", "rows_ns", "residual_ns")), "client_reconcile")
        if phase != "setup":
            require(nonnegative_int(row["wrapper_ns"]) and
                    row["outer_ns"] == row["total_ns"] + row["wrapper_ns"], "wrapper_reconcile")
        by_id[trace_id] = row

    measured = 0
    for key, phases in counts.items():
        for phase, required in (("precheck", 1), ("warmup", 3), ("measured", 21)):
            require(phases[phase] == set(range(required)), "phase_inventory")
        for phase in ("setup", "postcheck"):
            require(phases[phase] == set(range(len(phases[phase]))), "phase_inventory")
        if key[2] == "on":
            measured += len(phases["measured"])
    require(measured == 126, "measured_inventory")

    stages = {trace_id: {} for trace_id in by_id}
    for event in servers:
        require(type(event) is dict and set(event) == {"trace_id", "stage", "duration_ns"}, "server_fields")
        trace_id = canonical_uuid(event["trace_id"])
        require(trace_id in by_id, "unmatched_server_uuid")
        stage = event["stage"]
        require(stage in STAGES and stage not in stages[trace_id], "server_stage_inventory")
        require(nonnegative_int(event["duration_ns"]), "server_duration")
        stages[trace_id][stage] = event["duration_ns"]
    measured_events = 0
    residuals = []
    for trace_id, row in by_id.items():
        stage = stages[trace_id]
        require(set(stage) == STAGES, "server_stage_inventory")
        if row["phase"] == "measured":
            measured_events += len(stage)
        inner = sum(stage[name] for name in ("transaction_setup", "route_plan", "execute_multi",
                                                "completion_and_commit", "result_map"))
        outer = stage["execute_and_map"] + stage["response_build"]
        require(inner <= stage["execute_and_map"] + tolerance_ns, "server_inner_excess")
        require(outer <= row["transport_ns"] + tolerance_ns, "server_transport_excess")
        residuals.append({"trace_id": trace_id, "unattributed_ns": stage["execute_and_map"] - inner,
                          "combined_transport_residual_ns": row["transport_ns"] - outer})
    require(measured_events == 882, "measured_event_inventory")

    paired = {}
    for case in CASES:
        deltas = []
        off = []
        for trial in range(3):
            on_median = statistics.median(by_cell[(case, trial, "on")]["raw_seconds"]) * 1000
            off_median = statistics.median(by_cell[(case, trial, "off")]["raw_seconds"]) * 1000
            off.append(off_median)
            deltas.append(on_median - off_median)
        d = statistics.median(deltas)
        mad = statistics.median(abs(x - d) for x in deltas)
        gap = {"aggregate": 16.106 - 1.093, "ordered": 4.857 - .1938}[case]
        bound = min(.05 * statistics.median(off), .10 * gap)
        u = d + 2 * 1.4826 * mad
        paired[case] = {"order": [list(cell_order(i)) for i in range(3)],
                        "delta_ms": deltas, "D_ms": d, "MAD_ms": mad, "U_ms": u,
                        "bound_ms": bound, "small_overhead": max(0, u) <= bound}
    return {"status": "verified", "measured_uuids": measured,
            "measured_server_events": measured_events, "all_uuids": len(by_id),
            "all_server_events": len(servers), "paired": paired,
            "timer_residuals": residuals}
