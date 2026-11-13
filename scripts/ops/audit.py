"""Independent, fail-closed join auditor for private #712 trace records."""
from __future__ import annotations

import math
import statistics

from diagnostic import CASES, CLIENT_FIELDS, PHASES, STAGES, canonical_uuid, pair_order


def require(condition, code):
    if not condition:
        raise ValueError(code)


def nonnegative_int(value):
    return type(value) is int and value >= 0


def audit(cells, clients, servers, *, expected_oracle, tolerance_ns=0):
    """Accept only allowlisted records; caller verifies sealed artifact/file hashes.

    `cells` contains 12 verified mode summaries from six adjacent pairs:
    case, trial, mode, oracle, 21 raw samples, pair identity and order.
    """
    require(nonnegative_int(tolerance_ns), "invalid_timer_tolerance")
    expected = {(case, trial, mode) for case in CASES for trial in range(3) for mode in ("on", "off")}
    by_cell = {}
    for cell in cells:
        require(type(cell) is dict and set(cell) == {"case", "trial", "mode", "result_sha256",
                                                    "raw_seconds", "raw_ordinals", "raw_sha256", "pair_server",
                                                    "fixture_sha256", "pair_order"}, "cell_fields")
        key = (cell["case"], cell["trial"], cell["mode"])
        require(key in expected and key not in by_cell, "cell_inventory")
        require(cell["result_sha256"] == expected_oracle[cell["case"]], "oracle_identity")
        server = cell["pair_server"]
        require(type(server) is dict and set(server) == {"pid", "start_ticks"} and
                all(nonnegative_int(server[key]) and server[key] > 0 for key in server) and
                cell["fixture_sha256"] == expected_oracle[cell["case"]] and
                cell["pair_order"] == [list(pair_order(cell["trial"], i)) for i in range(21)],
                "pair_identity")
        require(type(cell["raw_sha256"]) is str and len(cell["raw_sha256"]) == 64 and
                all(c in "0123456789abcdef" for c in cell["raw_sha256"]), "raw_identity")
        raw = cell["raw_seconds"]
        require(type(raw) is list and len(raw) == 21 and
                all(type(x) in (int, float) and x > 0 and x < float("inf") for x in raw), "raw_samples")
        require(type(cell["raw_ordinals"]) is list and len(cell["raw_ordinals"]) == 21 and
                all(type(x) is int and 0 <= x < 42 for x in cell["raw_ordinals"]), "pair_order")
        by_cell[key] = cell
    require(set(by_cell) == expected, "cell_inventory")
    for case in CASES:
        for trial in range(3):
            on, off = (by_cell[(case, trial, mode)] for mode in ("on", "off"))
            require(on["pair_server"] == off["pair_server"] and
                    on["fixture_sha256"] == off["fixture_sha256"] and
                    on["pair_order"] == off["pair_order"], "pair_identity")

    by_id = {}
    counts = {key: {phase: set() for phase in PHASES} for key in expected}
    measured_timers = {}
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
        if phase == "measured":
            require(type(row.get("pair_ordinal")) is int and
                    0 <= row["pair_ordinal"] < 42, "pair_order")
            measured_timers[(key, sample)] = (row["outer_ns"], row["pair_ordinal"])
        if key[2] == "off":
            expected_off = {"cell_id", "case", "trial", "mode", "phase", "sample"}
            if phase != "setup":
                expected_off |= {"outer_ns", "result_sha256"}
            if phase == "measured":
                expected_off.add("pair_ordinal")
            require(set(row) == expected_off, "off_trace")
            continue
        base = {"cell_id", "case", "trial", "mode", "phase", "sample", "trace_id", *CLIENT_FIELDS}
        if phase != "setup":
            base |= {"outer_ns", "wrapper_ns", "result_sha256"}
        if phase == "measured":
            base.add("pair_ordinal")
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
    for key, cell in by_cell.items():
        for sample, seconds in enumerate(cell["raw_seconds"]):
            require(measured_timers[(key, sample)] ==
                    (round(seconds * 1_000_000_000), cell["raw_ordinals"][sample]),
                    "raw_client_timer_join")
    for case in CASES:
        for trial in range(3):
            for sample in range(21):
                for position, mode in enumerate(pair_order(trial, sample)):
                    key = (case, trial, mode)
                    require(by_cell[key]["raw_ordinals"][sample] == 2 * sample + position,
                            "pair_order")

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
        trial_u = []
        off = []
        for trial in range(3):
            on_raw = by_cell[(case, trial, "on")]["raw_seconds"]
            off_raw = by_cell[(case, trial, "off")]["raw_seconds"]
            off_median = statistics.median(off_raw) * 1000
            off.append(off_median)
            adjacent = [(on_raw[i] - off_raw[i]) * 1000 for i in range(21)]
            trial_d = statistics.median(adjacent)
            trial_mad = statistics.median(abs(x - trial_d) for x in adjacent)
            deltas.append(trial_d)
            trial_u.append(trial_d + 2 * 1.4826 * trial_mad / math.sqrt(21))
        d = statistics.median(deltas)
        mad = statistics.median(abs(x - d) for x in deltas)
        gap = {"aggregate": 16.106 - 1.093, "ordered": 4.857 - .1938}[case]
        bound = min(.05 * statistics.median(off), .10 * gap)
        # The upper bound must cover both within-trial adjacent-pair spread and
        # between-trial drift. Neither can be averaged away by the other.
        u = max(d + 2 * 1.4826 * mad, *trial_u)
        paired[case] = {"order": [[list(pair_order(i, j)) for j in range(21)] for i in range(3)],
                        "delta_ms": deltas, "trial_U_ms": trial_u,
                        "D_ms": d, "MAD_ms": mad, "U_ms": u,
                        "bound_ms": bound, "small_overhead": max(0, u) <= bound}
    return {"status": "verified", "measured_uuids": measured,
            "measured_server_events": measured_events, "all_uuids": len(by_id),
            "all_server_events": len(servers), "paired": paired,
            "timer_residuals": residuals}
