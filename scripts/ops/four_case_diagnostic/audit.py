"""Independent, fail-closed four-case audit; no controller imports."""
from __future__ import annotations

import math
import statistics
import uuid
import hashlib
import json

from .oracle import ORACLE_SHA256, ROLLBACK_SHA256

CASES = ("aggregate", "ordered", "select-all", "insert-rollback")
PHASES = ("setup", "precheck", "warmup", "measured", "postcheck")
CLIENT_FIELDS = ("bind_ns", "transport_ns", "parse_ns", "rows_ns", "residual_ns",
                 "total_ns", "response_chars", "result_rows")
READ_STAGES = frozenset(("transaction_setup", "route_plan", "execute_multi",
                         "completion_and_commit", "result_map", "execute_and_map", "response_build"))
SESSION_STAGES = (READ_STAGES - {"transaction_setup"}) | {"session_setup", "session_completion"}
SETUP = {"aggregate": 345, "ordered": 345, "select-all": 345, "insert-rollback": 3}
POSTCHECK = {"aggregate": 0, "ordered": 0, "select-all": 0, "insert-rollback": 1}
COUNTERS = ("user_cpu_seconds", "system_cpu_seconds", "rss_before_bytes", "rss_after_bytes",
            "read_bytes", "write_bytes", "cancelled_write_bytes")


def canonical_uuid(value):
    require(type(value) is str, "uuid")
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError("uuid") from error
    require(str(parsed) == value and parsed.version == 4, "uuid")
    return value


def pair_order(trial, sample):
    require(type(trial) is int and trial in range(3) and type(sample) is int and sample in range(21), "pair_order")
    return ("off", "on") if (trial + sample) % 2 == 0 else ("on", "off")


def require(condition, code):
    if not condition:
        raise ValueError(code)


def nonnegative_int(value):
    return type(value) is int and value >= 0


def _raw_samples(raw_bytes, cell, resource_digest):
    require(type(raw_bytes) is bytes and 0 < len(raw_bytes) <= 1_000_000 and
            hashlib.sha256(raw_bytes).hexdigest() == cell["raw_sha256"], "raw_digest")
    require(raw_bytes.endswith(b"\n") and len(raw_bytes.splitlines()) == 21, "raw_inventory")
    identity = cell["pair_server"]
    client_identity = None
    for index, line in enumerate(raw_bytes.splitlines()):
        require(0 < len(line) <= 50_000, "raw_bound")
        try:
            sample = json.loads(line)
        except (UnicodeError, ValueError) as error:
            raise ValueError("raw_format") from error
        require(type(sample) is dict and set(sample) == {"sample", "seconds", "pair_ordinal", "resources"},
                "raw_fields")
        require(sample["sample"] == index and type(sample["seconds"]) in (int, float)
                and math.isfinite(sample["seconds"]) and sample["seconds"] > 0 and
                sample["seconds"] == cell["raw_seconds"][index] and
                sample["pair_ordinal"] == cell["raw_ordinals"][index], "raw_sample_join")
        resource = sample["resources"]
        require(type(resource) is dict and resource.get("schema") == "alopex.sql-operation-resources/v1"
                and resource.get("status") == "observed" and
                resource.get("coverage") == "stable_pid_namespace_allocation" and
                resource.get("manifest_sha256") == resource_digest, "resource_coverage")
        client, servers = resource.get("client"), resource.get("server_processes")
        require(type(client) is dict and type(servers) is list and len(servers) == 1 and
                type(servers[0]) is dict, "resource_processes")
        require({name: servers[0].get(name) for name in ("pid", "start_ticks")} == identity and
                client.get("pid") != identity["pid"], "resource_pid")
        observed_client = (client.get("pid"), client.get("start_ticks"))
        if client_identity is None:
            client_identity = observed_client
        require(observed_client == client_identity, "resource_pid")
        for process in (client, servers[0]):
            require(type(process.get("pid")) is int and process["pid"] > 0 and
                    type(process.get("start_ticks")) is int and process["start_ticks"] > 0 and
                    all(type(process.get(name)) in (int, float) and
                        math.isfinite(process[name]) and process[name] >= 0 for name in COUNTERS),
                    "resource_counters")
    return client_identity


def audit(cells, clients, servers, *, binding, expected_binding, expected_fixtures,
          raw_by_cell, resource_manifests, tolerance_ns=0):
    """Accept only allowlisted records; caller verifies sealed artifact/file hashes.

    `cells` contains 24 verified mode summaries from twelve adjacent pairs:
    case, trial, mode, oracle, 21 raw samples, pair identity and order.
    """
    require(nonnegative_int(tolerance_ns), "invalid_timer_tolerance")
    require(binding == expected_binding, "binding_identity")
    require(type(expected_fixtures) is dict and set(expected_fixtures) == set(CASES) and
            all(type(value) is str and len(value) == 64 and
                all(char in "0123456789abcdef" for char in value)
                for value in expected_fixtures.values()), "fixture_pin")
    require(type(binding) is dict and set(binding) == {"source", "assets_digest", "controller_sha256",
                                                   "operator_sha256", "same_job_id", "qualification_passed",
                                                   "security_passed", "cleanup_passed"}, "binding")
    require(type(binding["source"]) is dict and set(binding["source"]) == {"commit", "tree", "profile"}
            and all(type(binding["source"][name]) is str and len(binding["source"][name]) == 40
                    for name in ("commit", "tree")) and binding["source"]["profile"] == "release"
            and all(type(binding[name]) is str and len(binding[name]) == 64 and
                    all(c in "0123456789abcdef" for c in binding[name])
                    for name in ("assets_digest", "controller_sha256", "operator_sha256"))
            and type(binding["same_job_id"]) is str and bool(binding["same_job_id"])
            and binding["qualification_passed"] is True and binding["security_passed"] is True
            and binding["cleanup_passed"] is True, "binding")
    expected = {(case, trial, mode) for case in CASES for trial in range(3) for mode in ("on", "off")}
    by_cell = {}
    for cell in cells:
        require(type(cell) is dict and set(cell) == {"case", "trial", "mode", "result_sha256",
                                                    "raw_seconds", "raw_ordinals", "raw_sha256", "pair_server",
                                                    "fixture_sha256", "pair_order", "source", "assets_digest",
                                                    "same_job_id", "rollback_sha256", "cleanup_passed"}, "cell_fields")
        key = (cell["case"], cell["trial"], cell["mode"])
        require(key in expected and key not in by_cell, "cell_inventory")
        require(cell["result_sha256"] == ORACLE_SHA256[cell["case"]], "oracle_identity")
        require(cell["source"] == binding["source"] and cell["assets_digest"] == binding["assets_digest"]
                and cell["same_job_id"] == binding["same_job_id"] and cell["cleanup_passed"] is True,
                "cell_binding")
        require(cell["rollback_sha256"] == (ROLLBACK_SHA256 if cell["case"] == "insert-rollback" else None),
                "rollback_state")
        server = cell["pair_server"]
        require(type(server) is dict and set(server) == {"pid", "start_ticks"} and
                all(nonnegative_int(server[key]) and server[key] > 0 for key in server) and
                cell["fixture_sha256"] == expected_fixtures[cell["case"]] and
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
    require(type(raw_by_cell) is dict and set(raw_by_cell) == expected and
            type(resource_manifests) is dict and
            set(resource_manifests) == {(case, trial) for case in CASES for trial in range(3)},
            "raw_inventory")
    for key, cell in by_cell.items():
        digest = resource_manifests[(key[0], key[1])]
        require(type(digest) is str and len(digest) == 64 and
                all(char in "0123456789abcdef" for char in digest), "resource_manifest")
        _raw_samples(raw_by_cell[key], cell, digest)
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
        require(row.get("result_sha256") == (ROLLBACK_SHA256 if key[0] == "insert-rollback" and phase == "postcheck"
                                                 else ORACLE_SHA256[key[0]]) if phase != "setup" else True,
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
        for phase, required in (("precheck", 1), ("warmup", 3), ("measured", 21),
                                ("setup", SETUP[key[0]] if key[2] == "on" else 0),
                                ("postcheck", POSTCHECK[key[0]])):
            require(phases[phase] == set(range(required)), "phase_inventory")
        if key[2] == "on":
            measured += len(phases["measured"])
    require(measured == 252 and len(by_id) == 3417 and len(clients) == 3720, "client_inventory")
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
        require(stage in READ_STAGES | SESSION_STAGES and stage not in stages[trace_id], "server_stage_inventory")
        require(nonnegative_int(event["duration_ns"]) and event["duration_ns"] <= 300_000_000_000,
                "server_duration")
        stages[trace_id][stage] = event["duration_ns"]
    measured_events = 0
    residuals = []
    for trace_id, row in by_id.items():
        stage = stages[trace_id]
        required = (SESSION_STAGES if row["case"] == "insert-rollback" and
                    row["phase"] in ("precheck", "warmup", "measured") else READ_STAGES)
        require(set(stage) == required, "server_stage_inventory")
        if row["phase"] == "measured":
            measured_events += len(stage)
        inner = sum(stage[name] for name in (("session_setup" if required == SESSION_STAGES else "transaction_setup"),
                                                "route_plan", "execute_multi",
                                                "completion_and_commit", "result_map"))
        outer = stage["execute_and_map"] + stage["response_build"]
        require(inner <= stage["execute_and_map"] + tolerance_ns, "server_inner_excess")
        require(outer <= row["transport_ns"] + tolerance_ns, "server_transport_excess")
        residuals.append({"trace_id": trace_id, "unattributed_ns": stage["execute_and_map"] - inner,
                          "combined_transport_residual_ns": row["transport_ns"] - outer})
    require(measured_events == 1827 and len(servers) == 23994, "server_event_inventory")

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
        bound = .05 * statistics.median(off)
        # The upper bound must cover both within-trial adjacent-pair spread and
        # between-trial drift. Neither can be averaged away by the other.
        u = max(d + 2 * 1.4826 * mad, *trial_u)
        paired[case] = {"order": [[list(pair_order(i, j)) for j in range(21)] for i in range(3)],
                        "delta_ms": deltas, "trial_U_ms": trial_u,
                        "D_ms": d, "MAD_ms": mad, "U_ms": u,
                        "bound_ms": bound, "small_overhead": max(0, u) <= bound}
    return {"status": "verified", "performance_qualification": False, "measured_uuids": measured,
            "measured_server_events": measured_events, "all_uuids": len(by_id),
            "all_server_events": len(servers), "paired": paired,
            "timer_residuals": residuals}
