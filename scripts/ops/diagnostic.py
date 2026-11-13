"""Private #712 diagnostic adapter. Never export these records to public artifacts.

The caller owns the frozen SQL, oracle, raw timer and resource observer. This
adapter only supplies phase identity and a strict callback/operation join.
"""
from __future__ import annotations

import hashlib
import json
import math
import time
import uuid

CASES = ("aggregate", "ordered")
# Frozen 34,250-row fixture: one DROP, one CREATE, ceil(rows/100) INSERTs,
# one precheck, three warmups, and 21 measured operations. No postcheck call.
CLIENT_RECORDS_PER_CELL = 2 + (34_250 + 99) // 100 + 1 + 3 + 21
CLIENT_RECORDS_OFF_CELL = 1 + 3 + 21


def expected_client_records(mode):
    if mode == "on":
        return CLIENT_RECORDS_PER_CELL
    if mode == "off":
        return CLIENT_RECORDS_OFF_CELL
    raise ValueError("invalid diagnostic mode")


PHASES = ("setup", "precheck", "warmup", "measured", "postcheck")
CLIENT_FIELDS = ("bind_ns", "transport_ns", "parse_ns", "rows_ns", "residual_ns", "total_ns",
                 "response_chars", "result_rows")
STAGES = frozenset(("transaction_setup", "route_plan", "execute_multi",
                    "completion_and_commit", "result_map", "execute_and_map", "response_build"))
INNER_STAGES = frozenset((
    "session_lookup_wait", "session_operation_wait", "session_txn_wait",
    "bridge_operation_wait", "bridge_state_wait", "worker_dispatch",
    "blocking_dispatch", "worker_parse", "worker_plan", "worker_execute_plan",
    "query_pipeline_build", "query_row_iteration", "query_projection",
    "query_row_scan", "aggregate_parallel_build"))
INNER_ROUTES = frozenset(("local_only", "future_distributed_required",
                          "scatter_gather_simulated"))
INNER_ROOTS = frozenset(("aggregate", "sort", "filter", "scan", "limit", "project", "other"))
INNER_BRANCHES = frozenset((
    "ordered_limit_primary_key", "filter_btree_index", "filter_btree_fallback",
    "aggregate_streaming_global", "aggregate_streaming_grouped", "aggregate_parallel",
    "aggregate_iterator", "aggregate", "sort", "filter", "scan", "limit",
    "project", "explain", "other"))


def audit_inner_events(events, mode, clients, servers):
    """Reduce private inner events to measured UUIDs; never qualify performance."""
    if mode not in ("on", "off") or type(events) is not list or len(events) > 8_000:
        raise ValueError("INNER_BOUND")
    if mode == "off" and events:
        raise ValueError("INNER_OFF")
    if type(clients) is not list or type(servers) is not list:
        raise ValueError("INNER_JOIN")
    client_ids = set()
    measured_ids = set()
    for client in clients:
        if type(client) is not dict or "trace_id" not in client:
            if mode == "off":
                continue
            raise ValueError("INNER_JOIN")
        trace_id = canonical_uuid(client["trace_id"])
        if trace_id in client_ids:
            raise ValueError("INNER_JOIN")
        client_ids.add(trace_id)
        if client.get("phase") == "measured":
            measured_ids.add(trace_id)
    server_ids = set()
    for server in servers:
        if type(server) is not dict or set(server) != {"trace_id", "stage", "duration_ns"} or \
                server["stage"] not in STAGES or not _integer(server["duration_ns"]):
            raise ValueError("INNER_JOIN")
        server_ids.add(canonical_uuid(server["trace_id"]))
    if mode == "on" and not measured_ids.issubset(server_ids):
        raise ValueError("INNER_JOIN")
    grouped = {}
    seen = set()
    for event in events:
        if type(event) is not dict or event.get("kind") not in ("stage", "route", "branch"):
            raise ValueError("INNER_FORMAT")
        trace_id = canonical_uuid(event.get("trace_id"))
        if trace_id not in client_ids or trace_id not in server_ids:
            raise ValueError("INNER_JOIN")
        kind = event["kind"]
        if kind == "stage":
            if set(event) != {"kind", "trace_id", "stage", "duration_ns"} or \
                    event["stage"] not in INNER_STAGES or not _integer(event["duration_ns"]) or \
                    event["duration_ns"] > 300_000_000_000:
                raise ValueError("INNER_FORMAT")
        elif kind == "route":
            if set(event) != {"kind", "trace_id", "statement_index", "route", "root"} or \
                    type(event["statement_index"]) is not int or not 0 <= event["statement_index"] < 16 or \
                    event["route"] not in INNER_ROUTES or event["root"] not in INNER_ROOTS:
                raise ValueError("INNER_FORMAT")
        elif set(event) != {"kind", "trace_id", "branch"} or event["branch"] not in INNER_BRANCHES:
            raise ValueError("INNER_FORMAT")
        key = (trace_id, kind, tuple((name, event[name]) for name in sorted(event) if name not in ("trace_id", "kind")))
        if key in seen:
            raise ValueError("INNER_DUPLICATE")
        seen.add(key)
        row = grouped.setdefault(trace_id, {"trace_id": trace_id, "stages": [], "routes": [], "branches": []})
        bucket = {"stage": "stages", "route": "routes", "branch": "branches"}[kind]
        item = {name: value for name, value in event.items() if name not in ("kind", "trace_id")}
        row[bucket].append(item)
        if len(row["stages"]) > 64 or len(row["routes"]) > 16 or len(row["branches"]) > 64 or \
                sum(len(row[name]) for name in ("stages", "routes", "branches")) > 128 or \
                (kind == "route" and len({x["statement_index"] for x in row["routes"]}) != len(row["routes"])):
            raise ValueError("INNER_BOUND")
    if mode == "on":
        if not measured_ids.issubset(grouped) or any(
            not grouped[trace_id]["stages"] or not grouped[trace_id]["routes"] or
            not grouped[trace_id]["branches"] for trace_id in measured_ids
        ):
            raise ValueError("INNER_JOIN")
    return {"schema": "alopex.sql-private-inner/v1", "performance_qualification": False,
            "measured": [grouped[trace_id] for trace_id in sorted(grouped) if trace_id in measured_ids]}


def canonical_uuid(value):
    if type(value) is not str:
        raise ValueError("invalid trace id")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise ValueError("invalid trace id") from exc
    if str(parsed) != value or parsed.version != 4:
        raise ValueError("noncanonical trace id")
    return value


def _integer(value):
    return type(value) is int and value >= 0


def _safe_checksum(value):
    """Only a digest leaves this function; raw rows stay in the oracle caller."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


class CellAdapter:
    def __init__(self, case, trial, mode, oracle):
        if case not in CASES or type(trial) is not int or trial not in range(3) or mode not in ("on", "off"):
            raise ValueError("invalid diagnostic cell")
        self.case, self.trial, self.mode = case, trial, mode
        self.cell_id = f"{case}-{trial}-{mode}"
        self.oracle = oracle  # frozen validate(case, result, 34250)
        self.phase = None
        self.sample = None
        self.pending = []
        self.calls = []
        self._indices = {phase: 0 for phase in PHASES}
        self.expected_checksum = None

    def sink(self, timing):
        if self.mode != "on" or self.phase is None:
            raise ValueError("unexpected timing callback")
        if type(timing) is not dict or timing.get("schema") != "alopex.sql-client-timing/v1":
            raise ValueError("invalid client timing schema")
        trace_id = canonical_uuid(timing.get("trace_id"))
        if set(timing) != set(CLIENT_FIELDS) | {"schema", "trace_id"}:
            raise ValueError("unexpected client timing field")
        if not all(_integer(timing[name]) for name in CLIENT_FIELDS):
            raise ValueError("invalid client timing value")
        if timing["total_ns"] != sum(timing[name] for name in
                                     ("bind_ns", "transport_ns", "parse_ns", "rows_ns", "residual_ns")):
            raise ValueError("client timer mismatch")
        if any(row["trace_id"] == trace_id for row in self.pending + self.calls):
            raise ValueError("duplicate client trace id")
        self.pending.append({"trace_id": trace_id, **{name: timing[name] for name in CLIENT_FIELDS}})

    def call(self, phase, operation, *, sample=None, measure=False, defer_oracle=False):
        if phase not in PHASES or (phase == "measured") != measure:
            raise ValueError("invalid phase")
        if self.phase is not None or self.pending:
            raise ValueError("unjoined callback")
        index = self._indices[phase]
        if sample is not None and (type(sample) is not int or sample != index):
            raise ValueError("wrong sample index")
        self._indices[phase] += 1
        self.phase, self.sample = phase, index
        started = time.perf_counter()
        try:
            result = operation()
            elapsed_seconds = time.perf_counter() - started
            outer_ns = round(elapsed_seconds * 1_000_000_000)
        except BaseException:
            self.phase = self.sample = None
            raise  # failed SQL is an incomplete diagnostic
        self.phase = self.sample = None
        pending, self.pending = self.pending, []
        if len(pending) != (1 if self.mode == "on" else 0):
            raise ValueError("callback count mismatch")
        # Frozen oracle checks full values, order and types before digest use.
        if pending:
            row = pending[0]
            if outer_ns < row["total_ns"]:
                raise ValueError("negative wrapper residual")
            self.calls.append({"cell_id": self.cell_id, "case": self.case, "trial": self.trial,
                               "mode": self.mode, "phase": phase, "sample": index,
                               "outer_ns": outer_ns, "wrapper_ns": outer_ns - row["total_ns"], **row})
        else:
            self.calls.append({"cell_id": self.cell_id, "case": self.case, "trial": self.trial,
                               "mode": self.mode, "phase": phase, "sample": index,
                               "outer_ns": outer_ns})
        if not defer_oracle:
            self.finalize_result(result)
        return result, elapsed_seconds

    def finalize_result(self, result):
        if not self.calls or "result_sha256" in self.calls[-1] or self.calls[-1]["phase"] == "setup":
            raise ValueError("unexpected oracle finalization")
        checksum = self.oracle(self.case, result, 34250)
        if self.expected_checksum is None:
            self.expected_checksum = checksum
        if checksum != self.expected_checksum:
            raise ValueError("oracle identity mismatch")
        self.calls[-1]["result_sha256"] = checksum
        return checksum

    def setup(self, operation):
        """Classify setup SQL individually by wrapping each execute() call."""
        index = self._indices["setup"]
        self._indices["setup"] += 1
        if self.phase is not None or self.pending:
            raise ValueError("unjoined setup callback")
        self.phase, self.sample = "setup", index
        try:
            result = operation()
        finally:
            self.phase = self.sample = None
        pending, self.pending = self.pending, []
        if len(pending) != (1 if self.mode == "on" else 0):
            raise ValueError("setup callback count mismatch")
        if pending:
            self.calls.append({"cell_id": self.cell_id, "case": self.case, "trial": self.trial,
                               "mode": self.mode, "phase": "setup", "sample": index, **pending[0]})
        return result

    def finish(self):
        counts = {phase: sum(row["phase"] == phase for row in self.calls) for phase in PHASES}
        if self.phase is not None or self.pending or counts["precheck"] != 1 or counts["warmup"] != 3 or counts["measured"] != 21:
            raise ValueError("incomplete cell")
        if any(row.get("result_sha256") != self.expected_checksum for row in self.calls if row["phase"] != "setup"):
            raise ValueError("incomplete oracle inventory")
        if len({row.get("trace_id") for row in self.calls if "trace_id" in row}) != (len(self.calls) if self.mode == "on" else 0):
            raise ValueError("duplicate trace id")
        return counts


def cell_order(trial):
    if type(trial) is not int or trial not in range(3):
        raise ValueError("invalid trial")
    return ("on", "off") if trial == 1 else ("off", "on")


def pair_order(trial, sample):
    """Alternate adjacent requests, reversing the first order by trial."""
    if type(trial) is not int or trial not in range(3) or type(sample) is not int or sample not in range(21):
        raise ValueError("invalid diagnostic pair")
    return ("off", "on") if (trial + sample) % 2 == 0 else ("on", "off")
