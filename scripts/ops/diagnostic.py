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
PHASES = ("setup", "precheck", "warmup", "measured", "postcheck")
CLIENT_FIELDS = ("bind_ns", "transport_ns", "parse_ns", "rows_ns", "residual_ns", "total_ns",
                 "response_chars", "result_rows")
STAGES = frozenset(("transaction_setup", "route_plan", "execute_multi",
                    "completion_and_commit", "result_map", "execute_and_map", "response_build"))


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
