"""Bounded public numeric projections of one four-case diagnostic run.

Each resource counter is observed per operation. Server counters are the sum
across the observed server process tree, including RSS before and after; this
is an aggregate of process RSS values, not unique physical memory. Private
trace and process identities are used only by callers before projection.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import stat

from . import adapter, log_parser, public_timing

SCHEMA = "four_case_attribution_v1"
STAGE_SCHEMA = "four_case_stage_v1"
COUNTERS = ("user_cpu_seconds", "system_cpu_seconds", "rss_before_bytes",
            "rss_after_bytes", "read_bytes", "write_bytes", "cancelled_write_bytes")
PHASES = adapter.CLIENT_FIELDS
BASE_FIELDS = ("schema", "case", "trial", "mode", "sample", "pair_ordinal",
               "elapsed_seconds", "coverage")
RESOURCE_FIELDS = tuple(f"{scope}_{name}" for scope in ("client", "server")
                        for name in COUNTERS)
OPERATION_FIELDS_ON = BASE_FIELDS + PHASES + RESOURCE_FIELDS
OPERATION_FIELDS_OFF = BASE_FIELDS + RESOURCE_FIELDS
STAGE_FIELDS = ("schema", "case", "trial", "sample", "stage", "duration_ns")
STAGE_ORDER_READ = ("transaction_setup", "route_plan", "execute_multi",
                    "completion_and_commit", "result_map", "execute_and_map",
                    "response_build")
STAGE_ORDER_SESSION = ("session_setup", "route_plan", "execute_multi",
                       "session_completion", "result_map", "execute_and_map",
                       "response_build")
COVERAGE = "stable_pid_namespace_allocation"
MAX_DURATION_NS = log_parser.MAX_DURATION_NS
MAX_OPERATION_BYTES = 80_000
MAX_STAGE_BYTES = 40_000
MAX_COUNTER_BYTES = 1_000_000_000_000_000


def _require(ok, code="ATTRIBUTION_INVALID"):
    if not ok:
        raise ValueError(code)


def _number(value, ceiling):
    return type(value) in (int, float) and 0 <= value <= ceiling and math.isfinite(value)


def _integer(value, ceiling):
    return type(value) is int and 0 <= value <= ceiling


def _identity(case, trial, mode=None, sample=None):
    _require(type(case) is str and case in adapter.CASES and
             type(trial) is int and trial in range(3), "ATTRIBUTION_ID")
    if mode is not None:
        _require(type(mode) is str and mode in public_timing.MODES, "ATTRIBUTION_ID")
    if sample is not None:
        _require(type(sample) is int and sample in range(public_timing.SAMPLES),
                 "ATTRIBUTION_ID")


def _ordinal(trial, mode, sample):
    return 2 * sample + adapter.pair_order(trial, sample).index(mode)


def _encode(row):
    return json.dumps(row, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n"


def _directory(root):
    path = Path(root)
    _require(path.is_absolute(), "ATTRIBUTION_DIRECTORY")
    for component in (*reversed(path.parents), path):
        try:
            info = component.lstat()
        except OSError as error:
            raise ValueError("ATTRIBUTION_DIRECTORY") from error
        _require(stat.S_ISDIR(info.st_mode), "ATTRIBUTION_DIRECTORY")
    info = path.lstat()
    _require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700,
             "ATTRIBUTION_DIRECTORY")
    return path


def operation_filename(case, trial, mode):
    _identity(case, trial, mode)
    return f"{case}-trial-{trial}-{mode}.jsonl"


def stage_filename(case, trial):
    _identity(case, trial)
    return f"{case}-trial-{trial}-stages.jsonl"


def _project_resources(resource):
    _require(type(resource) is dict and resource.get("schema") ==
             "alopex.sql-operation-resources/v1" and resource.get("status") == "observed" and
             resource.get("coverage") == COVERAGE, "ATTRIBUTION_RESOURCE")
    client, servers = resource.get("client"), resource.get("server_processes")
    _require(type(client) is dict and type(servers) is list and 1 <= len(servers) <= 64 and
             all(type(item) is dict for item in servers), "ATTRIBUTION_RESOURCE")
    values = {}
    for name in COUNTERS:
        _require(_number(client.get(name), MAX_COUNTER_BYTES), "ATTRIBUTION_RESOURCE")
        values[f"client_{name}"] = client[name]
    for name in COUNTERS:
        total = 0
        for server in servers:
            _require(_number(server.get(name), MAX_COUNTER_BYTES), "ATTRIBUTION_RESOURCE")
            total += server[name]
        _require(_number(total, MAX_COUNTER_BYTES), "ATTRIBUTION_RESOURCE")
        values[f"server_{name}"] = total
    return values


def operation_record(case, trial, mode, sample, pair_ordinal, elapsed_seconds,
                     client_row, resource_row):
    """Project one measured call; no private identifier can enter the result."""
    _identity(case, trial, mode, sample)
    _require(type(pair_ordinal) is int and pair_ordinal == _ordinal(trial, mode, sample),
             "ATTRIBUTION_ORDER")
    _require(_number(elapsed_seconds, MAX_DURATION_NS / 1e9), "ATTRIBUTION_DURATION")
    row = dict(zip(BASE_FIELDS, (SCHEMA, case, trial, mode, sample, pair_ordinal,
                                 elapsed_seconds, COVERAGE)))
    if mode == "on":
        _require(type(client_row) is dict and client_row.get("phase") == "measured" and
                 client_row.get("sample") == sample, "ATTRIBUTION_PHASE")
        for name in PHASES:
            _require(_integer(client_row.get(name), MAX_DURATION_NS if name.endswith("_ns")
                              else MAX_COUNTER_BYTES), "ATTRIBUTION_PHASE")
            row[name] = client_row[name]
        _require(row["total_ns"] == sum(row[name] for name in
                 ("bind_ns", "transport_ns", "parse_ns", "rows_ns", "residual_ns")),
                 "ATTRIBUTION_PHASE")
    else:
        _require(client_row is None, "ATTRIBUTION_PHASE")
    row.update(_project_resources(resource_row))
    _require(len(_encode(row).encode("ascii")) <= 2048, "ATTRIBUTION_BOUND")
    return row


class OperationCellWriter:
    """Create one fixed cell; append measured rows in sample order."""

    def __init__(self, root, case, trial, mode):
        self.case, self.trial, self.mode = case, trial, mode
        self.next_sample = 0
        self.path = _directory(root) / operation_filename(case, trial, mode)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self._stream = os.fdopen(fd, "w", encoding="ascii", newline="\n")

    def append(self, sample, pair_ordinal, elapsed_seconds, client_row, resource_row):
        _require(not self._stream.closed and sample == self.next_sample,
                 "ATTRIBUTION_ORDER")
        row = operation_record(self.case, self.trial, self.mode, sample, pair_ordinal,
                               elapsed_seconds, client_row, resource_row)
        self._stream.write(_encode(row))
        self._stream.flush()
        self.next_sample += 1
        return row

    def close(self):
        self._stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def _stage_order(case):
    return STAGE_ORDER_SESSION if case == "insert-rollback" else STAGE_ORDER_READ


def stage_record(case, trial, sample, stage, duration_ns):
    _identity(case, trial, sample=sample)
    _require(type(stage) is str and stage in _stage_order(case), "ATTRIBUTION_STAGE")
    _require(_integer(duration_ns, MAX_DURATION_NS), "ATTRIBUTION_DURATION")
    return dict(zip(STAGE_FIELDS, (STAGE_SCHEMA, case, trial, sample, stage, duration_ns)))


def stage_records(case, trial, on_clients, events):
    """Project measured stages from a privately joined pair, without identities."""
    _identity(case, trial)
    _require(type(on_clients) is list and type(events) is list,
             "ATTRIBUTION_STAGE_INVENTORY")
    traces = {}
    for client in on_clients:
        if type(client) is dict and client.get("phase") == "measured":
            sample, trace = client.get("sample"), client.get("trace_id")
            _identity(case, trial, sample=sample)
            _require(type(trace) is str and trace not in traces and
                     client.get("case") == case and client.get("trial") == trial and
                     client.get("mode") == "on", "ATTRIBUTION_STAGE_INVENTORY")
            traces[trace] = sample
    _require(len(traces) == 21 and set(traces.values()) == set(range(21)),
             "ATTRIBUTION_STAGE_INVENTORY")
    indexed = {}
    for event in events:
        _require(type(event) is dict and set(event) == {"trace_id", "stage", "duration_ns"},
                 "ATTRIBUTION_STAGE_INVENTORY")
        trace = event["trace_id"]
        _require(type(trace) is str and type(event["stage"]) is str,
                 "ATTRIBUTION_STAGE_INVENTORY")
        if trace not in traces:
            continue  # setup, precheck, warmup, and postcheck stay private
        sample = traces[trace]
        key = (sample, event["stage"])
        _require(key not in indexed, "ATTRIBUTION_STAGE_INVENTORY")
        indexed[key] = stage_record(case, trial, sample, event["stage"], event["duration_ns"])
    order = _stage_order(case)
    _require(set(indexed) == {(sample, stage) for sample in range(21) for stage in order},
             "ATTRIBUTION_STAGE_INVENTORY")
    rows = [indexed[(sample, stage)] for sample in range(21) for stage in order]
    return rows


def write_stage_cell(root, case, trial, on_clients, events):
    """Write a canonical stage cell after the caller has verified private joins."""
    rows = stage_records(case, trial, on_clients, events)
    path = _directory(root) / stage_filename(case, trial)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="ascii", newline="\n") as stream:
        stream.writelines(_encode(row) for row in rows)
    return rows


def _strict_pairs(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "ATTRIBUTION_FIELDS")
        result[key] = value
    return result


def _read(path, count, bound):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            before = os.fstat(stream.fileno())
            _require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and
                     before.st_uid == os.geteuid() and stat.S_IMODE(before.st_mode) == 0o600 and
                     before.st_size <= bound, "ATTRIBUTION_FILE")
            raw = stream.read(bound + 1)
            after = os.fstat(stream.fileno())
            _require(len(raw) == before.st_size and
                     (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                     (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
                     "ATTRIBUTION_FILE")
    except OSError as error:
        raise ValueError("ATTRIBUTION_FILE") from error
    _require(len(raw) <= bound and raw.count(b"\n") == count and raw.endswith(b"\n"),
             "ATTRIBUTION_INVENTORY")
    rows = []
    for line in raw.splitlines(keepends=True):
        try:
            row = json.loads(line.decode("ascii"), object_pairs_hook=_strict_pairs,
                             parse_constant=lambda _: (_require(False, "ATTRIBUTION_NUMBER")))
        except (UnicodeError, json.JSONDecodeError) as error:
            raise ValueError("ATTRIBUTION_FORMAT") from error
        _require(type(row) is dict and _encode(row).encode("ascii") == line,
                 "ATTRIBUTION_FORMAT")
        rows.append(row)
    return rows


def _check_operation(row, case, trial, mode, sample):
    _require(tuple(row) == (OPERATION_FIELDS_ON if mode == "on" else OPERATION_FIELDS_OFF),
             "ATTRIBUTION_FIELDS")
    _require(row["schema"] == SCHEMA and row["case"] == case and row["trial"] == trial and
             row["mode"] == mode and row["sample"] == sample and
             type(row["pair_ordinal"]) is int and
             row["pair_ordinal"] == _ordinal(trial, mode, sample) and
             _number(row["elapsed_seconds"], MAX_DURATION_NS / 1e9) and
             row["coverage"] == COVERAGE, "ATTRIBUTION_OPERATION")
    if mode == "on":
        _require(all(_integer(row[name], MAX_DURATION_NS if name.endswith("_ns") else
                             MAX_COUNTER_BYTES) for name in PHASES) and
                 row["total_ns"] == sum(row[name] for name in
                 ("bind_ns", "transport_ns", "parse_ns", "rows_ns", "residual_ns")),
                 "ATTRIBUTION_PHASE")
    _require(all(_number(row[name], MAX_COUNTER_BYTES) for name in RESOURCE_FIELDS),
             "ATTRIBUTION_RESOURCE")


def _read_operation_cell(path, case, trial, mode):
    """Strictly read one operation cell for the private/public pair audit."""
    _identity(case, trial, mode)
    rows = _read(path, 21, MAX_OPERATION_BYTES)
    for sample, row in enumerate(rows):
        _check_operation(row, case, trial, mode, sample)
    return rows


def load_operation_inventory(root):
    """Require all 504 operation projections across 24 canonical cells."""
    directory = _directory(root)
    expected = {operation_filename(case, trial, mode) for case in adapter.CASES
                for trial in range(3) for mode in public_timing.MODES}
    _require({path.name for path in directory.iterdir()} == expected,
             "ATTRIBUTION_FILE_INVENTORY")
    cells = {}
    for case in adapter.CASES:
        for trial in range(3):
            for mode in public_timing.MODES:
                cells[(case, trial, mode)] = _read_operation_cell(
                    directory / operation_filename(case, trial, mode), case, trial, mode)
    return [cells[(case, trial, mode)][sample] for case in adapter.CASES
            for trial in range(3) for sample in range(21)
            for mode in adapter.pair_order(trial, sample)]


def load_stage_inventory(root):
    """Require all 1,764 fixed server stages across 12 canonical cells."""
    directory = _directory(root)
    expected = {stage_filename(case, trial) for case in adapter.CASES for trial in range(3)}
    _require({path.name for path in directory.iterdir()} == expected,
             "ATTRIBUTION_FILE_INVENTORY")
    result = []
    for case in adapter.CASES:
        for trial in range(3):
            rows = _read(directory / stage_filename(case, trial), 147, MAX_STAGE_BYTES)
            keys = ((sample, stage) for sample in range(21) for stage in _stage_order(case))
            for row, (sample, stage) in zip(rows, keys):
                _require(tuple(row) == STAGE_FIELDS and row ==
                         stage_record(case, trial, sample, stage, row.get("duration_ns")),
                         "ATTRIBUTION_STAGE_INVENTORY")
            result.extend(rows)
    return result
