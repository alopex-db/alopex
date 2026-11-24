"""Fixed, public-safe four-case timing inventory and paired summary."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import stat
import statistics

from . import adapter


SCHEMA = "four_case_timing_v1"
MODES = ("on", "off")
FIELDS = ("schema", "case", "trial", "mode", "sample", "pair_ordinal", "elapsed_seconds")
SAMPLES = 21
MAX_CELL_BYTES = 8192


def _require(condition, code):
    if not condition:
        raise ValueError(code)


def _cell(case, trial, mode):
    _require(type(case) is str and case in adapter.CASES, "timing_case")
    _require(type(trial) is int and trial in range(3), "timing_trial")
    _require(type(mode) is str and mode in MODES, "timing_mode")


def cell_filename(case, trial, mode):
    """Derive a file name solely from validated, fixed identifiers."""
    _cell(case, trial, mode)
    return f"{case}-trial-{trial}-{mode}.jsonl"


def _record(case, trial, mode, sample, pair_ordinal, elapsed_seconds):
    _cell(case, trial, mode)
    _require(type(sample) is int and sample in range(SAMPLES), "timing_sample")
    _require(type(pair_ordinal) is int and pair_ordinal ==
             2 * sample + adapter.pair_order(trial, sample).index(mode), "timing_pair_order")
    _require(_finite_nonnegative(elapsed_seconds), "timing_duration")
    return dict(zip(FIELDS, (SCHEMA, case, trial, mode, sample, pair_ordinal,
                             elapsed_seconds)))


def _encode(record):
    return json.dumps(record, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _finite_nonnegative(value):
    if type(value) not in (int, float) or value < 0:
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _directory(root):
    directory = Path(root)
    _require(directory.is_absolute(), "timing_directory")
    for component in (*reversed(directory.parents), directory):
        try:
            info = component.lstat()
        except OSError as error:
            raise ValueError("timing_directory") from error
        _require(stat.S_ISDIR(info.st_mode), "timing_directory")
    info = directory.lstat()
    _require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700,
             "timing_directory")
    return directory


class TimingCellWriter:
    """Create one fixed cell and append a record at each measured call boundary."""

    def __init__(self, root, case, trial, mode):
        filename = cell_filename(case, trial, mode)
        self.case, self.trial, self.mode = case, trial, mode
        self.next_sample = 0
        directory = _directory(root)
        self.path = directory / filename
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600)
        self._stream = os.fdopen(fd, "w", encoding="utf-8", newline="\n")

    def append(self, sample, pair_ordinal, elapsed_seconds):
        _require(not self._stream.closed, "timing_closed")
        _require(sample == self.next_sample, "timing_order")
        record = _record(self.case, self.trial, self.mode, sample, pair_ordinal,
                         elapsed_seconds)
        self._stream.write(_encode(record) + "\n")
        self._stream.flush()
        self.next_sample += 1
        return record

    def close(self):
        self._stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def _strict_object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "timing_duplicate_field")
        result[key] = value
    return result


def _read_cell(path, case, trial, mode):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                     info.st_uid == os.getuid() and info.st_size <= MAX_CELL_BYTES,
                     "timing_file")
            raw = stream.read(MAX_CELL_BYTES + 1)
    except OSError as error:
        raise ValueError("timing_file") from error
    _require(len(raw) <= MAX_CELL_BYTES, "timing_file")
    try:
        lines = raw.decode("utf-8").splitlines(keepends=True)
    except UnicodeError as error:
        raise ValueError("timing_format") from error
    _require(len(lines) == SAMPLES, "timing_inventory")
    records = []
    for sample, line in enumerate(lines):
        _require(line.endswith("\n"), "timing_format")
        try:
            parsed = json.loads(line, object_pairs_hook=_strict_object,
                                parse_constant=lambda _: (_require(False, "timing_duration")))
        except (json.JSONDecodeError, UnicodeError) as error:
            raise ValueError("timing_format") from error
        _require(type(parsed) is dict and tuple(parsed) == FIELDS, "timing_fields")
        expected = _record(case, trial, mode, sample, parsed["pair_ordinal"],
                           parsed["elapsed_seconds"])
        _require(parsed == expected and line == _encode(expected) + "\n", "timing_record")
        records.append(expected)
    return records


def load_timing_inventory(root):
    """Read exactly 24 canonical cell files into pair execution order."""
    directory = _directory(root)
    expected_names = {cell_filename(case, trial, mode)
                      for case in adapter.CASES for trial in range(3) for mode in MODES}
    _require({path.name for path in directory.iterdir()} == expected_names,
             "timing_file_inventory")
    cells = {(case, trial, mode): _read_cell(directory / cell_filename(case, trial, mode),
                                            case, trial, mode)
             for case in adapter.CASES for trial in range(3) for mode in MODES}
    return [cells[(case, trial, mode)][sample]
            for case in adapter.CASES for trial in range(3)
            for sample in range(SAMPLES)
            for mode in adapter.pair_order(trial, sample)]


def summarize_timing_records(records):
    """Validate the full public inventory and reproduce audit.py paired arithmetic."""
    _require(type(records) is list and len(records) == len(adapter.CASES) * 3 * 2 * SAMPLES,
             "timing_inventory")
    cells = {(case, trial, mode): [] for case in adapter.CASES
             for trial in range(3) for mode in MODES}
    expected = ((case, trial, mode, sample)
                for case in adapter.CASES for trial in range(3)
                for sample in range(SAMPLES)
                for mode in adapter.pair_order(trial, sample))
    for record, (case, trial, mode, sample) in zip(records, expected):
        _require(type(record) is dict and tuple(record) == FIELDS, "timing_fields")
        checked = _record(case, trial, mode, sample, record["pair_ordinal"],
                          record["elapsed_seconds"])
        _require(record == checked, "timing_record")
        cells[(case, trial, mode)].append(record["elapsed_seconds"])

    paired = {}
    for case in adapter.CASES:
        deltas, trial_u, off = [], [], []
        for trial in range(3):
            on_raw = cells[(case, trial, "on")]
            off_raw = cells[(case, trial, "off")]
            off.append(statistics.median(off_raw) * 1000)
            adjacent = [(on_raw[i] - off_raw[i]) * 1000 for i in range(SAMPLES)]
            _require(math.isfinite(off[-1]) and all(math.isfinite(x) for x in adjacent),
                     "timing_summary_arithmetic")
            trial_d = statistics.median(adjacent)
            trial_mad = statistics.median(abs(x - trial_d) for x in adjacent)
            deltas.append(trial_d)
            trial_u.append(trial_d + 2 * 1.4826 * trial_mad / math.sqrt(SAMPLES))
        d = statistics.median(deltas)
        mad = statistics.median(abs(x - d) for x in deltas)
        bound = .05 * statistics.median(off)
        u = max(d + 2 * 1.4826 * mad, *trial_u)
        _require(all(math.isfinite(value) for value in
                     (*deltas, *trial_u, d, mad, u, bound)), "timing_summary_arithmetic")
        paired[case] = {"delta_ms": deltas, "trial_U_ms": trial_u,
                        "D_ms": d, "MAD_ms": mad, "U_ms": u,
                        "bound_ms": bound, "small_overhead": max(0, u) <= bound}
    return paired


def validate_timing_summary(records, paired):
    """Reject a published summary that differs from public timing arithmetic."""
    expected = summarize_timing_records(records)
    _require(type(paired) is dict and tuple(paired) == adapter.CASES,
             "timing_summary_cases")
    for case in adapter.CASES:
        actual = paired[case]
        computed = expected[case]
        _require(type(actual) is dict and tuple(actual) == tuple(computed),
                 "timing_summary_fields")
        for name, value in computed.items():
            candidate = actual[name]
            if type(value) is list:
                _require(type(candidate) is list and len(candidate) == 3 and
                         all(type(item) in (int, float) and math.isfinite(item)
                             for item in candidate) and candidate == value,
                         "timing_summary_arithmetic")
            elif type(value) is bool:
                _require(type(candidate) is bool and candidate == value,
                         "timing_summary_arithmetic")
            else:
                _require(type(candidate) in (int, float) and math.isfinite(candidate)
                         and candidate == value, "timing_summary_arithmetic")
    return expected
