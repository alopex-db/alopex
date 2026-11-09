"""Private, manual-only /proc/self/smaps parser for WAL open/begin diagnosis.

This module does not run the product workload or print mapping paths. The caller
must capture boundaries and manage any private evidence and cleanup separately.
PSS/RSS are boundary observations, not allocation attribution or interval peaks.
"""

from pathlib import Path
import base64
import json
import os
import re
import zlib


_HEADER = re.compile(
    r"^([0-9a-fA-F]+)-([0-9a-fA-F]+)\s+([rwxps-]{4})\s+"
    r"([0-9a-fA-F]+)\s+([0-9a-fA-F]+:[0-9a-fA-F]+)\s+"
    r"(\d+)(?:\s+(.*))?$"
)
_FIELD = re.compile(r"^([A-Za-z_][A-Za-z_0-9]*):\s*(.*)$")
_KB = re.compile(r"^(\d+) kB$")
_REQUIRED = {"Size", "Rss", "Pss", "VmFlags"}
_SELF_SMAPS = Path("/proc/self/smaps")
MAX_RAW_BYTES = 2_000_000
MAX_COMPRESSED_BYTES = 128_000


def _identity(pathname):
    # A valid anonymous header may end in whitespace. The optional pathname
    # capture then yields either None or an empty/whitespace-only string.
    if pathname is None or not pathname.strip():
        return "anonymous", None
    if pathname.startswith("[") and pathname.endswith("]"):
        return "special", pathname
    if pathname.startswith("/"):
        return "file", pathname
    # Linux can report other named mappings (for example SYSV segments).
    # Keep their identity without treating them as file-backed or anonymous.
    return "named", pathname


def parse_smaps(raw):
    """Return mapping and pathname totals in bytes; reject incomplete input.

    The caller owns `raw`. Returned pathnames are private host data and must not
    be logged or published without sanitization.
    """
    if not isinstance(raw, str) or not raw:
        raise ValueError("empty or non-text smaps input")

    mappings = []
    current = None

    def finish():
        if current is None:
            return
        if current["seen"] != _REQUIRED:
            raise ValueError("smaps mapping lacks required fields")
        if current["bytes"]["Pss"] > current["bytes"]["Rss"]:
            raise ValueError("smaps Pss exceeds Rss")
        del current["seen"]
        mappings.append(current)

    for line in raw.splitlines():
        if not line:
            raise ValueError("blank smaps line")
        header = _HEADER.fullmatch(line)
        if header:
            finish()
            start, end = int(header[1], 16), int(header[2], 16)
            if start >= end:
                raise ValueError("invalid smaps address range")
            category, pathname = _identity(header[7])
            current = {
                "start": start,
                "end": end,
                "category": category,
                "pathname": pathname,
                "bytes": {},
                "seen": set(),
            }
            continue
        if current is None:
            raise ValueError("smaps field before mapping header")
        field = _FIELD.fullmatch(line)
        if field is None:
            raise ValueError("unrecognized smaps line")
        name, value = field.groups()
        if name in _REQUIRED:
            if name in current["seen"]:
                raise ValueError("duplicate required smaps field")
            current["seen"].add(name)
        if name in ("Size", "Rss", "Pss"):
            amount = _KB.fullmatch(value)
            if amount is None:
                raise ValueError("invalid required smaps metric")
            if name != "Size":
                current["bytes"][name] = int(amount[1]) * 1024
        elif name == "VmFlags" and not value:
            raise ValueError("empty smaps VmFlags")

    finish()
    if not mappings:
        raise ValueError("smaps contains no mappings")

    grouped = {}
    for mapping in mappings:
        key = (mapping["category"], mapping["pathname"])
        total = grouped.setdefault(key, {"Rss": 0, "Pss": 0, "count": 0})
        for name in ("Rss", "Pss"):
            total[name] += mapping["bytes"][name]
        total["count"] += 1

    groups = [
        {"category": category, "pathname": pathname, "bytes": total}
        for (category, pathname), total in sorted(
            grouped.items(), key=lambda item: (item[0][0], item[0][1] or "")
        )
    ]
    return {
        "schema": "alopex.wal-mapping-boundary/v1",
        "mappings": mappings,
        "groups": groups,
        "totals": {
            name: sum(mapping["bytes"][name] for mapping in mappings)
            for name in ("Rss", "Pss")
        },
        "scope": "self-process boundary only; no allocation or interval-peak attribution",
    }


def read_self_smaps():
    """Read this process only; missing procfs or permission errors propagate."""
    raw = _SELF_SMAPS.read_text(encoding="ascii")
    if len(raw.encode("ascii")) > MAX_RAW_BYTES:
        raise ValueError("smaps exceeds capture bound")
    snapshot = parse_smaps(raw)
    snapshot["raw_smaps"] = raw
    return snapshot


BOUNDARIES = (
    "calibration_before", "calibration_after", "before_open", "after_open",
    "before_begin", "after_begin",
)


class BoundaryObserver:
    """Capture only the requested writer boundaries, preserving calibration raw."""

    def __init__(self, read=read_self_smaps):
        self._read = read
        self._pid = os.getpid()
        self._rows = []

    def capture(self, phase):
        if len(self._rows) >= len(BOUNDARIES) or phase != BOUNDARIES[len(self._rows)]:
            raise ValueError("mapping boundary order differs")
        if os.getpid() != self._pid:
            raise ValueError("mapping process identity changed")
        snapshot = self._read()
        if snapshot.get("schema") != "alopex.wal-mapping-boundary/v1":
            raise ValueError("mapping snapshot schema differs")
        raw = snapshot.get("raw_smaps")
        if not isinstance(raw, str) or len(raw.encode("ascii")) > MAX_RAW_BYTES:
            raise ValueError("mapping raw evidence unavailable or oversized")
        if parse_smaps(raw)["groups"] != snapshot["groups"]:
            raise ValueError("mapping groups differ from raw evidence")
        compressed = zlib.compress(raw.encode("ascii"))
        if len(compressed) > MAX_COMPRESSED_BYTES:
            raise ValueError("compressed smaps exceeds evidence bound")
        self._rows.append({
            "phase": phase,
            "groups": snapshot["groups"],
            "totals": snapshot["totals"],
            "raw_smaps_zlib_base64": base64.b64encode(compressed).decode("ascii"),
        })

    def report(self):
        if tuple(row["phase"] for row in self._rows) != BOUNDARIES:
            raise ValueError("mapping boundaries incomplete")
        rows = self._rows
        calibration = {
            key: rows[1]["totals"][key] - rows[0]["totals"][key]
            for key in ("Rss", "Pss")
        }
        return {
            "schema": "alopex.wal-mapping-observation/v1",
            "rows": rows,
            "calibration_delta_bytes": calibration,
            "performance_acceptance": False,
            "scope": "same writer process; self-observation overhead retained, not subtracted",
        }

    def save(self, path):
        report = self.report()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        fd = os.open(path, flags, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(report, stream, allow_nan=False)
            stream.write("\n")
        return report


def validate_report(report):
    if not isinstance(report, dict) or report.get("schema") != "alopex.wal-mapping-observation/v1":
        raise ValueError("mapping report schema differs")
    rows = report.get("rows")
    if not isinstance(rows, list) or tuple(row.get("phase") for row in rows) != BOUNDARIES:
        raise ValueError("mapping report boundaries differ")
    for row in rows:
        if not isinstance(row.get("groups"), list) or not isinstance(row.get("totals"), dict):
            raise ValueError("mapping report row differs")
        compressed = base64.b64decode(row["raw_smaps_zlib_base64"], validate=True)
        if len(compressed) > MAX_COMPRESSED_BYTES:
            raise ValueError("compressed smaps exceeds evidence bound")
        decompressor = zlib.decompressobj()
        raw = decompressor.decompress(compressed, MAX_RAW_BYTES + 1)
        if len(raw) > MAX_RAW_BYTES or not decompressor.eof or decompressor.unused_data:
            raise ValueError("mapping raw evidence incomplete or oversized")
        expected_snapshot = parse_smaps(raw.decode("ascii"))
        if row["groups"] != expected_snapshot["groups"] or row["totals"] != expected_snapshot["totals"]:
            raise ValueError("mapping report differs from raw evidence")
        for name in ("Rss", "Pss"):
            total = row["totals"].get(name)
            if type(total) is not int or total < 0:
                raise ValueError("mapping total differs")
            if sum(group["bytes"][name] for group in row["groups"]) != total:
                raise ValueError("mapping groups do not sum to total")
    expected = {name: rows[1]["totals"][name] - rows[0]["totals"][name]
                for name in ("Rss", "Pss")}
    if report.get("calibration_delta_bytes") != expected or report.get("performance_acceptance") is not False:
        raise ValueError("mapping calibration or scope differs")
    return report
