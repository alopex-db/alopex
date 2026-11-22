"""Validate public-purpose #681 bytes before an external publication choice."""

from __future__ import annotations

import hashlib
import json
import re

import issue681_hosted_acquire_source as source
import issue681_public as case
import issue681_public_pair as pair

ERROR = "PUBLIC_TRANSPORT"
MAX_BYTES = 500_000


def require(value):
    if not value:
        raise ValueError(ERROR)


def _fixed_sources(pins):
    pair._pins(pins)
    for arm in pair.ARMS:
        pin = pins[arm]
        expected = source.PINS[arm]
        require(pin["source_commit"] == expected["commit"] and
                pin["source_tree"] == expected["tree"] and
                pin["example_sha256"] == expected["example"] and
                pin["cargo_lock_sha256"] == expected["lock"] and
                pin["rust_version"] == "1.90.0")


def validate(raw: bytes, *, expected_sha256: str, expected_pins: dict,
             finish: str) -> bytes:
    """Return canonical public bytes only when every field matches the contract."""
    try:
        require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES and
                type(expected_sha256) is str and
                re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is not None and
                hashlib.sha256(raw).hexdigest() == expected_sha256 and
                finish in ("commit", "rollback"))
        _fixed_sources(expected_pins)
        value = json.loads(raw, object_pairs_hook=pair._unique)
        require(type(value) is dict and set(value) == pair.PAIR_FIELDS and
                value["pins"] == expected_pins and value["finish"] == finish and
                type(value["cases"]) is list and len(value["cases"]) == 30)
        reports = []
        for row, (_, _, arm) in zip(value["cases"], pair._order()):
            require(type(row) is dict and set(row) == {"arm", "report"} and
                    row["arm"] == arm)
            reports.append(row["report"])
        expected = pair.assemble(reports, expected_pins, finish)
        require(value == expected and raw == case._canonical(expected))
        return raw
    except Exception:
        raise ValueError(ERROR) from None
