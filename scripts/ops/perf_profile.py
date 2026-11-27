"""Validate a private PERF-90 pilot profile against caller-trusted identities.

This offline check does not authenticate the receipt or admit a full job. The
caller must verify its producer, artifact metadata, current host and budget.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re

from .throughput_budget import Stage


SCHEMA = "alopex.perf-throughput-profile/v1"
MAX_PROFILE_BYTES = 65_536
MAX_STAGE_MS = 5_400_000
STOP_BYTES = 40_000_000_000
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
NAME = re.compile(r"[a-z][a-z0-9_]{0,79}\Z")
PERF_TESTS = ("perf_mixed_point_latency",
              "perf_sequential_throughput_and_amplification")
SHA40_FIELDS = frozenset({"source_sha", "tree_sha", "workflow_blob",
                          "controller_blob"})
SHA64_FIELDS = frozenset({"runner_profile_sha256", "baseline_sha256",
                          "toolchain_lock_sha256"})
ID_FIELDS = frozenset({"repository_id", "run_id", "run_attempt",
                       "baseline_artifact_id"})
BINDING_FIELDS = SHA40_FIELDS | SHA64_FIELDS | ID_FIELDS
RECEIPT_FIELDS = frozenset({"profile_sha256", "artifact_id",
                            "artifact_digest", "repository_id", "run_id",
                            "run_attempt", "source_sha"})
PROFILE_FIELDS = frozenset({"schema", "binding", "stages", "reports",
                            "pilot_baseline_alopex_bytes",
                            "pilot_peak_alopex_bytes", "pilot_min_free_bytes"})


@dataclass(frozen=True)
class StageContract:
    name: str
    total_units: int
    group_size: int = 1
    checkpoint_size: int | None = None


@dataclass(frozen=True)
class ProfileObservation:
    stages: tuple[Stage, ...]
    pilot_growth_bytes: int
    pilot_min_free_bytes: int
    report_hashes: tuple[tuple[str, str], ...]


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("PROFILE")


def _positive_int(value: object) -> bool:
    return type(value) is int and value > 0


def _nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        _require(key not in result)
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("PROFILE")


def _binding(value: object) -> dict[str, object]:
    _require(type(value) is dict and set(value) == BINDING_FIELDS)
    for key in SHA40_FIELDS:
        _require(type(value[key]) is str and HEX40.fullmatch(value[key]) is not None)
    for key in SHA64_FIELDS:
        _require(type(value[key]) is str and HEX64.fullmatch(value[key]) is not None)
    for key in ID_FIELDS:
        _require(_positive_int(value[key]))
    return value


def _manifest(contracts: object) -> tuple[StageContract, ...]:
    _require(type(contracts) is tuple and 0 < len(contracts) <= 32)
    names: list[str] = []
    for contract in contracts:
        _require(type(contract) is StageContract)
        size = (contract.group_size if contract.checkpoint_size is None
                else contract.checkpoint_size)
        _require(type(contract.name) is str and NAME.fullmatch(contract.name)
                 is not None and contract.name not in names
                 and _positive_int(contract.total_units)
                 and _positive_int(contract.group_size)
                 and contract.total_units % contract.group_size == 0
                 and _positive_int(size) and contract.group_size % size == 0)
        names.append(contract.name)
    _require(all(name in names for name in PERF_TESTS)
             and names.index(PERF_TESTS[0]) < names.index(PERF_TESTS[1]))
    for contract in contracts:
        if contract.name in PERF_TESTS:
            _require(contract.total_units == contract.group_size == 1)
    return contracts


def validate_profile(profile_bytes: bytes, *, expected_binding: dict[str, object],
                     trusted_receipt: dict[str, object],
                     manifest: tuple[StageContract, ...]) -> ProfileObservation:
    """Return pilot observations; the caller still owns trust and admission."""
    _require(type(profile_bytes) is bytes
             and 0 < len(profile_bytes) <= MAX_PROFILE_BYTES)
    expected = _binding(expected_binding)
    _require(type(trusted_receipt) is dict
             and set(trusted_receipt) == RECEIPT_FIELDS)
    receipt = trusted_receipt
    _require(type(receipt["profile_sha256"]) is str
             and HEX64.fullmatch(receipt["profile_sha256"]) is not None
             and type(receipt["artifact_digest"]) is str
             and receipt["artifact_digest"].startswith("sha256:")
             and HEX64.fullmatch(receipt["artifact_digest"][7:]) is not None
             and _positive_int(receipt["artifact_id"]))
    for key in ("repository_id", "run_id", "run_attempt", "source_sha"):
        _require(receipt[key] == expected[key]
                 and type(receipt[key]) is type(expected[key]))
    _require(hashlib.sha256(profile_bytes).hexdigest()
             == receipt["profile_sha256"])
    contracts = _manifest(manifest)
    try:
        value = json.loads(profile_bytes.decode("utf-8"),
                           object_pairs_hook=_pairs,
                           parse_constant=_reject_constant)
        canonical = (json.dumps(value, sort_keys=True, separators=(",", ":"),
                                ensure_ascii=False, allow_nan=False)
                     .encode("utf-8") + b"\n")
    except (UnicodeError, json.JSONDecodeError, TypeError, ValueError,
            RecursionError):
        raise ValueError("PROFILE") from None
    _require(canonical == profile_bytes and type(value) is dict
             and set(value) == PROFILE_FIELDS and value["schema"] == SCHEMA
             and _binding(value["binding"]) == expected)
    baseline = value["pilot_baseline_alopex_bytes"]
    peak = value["pilot_peak_alopex_bytes"]
    free = value["pilot_min_free_bytes"]
    _require(_nonnegative_int(baseline) and _nonnegative_int(peak)
             and baseline <= peak < STOP_BYTES and _positive_int(free))
    reports = value["reports"]
    _require(type(reports) is dict and set(reports) == set(PERF_TESTS))
    for report_hash in reports.values():
        _require(type(report_hash) is str
                 and HEX64.fullmatch(report_hash) is not None)
    records = value["stages"]
    _require(type(records) is list and len(records) == len(contracts))
    stages: list[Stage] = []
    for record, contract in zip(records, contracts, strict=True):
        _require(type(record) is dict
                 and set(record) == {"name", "observed_units", "observed_ms"}
                 and record["name"] == contract.name
                 and type(record["name"]) is str
                 and _positive_int(record["observed_units"])
                 and record["observed_units"] <= contract.total_units
                 and record["observed_units"] % contract.group_size == 0
                 and _positive_int(record["observed_ms"])
                 and record["observed_ms"] <= MAX_STAGE_MS)
        stages.append(Stage(contract.name, contract.total_units, 0,
                            record["observed_units"],
                            record["observed_ms"] / 1000,
                            contract.group_size, contract.checkpoint_size))
    return ProfileObservation(tuple(stages), peak - baseline, free,
                              tuple((name, reports[name]) for name in PERF_TESTS))
