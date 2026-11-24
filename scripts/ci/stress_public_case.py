"""Assemble only public-by-construction Stage-2 measurements.

The Rust harness and guarded command write typed public records at measurement
time. This intake validates those records and refuses every other file; it does
not redact arbitrary diagnostics after the fact.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys


WORKSPACE = Path(__file__).resolve().parents[2]
CASES = frozenset({"stress", "asan", "tsan", "lsan", "msan",
                   "sql_parser", "dataframe_conversion", "perf"})
FUZZ_CASES = frozenset({"sql_parser", "dataframe_conversion"})
SANITIZER_CASES = frozenset({"asan", "tsan", "lsan", "msan"})
SHA40 = re.compile(r"[0-9a-f]{40}\Z")
NAME = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,79}\Z")
STAMP = re.compile(r"([0-9]{8}T[0-9]{6}Z)-([0-9]{4})\Z")
MAX_FILE_BYTES = 1_000_000
MAX_CASE_BYTES = 20_000_000
MAX_RUNS = 1_000
LANES = frozenset({"smoke", "ci", "nightly", "weekly", "soak", "perf", "fuzz", "sanitizer"})
MODELS = frozenset({"SyncSingle", "SyncMulti", "AsyncSingle", "AsyncMulti"})
CHECK_CODES = frozenset({"passed", "skipped", "test_error", "operation_timeout",
                         "scenario_timeout", "slo_failed"})
WATCHDOG_CODES = frozenset({"success", "operation_timeout", "scenario_timeout"})
PERF_FILES = frozenset({"perf_mixed_point_latency.json",
                        "perf_sequential_throughput_and_amplification.json"})
PERF_METRICS = {
    "perf_mixed_point_latency.json": frozenset({"throughput_ops_sec", "p50_latency_ns",
                                                 "p95_latency_ns", "p99_latency_ns"}),
    "perf_sequential_throughput_and_amplification.json": frozenset({
        "throughput_ops_sec", "write_amplification", "space_amplification"}),
}
BASELINE = {"run_id": 44059890460, "artifact_id": 14317682831,
            "source_commit": "a5fb412bb826689bb685c98d73213b5153bf1e22",
            "zip_sha256": "af73bac907d8bcdeb3f2acd1722e4baf0c19c946bcfe370599cc2770f943aadf"}
FAILURE_CODES = {"stress": "TEST", "asan": "SANITIZER",
                 "tsan": "SANITIZER", "lsan": "SANITIZER",
                 "msan": "SANITIZER", "sql_parser": "FUZZ",
                 "dataframe_conversion": "FUZZ", "perf": "PERF"}


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("EVIDENCE")
        value[key] = item
    return value


def read_json(path: Path, *, limit: int = MAX_FILE_BYTES,
              canonical: bool = False) -> dict:
    if not hasattr(os, "O_NOFOLLOW"):
        raise ValueError("EVIDENCE")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb", buffering=0) as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or not 0 < before.st_size <= limit):
            raise ValueError("EVIDENCE")
        body = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        if (len(body) != before.st_size or after.st_size != before.st_size
                or after.st_mtime_ns != before.st_mtime_ns):
            raise ValueError("EVIDENCE")
    value = json.loads(body, object_pairs_hook=_unique_object)
    if not isinstance(value, dict):
        raise ValueError("EVIDENCE")
    if canonical and body != (json.dumps(value, sort_keys=True, allow_nan=False,
                                         separators=(",", ":")).encode("utf-8") + b"\n"):
        raise ValueError("EVIDENCE")
    return value


def files_beneath(root: Path) -> dict[str, Path]:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("EVIDENCE")
    files: dict[str, Path] = {}
    total = 0
    pending = [(root, ())]
    directories = 0
    while pending:
        parent, parts = pending.pop()
        directories += 1
        if directories > 4 * MAX_RUNS + 4:
            raise ValueError("EVIDENCE")
        if len(parts) > 5:
            raise ValueError("EVIDENCE")
        with os.scandir(parent) as entries:
            for entry in entries:
                path = Path(entry.path)
                info = entry.stat(follow_symlinks=False)
                if entry.is_symlink():
                    raise ValueError("EVIDENCE")
                next_parts = (*parts, entry.name)
                if stat.S_ISDIR(info.st_mode):
                    pending.append((path, next_parts))
                elif stat.S_ISREG(info.st_mode):
                    if info.st_nlink != 1 or not 0 < info.st_size <= MAX_FILE_BYTES:
                        raise ValueError("EVIDENCE")
                    relative = "/".join(next_parts)
                    files[relative] = path
                    total += info.st_size
                    if len(files) > 4 * MAX_RUNS + 3 or total > MAX_CASE_BYTES:
                        raise ValueError("EVIDENCE")
                else:
                    raise ValueError("EVIDENCE")
    return files


def execution_record(value: dict, *, case_id: str, source: str,
                     outcome: str) -> dict:
    expected = {"schema", "case_id", "source_commit", "outcome", "duration_ms"}
    if case_id in FUZZ_CASES:
        expected.add("fuzz_executions")
    if (set(value) != expected
            or value["schema"] != "alopex.stress-execution/v1"
            or value["case_id"] != case_id
            or value["source_commit"] != source
            or value["outcome"] != outcome
            or type(value["duration_ms"]) is not int
            or not 0 <= value["duration_ms"] <= 172_800_000):
        raise ValueError("EVIDENCE")
    if case_id in FUZZ_CASES and (
            value["fuzz_executions"] is not None and not integer(value["fuzz_executions"])
            or outcome == "passed" and (not integer(value["fuzz_executions"])
                                        or value["fuzz_executions"] == 0)):
        raise ValueError("EVIDENCE")
    return value


def finite_number(value: object, *, nonnegative: bool = True) -> bool:
    return (type(value) in (int, float) and -1e300 <= value <= 1e300
            and math.isfinite(value)
            and (value >= 0 if nonnegative else True))


def public_root(workspace: Path, case_id: str) -> Path:
    if case_id not in CASES:
        raise ValueError("CASE")
    for parent in (workspace / "target", workspace / "target/stress-public-artifacts"):
        if parent.is_symlink():
            raise ValueError("EVIDENCE")
    return workspace / "target/stress-public-artifacts" / case_id


def source_identity(workspace: Path, source: str) -> None:
    if not SHA40.fullmatch(source):
        raise ValueError("SOURCE")
    head = subprocess.check_output(
        ["git", "-C", str(workspace), "rev-parse", "HEAD"],
        text=True, timeout=15).strip()
    if head != source:
        raise ValueError("SOURCE")


def exact(value: object, fields: set[str]) -> dict:
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("EVIDENCE")
    return value


def integer(value: object, maximum: int = 2**64 - 1) -> bool:
    return type(value) is int and 0 <= value <= maximum


def nullable_integer(value: object) -> bool:
    return value is None or integer(value)


def valid_slo(value: object) -> bool:
    if value is None:
        return True
    if not isinstance(value, dict) or set(value) != {
            "p99_max_latency_ns", "p95_max_latency_ns", "p999_max_latency_ns",
            "p9999_max_latency_ns", "min_throughput", "max_error_ratio"}:
        return False
    return (all(nullable_integer(value[key]) for key in (
        "p99_max_latency_ns", "p95_max_latency_ns", "p999_max_latency_ns",
        "p9999_max_latency_ns"))
        and (value["min_throughput"] is None or finite_number(value["min_throughput"]))
        and (value["max_error_ratio"] is None or
             finite_number(value["max_error_ratio"]) and value["max_error_ratio"] <= 1))


def run_record(value: dict, *, lane: str, test_name: str, source: str) -> dict:
    exact(value, {"schema_version", "test_name", "lane", "seed", "binary_version",
                  "execution_model", "concurrency", "scenario_timeout_ms",
                  "operation_timeout_ms", "metrics_interval_ms", "warmup_ops",
                  "slo", "replay", "git_sha"})
    if (type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["test_name"] != test_name or not NAME.fullmatch(test_name)
            or value["lane"] != lane or lane not in LANES
            or not integer(value["seed"])
            or not re.fullmatch(r"alopex-[a-z0-9_-]+@[0-9A-Za-z.+-]{1,40}",
                                value["binary_version"])
            or value["execution_model"] not in MODELS
            or not integer(value["concurrency"], 1_000_000)
            or value["concurrency"] == 0
            or not all(integer(value[key], 172_800_000) for key in (
                "scenario_timeout_ms", "operation_timeout_ms", "metrics_interval_ms"))
            or not integer(value["warmup_ops"], 1_000_000_000)
            or not valid_slo(value["slo"])
            or type(value["replay"]) is not bool
            or value["git_sha"] not in (None, source)):
        raise ValueError("EVIDENCE")
    return value


def metrics_record(value: dict) -> dict:
    exact(value, {"schema_version", "duration_ms", "successes", "errors",
                  "sample_count", "throughput_per_sec", "p50_ns", "p95_ns",
                  "p99_ns", "p999_ns", "p9999_ns", "outlier_count",
                  "outlier_ratio"})
    if (type(value["schema_version"]) is not int or value["schema_version"] != 1
            or not all(integer(value[key]) for key in (
                "duration_ms", "successes", "errors", "sample_count",
                "outlier_count"))
            or not finite_number(value["throughput_per_sec"])
            or not all(nullable_integer(value[key]) for key in (
                "p50_ns", "p95_ns", "p99_ns", "p999_ns", "p9999_ns"))
            or not finite_number(value["outlier_ratio"])
            or value["outlier_ratio"] > 1
            or value["outlier_count"] > value["sample_count"]):
        raise ValueError("EVIDENCE")
    return value


def check_record(value: dict) -> dict:
    exact(value, {"schema_version", "skipped", "success", "code", "watchdog",
                  "slo_passed", "duration_ms"})
    if (type(value["schema_version"]) is not int or value["schema_version"] != 1
            or any(type(value[key]) is not bool for key in (
                "skipped", "success", "slo_passed"))
            or value["code"] not in CHECK_CODES
            or value["watchdog"] not in WATCHDOG_CODES
            or not integer(value["duration_ms"], 172_800_000)
            or (value["code"] == "passed") != value["success"]
            or (value["code"] == "skipped") != value["skipped"]):
        raise ValueError("EVIDENCE")
    return value


def perf_record(value: dict, filename: str) -> dict:
    exact(value, {"schema_version", "metrics", "comparisons", "baseline_present",
                  "passed"})
    expected = PERF_METRICS[filename]
    metrics = exact(value["metrics"], set(expected))
    comparisons = exact(value["comparisons"],
                        set(expected) if value["baseline_present"] else set())
    if (type(value["schema_version"]) is not int or value["schema_version"] != 1
            or type(value["baseline_present"]) is not bool
            or type(value["passed"]) is not bool
            or not all(finite_number(item) for item in metrics.values())):
        raise ValueError("EVIDENCE")
    for key in comparisons:
        comparison = exact(comparisons[key], {"current", "baseline", "passed"})
        if (not finite_number(comparison["current"])
                or not finite_number(comparison["baseline"])
                or comparison["current"] != metrics[key]
                or type(comparison["passed"]) is not bool):
            raise ValueError("EVIDENCE")
    if (value["passed"] != (value["baseline_present"] and all(
            comparison["passed"] for comparison in comparisons.values()))):
        raise ValueError("EVIDENCE")
    return value


def assemble(*, workspace: Path, case_id: str, source: str, outcome: str) -> dict:
    if case_id not in CASES or outcome not in {"passed", "failed"}:
        raise ValueError("ARGUMENT")
    root = public_root(workspace, case_id)
    if root.exists() or root.is_symlink():
        files = files_beneath(root)
    elif outcome == "failed":
        files = {}
    else:
        raise ValueError("EVIDENCE")
    execution_path = files.pop("execution.json", None)
    execution = (execution_record(read_json(execution_path), case_id=case_id,
                                  source=source, outcome=outcome)
                 if execution_path else None)
    if outcome == "passed" and execution is None:
        raise ValueError("EVIDENCE")
    groups: dict[str, dict[str, Path]] = {}
    for relative, path in files.items():
        parts = relative.split("/")
        if (len(parts) not in {4, 5} or parts[0] not in LANES
                or not NAME.fullmatch(parts[1]) or not STAMP.fullmatch(parts[2])):
            raise ValueError("EVIDENCE")
        name = "/".join(parts[3:])
        if name not in {"run.json", "metrics.json", "checks/summary.json"} | {
                "checks/" + filename for filename in PERF_FILES}:
            raise ValueError("EVIDENCE")
        groups.setdefault("/".join(parts[:3]), {})[name] = path
    if len(groups) > MAX_RUNS:
        raise ValueError("EVIDENCE")
    runs = []
    for run_path, paths in sorted(groups.items()):
        lane, test_name, stamp = run_path.split("/")
        required = {"run.json", "checks/summary.json"}
        if not required <= paths.keys():
            raise ValueError("EVIDENCE")
        run = run_record(read_json(paths["run.json"]), lane=lane,
                         test_name=test_name, source=source)
        check = check_record(read_json(paths["checks/summary.json"]))
        metrics = (metrics_record(read_json(paths["metrics.json"]))
                   if "metrics.json" in paths else None)
        perf = {name: perf_record(read_json(path), name.removeprefix("checks/"))
                for name, path in paths.items() if name.startswith("checks/perf_")}
        if (check["success"] and metrics is None
                or case_id != "perf" and perf
                or case_id == "perf" and check["success"] and not perf):
            raise ValueError("EVIDENCE")
        stamp_match = STAMP.fullmatch(stamp)
        if stamp_match is None:
            raise ValueError("EVIDENCE")
        runs.append({"started_at_utc": stamp_match.group(1),
                     "run_index": int(stamp_match.group(2)),
                     "run": run, "metrics": metrics, "check": check,
                     "performance": perf})
    if (outcome == "passed" and case_id in {"stress", "perf"}
            and not any(item["check"]["success"] for item in runs)
            or outcome == "passed" and any(
                not item["check"]["success"] and not item["check"]["skipped"]
                or (case_id == "perf" and not all(
                    value["passed"] for value in item["performance"].values()))
                for item in runs)):
        raise ValueError("EVIDENCE")
    if outcome == "passed" and case_id == "perf":
        expected = {"perf_mixed_point_latency",
                    "perf_sequential_throughput_and_amplification"}
        if (len(runs) != len(expected)
                or {item["run"]["test_name"] for item in runs} != expected
                or any(item["run"]["lane"] != "perf"
                       or not item["check"]["success"]
                       or set(item["performance"]) != {
                           "checks/" + item["run"]["test_name"] + ".json"}
                       or not next(iter(item["performance"].values()))["passed"]
                       for item in runs)):
            raise ValueError("EVIDENCE")
    return {"schema": "alopex.stress-public-case/v1", "case_id": case_id,
            "source_commit": source, "outcome": outcome,
            "error_code": None if outcome == "passed" else FAILURE_CODES[case_id],
            "execution": execution, "runs": runs,
            "baseline": BASELINE if case_id == "perf" else None}


def write_result(*, workspace: Path, case_id: str, result: dict) -> Path:
    parent = workspace / "target/stress-publish"
    case_dir = parent / case_id
    for directory in (workspace / "target", parent, case_dir):
        if directory.is_symlink():
            raise ValueError("OUTPUT")
        directory.mkdir(mode=0o700, exist_ok=True)
        if directory.stat().st_uid != os.getuid():
            raise ValueError("OUTPUT")
    output = case_dir / f"{case_id}.json"
    body = json.dumps(result, sort_keys=True, allow_nan=False,
                      separators=(",", ":")).encode("utf-8") + b"\n"
    if not 0 < len(body) <= MAX_CASE_BYTES:
        raise ValueError("OUTPUT")
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(body)
    return output


def main() -> None:
    os.umask(0o077)
    args = sys.argv[1:]
    if (len(args) != 4 or args[0] != "--case" or args[2] != "--outcome"
            or args[1] not in CASES or args[3] not in {"passed", "failed"}):
        raise ValueError("ARGUMENT")
    case_id, outcome = args[1], args[3]
    source = os.environ.get("GITHUB_SHA", "")
    if (os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
            or os.environ.get("GITHUB_WORKSPACE") != str(WORKSPACE)
            or os.environ.get("STRESS_EVIDENCE_MODE") != "public"):
        raise ValueError("STATE")
    source_identity(WORKSPACE, source)
    result = assemble(workspace=WORKSPACE, case_id=case_id, source=source,
                      outcome=outcome)
    write_result(workspace=WORKSPACE, case_id=case_id, result=result)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, UnicodeDecodeError,
            json.JSONDecodeError,
            subprocess.CalledProcessError, subprocess.TimeoutExpired):
        raise SystemExit("STRESS_PUBLIC_CASE_REJECTED") from None
