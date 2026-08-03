#!/usr/bin/env python3
"""Normalize, compare, and retain advisory alopex-py performance metrics."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import re
import statistics
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = 1
DEFAULT_MIN_SAMPLES = 5
DEFAULT_ROBUST_Z_THRESHOLD = 3.5
DEFAULT_MAX_RECORDS_PER_PROFILE = 50
RELEASE_TAG_PATTERN = re.compile(r"^(?:alopex-py-)?v[0-9][0-9A-Za-z._-]*$")
SUBJECT_DISTRIBUTIONS = {"alopex"}

METRIC_DEFINITIONS = {
    "scan_overhead_ms": {"unit": "ms", "direction": "lower_is_better"},
    "read_overhead_pct": {"unit": "%", "direction": "lower_is_better"},
    "write_overhead_pct": {"unit": "%", "direction": "lower_is_better"},
}


class HarnessError(ValueError):
    """Raised when an input artifact cannot be compared safely."""


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise HarnessError(f"{name} must be numeric, got bool")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise HarnessError(f"{name} must be numeric, got {value!r}") from exc
    if not math.isfinite(number):
        raise HarnessError(f"{name} must be finite, got {value!r}")
    return number


def _sha256_json(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _environment_from_payload(
    payload: Mapping[str, Any],
    *,
    profile: str,
    dependency_versions: Mapping[str, str],
    runtime_environment: Mapping[str, str] | None,
) -> dict[str, str]:
    machine = payload.get("machine_info") or {}
    cpu = machine.get("cpu") or {}
    environment = {
        "profile": profile,
        "system": str(machine.get("system") or "unknown"),
        "machine": str(machine.get("machine") or "unknown"),
        "cpu": str(cpu.get("brand_raw") or machine.get("processor") or "unknown"),
        "kernel": str(machine.get("release") or "unknown"),
        "python": str(machine.get("python_version") or sys.version.split()[0]),
        "cpu_governor": "unknown",
        "turbo_policy": "unknown",
    }
    if runtime_environment:
        for key in ("cpu_governor", "turbo_policy", "kernel"):
            if key in runtime_environment:
                environment[key] = str(runtime_environment[key])

    fingerprint_input = {
        **environment,
        "dependencies": {
            name: version
            for name, version in sorted(dependency_versions.items())
            if name not in SUBJECT_DISTRIBUTIONS
        },
    }
    environment["fingerprint"] = _sha256_json(fingerprint_input)[:16]
    return environment


def build_record(
    benchmark_payload: Mapping[str, Any],
    *,
    commit: str,
    ref: str,
    profile: str,
    measured_at: str,
    dependency_versions: Mapping[str, str] | None = None,
    lock_hash: str = "unknown",
    workload_signature: str = "unknown",
    runtime_environment: Mapping[str, str] | None = None,
    junit_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one canonical record from matching benchmark JSON and xUnit data."""
    if not commit:
        raise HarnessError("commit must not be empty")
    if not profile:
        raise HarnessError("profile must not be empty")

    extracted: dict[str, float] = {}
    for benchmark in benchmark_payload.get("benchmarks") or []:
        extra_info = benchmark.get("extra_info") or {}
        for name in METRIC_DEFINITIONS:
            if name in extra_info:
                if name in extracted:
                    raise HarnessError(f"duplicate metric {name}")
                extracted[name] = _finite_float(extra_info[name], name)

    missing = sorted(set(METRIC_DEFINITIONS) - set(extracted))
    if missing:
        raise HarnessError(f"missing monitored metrics: {', '.join(missing)}")

    if junit_metrics is not None:
        junit_missing = sorted(set(METRIC_DEFINITIONS) - set(junit_metrics))
        if junit_missing:
            raise HarnessError(
                f"missing JUnit metrics: {', '.join(junit_missing)}"
            )
        for name, benchmark_value in extracted.items():
            junit_value = _finite_float(junit_metrics[name], name)
            if not math.isclose(
                benchmark_value,
                junit_value,
                rel_tol=1e-9,
                abs_tol=5e-7,
            ):
                raise HarnessError(
                    f"metric mismatch for {name}: benchmark={benchmark_value!r}, "
                    f"JUnit={junit_value!r}"
                )

    dependencies = dict(sorted((dependency_versions or {}).items()))
    environment = _environment_from_payload(
        benchmark_payload,
        profile=profile,
        dependency_versions=dependencies,
        runtime_environment=runtime_environment,
    )
    metrics = {
        name: {"value": extracted[name], **definition}
        for name, definition in METRIC_DEFINITIONS.items()
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "commit": commit,
        "ref": ref,
        "measured_at": measured_at,
        "profile": profile,
        "environment": environment,
        "dependencies": dependencies,
        "lock_hash": lock_hash,
        "workload_signature": workload_signature,
        "metrics": metrics,
    }


def _empty_history() -> dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "records": []}


def _validate_history(history: Mapping[str, Any]) -> list[dict[str, Any]]:
    if history.get("schema_version", SCHEMA_VERSION) != SCHEMA_VERSION:
        raise HarnessError("unsupported history schema_version")
    records = history.get("records", [])
    if not isinstance(records, list):
        raise HarnessError("history records must be a list")
    return [dict(record) for record in records]


def _series_key(record: Mapping[str, Any]) -> tuple[str, str]:
    environment = record.get("environment") or {}
    return (
        str(environment.get("fingerprint") or ""),
        str(record.get("workload_signature") or ""),
    )


def evaluate(
    current: Mapping[str, Any],
    history: Mapping[str, Any],
    *,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    robust_z_threshold: float = DEFAULT_ROBUST_Z_THRESHOLD,
    _check_consecutive: bool = True,
) -> dict[str, Any]:
    """Compare current values with their matching historical distribution."""
    if min_samples < 1:
        raise HarnessError("min_samples must be at least 1")
    if robust_z_threshold <= 0:
        raise HarnessError("robust_z_threshold must be positive")

    current_key = _series_key(current)
    current_commit = str(current.get("commit") or "")
    matching = [
        record
        for record in _validate_history(history)
        if _series_key(record) == current_key
        and str(record.get("commit") or "") != current_commit
    ]

    results: dict[str, Any] = {}
    has_regressions = False
    has_insufficient = False
    for name, definition in METRIC_DEFINITIONS.items():
        current_metric = (current.get("metrics") or {}).get(name) or {}
        current_value = _finite_float(current_metric.get("value"), name)
        samples = []
        for record in matching:
            metric = (record.get("metrics") or {}).get(name) or {}
            if "value" in metric:
                samples.append(_finite_float(metric["value"], name))

        result: dict[str, Any] = {
            "current": current_value,
            "unit": definition["unit"],
            "direction": definition["direction"],
            "sample_count": len(samples),
            "minimum_samples": min_samples,
            "robust_z_threshold": robust_z_threshold,
        }
        if len(samples) < min_samples:
            result["status"] = "insufficient_history"
            has_insufficient = True
            results[name] = result
            continue

        baseline_median = statistics.median(samples)
        deviations = [abs(value - baseline_median) for value in samples]
        mad = statistics.median(deviations)
        result.update(
            {
                "baseline_median": baseline_median,
                "baseline_mad": mad,
                "baseline_min": min(samples),
                "baseline_max": max(samples),
            }
        )

        is_worse = current_value > baseline_median
        if mad > 0:
            robust_z = 0.6745 * (current_value - baseline_median) / mad
            result["robust_z"] = robust_z
            is_regression = is_worse and robust_z >= robust_z_threshold
            result["method"] = "median_mad"
        else:
            result["robust_z"] = None
            is_regression = is_worse
            result["method"] = "median_zero_mad"

        result["status"] = "regression" if is_regression else "stable"
        has_regressions = has_regressions or is_regression
        results[name] = result

    if has_regressions:
        status = "regression"
    elif has_insufficient:
        status = "insufficient_history"
    else:
        status = "stable"

    notification_metrics: list[str] = []
    if _check_consecutive and matching:
        previous = max(
            matching, key=lambda record: str(record.get("measured_at") or "")
        )
        previous_commit = str(previous.get("commit") or "")
        previous_history = {
            "schema_version": SCHEMA_VERSION,
            "records": [
                record
                for record in matching
                if str(record.get("commit") or "") != previous_commit
            ],
        }
        previous_report = evaluate(
            previous,
            previous_history,
            min_samples=min_samples,
            robust_z_threshold=robust_z_threshold,
            _check_consecutive=False,
        )
        if previous_report["has_regressions"]:
            # A prior outlier must not inflate the baseline used to classify
            # the current value. Re-evaluate both points against the identical
            # pre-outlier distribution before deciding whether to notify.
            current_report = evaluate(
                current,
                previous_history,
                min_samples=min_samples,
                robust_z_threshold=robust_z_threshold,
                _check_consecutive=False,
            )
            results = current_report["metrics"]
            has_regressions = current_report["has_regressions"]
            status = current_report["status"]
        notification_metrics = sorted(
            name
            for name, metric in results.items()
            if metric["status"] == "regression"
            and (previous_report.get("metrics") or {}).get(name, {}).get("status")
            == "regression"
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "has_regressions": has_regressions,
        "notification_ready": bool(notification_metrics),
        "notification_metrics": notification_metrics,
        "advisory_only": True,
        "current_commit": current_commit,
        "environment_fingerprint": current_key[0],
        "workload_signature": current_key[1],
        "metrics": results,
    }


def update_history(
    history: Mapping[str, Any],
    current: Mapping[str, Any],
    *,
    max_records_per_profile: int = DEFAULT_MAX_RECORDS_PER_PROFILE,
) -> dict[str, Any]:
    """Idempotently add one record and retain recent samples per series."""
    if max_records_per_profile < 1:
        raise HarnessError("max_records_per_profile must be at least 1")
    records = _validate_history(history)
    current_key = (
        str(current.get("commit") or ""),
        *_series_key(current),
    )
    records = [
        record
        for record in records
        if (str(record.get("commit") or ""), *_series_key(record)) != current_key
    ]
    records.append(dict(current))

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(_series_key(record), []).append(record)

    retained = []
    for series in grouped.values():
        series.sort(key=lambda record: str(record.get("measured_at") or ""))
        retained.extend(series[-max_records_per_profile:])
    retained.sort(key=lambda record: str(record.get("measured_at") or ""))
    return {"schema_version": SCHEMA_VERSION, "records": retained}


def _format_metric(value: Any) -> str:
    if value is None:
        return "—"
    return f"{float(value):.4f}"


def render_history_markdown(history: Mapping[str, Any]) -> str:
    records = _validate_history(history)
    lines = [
        "# Alopex Python Performance History",
        "",
        "Generated from `history.json`. Measurements are advisory and never gate a build.",
        "Only records with identical environment and workload fingerprints are compared.",
        "",
        "| Measured at | Commit | Profile | Environment | Workload | "
        "scan_overhead_ms | read_overhead_pct | write_overhead_pct |",
        "|---|---|---|---|---|---:|---:|---:|",
    ]
    for record in sorted(
        records, key=lambda item: str(item.get("measured_at") or ""), reverse=True
    ):
        metrics = record.get("metrics") or {}
        lines.append(
            "| {measured} | `{commit}` | `{profile}` | `{environment}` | `{workload}` | "
            "{scan} | {read} | {write} |".format(
                measured=record.get("measured_at", "unknown"),
                commit=str(record.get("commit") or "")[:12],
                profile=record.get("profile", "unknown"),
                environment=(record.get("environment") or {}).get(
                    "fingerprint", "unknown"
                ),
                workload=str(record.get("workload_signature") or "")[:12],
                scan=_format_metric((metrics.get("scan_overhead_ms") or {}).get("value")),
                read=_format_metric((metrics.get("read_overhead_pct") or {}).get("value")),
                write=_format_metric((metrics.get("write_overhead_pct") or {}).get("value")),
            )
        )
    lines.append("")
    return "\n".join(lines)


def render_comparison_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Alopex Python Performance Advisory",
        "",
        f"Overall status: **{report.get('status', 'unknown')}**",
        f"Issue notification ready: **{str(bool(report.get('notification_ready'))).lower()}**",
        "",
        "> Advisory only: performance regressions create or update an issue; they do not fail CI.",
        "",
        "| Metric | Current | Baseline median | MAD | Samples | Status |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for name, metric in (report.get("metrics") or {}).items():
        lines.append(
            "| `{name}` | {current} {unit} | {median} | {mad} | {samples} | {status} |".format(
                name=name,
                current=_format_metric(metric.get("current")),
                unit=metric.get("unit", ""),
                median=_format_metric(metric.get("baseline_median")),
                mad=_format_metric(metric.get("baseline_mad")),
                samples=metric.get("sample_count", 0),
                status=metric.get("status", "unknown"),
            )
        )
    lines.append("")
    return "\n".join(lines)


def validate_release_tag(tag: str) -> str:
    if not RELEASE_TAG_PATTERN.fullmatch(tag):
        raise HarnessError(f"unsafe release tag: {tag!r}")
    return tag


def load_junit_metrics(path: Path) -> dict[str, float]:
    """Load the monitored numeric record_property values from xUnit XML."""
    if not path.is_file():
        raise HarnessError(f"JUnit input does not exist: {path}")
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError) as exc:
        raise HarnessError(f"could not read JUnit input {path}: {exc}") from exc

    metrics: dict[str, float] = {}
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] != "property":
            continue
        name = element.get("name")
        if name not in METRIC_DEFINITIONS:
            continue
        if name in metrics:
            raise HarnessError(f"duplicate JUnit metric {name}")
        metrics[name] = _finite_float(element.get("value"), name)

    missing = sorted(set(METRIC_DEFINITIONS) - set(metrics))
    if missing:
        raise HarnessError(f"missing JUnit metrics: {', '.join(missing)}")
    return metrics


def render_release_markdown(
    current: Mapping[str, Any], comparison: Mapping[str, Any], tag: str
) -> str:
    tag = validate_release_tag(tag)
    environment = current.get("environment") or {}
    product = "Alopex Python" if tag.startswith("alopex-py-") else "Alopex"
    version = tag.removeprefix("alopex-py-")
    lines = [
        f"# {product} {version} Performance Report",
        "",
        f"Commit: `{current.get('commit', 'unknown')}`",
        f"Measured at: {current.get('measured_at', 'unknown')}",
        f"Profile: `{current.get('profile', 'unknown')}`",
        f"CPU: {environment.get('cpu', 'unknown')}",
        f"Environment fingerprint: `{environment.get('fingerprint', 'unknown')}`",
        f"Workload signature: `{current.get('workload_signature', 'unknown')}`",
        "",
        render_comparison_markdown(comparison),
    ]
    return "\n".join(lines)


def render_missing_release_markdown(*, tag: str, commit: str, reason: str) -> str:
    tag = validate_release_tag(tag)
    product = "Alopex Python" if tag.startswith("alopex-py-") else "Alopex"
    version = tag.removeprefix("alopex-py-")
    return "\n".join(
        [
            f"# {product} {version} Performance Report",
            "",
            f"Commit: `{commit}`",
            "Performance measurement status: **not measured**.",
            f"Reason: `{reason}`",
            "",
            "This advisory status does not block the release.",
            "",
        ]
    )


def _read_json(path: Path, *, default: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        if default is not None:
            return dict(default)
        raise HarnessError(f"JSON input does not exist: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"could not read JSON input {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise HarnessError(f"JSON input must be an object: {path}")
    return value


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_write(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _hash_files(paths: Sequence[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.as_posix()):
        if not path.is_file():
            raise HarnessError(f"signature input does not exist: {path}")
        digest.update(path.as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _command_version(command: Sequence[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "unavailable"
    output = completed.stdout.strip() or completed.stderr.strip()
    return output.splitlines()[0] if output else "unknown"


def _dependency_versions() -> dict[str, str]:
    versions = {"python": sys.version.split()[0]}
    for distribution in importlib.metadata.distributions():
        name = distribution.metadata.get("Name")
        if name:
            normalized = re.sub(r"[-_.]+", "-", name).lower()
            versions[normalized] = distribution.version
    versions["rustc"] = _command_version(("rustc", "--version"))
    versions["cargo"] = _command_version(("cargo", "--version"))
    versions["nim"] = _command_version(("nim", "--version"))
    versions["patchelf"] = _command_version(("patchelf", "--version"))
    return versions


def _runtime_environment() -> dict[str, str]:
    result = {"kernel": _command_version(("uname", "-r"))}
    governor = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    result["cpu_governor"] = (
        governor.read_text(encoding="utf-8").strip() if governor.is_file() else "unavailable"
    )
    turbo_candidates = (
        Path("/sys/devices/system/cpu/intel_pstate/no_turbo"),
        Path("/sys/devices/system/cpu/cpufreq/boost"),
    )
    result["turbo_policy"] = "unavailable"
    for candidate in turbo_candidates:
        if candidate.is_file():
            setting = candidate.read_text(encoding="utf-8").strip()
            result["turbo_policy"] = f"{candidate.name}={setting}"
            break
    return result


def _collect(args: argparse.Namespace) -> None:
    benchmark_path = Path(args.benchmark_json)
    lock_path = Path(args.lock_file)
    workload_files = [Path(path) for path in args.workload_file]
    payload = _read_json(benchmark_path)
    measured_at = str(payload.get("datetime") or datetime.now(timezone.utc).isoformat())
    record = build_record(
        payload,
        commit=args.commit,
        ref=args.ref,
        profile=args.profile,
        measured_at=measured_at,
        dependency_versions=_dependency_versions(),
        lock_hash=_hash_files((lock_path,)),
        workload_signature=_hash_files(workload_files),
        runtime_environment=_runtime_environment(),
        junit_metrics=load_junit_metrics(Path(args.junit_xml)),
    )
    _write_json(Path(args.output), record)


def _evaluate(args: argparse.Namespace) -> None:
    current = _read_json(Path(args.current))
    history = _read_json(Path(args.history), default=_empty_history())
    report = evaluate(
        current,
        history,
        min_samples=args.min_samples,
        robust_z_threshold=args.robust_z_threshold,
    )
    _write_json(Path(args.output_json), report)
    _atomic_write(Path(args.output_markdown), render_comparison_markdown(report))
    print(f"has_regressions={'true' if report['has_regressions'] else 'false'}")
    print(
        f"notification_ready={'true' if report['notification_ready'] else 'false'}"
    )
    print(f"status={report['status']}")


def _record(args: argparse.Namespace) -> None:
    current = _read_json(Path(args.current))
    history_path = Path(args.history)
    history = _read_json(history_path, default=_empty_history())
    updated = update_history(
        history,
        current,
        max_records_per_profile=args.max_records_per_profile,
    )
    _write_json(history_path, updated)
    _atomic_write(Path(args.index), render_history_markdown(updated))

    if args.release_tag:
        tag = validate_release_tag(args.release_tag)
        if not args.comparison or not args.release_dir:
            raise HarnessError(
                "--comparison and --release-dir are required with --release-tag"
            )
        comparison = _read_json(Path(args.comparison))
        release_dir = Path(args.release_dir)
        _atomic_write(
            release_dir / f"{tag}.md",
            render_release_markdown(current, comparison, tag),
        )
        _write_json(
            release_dir / f"{tag}.json",
            {"record": current, "comparison": comparison},
        )


def _missing_release(args: argparse.Namespace) -> None:
    tag = validate_release_tag(args.release_tag)
    report = {
        "schema_version": SCHEMA_VERSION,
        "tag": tag,
        "commit": args.commit,
        "status": "not_measured",
        "reason": args.reason,
        "advisory_only": True,
    }
    _atomic_write(
        Path(args.output_markdown),
        render_missing_release_markdown(
            tag=tag,
            commit=args.commit,
            reason=args.reason,
        ),
    )
    _write_json(Path(args.output_json), report)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect", help="normalize pytest-benchmark JSON")
    collect.add_argument("--benchmark-json", required=True)
    collect.add_argument("--junit-xml", required=True)
    collect.add_argument("--output", required=True)
    collect.add_argument("--commit", required=True)
    collect.add_argument("--ref", required=True)
    collect.add_argument("--profile", required=True)
    collect.add_argument("--lock-file", required=True)
    collect.add_argument("--workload-file", action="append", required=True)
    collect.set_defaults(handler=_collect)

    compare = subparsers.add_parser("evaluate", help="compare with matching history")
    compare.add_argument("--current", required=True)
    compare.add_argument("--history", required=True)
    compare.add_argument("--output-json", required=True)
    compare.add_argument("--output-markdown", required=True)
    compare.add_argument("--min-samples", type=int, default=DEFAULT_MIN_SAMPLES)
    compare.add_argument(
        "--robust-z-threshold", type=float, default=DEFAULT_ROBUST_Z_THRESHOLD
    )
    compare.set_defaults(handler=_evaluate)

    record = subparsers.add_parser("record", help="append canonical history")
    record.add_argument("--current", required=True)
    record.add_argument("--history", required=True)
    record.add_argument("--index", required=True)
    record.add_argument(
        "--max-records-per-profile",
        type=int,
        default=DEFAULT_MAX_RECORDS_PER_PROFILE,
    )
    record.add_argument("--comparison")
    record.add_argument("--release-tag")
    record.add_argument("--release-dir")
    record.set_defaults(handler=_record)

    missing = subparsers.add_parser(
        "missing-release", help="record a non-blocking missing release measurement"
    )
    missing.add_argument("--release-tag", required=True)
    missing.add_argument("--commit", required=True)
    missing.add_argument("--reason", required=True)
    missing.add_argument("--output-markdown", required=True)
    missing.add_argument("--output-json", required=True)
    missing.set_defaults(handler=_missing_release)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        args.handler(args)
    except HarnessError as exc:
        print(f"performance harness error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
