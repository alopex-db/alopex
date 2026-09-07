"""Fail a dedicated performance lane on incomplete or out-of-budget parity evidence."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


THROUGHPUT_METRICS = {
    "rows_per_second",
    "steady_state_rows_per_second",
    "queries_per_second",
}
RESOURCE_METRICS = {"peak_rss_bytes", "index_size_bytes"}


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def _ratio(subject: object, reference: object, label: str, errors: list[str]) -> float | None:
    if not isinstance(subject, (int, float)) or not math.isfinite(subject):
        errors.append(f"{label}: subject metric must be finite")
        return None
    if not isinstance(reference, (int, float)) or not math.isfinite(reference) or reference <= 0:
        errors.append(f"{label}: reference metric must be finite and positive")
        return None
    return float(subject) / float(reference)


def evaluate(name: str, contract: dict[str, object], measurement: dict[str, object]) -> list[str]:
    errors: list[str] = []
    if measurement.get("reference_revision") != contract.get("reference_revision"):
        errors.append(f"{name}: reference_revision does not match the contract")
    if measurement.get("dataset_sha256") != contract.get("fixture", {}).get("dataset_sha256"):
        errors.append(f"{name}: dataset_sha256 does not match the contract")
    if "evidence_coverage" in contract:
        expected_coverage = contract["evidence_coverage"].get(
            measurement.get("evidence_id"), []
        )
        if measurement.get("covered_claims") != expected_coverage:
            errors.append(f"{name}: covered_claims do not match the contract")
    subject = measurement.get("subject", {})
    reference = measurement.get("reference", {})
    thresholds = contract.get("thresholds", {})
    for metric in contract.get("metrics", []):
        if metric not in subject or metric not in reference:
            errors.append(f"{name}: missing paired metric {metric}")
            continue
        if metric == "recall_at_10":
            value = subject[metric]
            reference_value = reference[metric]
            if not isinstance(value, (int, float)) or not math.isfinite(value):
                errors.append(f"{name}: subject recall_at_10 must be finite")
                continue
            if not isinstance(reference_value, (int, float)) or not math.isfinite(reference_value):
                errors.append(f"{name}: reference recall_at_10 must be finite")
                continue
            if value < thresholds["min_recall"]:
                errors.append(f"{name}: recall_at_10 is below min_recall")
            if reference_value < thresholds["min_recall"]:
                errors.append(f"{name}: reference recall_at_10 is below min_recall")
            delta = abs(float(value) - float(reference_value))
            limit = thresholds["max_recall_delta"]
            if delta > limit:
                errors.append(f"{name}: recall_at_10 delta {delta:.4f} exceeds {limit}")
            continue
        ratio = _ratio(subject[metric], reference[metric], f"{name}: {metric}", errors)
        if ratio is None:
            continue
        if metric in THROUGHPUT_METRICS:
            limit = thresholds["min_throughput_ratio"]
            if ratio < limit:
                errors.append(f"{name}: {metric} ratio {ratio:.4f} is below {limit}")
        elif metric in RESOURCE_METRICS:
            limit = thresholds["max_peak_memory_ratio"]
            if ratio > limit:
                errors.append(f"{name}: {metric} ratio {ratio:.4f} exceeds {limit}")
        elif metric == "temporary_io_bytes":
            limit = thresholds["max_temporary_io_ratio"]
            if ratio > limit:
                errors.append(f"{name}: {metric} ratio {ratio:.4f} exceeds {limit}")
        else:
            limit = thresholds.get("max_ratio_by_metric", {}).get(
                metric, thresholds["max_latency_ratio"]
            )
            if ratio > limit:
                errors.append(f"{name}: {metric} ratio {ratio:.4f} exceeds {limit}")
    return errors


def validate_document(
    contracts: dict[str, object], measurements: dict[str, object], suite: str
) -> list[str]:
    errors: list[str] = []
    profile = contracts.get("runner_profile", {})
    environment = measurements.get("environment", {})
    for field in (
        "os",
        "cpu_model",
        "logical_cpu_count",
        "cpu_affinity",
        "memory_bytes",
        "build_profile",
        "thread_count",
    ):
        if environment.get(field) != profile.get(field):
            errors.append(f"environment: {field} does not match the runner profile")
    for field in (
        "alopex_version",
        "alopex_revision",
        "alopex_tree_sha256",
        "kernel",
        "python_version",
    ):
        if not environment.get(field):
            errors.append(f"environment: missing {field}")
    results = measurements.get("results", [])
    indexed = {(row.get("contract"), row.get("evidence_id")): row for row in results}
    for name, contract in contracts.get("contracts", {}).items():
        evidence_ids = contract.get("evidence_ids", [])
        required = evidence_ids if suite == "full" else evidence_ids[:1]
        for evidence_id in required:
            row = indexed.get((name, evidence_id))
            if row is None:
                errors.append(f"{name}: missing {suite} evidence {evidence_id}")
            else:
                errors.extend(evaluate(f"{name}/{evidence_id}", contract, row))
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--contracts",
        type=Path,
        default=Path("docs/parity/performance-v0.8.11.json"),
    )
    parser.add_argument("--measurements", type=Path, required=True)
    parser.add_argument("--suite", choices=("curated", "full"), required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    contracts = json.loads(args.contracts.read_text(encoding="utf-8"))
    measurements = json.loads(args.measurements.read_text(encoding="utf-8"))
    errors = validate_document(contracts, measurements, args.suite)
    report = {
        "schema": "alopex.performance-gate/v1",
        "suite": args.suite,
        "passed": not errors,
        "errors": errors,
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    if errors:
        print("\n".join(errors))
        return 1
    print(f"validated {args.suite} performance parity evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
