"""Validate v0.8.11 compatibility ledgers without third-party dependencies."""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PERFORMANCE_CONTRACTS = ROOT / "docs/parity/performance-v0.8.11.json"
LEDGERS = (
    ROOT / "docs/parity/polars-v1.43.2.json",
    ROOT / "docs/parity/hnsw-v0.8.11.json",
    ROOT / "docs/parity/sql-v0.8.11.json",
)

COMPATIBLE_STATUSES = {"implemented-compatible", "reference-compatible"}
PERFORMANCE_STATUSES = COMPATIBLE_STATUSES | {"known-performance-divergence"}
FIXTURE_FIELDS = {
    "hardware_profile",
    "build_profile",
    "thread_count",
    "dataset_sha256",
    "rows",
    "columns",
    "dtypes",
    "null_ratio",
    "chunk_rows",
    "cache_states",
    "warmup_runs",
    "measurement_runs",
}
FIXTURE_FIELDS_BY_KIND = {
    "tabular": set(),
    "streaming": {"resource_limit_bytes"},
    "hnsw": {
        "dimension",
        "metric",
        "seed",
        "insertion_order",
        "m",
        "ef_construction",
        "ef_search",
    },
    "sql": {"queries", "parameters", "concurrency", "timeout_seconds"},
}
METRICS_BY_KIND = {
    "tabular": {"latency_p50_ms", "latency_p95_ms", "rows_per_second", "peak_rss_bytes"},
    "streaming": {
        "plan_build_p50_ms",
        "time_to_first_batch_p50_ms",
        "total_p50_ms",
        "steady_state_rows_per_second",
        "peak_rss_bytes",
    },
    "hnsw": {
        "recall_at_10",
        "build_latency_ms",
        "query_latency_p50_ms",
        "query_latency_p95_ms",
        "query_latency_p99_ms",
        "queries_per_second",
        "index_size_bytes",
        "peak_rss_bytes",
        "update_latency_ms",
        "delete_latency_ms",
        "reopen_latency_ms",
    },
    "sql": {
        "plan_latency_p50_ms",
        "execution_latency_p50_ms",
        "latency_p50_ms",
        "latency_p95_ms",
        "queries_per_second",
        "peak_rss_bytes",
        "temporary_io_bytes",
    },
}
COMMON_THRESHOLDS = {
    "max_latency_ratio",
    "min_throughput_ratio",
    "max_peak_memory_ratio",
}
MUTABLE_REVISIONS = {"main", "master", "latest", "current", "stable"}
SQL_UPSTREAM_FIELDS = {
    "feature",
    "repository",
    "commit",
    "file",
    "case",
    "license",
    "evidence",
    "issue",
}


def load_performance_contracts(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_performance_contracts(path: Path, payload: dict[str, object]) -> list[str]:
    errors: list[str] = []
    profile = payload.get("runner_profile", {})
    if not isinstance(profile, dict) or profile.get("labels") != [
        "self-hosted",
        "linux",
        "x64",
        "alopex-performance",
    ]:
        errors.append(f"{path}: performance runner must use the dedicated labels")
    contracts = payload.get("contracts", {})
    if not isinstance(contracts, dict) or not contracts:
        return [*errors, f"{path}: no performance contracts"]
    for name, contract in contracts.items():
        kind = contract.get("kind")
        required_metrics = METRICS_BY_KIND.get(kind)
        if required_metrics is None:
            errors.append(f"{path}: {name} has invalid kind {kind}")
            continue
        revision = str(contract.get("reference_revision", ""))
        pinned_revisions = revision.split(";")
        if (
            not revision
            or any(
                token in MUTABLE_REVISIONS
                for token in revision.lower().replace("/", "@").split("@")
            )
            or any(
                not re.fullmatch(r"[0-9a-f]{40}", pin.rsplit("@", 1)[-1])
                for pin in pinned_revisions
            )
        ):
            errors.append(f"{path}: {name} needs an exact reference_revision")
        fixture = contract.get("fixture", {})
        missing_fixture = (FIXTURE_FIELDS | FIXTURE_FIELDS_BY_KIND[kind]).difference(
            fixture
        )
        for field in sorted(missing_fixture):
            errors.append(f"{path}: {name} fixture missing {field}")
        queries = fixture.get("queries")
        if queries:
            query_path = ROOT / str(queries)
            if not query_path.is_file():
                errors.append(f"{path}: {name} query fixture does not exist")
            elif hashlib.sha256(query_path.read_bytes()).hexdigest() != fixture.get(
                "dataset_sha256"
            ):
                errors.append(f"{path}: {name} query fixture checksum does not match")
        metrics = set(contract.get("metrics", []))
        for metric in sorted(required_metrics.difference(metrics)):
            errors.append(f"{path}: {name} metrics missing {metric}")
        thresholds = contract.get("thresholds", {})
        compatibility_thresholds = contract.get("compatibility_thresholds", {})
        for threshold in sorted(COMMON_THRESHOLDS.difference(thresholds)):
            errors.append(f"{path}: {name} thresholds missing {threshold}")
        for threshold in sorted(COMMON_THRESHOLDS.difference(compatibility_thresholds)):
            errors.append(f"{path}: {name} compatibility_thresholds missing {threshold}")
        if kind == "hnsw" and "min_recall" not in thresholds:
            errors.append(f"{path}: {name} thresholds missing min_recall")
        if kind == "hnsw" and "min_recall" not in compatibility_thresholds:
            errors.append(f"{path}: {name} compatibility_thresholds missing min_recall")
        if kind == "sql" and "max_temporary_io_ratio" not in thresholds:
            errors.append(f"{path}: {name} thresholds missing max_temporary_io_ratio")
        if kind == "sql" and "max_temporary_io_ratio" not in compatibility_thresholds:
            errors.append(
                f"{path}: {name} compatibility_thresholds missing max_temporary_io_ratio"
            )
        evidence = contract.get("evidence")
        if not evidence or not (ROOT / str(evidence)).is_file():
            errors.append(f"{path}: {name} has no runnable evidence")
        if not contract.get("evidence_ids"):
            errors.append(f"{path}: {name} has no evidence_ids")
        comparison = contract.get("comparison", {})
        if comparison.get("method") != "paired-median-ratio":
            errors.append(f"{path}: {name} must use paired-median-ratio")
    return errors


def validate(path: Path, performance: dict[str, object] | None = None) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if performance is None:
        performance = load_performance_contracts(PERFORMANCE_CONTRACTS)
    contracts = performance.get("contracts", {})
    errors: list[str] = []
    statuses = set(payload.get("status_values", []))
    entries = payload.get("entries", [])
    seen: set[str] = set()
    for entry in entries:
        api = entry.get("api")
        if not api:
            errors.append(f"{path}: entry has no api")
            continue
        if api in seen:
            errors.append(f"{path}: duplicate api {api}")
        seen.add(api)
        if entry.get("status") not in statuses:
            errors.append(f"{path}: invalid status for {api}")
        if not entry.get("evidence"):
            errors.append(f"{path}: missing evidence for {api}")
        if entry.get("status") not in {
            "alopex-extension",
            "not-yet-implemented",
            "unsupported",
        } and not entry.get("reference"):
            errors.append(f"{path}: missing reference for {api}")
        if entry.get("status") in PERFORMANCE_STATUSES:
            contract_name = entry.get("performance_contract")
            if not contract_name:
                errors.append(f"{path}: missing performance_contract for {api}")
            elif contract_name not in contracts:
                errors.append(f"{path}: unknown performance_contract {contract_name} for {api}")
            else:
                contract = contracts[contract_name]
                evidence_id = entry.get("performance_evidence")
                if not evidence_id:
                    errors.append(f"{path}: missing performance_evidence for {api}")
                elif evidence_id not in contract.get("evidence_ids", []):
                    errors.append(f"{path}: invalid performance_evidence for {api}")
                if (
                    entry.get("status") in COMPATIBLE_STATUSES
                    and contract.get("thresholds")
                    != contract.get("compatibility_thresholds")
                ):
                    errors.append(f"{path}: {api} is outside the compatibility budget")
        if entry.get("status") == "known-performance-divergence" and not entry.get("issue"):
            errors.append(f"{path}: performance divergence {api} has no issue")
    if not entries:
        errors.append(f"{path}: empty ledger")
    if payload.get("schema") == "alopex.sql-conformance/v1":
        public_api = payload.get("public_api", [])
        names = [row.get("api") for row in public_api]
        if not public_api or len(names) != len(set(names)):
            errors.append(f"{path}: SQL public inventory is empty or duplicated")
        required_surfaces = {"statement", "scalar", "metadata", "Rust", "embedded", "Python", "CLI", "server"}
        missing_surfaces = required_surfaces.difference(
            row.get("surface") for row in public_api
        )
        if missing_surfaces:
            errors.append(f"{path}: SQL public inventory missing {sorted(missing_surfaces)}")
        upstream = payload.get("upstream_cases", [])
        features = {row.get("feature") for row in upstream}
        required_features = {
            "transaction/savepoint",
            "prepared statements",
            "EXPLAIN",
            "metadata",
            "schema evolution",
            "CHECK/FK",
            "RETURNING",
            "ON CONFLICT",
            "MERGE",
            "COPY",
            "IDENTITY",
            "SEQUENCE",
        }
        if missing := required_features.difference(features):
            errors.append(f"{path}: SQL upstream inventory missing {sorted(missing)}")
        for row in upstream:
            missing = SQL_UPSTREAM_FIELDS.difference(row)
            if missing:
                errors.append(f"{path}: upstream case missing {sorted(missing)}")
            if not re.fullmatch(r"[0-9a-f]{40}", str(row.get("commit", ""))):
                errors.append(f"{path}: upstream case needs an exact commit")
            for evidence in str(row.get("evidence", "")).split(";"):
                if evidence and not (ROOT / evidence).exists():
                    errors.append(f"{path}: upstream evidence does not exist: {evidence}")
    if payload.get("schema") == "alopex.hnsw-conformance/v1":
        public_api = payload.get("public_api", [])
        names = [row.get("api") for row in public_api]
        if not public_api or len(names) != len(set(names)):
            errors.append(f"{path}: HNSW public inventory is empty or duplicated")
        missing_surfaces = {"Rust", "embedded", "Python", "SQL", "docs"}.difference(
            row.get("surface") for row in public_api
        )
        if missing_surfaces:
            errors.append(f"{path}: HNSW public inventory missing {sorted(missing_surfaces)}")
        for row in public_api:
            if not {"api", "surface", "reference", "status", "evidence", "issue"}.issubset(row):
                errors.append(f"{path}: incomplete HNSW public row {row.get('api')}")
                continue
            if row["status"] not in statuses:
                errors.append(f"{path}: invalid HNSW public status for {row['api']}")
            if row["reference"] != "alopex" and not re.search(
                r"@[0-9a-f]{40}(?::|$)", str(row["reference"])
            ):
                errors.append(f"{path}: mutable HNSW public reference for {row['api']}")
            for evidence in str(row["evidence"]).split(";"):
                if evidence and not (ROOT / evidence).exists():
                    errors.append(f"{path}: missing HNSW public evidence {evidence}")
            if row["status"] in PERFORMANCE_STATUSES and (
                row.get("performance_contract") != "hnsw-pareto-v1"
                or row.get("performance_evidence") != "hnsw-pareto"
            ):
                errors.append(f"{path}: missing HNSW Pareto evidence for {row['api']}")
    return errors


def main() -> int:
    performance = load_performance_contracts(PERFORMANCE_CONTRACTS)
    errors = validate_performance_contracts(PERFORMANCE_CONTRACTS, performance)
    errors.extend(error for path in LEDGERS for error in validate(path, performance))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"validated {len(LEDGERS)} v0.8.11 ledgers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
