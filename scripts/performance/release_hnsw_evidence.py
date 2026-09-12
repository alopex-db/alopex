#!/usr/bin/env python3
"""Validate a publishable HNSW snapshot produced for an exact source commit."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

try:
    from scripts.performance.hnsw_v0811_contract import best_at_recall, render_markdown
except ModuleNotFoundError:  # Direct execution adds this script directory to sys.path.
    from hnsw_v0811_contract import best_at_recall, render_markdown


REQUIRED_ARTIFACTS = (
    "hnsw-diagnostic.raw.json",
    "hnsw-diagnostic.json",
    "hnsw-diagnostic.csv",
    "hnsw-diagnostic.md",
    "hnsw-builds.csv",
    "hnsw-recall-ceiling.csv",
    "hnsw-recall-configurations.csv",
    "hnsw-fixed-cost-runs.csv",
    "hnsw-latency-decomposition.csv",
    "hnsw-hybrid.csv",
    "hnsw-hybrid-summary.csv",
    "hnsw-scale.csv",
    "hnsw-scale-curve.csv",
    "environment.txt",
)
REQUIRED_ENGINES = {
    "alopex-hnsw",
    "hnswlib",
    "faiss-flat-exact",
    "faiss-hnsw",
}
REQUIRED_METRICS = {
    "recall_at_10",
    "tie_aware_recall_at_10",
    "queries_per_second",
    "build_time_seconds",
    "index_size_bytes",
    "peak_rss_bytes",
}
HYBRID_ARMS = {"alopex-sql-hnsw-postfilter", "sqlite-hnswlib", "filtered-exact"}
HYBRID_SELECTIVITIES = {0.001, 0.01, 0.05, 0.2, 1.0}
SCALE_ENGINES = {"alopex-hnsw", "hnswlib", "faiss-hnsw", "flat"}
SCALE_SIZES = {10_000, 50_000}
DATASET_SHA256 = "ec7ad773a70e654d65ee3757748fe99d614be24ad087330e437bcc84cda85291"


def _load(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"artifact must contain a JSON object: {path.name}")
    return payload


def _complete_rows(rows: object, required: set[str]) -> bool:
    return isinstance(rows, list) and bool(rows) and all(
        isinstance(row, dict) and required.issubset(row) for row in rows
    )


def validate_artifacts(root: Path, *, version: str, commit: str) -> None:
    missing = [name for name in REQUIRED_ARTIFACTS if not (root / name).is_file()]
    if missing:
        raise ValueError(f"missing artifact: {', '.join(missing)}")
    payload = validate_public_pair(root, version=version, commit=commit)
    raw = _load(root / "hnsw-diagnostic.raw.json")
    if raw.get("schema") != "alopex.hnsw-diagnostic-raw/v3":
        raise ValueError("unexpected raw diagnostic schema")
    if raw.get("provenance") != payload["provenance"] or raw.get(
        "dataset"
    ) != payload["dataset"]:
        raise ValueError("raw and normalized evidence identities differ")


def validate_public_pair(
    root: Path, *, version: str, commit: str
) -> dict[str, object]:
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None:
        raise ValueError("release version must be X.Y.Z")
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError("source commit must be a full lowercase SHA")

    payload = _load(root / "hnsw-diagnostic.json")
    if payload.get("schema") != "alopex.hnsw-diagnostic/v3":
        raise ValueError("unexpected diagnostic schema")
    if payload.get("release_version") != version:
        raise ValueError("release version does not match the evidence")

    provenance = payload.get("provenance")
    if not isinstance(provenance, dict) or provenance.get("source_commit") != commit:
        raise ValueError("source commit does not match the evidence")
    if (
        not isinstance(provenance.get("cpu_model"), str)
        or not provenance["cpu_model"].strip()
        or provenance.get("cpu_affinity") != [0]
    ):
        raise ValueError("canonical CPU and affinity metadata is incomplete")
    dependencies = provenance.get("dependencies")
    if not isinstance(dependencies, dict) or dependencies.get("alopex") != version:
        raise ValueError("measured Alopex version does not match the release version")
    dataset = payload.get("dataset")
    if not isinstance(dataset, dict) or dataset.get("source_sha256") != DATASET_SHA256:
        raise ValueError("benchmark dataset checksum does not match the contract")
    contract = payload.get("contract")
    required_contract = {
        "dataset_size": 9171,
        "dimension": 128,
        "warmup": "one complete query cycle per engine/setting",
        "min_duration_seconds": 2.0,
        "min_queries": 10000,
        "runs": 3,
    }
    if not isinstance(contract, dict) or any(
        contract.get(name) != value for name, value in required_contract.items()
    ):
        raise ValueError("benchmark measurement contract does not match")
    metrics = contract.get("metrics")
    if not isinstance(metrics, list) or not REQUIRED_METRICS.issubset(metrics):
        raise ValueError("benchmark metrics are incomplete")
    builds = payload.get("builds")
    engines = (
        {row.get("engine") for row in builds if isinstance(row, dict)}
        if isinstance(builds, list)
        else set()
    )
    if engines != REQUIRED_ENGINES:
        raise ValueError("benchmark comparison arms are incomplete")
    if not _complete_rows(
        builds, {"engine", "build_time_seconds", "index_size_bytes", "peak_rss_bytes"}
    ):
        raise ValueError("benchmark build measurements are incomplete")
    summary = payload.get("summary")
    summary_engines = (
        {row.get("engine") for row in summary if isinstance(row, dict)}
        if isinstance(summary, list)
        else set()
    )
    if summary_engines != REQUIRED_ENGINES:
        raise ValueError("benchmark summary arms are incomplete")
    if not _complete_rows(
        summary,
        {
            "engine",
            "ef_search",
            "median_recall_at_10",
            "median_queries_per_second",
            "median_latency_us",
        },
    ):
        raise ValueError("benchmark summary measurements are incomplete")
    best = payload.get("best_at_recall")
    if not isinstance(best, dict) or set(best) != {"0.95", "0.99"}:
        raise ValueError("recall threshold summary is incomplete")
    if best != best_at_recall(summary):
        raise ValueError("recall threshold summary does not match benchmark rows")
    scale = payload.get("scale")
    hybrid = payload.get("hybrid")
    if not isinstance(scale, dict) or not scale.get("results"):
        raise ValueError("scale evidence is incomplete")
    if not isinstance(hybrid, dict) or not hybrid.get("summary"):
        raise ValueError("hybrid evidence is incomplete")
    scale_matrix = {
        (row.get("dataset_size"), row.get("engine"))
        for row in scale["results"]
        if isinstance(row, dict)
    }
    if scale_matrix != {
        (size, engine) for size in SCALE_SIZES for engine in SCALE_ENGINES
    }:
        raise ValueError("scale comparison matrix is incomplete")
    if not _complete_rows(
        scale["results"],
        {
            "dataset_size",
            "engine",
            "build_time_seconds",
            "index_size_bytes",
            "peak_rss_bytes",
            "qps_at_recall_095",
            "ef_search_at_recall_095",
            "recall_at_selected_setting",
        },
    ):
        raise ValueError("scale measurements are incomplete")
    if (
        scale.get("dataset") != "glove-100-angular"
        or scale.get("sha256")
        != "544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4"
        or scale.get("requested_sizes") != [10_000, 50_000, 200_000, 1_000_000]
        or scale.get("max_n") != 50_000
    ):
        raise ValueError("scale dataset contract does not match")
    limits = scale.get("limits")
    if not isinstance(limits, list) or {
        row.get("dataset_size") for row in limits if isinstance(row, dict)
    } != {200_000, 1_000_000}:
        raise ValueError("scale execution limits are incomplete")
    hybrid_matrix = {
        (row.get("selectivity"), row.get("arm"))
        for row in hybrid["summary"]
        if isinstance(row, dict)
    }
    if hybrid_matrix != {
        (selectivity, arm)
        for selectivity in HYBRID_SELECTIVITIES
        for arm in HYBRID_ARMS
    }:
        raise ValueError("hybrid comparison matrix is incomplete")
    if not _complete_rows(
        hybrid["summary"],
        {
            "selectivity",
            "arm",
            "median_latency_p50_us",
            "median_latency_p95_us",
            "filtered_top_k_accuracy",
            "median_overfetch_amplification",
            "returns_k",
        },
    ):
        raise ValueError("hybrid measurements are incomplete")

    environment = (
        (root / "environment.txt")
        if (root / "environment.txt").is_file()
        else None
    )
    if environment is not None:
        metadata = environment.read_text(encoding="utf-8")
        if (
            not any(
                line.startswith("cpu_model=") and line != "cpu_model="
                for line in metadata.splitlines()
            )
            or "benchmark_core=0" not in metadata.splitlines()
            or "benchmark_affinity=0" not in metadata.splitlines()
        ):
            raise ValueError("benchmark environment metadata is incomplete")

    markdown = (root / "hnsw-diagnostic.md").read_bytes()
    if hashlib.sha256(markdown).hexdigest() != payload.get("markdown_sha256"):
        raise ValueError("Markdown hash does not match the canonical JSON")
    report = markdown.decode("utf-8")
    if report != render_markdown(payload):
        raise ValueError("Markdown does not match the canonical JSON rendering")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--public-pair", action="store_true")
    args = parser.parse_args()
    validator = validate_public_pair if args.public_pair else validate_artifacts
    validator(args.root, version=args.version, commit=args.commit)


if __name__ == "__main__":
    main()
