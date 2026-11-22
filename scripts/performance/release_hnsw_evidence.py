#!/usr/bin/env python3
"""Audit exact-source public HNSW evidence and the upload inventory."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
from pathlib import Path

try:
    from scripts.performance.hnsw_v0811_contract import analyze_scale, best_at_recall, summarize_by_engine
    from scripts.performance.hnsw_v0811_public import (
        ARMS, DATASET_SHA256, ENGINES, ROW_FIELDS, SCHEMA,
        render_public_markdown,
    )
except ModuleNotFoundError:
    from hnsw_v0811_contract import analyze_scale, best_at_recall, summarize_by_engine
    from hnsw_v0811_public import (
        ARMS, DATASET_SHA256, ENGINES, ROW_FIELDS, SCHEMA,
        render_public_markdown,
    )

PUBLIC_PAIR = frozenset(("hnsw-diagnostic.json", "hnsw-diagnostic.md"))
STATUS_PAIR = frozenset(("benchmark-status.json", "benchmark-status.md"))
STAGES = ("dependencies", "build", "contracts", "differential", "hnsw_dataset", "scale_dataset", "benchmark", "sql_knn_cost", "prepared_batch_throughput", "gate")
STAGE_OUTCOMES = {"success", "failure", "skipped", "cancelled"}
REQUIRED_ENGINES = ENGINES - {"flat"}
HYBRID_SELECTIVITIES = {0.001, 0.01, 0.05, 0.2, 1.0}
SCALE_SIZES = {10_000, 50_000}
SCALE_ENGINES = {"alopex-hnsw", "hnswlib", "faiss-hnsw", "flat"}
GLOVE_SHA256 = "544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4"


def _load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"), parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")))
    if not isinstance(value, dict):
        raise ValueError("public JSON must be an object")
    return value


def _keys(value: object, expected: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(f"{label} keys do not match the public schema")
    return value


def _number(value: object, label: str, *, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{label} must be a finite number")


def _rows(rows: object, kind: str, *, label: str = "engine") -> list[dict[str, object]]:
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{kind} measurements are incomplete")
    for row in rows:
        expected = {label, *ROW_FIELDS[kind]}
        if kind == "hybrid_summary":
            expected.add("returns_k")
        if kind == "scale_results":
            expected.add("curve")
        _keys(row, expected, kind)
        if row[label] not in (ARMS if label == "arm" else ENGINES):
            raise ValueError(f"{kind} label is invalid")
        for metric in ROW_FIELDS[kind]:
            _number(row[metric], f"{kind}.{metric}", nullable=(kind == "scale_results" and metric in ("ef_search_at_recall_095", "recall_at_selected_setting")) or (kind == "runs" and metric in ("native_search_latency_us", "python_binding_residual_us")) or (kind == "builds" and metric in ("update_latency_ms", "delete_latency_ms", "reopen_latency_ms")))
        if kind == "hybrid_summary" and type(row["returns_k"]) is not bool:
            raise ValueError("hybrid return result is invalid")
        if kind == "scale_results":
            _rows(row["curve"], "scale_curve")
    return rows


def _exact_files(root: Path, allowed: frozenset[str]) -> None:
    if not root.is_dir():
        raise ValueError("public artifact directory is missing")
    entries = list(root.iterdir())
    if {p.name for p in entries} != allowed or any(not p.is_file() or p.is_symlink() for p in entries):
        raise ValueError("public artifact inventory contains a missing or extra member")


def _check_timed_runs(runs: list[dict[str, object]], *, scale: bool) -> None:
    matrix: dict[tuple[object, object, object], set[int]] = {}
    for row in runs:
        if row["query_count"] < 10000 or row["duration_seconds"] < 2.0:
            raise ValueError("timed run is below the measurement minimum")
        if type(row["run"]) is not int or row["run"] not in (1, 2, 3):
            raise ValueError("timed run index is invalid")
        key = (row["dataset_size"], row["engine"], row["ef_search"])
        indices = matrix.setdefault(key, set())
        if row["run"] in indices:
            raise ValueError("duplicate timed run")
        indices.add(row["run"])
    if any(indices != {1, 2, 3} for indices in matrix.values()):
        raise ValueError("timed run coverage is incomplete")
    if not scale:
        expected = {(9171, engine, ef) for engine in REQUIRED_ENGINES - {"faiss-flat-exact"} for ef in (16, 32, 64, 128, 256)}
        expected.add((9171, "faiss-flat-exact", 9171))
        if set(matrix) != expected:
            raise ValueError("primary search setting coverage is incomplete")
    else:
        expected = {(size, engine, ef) for size in SCALE_SIZES for engine in REQUIRED_ENGINES - {"faiss-flat-exact"} for ef in (16, 32, 64, 128, 256)}
        expected |= {(size, "faiss-flat-exact", size) for size in SCALE_SIZES}
        if set(matrix) != expected:
            raise ValueError("scale search setting coverage is incomplete")


def validate_public_pair(root: Path, *, version: str, commit: str, run_id: int | None = None, run_attempt: int | None = None) -> dict[str, object]:
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError("invalid expected release identity")
    payload = _load(root / "hnsw-diagnostic.json")
    _keys(payload, {"schema", "release_version", "source_commit", "run_id", "run_attempt", "dataset", "contract", "builds", "runs", "summary", "recall_ceiling", "recall_investigation", "fixed_cost_runs", "latency_decomposition", "hybrid", "scale", "best_at_recall", "median_queries_per_second", "markdown_sha256"}, "public root")
    if payload["schema"] != SCHEMA or payload["release_version"] != version or payload["source_commit"] != commit:
        raise ValueError("public evidence source identity does not match")
    for field in ("run_id", "run_attempt"):
        if type(payload[field]) is not int or payload[field] <= 0:
            raise ValueError("invalid public run identity")
    if run_id is not None and (payload["run_id"], payload["run_attempt"]) != (run_id, run_attempt):
        raise ValueError("public evidence run identity does not match")
    dataset = _keys(payload["dataset"], {"source_sha256", "clean_rows", "dimension", "query_rows", "explained_variance"}, "dataset")
    if dataset["source_sha256"] != DATASET_SHA256 or dataset["clean_rows"] != 9171 or dataset["dimension"] != 128:
        raise ValueError("benchmark dataset contract does not match")
    for metric in ("clean_rows", "dimension", "query_rows", "explained_variance"):
        _number(dataset[metric], f"dataset.{metric}")
    contract = _keys(payload["contract"], {"dataset_size", "dimension", "min_duration_seconds", "min_queries", "runs", "warmup", "metrics", "build_metrics"}, "contract")
    if any(contract[key] != value for key, value in {"dataset_size": 9171, "dimension": 128, "min_duration_seconds": 2.0, "min_queries": 10000, "runs": 3, "warmup": "one complete query cycle per engine/setting"}.items()):
        raise ValueError("benchmark measurement contract does not match")
    if contract["metrics"] != ["recall_at_10", "tie_aware_recall_at_10", "query_latency_p50_us", "query_latency_p95_us", "query_latency_p99_us", "native_search_latency_us", "python_binding_residual_us", "queries_per_second"] or contract["build_metrics"] != ["build_time_seconds", "index_size_bytes", "peak_rss_bytes", "update_latency_ms", "delete_latency_ms", "reopen_latency_ms"]:
        raise ValueError("benchmark metric contract does not match")
    builds = _rows(payload["builds"], "builds")
    runs = _rows(payload["runs"], "runs")
    _check_timed_runs(runs, scale=False)
    summary = _rows(payload["summary"], "summary")
    if {r["engine"] for r in builds} != REQUIRED_ENGINES or {r["engine"] for r in summary} != REQUIRED_ENGINES:
        raise ValueError("benchmark comparison arms are incomplete")
    if {r["engine"] for r in runs} != REQUIRED_ENGINES or summary != summarize_by_engine(runs):
        raise ValueError("benchmark summary does not match measured runs")
    ceiling = _rows(payload["recall_ceiling"], "recall_ceiling")
    if {(r["engine"], r["ef_search"]) for r in ceiling} != {(engine, ef) for engine in REQUIRED_ENGINES - {"faiss-flat-exact"} for ef in (512, 1024, 4096, 9171)}:
        raise ValueError("recall ceiling coverage is incomplete")
    investigation = _keys(payload["recall_investigation"], {"index_count", "input_count", "index_count_matches_input", "self_match_rate", "self_match_ef_search", "recall_configurations", "exact_metric_agreement"}, "recall investigation")
    for metric in ("index_count", "input_count", "self_match_rate", "self_match_ef_search"):
        _number(investigation[metric], metric)
    if type(investigation["index_count_matches_input"]) is not bool:
        raise ValueError("recall count result is invalid")
    metric_agreement = _keys(investigation["exact_metric_agreement"], {"rows", "queries", "normalized", "l2_inner_product_top_10_equal"}, "exact metric agreement")
    for metric in ("rows", "queries"):
        _number(metric_agreement[metric], metric)
    if any(type(metric_agreement[metric]) is not bool for metric in ("normalized", "l2_inner_product_top_10_equal")):
        raise ValueError("exact metric agreement result is invalid")
    _rows(investigation["recall_configurations"], "recall_configurations")
    _rows(payload["fixed_cost_runs"], "fixed_cost_runs")
    _rows(payload["latency_decomposition"], "latency_decomposition")
    hybrid = _keys(payload["hybrid"], {"summary", "filter_aware_traversal", "alopex_advantageous_selectivities"}, "hybrid")
    hybrid_rows = _rows(hybrid["summary"], "hybrid_summary", label="arm")
    if {(r["selectivity"], r["arm"]) for r in hybrid_rows} != {(s, a) for s in HYBRID_SELECTIVITIES for a in ARMS}:
        raise ValueError("hybrid comparison matrix is incomplete")
    if type(hybrid["filter_aware_traversal"]) is not bool or not isinstance(hybrid["alopex_advantageous_selectivities"], list):
        raise ValueError("hybrid contract is invalid")
    for value in hybrid["alopex_advantageous_selectivities"]:
        _number(value, "hybrid selectivity")
    scale = _keys(payload["scale"], {"dataset", "sha256", "requested_sizes", "max_n", "limits", "build_results", "runs", "results", "brute_force_crossover", "gap_trend_vs_flat"}, "scale")
    if scale["dataset"] != "glove-100-angular" or scale["sha256"] != GLOVE_SHA256 or scale["requested_sizes"] != [10000, 50000, 200000, 1000000] or scale["max_n"] != 50000:
        raise ValueError("scale dataset contract does not match")
    if not isinstance(scale["limits"], list) or {(_keys(r, {"dataset_size"}, "scale limit")["dataset_size"]) for r in scale["limits"]} != {200000, 1000000}:
        raise ValueError("scale limits are incomplete")
    scale_builds = _rows(scale["build_results"], "scale_build_results")
    scale_runs = _rows(scale["runs"], "runs")
    _check_timed_runs(scale_runs, scale=True)
    scale_rows = _rows(scale["results"], "scale_results")
    matrix = {(size, engine) for size in SCALE_SIZES for engine in SCALE_ENGINES}
    if {(r["dataset_size"], r["engine"]) for r in scale_builds} != matrix or {(r["dataset_size"], r["engine"]) for r in scale_rows} != matrix:
        raise ValueError("scale comparison matrix is incomplete")
    analysis = analyze_scale(scale_rows, requested_sizes=(10000, 50000, 200000, 1000000))
    if scale["brute_force_crossover"] != analysis["brute_force_crossover"] or scale["gap_trend_vs_flat"] != analysis["gap_trend_vs_flat"]:
        raise ValueError("scale crossover analysis does not match measured rows")
    for result in scale_rows:
        engine = "faiss-flat-exact" if result["engine"] == "flat" else result["engine"]
        measured = [r for r in scale_runs if r["dataset_size"] == result["dataset_size"] and r["engine"] == engine]
        expected_curve = summarize_by_engine(measured)
        if result["curve"] != [{key: row[key] for key in ("engine", *ROW_FIELDS["scale_curve"])} for row in expected_curve]:
            raise ValueError("scale curve does not match measured runs")
        eligible = [r for r in expected_curve if r["median_recall_at_10"] >= 0.95]
        fastest = max(eligible, key=lambda r: r["median_queries_per_second"], default=None)
        if result["qps_at_recall_095"] != (fastest["median_queries_per_second"] if fastest else 0.0) or result["ef_search_at_recall_095"] != (fastest["ef_search"] if fastest else None) or result["recall_at_selected_setting"] != (fastest["median_recall_at_10"] if fastest else None):
            raise ValueError("scale selected setting does not match measured runs")
    best = payload["best_at_recall"]
    if not isinstance(best, dict) or set(best) != {"0.95", "0.99"} or best != best_at_recall(summary):
        raise ValueError("recall threshold summary does not match benchmark rows")
    _number(payload["median_queries_per_second"], "median QPS")
    if payload["median_queries_per_second"] != statistics.median(r["queries_per_second"] for r in runs):
        raise ValueError("median QPS does not match measured runs")
    markdown = (root / "hnsw-diagnostic.md").read_bytes()
    if hashlib.sha256(markdown).hexdigest() != payload["markdown_sha256"]:
        raise ValueError("Markdown hash does not match canonical JSON")
    if markdown.decode("utf-8") != render_public_markdown(payload):
        raise ValueError("Markdown does not match canonical JSON rendering")
    return payload


def render_status_markdown(status: dict[str, object]) -> str:
    lines = ["# HNSW benchmark run", "", f"Status: `{status['status']}`", "", f"Source commit: `{status['source_commit']}`", f"Run: `{status['run_id']}` attempt `{status['run_attempt']}`", "", "| stage | outcome |", "|---|---|"]
    lines.extend(f"| {name} | {status['stages'][name]} |" for name in STAGES)
    return "\n".join(lines) + "\n"


def validate_artifacts(root: Path, *, version: str, commit: str, run_id: int | None = None, run_attempt: int | None = None) -> None:
    status = _load(root / "benchmark-status.json")
    _keys(status, {"schema", "source_commit", "run_id", "run_attempt", "status", "failure_stage", "stages"}, "run status")
    if status["schema"] != "alopex.hnsw-diagnostic-run/v1" or status["source_commit"] != commit:
        raise ValueError("run status identity does not match")
    if type(status["run_id"]) is not int or status["run_id"] <= 0 or type(status["run_attempt"]) is not int or status["run_attempt"] <= 0:
        raise ValueError("invalid run status identity")
    if run_id is not None and (status["run_id"], status["run_attempt"]) != (run_id, run_attempt):
        raise ValueError("run status identity does not match")
    stages = _keys(status["stages"], set(STAGES), "run stages")
    if any(value not in STAGE_OUTCOMES for value in stages.values()):
        raise ValueError("invalid run stage result")
    failure = next((name for name in STAGES if stages[name] == "failure"), None)
    incomplete = next((name for name in STAGES if stages[name] != "success"), None)
    expected = "failure" if failure else "incomplete" if incomplete else "success"
    if status["status"] != expected or status["failure_stage"] != (failure or incomplete):
        raise ValueError("run status disagrees with stages")
    if (root / "benchmark-status.md").read_text(encoding="utf-8") != render_status_markdown(status):
        raise ValueError("run status Markdown disagrees with JSON")
    _exact_files(root, STATUS_PAIR | (PUBLIC_PAIR if expected == "success" else frozenset()))
    if expected == "success":
        validate_public_pair(root, version=version, commit=commit, run_id=status["run_id"], run_attempt=status["run_attempt"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--run-attempt", type=int)
    parser.add_argument("--public-pair", action="store_true")
    args = parser.parse_args()
    if (args.run_id is None) != (args.run_attempt is None):
        parser.error("run ID and attempt must be provided together")
    if args.public_pair:
        validate_public_pair(args.root, version=args.version, commit=args.commit, run_id=args.run_id, run_attempt=args.run_attempt)
    else:
        validate_artifacts(args.root, version=args.version, commit=args.commit, run_id=args.run_id, run_attempt=args.run_attempt)


if __name__ == "__main__":
    main()
