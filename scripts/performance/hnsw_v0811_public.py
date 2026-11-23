"""Build the publishable HNSW pair from typed in-memory observations.

This module deliberately has no reader for the debug artifacts. Every string in
the output is an enumerated label or a validated identity, never a measured
environment value or a diagnostic message.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
from pathlib import Path

try:
    from .hnsw_v0811_contract import (
        DATASET_SIZE, DIMENSION, GLOVE_SHA256, MEASUREMENT_RUNS,
        MIN_DURATION_SECONDS, QUERY_COUNT, best_at_recall, summarize_by_engine,
    )
except ImportError:
    from hnsw_v0811_contract import (
        DATASET_SIZE, DIMENSION, GLOVE_SHA256, MEASUREMENT_RUNS,
        MIN_DURATION_SECONDS, QUERY_COUNT, best_at_recall, summarize_by_engine,
    )

SCHEMA = "alopex.hnsw-diagnostic-public/v4"
ENGINES = {"alopex-hnsw", "hnswlib", "faiss-flat-exact", "faiss-hnsw", "flat"}
ARMS = {"alopex-sql-hnsw-postfilter", "sqlite-hnswlib", "filtered-exact"}
DATASET_SHA256 = "ec7ad773a70e654d65ee3757748fe99d614be24ad087330e437bcc84cda85291"

# Only these numeric observations can cross the public boundary. There is no
# generic dictionary serializer and no pass-through of arbitrary row fields.
ROW_FIELDS = {
    "runs": ("ef_search", "run", "dataset_size", "dimension", "query_count", "duration_seconds", "queries_per_second", "latency_us", "native_search_latency_us", "python_binding_residual_us", "query_latency_p50_us", "query_latency_p95_us", "query_latency_p99_us", "recall_at_10", "tie_aware_recall_at_10"),
    "builds": ("build_time_seconds", "index_size_bytes", "peak_rss_bytes", "update_latency_ms", "delete_latency_ms", "reopen_latency_ms", "node_count"),
    "summary": ("ef_search", "median_recall_at_10", "median_tie_aware_recall_at_10", "median_queries_per_second", "median_latency_us", "median_query_latency_p50_us", "median_query_latency_p95_us", "median_query_latency_p99_us"),
    "recall_ceiling": ("ef_search", "query_count", "recall_at_10", "tie_aware_recall_at_10"),
    "recall_configurations": ("m", "ef_construction", "ef_search", "recall_at_10", "tie_aware_recall_at_10"),
    "fixed_cost_runs": ("vectors", "k", "ef_search", "run", "query_count", "duration_seconds", "latency_us"),
    "latency_decomposition": ("ef_search", "actual_latency_us", "fixed_lower_bound_us", "exploration_residual_us", "regression_intercept_us", "slope_us_per_ef"),
    "hybrid_summary": ("selectivity", "median_latency_p50_us", "median_latency_p95_us", "filtered_top_k_accuracy", "median_overfetch_amplification"),
    "scale_build_results": ("dataset_size", "build_time_seconds", "index_size_bytes", "peak_rss_bytes", "node_count"),
    "scale_results": ("dataset_size", "qps_at_recall_095", "ef_search_at_recall_095", "recall_at_selected_setting"),
    "scale_curve": ("ef_search", "median_recall_at_10", "median_tie_aware_recall_at_10", "median_queries_per_second", "median_latency_us"),
}


def _number(value: object) -> int | float | None:
    if value is None:
        return None
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError("public observation is not a finite number")
    return value


def _rows(rows: object, kind: str, *, label: str = "engine") -> list[dict[str, object]]:
    if not isinstance(rows, list):
        raise ValueError("public observation rows are missing")
    result = []
    for row in rows:
        if not isinstance(row, dict) or row.get(label) not in (ARMS if label == "arm" else ENGINES):
            raise ValueError("public observation label is invalid")
        item = {label: row[label]}
        for key in ROW_FIELDS[kind]:
            if key not in row:
                raise ValueError("public observation metric is missing")
            item[key] = _number(row[key])
            if item[key] is None and not ((kind == "runs" and key in ("native_search_latency_us", "python_binding_residual_us")) or (kind == "scale_results" and key in ("ef_search_at_recall_095", "recall_at_selected_setting")) or (kind == "builds" and key in ("update_latency_ms", "delete_latency_ms", "reopen_latency_ms"))):
                raise ValueError("public observation metric is missing")
        if kind == "hybrid_summary":
            if type(row.get("returns_k")) is not bool:
                raise ValueError("public observation boolean is invalid")
            item["returns_k"] = row["returns_k"]
        if kind == "scale_results":
            item["curve"] = _rows(row.get("curve"), "scale_curve")
        result.append(item)
    return result


def build_public_payload(
    *, release_version: str, source_commit: str, run_id: int,
    run_attempt: int, runs: list[dict[str, object]], dataset: dict[str, object],
    builds: list[dict[str, object]], recall_ceiling: list[dict[str, object]],
    diagnostics: dict[str, object], scale: dict[str, object],
) -> dict[str, object]:
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", release_version) is None:
        raise ValueError("invalid release version")
    if re.fullmatch(r"[0-9a-f]{40}", source_commit) is None:
        raise ValueError("invalid source commit")
    if type(run_id) is not int or run_id <= 0 or type(run_attempt) is not int or run_attempt <= 0:
        raise ValueError("invalid run identity")
    if dataset.get("source_sha256") != DATASET_SHA256 or scale.get("sha256") != GLOVE_SHA256:
        raise ValueError("dataset checksum does not match public contract")
    public_runs = _rows(runs, "runs")
    summary = _rows(summarize_by_engine(public_runs), "summary")
    investigation = diagnostics["recall_investigation"]
    metric_agreement = investigation["exact_metric_agreement"]
    hybrid = diagnostics["hybrid"]
    if type(investigation.get("index_count_matches_input")) is not bool:
        raise ValueError("invalid recall count result")
    if type(hybrid.get("filter_aware_traversal")) is not bool:
        raise ValueError("invalid hybrid traversal result")
    public = {
        "schema": SCHEMA,
        "release_version": release_version,
        "source_commit": source_commit,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "dataset": {
            "source_sha256": DATASET_SHA256,
            "clean_rows": _number(dataset["clean_rows"]),
            "dimension": _number(dataset["dimension"]),
            "query_rows": _number(dataset["query_rows"]),
            "explained_variance": _number(dataset["explained_variance"]),
        },
        "contract": {
            "dataset_size": DATASET_SIZE, "dimension": DIMENSION,
            "min_duration_seconds": MIN_DURATION_SECONDS,
            "min_queries": QUERY_COUNT, "runs": MEASUREMENT_RUNS,
            "warmup": "one complete query cycle per engine/setting",
            "metrics": ["recall_at_10", "tie_aware_recall_at_10", "query_latency_p50_us", "query_latency_p95_us", "query_latency_p99_us", "native_search_latency_us", "python_binding_residual_us", "queries_per_second"],
            "build_metrics": ["build_time_seconds", "index_size_bytes", "peak_rss_bytes", "update_latency_ms", "delete_latency_ms", "reopen_latency_ms"],
        },
        "builds": _rows(builds, "builds"),
        "runs": public_runs,
        "summary": summary,
        "recall_ceiling": _rows(recall_ceiling, "recall_ceiling"),
        "recall_investigation": {
            "index_count": _number(investigation["index_count"]),
            "input_count": _number(investigation["input_count"]),
            "index_count_matches_input": investigation["index_count_matches_input"],
            "self_match_rate": _number(investigation["self_match_rate"]),
            "self_match_ef_search": _number(investigation["self_match_ef_search"]),
            "recall_configurations": _rows(investigation["ef_construction_and_m"], "recall_configurations"),
            "exact_metric_agreement": {
                "rows": _number(metric_agreement["rows"]),
                "queries": _number(metric_agreement["queries"]),
                "normalized": metric_agreement["normalized"],
                "l2_inner_product_top_10_equal": metric_agreement["l2_inner_product_top_10_equal"],
            },
        },
        "fixed_cost_runs": _rows(diagnostics["fixed_cost_runs"], "fixed_cost_runs"),
        "latency_decomposition": _rows(diagnostics["latency_decomposition"], "latency_decomposition"),
        "hybrid": {
            "summary": _rows(hybrid["summary"], "hybrid_summary", label="arm"),
            "filter_aware_traversal": hybrid["filter_aware_traversal"],
            "alopex_advantageous_selectivities": [_number(x) for x in hybrid["alopex_advantageous_selectivities"]],
        },
        "scale": {
            "dataset": "glove-100-angular", "sha256": GLOVE_SHA256,
            "requested_sizes": [10_000, 50_000, 200_000, 1_000_000],
            "max_n": _number(scale["max_n"]),
            "limits": [{"dataset_size": _number(row["dataset_size"])} for row in scale["limits"]],
            "build_results": _rows(scale["build_results"], "scale_build_results"),
            "runs": _rows(scale["runs"], "runs"),
            "results": _rows(scale["results"], "scale_results"),
            "brute_force_crossover": {engine: _number(scale["brute_force_crossover"][engine]) for engine in ("alopex-hnsw", "faiss-hnsw", "hnswlib")},
            "gap_trend_vs_flat": {engine: scale["gap_trend_vs_flat"][engine] for engine in ("alopex-hnsw", "faiss-hnsw", "hnswlib")},
        },
    }
    if any(value not in {"constant", "widens", "narrows"} for value in public["scale"]["gap_trend_vs_flat"].values()):
        raise ValueError("invalid scale trend")
    if any(type(public["recall_investigation"]["exact_metric_agreement"][key]) is not bool for key in ("normalized", "l2_inner_product_top_10_equal")):
        raise ValueError("invalid metric agreement result")
    public["best_at_recall"] = best_at_recall(summary)
    public["median_queries_per_second"] = _number(
        statistics.median(float(row["queries_per_second"]) for row in runs)
    )
    return public


def render_public_markdown(payload: dict[str, object]) -> str:
    lines = [
        "# HNSW public benchmark", "",
        f"Release: `{payload['release_version']}`; source: `{payload['source_commit']}`; run: `{payload['run_id']}` attempt `{payload['run_attempt']}`.",
        "", "## Comparison", "",
        "| engine | ef_search | recall@10 | tie-aware recall@10 | QPS | us/query |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    lines += [f"| {r['engine']} | {r['ef_search']} | {r['median_recall_at_10']:.4f} | {r['median_tie_aware_recall_at_10']:.4f} | {r['median_queries_per_second']:.1f} | {r['median_latency_us']:.1f} |" for r in payload["summary"]]
    lines += ["", "## Build", "", "| engine | seconds | index bytes | peak RSS bytes |", "|---|---:|---:|---:|"]
    lines += [f"| {r['engine']} | {r['build_time_seconds']:.2f} | {r['index_size_bytes']} | {r['peak_rss_bytes']} |" for r in payload["builds"]]
    agreement = payload["recall_investigation"]["exact_metric_agreement"]
    lines += ["", "## Exact metric agreement", "", f"Normalized vectors: `{str(agreement['normalized']).lower()}`; L2/IP top-10 equal: `{str(agreement['l2_inner_product_top_10_equal']).lower()}`; rows: `{agreement['rows']}`; queries: `{agreement['queries']}`."]
    lines += ["", "## Recall thresholds", "", "| threshold | engine | ef_search | recall@10 | QPS |", "|---:|---|---:|---:|---:|"]
    for threshold, winners in payload["best_at_recall"].items():
        for engine in sorted({r["engine"] for r in payload["summary"]}):
            row = winners.get(engine)
            ef = row["ef_search"] if row else "not met"
            recall = f"{row['median_recall_at_10']:.4f}" if row else "not met"
            qps = f"{row['median_queries_per_second']:.1f}" if row else "not met"
            lines.append(f"| {threshold} | {engine} | {ef} | {recall} | {qps} |")
    lines += ["", "## Hybrid", "", "| arm | selectivity | p50 us | p95 us | top-k accuracy |", "|---|---:|---:|---:|---:|"]
    lines += [f"| {r['arm']} | {r['selectivity']:.3f} | {r['median_latency_p50_us']:.1f} | {r['median_latency_p95_us']:.1f} | {r['filtered_top_k_accuracy']:.4f} |" for r in payload["hybrid"]["summary"]]
    lines += ["", "## Scale", "", "| N | engine | QPS @ recall ≥ 0.95 | recall |", "|---:|---|---:|---:|"]
    lines += [f"| {r['dataset_size']} | {r['engine']} | {r['qps_at_recall_095']:.1f} | {r['recall_at_selected_setting'] if r['recall_at_selected_setting'] is not None else 'not met'} |" for r in payload["scale"]["results"]]
    lines += ["", "## Scale crossover", "", "| engine | first N faster than flat | gap trend |", "|---|---:|---|"]
    lines += [f"| {engine} | {payload['scale']['brute_force_crossover'][engine] if payload['scale']['brute_force_crossover'][engine] is not None else 'not met'} | {payload['scale']['gap_trend_vs_flat'][engine]} |" for engine in ("alopex-hnsw", "faiss-hnsw", "hnswlib")]
    return "\n".join(lines) + "\n"


def write_public_pair(output: Path, payload: dict[str, object]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    markdown = render_public_markdown(payload)
    document = {**payload, "markdown_sha256": hashlib.sha256(markdown.encode()).hexdigest()}
    (output / "hnsw-diagnostic.md").write_text(markdown, encoding="utf-8")
    (output / "hnsw-diagnostic.json").write_text(json.dumps(document, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
