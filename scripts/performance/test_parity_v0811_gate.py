import json
import tempfile
import unittest
from pathlib import Path

from scripts.performance.parity_v0811_gate import evaluate, percentile, validate_document
from scripts.performance.parity_v0811_measure import normalize_hnsw, summarize_latencies


class PerformanceParityGateTests(unittest.TestCase):
    def test_measurement_summary_reports_percentiles_and_throughput(self):
        summary = summarize_latencies([0.001, 0.002, 0.004, 0.003], rows=100)
        self.assertEqual(summary["latency_p50_ms"], 2.0)
        self.assertEqual(summary["latency_p95_ms"], 4.0)
        self.assertEqual(summary["rows_per_second"], 50000.0)

    def test_hnsw_normalizer_pairs_same_recall_setting(self):
        artifact = {
            "dataset": {"source_sha256": "a" * 64},
            "summary": [
                {
                    "engine": engine,
                    "ef_search": 64,
                    "median_tie_aware_recall_at_10": 1.0,
                    "median_queries_per_second": qps,
                    "median_query_latency_p50_us": latency,
                    "median_query_latency_p95_us": latency * 2,
                    "median_query_latency_p99_us": latency * 3,
                }
                for engine, qps, latency in (
                    ("alopex-hnsw", 1000.0, 100.0),
                    ("hnswlib", 2000.0, 50.0),
                )
            ],
            "builds": [
                {
                    "engine": engine,
                    "build_time_seconds": build,
                    "index_size_bytes": size,
                    "peak_rss_bytes": size * 2,
                    "update_latency_ms": 1.0,
                    "delete_latency_ms": 2.0,
                    "reopen_latency_ms": 3.0,
                }
                for engine, build, size in (("alopex-hnsw", 2.0, 2000), ("hnswlib", 1.0, 1000))
            ],
        }

        row = normalize_hnsw(artifact, "a" * 64)

        self.assertEqual(row["subject"]["recall_at_10"], 1.0)
        self.assertEqual(row["subject"]["build_latency_ms"], 2000.0)
        self.assertEqual(row["reference"]["queries_per_second"], 2000.0)

    def test_percentile_uses_sorted_nearest_rank(self):
        self.assertEqual(percentile([4.0, 1.0, 3.0, 2.0], 0.50), 2.0)
        self.assertEqual(percentile([4.0, 1.0, 3.0, 2.0], 0.95), 4.0)

    def test_gate_accepts_paired_metrics_inside_thresholds(self):
        contract = {
            "kind": "tabular",
            "reference_revision": "project@v1.0.0",
            "fixture": {"dataset_sha256": "a" * 64},
            "metrics": [
                "latency_p50_ms",
                "latency_p95_ms",
                "rows_per_second",
                "peak_rss_bytes",
            ],
            "thresholds": {
                "max_latency_ratio": 2.0,
                "min_throughput_ratio": 0.5,
                "max_peak_memory_ratio": 2.0,
            },
        }
        measurement = {
            "reference_revision": "project@v1.0.0",
            "dataset_sha256": "a" * 64,
            "subject": {
                "latency_p50_ms": 1.5,
                "latency_p95_ms": 1.8,
                "rows_per_second": 700.0,
                "peak_rss_bytes": 1500.0,
            },
            "reference": {
                "latency_p50_ms": 1.0,
                "latency_p95_ms": 1.0,
                "rows_per_second": 1000.0,
                "peak_rss_bytes": 1000.0,
            },
        }

        self.assertEqual(evaluate("example", contract, measurement), [])

    def test_gate_rejects_wrong_revision_missing_metric_and_regression(self):
        contract = {
            "kind": "sql",
            "reference_revision": "project@v1.0.0",
            "fixture": {"dataset_sha256": "a" * 64},
            "metrics": [
                "latency_p50_ms",
                "queries_per_second",
                "peak_rss_bytes",
                "temporary_io_bytes",
            ],
            "thresholds": {
                "max_latency_ratio": 2.0,
                "min_throughput_ratio": 0.5,
                "max_peak_memory_ratio": 2.0,
                "max_temporary_io_ratio": 2.0,
            },
        }
        measurement = {
            "reference_revision": "project@main",
            "dataset_sha256": "a" * 64,
            "subject": {
                "latency_p50_ms": 3.0,
                "queries_per_second": 400.0,
                "peak_rss_bytes": 1000.0,
            },
            "reference": {
                "latency_p50_ms": 1.0,
                "queries_per_second": 1000.0,
                "peak_rss_bytes": 1000.0,
            },
        }

        errors = evaluate("example", contract, measurement)

        self.assertTrue(any("reference_revision" in error for error in errors))
        self.assertTrue(any("temporary_io_bytes" in error for error in errors))
        self.assertTrue(any("latency_p50_ms ratio" in error for error in errors))
        self.assertTrue(any("queries_per_second ratio" in error for error in errors))

    def test_gate_rejects_measurements_from_another_runner(self):
        contracts = {
            "runner_profile": {
                "os": "ubuntu-24.04",
                "cpu_model": "fixed-cpu",
                "logical_cpu_count": 8,
                "cpu_affinity": [0],
                "memory_bytes": 1024,
                "build_profile": "release",
                "thread_count": 1,
            },
            "contracts": {},
        }
        measurements = {
            "environment": {
                "os": "ubuntu-24.04",
                "cpu_model": "other-cpu",
                "logical_cpu_count": 8,
                "cpu_affinity": [0],
                "memory_bytes": 1024,
                "build_profile": "release",
                "thread_count": 1,
                "alopex_version": "0.8.11",
                "alopex_revision": "a" * 40,
                "alopex_tree_sha256": "b" * 64,
                "kernel": "6.0",
                "python_version": "3.11.11",
            },
            "results": [],
        }

        errors = validate_document(contracts, measurements, "curated")

        self.assertEqual(errors, ["environment: cpu_model does not match the runner profile"])

    def test_gate_uses_metric_specific_latency_budget(self):
        contract = {
            "reference_revision": "project@v1.0.0",
            "fixture": {"dataset_sha256": "a" * 64},
            "metrics": ["delete_latency_ms"],
            "thresholds": {
                "max_latency_ratio": 2.0,
                "min_throughput_ratio": 0.5,
                "max_peak_memory_ratio": 2.0,
                "max_ratio_by_metric": {"delete_latency_ms": 4.0},
            },
        }
        measurement = {
            "reference_revision": "project@v1.0.0",
            "dataset_sha256": "a" * 64,
            "subject": {"delete_latency_ms": 3.0},
            "reference": {"delete_latency_ms": 1.0},
        }

        self.assertEqual(evaluate("example", contract, measurement), [])


if __name__ == "__main__":
    unittest.main()
