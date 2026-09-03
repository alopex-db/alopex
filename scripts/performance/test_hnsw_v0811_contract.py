import json
import tempfile
import unittest
from pathlib import Path

from scripts.performance.hnsw_v0811_contract import (
    DATASET_SIZE,
    DIMENSION,
    analyze_scale,
    decompose_latency,
    load_amazon_products,
    recall_at_k,
    summarize_by_engine,
    tie_aware_recall_at_k,
    write_artifacts,
)


class HnswDiagnosticContractTests(unittest.TestCase):
    def test_reference_dataset_preprocessing_keeps_meaningful_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "products.csv"
            source.write_text(
                "Product Name,Category,Selling Price,About Product,Product Description\n"
                'Headphones,"Electronics | Audio","$19.99",Wireless over ear,Clear sound\n'
                'No category,,"$5.00",Useful description,More words here\n'
                'Too short,Toys & Games,"$3.00",x,y\n',
                encoding="utf-8",
            )
            products = load_amazon_products(source)

        self.assertEqual(products["name"].tolist(), ["Headphones"])
        self.assertEqual(products["category"].tolist(), ["Electronics"])
        self.assertEqual(products["price"].tolist(), [19.99])
        self.assertIn("Wireless over ear", products["text"].iloc[0])

    def test_recall_and_engine_medians_follow_the_shared_contract(self):
        self.assertEqual(recall_at_k([[1, 2], [3, 4]], [[2, 5], [3, 6]]), 0.5)
        self.assertEqual(tie_aware_recall_at_k([[1, 3]], [[1, 2, 3]], 2), 1.0)
        summary = summarize_by_engine(
            [
                {
                    "engine": "a",
                    "ef_search": 16,
                    "queries_per_second": 10.0,
                    "recall_at_10": 0.9,
                },
                {
                    "engine": "a",
                    "ef_search": 16,
                    "queries_per_second": 30.0,
                    "recall_at_10": 1.0,
                },
                {
                    "engine": "a",
                    "ef_search": 16,
                    "queries_per_second": 20.0,
                    "recall_at_10": 0.95,
                },
            ]
        )
        self.assertEqual(summary[0]["median_queries_per_second"], 20.0)
        self.assertEqual(summary[0]["median_recall_at_10"], 0.95)

    def test_latency_decomposition_reports_fixed_and_exploration_costs(self):
        rows = decompose_latency(
            [
                {"engine": "a", "ef_search": 16, "median_latency_us": 12.0},
                {"engine": "a", "ef_search": 32, "median_latency_us": 20.0},
                {"engine": "a", "ef_search": 64, "median_latency_us": 36.0},
            ],
            {"a": 3.0},
        )
        self.assertAlmostEqual(rows[0]["regression_intercept_us"], 4.0)
        self.assertAlmostEqual(rows[0]["slope_us_per_ef"], 0.5)
        self.assertEqual(rows[0]["fixed_lower_bound_us"], 3.0)
        self.assertEqual(rows[0]["exploration_residual_us"], 9.0)

    def test_scale_analysis_records_crossovers_trends_and_limits(self):
        analysis = analyze_scale(
            [
                {"dataset_size": 10_000, "engine": "flat", "qps_at_recall_095": 100.0},
                {"dataset_size": 10_000, "engine": "alopex", "qps_at_recall_095": 80.0},
                {"dataset_size": 50_000, "engine": "flat", "qps_at_recall_095": 20.0},
                {"dataset_size": 50_000, "engine": "alopex", "qps_at_recall_095": 90.0},
            ],
            requested_sizes=(10_000, 50_000, 200_000),
        )
        self.assertEqual(analysis["brute_force_crossover"]["alopex"], 50_000)
        self.assertEqual(analysis["gap_trend_vs_flat"]["alopex"], "widens")
        self.assertEqual(analysis["limits"][0]["dataset_size"], 200_000)

    def test_artifacts_are_reproducible_and_machine_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            write_artifacts(
                Path(directory),
                [
                    {
                        "engine": "alopex-hnsw",
                        "dataset_size": DATASET_SIZE,
                        "dimension": DIMENSION,
                        "query_count": 10000,
                        "seed": 42,
                        "duration_seconds": 2.0,
                        "queries_per_second": 5000.0,
                        "checksum": None,
                        "recall_at_10": 1.0,
                        "latency_ms": 0.2,
                        "query_latency_p50_us": 190.0,
                        "query_latency_p95_us": 240.0,
                        "query_latency_p99_us": 280.0,
                        "ef_search": 64,
                        "recall_by_ef_search": {"64": 1.0},
                        "latency_by_ef_search_ms": {"64": 0.2},
                        "fixed_latency_ms": 0.1,
                        "exploration_latency_ms": 0.1,
                    }
                ]
                * 3,
                release_version="0.8.11",
                builds=[
                    {
                        "engine": "alopex-hnsw",
                        "build_time_seconds": 1.0,
                        "index_size_bytes": 1024,
                        "peak_rss_bytes": 2048,
                        "update_latency_ms": 0.1,
                        "delete_latency_ms": 0.1,
                        "reopen_latency_ms": 0.2,
                    }
                ],
                diagnostics={
                    "recall_investigation": {"conclusion": "boundary tie"},
                    "fixed_cost_runs": [{"engine": "alopex-hnsw", "latency_us": 1.0}],
                    "latency_decomposition": [
                        {
                            "engine": "alopex-hnsw",
                            "ef_search": 64,
                            "actual_latency_us": 2.0,
                        }
                    ],
                    "hybrid": {
                        "runs": [
                            {
                                "arm": "alopex-sql-hnsw-postfilter",
                                "selectivity": 0.01,
                                "latency_p50_us": 3.0,
                            }
                        ],
                        "alopex_advantageous_selectivities": [0.01],
                    },
                },
                scale={
                    "results": [
                        {
                            "dataset_size": 10_000,
                            "engine": "alopex-hnsw",
                            "qps_at_recall_095": 10.0,
                            "curve": [],
                        }
                    ],
                    "brute_force_crossover": {"alopex-hnsw": 10_000},
                    "limits": [],
                },
            )
            self.assertTrue((Path(directory) / "hnsw-diagnostic.raw.json").is_file())
            self.assertTrue((Path(directory) / "hnsw-diagnostic.json").is_file())
            self.assertTrue((Path(directory) / "hnsw-diagnostic.csv").is_file())
            self.assertTrue((Path(directory) / "hnsw-diagnostic.md").is_file())
            self.assertTrue(
                (Path(directory) / "hnsw-latency-decomposition.csv").is_file()
            )
            self.assertTrue((Path(directory) / "hnsw-hybrid.csv").is_file())
            self.assertTrue((Path(directory) / "hnsw-scale.csv").is_file())
            payload = json.loads((Path(directory) / "hnsw-diagnostic.json").read_text())
            self.assertEqual(
                payload["contract"]["metrics"],
                [
                    "recall_at_10",
                    "tie_aware_recall_at_10",
                    "query_latency_p50_us",
                    "query_latency_p95_us",
                    "query_latency_p99_us",
                    "native_search_latency_us",
                    "python_binding_residual_us",
                    "queries_per_second",
                    "build_time_seconds",
                    "index_size_bytes",
                    "peak_rss_bytes",
                    "update_latency_ms",
                    "delete_latency_ms",
                    "reopen_latency_ms",
                ],
            )
            self.assertEqual(payload["release_version"], "0.8.11")
            self.assertIn("engine", payload["runs"][0])
            self.assertEqual(
                payload["recall_investigation"]["conclusion"], "boundary tie"
            )

    def test_post_release_workflow_uses_exact_wheel_and_keeps_environment_evidence(
        self,
    ):
        workflow = (
            Path(__file__).resolve().parents[2]
            / ".github/workflows/post-release-hnsw.yml"
        ).read_text(encoding="utf-8")
        self.assertIn('default: "v0.8.11"', workflow)
        self.assertIn('"alopex==${version}"', workflow)
        self.assertIn("--only-binary=:all:", workflow)
        self.assertIn("--release-version", workflow)
        self.assertIn("artifacts-environment.txt", workflow)
        self.assertIn("31764184/archive.zip", workflow)
        self.assertIn('"faiss-cpu==1.15.0"', workflow)
        self.assertIn('"hnswlib==0.8.0"', workflow)
        self.assertIn('"h5py==3.15.1"', workflow)
        self.assertIn("glove-100-angular.hdf5", workflow)
        self.assertIn(
            "544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4", workflow
        )
        self.assertIn("--max-scale-n 50000", workflow)
        self.assertNotIn("--baseline-only", workflow)
        self.assertIn('OMP_NUM_THREADS: "1"', workflow)
        self.assertIn('RAYON_NUM_THREADS: "1"', workflow)
        self.assertNotIn("  release:\n", workflow)
        release_workflow = (
            Path(__file__).resolve().parents[2]
            / ".github/workflows/alopex-py-release.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("post-release-hnsw:", release_workflow)
        self.assertIn("needs: [final-release-join]", release_workflow)
        self.assertIn(
            "uses: ./.github/workflows/post-release-hnsw.yml", release_workflow
        )
        parity_workflow = (
            Path(__file__).resolve().parents[2]
            / ".github/workflows/parity-performance.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("--baseline-only", parity_workflow)


if __name__ == "__main__":
    unittest.main()
