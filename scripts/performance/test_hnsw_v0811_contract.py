import json
import tempfile
import unittest
from pathlib import Path

from scripts.performance.hnsw_v0811_contract import (
    DATASET_SIZE,
    DIMENSION,
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
                {"engine": "a", "ef_search": 16, "queries_per_second": 10.0, "recall_at_10": 0.9},
                {"engine": "a", "ef_search": 16, "queries_per_second": 30.0, "recall_at_10": 1.0},
                {"engine": "a", "ef_search": 16, "queries_per_second": 20.0, "recall_at_10": 0.95},
            ]
        )
        self.assertEqual(summary[0]["median_queries_per_second"], 20.0)
        self.assertEqual(summary[0]["median_recall_at_10"], 0.95)

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
                        "ef_search": 64,
                        "recall_by_ef_search": {"64": 1.0},
                        "latency_by_ef_search_ms": {"64": 0.2},
                        "fixed_latency_ms": 0.1,
                        "exploration_latency_ms": 0.1,
                    }
                ]
                * 3,
                release_version="0.8.11",
            )
            self.assertTrue((Path(directory) / "hnsw-diagnostic.raw.json").is_file())
            self.assertTrue((Path(directory) / "hnsw-diagnostic.json").is_file())
            self.assertTrue((Path(directory) / "hnsw-diagnostic.csv").is_file())
            self.assertTrue((Path(directory) / "hnsw-diagnostic.md").is_file())
            payload = json.loads((Path(directory) / "hnsw-diagnostic.json").read_text())
            self.assertEqual(
                payload["contract"]["metrics"],
                [
                    "recall_at_10",
                    "tie_aware_recall_at_10",
                    "latency_us",
                    "queries_per_second",
                ],
            )
            self.assertEqual(payload["release_version"], "0.8.11")
            self.assertIn("engine", payload["runs"][0])

    def test_post_release_workflow_uses_exact_wheel_and_keeps_environment_evidence(self):
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
        self.assertIn('OMP_NUM_THREADS: "1"', workflow)
        self.assertNotIn("  release:\n", workflow)
        release_workflow = (
            Path(__file__).resolve().parents[2]
            / ".github/workflows/alopex-py-release.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("post-release-hnsw:", release_workflow)
        self.assertIn("needs: [final-release-join]", release_workflow)
        self.assertIn("uses: ./.github/workflows/post-release-hnsw.yml", release_workflow)


if __name__ == "__main__":
    unittest.main()
