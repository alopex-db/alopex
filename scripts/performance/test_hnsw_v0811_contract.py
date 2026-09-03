import json
import tempfile
import unittest
from pathlib import Path

from scripts.performance.hnsw_v0811_contract import DATASET_SIZE, DIMENSION, write_artifacts


class HnswDiagnosticContractTests(unittest.TestCase):
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
                    "latency_ms",
                    "queries_per_second",
                    "recall_by_ef_search",
                    "latency_by_ef_search_ms",
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


if __name__ == "__main__":
    unittest.main()
