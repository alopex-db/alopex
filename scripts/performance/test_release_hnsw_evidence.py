from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.performance.hnsw_v0811_contract import best_at_recall, render_markdown
from scripts.performance.release_hnsw_evidence import (
    REQUIRED_ARTIFACTS,
    validate_artifacts,
    validate_public_pair,
)


class ReleaseHnswEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        provenance = {
            "source_commit": "a" * 40,
            "python": "3.11.11",
            "platform": "Linux",
            "cpu_model": "Test CPU",
            "cpu_affinity": [0],
            "dependencies": {"alopex": "0.8.12"},
        }
        dataset = {
            "source": "Amazon Product Dataset 2020",
            "source_sha256": "ec7ad773a70e654d65ee3757748fe99d614be24ad087330e437bcc84cda85291",
            "clean_rows": 9171,
            "dimension": 128,
        }
        payload = {
            "schema": "alopex.hnsw-diagnostic/v3",
            "release_version": "0.8.12",
            "provenance": provenance,
            "dataset": dataset,
            "contract": {
                "dataset_size": 9171,
                "dimension": 128,
                "warmup": "one complete query cycle per engine/setting",
                "min_duration_seconds": 2.0,
                "min_queries": 10000,
                "runs": 3,
                "metrics": [
                    "recall_at_10",
                    "tie_aware_recall_at_10",
                    "queries_per_second",
                    "build_time_seconds",
                    "index_size_bytes",
                    "peak_rss_bytes",
                ],
            },
            "builds": [
                {
                    "engine": engine,
                    "build_time_seconds": 1.0,
                    "index_size_bytes": 1024,
                    "peak_rss_bytes": 2048,
                }
                for engine in (
                    "alopex-hnsw",
                    "hnswlib",
                    "faiss-flat-exact",
                    "faiss-hnsw",
                )
            ],
            "summary": [
                {
                    "engine": engine,
                    "ef_search": 64,
                    "median_recall_at_10": 0.99,
                    "median_tie_aware_recall_at_10": 0.99,
                    "median_queries_per_second": 1000.0,
                    "median_latency_us": 100.0,
                }
                for engine in (
                    "alopex-hnsw",
                    "hnswlib",
                    "faiss-flat-exact",
                    "faiss-hnsw",
                )
            ],
            "recall_ceiling": [],
            "recall_investigation": {},
            "latency_decomposition": [],
            "scale": {
                "dataset": "glove-100-angular",
                "sha256": "544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4",
                "requested_sizes": [10_000, 50_000, 200_000, 1_000_000],
                "max_n": 50_000,
                "limits": [
                    {"dataset_size": 200_000, "reason": "configured limit"},
                    {"dataset_size": 1_000_000, "reason": "configured limit"},
                ],
                "results": [
                    {
                        "dataset_size": size,
                        "engine": engine,
                        "build_time_seconds": 1.0,
                        "index_size_bytes": 1024,
                        "peak_rss_bytes": 2048,
                        "qps_at_recall_095": 1000.0,
                        "ef_search_at_recall_095": 64,
                        "recall_at_selected_setting": 0.99,
                    }
                    for size in (10_000, 50_000)
                    for engine in ("alopex-hnsw", "hnswlib", "faiss-hnsw", "flat")
                ]
            },
            "hybrid": {
                "summary": [
                    {
                        "selectivity": selectivity,
                        "arm": arm,
                        "median_latency_p50_us": 100.0,
                        "median_latency_p95_us": 150.0,
                        "filtered_top_k_accuracy": 1.0,
                        "median_overfetch_amplification": 1.0,
                        "returns_k": True,
                    }
                    for selectivity in (0.001, 0.01, 0.05, 0.2, 1.0)
                    for arm in (
                        "alopex-sql-hnsw-postfilter",
                        "sqlite-hnswlib",
                        "filtered-exact",
                    )
                ]
            },
        }
        payload["best_at_recall"] = best_at_recall(payload["summary"])
        markdown = render_markdown(payload).encode("utf-8")
        payload["markdown_sha256"] = hashlib.sha256(markdown).hexdigest()
        raw = {
            "schema": "alopex.hnsw-diagnostic-raw/v3",
            "dataset": dataset,
            "provenance": provenance,
        }
        (self.root / "hnsw-diagnostic.json").write_text(
            json.dumps(payload), encoding="utf-8"
        )
        (self.root / "hnsw-diagnostic.raw.json").write_text(
            json.dumps(raw), encoding="utf-8"
        )
        (self.root / "hnsw-diagnostic.md").write_bytes(markdown)
        for name in (
            "hnsw-diagnostic.csv",
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
        ):
            content = (
                "cpu_model=Test CPU\nbenchmark_core=0\nbenchmark_affinity=0\n"
                if name == "environment.txt"
                else "evidence\n"
            )
            (self.root / name).write_text(content, encoding="utf-8")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_accepts_complete_exact_sha_release_snapshot(self) -> None:
        validate_artifacts(self.root, version="0.8.12", commit="a" * 40)

    def test_accepts_the_retained_public_pair_after_actions_artifacts_expire(self) -> None:
        for name in REQUIRED_ARTIFACTS:
            if name not in {"hnsw-diagnostic.json", "hnsw-diagnostic.md"}:
                (self.root / name).unlink()

        validate_public_pair(self.root, version="0.8.12", commit="a" * 40)

    def test_rejects_wrong_scale_dataset_and_incomplete_environment(self) -> None:
        path = self.root / "hnsw-diagnostic.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["scale"]["sha256"] = "0" * 64
        path.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "scale dataset"):
            validate_artifacts(self.root, version="0.8.12", commit="a" * 40)

        payload["scale"]["sha256"] = (
            "544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4"
        )
        path.write_text(json.dumps(payload), encoding="utf-8")
        (self.root / "environment.txt").write_text(
            "cpu_model=Test CPU\nbenchmark_core=0\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "environment metadata"):
            validate_artifacts(self.root, version="0.8.12", commit="a" * 40)

    def test_rejects_public_pair_without_canonical_cpu_affinity(self) -> None:
        path = self.root / "hnsw-diagnostic.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["provenance"]["cpu_affinity"] = []
        path.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "canonical CPU"):
            validate_public_pair(self.root, version="0.8.12", commit="a" * 40)

    def test_rejects_threshold_summary_that_disagrees_with_measurements(self) -> None:
        path = self.root / "hnsw-diagnostic.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["best_at_recall"]["0.99"] = {}
        path.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "threshold summary does not match"):
            validate_public_pair(self.root, version="0.8.12", commit="a" * 40)

    def test_rejects_public_markdown_without_threshold_comparison(self) -> None:
        path = self.root / "hnsw-diagnostic.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        markdown = b"# HNSW diagnostic\nCPU: `Test CPU`; CPU affinity: `[0]`.\n"
        payload["markdown_sha256"] = hashlib.sha256(markdown).hexdigest()
        path.write_text(json.dumps(payload), encoding="utf-8")
        (self.root / "hnsw-diagnostic.md").write_bytes(markdown)
        with self.assertRaisesRegex(ValueError, "canonical JSON rendering"):
            validate_public_pair(self.root, version="0.8.12", commit="a" * 40)

    def test_rejects_wrong_identity_changed_markdown_and_missing_artifact(self) -> None:
        with self.subTest("version"):
            with self.assertRaisesRegex(ValueError, "release version"):
                validate_artifacts(self.root, version="0.8.13", commit="a" * 40)
        with self.subTest("commit"):
            with self.assertRaisesRegex(ValueError, "source commit"):
                validate_artifacts(self.root, version="0.8.12", commit="b" * 40)
        with self.subTest("markdown"):
            (self.root / "hnsw-diagnostic.md").write_text("changed", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Markdown hash"):
                validate_artifacts(self.root, version="0.8.12", commit="a" * 40)
        with self.subTest("missing"):
            payload = json.loads(
                (self.root / "hnsw-diagnostic.json").read_text(encoding="utf-8")
            )
            (self.root / "hnsw-diagnostic.md").write_text(
                render_markdown(payload), encoding="utf-8"
            )
            (self.root / "environment.txt").unlink()
            with self.assertRaisesRegex(ValueError, "missing artifact"):
                validate_artifacts(self.root, version="0.8.12", commit="a" * 40)


if __name__ == "__main__":
    unittest.main()
