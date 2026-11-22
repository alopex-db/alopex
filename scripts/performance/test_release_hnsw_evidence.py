from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.performance.hnsw_v0811_public import (
    ARMS, DATASET_SHA256, ENGINES, GLOVE_SHA256, ROW_FIELDS,
    build_public_payload, write_public_pair,
)
from scripts.performance.release_hnsw_evidence import (
    STAGES, render_status_markdown, validate_artifacts, validate_public_pair,
)


def row(kind: str, label: str, name: str, **values: object) -> dict[str, object]:
    result = {label: name, **{field: 1.0 for field in ROW_FIELDS[kind]}}
    result.update(values)
    return result


class PublicHnswEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.version = "0.8.17"
        self.commit = "a" * 40
        def timed_run(engine: str, ef: int, run: int, size: int) -> dict[str, object]:
            return {**row("runs", "engine", engine), "ef_search": ef, "run": run,
                    "dataset_size": size, "dimension": 128, "query_count": 10000,
                    "duration_seconds": 2.1, "queries_per_second": 1000.0,
                    "latency_us": 100.0, "query_latency_p50_us": 100.0,
                    "query_latency_p95_us": 120.0, "query_latency_p99_us": 140.0,
                    "native_search_latency_us": 50.0 if engine == "alopex-hnsw" else None,
                    "python_binding_residual_us": 50.0 if engine == "alopex-hnsw" else None,
                    "recall_at_10": 0.99, "tie_aware_recall_at_10": 0.99}
        self.runs = [timed_run(engine, ef, run, 9171)
                     for engine in sorted(ENGINES - {"flat", "faiss-flat-exact"})
                     for ef in (16, 32, 64, 128, 256) for run in (1, 2, 3)]
        self.runs += [timed_run("faiss-flat-exact", 9171, run, 9171) for run in (1, 2, 3)]
        self.dataset = {"source_sha256": DATASET_SHA256, "clean_rows": 9171,
                        "dimension": 128, "query_rows": 200, "explained_variance": 0.95,
                        "source_file": "/private/runner/dataset.csv"}
        self.builds = [row("builds", "engine", engine) for engine in sorted(ENGINES - {"flat"})]
        for build in self.builds:
            if build["engine"].startswith("faiss-"):
                for metric in ("update_latency_ms", "delete_latency_ms", "reopen_latency_ms"):
                    build[metric] = None
        self.recall = [row("recall_ceiling", "engine", engine, ef_search=ef)
                       for engine in sorted(ENGINES - {"flat", "faiss-flat-exact"})
                       for ef in (512, 1024, 4096, 9171)]
        investigation = {"index_count": 9171, "input_count": 9171,
                         "index_count_matches_input": True, "self_match_rate": 1.0,
                         "self_match_ef_search": 64,
                         "exact_metric_agreement": {"rows": 512, "queries": 32,
                                                    "normalized": True,
                                                    "l2_inner_product_top_10_equal": True},
                         "ef_construction_and_m": [row("recall_configurations", "engine", "alopex-hnsw")],
                         "minimal_reproduction": {"raw_error": "/private/runner"}}
        self.diagnostics = {
            "recall_investigation": investigation,
            "fixed_cost_runs": [row("fixed_cost_runs", "engine", "alopex-hnsw")],
            "latency_decomposition": [row("latency_decomposition", "engine", "alopex-hnsw")],
            "hybrid": {
                "summary": [row("hybrid_summary", "arm", arm, selectivity=s,
                                returns_k=True) for s in (0.001, 0.01, 0.05, 0.2, 1.0)
                            for arm in sorted(ARMS)],
                "filter_aware_traversal": False,
                "alopex_advantageous_selectivities": [0.2],
                "runs": [{"host": "/private/runner"}],
            },
        }
        self.scale = {
            "sha256": GLOVE_SHA256, "max_n": 50000,
            "brute_force_crossover": {engine: None for engine in ("alopex-hnsw", "faiss-hnsw", "hnswlib")},
            "gap_trend_vs_flat": {engine: "constant" for engine in ("alopex-hnsw", "faiss-hnsw", "hnswlib")},
            "limits": [{"dataset_size": 200000, "reason": "/private/runner"},
                       {"dataset_size": 1000000, "reason": "/private/runner"}],
            "build_results": [row("scale_build_results", "engine", engine, dataset_size=size)
                              for size in (10000, 50000) for engine in sorted({"alopex-hnsw", "hnswlib", "faiss-hnsw", "flat"})],
            "runs": [timed_run(engine, ef, run, size)
                     for size in (10000, 50000)
                     for engine in sorted(ENGINES - {"flat", "faiss-flat-exact"})
                     for ef in (16, 32, 64, 128, 256) for run in (1, 2, 3)]
                    + [timed_run("faiss-flat-exact", size, run, size)
                       for size in (10000, 50000) for run in (1, 2, 3)],
            "results": [row("scale_results", "engine", engine, dataset_size=size,
                            qps_at_recall_095=1000.0,
                            ef_search_at_recall_095=size if engine == "flat" else 16,
                            recall_at_selected_setting=0.99,
                            curve=[row("scale_curve", "engine", "faiss-flat-exact" if engine == "flat" else engine,
                                       ef_search=ef, median_recall_at_10=0.99,
                                       median_tie_aware_recall_at_10=0.99,
                                       median_queries_per_second=1000.0,
                                       median_latency_us=100.0)
                                   for ef in ((size,) if engine == "flat" else (16, 32, 64, 128, 256))])
                        for size in (10000, 50000) for engine in sorted({"alopex-hnsw", "hnswlib", "faiss-hnsw", "flat"})],
        }
        self.write_success()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_success(self) -> None:
        payload = build_public_payload(
            release_version=self.version, source_commit=self.commit,
            run_id=123, run_attempt=2, runs=self.runs, dataset=self.dataset,
            builds=self.builds, recall_ceiling=self.recall,
            diagnostics=self.diagnostics, scale=self.scale,
        )
        write_public_pair(self.root, payload)
        self.write_status("success")

    def write_status(self, outcome: str) -> None:
        stages = {name: "success" for name in STAGES}
        if outcome != "success":
            stages["benchmark"] = "failure"
            for name in STAGES[STAGES.index("benchmark") + 1:]:
                stages[name] = "skipped"
            for name in ("hnsw-diagnostic.json", "hnsw-diagnostic.md"):
                (self.root / name).unlink(missing_ok=True)
        status = {"schema": "alopex.hnsw-diagnostic-run/v1", "source_commit": self.commit,
                  "run_id": 123, "run_attempt": 2, "status": outcome,
                  "failure_stage": None if outcome == "success" else "benchmark",
                  "stages": stages}
        (self.root / "benchmark-status.json").write_text(json.dumps(status))
        (self.root / "benchmark-status.md").write_text(render_status_markdown(status))

    def validate(self) -> None:
        validate_artifacts(self.root, version=self.version, commit=self.commit,
                           run_id=123, run_attempt=2)

    def test_success_inventory_and_pair_have_exact_run_identity(self) -> None:
        self.validate()
        self.assertEqual({p.name for p in self.root.iterdir()}, {
            "benchmark-status.json", "benchmark-status.md", "hnsw-diagnostic.json", "hnsw-diagnostic.md"})
        validate_public_pair(self.root, version=self.version, commit=self.commit,
                             run_id=123, run_attempt=2)
        with self.assertRaisesRegex(ValueError, "run identity"):
            validate_public_pair(self.root, version=self.version, commit=self.commit,
                                 run_id=123, run_attempt=3)

    def test_public_producer_drops_private_observations(self) -> None:
        text = (self.root / "hnsw-diagnostic.json").read_text() + (self.root / "hnsw-diagnostic.md").read_text()
        for forbidden in ("/private/runner", "source_file", "cpu_model", "platform", "affinity", "dependencies", "raw_error"):
            self.assertNotIn(forbidden, text)

    def test_public_producer_rejects_arbitrary_string_as_numeric_observation(self) -> None:
        for rows, key in ((self.builds, "peak_rss_bytes"),
                          (self.runs, "queries_per_second"),
                          (self.scale["runs"], "duration_seconds")):
            with self.subTest(key=key):
                original = rows[0][key]
                rows[0][key] = "/private/runner"
                with self.assertRaisesRegex(ValueError, "finite number"):
                    self.write_success()
                rows[0][key] = original

    def test_public_validator_rejects_extra_keys_and_non_numeric_rows(self) -> None:
        path = self.root / "hnsw-diagnostic.json"
        original = json.loads(path.read_text())
        for mutation in (lambda p: p.update({"host": "private"}),
                         lambda p: p["scale"]["results"][0].update({"path": "/private/runner"}),
                         lambda p: p["builds"][0].update({"peak_rss_bytes": "private"})):
            payload = copy.deepcopy(original)
            mutation(payload)
            path.write_text(json.dumps(payload))
            with self.assertRaises(ValueError):
                self.validate()
        path.write_text(json.dumps(original))

    def test_failure_inventory_has_only_fixed_status_pair(self) -> None:
        self.write_status("failure")
        self.validate()
        self.assertEqual({p.name for p in self.root.iterdir()}, {"benchmark-status.json", "benchmark-status.md"})
        (self.root / "environment.txt").write_text("host=private")
        with self.assertRaisesRegex(ValueError, "inventory"):
            self.validate()

    def test_incomplete_inventory_has_only_fixed_status_pair(self) -> None:
        self.write_status("failure")
        status_path = self.root / "benchmark-status.json"
        status = json.loads(status_path.read_text())
        status["status"] = "incomplete"
        status["stages"]["benchmark"] = "cancelled"
        status_path.write_text(json.dumps(status))
        (self.root / "benchmark-status.md").write_text(render_status_markdown(status))
        self.validate()

    def test_success_inventory_rejects_raw_member(self) -> None:
        (self.root / "hnsw-diagnostic.raw.ndjson").write_text("raw")
        with self.assertRaisesRegex(ValueError, "inventory"):
            self.validate()

    def test_workflow_uploads_only_the_four_enumerated_public_files(self) -> None:
        workflow = (Path(__file__).resolve().parents[2] / ".github/workflows/parity-performance.yml").read_text()
        upload = workflow.split("name: parity-performance-public-${{ github.run_id }}-${{ github.run_attempt }}", 1)[1].split("if-no-files-found:", 1)[0]
        members = [line.strip() for line in upload.splitlines() if line.strip().startswith("performance-public/")]
        self.assertEqual(members, [
            "performance-public/benchmark-status.json",
            "performance-public/benchmark-status.md",
            "performance-public/hnsw-diagnostic.json",
            "performance-public/hnsw-diagnostic.md",
        ])
        self.assertIn("steps.public_inventory.outcome == 'success'", workflow)

    def test_workflow_keeps_raw_process_output_out_of_actions_logs(self) -> None:
        workflow = (Path(__file__).resolve().parents[2] / ".github/workflows/parity-performance.yml").read_text()
        self.assertNotIn("| tee", workflow)
        for stage in STAGES + ("status", "public_inventory", "prepare_python"):
            self.assertIn(f"run_private_stage.sh {stage} bash -euo pipefail", workflow)
        self.assertNotIn("ALOPEX_KNN_COST_RAW_NDJSON: ${{ github.workspace }}", workflow)
        self.assertIn("performance-outcome-private.log 2>&1", workflow)
        self.assertIn("PARITY_OUTCOME_FAILED", workflow)

    def test_private_stage_fails_fast_with_fixed_public_code(self) -> None:
        helper = Path(__file__).with_name("run_private_stage.sh")
        with tempfile.TemporaryDirectory() as directory:
            completed = subprocess.run(
                ["bash", str(helper), "benchmark", "bash", "-euo", "pipefail"],
                input="printf 'private host /runner/path\\n'\nfalse\nprintf 'should not run\\n'\n",
                text=True, capture_output=True, cwd=directory,
            )
            self.assertEqual(completed.returncode, 1)
            self.assertEqual(completed.stdout, "::error::PARITY_benchmark_FAILED\n")
            self.assertEqual(completed.stderr, "")
            diagnostic = Path(directory) / "performance-debug"
            self.assertEqual(diagnostic.stat().st_mode & 0o777, 0o700)
            log = (diagnostic / "benchmark.log").read_text()
            self.assertIn("private host /runner/path", log)
            self.assertNotIn("should not run", log)


if __name__ == "__main__":
    unittest.main()
