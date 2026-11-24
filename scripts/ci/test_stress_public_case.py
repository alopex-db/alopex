"""Consumer-boundary checks for typed public Stage-2 evidence."""

import json
from pathlib import Path
import shutil
import tempfile
import unittest

import stress_public_case as public_case


SOURCE = "a" * 40


def write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, allow_nan=True))


def fixture(root: Path, case_id: str = "stress") -> tuple[Path, Path]:
    workspace = root / "checkout"
    case = workspace / "target/stress-public-artifacts" / case_id
    run = case / "ci" / "linearizability_register" / "20261122T010000Z-0000"
    write(case / "execution.json", {
        "schema": "alopex.stress-execution/v1", "case_id": case_id,
        "source_commit": SOURCE, "outcome": "passed", "duration_ms": 100,
    })
    write(run / "run.json", {
        "schema_version": 1, "test_name": "linearizability_register", "lane": "ci",
        "seed": 42, "binary_version": "alopex-core@0.8.17",
        "execution_model": "SyncSingle", "concurrency": 1,
        "scenario_timeout_ms": 120000, "operation_timeout_ms": 1000,
        "metrics_interval_ms": 1000, "warmup_ops": 0, "slo": None,
        "replay": False, "git_sha": SOURCE,
    })
    write(run / "metrics.json", {
        "schema_version": 1, "duration_ms": 100, "successes": 12, "errors": 0,
        "sample_count": 12, "throughput_per_sec": 120.0,
        "p50_ns": 10, "p95_ns": 20, "p99_ns": 25, "p999_ns": 25,
        "p9999_ns": 25, "outlier_count": 0, "outlier_ratio": 0.0,
    })
    write(run / "checks/summary.json", {
        "schema_version": 1, "skipped": False, "success": True,
        "code": "passed", "watchdog": "success", "slo_passed": True,
        "duration_ms": 100,
    })
    return workspace, run


def smoke_fixture(root: Path) -> tuple[Path, dict[str, Path]]:
    workspace, template = fixture(root)
    case = template.parents[2]
    runs = {}
    for name in sorted(public_case.STRESS_RUNS):
        run = case / "smoke" / name / template.name
        shutil.copytree(template, run)
        metadata = json.loads((run / "run.json").read_bytes())
        metadata["test_name"] = name
        metadata["lane"] = "smoke"
        metadata["execution_model"] = {
            "sync_single": "SyncSingle", "sync_multi": "SyncMulti",
            "async_multi": "AsyncMulti",
        }[name.rsplit("_", 1)[0]]
        metadata["concurrency"] = {
            "sync_single": 1, "sync_multi": 4, "async_multi": 2,
        }[name.rsplit("_", 1)[0]]
        write(run / "run.json", metadata)
        runs[name] = run
    shutil.rmtree(case / "ci")
    return workspace, runs


class PublicCaseTests(unittest.TestCase):
    def test_assembles_numeric_measurement_and_source(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, run = fixture(Path(directory), case_id="asan")
            shutil.copytree(run, run.with_name("20261122T010000Z-0001"))
            result = public_case.assemble(workspace=workspace, case_id="asan",
                                          source=SOURCE, outcome="passed")
            self.assertEqual(result["runs"][0]["metrics"]["successes"], 12)
            self.assertEqual([item["run_index"] for item in result["runs"]], [0, 1])
            self.assertEqual(result["source_commit"], SOURCE)

    def test_rejects_local_strings_extra_files_nonfinite_and_missing_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, runs = smoke_fixture(Path(directory))
            run = runs["sync_single_memory"]
            original = (run / "run.json").read_bytes()
            value = json.loads(original)
            value["host_path"] = "/private/sensitive"
            write(run / "run.json", value)
            with self.assertRaises(ValueError):
                public_case.assemble(workspace=workspace, case_id="stress",
                                     source=SOURCE, outcome="passed")
            (run / "run.json").write_bytes(original)
            metrics = json.loads((run / "metrics.json").read_bytes())
            metrics["throughput_per_sec"] = float("nan")
            write(run / "metrics.json", metrics)
            with self.assertRaises(ValueError):
                public_case.assemble(workspace=workspace, case_id="stress",
                                     source=SOURCE, outcome="passed")
            (run / "metrics.json").unlink()
            with self.assertRaises(ValueError):
                public_case.assemble(workspace=workspace, case_id="stress",
                                     source=SOURCE, outcome="passed")
            write(run / "raw.log", {"path": "/private/sensitive"})
            with self.assertRaises(ValueError):
                public_case.assemble(workspace=workspace, case_id="stress",
                                     source=SOURCE, outcome="passed")

    def test_rejects_symlink_and_outcome_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, runs = smoke_fixture(Path(directory))
            run = runs["sync_single_memory"]
            (run / "logs").symlink_to(run)
            with self.assertRaises(ValueError):
                public_case.assemble(workspace=workspace, case_id="stress",
                                     source=SOURCE, outcome="passed")
            (run / "logs").unlink()
            with self.assertRaises(ValueError):
                public_case.assemble(workspace=workspace, case_id="stress",
                                     source=SOURCE, outcome="failed")

    def test_passed_stress_requires_exact_six_smoke_runs(self):
        for change in ("missing", "skipped", "wrong_name", "wrong_lane", "extra"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                workspace, runs = smoke_fixture(Path(directory))
                result = public_case.assemble(workspace=workspace, case_id="stress",
                                              source=SOURCE, outcome="passed")
                self.assertEqual(len(result["runs"]), 6)
                run = runs["sync_single_memory"]
                if change == "missing":
                    shutil.rmtree(run)
                elif change == "skipped":
                    check = json.loads((run / "checks/summary.json").read_bytes())
                    check.update(skipped=True, success=False, code="skipped")
                    write(run / "checks/summary.json", check)
                elif change in {"wrong_name", "wrong_lane"}:
                    renamed = (run.parents[2] /
                               ("ci" if change == "wrong_lane" else "smoke") /
                               ("unexpected_run" if change == "wrong_name"
                                else "sync_single_memory") / run.name)
                    renamed.parent.mkdir(parents=True)
                    run.rename(renamed)
                    metadata = json.loads((renamed / "run.json").read_bytes())
                    metadata["test_name"] = renamed.parent.name
                    metadata["lane"] = renamed.parent.parent.name
                    write(renamed / "run.json", metadata)
                else:
                    extra = run.parents[2] / "smoke" / "unexpected_run" / run.name
                    shutil.copytree(run, extra)
                    metadata = json.loads((extra / "run.json").read_bytes())
                    metadata["test_name"] = "unexpected_run"
                    write(extra / "run.json", metadata)
                with self.assertRaises(ValueError):
                    public_case.assemble(workspace=workspace, case_id="stress",
                                         source=SOURCE, outcome="passed")

    def test_perf_comparison_keeps_numeric_baseline_and_rejects_raw_text(self):
        name = "perf_mixed_point_latency.json"
        metrics = {key: 2.0 for key in public_case.PERF_METRICS[name]}
        report = {"schema_version": 1, "metrics": metrics,
                  "comparisons": {key: {"current": 2.0, "baseline": 1.0,
                                        "passed": True} for key in metrics},
                  "baseline_present": True, "passed": True}
        self.assertEqual(public_case.perf_record(report, name)["metrics"], metrics)
        report["comparisons"]["p99_latency_ns"]["baseline"] = "/private/path"
        with self.assertRaises(ValueError):
            public_case.perf_record(report, name)

    def test_passed_perf_requires_both_named_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, run = fixture(Path(directory), case_id="perf")
            renamed = (run.parent.parent.parent / "perf" /
                       "perf_mixed_point_latency" / run.name)
            renamed.parent.mkdir(parents=True)
            run.rename(renamed)
            metadata = json.loads((renamed / "run.json").read_bytes())
            metadata["test_name"] = "perf_mixed_point_latency"
            metadata["lane"] = "perf"
            write(renamed / "run.json", metadata)
            name = "perf_mixed_point_latency.json"
            metrics = {key: 2.0 for key in public_case.PERF_METRICS[name]}
            write(renamed / "checks" / name, {
                "schema_version": 1, "metrics": metrics,
                "comparisons": {key: {"current": 2.0, "baseline": 1.0,
                                      "passed": True} for key in metrics},
                "baseline_present": True, "passed": True,
            })
            with self.assertRaises(ValueError):
                public_case.assemble(workspace=workspace, case_id="perf",
                                     source=SOURCE, outcome="passed")


if __name__ == "__main__":
    unittest.main()
