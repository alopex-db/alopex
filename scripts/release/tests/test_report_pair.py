from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "report_pair.py"
spec = importlib.util.spec_from_file_location("report_pair", SCRIPT)
pair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pair)


class ReportPairTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="alopex-report-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "download" / "nested"
        self.source.mkdir(parents=True)
        self.results = self.source / "v0.8.17.json"
        self.payload = {
            "schema": "alopex-release-verification/v2", "version": "0.8.17",
            "started_at": "2026-10-29T00:00:00Z", "completed_at": "2026-10-29T00:01:00Z",
            "outcome": "success", "failure_stage": None,
            "identity": {
                "commit": "a" * 40, "tag": "alopex-py-v0.8.17", "run_id": "123",
                "run_attempt": 1, "run_url": "https://github.com/alopex-db/alopex/actions/runs/123",
                "responsibility": "Stable Delivery / downstream availability",
            },
            "environment": {"package_source": "PyPI", "rust": "1.90", "nim": "2.2.10", "python": "3.11"},
            "steps": [{"name": "availability", "status": "success", "description": "exact wheel",
                       "log_excerpt": [], "diagnostics": [], "duration_ms": 1}],
        }
        self.write_pair()

    def write_pair(self):
        self.results.write_text(json.dumps(self.payload))
        pair.report.render(SimpleNamespace(results=self.results, output_dir=self.source))

    def select(self, artifact="release-verification-123-1"):
        return pair.select_pair(self.root / "download", artifact, "123", "alopex-db/alopex")

    def test_retry_keeps_producing_attempt_path_and_bytes(self):
        before = {p.name: p.read_bytes() for p in self.source.iterdir()}
        selected = self.select()
        self.assertEqual(selected["REPORT_RUN_ATTEMPT"], "1")
        self.assertEqual(selected["REPORT_PATH"], "reports/release-verification/v0.8.17/run-123-1")
        self.assertEqual(selected["REPORT_ROOT"], str(self.source))
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.source.iterdir()})

    def test_artifact_from_publishing_attempt_cannot_substitute_for_producer(self):
        with self.assertRaisesRegex(ValueError, "producing artifact"):
            self.select("release-verification-123-2")

    def test_foreign_run_and_malformed_artifact_are_rejected(self):
        for name in ["release-verification-124-1", "release-verification-123-0", "latest", "release-verification-123-1\n"]:
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.select(name)

    def test_run_url_must_match_repository(self):
        self.payload["identity"]["run_url"] = "https://github.com/other/repo/actions/runs/123"
        self.write_pair()
        with self.assertRaisesRegex(ValueError, "repository/run"):
            self.select()

    def test_missing_markdown_rejected(self):
        self.results.with_suffix(".md").unlink()
        with self.assertRaisesRegex(ValueError, "missing"):
            self.select()

    def test_markdown_from_different_result_rejected(self):
        self.results.with_suffix(".md").write_text("unrelated success report\n")
        with self.assertRaisesRegex(ValueError, "does not render"):
            self.select()

    def test_ambiguous_reports_rejected(self):
        (self.source / "v0.8.16.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.select()

    def test_malformed_identity_and_outcome_rejected(self):
        for field, value in [("run_attempt", True), ("run_attempt", 2), ("run_id", "124")]:
            with self.subTest(field=field, value=value):
                original = self.payload["identity"][field]
                self.payload["identity"][field] = value
                self.write_pair()
                with self.assertRaises((ValueError, SystemExit)):
                    self.select()
                self.payload["identity"][field] = original
        self.payload["outcome"] = "failure"
        self.write_pair()
        with self.assertRaises(SystemExit):
            self.select()

    def test_terminal_failure_and_unresolved_version_are_publishable(self):
        self.payload.update(version="unknown", outcome="failure", failure_stage="availability")
        self.payload["steps"][0]["status"] = "failure"
        self.payload["identity"].update(commit="unresolved", tag="unresolved")
        for p in self.source.iterdir():
            p.unlink()
        self.results = self.source / "vunknown.json"
        self.write_pair()
        self.assertEqual(self.select()["VERSION"], "unknown")

    def test_version_path_traversal_rejected(self):
        self.payload["version"] = "../../outside"
        self.results.write_text(json.dumps(self.payload))
        with self.assertRaisesRegex(ValueError, "version"):
            self.select()

    def test_symlink_and_oversized_file_rejected(self):
        linked = self.source / "extra"
        try:
            linked.symlink_to(self.results)
        except OSError:
            self.skipTest("native symlink creation unavailable")
        with self.assertRaisesRegex(ValueError, "symlinks"):
            self.select()
        linked.unlink()
        with self.results.with_suffix(".md").open("wb") as stream:
            stream.truncate(pair.MAX_REPORT_BYTES + 1)
        with self.assertRaisesRegex(ValueError, "exceeds"):
            self.select()

    def test_pair_comparison_requires_both_exact_files(self):
        right = self.root / "public"
        right.mkdir()
        left_prefix = self.source / "v0.8.17"
        right_prefix = right / "run-123-1"
        for extension in (".json", ".md"):
            shutil.copyfile(Path(str(left_prefix) + extension), Path(str(right_prefix) + extension))
        pair.compare_pair(left_prefix, right_prefix)
        for extension in (".json", ".md"):
            path = Path(str(right_prefix) + extension)
            original = path.read_bytes()
            path.write_bytes(original + b"\n")
            with self.assertRaisesRegex(ValueError, "differs"):
                pair.compare_pair(left_prefix, right_prefix)
            path.unlink()
            with self.assertRaisesRegex(ValueError, "missing"):
                pair.compare_pair(left_prefix, right_prefix)
            path.write_bytes(original)


if __name__ == "__main__":
    unittest.main()
