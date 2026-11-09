import base64
import gzip
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from common import Failure, digest, record_cleanup_error
import hosted_native_performance as controller
import native_sql_matrix as matrix
import native_sql_measure_container as container


class MatrixStopTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        candidate = self.repo / "native-candidate-import/assets"
        candidate.mkdir(parents=True)
        (candidate / "harness.tar").write_bytes(b"fixture")
        (candidate / "assets.json").write_text(json.dumps({"files": [{"role": "harness", "path": "harness.tar"}]}))
        self.work, self.export = self.repo / "work", self.repo / "export"
        self.work.mkdir(); self.export.mkdir()
        self.config = {"operators": {}, "docs_harness_digest": digest(candidate / "harness.tar"),
                       "product_harness": {"files": {}}, "matrix_seconds": 1000}
        self.profile = {"cpu": {}, "private_output": str(self.work)}
        self.cells = matrix.plan()[:2]

    def execute(self, error, elapsed=0):
        public = {"image_digest": "a" * 64}
        with patch.object(matrix, "plan", return_value=self.cells), \
             patch.object(matrix, "compose_harness", return_value={}), \
             patch.object(matrix, "prepare_cell", return_value=(self.work / "assets.json", {})), \
             patch.object(matrix, "summarize", return_value={"performance_acceptance": False}), \
             patch.object(container, "execute_cell", side_effect=error) as execute, \
             patch.object(controller.time, "monotonic", side_effect=[0, elapsed, elapsed]):
            with self.assertRaises(BaseException) as caught:
                controller.operation(self.repo, self.config)(self.profile, None, Mock(), self.work, self.export, public)
        rows = json.loads((self.work / "matrix-results.json").read_text())
        self.assertFalse(public["matrix_complete"])
        self.assertFalse(public["performance_qualification"])
        return caught.exception, rows, execute.call_count

    def test_case_failure_retains_each_independent_result(self):
        error, rows, calls = self.execute(Failure("CASE"))
        self.assertEqual(calls, 2)
        self.assertEqual([r["status"] for r in rows], ["failed", "failed"])
        self.assertEqual(error.code, "CASE")

    def test_collection_failure_attached_to_case_stops_next_cell(self):
        failure = Failure("CASE")
        record_cleanup_error(failure, "collect", Failure("COLLECT"))
        error, rows, calls = self.execute(failure)
        self.assertIs(error, failure)
        self.assertEqual(calls, 1)
        self.assertEqual([r["status"] for r in rows], ["failed", "not_run"])

    def test_capacity_or_identity_failure_stops_new_work(self):
        for code in ("CAPACITY", "IDENTITY", "MEASUREMENT"):
            with self.subTest(code=code):
                error, rows, calls = self.execute(Failure(code))
                self.assertEqual(error.code, code)
                self.assertEqual(calls, 1)
                self.assertEqual(rows[1]["status"], "not_run")

    def test_interruption_preserved_and_stops_next_cell(self):
        failure = InterruptedError("fixture interruption")
        error, rows, calls = self.execute(failure)
        self.assertIs(error, failure)
        self.assertEqual(calls, 1)
        self.assertEqual(rows[1]["status"], "not_run")

    def test_deadline_reserves_full_cell_before_start(self):
        _, rows, calls = self.execute(Failure("CASE"), elapsed=581)
        self.assertEqual(calls, 0)
        self.assertTrue(all(row["status"] == "not_run" for row in rows))


class ConfigTests(unittest.TestCase):
    def test_product_artifact_name_is_allowlisted(self):
        self.assertEqual(controller.product_artifact_name({}), "native-product-assets")
        self.assertEqual(controller.product_artifact_name(
            {"artifact_name": "native-product-assets-no-mount"}),
            "native-product-assets-no-mount")
        with self.assertRaisesRegex(ValueError, "unqualified"):
            controller.product_artifact_name({"artifact_name": "arbitrary"})

    def test_seccomp_policy_is_pinned_before_construction(self):
        controller.verify_boundary_inputs(
            {"seccomp_sha256": controller.EXPECTED_SECCOMP_SHA256})
        with self.assertRaisesRegex(ValueError, "unreviewed"):
            controller.verify_boundary_inputs({"seccomp_sha256": "0" * 64})

    def test_requires_exact_subjects_and_bounded_duration(self):
        good = {"subjects": {name: {} for name in ("published", "before", "candidate")},
                "matrix_seconds": 7200, "limits": {"case_seconds": 300, "run_seconds": 420}}
        def read(value):
            encoded = base64.b64encode(json.dumps(value).encode()).decode()
            with patch.dict(os.environ, PUBLIC_PERFORMANCE_CONFIG=encoded): return controller.settings()
        self.assertEqual(read(good), good)
        for value in (dict(good, matrix_seconds=True), dict(good, matrix_seconds=7201),
                      dict(good, subjects={"candidate": {}})):
            with self.assertRaises(ValueError): read(value)

    def test_outer_timer_is_removed_even_when_construction_fails(self):
        inputs = base64.b64encode(gzip.compress(json.dumps(
            {"bounds": {}, "limits": {}, "seccomp_sha256": controller.EXPECTED_SECCOMP_SHA256}
        ).encode())).decode()
        config = {"image": {}, "bounds": {}, "limits": {}, "matrix_seconds": 7200}
        with patch.dict(os.environ, RUNNER_KIND="github-hosted", GITHUB_WORKSPACE="/fixture",
                        RUNNER_TEMP="/temporary-fixture", NATIVE_IMAGE_INPUTS=inputs), \
             patch.object(controller.sys, "argv", ["controller", "run"]), \
             patch.object(controller.os, "geteuid", return_value=0), \
             patch.object(controller, "settings", return_value=config), \
             patch.object(controller.signal, "alarm") as alarm, \
             patch.object(controller, "construct", side_effect=InterruptedError):
            with self.assertRaises(InterruptedError): controller.main()
        self.assertEqual([call.args[0] for call in alarm.call_args_list], [7800, 0])


if __name__ == "__main__":
    unittest.main()
