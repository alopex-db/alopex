import base64
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from common import Failure, record_cleanup_error
import hosted_native_begin_observer as controller
import native_begin_observer_matrix as matrix


class HostedBeginTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name)
        self.work, self.export = self.repo / "work", self.repo / "export"
        self.work.mkdir(); self.export.mkdir()
        self.config = dict(operators={}, matrix_seconds=1000, limits={"run_seconds": 420})
        self.profile = dict(cpu={}, private_output=str(self.work))
        self.cells = matrix.plan()[:2]

    def execute(self, failure=None, elapsed=0):
        public = dict(image_digest="a" * 64)
        path = self.work / "assets.json"
        path.write_text("{}")
        owned = SimpleNamespace(record={"phase": "deleted"}, verify_collection=lambda: None)
        with patch.object(matrix, "plan", return_value=self.cells), \
             patch.object(matrix, "prepare_cell", return_value=(path, {})), \
             patch.object(matrix, "execute_cell", side_effect=failure,
                          return_value={"status": "passed", "processes": 6}) as execute, \
             patch("lifecycle.Run.load", return_value=owned), \
             patch.object(controller.time, "monotonic", side_effect=[0, elapsed, elapsed]):
            error = None
            try:
                controller.operation(self.repo, self.config)(self.profile, None, Mock(), self.work, self.export, public)
            except BaseException as caught:
                error = caught
        return error, json.loads((self.work / "begin-results.json").read_text()), execute.call_count, public

    def test_success_collects_all_cells_without_performance_claim(self):
        error, rows, calls, public = self.execute()
        self.assertIsNone(error)
        self.assertEqual(calls, 2)
        self.assertTrue(public["matrix_complete"])
        self.assertFalse(public["performance_qualification"])
        summary = json.loads((self.export / "begin-comparison.json").read_text())
        self.assertFalse(summary["performance_acceptance"])
        self.assertEqual(set(summary["cells"][0]), {"id", "rows", "indexes", "trial", "subject", "status"})

    def test_reported_worker_failure_continues_independent_cells(self):
        error, rows, calls, public = self.execute(Failure("CASE"))
        self.assertEqual(error.code, "CASE")
        self.assertEqual(calls, 2)
        self.assertEqual([row["status"] for row in rows], ["failed", "failed"])
        self.assertFalse(public["matrix_complete"])

    def test_infrastructure_failure_or_interruption_stops_new_cells(self):
        cleanup = Failure("CASE")
        record_cleanup_error(cleanup, "collect", Failure("COLLECT"))
        for failure in (Failure("IDENTITY"), Failure("CAPACITY"), InterruptedError("interrupt"), cleanup):
            error, rows, calls, _ = self.execute(failure)
            self.assertIs(error, failure)
            self.assertEqual(calls, 1)
            self.assertEqual([row["status"] for row in rows], ["failed", "not_run"])

    def test_deadline_preserves_full_cell_budget(self):
        error, rows, calls, public = self.execute(elapsed=581)
        self.assertEqual(calls, 0)
        self.assertEqual(error.code, "CASE")
        self.assertTrue(all(row["status"] == "not_run" for row in rows))
        self.assertFalse(public["matrix_complete"])

    def test_fixed_subject_and_duration_validation(self):
        good = dict(schema="alopex.native-begin-observer-input/v1", subjects={"before": {}, "candidate": {}},
                    matrix_seconds=3600, limits={"run_seconds": 420, "case_seconds": 300})
        def read(value):
            encoded = base64.b64encode(json.dumps(value).encode()).decode()
            with patch.dict(os.environ, PUBLIC_PERFORMANCE_CONFIG=encoded): return controller.settings()
        self.assertEqual(read(good), good)
        for value in (dict(good, subjects={"candidate": {}}), dict(good, matrix_seconds=True),
                      dict(good, matrix_seconds=3601), dict(good, schema="other")):
            with self.assertRaises(ValueError): read(value)

    def test_shared_vm_boundary_receives_begin_callbacks(self):
        with patch.object(controller.shared, "main", return_value=0) as main:
            self.assertEqual(controller.main(), 0)
        self.assertEqual(main.call_args.kwargs,
                         dict(settings_reader=controller.settings, operation_factory=controller.operation))


if __name__ == "__main__":
    unittest.main()
