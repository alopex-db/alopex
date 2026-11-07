import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from common import Failure, digest
from lifecycle import Run
from docker_backend import Docker
import native_sql_measure_container as controller


class ContainerCollectionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.assets = self.root / "assets.json"
        self.assets.write_text("{}")
        self.profile = {"private_output": str(self.root), "run_id": "unit-cell",
                        "assets": str(self.assets), "docker_context": "unit-context"}
        self.events = []

    def execute(self, *args):
        run = Run(self.root, "unit-cell").create({})
        run.transition("running")
        run.transition("stopped")
        self.events.append("stopped")

    def collect(self, *args):
        self.events.append("collected")
        def copy(destination):
            (destination / "result.json").write_text(json.dumps({"status": "passed"}))
        Run(self.root, "unit-cell").load().collect(copy)

    def test_reuses_full_docker_boundary_changing_only_program(self):
        original = ["create", "--network=none", "--ipc=private", "--cap-drop=ALL",
                    "--memory", "unit-limit", "--security-opt", "unit-policy", "unit-image"]
        with patch.object(Docker, "arguments", return_value=original):
            actual = controller.MeasurementDocker(self.profile).arguments("unit-cell", "unit-image", "in", "work", "token")
        self.assertEqual(actual[:-5], original[:-1])
        self.assertEqual(actual[-5:], ["--entrypoint", "python3", "unit-image", "-B", "/input/controller.py"])

    def test_success_verifies_collection_before_deletion(self):
        def verify(run, *args):
            run.verify_collection()
            self.assertEqual(run.record["phase"], "collected")
            self.events.append("verified")
            return {"status": "passed"}
        with patch.object(controller.envctl, "execute", side_effect=self.execute), \
             patch.object(controller.envctl, "collect", side_effect=self.collect), \
             patch.object(controller, "verify_collected", side_effect=verify):
            self.assertEqual(controller.execute_cell(self.profile, None, {}), {"status": "passed"})
        self.assertEqual(self.events, ["stopped", "collected", "verified"])
        self.assertEqual(Run(self.root, "unit-cell").load().record["phase"], "deleted")

    def test_failed_collection_never_authorizes_deletion(self):
        with patch.object(controller.envctl, "execute", side_effect=self.execute), \
             patch.object(controller.envctl, "collect", side_effect=Failure("COLLECT")), \
             patch.object(Run, "clean") as clean:
            with self.assertRaises(Failure): controller.execute_cell(self.profile, None, {})
        clean.assert_not_called()
        self.assertEqual(Run(self.root, "unit-cell").load().record["phase"], "stopped")

    def test_failed_execution_collects_evidence_then_preserves_failure(self):
        def failed(*args):
            self.execute()
            raise Failure("CASE", "worker failed")
        with patch.object(controller.envctl, "execute", side_effect=failed), \
             patch.object(controller.envctl, "collect", side_effect=self.collect):
            with self.assertRaises(Failure) as caught: controller.execute_cell(self.profile, None, {})
        self.assertEqual(caught.exception.code, "CASE")
        self.assertEqual(self.events, ["stopped", "collected"])
        run = Run(self.root, "unit-cell").load()
        self.assertEqual(run.record["phase"], "deleted")
        run.verify_collection()

    def test_tampered_collection_prevents_cleanup(self):
        def tamper(run, *args):
            (run.root / "collected/result.json").write_text("changed")
            raise Failure("IDENTITY")
        with patch.object(controller.envctl, "execute", side_effect=self.execute), \
             patch.object(controller.envctl, "collect", side_effect=self.collect), \
             patch.object(controller, "verify_collected", side_effect=tamper):
            with self.assertRaises(Failure) as caught: controller.execute_cell(self.profile, None, {})
        self.assertEqual(caught.exception.code, "IDENTITY")
        self.assertEqual(Run(self.root, "unit-cell").load().record["phase"], "collected")

    def recovered_worker(self, stop_error=None, wrong_source=False):
        config = {"case": "aggregate", "engine": "alopex-server", "trial": 0, "source": {"commit": "a" * 40}}
        original = Failure("COMMAND")
        def execute(*args):
            self.execute()
            raise original
        def collect(*args):
            def copy(destination):
                cell = destination / "cell"
                cell.mkdir()
                (cell / "status.json").write_text(json.dumps({"status": "failed", "worker_error": "fixture failure", "stop_error": stop_error}))
                report = dict(config, schema="alopex.native-sql-measurement/v1", status="failed",
                              assets_digest=digest(self.assets), cell_status_sha256=digest(cell / "status.json"))
                if wrong_source: report["source"] = {"commit": "b" * 40}
                (destination / "result.json").write_text(json.dumps(report))
            Run(self.root, "unit-cell").load().collect(copy)
        with patch.object(controller.envctl, "execute", side_effect=execute), \
             patch.object(controller.envctl, "collect", side_effect=collect):
            with self.assertRaises(Failure) as caught: controller.execute_cell(self.profile, None, config)
        return caught.exception, original

    def test_recovered_worker_command_error_becomes_independent_case_failure(self):
        error, original = self.recovered_worker()
        self.assertEqual(error.code, "CASE")
        self.assertIs(error.__cause__, original)
        self.assertEqual(Run(self.root, "unit-cell").load().record["phase"], "deleted")

    def test_worker_stop_failure_is_not_reclassified_as_case_failure(self):
        error, original = self.recovered_worker(stop_error="stop failed")
        self.assertIs(error, original)
        self.assertEqual(error.code, "COMMAND")

    def test_wrong_subject_failure_report_stops_matrix(self):
        error, _ = self.recovered_worker(wrong_source=True)
        self.assertEqual(error.code, "IDENTITY")


if __name__ == "__main__":
    unittest.main()
