"""Offline SQL diagnostic coverage; no Docker or network calls."""
import sys
from pathlib import Path as _BootstrapPath
_OPS = _BootstrapPath(__file__).resolve().parents[1]
sys.path.insert(0, str(_OPS))
sys.path.insert(0, str(_OPS.parent / "test-env"))

import json
import contextlib
import io
from pathlib import Path
import shutil
import sys
import tempfile
import tarfile
import unittest
from unittest.mock import Mock, patch

HERE = Path(__file__).resolve().parents[1]
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "scripts/test-env"))
sys.path.insert(0, str(REPO / "scripts/ops"))

import diagnostic_entrypoint as lane


class FakeRun:
    def __init__(self, output, run_id):
        self.root = Path(output) / run_id
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "run.json"
        self.path.write_text("{}")
        self.record = {"phase": "preparing"}
        self.verified = False
    def load(self):
        return self
    def clean(self, inspect, remove):
        self.record["phase"] = "deleted"
    def verify_collection(self):
        self.verified = True


class EntrypointTests(unittest.TestCase):
    def test_wrapped_primary_failure_uses_fixed_code_and_source_site(self):
        source = str(HERE / "diagnostic_container.py")
        scope = {"Failure": lane.Failure}
        exec(compile("def fail():\n    raise Failure('MEASUREMENT', 'private detail')\n", source, "exec"), scope)
        try:
            scope["fail"]()
        except lane.Failure as primary:
            wrapped = lane.DiagnosticFailure(primary)
        self.assertEqual(lane.fixed_error_code(wrapped), "MEASUREMENT")
        self.assertEqual(lane.fixed_error_site(wrapped),
                         {"module": "diagnostic_container.py", "function": "fail", "line": 2})

    def test_failure_seal_includes_only_fixed_inner_stage_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "alopex-712-diagnostic"
            root.mkdir(mode=0o700)
            lane.atomic_json(root / "diagnostic-failure.private.json",
                             {"schema": "alopex.sql-diagnostic-failure/v1", "status": "failed",
                              "error_code": "INTERNAL"})
            inner = root / "work/plain/failure.private.json"
            value = {"schema": "alopex.sql-diagnostic-failure/v1", "status": "failed",
                     "phase": "qualification", "primary": {"type": "ValueError", "code": "IDENTITY"},
                     "cleanup": None, "performance_qualification": False}
            lane.atomic_json(inner, value)
            names = []
            def encrypt(command, **_):
                with tarfile.open(root / "diagnostic-failure.tar.gz") as archive:
                    names.extend(archive.getnames())
                Path(command[command.index("-out") + 1]).write_bytes(b"encrypted")
            with patch.object(lane.subprocess, "run", side_effect=encrypt):
                lane.seal_failure(root)
            self.assertEqual(names, ["diagnostic/diagnostic-failure.private.json",
                                     "diagnostic/orchestrate-failure.private.json"])
            self.assertEqual({path.name for path in (root / "export").iterdir()}, {"evidence.cms"})

    def test_failure_seal_rejects_inner_record_with_raw_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "alopex-712-diagnostic"
            root.mkdir(mode=0o700)
            lane.atomic_json(root / "diagnostic-failure.private.json", {"status": "failed"})
            lane.atomic_json(root / "work/plain/failure.private.json",
                             {"schema": "alopex.sql-diagnostic-failure/v1", "status": "failed",
                              "phase": "cells", "primary": None, "cleanup": None,
                              "performance_qualification": False, "raw_exception": "secret"})
            with self.assertRaises(lane.Failure):
                lane.seal_failure(root)
            self.assertFalse((root / "export/evidence.cms").exists())

    def test_failure_seal_retains_hash_verified_failed_cell_details(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "alopex-712-diagnostic"
            root.mkdir(mode=0o700)
            lane.atomic_json(root / "diagnostic-failure.private.json",
                             {"schema": "alopex.sql-diagnostic-failure/v1", "status": "failed",
                              "error_code": "CASE"})
            run = lane.Run(root / "runs", "diag-aggregate-0-off")
            run.create({})
            run.record["phase"] = "deleted"
            run.save()
            collected = run.root / "collected"
            cell = collected / "cell"
            cell.mkdir(parents=True, mode=0o700)
            lane.atomic_json(collected / "result.json", {"status": "failed"})
            lane.atomic_json(cell / "status.json", {"status": "failed",
                                                   "worker_error": "TRACE_FORMAT",
                                                   "stop_error": None})
            worker_log = cell / "worker.log"
            worker_log.write_text("bounded private worker failure\n")
            worker_log.chmod(0o600)
            lane.atomic_json(run.root / "collection.json", {
                "result.json": lane.digest(collected / "result.json"),
                "cell/status.json": lane.digest(cell / "status.json"),
                "cell/worker.log": lane.digest(worker_log)})
            original_status = (cell / "status.json").read_bytes()
            (cell / "status.json").write_bytes(original_status.replace(b"TRACE_FORMAT", b"TRACE_CHANGED"))
            with self.assertRaises(lane.Failure):
                lane.seal_failure(root)
            self.assertFalse((root / "export/evidence.cms").exists())
            (cell / "status.json").write_bytes(original_status)
            captured = {}
            def encrypt(command, **_):
                with tarfile.open(root / "diagnostic-failure.tar.gz") as archive:
                    captured.update({name: archive.extractfile(name).read()
                                     for name in archive.getnames()})
                Path(command[command.index("-out") + 1]).write_bytes(b"encrypted")
            with patch.object(lane.subprocess, "run", side_effect=encrypt):
                lane.seal_failure(root)
            self.assertEqual(set(captured), {
                "diagnostic/diagnostic-failure.private.json",
                "diagnostic/failed-cell/result.json",
                "diagnostic/failed-cell/cell/status.json",
                "diagnostic/failed-cell/cell/worker.log"})
            self.assertEqual(json.loads(captured["diagnostic/failed-cell/cell/status.json"])
                             ["worker_error"], "TRACE_FORMAT")
            self.assertEqual({path.name for path in (root / "export").iterdir()},
                             {"evidence.cms"})

    def test_failure_seal_retains_marked_case_mismatch_even_when_report_passed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "alopex-712-diagnostic"
            root.mkdir(mode=0o700)
            lane.atomic_json(root / "diagnostic-failure.private.json",
                             {"schema": "alopex.sql-diagnostic-failure/v1", "status": "failed",
                              "error_code": "CASE"})
            marker = root / "failed-cell.private.json"
            lane.atomic_json(marker, {
                "schema": "alopex.sql-diagnostic-failed-cell/v1",
                "cell_id": "aggregate-0-off", "error_code": "CASE"})
            run = lane.Run(root / "runs", "diag-aggregate-0-off")
            run.create({})
            run.record["phase"] = "deleted"
            run.save()
            collected = run.root / "collected"
            cell = collected / "cell"
            cell.mkdir(parents=True, mode=0o700)
            lane.atomic_json(collected / "result.json",
                             {"status": "passed", "source": {"commit": "mismatch"}})
            lane.atomic_json(cell / "status.json",
                             {"status": "passed", "worker_error": None, "stop_error": None})
            lane.atomic_json(run.root / "collection.json", {
                "result.json": lane.digest(collected / "result.json"),
                "cell/status.json": lane.digest(cell / "status.json")})
            lane.atomic_json(marker, {
                "schema": "alopex.sql-diagnostic-failed-cell/v1",
                "cell_id": "aggregate-0-on", "error_code": "CASE"})
            with self.assertRaises(lane.Failure):
                lane.seal_failure(root)
            self.assertFalse((root / "export/evidence.cms").exists())
            lane.atomic_json(marker, {
                "schema": "alopex.sql-diagnostic-failed-cell/v1",
                "cell_id": "aggregate-0-off", "error_code": "CASE"})
            captured = {}
            def encrypt(command, **_):
                with tarfile.open(root / "diagnostic-failure.tar.gz") as archive:
                    captured.update({name: archive.extractfile(name).read()
                                     for name in archive.getnames()})
                Path(command[command.index("-out") + 1]).write_bytes(b"encrypted")
            with patch.object(lane.subprocess, "run", side_effect=encrypt):
                lane.seal_failure(root)
            self.assertEqual(json.loads(captured["diagnostic/failed-cell/result.json"])
                             ["source"], {"commit": "mismatch"})
            self.assertIn("diagnostic/failed-cell.private.json", captured)

    def test_explicit_pins_and_operator_bytes(self):
        source = HERE / "sql-diagnostic-lock.json"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            operators = root / "ops"
            operators.mkdir()
            for name in lane.OPERATOR_NAMES:
                original = HERE / name
                shutil.copyfile(original, operators / name)
            sha = "30d118f71ddc489a6457ed3b09c610049f2d79aa"
            with patch.dict("os.environ", {"GITHUB_SHA": sha}):
                pin = lane.load_pins(source, controller_sha=sha, operator_dir=operators)
                self.assertEqual(pin["artifact_id"], 14028929577)
                (operators / "diagnostic_worker.py").write_text("changed")
                with self.assertRaises(Exception):
                    lane.load_pins(source, controller_sha=sha, operator_dir=operators)
                with self.assertRaises(Exception):
                    lane.load_pins(source, controller_sha="a" * 40, operator_dir=operators)

    def test_owned_cleanup_reconciles_only_known_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = root / "cells/aggregate-0-off"
            inputs.mkdir(parents=True)
            run = FakeRun(root / "runs", "diag-aggregate-0-off")
            selected = {"private_output": str(root / "runs"), "run_id": "diag-aggregate-0-off"}
            def collect(*args):
                run.record["phase"] = "collected"
            self.assertTrue(lane.cleanup_owned_runs(
                [(selected, inputs)], owned_root=root, budget=Mock(),
                run_factory=lambda *args: run, docker_factory=lambda profile: Mock(),
                collect=collect))
            self.assertTrue(run.verified)
            self.assertEqual(run.record["phase"], "deleted")
            self.assertFalse(inputs.exists())

    def test_cleanup_failure_or_unrelated_path_preserves_input(self):
        for failure in ("collect", "outside"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                inputs = root / "cells/aggregate-0-off"
                inputs.mkdir(parents=True)
                run = FakeRun(root / "runs", "diag-aggregate-0-off")
                selected = {"private_output": str(root / "runs"), "run_id": "diag-aggregate-0-off"}
                if failure == "outside":
                    inputs = root / "elsewhere/aggregate-0-off"
                    inputs.mkdir(parents=True)
                with self.assertRaisesRegex(ValueError, "CLEANUP"):
                    lane.cleanup_owned_runs(
                        [(selected, inputs)], owned_root=root, budget=Mock(),
                        run_factory=lambda *args: run, docker_factory=lambda profile: Mock(),
                        collect=lambda *args: (_ for _ in ()).throw(ValueError("COLLECT")))
                self.assertTrue(inputs.exists())

    def test_fixed_failure_record_never_prints_private_exception(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "alopex-712-diagnostic"
            root.mkdir(mode=0o700)
            output = io.StringIO()
            with patch.dict("os.environ", {"RUNNER_TEMP": directory}), \
                 patch.object(lane, "execute_diagnostic", side_effect=ValueError("secret path")), \
                 contextlib.redirect_stdout(output):
                self.assertEqual(lane.main(), 1)
            self.assertNotIn("secret path", output.getvalue())
            record = json.loads((root / "diagnostic-failure.private.json").read_text())
            self.assertEqual(record["error_code"], "INTERNAL")
            evidence = root / "export/evidence.cms"
            self.assertTrue(evidence.is_file())
            self.assertGreater(evidence.stat().st_size, 0)
            self.assertEqual({path.name for path in evidence.parent.iterdir()}, {"evidence.cms"})

    def test_known_measurement_failure_is_sealed_as_fixed_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "alopex-712-diagnostic"
            root.mkdir(mode=0o700)
            with patch.dict("os.environ", {"RUNNER_TEMP": directory}), \
                 patch.object(lane, "execute_diagnostic", side_effect=lane.Failure("MEASUREMENT")), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(lane.main(), 1)
            record = json.loads((root / "diagnostic-failure.private.json").read_text())
            self.assertEqual(record["error_code"], "MEASUREMENT")
            self.assertTrue((root / "export/evidence.cms").is_file())


if __name__ == "__main__":
    unittest.main()
