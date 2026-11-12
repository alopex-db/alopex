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
                self.assertEqual(pin["artifact_id"], 13916314157)
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


if __name__ == "__main__":
    unittest.main()
