"""Synthetic public/private finalization checks for one Stage-2 case."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import stress_finalize_case as finalizer
from test_stress_private_evidence import certificate_pair, make_checkout


def prepare(root: Path, with_report: bool) -> dict:
    workspace = root / "checkout"
    workspace.mkdir()
    source = make_checkout(workspace)
    if with_report:
        reports = workspace / "target/stress-reports"
        reports.mkdir(parents=True)
        (reports / "run.json").write_bytes(b"private-machine-detail")
        private = root / "stress-private"
        private.mkdir(mode=0o700)
        (private / "stress-test.output.private.log").write_bytes(b"private-run-log")
    cert, _ = certificate_pair(root)
    return dict(workspace=workspace, runner_temp=root, case_id="stress",
                source=source, certificate=cert,
                expected_cert_sha256=hashlib.sha256(cert.read_bytes()).hexdigest())


class FinalizeCaseTests(unittest.TestCase):
    def test_passed_case_emits_only_fixed_status_and_cms(self):
        with tempfile.TemporaryDirectory() as directory:
            args = prepare(Path(directory), True)
            status_path, cms, passed = finalizer.finalize(outcome="passed", **args)
            self.assertTrue(passed)
            self.assertTrue(cms.is_file())
            status = json.loads(status_path.read_bytes())
            self.assertEqual(status["outcome"], "passed")
            self.assertEqual(status["error_code"], "NONE")
            self.assertEqual(status["source_commit"], args["source"])
            self.assertEqual(status["ciphertext_sha256"],
                             hashlib.sha256(cms.read_bytes()).hexdigest())
            self.assertNotIn(b"private-machine-detail", status_path.read_bytes())

    def test_failed_case_keeps_encrypted_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            args = prepare(Path(directory), True)
            status_path, cms, passed = finalizer.finalize(outcome="failed", **args)
            self.assertFalse(passed)
            self.assertTrue(cms.is_file())
            status = json.loads(status_path.read_bytes())
            self.assertEqual((status["outcome"], status["error_code"]),
                             ("failed", "TEST"))

    def test_empty_raw_failure_has_fixed_status_without_cms(self):
        with tempfile.TemporaryDirectory() as directory:
            args = prepare(Path(directory), False)
            status_path, cms, passed = finalizer.finalize(outcome="passed", **args)
            self.assertFalse(passed)
            self.assertIsNone(cms)
            status = json.loads(status_path.read_bytes())
            self.assertEqual((status["outcome"], status["error_code"]),
                             ("failed", "EVIDENCE"))
            self.assertEqual(set(status), {"schema", "case_id", "source_commit",
                                           "outcome", "error_code"})

    def test_successful_fuzz_with_only_private_command_log_is_sealed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare(root, False)
            private = root / "stress-private"
            private.mkdir(mode=0o700)
            (private / "sql_parser-test.output.private.log").write_bytes(
                b"libFuzzer completed 600 seconds")
            status_path, cms, passed = finalizer.finalize(
                outcome="passed", **{**args, "case_id": "sql_parser"})
            self.assertTrue(passed)
            self.assertIsNotNone(cms)
            self.assertEqual(json.loads(status_path.read_bytes())["error_code"],
                             "NONE")

    def test_wrong_source_does_not_publish_failure_status(self):
        with tempfile.TemporaryDirectory() as directory:
            args = prepare(Path(directory), False)
            with self.assertRaises(ValueError):
                finalizer.finalize(outcome="failed",
                                   **{**args, "source": "0" * 40})
            self.assertFalse((args["workspace"] / "target/stress-publish").exists())

    def test_cli_invalid_case_uses_fixed_failure_code(self):
        result = subprocess.run([
            sys.executable, str(Path(finalizer.__file__)),
            "--case", "private-machine-path", "--outcome", "passed",
        ], capture_output=True, text=True, env={"GITHUB_SHA": "0" * 40})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr.strip(),
                         "STRESS_CASE_FINALIZATION_REJECTED")
        self.assertNotIn("private-machine-path", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
