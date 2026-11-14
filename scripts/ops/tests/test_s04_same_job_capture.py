"""S04 capture routing and evidence checks without Docker access."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
SPEC = importlib.util.spec_from_file_location("s04_same_job_capture", HERE / "s04_same_job_capture.py")
capture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(capture)
from common import Failure  # noqa: E402
SPEC = importlib.util.spec_from_file_location("s04_capture_seal", HERE / "s04_capture_seal.py")
sealer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sealer)


class SameJob(unittest.TestCase):
    def test_rejects_unqualified_job_before_profile_or_daemon(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "GITHUB_ACTIONS": "false", "RUNNER_ENVIRONMENT": "github-hosted",
                "GITHUB_SHA": "a" * 40, "EXPECTED_CONTROLLER_SHA": "a" * 40}):
            with self.assertRaises(Failure):
                capture.accepted_inputs(Path(directory))

    def test_workflow_routes_only_684_capture_after_qualification(self):
        source = (HERE.parents[1] / ".github/workflows/alopex-performance.yml").read_text()
        self.assertIn("inputs.issue_number == '684' && inputs.s04_capture_mode", source)
        self.assertLess(source.index("--execute"), source.index("s04_same_job_capture.py"))
        self.assertIn("--cli /usr/bin/docker", source)
        self.assertIn("native-s04-wire-export/private-evidence.cms", source)

    def test_fake_version_uses_selected_api(self):
        status, payload = capture.wire.response(b"GET /version HTTP/1.1", "1.48", "28.0.4")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload)["ApiVersion"], "1.48")


class Sealing(unittest.TestCase):
    def test_seals_offline_receipt_then_removes_plaintext(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / "private"
            export = Path(directory) / "export"
            private.mkdir(mode=0o700)
            receipt = private / "s04-wire.private.json"
            receipt.write_text(json.dumps({"fake_only": True, "outcome": "CAPTURE_COMPLETE"}))
            receipt.chmod(0o600)
            public, code = sealer.seal(private, export, "success")
            self.assertEqual((public["outcome"], code), ("captured_offline", 0))
            self.assertTrue((export / "private-evidence.cms").is_file())
            self.assertFalse(receipt.exists())

    def test_failed_capture_exposes_only_fixed_status(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / "private"
            export = Path(directory) / "export"
            private.mkdir(mode=0o700)
            receipt = private / "s04-wire.private.json"
            receipt.write_text("private wire")
            receipt.chmod(0o600)
            public, code = sealer.seal(private, export, "failure")
            self.assertEqual((public["error_code"], code), ("SEAL_FAILED", 1))
            self.assertFalse(receipt.exists())
            self.assertFalse((export / "private-evidence.cms").exists())

    def test_incomplete_capture_keeps_only_encrypted_diagnostic(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / "private"
            export = Path(directory) / "export"
            private.mkdir(mode=0o700)
            receipt = private / "s04-wire.private.json"
            receipt.write_text(json.dumps({"fake_only": True,
                                           "outcome": "CAPTURE_INCOMPLETE",
                                           "records": [{"body_hex": "private"}]}))
            receipt.chmod(0o600)
            public, code = sealer.seal(private, export, "failure")
            self.assertEqual((public["error_code"], code), ("CAPTURE_INCOMPLETE", 1))
            self.assertTrue((export / "private-evidence.cms").is_file())
            self.assertFalse(receipt.exists())


if __name__ == "__main__":
    unittest.main()
