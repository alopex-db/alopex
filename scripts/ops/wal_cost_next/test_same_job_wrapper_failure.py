"""Fixed private diagnostics for pre-comparator capacity failures."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from common import Failure
from same_job_wrapper import seal_wrapper_failure


class WrapperFailureTests(unittest.TestCase):
    def test_records_fixed_capture_gate_without_exposing_detail(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / "alopex-804-qualification"
            private.mkdir(mode=0o700)
            error = Failure("CAPACITY", "host_growth")
            error.wrapper_phase = "capture_signed_indexes"
            error.capture_stage = "capture_resolver"
            with patch.dict(os.environ, {"RUNNER_TEMP": directory, "GITHUB_RUN_ID": "42"}):
                seal_wrapper_failure(error)
            receipt = json.loads((private / "wrapper-result.private.json").read_text())
            self.assertEqual(receipt["run_id"], "42")
            self.assertEqual(receipt["wrapper_phase"], "capture_signed_indexes")
            self.assertEqual(receipt["capture_stage"], "capture_resolver")
            self.assertEqual(receipt["capacity_gate"], "host_growth")
            self.assertEqual(receipt["original_error"], "Failure('CAPACITY')")

    def test_unknown_detail_is_not_copied_into_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / "alopex-804-qualification"
            private.mkdir(mode=0o700)
            error = Failure("CAPACITY", "private-path-or-untrusted-text")
            error.capture_stage = "private-path-or-untrusted-text"
            with patch.dict(os.environ, {"RUNNER_TEMP": directory, "GITHUB_RUN_ID": "42"}):
                seal_wrapper_failure(error)
            receipt = json.loads((private / "wrapper-result.private.json").read_text())
            self.assertIsNone(receipt["capacity_gate"])
            self.assertIsNone(receipt["capture_stage"])
            self.assertNotIn("private-path", json.dumps(receipt))


if __name__ == "__main__":
    unittest.main()
