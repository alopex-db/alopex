"""An old-reader abort is evidence, not a successful read or a lost fixture."""

from pathlib import Path
import sys
import tempfile
import unittest

from scripts.compatibility.legacy_probe import observe


class LegacyProbeTests(unittest.TestCase):
    def test_success_preserves_observed_rows(self):
        result = observe(
            [sys.executable, "-c", 'import json; print(json.dumps({"rows": [1, 2]}))']
        )
        self.assertEqual(result, {"status": "observed", "result": {"rows": [1, 2]}})

    def test_abort_is_reported_and_does_not_remove_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture"
            fixture.write_bytes(b"old database bytes")
            result = observe([sys.executable, "-c", "import os; os.abort()"])
            self.assertEqual(result["status"], "process_failed")
            self.assertNotEqual(result["returncode"], 0)
            self.assertEqual(fixture.read_bytes(), b"old database bytes")

    def test_timeout_is_not_a_successful_read(self):
        result = observe(
            [sys.executable, "-c", "import time; time.sleep(60)"], timeout=0.1
        )
        self.assertEqual(result["status"], "timeout")

    def test_missing_json_is_not_a_successful_read(self):
        result = observe([sys.executable, "-c", "print('no report')"])
        self.assertEqual(result["status"], "invalid_report")


if __name__ == "__main__":
    unittest.main()
