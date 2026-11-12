"""Offline SQL diagnostic coverage; no Docker or network calls."""
import sys
from pathlib import Path as _BootstrapPath
_OPS = _BootstrapPath(__file__).resolve().parents[1]
sys.path.insert(0, str(_OPS))
sys.path.insert(0, str(_OPS.parent / "test-env"))

import json
from pathlib import Path
import re
import subprocess
import unittest

HERE = Path(__file__).resolve().parents[1]
WORKFLOW = HERE.parents[1] / ".github/workflows/alopex-performance.yml"
LOCK = HERE / "sql-diagnostic-lock.json"


class WorkflowDiagnosticTests(unittest.TestCase):
    def test_same_job_qualification_then_cms_only_diagnostic(self):
        ruby = ('require "yaml"; require "json"; '
                'd=YAML.load_file(ARGV.fetch(0)); puts JSON.generate(d)')
        doc = json.loads(subprocess.check_output(
            ["ruby", "-e", ruby, str(WORKFLOW)], text=True))
        steps = doc["jobs"]["qualification"]["steps"]
        names = [step.get("name") for step in steps]
        self.assertLess(names.index("Admit runner for exact 1951 diagnostic qualification"),
                        names.index("Retrieve exact 1951 diagnostic assets"))
        self.assertLess(names.index("Retrieve exact 1951 diagnostic assets"),
                        names.index("Qualify 1951 on this runner"))
        self.assertLess(names.index("Qualify 1951 on this runner"),
                        names.index("Run paired 12-cell SQL stage diagnostic"))
        self.assertLess(names.index("Run paired 12-cell SQL stage diagnostic"),
                        names.index("Preserve encrypted diagnostic evidence"))
        upload = steps[names.index("Preserve encrypted diagnostic evidence")]
        self.assertEqual(upload["with"]["path"],
                         "${{ runner.temp }}/alopex-712-diagnostic/export/evidence.cms")
        self.assertEqual(upload["if"],
                         "inputs.sql_diagnostic_mode && steps.diagnostic.outcome == 'success'")
        self.assertNotIn("plain", upload["with"]["path"])
        self.assertIn("sql_diagnostic_mode", doc.get("on", doc.get("true"))["workflow_dispatch"]["inputs"])

    def test_nonsecret_source_lock_and_cms_upload(self):
        lock = json.loads(LOCK.read_text())
        self.assertLessEqual(LOCK.stat().st_size, 16_384)
        self.assertEqual(set(lock), {"schema", "pin"})
        pin = lock["pin"]
        self.assertNotIn("controller_commit", pin)
        for key, value in pin.items():
            self.assertNotIn(key, {"hostname", "runner_label", "profile", "machine_path",
                                   "docker_root", "workspace_root", "receipt"})
            if isinstance(value, str):
                self.assertRegex(value, r"\A[a-f0-9]{40}(?:[a-f0-9]{24})?\Z")
            elif isinstance(value, dict):
                self.assertEqual(key, "operator_sha256")
                self.assertTrue(all(re.fullmatch(r"[a-f0-9]{64}", digest)
                                    for digest in value.values()))
            else:
                self.assertIsInstance(value, int)
        workflow = WORKFLOW.read_text()
        self.assertNotIn("sql-diagnostic-pins.private.json", workflow)
        self.assertNotIn("diagnostic-failure.private.json", workflow)
        self.assertNotIn("diagnostic-failure.private.json", str(lock))
        self.assertEqual(pin["controller_digest"],
                         "51bd5396d5448a53c53fec282856ac6a2e6492180e484dbe9eae0361e6bd2fa3")
        self.assertEqual(pin["source_commit"],
                         "1951a3fdfe086ea6e8bc537c16766f91f619396f")


if __name__ == "__main__":
    unittest.main()
