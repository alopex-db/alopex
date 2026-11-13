"""Inspect-only workflow and private CMS exporter checks."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[3]
WORKFLOW = REPO / ".github/workflows/alopex-performance.yml"
SEALER = REPO / "scripts/ops/s04_inspect_seal.py"
SPEC = importlib.util.spec_from_file_location("s04_inspect_seal", SEALER)
sealer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sealer)


class Workflow(unittest.TestCase):
    def test_inspect_only_mode_has_precheckout_gate_and_no_asset_or_capture(self):
        source = WORKFLOW.read_text()
        self.assertIn("inputs.issue_number == '684'", source)
        self.assertIn("!inputs.qualification_mode && !inputs.sql_four_case_mode", source)
        mode = source.split("  s04_inspect:\n", 1)[1]
        self.assertLess(mode.index("Gate exact source before checkout"),
                        mode.index("uses: actions/checkout@v4"))
        self.assertLess(mode.index("Measure selected-host storage before checkout"),
                        mode.index("uses: actions/checkout@v4"))
        self.assertIn("expected == os.environ.get('GITHUB_SHA')", mode)
        self.assertIn("ref: ${{ inputs.expected_controller_sha }}", mode)
        self.assertIn("context=$(/usr/bin/docker context show)", mode)
        self.assertIn("inspect --context \"$context\"", mode)
        self.assertNotIn(" capture ", mode)
        self.assertNotIn("hosted_test_env_qualification.py", mode)
        self.assertNotIn("s04-relay", mode)
        self.assertIn("native-s04-inspect-export/private-evidence.cms", mode)
        self.assertIn("native-s04-inspect-export/public.json", mode)

    def test_workflow_parses_as_yaml(self):
        try:
            import yaml
        except ImportError:
            self.skipTest("PyYAML unavailable")
        parsed = yaml.safe_load(WORKFLOW.read_text())
        self.assertIn("s04_inspect", parsed["jobs"])

    def test_precheckout_sha_and_issue_guard(self):
        try:
            import yaml
        except ImportError:
            self.skipTest("PyYAML unavailable")
        job = yaml.safe_load(WORKFLOW.read_text())["jobs"]["s04_inspect"]
        gate = job["steps"][0]["run"]
        sha = "a" * 40
        base = dict(os.environ, EXPECTED_CONTROLLER_SHA=sha, GITHUB_SHA=sha,
                    ISSUE_NUMBER="684", GITHUB_EVENT_NAME="workflow_dispatch")
        for overrides, expected in (({}, 0), ({"ISSUE_NUMBER": "804"}, 1),
                                    ({"GITHUB_SHA": "b" * 40}, 1),
                                    ({"EXPECTED_CONTROLLER_SHA": "not-a-sha"}, 1)):
            with self.subTest(overrides=overrides):
                result = subprocess.run(["bash", "-e", "-c", gate],
                                        env=dict(base, **overrides), capture_output=True, text=True)
                self.assertEqual(result.returncode, expected)
                self.assertNotIn(sha, result.stdout + result.stderr)

    def test_storage_gate_fails_with_fixed_code_when_roots_are_unavailable(self):
        try:
            import yaml
        except ImportError:
            self.skipTest("PyYAML unavailable")
        job = yaml.safe_load(WORKFLOW.read_text())["jobs"]["s04_inspect"]
        gate = job["steps"][1]["run"]
        self.assertIn("aggregate + 1_000_000_000 < 40_000_000_000", gate)
        self.assertIn("/usr/bin/du", gate)
        env = dict(os.environ)
        env.pop("RUNNER_TEMP", None)
        result = subprocess.run(["bash", "-e", "-c", gate], env=env,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout),
                         {"case": "S04_INSPECT_STORAGE", "outcome": "failed",
                          "error_code": "MEASUREMENT"})
        self.assertEqual(result.stderr, "")


class Sealer(unittest.TestCase):
    def setup_paths(self, base):
        private = base / "alopex-s04-inspect"
        export = base / "export"
        private.mkdir(mode=0o700)
        export.mkdir(mode=0o700)
        os.chmod(private, 0o700)
        os.chmod(export, 0o700)
        return private, export

    def test_success_seals_and_deletes_plaintext(self):
        if not shutil.which("openssl"):
            self.skipTest("OpenSSL unavailable")
        with tempfile.TemporaryDirectory() as directory:
            private, export = self.setup_paths(Path(directory))
            receipt = private / "inspector.private.json"
            receipt.write_text(json.dumps({"read_only": True, "endpoint": "private"}))
            receipt.chmod(0o600)
            public, code = sealer.seal(private, export, "success")
            self.assertEqual(code, 0)
            self.assertEqual(public["outcome"], "inspected_read_only")
            self.assertEqual(public["ciphertext_sha256"],
                             sealer.digest(export / "private-evidence.cms"))
            self.assertFalse(private.exists())
            self.assertNotIn(b'"endpoint": "private"',
                             (export / "private-evidence.cms").read_bytes())
            parsed = subprocess.run(["openssl", "cms", "-cmsout", "-print", "-inform", "DER",
                                     "-in", str(export / "private-evidence.cms")],
                                    capture_output=True, check=False)
            self.assertEqual(parsed.returncode, 0)

    def test_failed_inspect_cleans_plaintext_and_emits_fixed_code(self):
        with tempfile.TemporaryDirectory() as directory:
            private, export = self.setup_paths(Path(directory))
            receipt = private / "inspector.private.json"
            receipt.write_text("sensitive detail")
            receipt.chmod(0o600)
            public, code = sealer.seal(private, export, "failure")
            self.assertEqual((code, public["error_code"]), (1, "INSPECT_FAILED"))
            self.assertFalse(private.exists())
            self.assertFalse((export / "private-evidence.cms").exists())
            self.assertNotIn("sensitive", json.dumps(public))

    def test_wrong_cert_blocks_export_and_cleans_plaintext(self):
        with tempfile.TemporaryDirectory() as directory:
            private, export = self.setup_paths(Path(directory))
            receipt = private / "inspector.private.json"
            receipt.write_text('{"read_only":true}')
            receipt.chmod(0o600)
            with patch.object(sealer, "CERT_SHA256", "0" * 64):
                public, code = sealer.seal(private, export, "success")
            self.assertEqual((code, public["error_code"]), (1, "CERT_MISMATCH"))
            self.assertFalse(private.exists())
            self.assertFalse((export / "private-evidence.cms").exists())


if __name__ == "__main__":
    unittest.main()
