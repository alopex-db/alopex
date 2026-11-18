"""Offline workflow handoff contract for the #712 same-job four-case lane."""
import base64
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/ops"))
import four_case_job as job
from four_case_diagnostic.test_qualification import fixture


class FourCaseJobTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(strict=True)
        lock, _, _ = fixture()
        lock_raw = json.dumps(lock).encode()
        self.lock_sha = hashlib.sha256(lock_raw).hexdigest()
        manifest = {"schema": job.controller.SCHEMA,
                    "lock_sha256": self.lock_sha,
                    "source_commit": lock["pin"]["source_commit"],
                    "source_tree": lock["pin"]["source_tree"],
                    "assets_digest": lock["pin"]["assets_digest"],
                    "fixture_sha256": "a" * 64,
                    "package_files": {f"four_case_diagnostic/{name}.py": "b" * 64
                                      for name in ("__init__", "adapter",
                                                   "container_pair_entrypoint", "log_parser",
                                                   "operation", "oracle", "runner",
                                                   "supervisor", "worker")},
                    "docs_files": {"subject_identity.py": "c" * 64,
                                   "sql_resources.py": "d" * 64},
                    "product_files": {"parity_v0811_measure.py": "e" * 64}}
        manifest_raw = json.dumps(manifest).encode()
        self.env = {"RUNNER_TEMP": str(self.root),
                    "ALOPEX_FOUR_CASE_LOCK_SHA256": self.lock_sha,
                    "ALOPEX_FOUR_CASE_LOCK_B64": base64.b64encode(lock_raw).decode(),
                    "ALOPEX_FOUR_CASE_MANIFEST_SHA256":
                        hashlib.sha256(manifest_raw).hexdigest(),
                    "ALOPEX_FOUR_CASE_MANIFEST_B64":
                        base64.b64encode(manifest_raw).decode()}

    def test_install_requires_independent_hashes_and_private_fixed_paths(self):
        manifest = job.install_inputs(self.env)
        directory = self.root / "alopex-712-lock"
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        for name in ("lock.json", "manifest.json"):
            self.assertEqual((directory / name).stat().st_mode & 0o777, 0o600)
        self.assertEqual(manifest["lock_sha256"], self.lock_sha)

    def test_install_rejects_changed_bytes_before_file_creation(self):
        self.env["ALOPEX_FOUR_CASE_LOCK_SHA256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            job.install_inputs(self.env)
        self.assertFalse((self.root / "alopex-712-lock").exists())

    def test_install_rejects_noncanonical_base64_and_dynamic_manifest(self):
        self.env["ALOPEX_FOUR_CASE_LOCK_B64"] += "\n"
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            job.install_inputs(self.env)
        self.assertFalse((self.root / "alopex-712-lock").exists())

    def test_workflow_routes_only_four_case_lane_and_uploads_only_cms(self):
        source = (REPO / ".github/workflows/alopex-performance.yml").read_text()
        qualification = source.split("  qualification:\n", 1)[1].split(
            "  wal_cost_682:\n", 1)[0]
        for phase in ("--admission-only", "--acquire-assets", "--execute"):
            self.assertIn("hosted_test_env_qualification_four_case.py " + phase,
                          qualification)
        self.assertIn("scripts/ops/four_case_job.py install", qualification)
        self.assertIn("scripts/ops/four_case_job.py run", qualification)
        self.assertIn("scripts/ops/four_case_job.py seal", qualification)
        self.assertNotIn("scripts/ops/same_job_four_case.py", qualification)
        self.assertIn("native-sql-four-case-export/private-evidence.cms",
                      qualification)
        self.assertIn("native-sql-four-case-export/public.json", qualification)
        self.assertNotIn("native-sql-four-case-export/\n", qualification)
        self.assertIn("if: ${{ !inputs.sql_diagnostic_mode && !inputs.sql_four_case_mode }}",
                      qualification)

    def test_seal_allows_bounded_twelve_pair_inventory_and_checks_capacity(self):
        private = self.root / job.MATRIX_DIR
        private.mkdir(mode=0o700)
        for index in range(260):
            path = private / f"member-{index}.json"
            path.write_text("{}")
            path.chmod(0o600)
        cert = self.root / "cert.pem"
        cert.write_bytes(b"test certificate")
        checks = []
        def capacity(root, extra):
            checks.append(extra)
            return {"stop_bytes": 40_000_000_000}
        def encrypt(argv, **kwargs):
            Path(argv[argv.index("-out") + 1]).write_bytes(b"ciphertext")
            return SimpleNamespace(returncode=0)
        with patch.object(job, "REPO", self.root), \
             patch.object(job, "CERT", cert), \
             patch.object(job, "CERT_SHA256", hashlib.sha256(cert.read_bytes()).hexdigest()), \
             patch.object(job, "_capacity_before_seal", side_effect=capacity), \
             patch.object(job.subprocess, "run", side_effect=encrypt):
            public = job.seal_job(self.env)
        self.assertEqual(public["status"], "sealed")
        self.assertEqual(len(checks), 3)
        self.assertTrue((self.root / "native-sql-four-case-export/private-evidence.cms").is_file())
        self.assertFalse((private / "evidence.tar.gz").exists())

    def test_run_attests_before_docs_stage_and_controller(self):
        from four_case_diagnostic import docs_stage
        order = []
        self.env["GITHUB_RUN_ID"] = "12345678"
        manifest = {"lock_sha256": self.lock_sha, "docs_files": {
            "subject_identity.py": "c" * 64, "sql_resources.py": "d" * 64}}
        with patch.object(job.controller, "load_manifest", return_value=manifest), \
             patch.object(job.entrypoint, "load_reviewed_lock",
                          return_value={"pin": {"assets_digest": "a" * 64}}), \
             patch.object(job.entrypoint, "controller_operations", return_value={}), \
             patch.object(job.entrypoint, "attest_current_job",
                          side_effect=lambda *a, **kw: order.append("attest")), \
             patch.object(docs_stage, "stage_docs",
                          side_effect=lambda *a, **kw: order.append("docs")), \
             patch.object(job.stage_pair, "make_stage_pair", return_value=lambda *a: None), \
             patch.object(job.controller, "run_current_job",
                          side_effect=lambda *a, **kw: order.append("matrix")):
            job.run_job(self.env)
        self.assertEqual(order, ["attest", "docs", "matrix"])


if __name__ == "__main__":
    unittest.main()
