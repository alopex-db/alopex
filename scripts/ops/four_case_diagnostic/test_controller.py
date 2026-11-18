"""Offline same-job controller boundary checks; no server or container."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from . import adapter, controller
from .test_qualification import fixture, JOB


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.lock, self.proof, self.receipt = fixture()
        lock_dir = self.root / "alopex-712-lock"
        lock_dir.mkdir(mode=0o700)
        self.lock_path = lock_dir / "lock.json"
        lock_bytes = json.dumps(self.lock).encode()
        self.lock_path.write_bytes(lock_bytes)
        self.lock_path.chmod(0o600)
        self.manifest = {
            "schema": controller.SCHEMA,
            "lock_sha256": hashlib.sha256(lock_bytes).hexdigest(),
            "source_commit": self.lock["pin"]["source_commit"],
            "source_tree": self.lock["pin"]["source_tree"],
            "assets_digest": self.lock["pin"]["assets_digest"],
            "fixture_sha256": "a" * 64,
            "package_files": {f"four_case_diagnostic/{name}.py": "b" * 64 for name in
                              ("__init__", "adapter", "log_parser", "operation",
                               "oracle", "runner", "worker")}}
        self.manifest_path = self.root / "manifest.json"
        self.output = self.root / "output"
        self.env = {"GITHUB_RUN_ID": JOB, "GITHUB_RUN_ATTEMPT": "1",
                    "GITHUB_ACTIONS": "true",
                    "RUNNER_ENVIRONMENT": "github-hosted",
                    "RUNNER_TEMP": str(self.root),
                    "ALOPEX_FOUR_CASE_LOCK_SHA256": self.manifest["lock_sha256"]}

    def save(self):
        data = json.dumps(self.manifest).encode()
        self.manifest_path.write_bytes(data)
        self.manifest_path.chmod(0o600)
        return hashlib.sha256(data).hexdigest()

    def invoke(self, digest, stage):
        return controller.run_current_job(
            self.manifest_path, digest, repo=self.root, runner_temp=self.root,
            environment=self.env, output=self.output, operations={},
            stage_pair=stage)

    def staged(self):
        return {"start": lambda: None, "identity_of": lambda _: None,
                "subject_environment": {}, "python": str(self.root / "python"),
                "package_root": str(self.root / "package"),
                "docs_harness_root": str(self.root / "docs"),
                "product_harness_root": str(self.root / "product")}

    def test_manifest_requires_independent_digest_and_private_mode(self):
        digest = self.save()
        self.assertEqual(controller.load_manifest(self.manifest_path, digest), self.manifest)
        self.assertFalse({"job_id", "run_attempt", "lock_path", "package_root",
                          "docs_harness_root", "product_harness_root", "python"}
                         & set(self.manifest))
        with self.assertRaisesRegex(ValueError, "PRIVATE_MANIFEST"):
            controller.load_manifest(self.manifest_path, "0" * 64)
        self.manifest_path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "PRIVATE_MANIFEST"):
            controller.load_manifest(self.manifest_path, digest)

    def test_rejects_dynamic_path_in_reviewed_manifest(self):
        self.manifest["lock_path"] = str(self.lock_path)
        digest = self.save()
        with self.assertRaisesRegex(ValueError, "PRIVATE_MANIFEST"):
            controller.load_manifest(self.manifest_path, digest)

    def test_rejects_lock_digest_not_matching_independent_environment_pin(self):
        digest = self.save()
        self.env["ALOPEX_FOUR_CASE_LOCK_SHA256"] = "0" * 64
        with patch.object(controller.entrypoint, "attest_current_job") as attest:
            with self.assertRaisesRegex(ValueError, "SAME_JOB"):
                self.invoke(digest, lambda *args: None)
        attest.assert_not_called()

    def test_same_job_attestation_precedes_staging_and_runs_all_pairs(self):
        digest = self.save()
        staged = []
        def stage(config, output, proof, receipt):
            staged.append((config["case"], config["trial"], output))
            return self.staged()
        with patch.object(controller.entrypoint, "attest_current_job",
                          return_value=(self.proof, self.receipt)) as attest, \
             patch.object(controller.supervisor, "run_owned_pair",
                          return_value={"status": "passed", "primary_error": None,
                                        "worker_cleanup_error": None,
                                        "stop_error": None}) as pair:
            result = self.invoke(digest, stage)
        self.assertEqual(result["pair_count"], 12)
        self.assertEqual(result["run_attempt"], "1")
        self.assertFalse(result["performance_qualification"])
        self.assertEqual([(case, trial) for case, trial, _ in staged],
                         [(case, trial) for case in adapter.CASES for trial in range(3)])
        self.assertEqual(pair.call_count, 12)
        self.assertEqual(attest.call_count, 1)
        self.assertTrue((self.output / "receipt.private.json").is_file())
        self.assertEqual(controller.verify_public_receipt(
            self.output, job_id=JOB, run_attempt="1",
            binding_sha256=self.receipt["binding_sha256"]), result)
        with self.assertRaisesRegex(ValueError, "RECEIPT_IDENTITY"):
            controller.verify_public_receipt(
                self.output, job_id=JOB, run_attempt="2",
                binding_sha256=self.receipt["binding_sha256"])

    def test_invalid_attempt_fails_closed_before_attestation(self):
        digest = self.save()
        self.env["GITHUB_RUN_ATTEMPT"] = "0"
        with patch.object(controller.entrypoint, "attest_current_job") as attest:
            with self.assertRaisesRegex(ValueError, "SAME_JOB"):
                self.invoke(digest, lambda *args: None)
        attest.assert_not_called()
        self.assertEqual(json.loads((self.output / "receipt.public.json").read_text())
                         ["status"], "failed")

    def test_failed_pair_never_emits_success_receipt(self):
        digest = self.save()
        staged = lambda *args: self.staged()
        with patch.object(controller.entrypoint, "attest_current_job",
                          return_value=(self.proof, self.receipt)), \
             patch.object(controller.supervisor, "run_owned_pair",
                          return_value={"status": "failed", "primary_error": "PAIR_FAILED",
                                        "worker_cleanup_error": None,
                                        "stop_error": None}):
            with self.assertRaisesRegex(ValueError, "PAIR_FAILED"):
                self.invoke(digest, staged)
        failure = json.loads((self.output / "receipt.private.json").read_text())
        self.assertEqual(failure["status"], "failed")
        public = json.loads((self.output / "receipt.public.json").read_text())
        self.assertEqual(public["status"], "failed")
        self.assertEqual(public["pair_count"], 0)

    def test_reinvocation_replaces_stale_success_with_failed_attempt(self):
        digest = self.save()
        self.output.mkdir(mode=0o700)
        for name in ("receipt.private.json", "receipt.public.json"):
            path = self.output / name
            path.write_text(json.dumps({"status": "passed", "run_attempt": "1"}))
            path.chmod(0o600)
        self.env["GITHUB_RUN_ATTEMPT"] = "2"
        with patch.object(controller.entrypoint, "attest_current_job",
                          return_value=(self.proof, self.receipt)) as attest:
            with self.assertRaises(FileExistsError):
                self.invoke(digest, lambda *args: None)
        self.assertEqual(attest.call_count, 1)
        public = json.loads((self.output / "receipt.public.json").read_text())
        private = json.loads((self.output / "receipt.private.json").read_text())
        self.assertEqual(public["status"], "failed")
        self.assertEqual(public["run_attempt"], "2")
        self.assertEqual(private["status"], "failed")
        with self.assertRaisesRegex(ValueError, "RECEIPT_IDENTITY"):
            controller.verify_public_receipt(
                self.output, job_id=JOB, run_attempt="2",
                binding_sha256=self.receipt["binding_sha256"])

    def test_same_attempt_reinvocation_invalidates_old_success(self):
        digest = self.save()
        self.output.mkdir(mode=0o700)
        for name in ("receipt.private.json", "receipt.public.json"):
            path = self.output / name
            path.write_text(json.dumps({"status": "passed", "run_attempt": "1"}))
            path.chmod(0o600)
        with patch.object(controller.entrypoint, "attest_current_job",
                          return_value=(self.proof, self.receipt)):
            with self.assertRaises(FileExistsError):
                self.invoke(digest, lambda *args: None)
        self.assertEqual(json.loads((self.output / "receipt.public.json").read_text())
                         ["status"], "failed")


if __name__ == "__main__":
    unittest.main()
