"""Offline checks for the reviewed-lock and live-attestor handoff boundaries."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from . import entrypoint
from .test_qualification import fixture, JOB


class EntrypointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "reviewed.json"
        self.lock, self.proof, self.receipt = fixture()

    def save(self, value=None):
        raw = (json.dumps(self.lock if value is None else value,
                          sort_keys=True) + "\n").encode()
        self.path.write_bytes(raw)
        return hashlib.sha256(raw).hexdigest()

    def test_lock_requires_independent_exact_digest(self):
        expected = self.save()
        self.assertEqual(entrypoint.load_reviewed_lock(self.path, expected), self.lock)
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            entrypoint.load_reviewed_lock(self.path, "0" * 64)
        self.path.write_bytes(self.path.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            entrypoint.load_reviewed_lock(self.path, expected)

    def test_lock_rejects_duplicate_keys_and_symlink(self):
        raw = b'{"schema":"x","schema":"y","pin":{}}'
        self.path.write_bytes(raw)
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            entrypoint.load_reviewed_lock(self.path, hashlib.sha256(raw).hexdigest())
        self.path.unlink()
        target = Path(self.temp.name) / "target.json"
        target.write_bytes(b"{}")
        self.path.symlink_to(target)
        with self.assertRaises(OSError):
            entrypoint.load_reviewed_lock(self.path,
                                          hashlib.sha256(target.read_bytes()).hexdigest())

    def test_acquire_forwards_pinned_job_to_verifier(self):
        with patch.object(entrypoint.acquire, "acquire", return_value={"ok": True}) as called:
            fetch = lambda path, **kwargs: None
            result = entrypoint.acquire_current_job(
                self.lock, job_id=JOB, directory=self.path, fetch=fetch)
        self.assertEqual(result, {"ok": True})
        self.assertEqual(called.call_args.kwargs,
                         {"job_id": JOB, "directory": self.path, "fetch": fetch})

    def test_controller_loader_rejects_missing_four_case_controller(self):
        repo = Path(self.temp.name) / "repo"
        (repo / "scripts/ops").mkdir(parents=True)
        (repo / "scripts/test-env").mkdir(parents=True)
        self.lock["pin"]["operator_sha256"][
            "hosted_test_env_qualification_four_case.py"] = "a" * 64
        with patch.object(entrypoint.importlib, "import_module") as imported:
            with self.assertRaisesRegex(ValueError, "OPERATOR_IDENTITY"):
                entrypoint.controller_operations(repo, self.lock)
        imported.assert_not_called()

    def test_cli_only_validates_lock(self):
        digest = self.save()
        with patch.object(entrypoint, "acquire_current_job") as acquire, \
             patch.object(entrypoint, "attest_current_job") as attest:
            self.assertEqual(entrypoint.main(["--lock", str(self.path),
                                              "--lock-sha256", digest]), 0)
            acquire.assert_not_called()
            attest.assert_not_called()


if __name__ == "__main__":
    unittest.main()
