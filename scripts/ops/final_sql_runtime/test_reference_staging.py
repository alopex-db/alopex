"""Focused offline checks for the pre-upload staging guard."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from . import reference_staging as staging


class ReferenceStagingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve() / "originals"
        self.directory.mkdir(mode=0o700)
        self.run = {"run_id": 42, "run_attempt": 1, "event": "workflow_dispatch",
                    "head_sha": "a" * 40, "repository_id": 7, "workflow_id": 9}
        self.expected = {"head_sha": "a" * 40, "repository_id": 7, "workflow_id": 9}
        self.lock = {"schema": "alopex.reference-original-staging/v1", "originals": {}}
        for subject in staging.SUBJECTS:
            name = f"{subject}-original.zip"
            data = subject.encode()
            self.lock["originals"][subject] = {
                "basename": name, "zip_sha256": hashlib.sha256(data).hexdigest(),
                "zip_size_bytes": len(data)}
            path = self.directory / name
            path.write_bytes(data)
            path.chmod(0o400)

    def verify(self):
        return staging.verify_staging(self.directory, run=self.run,
                                      expected=self.expected, lock=self.lock)

    def rejects(self, code):
        with self.assertRaisesRegex(ValueError, f"^{code}$"):
            self.verify()

    def test_success_exposes_only_reviewed_basename_and_pins(self):
        result = self.verify()
        self.assertEqual(result["status"], "verified")
        self.assertEqual(result["originals"], self.lock["originals"])
        self.assertNotIn(str(self.directory), json.dumps(result))

    def test_changed_bytes_same_size_rejected(self):
        path = self.directory / "before-original.zip"
        path.chmod(0o600)
        path.write_bytes(b"xxxxxx")
        path.chmod(0o400)
        self.rejects("ORIGINAL_BYTES")

    def test_extra_file_and_missing_file_rejected(self):
        extra = self.directory / "extra"
        extra.write_bytes(b"x")
        self.rejects("STAGING_INVENTORY")
        extra.unlink()
        (self.directory / "before-original.zip").unlink()
        self.rejects("STAGING_INVENTORY")

    def test_symlink_rejected(self):
        path = self.directory / "before-original.zip"
        path.unlink()
        path.symlink_to(self.directory / "published-original.zip")
        with self.assertRaises(OSError):
            self.verify()

    def test_symlink_in_staging_ancestry_is_rejected(self):
        original = self.directory
        alias = original.parent / "alias"
        alias.symlink_to(original.parent, target_is_directory=True)
        self.directory = alias / "originals"
        try:
            self.rejects("STAGING_PATH")
        finally:
            self.directory = original
        for subject in staging.SUBJECTS:
            name = self.lock["originals"][subject]["basename"]
            self.assertEqual(subject.encode(), (original / name).read_bytes())

    def test_file_and_directory_modes_rejected(self):
        path = self.directory / "before-original.zip"
        path.chmod(0o600)
        self.rejects("ORIGINAL_PRIVACY")
        path.chmod(0o400)
        self.directory.chmod(0o755)
        self.rejects("STAGING_PRIVACY")

    def test_wrong_workflow_identity_rejected(self):
        for key, value in (("head_sha", "b" * 40), ("repository_id", 8),
                           ("workflow_id", 10), ("event", "push"),
                           ("run_attempt", 2)):
            original = self.run[key]
            self.run[key] = value
            self.rejects("WORKFLOW_IDENTITY")
            self.run[key] = original
        self.expected["head_sha"] = "b" * 40
        self.rejects("WORKFLOW_IDENTITY")

    def test_lock_rejects_extra_subject_or_unsafe_basename(self):
        self.lock["originals"]["candidate"] = self.lock["originals"]["before"]
        self.rejects("LOCK_SCHEMA")
        del self.lock["originals"]["candidate"]
        self.lock["originals"]["before"]["basename"] = "../before.zip"
        self.rejects("LOCK_PIN")
        self.lock["originals"]["before"]["basename"] = "before\n.zip"
        self.rejects("LOCK_PIN")

    def test_handoff_gate_requires_exact_private_empty_file(self):
        gate = Path(self.temp.name) / "ready"
        with self.assertRaisesRegex(ValueError, "^GATE_TIMEOUT$"):
            staging.wait_for_handoff_gate(gate, timeout_seconds=1)
        gate.write_bytes(b"ready")
        gate.chmod(0o400)
        with self.assertRaisesRegex(ValueError, "^GATE_PRIVACY$"):
            staging.wait_for_handoff_gate(gate)
        gate.chmod(0o600)
        gate.write_bytes(b"")
        gate.chmod(0o400)
        staging.wait_for_handoff_gate(gate)
        gate.unlink()
        gate.symlink_to(self.directory)
        with self.assertRaisesRegex(ValueError, "^GATE_PRIVACY$"):
            staging.wait_for_handoff_gate(gate)


if __name__ == "__main__":
    unittest.main()
