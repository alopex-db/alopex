"""Synthetic local-only staging checks; no preserved reference is copied."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from . import reference_staging, reference_transfer
from .reference_local_stage import prepare_local_staging


class LocalStageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.source = self.root / "source"
        self.source.mkdir()
        self.destination = self.root / "private-stage"
        self.originals = {}
        self.bytes = {}
        pins = {}
        for subject in ("before", "published", "postgresql"):
            payload = subject.encode()
            path = self.source / f"{subject}.zip"
            path.write_bytes(payload)
            path.chmod(0o644)
            self.originals[subject] = path
            self.bytes[subject] = payload
            pins[subject] = {"basename": path.name,
                             "zip_size_bytes": len(payload),
                             "zip_sha256": hashlib.sha256(payload).hexdigest()}
        self.lock = {"schema": "alopex.reference-original-staging/v1",
                     "originals": pins}

    def test_private_copies_preserve_original_modes_and_bytes(self):
        receipt = prepare_local_staging(
            originals=self.originals, destination=self.destination,
            lock=self.lock)
        self.assertEqual("staged", receipt["status"])
        self.assertEqual(0o700, self.destination.stat().st_mode & 0o777)
        for subject, path in self.originals.items():
            self.assertEqual(0o644, path.stat().st_mode & 0o777)
            self.assertEqual(self.bytes[subject], path.read_bytes())
            staged = self.destination / path.name
            self.assertEqual(0o400, staged.stat().st_mode & 0o777)
            self.assertEqual(self.bytes[subject], staged.read_bytes())

    def test_mismatched_hash_leaves_no_stage_or_temporary_copy(self):
        self.lock["originals"]["before"]["zip_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "LOCAL_STAGE_SOURCE_CHANGED"):
            prepare_local_staging(originals=self.originals,
                                  destination=self.destination, lock=self.lock)
        self.assertFalse(self.destination.exists())
        self.assertEqual([], list(self.root.glob(".reference-stage-*")))

    def test_existing_destination_is_not_overwritten(self):
        self.destination.mkdir()
        with self.assertRaisesRegex(ValueError, "LOCAL_STAGE_INPUT"):
            prepare_local_staging(originals=self.originals,
                                  destination=self.destination, lock=self.lock)
        self.assertEqual([], list(self.destination.iterdir()))

    def test_symlink_in_destination_ancestry_is_rejected_without_copy(self):
        real_root = self.root / "real-root"
        parent = real_root / "nested"
        parent.mkdir(parents=True)
        alias = self.root / "alias"
        alias.symlink_to(real_root, target_is_directory=True)
        destination = alias / "nested" / "private-stage"
        with self.assertRaisesRegex(ValueError, "LOCAL_STAGE_PARENT"):
            prepare_local_staging(originals=self.originals,
                                  destination=destination, lock=self.lock)
        self.assertFalse((parent / "private-stage").exists())
        self.assertEqual([], list(parent.glob(".reference-stage-*")))
        for subject, path in self.originals.items():
            self.assertEqual(self.bytes[subject], path.read_bytes())
            self.assertEqual(0o644, path.stat().st_mode & 0o777)

    def test_private_stage_passes_real_transfer_verifier_and_receiver(self):
        prepare_local_staging(originals=self.originals,
                              destination=self.destination, lock=self.lock)
        lock_path = self.root / "synthetic-lock.json"
        lock_path.write_text(json.dumps(self.lock), encoding="utf-8")
        known_hosts = self.root / "known_hosts"
        key = self.root / "key"
        for path in (known_hosts, key):
            path.write_bytes(b"test-only")
            path.chmod(0o600)
        runner_root = self.root / "runner"
        workspace = runner_root / "_work" / "alopex" / "alopex"
        workspace.mkdir(parents=True)
        expected = {"run_id": 1, "repository_id": 2, "workflow_id": 3,
                    "workflow_path": ".github/workflows/alopex-performance.yml",
                    "head_sha": "a" * 40, "job_name": "stage", "runner_id": 4,
                    "runner_group_id": 5, "runner_label": "test-label"}
        def local_receiver(_argv, **kwargs):
            return subprocess.Popen(
                [sys.executable, "-c", reference_transfer.RECEIVER,
                 str(runner_root), "alopex"], **kwargs)
        with mock.patch.object(reference_staging, "LOCK", lock_path):
            result = reference_transfer.transfer(
                repository="alopex-db/alopex", expected=expected,
                token="test-only", source=self.destination,
                ssh_host="127.0.0.1", ssh_port=22, ssh_user="runner",
                known_hosts=known_hosts, private_key=key,
                runner_root=str(runner_root), runner_repo="alopex",
                fetch=lambda **_: {"run_id": 1, "runner_id": 4,
                                   "head_sha": "a" * 40, "job_id": 6,
                                   "started_at": datetime.now(timezone.utc).strftime(
                                       "%Y-%m-%dT%H:%M:%SZ")},
                popen=local_receiver)
        self.assertEqual("transferred", result["status"])
        self.assertTrue((workspace / "reference-originals.ready").exists())
        for subject, original in self.originals.items():
            self.assertEqual(self.bytes[subject], original.read_bytes())
            self.assertEqual(self.bytes[subject],
                             (workspace / "reference-originals" / original.name).read_bytes())


if __name__ == "__main__":
    unittest.main()
