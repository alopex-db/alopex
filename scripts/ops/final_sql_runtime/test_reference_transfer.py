"""Local receiver contract checks; no SSH, runner, or original ZIPs."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from . import reference_transfer
from .reference_transfer import RECEIVER


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.root = self.base / "runner"
        self.workspace = self.root / "_work" / "alopex" / "alopex"
        self.workspace.mkdir(parents=True)
        self.payloads = [b"before", b"published", b"postgresql"]
        self.pins = [
            {"basename": f"reference-{i}.zip", "zip_size_bytes": len(data),
             "zip_sha256": hashlib.sha256(data).hexdigest()}
            for i, data in enumerate(self.payloads)
        ]

    def run_receiver(self, *, root=None, pins=None, payloads=None):
        pins = self.pins if pins is None else pins
        payloads = self.payloads if payloads is None else payloads
        stream = (json.dumps(pins).encode() + b"\n" +
                  b"".join(payloads) + b"!")
        return subprocess.run(
            [sys.executable, "-c", RECEIVER, str(root or self.root), "alopex"],
            input=stream, capture_output=True, timeout=5)

    def assert_no_stage(self):
        self.assertFalse((self.workspace / "reference-originals").exists())
        self.assertFalse((self.workspace / "reference-originals.ready").exists())
        self.assertEqual([], list(self.workspace.glob(".reference-originals-*")))

    def test_exact_stream_commits_private_files_and_marker(self):
        result = self.run_receiver()
        self.assertEqual(0, result.returncode, result.stderr.decode())
        self.assertEqual(b"TRANSFER_COMPLETE\n", result.stdout)
        stage = self.workspace / "reference-originals"
        self.assertEqual(0o700, stage.stat().st_mode & 0o777)
        for pin, data in zip(self.pins, self.payloads):
            path = stage / pin["basename"]
            self.assertEqual(data, path.read_bytes())
            self.assertEqual(0o400, path.stat().st_mode & 0o777)
        marker = self.workspace / "reference-originals.ready"
        self.assertEqual(0o400, marker.stat().st_mode & 0o777)

    def test_hash_failure_cleans_temporary_files(self):
        result = self.run_receiver(payloads=[b"wrong!", *self.payloads[1:]])
        self.assertNotEqual(0, result.returncode)
        self.assert_no_stage()

    def test_symlink_in_root_ancestry_is_rejected(self):
        alias = self.base / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        result = self.run_receiver(root=alias)
        self.assertNotEqual(0, result.returncode)
        self.assert_no_stage()

    def test_existing_pending_marker_is_preserved(self):
        pending = self.workspace / ".reference-originals.ready.pending"
        pending.write_bytes(b"keep")
        result = self.run_receiver()
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(b"keep", pending.read_bytes())
        self.assert_no_stage()

    def test_assignment_mismatch_prevents_ssh_and_transfer(self):
        source = self.base / "source"
        source.mkdir(mode=0o700)
        for pin, data in zip(self.pins, self.payloads):
            path = source / pin["basename"]
            path.write_bytes(data)
            path.chmod(0o400)
        known_hosts = self.base / "known_hosts"
        key = self.base / "key"
        for path in (known_hosts, key):
            path.write_bytes(b"test-only")
            path.chmod(0o600)
        expected = {
            "run_id": 1, "repository_id": 2, "workflow_id": 3,
            "workflow_path": ".github/workflows/alopex-performance.yml",
            "head_sha": "a" * 40, "job_name": "stage", "runner_id": 4,
            "runner_group_id": 5, "runner_label": "test-label"}
        popen = mock.Mock(side_effect=AssertionError("SSH must not start"))
        with mock.patch.object(reference_transfer.reference_staging, "_pins",
                               return_value={str(i): pin for i, pin in enumerate(self.pins)}), \
             mock.patch.object(reference_transfer.reference_staging, "verify_staging"):
            with self.assertRaisesRegex(ValueError, "TRANSFER_ASSIGNMENT"):
                reference_transfer.transfer(
                    repository="alopex-db/alopex", expected=expected,
                    token="test-only", source=source, ssh_host="127.0.0.1",
                    ssh_port=22, ssh_user="runner", known_hosts=known_hosts,
                    private_key=key, runner_root="/runner", runner_repo="alopex",
                    fetch=lambda **_: {"run_id": 1, "runner_id": 99,
                                       "head_sha": "a" * 40, "job_id": 6},
                    popen=popen)
        popen.assert_not_called()

    def test_host_stream_commits_only_after_exact_assignment(self):
        source = self.base / "source"
        source.mkdir(mode=0o700)
        for pin, data in zip(self.pins, self.payloads):
            path = source / pin["basename"]
            path.write_bytes(data)
            path.chmod(0o400)
        known_hosts = self.base / "known_hosts"
        key = self.base / "key"
        for path in (known_hosts, key):
            path.write_bytes(b"test-only")
            path.chmod(0o600)
        expected = {
            "run_id": 1, "repository_id": 2, "workflow_id": 3,
            "workflow_path": ".github/workflows/alopex-performance.yml",
            "head_sha": "a" * 40, "job_name": "stage", "runner_id": 4,
            "runner_group_id": 5, "runner_label": "test-label"}
        def local_receiver(_argv, **kwargs):
            return subprocess.Popen(
                [sys.executable, "-c", RECEIVER, str(self.root), "alopex"],
                **kwargs)
        with mock.patch.object(reference_transfer.reference_staging, "_pins",
                               return_value={str(i): pin for i, pin in enumerate(self.pins)}), \
             mock.patch.object(reference_transfer.reference_staging, "verify_staging"):
            result = reference_transfer.transfer(
                repository="alopex-db/alopex", expected=expected,
                token="test-only", source=source, ssh_host="127.0.0.1",
                ssh_port=22, ssh_user="runner", known_hosts=known_hosts,
                private_key=key, runner_root=str(self.root), runner_repo="alopex",
                fetch=lambda **_: {"run_id": 1, "runner_id": 4,
                                   "head_sha": "a" * 40, "job_id": 6},
                popen=local_receiver)
        self.assertEqual("transferred", result["status"])
        self.assertTrue((self.workspace / "reference-originals.ready").exists())
        for pin, data in zip(self.pins, self.payloads):
            self.assertEqual(data, (self.workspace / "reference-originals" /
                                    pin["basename"]).read_bytes())


if __name__ == "__main__":
    unittest.main()
