"""Focused offline trust-boundary tests for staged original ZIP admission."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
import os
from pathlib import Path
import tempfile
import unittest
import zipfile

from . import reference_ingress as ingress


NOW = datetime(2026, 11, 21, tzinfo=timezone.utc)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def wrapper(entries):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for name, data, mode in entries:
            item = zipfile.ZipInfo(name)
            item.external_attr = mode << 16
            archive.writestr(item, data)
    return output.getvalue()


class ReferenceIngressTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.dest = self.root / "private"
        self.dest.mkdir(mode=0o700)
        self.run = {"id": 42, "head_sha": "a" * 40,
                    "repository": {"id": 7}, "workflow_id": 8,
                    "run_attempt": 1, "event": "workflow_dispatch",
                    "status": "completed", "conclusion": "success"}
        self.descriptors = {}
        self.pins = {}
        self.metadata = {}
        self.wrappers = {}
        for number, subject in enumerate(sorted(ingress.SUBJECTS), 1):
            original = b"original ZIP " + subject.encode()
            payload = wrapper([(subject + ".zip", original, 0o100600)])
            path = self.root / f"wrapper-{subject}.zip"
            path.write_bytes(payload)
            self.wrappers[subject] = path
            self.pins[subject] = {"zip_sha256": sha(original), "zip_size_bytes": len(original),
                                  "basename": subject + ".zip"}
            self.descriptors[subject] = {
                "run_id": 42, "head_sha": "a" * 40,
                "repository_id": 7, "workflow_id": 8, "artifact_id": number,
                "artifact_name": f"reference-{subject}", "artifact_digest": sha(payload),
                "artifact_size_bytes": len(payload), "inner_name": subject + ".zip"}
            self.metadata[subject] = {
                "id": number, "name": f"reference-{subject}",
                "digest": "sha256:" + sha(payload), "size_in_bytes": len(payload),
                "expired": False, "created_at": "2026-11-20T00:00:00Z",
                "expires_at": "2026-11-22T00:00:00Z",
                "workflow_run": {"id": 42, "head_sha": "a" * 40}}

    def admit(self):
        return ingress.admit_wrapped_originals(
            descriptors=self.descriptors, original_pins=self.pins,
            staging_run=self.run, artifact_metadata=self.metadata,
            wrappers=self.wrappers, destination=self.dest, now=NOW)

    def assert_rejected(self, code):
        with self.assertRaisesRegex(ValueError, code):
            self.admit()
        self.assertEqual(list(self.dest.iterdir()), [])

    def test_three_exact_originals(self):
        result = self.admit()
        self.assertEqual(set(result), ingress.SUBJECTS)
        for subject, path in result.items():
            ingress.verify_original_zip(path, self.pins[subject])
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_wrong_run_head_expiry_and_duplicate_artifact(self):
        self.metadata["before"]["workflow_run"]["id"] = 43
        self.assert_rejected("ARTIFACT_METADATA")
        self.metadata["before"]["workflow_run"]["id"] = 42
        self.run["head_sha"] = "b" * 40
        self.assert_rejected("STAGING_RUN")
        self.run["head_sha"] = "a" * 40
        self.metadata["before"]["expires_at"] = "2026-11-21T00:00:00Z"
        self.assert_rejected("ARTIFACT_METADATA")
        self.metadata["before"]["expires_at"] = "2026-11-22T00:00:00Z"
        self.metadata["before"]["created_at"] = "2026-11-19T23:59:59Z"
        self.assert_rejected("ARTIFACT_METADATA")
        self.metadata["before"]["created_at"] = "2026-11-20T00:00:00Z"
        self.descriptors["before"]["artifact_id"] = self.descriptors["published"]["artifact_id"]
        self.assert_rejected("ARTIFACT_METADATA|DUPLICATE_ARTIFACT")

    def test_wrong_repository_workflow_attempt_and_event(self):
        for field, wrong in (("repository", {"id": 9}), ("workflow_id", 9),
                             ("run_attempt", 2), ("event", "push")):
            with self.subTest(field=field):
                original = self.run[field]
                self.run[field] = wrong
                self.assert_rejected("STAGING_RUN")
                self.run[field] = original
        self.descriptors["before"]["repository_id"] = 9
        self.assert_rejected("STAGING_RUN")
        self.descriptors["before"]["repository_id"] = 7
        self.descriptors["before"]["workflow_id"] = 9
        self.assert_rejected("STAGING_RUN")

    def test_changed_wrapper_and_original_pin(self):
        self.wrappers["before"].write_bytes(self.wrappers["before"].read_bytes() + b"x")
        self.assert_rejected("WRAPPER_SIZE")
        self.wrappers["before"].write_bytes(self.wrappers["before"].read_bytes()[:-1])
        self.pins["before"]["zip_sha256"] = "0" * 64
        self.assert_rejected("ORIGINAL_BYTES")

    def test_later_original_failure_removes_earlier_extraction(self):
        self.pins["published"]["zip_sha256"] = "0" * 64
        self.assert_rejected("ORIGINAL_BYTES")

    def test_descriptor_requires_reviewed_original_basename(self):
        self.descriptors["before"]["inner_name"] = "other.zip"
        self.assert_rejected("STAGING_DESCRIPTOR")

    def test_extra_duplicate_traversal_and_symlink_member(self):
        for entries, code in (
            ([("before.zip", b"x", 0o100600), ("extra", b"y", 0o100600)], "WRAPPER_MEMBERS"),
            ([("before.zip", b"x", 0o100600), ("before.zip", b"y", 0o100600)], "WRAPPER_MEMBERS"),
            ([("../before.zip", b"x", 0o100600)], "WRAPPER_MEMBER"),
            ([("before.zip", b"x", 0o120777)], "WRAPPER_MEMBER"),
        ):
            with self.subTest(code=code, entries=entries):
                data = wrapper(entries)
                self.wrappers["before"].write_bytes(data)
                self.descriptors["before"]["artifact_digest"] = sha(data)
                self.descriptors["before"]["artifact_size_bytes"] = len(data)
                self.metadata["before"]["digest"] = "sha256:" + sha(data)
                self.metadata["before"]["size_in_bytes"] = len(data)
                self.assert_rejected(code)

    def test_existing_target_private_destination_and_wrapper_symlink(self):
        (self.dest / "before.zip").write_bytes(b"existing")
        with self.assertRaises(FileExistsError):
            self.admit()
        self.assertEqual((self.dest / "before.zip").read_bytes(), b"existing")
        (self.dest / "before.zip").unlink()
        os.chmod(self.dest, 0o755)
        self.assert_rejected("DESTINATION_PRIVACY")
        os.chmod(self.dest, 0o700)
        target = self.wrappers["before"]
        link = self.root / "wrapper-link"
        link.symlink_to(target)
        self.wrappers["before"] = link
        with self.assertRaises(OSError):
            self.admit()


if __name__ == "__main__":
    unittest.main()
