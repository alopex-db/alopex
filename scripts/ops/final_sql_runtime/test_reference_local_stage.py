"""Synthetic local-only staging checks; no preserved reference is copied."""
import hashlib
from pathlib import Path
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
