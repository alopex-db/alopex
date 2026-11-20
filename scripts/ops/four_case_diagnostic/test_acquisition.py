"""Focused offline checks for original native asset acquisition bytes."""
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from . import acquisition


JOB = "12345678"
ZIP = b"original native asset ZIP bytes"
MANIFEST = b'{"files":[]}'


def sha(data):
    return hashlib.sha256(data).hexdigest()


class AcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.archive = root / "candidate.original.zip"
        self.manifest = root / "assets.json"
        self.receipt = root / "acquisition.private.json"
        self.archive.write_bytes(ZIP)
        self.manifest.write_bytes(MANIFEST)
        for path in (self.archive, self.manifest):
            path.chmod(0o600)
        self.lock = {"schema": "alopex.sql-four-case-pins/v1",
                     "pin": {"asset_run_id": 88, "asset_run_head": "a" * 39 + "b",
                             "artifact_id": 77, "artifact_sha256": sha(ZIP),
                             "zip_sha256": sha(ZIP),
                             "asset_zip_size_bytes": len(ZIP),
                             "assets_digest": sha(MANIFEST)}}
        self.proof = {"schema": acquisition.SCHEMA,
                      "qualification_job_id": JOB, "run": 88,
                      "head": "a" * 39 + "b", "artifact_id": 77,
                      "zip_sha256": sha(ZIP), "zip_size_bytes": len(ZIP),
                      "assets_digest": sha(MANIFEST)}
        self.write_receipt()

    def write_receipt(self):
        self.receipt.write_text(json.dumps(self.proof))
        self.receipt.chmod(0o600)

    def verify(self):
        return acquisition.verify(self.lock, receipt_path=self.receipt,
                                  archive_path=self.archive,
                                  manifest_path=self.manifest, job_id=JOB)

    def test_exact_current_job_bytes(self):
        self.assertEqual(self.verify(), self.proof)

    def test_rejects_stale_receipt_and_wrong_identity(self):
        for key, value in (("qualification_job_id", "12345677"),
                           ("run", 87), ("head", "b" * 39 + "c"),
                           ("artifact_id", 76), ("zip_size_bytes", len(ZIP) + 1),
                           ("zip_sha256", sha(b"other")),
                           ("assets_digest", sha(b"other"))):
            with self.subTest(key=key):
                prior = copy.deepcopy(self.proof)
                self.proof[key] = value
                self.write_receipt()
                with self.assertRaisesRegex(ValueError, "ACQUISITION_BINDING"):
                    self.verify()
                self.proof = prior
        self.proof["extra"] = True
        self.write_receipt()
        with self.assertRaisesRegex(ValueError, "ACQUISITION_BINDING"):
            self.verify()

    def test_rejects_changed_files_and_private_mode(self):
        self.archive.write_bytes(ZIP[:-1] + b"X")
        with self.assertRaisesRegex(ValueError, "ACQUISITION_BINDING"):
            self.verify()
        self.archive.write_bytes(ZIP)
        self.archive.chmod(0o600)
        self.manifest.write_bytes(b'{"files":[1]}')
        with self.assertRaisesRegex(ValueError, "ACQUISITION_BINDING"):
            self.verify()
        self.manifest.write_bytes(MANIFEST)
        self.manifest.chmod(0o600)
        for path in (self.archive, self.manifest, self.receipt):
            with self.subTest(path=path):
                path.chmod(0o644)
                with self.assertRaisesRegex(ValueError, "ACQUISITION_BINDING"):
                    self.verify()
                path.chmod(0o600)

    def test_rejects_untrusted_size_and_duplicate_receipt_field(self):
        self.lock["pin"]["asset_zip_size_bytes"] += 1
        with self.assertRaisesRegex(ValueError, "ACQUISITION_BINDING"):
            self.verify()
        self.lock["pin"]["asset_zip_size_bytes"] -= 1
        self.receipt.write_text(json.dumps(self.proof)[:-1] + ',"run":88}')
        self.receipt.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "ACQUISITION_BINDING"):
            self.verify()

    def test_rejects_symlinked_archive(self):
        target = self.archive.with_suffix(".saved")
        self.archive.rename(target)
        os.symlink(target, self.archive)
        with self.assertRaisesRegex(ValueError, "ACQUISITION_BINDING"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
