"""Source binding and fail-closed four-case qualification tests; no Docker."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

SCRIPT = Path(__file__).resolve().parents[1] / "hosted_test_env_qualification_four_case.py"
SPEC = importlib.util.spec_from_file_location("hosted_four_case", SCRIPT)
subject = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(subject)


def digest(data):
    return hashlib.sha256(data).hexdigest()


class FourCaseQualification(unittest.TestCase):
    def test_reused_host_controller_is_exactly_pinned(self):
        self.assertIsNotNone(subject._base().admission_facts)
        with patch.object(subject, "BASE_SHA256", "0" * 64):
            with self.assertRaisesRegex(ValueError, "OPERATOR_IDENTITY"):
                subject._base()

    def test_manifest_requires_independent_sha_and_same_job(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "input.json"
            raw = json.dumps({"schema": subject.INPUT_SCHEMA, "job_id": "123",
                              "lock_path": str(root / "lock.json"),
                              "lock_sha256": "a" * 64}).encode()
            manifest.write_bytes(raw)
            manifest.chmod(0o600)
            with self.assertRaisesRegex(ValueError, "PRIVATE_MANIFEST"):
                subject.load_inputs(manifest, "0" * 64)
            with self.assertRaisesRegex(ValueError, "SAME_JOB"):
                subject.load_inputs(manifest, digest(raw), environment={})

    def test_acquisition_installs_v2_receipt_and_archive_at_attestor_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            private.mkdir(mode=0o700)
            assets_root = root / "alopex-804-candidate"
            lock = {"pin": {"assets_digest": "b" * 64}}

            def fake_acquire(_lock, *, job_id, directory, fetch):
                directory.mkdir(mode=0o700)
                (directory / "assets-root" / "assets").mkdir(parents=True)
                (directory / "assets-root" / "assets" / "assets.json").write_text("{}")
                (directory / "candidate.original.zip").write_bytes(b"archive")
                (directory / "acquisition.private.json").write_text("{}")
                return {"assets_digest": "b" * 64}

            with patch.object(subject.acquire, "acquire", side_effect=fake_acquire), \
                 patch.object(subject.acquisition, "verify", return_value={}) as verify:
                subject.acquire_assets(lock, private=private, assets_root=assets_root,
                                       job_id="123", fetch=Mock())
            self.assertEqual((private / "candidate.original.zip").read_bytes(), b"archive")
            self.assertTrue((private / "acquisition.private.json").is_file())
            self.assertTrue((assets_root / "assets/assets.json").is_file())
            self.assertEqual(verify.call_args.kwargs["job_id"], "123")
            self.assertEqual(verify.call_args.kwargs["manifest_path"],
                             assets_root / "assets/assets.json")

    def test_old_asset_identity_is_not_used(self):
        base = subject._base()
        pin = {"controller_digest": "1" * 64, "image_lock_sha256": "2" * 64,
               "seccomp_sha256": "3" * 64, "source_commit": "4" * 40,
               "assets_digest": "5" * 64}
        subject._configure_base(base, pin)
        self.assertEqual(base.PRODUCT, pin["source_commit"])
        self.assertEqual(base.ASSETS_DIGEST, pin["assets_digest"])
        self.assertEqual(base.POLICY_DIGEST, pin["seccomp_sha256"])

    def test_verification_rejects_stale_manifest_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "assets").mkdir()
            (root / "assets/assets.json").write_text("{}")
            with self.assertRaisesRegex(ValueError, "IDENTITY"):
                subject.verified_inputs(Mock(), {"assets_digest": "0" * 64}, root)


if __name__ == "__main__":
    unittest.main()
