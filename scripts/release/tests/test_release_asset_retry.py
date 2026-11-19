"""The RC upload must be safe to retry after its manifest is published."""

import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts.release import release_asset_retry as retry


class FakeGh:
    def __init__(self, remote=None, conflict=None):
        self.remote = dict(remote or {})
        self.conflict = conflict
        self.uploads = []
        self.downloads = []

    def __call__(self, *args, max_file_size=None):
        if args[0] == "api":
            if "/tags/" in args[1]:
                return json.dumps({"id": 123})
            page = int(args[1].split("page=")[-1])
            return json.dumps([
                {"name": name, "size": len(data), "digest": "sha256:" + hashlib.sha256(data).hexdigest()}
                for name, data in list(self.remote.items())[(page - 1) * 100:page * 100]
            ])
        if args[:2] == ("release", "upload"):
            path = Path(args[3])
            self.uploads.append(path.name)
            self.assert_no_clobber(args)
            if self.conflict == path.name:
                self.conflict = None
                self.remote[path.name] = self.conflict_bytes
                raise subprocess.CalledProcessError(1, args)
            self.remote[path.name] = path.read_bytes()
            return ""
        if args[:2] == ("release", "download"):
            name = args[args.index("--pattern") + 1]
            self.downloads.append(name)
            Path(args[args.index("--dir") + 1], name).write_bytes(self.remote[name])
            return ""
        raise AssertionError(args)

    @staticmethod
    def assert_no_clobber(args):
        if "--clobber" in args:
            raise AssertionError("upload must not replace an asset")


class ReleaseAssetRetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        (self.directory / "one.whl").write_bytes(b"one")
        (self.directory / "two.tar.gz").write_bytes(b"two")

    def run_upload(self, fake):
        with patch.object(retry, "gh", fake):
            retry.upload("owner/repo", "v1", self.directory)

    def test_first_upload_and_identical_retry(self):
        fake = FakeGh()
        self.run_upload(fake)
        self.assertEqual(fake.uploads, ["one.whl", "two.tar.gz"])
        self.run_upload(fake)
        self.assertEqual(fake.uploads, ["one.whl", "two.tar.gz"])

    def test_lists_every_release_asset_page(self):
        fake = FakeGh({f"asset-{index}": b"x" for index in range(101)})
        with patch.object(retry, "gh", fake):
            self.assertEqual(len(retry.assets("owner/repo", "v1")), 101)

    def test_different_existing_bytes_rejected_before_any_upload(self):
        fake = FakeGh({"one.whl": b"wrong"})
        with self.assertRaisesRegex(ValueError, "differs"):
            self.run_upload(fake)
        self.assertEqual(fake.uploads, [])
        self.assertEqual(fake.remote["one.whl"], b"wrong")

    def test_partial_release_uploads_only_missing_name(self):
        fake = FakeGh({"one.whl": b"one"})
        self.run_upload(fake)
        self.assertEqual(fake.uploads, ["two.tar.gz"])

    def test_only_direct_files_are_uploaded(self):
        nested = self.directory / "rc-storage"
        nested.mkdir()
        (nested / "evidence.json").write_bytes(b"evidence")
        fake = FakeGh()
        self.run_upload(fake)
        self.assertEqual(fake.uploads, ["one.whl", "two.tar.gz"])

    def test_direct_symlink_is_rejected(self):
        (self.directory / "linked.whl").symlink_to(self.directory / "one.whl")
        fake = FakeGh()
        with self.assertRaisesRegex(ValueError, "symlinks"):
            self.run_upload(fake)
        self.assertEqual(fake.uploads, [])

    def test_concurrent_identical_name_is_accepted(self):
        fake = FakeGh(conflict="one.whl")
        fake.conflict_bytes = b"one"
        self.run_upload(fake)
        self.assertEqual(fake.uploads, ["one.whl", "two.tar.gz"])

    def test_concurrent_different_name_fails_without_overwrite(self):
        fake = FakeGh(conflict="one.whl")
        fake.conflict_bytes = b"wrong"
        with self.assertRaisesRegex(ValueError, "differs"):
            self.run_upload(fake)
        self.assertEqual(fake.uploads, ["one.whl"])
        self.assertEqual(fake.remote["one.whl"], b"wrong")

    def test_missing_digest_downloads_and_compares_bytes(self):
        fake = FakeGh({"one.whl": b"one", "two.tar.gz": b"two"})

        def no_digest(*args, max_file_size=None):
            result = fake(*args, max_file_size=max_file_size)
            if args[0] == "api" and "/assets?" in args[1]:
                payload = json.loads(result)
                for asset in payload:
                    asset.pop("digest")
                return json.dumps(payload)
            if args[:2] == ("release", "download"):
                self.assertEqual(max_file_size, len(fake.remote[args[args.index("--pattern") + 1]]))
            return result

        self.run_upload(no_digest)
        self.assertEqual(fake.downloads, ["one.whl", "two.tar.gz", "one.whl", "two.tar.gz"])

    def test_missing_digest_rejects_equal_size_different_bytes(self):
        fake = FakeGh({"one.whl": b"bad", "two.tar.gz": b"two"})

        def no_digest(*args, max_file_size=None):
            result = fake(*args, max_file_size=max_file_size)
            if args[0] == "api" and "/assets?" in args[1]:
                payload = json.loads(result)
                for asset in payload:
                    asset.pop("digest")
                return json.dumps(payload)
            return result

        with self.assertRaisesRegex(ValueError, "differs in bytes"):
            self.run_upload(no_digest)
        self.assertEqual(fake.uploads, [])

    def test_workflow_uses_guard_after_inventory_validation(self):
        workflow = Path(".github/workflows/alopex-py-release.yml").read_text()
        block = workflow.split("  candidate-release:", 1)[1].split("  publish-pypi:", 1)[0]
        self.assertLess(block.index("primary_distributions.py"), block.index("release_asset_retry.py"))
        self.assertNotIn("--clobber", block)
        self.assertIn("CORE_TAG: ${{ inputs.core_tag }}", block)


if __name__ == "__main__":
    unittest.main()
