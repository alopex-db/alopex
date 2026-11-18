"""Synthetic archive checks for source-bound docs staging; no runtime."""
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from . import docs_stage


def digest(value):
    return hashlib.sha256(value).hexdigest()


def tar_bytes(entries):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name, data, kind in entries:
            member = tarfile.TarInfo(name)
            if kind == "file":
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
            elif kind == "symlink":
                member.type = tarfile.SYMTYPE
                member.linkname = "outside"
                archive.addfile(member)
            elif kind == "dir":
                member.type = tarfile.DIRTYPE
                archive.addfile(member)
    return output.getvalue()


class DocsStageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.runner = self.root / "runner"
        self.runner.mkdir(mode=0o700)
        self.assets = self.root / "assets-root"
        self.assets.mkdir(mode=0o700)
        (self.assets / "assets").mkdir(mode=0o700)
        self.docs = {"subject_identity.py": b"# subject\n",
                     "sql_resources.py": b"# resources\n"}
        self.inventory = {name: digest(data) for name, data in self.docs.items()}
        self.entries = [("scripts/performance/" + name, data, "file")
                        for name, data in self.docs.items()]

    def prepare(self, entries=None):
        raw = tar_bytes(self.entries if entries is None else entries)
        archive = self.assets / "assets/harness.tar"
        archive.write_bytes(raw)
        archive.chmod(0o600)
        roles = {"server": "1" * 64, "wheel": "2" * 64,
                 "library": "3" * 64, "receipt": "4" * 64,
                 "harness": digest(raw)}
        rows = [{"role": role, "path": "harness.tar" if role == "harness"
                 else role + ".bin", "size": len(raw) if role == "harness" else 1,
                 "sha256": value} for role, value in roles.items()]
        manifest = {"schema": "alopex.test-env-assets/v1",
                    "source_commit": "a" * 39 + "b",
                    "harness_commit": "b" * 39 + "c",
                    "platform": "linux/amd64", "expanded_limit_bytes": 8_000_000,
                    "files": rows}
        manifest_raw = json.dumps(manifest).encode()
        manifest_path = self.assets / "assets/assets.json"
        manifest_path.write_bytes(manifest_raw)
        manifest_path.chmod(0o600)
        pin = {"assets_digest": digest(manifest_raw),
               "asset_file_digests": roles,
               "source_commit": manifest["source_commit"],
               "harness_commit": manifest["harness_commit"]}
        return pin

    def stage(self, pin):
        return docs_stage.stage_docs(self.assets, self.runner, pin=pin,
                                     docs_files=self.inventory)

    def test_selects_only_reviewed_files_and_rejects_retry(self):
        entries = self.entries + [
            ("scripts/performance/unused.py", b"unused", "file"),
            ("scripts/performance/nested/fixture.json", b"{}", "file")]
        result = self.stage(self.prepare(entries))
        self.assertEqual(result, self.runner / "alopex-712-docs")
        self.assertEqual({p.name for p in (result / "scripts/performance").iterdir()},
                         set(self.docs))
        for path in (result, result / "scripts", result / "scripts/performance"):
            self.assertEqual(path.stat().st_mode & 0o777, 0o700)
        for name, data in self.docs.items():
            path = result / "scripts/performance" / name
            self.assertEqual(path.read_bytes(), data)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        with self.assertRaisesRegex(ValueError, "OUTPUT_STATE"):
            self.stage(self.prepare(entries))

    def test_rejects_malformed_tar_without_output(self):
        cases = [
            self.entries + [("../outside", b"x", "file")],
            self.entries + [("/absolute", b"x", "file")],
            self.entries + [("scripts/performance/evil.py", b"", "symlink")],
            self.entries + [self.entries[0]],
            self.entries + [("elsewhere/other.py", b"x", "file")],
            self.entries + [("scripts/performance/evil.py", b"x", "dir")],
        ]
        for entries in cases:
            with self.subTest(entries=entries[-1][0]):
                pin = self.prepare(entries)
                with self.assertRaisesRegex(ValueError, "HARNESS_ARCHIVE"):
                    self.stage(pin)
                self.assertFalse((self.runner / "alopex-712-docs").exists())
                self.assertEqual(list(self.runner.iterdir()), [])

    def test_rejects_digest_size_and_missing_reviewed_file(self):
        pin = self.prepare()
        pin["asset_file_digests"]["harness"] = "0" * 64
        with self.assertRaises(ValueError):
            self.stage(pin)
        self.assertFalse((self.runner / "alopex-712-docs").exists())
        pin = self.prepare(self.entries[:1])
        with self.assertRaisesRegex(ValueError, "HARNESS_IDENTITY"):
            self.stage(pin)
        self.assertFalse((self.runner / "alopex-712-docs").exists())
        pin = self.prepare()
        manifest = self.assets / "assets/assets.json"
        manifest.write_bytes(manifest.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "ASSET_IDENTITY"):
            self.stage(pin)

    def test_failed_write_removes_pending_tree_and_lock(self):
        pin = self.prepare()
        with patch.object(docs_stage.os, "fsync", side_effect=OSError("disk failed")):
            with self.assertRaises(OSError):
                self.stage(pin)
        self.assertEqual(list(self.runner.iterdir()), [])

    def test_rejects_symlink_ancestor_inside_assets_root(self):
        pin = self.prepare()
        real = self.assets / "assets/real"
        real.mkdir(mode=0o700)
        (self.assets / "assets/harness.tar").rename(real / "harness.tar")
        (self.assets / "assets/link").symlink_to(real, target_is_directory=True)
        path = self.assets / "assets/assets.json"
        manifest = json.loads(path.read_text())
        harness = next(row for row in manifest["files"] if row["role"] == "harness")
        harness["path"] = "link/harness.tar"
        raw = json.dumps(manifest).encode()
        path.write_bytes(raw)
        pin["assets_digest"] = digest(raw)
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            self.stage(pin)
        self.assertEqual(list(self.runner.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
