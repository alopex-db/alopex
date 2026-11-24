"""Pinned docs worker archive rejects changed source and local metadata."""
import hashlib
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from scripts.ops.final_sql_runtime import docs_harness as docs


class DocsHarnessTests(unittest.TestCase):
    def test_fixed_hashes_are_complete_sha256_values(self):
        self.assertEqual(set(docs.FILES), {"server-sql-isolated-v0816.py",
                                           "sql_resources.py", "subject_identity.py"})
        self.assertTrue(all(size > 0 and len(digest) == 64 and
                            all(char in "0123456789abcdef" for char in digest)
                            for size, digest in docs.FILES.values()))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        self.checkout = root / "checkout"
        source = self.checkout / "scripts/performance"
        source.mkdir(parents=True)
        self.output = root / "private"
        self.output.mkdir(mode=0o700)
        self.contents = {
            "server-sql-isolated-v0816.py": b"from sql_resources import x\n",
            "sql_resources.py": b"x = 1\n",
            "subject_identity.py": b"identity = True\n",
        }
        self.spec = {name: (len(data), hashlib.sha256(data).hexdigest())
                     for name, data in self.contents.items()}
        for name, data in self.contents.items():
            (source / name).write_bytes(data)

    def build(self, filename="worker.tar", *, head=docs.COMMIT, tree=docs.TREE):
        with patch.object(docs, "FILES", self.spec), patch.object(
            docs, "_git", side_effect=lambda _checkout, expression:
                head if expression == "HEAD" else tree):
            return docs.build(self.checkout, self.output / filename)

    def test_archive_contains_only_exact_sources_and_no_local_headers(self):
        record = self.build()
        self.assertEqual(record["commit"], docs.COMMIT)
        with tarfile.open(self.output / "worker.tar") as archive:
            self.assertEqual(archive.getnames(), [
                "scripts/performance/" + name for name in sorted(self.contents)])
            for member in archive.getmembers():
                self.assertEqual((member.uid, member.gid, member.mtime), (0, 0, 0))
                self.assertEqual((member.uname, member.gname), ("", ""))
        other = self.build("repeat.tar")
        self.assertEqual(record["archive_sha256"], other["archive_sha256"])

    def test_wrong_commit_tree_or_source_bytes_leave_no_archive(self):
        with self.assertRaisesRegex(ValueError, "DOCS_HARNESS"):
            self.build(head="a" * 40)
        with self.assertRaisesRegex(ValueError, "DOCS_HARNESS"):
            self.build(tree="b" * 40)
        (self.checkout / "scripts/performance/sql_resources.py").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "DOCS_HARNESS"):
            self.build()
        self.assertFalse(list(self.output.iterdir()))

    def test_unsafe_source_or_output_permission_rejected(self):
        target = self.checkout / "scripts/performance/subject_identity.py"
        target.unlink()
        target.symlink_to(self.checkout / "scripts/performance/sql_resources.py")
        with self.assertRaisesRegex(ValueError, "DOCS_HARNESS"):
            self.build()
        target.unlink()
        target.write_bytes(self.contents["subject_identity.py"])
        self.output.chmod(0o755)
        with self.assertRaisesRegex(ValueError, "DOCS_HARNESS"):
            self.build()
        self.assertFalse(list(self.output.iterdir()))

    def test_final_digest_read_failure_removes_private_archive(self):
        original = Path.read_bytes
        destination = self.output / "failed.tar"
        def fail_final_read(path):
            if path == destination:
                raise OSError("injected read failure")
            return original(path)
        with patch.object(docs, "FILES", self.spec), patch.object(
            docs, "_git", side_effect=lambda _checkout, expression:
                docs.COMMIT if expression == "HEAD" else docs.TREE), patch.object(
                    Path, "read_bytes", fail_final_read):
            with self.assertRaisesRegex(ValueError, "DOCS_HARNESS"):
                docs.build(self.checkout, destination)
        self.assertFalse(destination.exists())

    def test_symlinked_source_directory_rejected(self):
        source = self.checkout / "scripts/performance"
        moved = self.checkout / "scripts/source"
        source.rename(moved)
        source.symlink_to(moved, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "DOCS_HARNESS"):
            self.build()
        self.assertFalse(list(self.output.iterdir()))


if __name__ == "__main__":
    unittest.main()
