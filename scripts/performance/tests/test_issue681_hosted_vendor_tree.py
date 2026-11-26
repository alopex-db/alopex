"""Build admission must reopen every installed public vendor source byte."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_vendor_tree as tree


class HostedVendorTreeTest(unittest.TestCase):
    def fixture(self, parent):
        root = parent / "vendor"
        package = root / "pkg"
        root.mkdir(mode=0o700)
        package.mkdir(mode=0o700)
        files = {"pkg/source.rs": b"source bytes",
                 "pkg/.cargo-checksum.json": b"{}"}
        for name, content in files.items():
            path = root / name
            path.write_bytes(content)
            path.chmod(0o600)
        digest = hashlib.sha256()
        mode_digest = hashlib.sha256()
        for name, content in sorted(files.items()):
            row = (name, len(content), hashlib.sha256(content).hexdigest())
            digest.update(json.dumps(row, separators=(",", ":")).encode() + b"\n")
            mode_digest.update(json.dumps((name, 0o600),
                                          separators=(",", ":")).encode() + b"\n")
        patches = [mock.patch.object(tree, "FILE_COUNT", len(files)),
                   mock.patch.object(tree, "CONTENT_BYTES",
                                     sum(map(len, files.values()))),
                   mock.patch.object(tree, "TREE_SHA256", digest.hexdigest()),
                   mock.patch.object(tree, "GUARD_BYTES", 1)]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        return root, mode_digest.hexdigest()

    def test_exact_tree_reopened_with_storage_guard(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, mode_digest = self.fixture(Path(tmp).resolve())
            calls = []
            self.assertEqual(tree.verify_tree(root, lambda: calls.append(True),
                                              mode_digest),
                             tree.TREE_SHA256)
            self.assertGreaterEqual(len(calls), 3)

    def test_changed_source_or_extra_entry_is_rejected(self):
        for change in ("bytes", "mode", "link", "empty-dir"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as tmp:
                root, mode_digest = self.fixture(Path(tmp).resolve())
                if change == "bytes":
                    (root / "pkg/source.rs").write_bytes(b"changed bytes")
                elif change == "mode":
                    (root / "pkg/source.rs").chmod(0o700)
                elif change == "link":
                    (root / "pkg/link").symlink_to(root / "pkg/source.rs")
                else:
                    (root / "pkg/extra").mkdir(mode=0o700)
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    tree.verify_tree(root, lambda: None, mode_digest)

    def test_guard_failure_is_fixed_and_stops_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, mode_digest = self.fixture(Path(tmp).resolve())
            with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                tree.verify_tree(root, lambda: (_ for _ in ()).throw(
                    OSError("private mount detail")), mode_digest)


if __name__ == "__main__":
    unittest.main()
