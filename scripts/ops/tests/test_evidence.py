"""Offline SQL diagnostic coverage; no Docker or network calls."""
import sys
from pathlib import Path as _BootstrapPath
_OPS = _BootstrapPath(__file__).resolve().parents[1]
sys.path.insert(0, str(_OPS))
sys.path.insert(0, str(_OPS.parent / "test-env"))

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from evidence import seal_cms
from orchestrate import FILES, sha256


class CmsAllowlistTests(unittest.TestCase):
    def make_files(self, parent):
        root = parent / "evidence"
        root.mkdir(mode=0o700)
        for name in FILES:
            (root / name).write_bytes(b"{}\n")
        hashes = {name: {"sha256": sha256(root / name), "size": 3} for name in FILES}
        cert = parent / "recipient.pem"
        cert.write_bytes(b"fake certificate")
        return root, hashes, cert

    def test_verified_ciphertext_then_plain_archive_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root, hashes, cert = self.make_files(parent)
            output = parent / "sealed.cms"
            def encrypt(command, **kwargs):
                Path(command[command.index("-out") + 1]).write_bytes(b"fake ciphertext")
            with patch("evidence.subprocess.run", side_effect=encrypt):
                result = seal_cms(root, hashes, ciphertext=output, certificate=cert)
            self.assertTrue(result["verified"])
            self.assertEqual(result["sha256"], sha256(output))
            self.assertFalse((parent / "private-evidence.tar.gz").exists())
            self.assertEqual({p.name for p in root.iterdir()}, set(FILES))

    def test_encryption_failure_preserves_plaintext(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root, hashes, cert = self.make_files(parent)
            with patch("evidence.subprocess.run", side_effect=subprocess.CalledProcessError(1, "openssl")):
                with self.assertRaises(subprocess.CalledProcessError):
                    seal_cms(root, hashes, ciphertext=parent / "sealed.cms", certificate=cert)
            self.assertTrue((parent / "private-evidence.tar.gz").exists())
            self.assertEqual({p.name for p in root.iterdir()}, set(FILES))

    def test_extra_file_rejected_before_encryption(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root, hashes, cert = self.make_files(parent)
            (root / "raw.sql").write_text("private")
            with self.assertRaisesRegex(ValueError, "EVIDENCE_ALLOWLIST"):
                seal_cms(root, hashes, ciphertext=parent / "sealed.cms", certificate=cert)


if __name__ == "__main__":
    unittest.main()
