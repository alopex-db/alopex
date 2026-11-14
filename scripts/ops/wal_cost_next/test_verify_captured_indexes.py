"""Synthetic proof checks; no network, container or real Ubuntu key is used."""
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from verify_captured_indexes import ProofError, prove, signed_hashes, signer


class CaptureProofTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.capture = self.root / "capture"
        (self.capture / "lists").mkdir(parents=True)
        (self.capture / "packages").mkdir()
        self.script = self.root / "resolver.sh"
        self.script.write_text("#!/bin/sh\nexit 0\n")
        (self.capture / "resolver.sha256").write_text(
            hashlib.sha256(self.script.read_bytes()).hexdigest() + "  /resolution/resolve.sh\n")
        self.package = b"Package: gcc\nVersion: 1\nArchitecture: amd64\n\n"
        h = hashlib.sha256(self.package).hexdigest()
        self.inrelease = ("-----BEGIN PGP SIGNED MESSAGE-----\nHash: SHA512\n\n"
                          "Suite: noble\nSHA256:\n"
                          f" {h} {len(self.package)} main/binary-amd64/Packages\n"
                          "-----BEGIN PGP SIGNATURE-----\nsynthetic\n"
                          "-----END PGP SIGNATURE-----\n").encode()
        self.index = "archive.ubuntu.com_ubuntu_dists_noble_main_binary-amd64_Packages.lz4"
        self.irelease = self.capture / "lists/archive.ubuntu.com_ubuntu_dists_noble_InRelease"
        self.irelease.write_bytes(self.inrelease)
        (self.capture / "packages" / self.index).write_bytes(self.package)
        (self.capture / "capture.status").write_text("capture-complete\n")
        (self.capture / "ubuntu.sources").write_text(
            "Types: deb\nURIs: https://archive.ubuntu.com/ubuntu\n"
            "Suites: noble\nComponents: main\n"
            "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n")
        (self.capture / "trusted-keyring.gpg").write_bytes(b"synthetic keyring")
        keyhash = hashlib.sha256(b"synthetic keyring").hexdigest()
        (self.capture / "source-keyring.sha256").write_text(
            f"{keyhash}  /usr/share/keyrings/ubuntu-archive-keyring.gpg\n")
        (self.capture / "base.tsv").write_text("libc6\t2.39\n")
        apt_uri = "'https://archive.ubuntu.com/ubuntu/pool/main/g/gcc.deb' gcc.deb 1 md5\n"
        (self.capture / "apt-raw.txt").write_text("Reading package lists...\n" + apt_uri)
        (self.capture / "apt-uris.txt").write_text(apt_uri)
        (self.capture / "indextargets.raw").write_text(
            "Created-By: Packages\nArchitecture: amd64\n"
            "URI: https://archive.ubuntu.com/ubuntu/dists/noble/main/binary-amd64/Packages.xz\n"
            "MetaKey: main/binary-amd64/Packages\n"
            f"Filename: /resolution/results/lists/{self.index}\n")

    def check(self):
        with patch("verify_captured_indexes.signer", return_value="A" * 40):
            return prove(self.capture, self.script)

    def test_binds_signed_index_and_exact_apt_output(self):
        base, auth, indexes = self.check()
        self.assertEqual(base["packages"], {"libc6": "2.39"})
        self.assertEqual(auth["indexes"][0]["packages_sha256"],
                         hashlib.sha256(self.package).hexdigest())
        self.assertEqual(len(indexes), 1)

    def test_rejects_index_hash_mismatch(self):
        (self.capture / "packages" / self.index).write_bytes(self.package + b"tamper")
        with self.assertRaisesRegex(ProofError, "signed hash mismatch"):
            self.check()

    def test_rejects_unexpected_source_and_target_escape(self):
        path = self.capture / "indextargets.raw"
        path.write_text(path.read_text().replace("archive.ubuntu.com", "example.com"))
        with self.assertRaisesRegex(ProofError, "unknown index source"):
            self.check()
        path.write_text(path.read_text().replace("example.com", "archive.ubuntu.com")
                        .replace("/resolution/results/lists/", "/tmp/"))
        with self.assertRaisesRegex(ProofError, "outside capture"):
            self.check()

    def test_rejects_apt_extraction_drift(self):
        (self.capture / "apt-uris.txt").write_text("wrong\n")
        with self.assertRaisesRegex(ProofError, "APT URI extraction drift"):
            self.check()

    def test_rejects_resolver_drift_and_unaccounted_index(self):
        self.script.write_text("#!/bin/sh\nexit 1\n")
        with self.assertRaisesRegex(ProofError, "resolver script drift"):
            self.check()
        self.script.write_text("#!/bin/sh\nexit 0\n")
        (self.capture / "lists/extra_InRelease").write_bytes(self.inrelease)
        with self.assertRaisesRegex(ProofError, "unaccounted InRelease"):
            self.check()

    def test_rejects_extra_source(self):
        path = self.capture / "ubuntu.sources"
        path.write_text(path.read_text() + "\nTypes: deb\nURIs: https://example.com/ubuntu\n"
                        "Suites: noble\nComponents: main\n"
                        "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n")
        with self.assertRaisesRegex(ProofError, "unexpected APT source"):
            self.check()

    def test_rejects_index_not_declared_by_source_stanza(self):
        path = self.capture / "ubuntu.sources"
        path.write_text(path.read_text().replace("archive.ubuntu.com", "security.ubuntu.com"))
        with self.assertRaisesRegex(ProofError, "index absent from declared source"):
            self.check()
        path.write_text(path.read_text().replace("security.ubuntu.com", "archive.ubuntu.com")
                        .replace("Components: main", "Components: universe"))
        with self.assertRaisesRegex(ProofError, "index absent from declared source"):
            self.check()

    def test_rejects_unsigned_inrelease(self):
        with self.assertRaisesRegex(ProofError, "unsigned InRelease"):
            signed_hashes(b"SHA256:\n synthetic\n")

    def test_rejects_gpgv_failure_even_with_validsig_text(self):
        class Failed:
            returncode = 1
            stdout = "[GNUPG:] VALIDSIG " + "A" * 40

        with patch("verify_captured_indexes.subprocess.run", return_value=Failed()):
            with self.assertRaisesRegex(ProofError, "signature verification failed"):
                signer(self.irelease, self.capture / "trusted-keyring.gpg")

    def test_rejects_capture_symlink_and_byte_cap(self):
        (self.capture / "packages/alias").symlink_to(self.index)
        with self.assertRaisesRegex(ProofError, "capture symlink"):
            self.check()
        (self.capture / "packages/alias").unlink()
        with patch("verify_captured_indexes.MAX_CAPTURE_BYTES", 1):
            with self.assertRaisesRegex(ProofError, "capture byte cap"):
                self.check()

    def test_rejects_fifo_before_reading(self):
        fifo = self.capture / "lists/fifo"
        os.mkfifo(fifo)
        with self.assertRaisesRegex(ProofError, "capture special file"):
            self.check()


if __name__ == "__main__":
    unittest.main()
