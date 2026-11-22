"""Synthetic GitHub artifact ZIP intake checks."""

import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import stress_artifact_intake as intake


CERT = Path(__file__).resolve().parents[2] / ".github/ops/native-evidence-cert.pem"


def zip_artifact(path: Path, name: str, body: bytes,
                 *, symlink: bool = False) -> str:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        member = zipfile.ZipInfo(name)
        member.compress_type = zipfile.ZIP_DEFLATED
        if symlink:
            member.create_system = 3
            member.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(member, body)
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def make_cms(root: Path) -> bytes:
    source = root / "raw.txt"
    output = root / "raw.cms"
    source.write_bytes(b"private diagnostics")
    subprocess.run(["openssl", "cms", "-encrypt", "-binary", "-aes-256-cbc",
                    "-outform", "DER", "-in", str(source), "-out", str(output),
                    str(CERT)], check=True, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
    return output.read_bytes()


class ArtifactIntakeTests(unittest.TestCase):
    def test_reads_public_status_from_exact_zip(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            status = {"case_id": "perf", "outcome": "passed"}
            artifact = root / "public.zip"
            digest = zip_artifact(artifact, "perf.json", json.dumps(status).encode())
            self.assertEqual(intake.read_public_status(artifact, digest), status)

    def test_writes_only_verified_cms_into_private_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cms = make_cms(root)
            artifact = root / "private.zip"
            digest = zip_artifact(artifact, "perf.cms", cms)
            destination_dir = root / "private"
            destination_dir.mkdir(mode=0o700)
            output = destination_dir / "perf.cms"
            intake.write_private_ciphertext(
                artifact, digest, hashlib.sha256(cms).hexdigest(), output)
            self.assertEqual(output.read_bytes(), cms)
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)

    def test_rejects_zip_digest_member_and_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "public.zip"
            digest = zip_artifact(artifact, "perf.json", b"{}")
            with self.assertRaises(ValueError):
                intake.read_public_status(artifact, "sha256:" + "0" * 64)
            digest = zip_artifact(artifact, "../perf.json", b"{}")
            with self.assertRaises(ValueError):
                intake.read_public_status(artifact, digest)
            digest = zip_artifact(artifact, "perf.json", b"{}", symlink=True)
            with self.assertRaises(ValueError):
                intake.read_public_status(artifact, digest)
            fifo = root / "fifo.zip"
            os.mkfifo(fifo)
            with self.assertRaises(ValueError):
                intake.read_public_status(fifo, digest)

    def test_rejects_unbounded_or_wrong_cms_without_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "private.zip"
            private_dir = root / "private"
            private_dir.mkdir(mode=0o700)
            output = private_dir / "perf.cms"
            digest = zip_artifact(artifact, "perf.cms", b"plaintext")
            with self.assertRaises(ValueError):
                intake.write_private_ciphertext(artifact, digest,
                                                hashlib.sha256(b"plaintext").hexdigest(),
                                                output)
            self.assertFalse(output.exists())
            with patch.object(intake, "MAX_CMS_BYTES", 3):
                with self.assertRaises(ValueError):
                    intake.write_private_ciphertext(artifact, digest,
                                                    hashlib.sha256(b"plaintext").hexdigest(),
                                                    output)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
