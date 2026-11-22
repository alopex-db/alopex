"""Boundary checks for public Stage-2 evidence."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import stress_public_evidence as evidence


SOURCE = "d" * 40
CERT = Path(__file__).resolve().parents[2] / ".github/ops/native-evidence-cert.pem"


def make_cms(root: Path, name: str, plaintext: bytes) -> Path:
    source = root / "synthetic-input"
    output = root / name
    source.write_bytes(plaintext)
    try:
        subprocess.run([
            "openssl", "cms", "-encrypt", "-binary", "-aes-256-cbc",
            "-outform", "DER", "-in", str(source), "-out", str(output),
            str(CERT),
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    finally:
        source.unlink()
    return output


class PublicEvidenceTests(unittest.TestCase):
    def test_output_contains_only_fixed_fields_and_ciphertext_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            secret = b"host=private-machine path=/private/sensitive token=secret"
            cms = make_cms(root, "private.cms", secret)
            status = evidence.build_status(
                case_id="dataframe_conversion", source_commit=SOURCE,
                outcome="passed", error_code="NONE", ciphertext=cms,
            )
            output = root / "status.json"
            evidence.write_status(output, status)
            body = output.read_bytes()
            self.assertEqual(set(json.loads(body)), {
                "schema", "case_id", "source_commit", "outcome", "error_code",
                "ciphertext_sha256", "ciphertext_size",
            })
            self.assertEqual(status["ciphertext_sha256"],
                             hashlib.sha256(cms.read_bytes()).hexdigest())
            self.assertNotIn(secret, body)
            self.assertNotIn(str(root).encode(), body)

    def test_rejects_unsafe_identity_or_status_without_public_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cms = make_cms(root, "raw.cms", b"private")
            link = root / "link.cms"
            link.symlink_to(cms)
            for changes in (
                {"case_id": "../../host"},
                {"source_commit": "not-a-commit"},
                {"outcome": "passed", "error_code": "STORAGE"},
                {"outcome": "failed", "error_code": "NONE"},
                {"ciphertext": link},
            ):
                args = dict(case_id="stress", source_commit=SOURCE,
                            outcome="failed", error_code="TEST", ciphertext=cms)
                args.update(changes)
                with self.subTest(changes=changes), self.assertRaises(ValueError):
                    evidence.build_status(**args)
            with patch.object(evidence, "MAX_CMS_BYTES", 3):
                with self.assertRaises(ValueError):
                    evidence.ciphertext_identity(cms)
            invalid = root / "plaintext.cms"
            invalid.write_bytes(b"not encrypted")
            with self.assertRaises(ValueError):
                evidence.ciphertext_identity(invalid)

    def test_refuses_to_replace_existing_status_or_follow_output_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            status = evidence.build_status(case_id="stress", source_commit=SOURCE,
                                           outcome="passed", error_code="NONE")
            existing = root / "existing.json"
            existing.write_text("keep")
            link = root / "link.json"
            link.symlink_to(existing)
            for path in (existing, link):
                with self.assertRaises(ValueError):
                    evidence.write_status(path, status)
            self.assertEqual(existing.read_text(), "keep")

    def test_write_api_rejects_extra_raw_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            status = evidence.build_status(case_id="perf", source_commit=SOURCE,
                                           outcome="failed", error_code="PERF")
            status["host_path"] = "/private/secret"
            with self.assertRaises(ValueError):
                evidence.write_status(root / "status.json", status)
            self.assertFalse((root / "status.json").exists())

    def test_cli_failure_prints_only_fixed_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private_path = root / "sensitive-host-name.cms"
            private_path.symlink_to(root / "missing")
            result = subprocess.run([
                sys.executable, str(Path(evidence.__file__)),
                "--case-id", "stress", "--source-commit", SOURCE,
                "--outcome", "failed", "--error-code", "EVIDENCE",
                "--ciphertext", str(private_path),
                "--output", str(root / "public.json"),
            ], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stderr.strip(), "STRESS_PUBLIC_EVIDENCE_REJECTED")
            self.assertNotIn("sensitive-host-name", result.stdout + result.stderr)
            self.assertFalse((root / "public.json").exists())


if __name__ == "__main__":
    unittest.main()
