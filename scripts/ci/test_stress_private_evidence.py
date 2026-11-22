"""Synthetic CMS and denial checks for Stage-2 private evidence."""

import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import stress_private_evidence as evidence
import stress_public_evidence as public_evidence


SOURCE = "d" * 40


def make_checkout(workspace: Path) -> str:
    workspace.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "-C", str(workspace), "init", "-q"],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run([
        "git", "-C", str(workspace), "-c", "user.name=Test",
        "-c", "user.email=test@example.invalid", "commit", "--allow-empty",
        "-qm", "synthetic source",
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return subprocess.check_output(
        ["git", "-C", str(workspace), "rev-parse", "HEAD"], text=True,
    ).strip()


def certificate_pair(root: Path) -> tuple[Path, Path]:
    cert, key = root / "recipient.pem", root / "recipient.key"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", str(key), "-out", str(cert), "-days", "1",
        "-subj", "/CN=synthetic-stage2-test",
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return cert, key


class PrivateEvidenceTests(unittest.TestCase):
    def test_rejects_symlinked_private_command_log(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            workspace.mkdir()
            private = root / "stress-private"
            private.mkdir(mode=0o700)
            raw = root / "raw.log"
            raw.write_bytes(b"machine detail")
            log = private / "sql_parser-test.output.private.log"
            log.symlink_to(raw)
            with self.assertRaises(ValueError):
                evidence.collect_files(workspace, "sql_parser", log)

    def test_exact_case_is_encrypted_and_decryptable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            report = workspace / "target/stress-reports"
            report.mkdir(parents=True)
            source = make_checkout(workspace)
            secret = b"machine=private path=/private/secret"
            (report / "run.json").write_bytes(secret)
            cert, key = certificate_pair(root)
            output = root / "evidence.cms"
            result = evidence.seal(
                workspace=workspace, case_id="stress", source_commit=source,
                certificate=cert,
                expected_cert_sha256=hashlib.sha256(cert.read_bytes()).hexdigest(),
                output=output,
            )
            self.assertEqual(result["file_count"], 1)
            self.assertEqual(result["ciphertext_sha256"],
                             hashlib.sha256(output.read_bytes()).hexdigest())
            self.assertNotIn(secret, output.read_bytes())
            public_status = public_evidence.build_status(
                case_id="stress", source_commit=source,
                outcome="passed", error_code="NONE", ciphertext=output,
            )
            self.assertEqual(public_status["ciphertext_sha256"],
                             result["ciphertext_sha256"])
            self.assertEqual(public_status["ciphertext_size"],
                             result["ciphertext_size"])
            decrypted = subprocess.run([
                "openssl", "cms", "-decrypt", "-binary", "-inform", "DER",
                "-in", str(output), "-recip", str(cert), "-inkey", str(key),
            ], check=True, capture_output=True).stdout
            with tarfile.open(fileobj=io.BytesIO(decrypted), mode="r:gz") as bundle:
                self.assertEqual(sorted(bundle.getnames()),
                                 ["manifest.json", "reports/run.json"])
                self.assertEqual(bundle.extractfile("reports/run.json").read(), secret)
                manifest = json.load(bundle.extractfile("manifest.json"))
                self.assertEqual(manifest["source_commit"], source)
                self.assertEqual(manifest["files"][0]["sha256"],
                                 hashlib.sha256(secret).hexdigest())

    def test_rejects_symlink_unbounded_file_and_wrong_recipient(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            report = workspace / "target/stress-reports"
            report.mkdir(parents=True)
            source = make_checkout(workspace)
            raw = report / "run.json"
            raw.write_bytes(b"private")
            cert, _ = certificate_pair(root)
            args = dict(workspace=workspace, case_id="stress", source_commit=source,
                        certificate=cert,
                        expected_cert_sha256=hashlib.sha256(cert.read_bytes()).hexdigest(),
                        output=root / "evidence.cms")
            link = report / "link.json"
            link.symlink_to(raw)
            with self.assertRaises(ValueError):
                evidence.seal(**args)
            link.unlink()
            with patch.object(evidence, "MAX_FILE_BYTES", 3):
                with self.assertRaises(ValueError):
                    evidence.seal(**args)
            with self.assertRaises(ValueError):
                evidence.seal(**{**args, "expected_cert_sha256": "0" * 64})
            with self.assertRaises(ValueError):
                evidence.seal(**{**args, "source_commit": SOURCE})
            self.assertFalse(args["output"].exists())

    def test_rejects_target_symlink_and_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            workspace.mkdir()
            outside = root / "outside"
            (outside / "stress-reports").mkdir(parents=True)
            (outside / "stress-reports/run.json").write_bytes(b"private")
            (workspace / "target").symlink_to(outside)
            with self.assertRaises(ValueError):
                evidence.collect_files(workspace, "stress")
            cert, _ = certificate_pair(root)
            output = root / "evidence.cms"
            output.write_bytes(b"keep")
            with self.assertRaises(ValueError):
                evidence.seal(
                    workspace=workspace, case_id="stress", source_commit=SOURCE,
                    certificate=cert,
                    expected_cert_sha256=hashlib.sha256(cert.read_bytes()).hexdigest(),
                    output=output,
                )
            self.assertEqual(output.read_bytes(), b"keep")

    def test_cli_failure_uses_fixed_code_without_private_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cert, _ = certificate_pair(root)
            private_workspace = root / "sensitive-host-location"
            result = subprocess.run([
                sys.executable, str(Path(evidence.__file__)),
                "--workspace", str(private_workspace),
                "--case-id", "stress", "--source-commit", SOURCE,
                "--certificate", str(cert),
                "--expected-cert-sha256", hashlib.sha256(cert.read_bytes()).hexdigest(),
                "--output", str(root / "evidence.cms"),
            ], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stderr.strip(), "STRESS_PRIVATE_EVIDENCE_REJECTED")
            self.assertNotIn("sensitive-host-location", result.stdout + result.stderr)
            self.assertFalse((root / "evidence.cms").exists())

    def test_rejects_non_private_output_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            report = workspace / "target/stress-reports"
            report.mkdir(parents=True)
            source = make_checkout(workspace)
            (report / "run.json").write_bytes(b"private")
            cert, _ = certificate_pair(root)
            public = root / "public"
            public.mkdir(mode=0o755)
            with self.assertRaises(ValueError):
                evidence.seal(
                    workspace=workspace, case_id="stress", source_commit=source,
                    certificate=cert,
                    expected_cert_sha256=hashlib.sha256(cert.read_bytes()).hexdigest(),
                    output=public / "evidence.cms",
                )
            self.assertFalse((public / "evidence.cms").exists())


if __name__ == "__main__":
    unittest.main()
