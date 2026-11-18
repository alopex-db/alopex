"""Focused sealed-intake rejection and byte-forwarding checks."""
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from . import audit, intake


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def fixture():
    files = {}
    resources = {}
    for case in audit.CASES:
        for trial in range(3):
            key = f"{case}-{trial}"
            resource = encode({"schema": "alopex.sql-operation-resources/v1",
                               "status": "observed",
                               "coverage": "stable_pid_namespace_allocation"})
            files[f"resources/{key}.json"] = resource
            resources[key] = intake.digest(resource)
            for mode in ("on", "off"):
                files[f"raw/{key}-{mode}.ndjson"] = b"sample\n"
    files.update({"resource-manifests.json": encode(resources),
                  "cells.ndjson": b"{}\n" * 24,
                  "client.ndjson": b"{}\n" * 3720,
                  "server.ndjson": b"{}\n" * 23994})
    binding = {"source": {"commit": "a" * 40, "tree": "b" * 40,
                          "profile": "release"},
               "assets_digest": "c" * 64, "controller_sha256": "d" * 64,
               "operator_sha256": "e" * 64, "same_job_id": "job-1",
               "qualification_passed": True, "security_passed": True,
               "cleanup_passed": True}
    manifest = {"schema": intake.SCHEMA, "binding": binding,
                "files": {name: {"sha256": intake.digest(data), "size": len(data)}
                          for name, data in files.items()},
                "cell_count": 24, "client_count": 3720, "server_count": 23994,
                "cleanup_verified": True, "performance_qualification": False}
    files["manifest.json"] = encode(manifest)
    return files, binding


def archive(files):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as bundle:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            bundle.addfile(info, io.BytesIO(data))
    return output.getvalue()


class IntakeTests(unittest.TestCase):
    def test_real_cms_round_trip_requires_pinned_digest(self):
        files, binding = fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cert, key, plaintext, ciphertext = (root / name for name in
                                              ("cert.pem", "key.pem", "archive.tar.gz", "evidence.cms"))
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048",
                            "-nodes", "-subj", "/CN=intake-test", "-days", "1",
                            "-keyout", str(key), "-out", str(cert)],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            plaintext.write_bytes(archive(files))
            subprocess.run(["openssl", "cms", "-encrypt", "-binary", "-aes-256-cbc",
                            "-outform", "DER", "-in", str(plaintext), "-out",
                            str(ciphertext), str(cert)], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            encrypted = ciphertext.read_bytes()
            with patch.object(intake.audit, "audit", return_value={"status": "verified"}):
                self.assertEqual(intake.audit_cms(
                    ciphertext, expected_ciphertext_sha256=intake.digest(encrypted),
                    expected_ciphertext_size=len(encrypted), recipient_certificate=cert,
                    recipient_key=key, expected_binding=binding,
                    expected_fixtures={}), {"status": "verified"})

    def test_forwards_verified_bytes_and_binding(self):
        files, binding = fixture()
        with patch.object(intake.audit, "audit", return_value={"status": "verified"}) as checked:
            self.assertEqual(intake._decode(intake._archive(archive(files)), binding, {}),
                             {"status": "verified"})
        kwargs = checked.call_args.kwargs
        self.assertEqual(kwargs["raw_by_cell"][("aggregate", 0, "on")], b"sample\n")
        self.assertEqual(kwargs["resource_manifests"][("aggregate", 0)],
                         intake.digest(files["resources/aggregate-0.json"]))
        self.assertIs(kwargs["expected_binding"], binding)

    def test_rejects_file_and_binding_drift(self):
        files, binding = fixture()
        files["raw/aggregate-0-on.ndjson"] = b"changed\n"
        with self.assertRaisesRegex(ValueError, "file_digest"):
            intake._decode(intake._archive(archive(files)), binding, {})
        files, binding = fixture()
        wrong = dict(binding, assets_digest="0" * 64)
        with self.assertRaisesRegex(ValueError, "manifest_identity"):
            intake._decode(intake._archive(archive(files)), wrong, {})

    def test_rejects_missing_member_and_unpinned_cms(self):
        files, binding = fixture()
        del files["raw/aggregate-0-on.ndjson"]
        with self.assertRaisesRegex(ValueError, "archive_inventory"):
            intake._archive(archive(files))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("cms", "cert", "key"):
                (root / name).write_bytes(b"x")
            args = {"recipient_certificate": root / "cert", "recipient_key": root / "key",
                    "expected_binding": binding, "expected_fixtures": {}}
            with self.assertRaisesRegex(ValueError, "cms_pin"):
                intake.audit_cms(root / "cms", expected_ciphertext_sha256=None,
                                 expected_ciphertext_size=1, **args)
            with self.assertRaisesRegex(ValueError, "cms_digest"):
                intake.audit_cms(root / "cms", expected_ciphertext_sha256="0" * 64,
                                 expected_ciphertext_size=1, **args)


if __name__ == "__main__":
    unittest.main()
