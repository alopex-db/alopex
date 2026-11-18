"""Focused producer-to-intake compatibility and rejection checks."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from . import audit, intake, producer, test_contract


def lines(rows):
    return b"".join(producer._json_line(row) for row in rows)


def evidence_files():
    cells, clients, servers, raw, _ = test_contract.evidence()
    files = {"client.ndjson": lines(clients), "server.ndjson": lines(servers)}
    resources = {}
    for case in audit.CASES:
        for trial in range(3):
            key = f"{case}-{trial}"
            resource = producer._json_line({
                "schema": "alopex.sql-operation-resources/v1",
                "status": "observed", "coverage": "stable_pid_namespace_allocation"})
            digest = intake.digest(resource)
            resources[key] = digest
            files[f"resources/{key}.json"] = resource
            for mode in ("on", "off"):
                raw_key = (case, trial, mode)
                rows = [json.loads(line) for line in raw[raw_key].splitlines()]
                for row in rows:
                    row["resources"]["manifest_sha256"] = digest
                data = lines(rows)
                files[f"raw/{key}-{mode}.ndjson"] = data
                next(cell for cell in cells if
                     (cell["case"], cell["trial"], cell["mode"]) == raw_key)["raw_sha256"] = intake.digest(data)
    files["resource-manifests.json"] = producer._json_line(resources)
    files["cells.ndjson"] = lines(cells)
    return files


class ProducerTests(unittest.TestCase):
    def test_real_cms_round_trip_to_full_intake_audit(self):
        files = evidence_files()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cert, key, output = (root / name for name in ("cert.pem", "key.pem", "evidence.cms"))
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048",
                            "-nodes", "-subj", "/CN=producer-test", "-days", "1",
                            "-keyout", str(key), "-out", str(cert)],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            receipt = producer.seal_evidence(
                files, binding=test_contract.BINDING,
                expected_fixtures=test_contract.FIXTURES,
                recipient_certificate=cert, output_path=output)
            self.assertEqual(set(receipt), {"ciphertext_sha256", "ciphertext_size"})
            self.assertEqual(receipt["ciphertext_sha256"], intake.digest(output.read_bytes()))
            self.assertEqual(receipt["ciphertext_size"], output.stat().st_size)
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            result = intake.audit_cms(
                output, expected_ciphertext_sha256=receipt["ciphertext_sha256"],
                expected_ciphertext_size=receipt["ciphertext_size"],
                recipient_certificate=cert, recipient_key=key,
                expected_binding=test_contract.BINDING,
                expected_fixtures=test_contract.FIXTURES)
            self.assertEqual((result["measured_uuids"], result["measured_server_events"]),
                             (252, 1764))
            self.assertFalse(result["performance_qualification"])
            with self.assertRaisesRegex(ValueError, "cms_output"):
                producer.seal_evidence(
                    files, binding=test_contract.BINDING,
                    expected_fixtures=test_contract.FIXTURES,
                    recipient_certificate=cert, output_path=output)

    def test_rejects_invalid_and_oversize_before_encrypt(self):
        files = evidence_files()
        with patch.object(producer.subprocess, "run") as encrypt:
            bad = dict(files, **{"client.ndjson": files["client.ndjson"] + b"{}\n"})
            with self.assertRaisesRegex(ValueError, "client_inventory"):
                producer.seal_evidence(
                    bad, binding=test_contract.BINDING,
                    expected_fixtures=test_contract.FIXTURES,
                    recipient_certificate="unused", output_path="unused")
            bad = dict(files, **{"server.ndjson": b"x" * (intake.BASE["server.ndjson"] + 1)})
            with self.assertRaisesRegex(ValueError, "archive_bound"):
                producer.seal_evidence(
                    bad, binding=test_contract.BINDING,
                    expected_fixtures=test_contract.FIXTURES,
                    recipient_certificate="unused", output_path="unused")
            bad = dict(files, **{"unexpected.txt": b"private"})
            with self.assertRaisesRegex(ValueError, "archive_inventory"):
                producer.seal_evidence(
                    bad, binding=test_contract.BINDING,
                    expected_fixtures=test_contract.FIXTURES,
                    recipient_certificate="unused", output_path="unused")
            encrypt.assert_not_called()


if __name__ == "__main__":
    unittest.main()
