"""Strict collected-pair assembly against the full independent intake audit."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from . import adapter, evidence_assembly, intake, test_contract, test_producer
from .test_qualification import fixture, JOB


class EvidenceAssemblyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.files = test_producer.evidence_files()
        self.cells = [json.loads(line) for line in self.files["cells.ndjson"].splitlines()]
        self.clients = [json.loads(line) for line in self.files["client.ndjson"].splitlines()]
        self.servers = [json.loads(line) for line in self.files["server.ndjson"].splitlines()]
        self.manifest = {"fixture_sha256": "a" * 64,
                         "docs_files": {}, "product_files": {}}
        self.runs = {}
        for case in adapter.CASES:
            for trial in range(3):
                pair = f"{case}-{trial}"
                root = self.root / pair
                collected = root / "collected"
                collected.mkdir(parents=True)
                resource = self.files[f"resources/{pair}.json"]
                server = json.loads(resource)["servers"]["alopex-server"]
                self._write(collected / "resources.json", resource)
                self._write(collected / "result.json",
                            json.dumps({"server": server}).encode())
                for mode in ("on", "off"):
                    directory = collected / mode
                    directory.mkdir()
                    self._write(directory / "raw.ndjson",
                                self.files[f"raw/{pair}-{mode}.ndjson"])
                    rows = [row for row in self.clients if row["cell_id"] ==
                            f"{pair}-{mode}"]
                    self._write(directory / "client.ndjson",
                                b"".join(evidence_assembly.producer._json_line(row)
                                         for row in rows))
                self.runs[(case, trial)] = SimpleNamespace(
                    root=root, record={"phase": "deleted"},
                    verify_collection=lambda: None)

    @staticmethod
    def _write(path, data):
        path.write_bytes(data)
        path.chmod(0o600)

    def _verified(self, config, collected, identity, digest):
        case, trial = config["case"], config["trial"]
        cells = {mode: next(cell for cell in self.cells if
                            (cell["case"], cell["trial"], cell["mode"]) ==
                            (case, trial, mode)) for mode in ("on", "off")}
        trace_ids = {row["trace_id"] for row in self.clients if
                     row.get("trace_id") and row["case"] == case and
                     row["trial"] == trial}
        events = [row for row in self.servers if row["trace_id"] in trace_ids]
        return {"cells": cells, "server_timing": events}

    def assemble(self):
        with patch.object(evidence_assembly, "_pins",
                          return_value=(test_contract.BINDING,
                                        test_contract.FIXTURES)), \
             patch.object(evidence_assembly.supervisor, "run_collected_pair") as verified, \
             patch.object(evidence_assembly.supervisor, "verify_output",
                          side_effect=self._verified):
            files, binding, fixtures = evidence_assembly.assemble_files(
                self.runs, lock={}, manifest=self.manifest, proof={}, receipt={},
                job_id="job-1")
        self.assertEqual(verified.call_count, 12)
        return files, binding, fixtures

    def test_exact_collected_inventory_passes_full_intake(self):
        files, binding, fixtures = self.assemble()
        self.assertEqual(files, self.files)
        self.assertEqual(set(files), set(intake._paths()) - {"manifest.json"})
        self.assertEqual(binding, test_contract.BINDING)
        self.assertEqual(fixtures, test_contract.FIXTURES)

    def test_extra_client_record_rejected_before_seal(self):
        member = self.root / "aggregate-0/collected/on/client.ndjson"
        member.write_bytes(member.read_bytes() + b"{}\n")
        with self.assertRaisesRegex(ValueError, "client_inventory"):
            self.assemble()

    def test_missing_pair_rejected_before_collection(self):
        self.runs.pop(("aggregate", 0))
        with self.assertRaisesRegex(ValueError, "PAIR_INVENTORY"):
            self.assemble()

    def test_binding_is_derived_from_reviewed_lock_and_same_job_proof(self):
        lock, proof, receipt = fixture()
        pin = lock["pin"]
        manifest = {"source_commit": pin["source_commit"],
                    "source_tree": pin["source_tree"],
                    "assets_digest": pin["assets_digest"],
                    "fixture_sha256": "a" * 64,
                    "docs_files": {}, "product_files": {}}
        binding, fixtures = evidence_assembly._pins(
            lock, manifest, proof, receipt, JOB)
        self.assertEqual(binding["same_job_id"], JOB)
        self.assertEqual(binding["source"]["commit"], pin["source_commit"])
        self.assertEqual(binding["controller_sha256"],
                         pin["operator_sha256"]["four_case_diagnostic/controller.py"])
        self.assertEqual(fixtures, {case: "a" * 64 for case in adapter.CASES})
        manifest["source_tree"] = "0" * 40
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            evidence_assembly._pins(lock, manifest, proof, receipt, JOB)

    def test_collected_runs_seal_and_round_trip_through_independent_intake(self):
        cert, key, output = (self.root / name for name in
                             ("recipient.pem", "recipient.key", "evidence.cms"))
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048",
                        "-nodes", "-subj", "/CN=assembly-test", "-days", "1",
                        "-keyout", str(key), "-out", str(cert)],
                       check=True, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        with patch.object(evidence_assembly, "_pins",
                          return_value=(test_contract.BINDING,
                                        test_contract.FIXTURES)), \
             patch.object(evidence_assembly.supervisor, "run_collected_pair"), \
             patch.object(evidence_assembly.supervisor, "verify_output",
                          side_effect=self._verified):
            receipt = evidence_assembly.seal_runs(
                self.runs, lock={}, manifest=self.manifest, proof={}, receipt={},
                job_id="job-1", recipient_certificate=cert, output_path=output)
        result = intake.audit_cms(
            output, expected_ciphertext_sha256=receipt["ciphertext_sha256"],
            expected_ciphertext_size=receipt["ciphertext_size"],
            recipient_certificate=cert, recipient_key=key,
            expected_binding=test_contract.BINDING,
            expected_fixtures=test_contract.FIXTURES)
        self.assertEqual(result["measured_uuids"], 252)
        self.assertEqual(result["measured_server_events"], 1764)
        self.assertFalse(result["performance_qualification"])

    def test_encrypt_failure_leaves_no_strict_artifact(self):
        cert, output = self.root / "recipient.pem", self.root / "evidence.cms"
        cert.write_bytes(b"certificate placeholder")
        with patch.object(evidence_assembly, "_pins",
                          return_value=(test_contract.BINDING,
                                        test_contract.FIXTURES)), \
             patch.object(evidence_assembly.supervisor, "run_collected_pair"), \
             patch.object(evidence_assembly.supervisor, "verify_output",
                          side_effect=self._verified), \
             patch.object(evidence_assembly.producer.subprocess, "run",
                          side_effect=subprocess.TimeoutExpired("openssl", 120)), \
             self.assertRaisesRegex(ValueError, "cms_encrypt"):
            evidence_assembly.seal_runs(
                self.runs, lock={}, manifest=self.manifest, proof={}, receipt={},
                job_id="job-1", recipient_certificate=cert, output_path=output)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
