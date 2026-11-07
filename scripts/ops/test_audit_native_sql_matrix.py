import copy
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import audit_native_sql_matrix as audit


class CellAuditTests(unittest.TestCase):
    def setUp(self):
        self.source = {"commit": "a" * 40, "tree": "b" * 40, "profile": "release"}
        self.config = {"subjects": {"candidate": {"source_commit": self.source["commit"], "source_tree": self.source["tree"]}}}
        self.row = {"case": "aggregate", "trial": 0, "subject": "candidate", "source": self.source,
                    "result_checksum": "c" * 64, "trial_median_seconds": .001, "sample_mad_seconds": 0.0}
        resource = b"{}"
        process = dict.fromkeys(audit.COUNTERS, 0)
        process.update(pid=2, start_ticks=5)
        observation = {"schema": "alopex.sql-operation-resources/v1", "status": "observed",
                       "coverage": "stable_pid_namespace_allocation", "manifest_sha256": audit.sha(resource),
                       "client": dict(process, pid=3), "server_processes": [process]}
        self.raw = [{"sample": i, "seconds": .001, "resources": copy.deepcopy(observation)} for i in range(21)]
        self.result = {"case": "aggregate", "engine": "alopex-server", "trial": 0,
                       "subject_identity": {"source": self.source}, "server_client": True,
                       "fixture_rows": 34250, "warmups": 3, "samples": 21,
                       "result_sha256": "c" * 64, "latency_p50_ms": 1.0}
        self.phase = "deleted"

    def evidence(self):
        encoded = lambda value: json.dumps(value).encode()
        raw_bytes = b"\n".join(encoded(row) for row in self.raw)
        self.result["raw_sha256"] = self.row["raw_sha256"] = audit.sha(raw_bytes)
        result_bytes = encoded(self.result)
        status = {"status": "passed", "worker_error": None, "stop_error": None,
                  "measurement": {"result_sha256": audit.sha(result_bytes), "raw_sha256": audit.sha(raw_bytes)}}
        report = {"schema": "alopex.native-sql-measurement/v1", "case": "aggregate", "engine": "alopex-server", "trial": 0,
                  "status": "passed", "source": self.source, "assets_digest": "d" * 64,
                  "cell_status_sha256": audit.sha(encoded(status))}
        files = {"result.json": encoded(report), "cell/status.json": encoded(status),
                 "cell/raw.ndjson": raw_bytes, "cell/measurement.json": result_bytes, "cell/resources.json": b"{}"}
        prefix = "results/sql-aggregate-0-candidate/"
        data = {prefix + "collected/" + name: value for name, value in files.items()}
        data[prefix + "collection.json"] = encoded({name: audit.sha(value) for name, value in files.items()})
        data[prefix + "run.json"] = encoded({"phase": self.phase, "identities": {"assets_digest": "d" * 64}})
        class FakeEvidence:
            def read(self, name): return data[name]
            def json(self, name): return json.loads(self.read(name))
        return FakeEvidence(), data

    def verify(self):
        evidence, _ = self.evidence()
        return audit.audit_cell(evidence, self.row, self.config)

    def test_complete_cell_recomputes_raw_median_and_resources(self):
        self.assertEqual(self.verify(), .001)

    def test_changed_summary_fails_even_with_matching_file_hashes(self):
        self.row["trial_median_seconds"] = .0005
        with self.assertRaises(ValueError): self.verify()

    def test_missing_sample_is_not_replaced_by_summary(self):
        self.raw.pop()
        with self.assertRaises(ValueError): self.verify()

    def test_missing_io_and_duplicate_process_fail(self):
        del self.raw[0]["resources"]["client"]["read_bytes"]
        with self.assertRaises(KeyError): self.verify()
        self.raw[0]["resources"]["client"]["read_bytes"] = 0
        self.raw[0]["resources"]["client"]["pid"] = 2
        with self.assertRaises(ValueError): self.verify()

    def test_wrong_source_fails_even_with_self_consistent_receipts(self):
        self.config["subjects"]["candidate"]["source_commit"] = "e" * 40
        with self.assertRaises(ValueError): self.verify()

    def test_cleanup_not_finished_cannot_pass(self):
        self.phase = "collected"
        with self.assertRaises(ValueError): self.verify()

    def test_tampered_collection_bytes_fail(self):
        evidence, data = self.evidence()
        data["results/sql-aggregate-0-candidate/collected/cell/resources.json"] = b"changed"
        with self.assertRaises(ValueError): audit.audit_cell(evidence, self.row, self.config)


class DecisionTests(unittest.TestCase):
    def test_thresholds_and_noise_preserve_unaccepted_results(self):
        self.assertEqual(audit.decision([100] * 3, [105] * 3)["status"], "within_tolerance")
        self.assertEqual(audit.decision([100] * 3, [105.01] * 3)["status"], "regression")
        self.assertEqual(audit.decision([97, 100, 103], [90] * 3)["status"], "pending_noise")
        self.assertFalse(audit.decision([120] * 3, [112, 114, 116])["improvement_proven"])
        self.assertTrue(audit.decision([100] * 3, [95] * 3)["improvement_proven"])

    def test_malformed_numeric_values_fail(self):
        for value in (False, 0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError): audit.decision([100] * 3, [value] * 3)

    def test_boolean_summary_cannot_match_numeric_one(self):
        with self.assertRaises(ValueError): audit.equal({"passed": True}, {"passed": 1})

    def test_archive_rejects_traversal_and_links(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.tar"
            for name, kind in (("../outside", tarfile.REGTYPE), ("link", tarfile.SYMTYPE)):
                with tarfile.open(path, "w") as archive:
                    member = tarfile.TarInfo(name); member.type = kind
                    archive.addfile(member, io.BytesIO())
                with self.assertRaises(ValueError): audit.Evidence(path)


class MatrixAuditTests(unittest.TestCase):
    def setUp(self):
        import native_sql_matrix
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.export = Path(self.temp.name)
        self.config = {"image": {"image_record": {"id": "sha256:" + "a" * 64}}}
        self.rows = [dict(row, status="passed", trial_median_seconds=.001,
                          result_checksum="b" * 64, input_checksum="c" * 64)
                     for row in native_sql_matrix.plan()]
        self.summarize = native_sql_matrix.summarize

    def verify(self, wrong_ratio=False, false_complete=False):
        result = self.summarize(self.rows)
        if wrong_ratio: result["cases"][0]["reference_latency_ratio"] = 2.0
        encoded = json.dumps(result).encode()
        (self.export / "comparison.json").write_bytes(encoded)
        complete = all(row["status"] == "passed" for row in self.rows)
        public = {"schema": "alopex.native-sql-matrix/v1", "image_digest": "a" * 64,
                  "performance_qualification": False, "owned_resources_cleaned": True,
                  "status": "passed" if complete else "failed", "matrix_complete": True if false_complete else complete,
                  "completed_cells": sum(row["status"] == "passed" for row in self.rows),
                  "comparison_sha256": audit.sha(encoded)}
        (self.export / "result.json").write_text(json.dumps(public))
        values = {"matrix-plan.json": {"configuration": self.config}, "matrix-results.json": self.rows,
                  "comparison.json": result}
        class FakeEvidence:
            members = set(values)
            def json(self, name): return values[name]
            def close(self): pass
        with patch.object(audit, "Evidence", return_value=FakeEvidence()), \
             patch.object(audit, "audit_cell", side_effect=lambda evidence, row, config: row["trial_median_seconds"]):
            return audit.audit(self.export, "unused", self.config)

    def test_complete_matrix_reconciles_producer_comparison_without_acceptance_claim(self):
        result = self.verify()
        self.assertEqual(result["verified_cells"], 120)
        self.assertEqual(result["verified_samples"], 2520)
        self.assertTrue(result["measurement_complete"])
        self.assertFalse(result["performance_acceptance"])

    def test_failed_cell_is_preserved_and_does_not_pass(self):
        self.rows[0]["status"] = "failed"
        result = self.verify()
        self.assertEqual(result["verified_cells"], 119)
        self.assertFalse(result["measurement_complete"])
        self.assertEqual(result["case_status"]["aggregate"], "failed_cell")
        with self.assertRaises(ValueError): self.verify(false_complete=True)

    def test_consistently_altered_public_private_ratio_still_fails_raw_recalculation(self):
        with self.assertRaises(ValueError): self.verify(wrong_ratio=True)


if __name__ == "__main__":
    unittest.main()
