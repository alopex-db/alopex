"""Actual final SQL public file boundary and private/public rejoin tests."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scripts.ops.final_sql_runtime import controller, public_export, public_upload
from scripts.ops.final_sql_runtime import test_audit_bridge as fixtures


class PublicUploadTests(unittest.TestCase):
    def setUp(self):
        fixture = fixtures.AuditBridgeTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        for pin in fixture.pins.values():
            pin["source"].update(commit="a" * 40, tree="b" * 40,
                                 profile="release")
        for collection, receipt in zip(fixture.collections, fixture.receipts):
            path = collection["files"]["measurement"]
            measurement = json.loads(path.read_bytes())
            measurement["subject_identity"]["source"] = fixture.pins[
                collection["subject"]]["source"]
            data = json.dumps(measurement).encode()
            path.write_bytes(data)
            receipt["sha256"]["measurement"] = hashlib.sha256(data).hexdigest()
        self.operation = fixture.operation
        self.audit = fixture.audit()
        self.pins = fixture.pins
        self.fixture = fixture
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve() / "export"
        self.directory.mkdir(mode=0o700)

    def result(self):
        operation = copy.deepcopy(self.operation)
        audit = copy.deepcopy(self.audit)
        return {"qualification": {}, "operation": operation,
                "audit": audit,
                "public": public_export.completed(operation, audit, self.pins),
                "performance_acceptance": False}

    def evidence(self, _operation):
        return {"collections": self.fixture.collections,
                "receipts": self.fixture.receipts,
                "preflight_collections": self.fixture.preflight_collections,
                "preflight_receipts": self.fixture.preflight_receipts,
                "authenticate_receipt": self.fixture.authenticate}

    def test_success_is_one_recomputed_public_file(self):
        result = self.result()
        report = public_upload.prepare_success(result, self.pins, self.directory,
                                               self.evidence)
        self.assertEqual(public_upload.verify(self.directory, pins=self.pins,
                                              private_result=result,
                                              independent_evidence=self.evidence), report)
        self.assertEqual([path.name for path in self.directory.iterdir()], ["public.json"])
        self.assertEqual(len(report["operations"]), 1008)
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public_upload.verify(self.directory, pins=self.pins, private_result=result,
                                 independent_evidence=None)

    def test_changed_private_or_public_rows_fail_at_export_boundary(self):
        result = self.result()
        result["operation"]["evidence"]["public_operations"][0]["elapsed_seconds"] = 2
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public_upload.prepare_success(result, self.pins, self.directory, self.evidence)
        self.assertFalse(list(self.directory.iterdir()))
        good = self.result()
        report = public_upload.prepare_success(good, self.pins, self.directory,
                                               self.evidence)
        changed = copy.deepcopy(report)
        changed["operations"][0]["elapsed_seconds"] = 2
        (self.directory / "public.json").write_text(json.dumps(
            changed, sort_keys=True, separators=(",", ":")) + "\n")
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public_upload.verify(self.directory, pins=self.pins, private_result=good,
                                 independent_evidence=self.evidence)

    def test_extra_private_file_blocks_upload(self):
        result = self.result()
        public_upload.prepare_success(result, self.pins, self.directory, self.evidence)
        (self.directory / "private-evidence.cms").write_text("private")
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public_upload.verify(self.directory, pins=self.pins, private_result=result,
                                 independent_evidence=self.evidence)

    def test_changed_source_pin_and_extra_public_field_fail(self):
        result = self.result()
        public_upload.prepare_success(result, self.pins, self.directory, self.evidence)
        other_pins = copy.deepcopy(self.pins)
        other_pins["before"]["assets_digest"] = "d" * 64
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public_upload.verify(self.directory, pins=other_pins, private_result=result,
                                 independent_evidence=self.evidence)
        self.assertFalse(list(self.directory.iterdir()))
        public_upload.prepare_success(result, self.pins, self.directory, self.evidence)
        report = copy.deepcopy(result["public"])
        report["debug_path"] = "/private/secret"
        (self.directory / "public.json").write_text(json.dumps(
            report, sort_keys=True, separators=(",", ":")) + "\n")
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public_upload.verify(self.directory, pins=self.pins, private_result=result,
                                 independent_evidence=self.evidence)
        self.assertFalse(list(self.directory.iterdir()))

    def test_post_audit_private_mutation_blocks_upload(self):
        result = self.result()
        public_upload.prepare_success(result, self.pins, self.directory, self.evidence)
        path = self.fixture.collections[0]["files"]["raw"]
        path.write_bytes(path.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public_upload.verify(self.directory, pins=self.pins, private_result=result,
                                 independent_evidence=self.evidence)
        self.assertFalse((self.directory / "public.json").exists())

    def test_fixed_failure_log_excludes_private_cause_and_artifact(self):
        failure = controller.ControllerFailure("PUBLIC_INCOMPLETE", 52,
                                               RuntimeError("/private/secret"))
        body = public_upload.failure_log(failure)
        self.assertNotIn(b"/private/secret", body)
        self.assertEqual(json.loads(body)["completed_runs"], "unknown")
        self.assertFalse(list(self.directory.iterdir()))


if __name__ == "__main__":
    unittest.main()
