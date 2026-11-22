"""Public artifact completeness and private-field exclusion checks."""
import copy
import hashlib
import json
import unittest

from scripts.ops.final_sql_runtime import matrix, operation, public_export
from scripts.ops.final_sql_runtime.test_operation import OperationTests


class PublicExportTests(unittest.TestCase):
    def setUp(self):
        fixture = OperationTests()
        fixture.setUp()
        self.operation = operation.run(**fixture.args())
        self.audit = {"verified_cells": 48, "verified_samples": 1008,
                      "performance_acceptance": False,
                      "public_sha256": hashlib.sha256(json.dumps(
                          self.operation["evidence"]["public_operations"],
                          separators=(",", ":")).encode()).hexdigest()}
        source = {"commit": "a" * 40, "tree": "b" * 40, "profile": "release"}
        self.pins = {subject: {"source": source, "assets_digest": "c" * 64}
                     for subject in matrix.SUBJECTS}

    def test_complete_52_run_public_report_has_1008_allowlisted_rows(self):
        report = public_export.completed(self.operation, self.audit, self.pins)
        self.assertEqual(report["completed_runs"], 52)
        self.assertEqual(len(report["preflights"]), 4)
        self.assertEqual(len(report["operations"]), 1008)
        self.assertFalse(report["performance_acceptance"])
        text = json.dumps(report)
        for forbidden in ("raw", "pid", "hostname", "local_path", "exception", "/private/"):
            if forbidden == "pid":
                self.assertFalse(any("pid" in key for row in report["operations"] for key in row))
            else:
                self.assertNotIn(forbidden, text)

    def test_missing_mutated_and_extra_public_rows_fail(self):
        for edit in (
            lambda value: value["evidence"]["public_operations"].pop(),
            lambda value: value["evidence"]["public_operations"][0].update(elapsed_seconds=2),
            lambda value: value["evidence"]["public_operations"][0].update(local_path="/private/x"),
            lambda value: value["evidence"]["public_operations"].__setitem__(0,
                copy.deepcopy(value["evidence"]["public_operations"][1])),
        ):
            with self.subTest(edit=edit):
                value = copy.deepcopy(self.operation)
                edit(value)
                with self.assertRaises(ValueError):
                    public_export.completed(value, self.audit, self.pins)

    def test_summary_is_computed_from_public_rows(self):
        original = public_export.completed(self.operation, self.audit, self.pins)["summary"]
        changed = copy.deepcopy(self.operation)
        changed["evidence"]["cells"][0]["trial_median_seconds"] += 1e-16
        changed["summary"] = matrix.summarize(changed["evidence"]["cells"])
        self.assertNotEqual(changed["summary"], original)
        self.assertEqual(public_export.completed(changed, self.audit, self.pins)["summary"],
                         original)

    def test_failure_status_never_copies_exception_text(self):
        with self.assertRaises(ValueError):
            public_export.failed("/private/exception", 12)
        self.assertEqual(public_export.failed("PUBLIC_INCOMPLETE", 12),
                         {"schema": public_export.SCHEMA, "status": "failed",
                          "error_code": "PUBLIC_INCOMPLETE", "completed_runs": 12,
                          "performance_acceptance": False})


if __name__ == "__main__":
    unittest.main()
