"""Offline same-job identity and original-reference checks; no Docker or API calls."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO / "scripts/test-env"))
sys.path.insert(0, str(REPO / "scripts/ops"))
sys.path.insert(0, str(HERE))

import same_job_four_case as lane


class SameJobOfflineTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(lane, "REPO", REPO)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_config_pins_qualified_product_policy_reference_and_four_cases(self):
        config = lane.fixed_config()
        self.assertEqual(config["native_acceptance"]["mode"], "same-job-804")
        self.assertEqual(config["subjects"]["candidate"]["artifact_id"], 13895989882)
        self.assertEqual(config["subjects"]["before"]["artifact_id"], 13585234163)
        self.assertEqual(config["subjects"]["published"]["artifact_id"], 13585845849)
        self.assertEqual(config["matrix_seconds"], 7200)
        self.assertEqual(config["limits"], {"case_seconds": 300, "run_seconds": 420})

    def test_replaced_artifact_is_rejected_before_zip_read(self):
        expected = lane.fixed_config()["subjects"]["before"]
        calls = []
        def api(path, **kwargs):
            calls.append(path)
            if "/artifacts?" in path:
                return {"total_count": 1, "artifacts": [{"id": 0}]}
            return {"status": "completed", "conclusion": "success",
                    "head_sha": expected["controller"], "run_attempt": 1}
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(lane.qualification, "gh_api", side_effect=api), \
             self.assertRaises(lane.qualification.Failure):
            lane.reference_archive("before", expected, Path(directory) / "import")
        self.assertFalse(any(path.endswith("/zip") for path in calls))

    def test_workflow_requalifies_before_matrix_and_uploads_evidence(self):
        source = (REPO / ".github/workflows/alopex-performance.yml").read_text()
        positions = [source.index(token) for token in (
            "Bound checkout before source acquisition", "--admission-only",
            "--acquire-assets", "--execute", "Preserve encrypted qualification evidence",
            "scripts/ops/same_job_four_case.py", "native-sql-four-case-export/")]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("timeout-minutes: 360", source)
        self.assertIn("inputs.issue_number == '712' && ((inputs.sql_four_case_mode && !inputs.sql_diagnostic_mode)", source)

    def test_evidence_archive_reserves_both_copies_before_writing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            work = root / "work"
            work.mkdir()
            (work / "sample.json").write_text("{}")
            export = root / "export"
            seen = []
            class RejectingBudget:
                def __init__(self, storage):
                    seen.append(storage["planned_bytes"]["evidence_archive"])
                def before(self, operation):
                    seen.append(operation)
                    raise lane.qualification.Failure("CAPACITY")
            with patch.object(lane, "EXPORT", export), patch.object(lane, "Budget", RejectingBudget), \
                 self.assertRaises(lane.qualification.Failure):
                lane.preserve_evidence(work, {}, {"matrix_evidence_bound_bytes": 1_000},
                                       {"planned_bytes": {}})
            self.assertEqual(seen, [32_002_000, "evidence_archive"])
            self.assertFalse((export / "private-evidence.tar.gz").exists())

    def test_evidence_archive_encrypts_without_exporting_plaintext(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            work = root / "work"
            work.mkdir()
            (work / "sample.json").write_text("{}")
            export = root / "export"
            storage = {"roots": [str(root)], "planned_bytes": {},
                       "reserve_bytes": 1_000_000_000, "stop_bytes": 40_000_000_000,
                       "poll_seconds": 1}
            with patch.object(lane, "EXPORT", export), \
                 patch.object(lane, "CERT", REPO / ".github/ops/native-evidence-cert.pem"):
                lane.preserve_evidence(work, {"status": "passed"},
                                       {"matrix_evidence_bound_bytes": 1_000_000}, storage)
            self.assertTrue((export / "private-evidence.cms").is_file())
            self.assertFalse((export / "private-evidence.tar.gz").exists())
            self.assertFalse((work / "private-evidence.tar.gz").exists())

    def test_failed_encryption_removes_plaintext_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            work = root / "work"
            work.mkdir()
            (work / "sample.json").write_text("{}")
            export = root / "export"
            class FailingBudget:
                def __init__(self, storage): pass
                def before(self, operation): pass
                def check(self): pass
                def run(self, *args, **kwargs):
                    raise lane.qualification.Failure("COMMAND")
            with patch.object(lane, "EXPORT", export), \
                 patch.object(lane, "Budget", FailingBudget), \
                 self.assertRaises(lane.qualification.Failure):
                lane.preserve_evidence(work, {}, {"matrix_evidence_bound_bytes": 1_000_000},
                                       {"planned_bytes": {}})
            self.assertFalse((export / "private-evidence.tar.gz").exists())
            self.assertFalse((work / "private-evidence.tar.gz").exists())


if __name__ == "__main__":
    unittest.main()
