import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

from common import digest, verify_assets
import native_begin_matrix as matrix
import native_sql_measure_container as container
import test_native_sql_prepare as fixtures
from test_native_begin_supervisor import CONFIG, DIGEST, events


class BeginMatrixTests(unittest.TestCase):
    def setUp(self):
        fixtures.PrepareCellTests.setUp(self)
        with zipfile.ZipFile(self.source / "fixture.whl", "a") as archive:
            archive.writestr("alopex/_alopex.abi3.so", b"fixture extension")
        receipt = json.loads((self.source / "receipt.json").read_text())
        receipt["python_wheel_sha256"] = digest(self.source / "fixture.whl")
        (self.source / "receipt.json").write_text(json.dumps(receipt))
        manifest = json.loads(self.manifest.read_text())
        for item in manifest["files"]:
            file = self.source / item["path"]
            item.update(size=file.stat().st_size, sha256=digest(file))
        self.manifest.write_text(json.dumps(manifest))

    def test_plan_has_all_fixture_subject_trial_combinations_once(self):
        plan = matrix.plan()
        self.assertEqual(len(plan), 54)
        self.assertEqual(len({row["id"] for row in plan}), 54)
        self.assertEqual({(r["rows"], r["indexes"], r["trial"], r["subject"]) for r in plan},
                         {(rows, indexes, trial, subject) for rows in (0, 100, 1000)
                          for indexes in (0, 1, 2) for trial in range(3) for subject in ("before", "candidate")})
        self.assertEqual([row["subject"] for row in plan[:6]],
                         ["before", "candidate", "candidate", "before", "before", "candidate"])

    def test_preparation_preserves_all_original_assets_and_receipt(self):
        original = self.manifest.read_bytes()
        path, config = matrix.prepare_cell(self.manifest, self.root / "input", matrix.plan()[0], self.operators)
        verified = verify_assets(path, "fixture-platform")
        for item in json.loads(original)["files"]:
            self.assertEqual((path.parent / item["path"]).read_bytes(), (self.source / item["path"]).read_bytes())
        self.assertEqual(self.manifest.read_bytes(), original)
        self.assertEqual(config["extension_sha256"], matrix.extension_digest(self.source / "fixture.whl"))
        self.assertEqual(len(verified["files"]), len(json.loads(original)["files"]) + 3)
        with self.assertRaises(ValueError):
            matrix.prepare_cell(self.manifest, path.parent, matrix.plan()[0], self.operators)

    def collected(self):
        root = self.root / "run"
        cell = root / "collected/cell"
        cell.mkdir(parents=True)
        config = dict(CONFIG, trial=0, source={"commit": "a" * 40}, extension_sha256=DIGEST)
        records = []
        for index, spec in enumerate([None, *matrix.probes(CONFIG["rows"])]):
            raw = cell / f"process-{index}.ndjson"
            raw.write_text("\n".join(json.dumps(row) for row in events(spec)))
            records.append(dict(ordinal=index, spec=spec, status="passed", raw_sha256=digest(raw)))
        (cell / "processes.json").write_text(json.dumps(records))
        report = dict(config, schema="alopex.native-begin-measurement/v1", status="passed",
                      assets_digest="asset", worker_error=None, finalization_error=None,
                      processes_sha256=digest(cell / "processes.json"))
        (root / "collected/result.json").write_text(json.dumps(report))
        return SimpleNamespace(root=root, record={"phase": "deleted"}, verify_collection=lambda: None), config, report

    def test_collected_complete_processes_pass_but_changed_raw_fails(self):
        run, config, _ = self.collected()
        self.assertEqual(matrix.verify_collected(run, config, "asset")["processes"], 8)
        (run.root / "collected/cell/process-1.ndjson").write_text("changed")
        with self.assertRaises(ValueError): matrix.verify_collected(run, config, "asset")

    def test_wrong_source_and_extension_fail(self):
        run, config, _ = self.collected()
        for change in ({"source": {"commit": "b" * 40}}, {"extension_sha256": "c" * 64}):
            with self.assertRaises(ValueError): matrix.verify_collected(run, dict(config, **change), "asset")

    def test_only_stopped_collected_deleted_failure_can_continue(self):
        run, config, report = self.collected()
        self.assertFalse(matrix.recovered(run, config, "asset"))
        report.update(status="failed", worker_error="worker failed")
        target = run.root / "collected/result.json"
        target.write_text(json.dumps(report))
        self.assertTrue(matrix.recovered(run, config, "asset"))
        run.record["phase"] = "collected"
        self.assertFalse(matrix.recovered(run, config, "asset"))
        run.record["phase"] = "deleted"
        report["finalization_error"] = "failed to inspect results"
        target.write_text(json.dumps(report))
        self.assertFalse(matrix.recovered(run, config, "asset"))

    def test_execution_uses_qualified_lifecycle_with_begin_verifiers(self):
        with patch.object(container, "execute_cell", return_value={"status": "passed"}) as execute:
            self.assertEqual(matrix.execute_cell({}, None, {}), {"status": "passed"})
        self.assertEqual(execute.call_args.kwargs,
                         dict(verify=matrix.verify_collected, recovered=matrix.recovered))


if __name__ == "__main__":
    unittest.main()
