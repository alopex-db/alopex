"""Stage ordering and stop conditions; no selected-host workload."""

from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public_pair as pair
import issue681_public_run as runner
from test_issue681_public_pair import PINS, fixture


class PublicRunPlanTests(unittest.TestCase):
    def test_three_stages_export_only_after_all_thirty_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            work, export = root / "work", root / "export"
            work.mkdir(mode=0o700)
            export.mkdir(mode=0o700)
            expected = fixture()
            sequence = iter(expected)
            stages = []
            cases = []
            def execute(**kwargs):
                cases.append((kwargs["rows"], kwargs["trial"],
                              kwargs["source_commit"]))
                return next(sequence)
            assets = {arm: root / arm for arm in pair.ARMS}
            def verify_asset(path, pin, *, private_roots):
                return {"binary": root / "binary"}
            with mock.patch.object(runner.assets, "verify", side_effect=verify_asset), \
                    mock.patch.object(runner.audit, "audit_case",
                                      side_effect=expected), \
                    mock.patch.object(runner.stage_audit, "audit_stage",
                                      return_value=True):
                result = runner._run_mode_fixed(
                    pins=PINS, asset_locations=assets, work=work, export=export,
                    finish="rollback", private_roots=(str(root),),
                    admission_dir=root / "admission",
                    additional_bytes=1_000_000_000,
                    admit_stage=lambda rows: stages.append(rows) or True,
                    admit_case=lambda rows, trial, arm: True,
                    complete_case=lambda rows, trial, arm: True,
                    case_runner=execute)
            self.assertEqual(stages, [1_000, 10_000, 100_000])
            self.assertEqual(len(cases), 30)
            self.assertEqual([row[0] for row in cases],
                             [rows for rows, _, _ in pair._order()])
            self.assertEqual(len(result["cases"]), 30)
            self.assertEqual(set(export.iterdir()), {export / "public.json"})

    def test_failed_stage_does_not_start_later_cases_or_export(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            work, export = root / "work", root / "export"
            work.mkdir(mode=0o700)
            export.mkdir(mode=0o700)
            expected = fixture()
            sequence = iter(expected)
            calls = []
            def execute(**kwargs):
                calls.append(kwargs["rows"])
                return next(sequence)
            assets = {arm: root / arm for arm in pair.ARMS}
            with mock.patch.object(runner.assets, "verify",
                                   return_value={"binary": root / "binary"}), \
                    mock.patch.object(runner.audit, "audit_case",
                                      side_effect=expected):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_STAGE$"):
                    runner._run_mode_fixed(
                        pins=PINS, asset_locations=assets,
                        work=work, export=export, finish="rollback",
                        private_roots=(str(root),),
                        admission_dir=root / "admission",
                        additional_bytes=1_000_000_000,
                        admit_stage=lambda rows: rows == 1_000,
                        admit_case=lambda rows, trial, arm: True,
                        complete_case=lambda rows, trial, arm: True,
                        case_runner=execute)
            self.assertEqual(calls, [1_000] * 10)
            self.assertEqual(list(export.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
