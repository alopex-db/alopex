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

    def test_case_failure_records_allowlisted_case_error_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            work, export = root / "work", root / "export"
            work.mkdir(mode=0o700)
            export.mkdir(mode=0o700)
            diag = {}
            assets = {arm: root / arm for arm in pair.ARMS}
            with mock.patch.object(runner.assets, "verify",
                                   return_value={"binary": root / "binary"}):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_STAGE$"):
                    runner._run_mode_fixed(
                        pins=PINS, asset_locations=assets,
                        work=work, export=export, finish="commit",
                        private_roots=(str(root),),
                        admission_dir=root / "admission",
                        additional_bytes=1_000_000_000,
                        admit_stage=lambda rows: True,
                        admit_case=lambda rows, trial, arm: True,
                        complete_case=lambda rows, trial, arm: True,
                        case_runner=lambda **_: {
                            "status": "failed",
                            "error_code": "PUBLIC_TIMEOUT",
                            "completed_phases": 2,
                        },
                        diag=diag)
            self.assertEqual(diag["code"], "MEASURE_CASE_PUBLIC_TIMEOUT")

    def test_budget_projection_early_stop_and_on_case_callbacks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            work, export = root / "work", root / "export"
            work.mkdir(mode=0o700)
            export.mkdir(mode=0o700)
            expected = fixture("commit")
            sequence = iter(expected)
            observed_cases = []
            diag = {}
            assets = {arm: root / arm for arm in pair.ARMS}

            def budget(position, rows, trial, arm, case_elapsed_s):
                if case_elapsed_s is not None and position == 1:
                    raise ValueError("MEASURE_TIME_BUDGET_PROJECTED")
                return True

            with mock.patch.object(runner.assets, "verify",
                                   return_value={"binary": root / "binary"}), \
                    mock.patch.object(runner.audit, "audit_case",
                                      side_effect=expected):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_STAGE$"):
                    runner._run_mode_fixed(
                        pins=PINS, asset_locations=assets,
                        work=work, export=export, finish="commit",
                        private_roots=(str(root),),
                        admission_dir=root / "admission",
                        additional_bytes=1_000_000_000,
                        admit_stage=lambda rows: True,
                        admit_case=lambda rows, trial, arm: True,
                        complete_case=lambda rows, trial, arm: True,
                        case_runner=lambda **_: next(sequence),
                        diag=diag,
                        on_case=lambda pos, rows, trial, arm, elapsed:
                            observed_cases.append((pos, rows, trial, arm)),
                        budget_check=budget)
            self.assertEqual(observed_cases, [(0, 1_000, 0, "before"),
                                              (1, 1_000, 0, "candidate")])
            self.assertEqual(diag["code"], "MEASURE_TIME_BUDGET_PROJECTED")
            self.assertEqual(list(export.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
