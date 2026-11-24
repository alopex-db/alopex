"""One selected job keeps its pre-acquisition guard through public cases."""

import copy
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_build_guard as capacity
import issue681_hosted_measure_join as join
import issue681_public_pair as pair
from test_issue681_public_pair import PINS, fixture


def reports(finish, pins):
    values = fixture(finish)
    for value, (_, _, arm) in zip(values, pair._order()):
        value["source_commit"] = pins[arm]["source_commit"]
    return values


class Admission:
    def __init__(self, profile, *, roots, log_dir):
        self.profile = profile
        self.roots = roots
        self.log_dir = log_dir

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def admit_stage(self, *_):
        return True

    def admit_case(self, *_):
        return True

    def complete_case(self, *_):
        return True


class HostedMeasureJoinTests(unittest.TestCase):
    def setup_inputs(self, root):
        def directory(path):
            path.mkdir(mode=0o700, parents=True)
            path.chmod(0o700)
            return path

        workspace = directory(root / "workspace")
        task = directory(root / "task")
        for arm in pair.ARMS:
            directory(task / "sources" / arm)
        cargo = directory(root / "cargo")
        build = directory(root / "build")
        staged = directory(build / "bundles" / "pair")
        for arm in pair.ARMS:
            directory(staged / arm)
        measure = directory(root / "measure")
        guard = object.__new__(capacity.HostStorageGuard)
        acquired = dict.fromkeys(join.build_join.ACQUIRED_KEYS)
        acquired.update(guard=guard, task=task, cargo_home=cargo,
                        sources={arm: task / "sources" / arm
                                 for arm in pair.ARMS})
        pins = copy.deepcopy(PINS)
        for arm in pair.ARMS:
            source = join.build_join.acquire_source.PINS[arm]
            pins[arm].update(source_commit=source["commit"],
                             source_tree=source["tree"],
                             example_sha256=source["example"],
                             cargo_lock_sha256=source["lock"])
        built = {"pair": {"pins": pins, "staging": staged},
                 "build_task": build, "guard": guard, "cargo_home": cargo}
        profile = {"schema": join.admission.SCHEMA,
                   "runner_kind": "hosted", "cpu_count": 4,
                   "additional_bytes": 1_000_000_000}
        return acquired, built, workspace, measure, profile

    def test_one_guard_flows_through_all_thirty_cases_and_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            acquired, built, workspace, measure, profile = self.setup_inputs(Path(tmp).resolve())
            calls = []

            def run_mode(**values):
                self.assertIs(values["host_guard"], acquired["guard"])
                self.assertEqual(values["asset_locations"], {
                    arm: built["pair"]["staging"] / arm for arm in pair.ARMS})
                for rows in pair.SCALES:
                    self.assertTrue(values["admit_stage"](rows))
                for rows, trial, arm in pair._order():
                    self.assertTrue(values["admit_case"](rows, trial, arm))
                    self.assertTrue(values["complete_case"](rows, trial, arm))
                calls.append("thirty_cases")
                return pair.write(reports("commit", built["pair"]["pins"]),
                                  built["pair"]["pins"], values["export"],
                                  finish="commit", independent_audit=lambda _: True)

            with mock.patch.object(capacity.HostStorageGuard, "__call__",
                                   side_effect=lambda: calls.append("guard")), \
                 mock.patch.object(join.admission, "Admission", Admission), \
                 mock.patch.object(join.run, "_run_mode_fixed",
                                   side_effect=run_mode), \
                 mock.patch.object(join.stage_audit, "audit_stage",
                                   return_value=True) as audit:
                result = join.measure(acquired, built, workspace, measure,
                                      profile, "commit")
            self.assertEqual(result["finish"], "commit")
            self.assertIs(result["guard"], acquired["guard"])
            self.assertEqual(result["public"].name, "public.json")
            self.assertEqual(len(list(measure.iterdir())), 1)
            self.assertEqual(calls.count("thirty_cases"), 1)
            self.assertGreaterEqual(calls.count("guard"), 68)
            self.assertEqual(audit.call_count, 1)

    def test_mismatched_guard_fails_before_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            acquired, built, workspace, measure, profile = self.setup_inputs(Path(tmp).resolve())
            built["guard"] = object.__new__(capacity.HostStorageGuard)
            with mock.patch.object(join.run, "_run_mode_fixed") as run_mode:
                with self.assertRaisesRegex(ValueError, "^PUBLIC_STAGE$"):
                    join.measure(acquired, built, workspace, measure,
                                 profile, "rollback")
            run_mode.assert_not_called()
            self.assertEqual(list(measure.iterdir()), [])

    def test_changed_source_pin_fails_before_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            acquired, built, workspace, measure, profile = self.setup_inputs(Path(tmp).resolve())
            built["pair"]["pins"]["candidate"]["source_commit"] = "0" * 40
            with mock.patch.object(join.run, "_run_mode_fixed") as run_mode:
                with self.assertRaisesRegex(ValueError, "^PUBLIC_STAGE$"):
                    join.measure(acquired, built, workspace, measure,
                                 profile, "rollback")
            run_mode.assert_not_called()
            self.assertEqual(list(measure.iterdir()), [])

    def test_identity_helper_failure_cleans_owned_measurement_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            acquired, built, workspace, measure, profile = self.setup_inputs(Path(tmp).resolve())
            with mock.patch.object(capacity.HostStorageGuard, "__call__"), \
                 mock.patch.object(join.acquire_all, "_identity",
                                   side_effect=OSError("/private/host")), \
                 mock.patch.object(join.run, "_run_mode_fixed") as run_mode:
                with self.assertRaisesRegex(ValueError, "^PUBLIC_STAGE$"):
                    join.measure(acquired, built, workspace, measure,
                                 profile, "rollback")
            run_mode.assert_not_called()
            self.assertEqual(list(measure.iterdir()), [])

    def test_failed_second_audit_removes_public_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            acquired, built, workspace, measure, profile = self.setup_inputs(Path(tmp).resolve())

            def run_mode(**values):
                return pair.write(reports("rollback", built["pair"]["pins"]),
                                  built["pair"]["pins"], values["export"],
                                  finish="rollback", independent_audit=lambda _: True)

            with mock.patch.object(capacity.HostStorageGuard, "__call__"), \
                 mock.patch.object(join.admission, "Admission", Admission), \
                 mock.patch.object(join.run, "_run_mode_fixed",
                                   side_effect=run_mode), \
                 mock.patch.object(join.stage_audit, "audit_stage",
                                   return_value=False):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_STAGE$"):
                    join.measure(acquired, built, workspace, measure,
                                 profile, "rollback")
            self.assertEqual(list(measure.iterdir()), [])
