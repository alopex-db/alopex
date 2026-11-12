"""Fake-proc and public-boundary tests for the read-only #682 probe."""

import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, call, patch
from types import SimpleNamespace

SCRIPT = Path(__file__).resolve().parents[1] / "schedstat_capability_probe.py"
SPEC = importlib.util.spec_from_file_location("schedstat_capability_probe", SCRIPT)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
REPO = SCRIPT.parents[2]


class FakeProc(unittest.TestCase):
    def test_missing_zero_advancing_and_static(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            sibling = proc / "123"
            sibling.mkdir()
            self_path = proc / "self/schedstat"
            self_path.parent.mkdir()
            sibling_path = sibling / "schedstat"
            self.assertEqual(probe.read_schedstat(self_path)["status"], "missing")
            self_path.write_text("0 0 0\n")
            sibling_path.write_text("0 0 0\n")
            zeros = probe.sample(proc, 123, lambda: None)
            self.assertEqual({row["classification"] for row in zeros.values()}, {"zero"})

            def advance():
                self_path.write_text("10 0 1\n")
                sibling_path.write_text("20 0 2\n")

            moved = probe.sample(proc, 123, advance)
            self.assertEqual({row["classification"] for row in moved.values()},
                             {"advancing"})
            self.assertEqual(moved["self"]["deltas"],
                             {"runtime_ns": 10, "runqueue_wait_ns": 0, "timeslices": 1})
            self.assertEqual(probe.capability_code({"status": "read", "value": "1"}, moved),
                             "SCHEDSTAT_WAIT_UNPROVEN")

            def wait_advances():
                self_path.write_text("11 5 2\n")
                sibling_path.write_text("21 7 3\n")

            waited = probe.sample(proc, 123, wait_advances)
            self.assertEqual(waited["self"]["deltas"]["runqueue_wait_ns"], 5)
            self.assertEqual(waited["sibling"]["deltas"]["runqueue_wait_ns"], 7)
            self.assertEqual(probe.capability_code({"status": "read", "value": "1"}, waited),
                             "SCHEDSTAT_ADVANCING")
            still = probe.sample(proc, 123, lambda: None)
            self.assertEqual({row["classification"] for row in still.values()}, {"static"})
            self_path.write_text("bad counter\n")
            self.assertEqual(probe.read_schedstat(self_path)["status"], "unreadable")

    def test_kernel_setting_and_fixed_code(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            setting = proc / "sys/kernel/sched_schedstats"
            self.assertEqual(probe.kernel_setting(proc)["status"], "missing")
            setting.parent.mkdir(parents=True)
            setting.write_text("0\n")
            self.assertEqual(probe.kernel_setting(proc)["value"], "0")
            samples = {"self": {"classification": "advancing",
                                "deltas": {"runtime_ns": 1, "runqueue_wait_ns": 2,
                                           "timeslices": 1}},
                       "sibling": {"classification": "advancing",
                                   "deltas": {"runtime_ns": 1, "runqueue_wait_ns": 3,
                                              "timeslices": 1}}}
            self.assertEqual(probe.capability_code(probe.kernel_setting(proc), samples),
                             "SCHEDSTAT_DISABLED")
            setting.write_text("1\n")
            self.assertEqual(probe.capability_code(probe.kernel_setting(proc), samples),
                             "SCHEDSTAT_ADVANCING")
            samples["sibling"]["deltas"]["runqueue_wait_ns"] = 0
            self.assertEqual(probe.capability_code(probe.kernel_setting(proc), samples),
                             "SCHEDSTAT_WAIT_UNPROVEN")
            samples["sibling"]["deltas"]["runqueue_wait_ns"] = 3
            samples["sibling"]["classification"] = "zero"
            self.assertEqual(probe.capability_code(probe.kernel_setting(proc), samples),
                             "SCHEDSTAT_ZERO")
            samples["sibling"]["classification"] = "missing"
            self.assertEqual(probe.capability_code(probe.kernel_setting(proc), samples),
                             "SCHEDSTAT_MISSING")

    def test_timeslice_only_movement_does_not_prove_wait_counter(self):
        before = {"status": "read", "values": [0, 0, 1]}
        after = {"status": "read", "values": [0, 0, 2]}
        row = {"classification": probe.classify(before, after),
               "deltas": probe.counter_deltas(before, after)}
        self.assertEqual(row["classification"], "advancing")
        self.assertEqual(row["deltas"]["runqueue_wait_ns"], 0)
        self.assertEqual(probe.capability_code({"status": "read", "value": "1"},
                                               {"self": row, "sibling": row}),
                         "SCHEDSTAT_WAIT_UNPROVEN")

    def test_affinity_is_pinned_before_child_and_restored_after_sample(self):
        child = Mock(pid=123)
        samples = {"self": {"classification": "advancing",
                            "deltas": {"runqueue_wait_ns": 1}},
                   "sibling": {"classification": "advancing",
                               "deltas": {"runqueue_wait_ns": 2}}}
        events = []

        def set_affinity(pid, cpus):
            events.append(("affinity", pid, set(cpus)))

        def launch(*_args, **_kwargs):
            events.append(("child",))
            return child

        with patch.object(probe.os, "sched_getaffinity", return_value={3, 4}, create=True), \
             patch.object(probe.os, "sched_setaffinity", side_effect=set_affinity,
                          create=True), \
             patch.object(probe.subprocess, "Popen", side_effect=launch), \
             patch.object(probe, "kernel_setting", return_value={"status": "read", "value": "1"}), \
             patch.object(probe, "sample", return_value=samples):
            facts = probe.collect(Path("/unused"))
        self.assertEqual(facts["affinity"], "pinned")
        self.assertEqual(events, [("affinity", 0, {3}), ("child",),
                                  ("affinity", 0, {3, 4})])
        child.terminate.assert_called_once_with()
        child.wait.assert_called_once_with(timeout=0.5)

    def test_affinity_restored_and_child_reaped_when_sample_fails(self):
        child = Mock(pid=123)
        with patch.object(probe.os, "sched_getaffinity", return_value={2, 5}, create=True), \
             patch.object(probe.os, "sched_setaffinity", create=True) as setter, \
             patch.object(probe.subprocess, "Popen", return_value=child), \
             patch.object(probe, "kernel_setting", return_value={"status": "read", "value": "1"}), \
             patch.object(probe, "sample", side_effect=RuntimeError("fake sample failure")):
            with self.assertRaisesRegex(RuntimeError, "fake sample failure"):
                probe.collect(Path("/unused"))
        self.assertEqual(setter.call_args_list, [call(0, {2}), call(0, {2, 5})])
        child.terminate.assert_called_once_with()
        child.wait.assert_called_once_with(timeout=0.5)

    def test_unavailable_affinity_is_inconclusive_without_child(self):
        with patch.object(probe.os, "sched_getaffinity", side_effect=OSError(1, "denied"),
                          create=True), \
             patch.object(probe.os, "sched_setaffinity", create=True) as setter, \
             patch.object(probe.subprocess, "Popen") as launch, \
             patch.object(probe, "kernel_setting", return_value={"status": "read", "value": "1"}):
            facts = probe.collect(Path("/unused"))
        self.assertEqual(facts["affinity"], "unavailable")
        self.assertEqual(probe.capability_code(facts["setting"], facts["samples"],
                                               facts["affinity"]),
                         "SCHEDSTAT_AFFINITY_UNAVAILABLE")
        setter.assert_not_called()
        launch.assert_not_called()

        with patch.object(probe.os, "sched_getaffinity", return_value={2, 5}, create=True), \
             patch.object(probe.os, "sched_setaffinity", side_effect=OSError(1, "denied"),
                          create=True) as setter, \
             patch.object(probe.subprocess, "Popen") as launch, \
             patch.object(probe, "kernel_setting", return_value={"status": "read", "value": "1"}):
            facts = probe.collect(Path("/unused"))
        self.assertEqual(facts["affinity"], "unavailable")
        self.assertEqual(probe.capability_code(facts["setting"], facts["samples"],
                                               facts["affinity"]),
                         "SCHEDSTAT_AFFINITY_UNAVAILABLE")
        setter.assert_called_once_with(0, {2})
        launch.assert_not_called()

    def test_workflow_is_682_only_with_existing_inventory_upload(self):
        workflow = (REPO / ".github/workflows/alopex-performance.yml").read_text()
        inventory = workflow.split("  inventory:\n", 1)[1].split("  qualification:\n", 1)[0]
        self.assertIn("!inputs.qualification_mode && !inputs.sql_four_case_mode && !inputs.wal_cost_mode", inventory)
        self.assertIn("if: inputs.issue_number == '682'", inventory)
        self.assertIn("run: python3 -B scripts/ops/schedstat_capability_probe.py", inventory)
        self.assertLess(inventory.index("Collect encrypted read-only host inventory"),
                        inventory.index("Probe schedstat capability"))
        self.assertLess(inventory.index("Probe schedstat capability"),
                        inventory.index("Preserve encrypted inventory"))
        self.assertIn("path: native-inventory-export/", inventory)

    def test_public_result_is_allowlisted_and_private_facts_are_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            facts = {"setting": {"status": "read", "value": "1"},
                     "affinity": "pinned",
                     "samples": {"self": {"classification": "advancing",
                                          "deltas": {"runqueue_wait_ns": 0}},
                                 "sibling": {"classification": "advancing",
                                             "deltas": {"runqueue_wait_ns": 0}}},
                     "private_identity": "secret-host-id"}

            def encrypt(args, **_kwargs):
                Path(args[args.index("-out") + 1]).write_bytes(b"ciphertext")
                return SimpleNamespace(returncode=0)

            previous = Path.cwd()
            try:
                os.chdir(root)
                with patch.dict(os.environ, {"RUNNER_TEMP": str(root), "ISSUE_NUMBER": "682"}), \
                     patch.object(probe, "collect", return_value=facts), \
                     patch.object(probe.subprocess, "run", side_effect=encrypt), \
                     contextlib.redirect_stdout(io.StringIO()) as output:
                    self.assertEqual(probe.main(), 0)
                public = (root / "native-inventory-export/schedstat-public.json").read_text()
                self.assertNotIn("secret-host-id", public + output.getvalue())
                self.assertEqual(json.loads(public)["code"], "SCHEDSTAT_WAIT_UNPROVEN")
                self.assertFalse((root / "alopex-682-schedstat.private.json").exists())
                self.assertTrue((root / "native-inventory-export/schedstat-private-evidence.cms").is_file())
            finally:
                os.chdir(previous)

    def test_wrong_issue_emits_only_fixed_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            previous = Path.cwd()
            try:
                os.chdir(directory)
                with patch.dict(os.environ, {"ISSUE_NUMBER": "804"}, clear=True), \
                     contextlib.redirect_stdout(io.StringIO()) as output:
                    self.assertEqual(probe.main(), 1)
                self.assertEqual(json.loads(output.getvalue())["code"], "PROBE_FAILED")
                self.assertNotIn("804", output.getvalue())
            finally:
                os.chdir(previous)


if __name__ == "__main__":
    unittest.main()
