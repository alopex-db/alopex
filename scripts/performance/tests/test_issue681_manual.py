"""Tiny process/protocol tests; no database, performance fixture, or load test.

Storage and kernel counters are injected here to exercise runner decisions.
The Linux counter itself has a separate manual runtime probe.
"""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_manual as runner


class Counter:
    instances = []
    fail = None
    peak = 1024

    def __init__(self, pid):
        self.pid = pid
        self.active = None
        self.pending_observation = None
        self.closed = False
        self.instances.append(self)

    def begin(self, phase):
        self.active = phase
        if self.fail == "begin":
            raise RuntimeError("injected reset failure")

    def finish(self, phase):
        if self.fail == "finish":
            self.pending_observation = {"phase": phase, "after_reset": {"high_water_bytes": 2048},
                                        "after": {"high_water_bytes": 1024}}
            raise RuntimeError("injected counter failure")
        if self.active != phase:
            raise AssertionError("unexpected phase")
        self.active = None
        return {"phase": phase, "kernel_reported_peak_rss_bytes": self.peak}

    def close(self):
        self.closed = True


@unittest.skipUnless(os.name == "posix", "runner uses POSIX process groups")
class RunnerTests(unittest.TestCase):
    def run_fixture(self, events=None, *, kernel=True, fail=None, peak=1024,
                    tail="", timeout=5, status=0, raw=None, storage_failure=False,
                    finish=None):
        if events is None:
            events = [{"phase": phase, "data": {"event": kind} if kind else {"stats": {}}}
                      for phase, kind in runner.EXPECTED_EVENTS]
            if finish is not None:
                events[-3]["phase"] = events[-2]["phase"] = finish
                events[-2]["data"]["elapsed_ns"] = 100
                events[-1]["data"].update(finish=finish, result_validated=True)
        Counter.instances = []
        Counter.fail = fail
        Counter.peak = peak
        children = []
        original_popen = subprocess.Popen

        def capture(*args, **kwargs):
            child = original_popen(*args, **kwargs)
            if kwargs.get("start_new_session"):
                children.append(child)
            return child

        with tempfile.TemporaryDirectory(prefix="alopex-runner-contract-") as directory:
            root = Path(directory)
            binary = root / "fixture"
            # Every event waits for the actual runner acknowledgement, as the
            # product example does. Fixture data stays below a few KiB.
            lines = raw if raw is not None else [json.dumps(event) for event in events]
            binary.write_text(f"#!{sys.executable}\nimport sys,time\n"
                              f"lines={lines!r}\n"
                              "for line in lines:\n"
                              " print(line,flush=True)\n"
                              " if sys.stdin.readline().strip() != 'continue': sys.exit(9)\n"
                              f"{tail}\nsys.exit({status})\n")
            binary.chmod(0o700)
            output = root / "result"
            argv = ["issue681_manual.py", str(binary), "--output", str(output),
                    "--rows", "1", "--dimensions", "1", "--timeout", str(timeout)]
            if kernel:
                argv.append("--phase-kernel-rss")
            if finish is not None:
                argv.extend(["--finish", finish])
            inventory = {"allocated_bytes": 0}
            with mock.patch.dict(os.environ, {}, clear=False), \
                    mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(runner, "sys", mock.Mock(platform="linux")), \
                    mock.patch.object(runner, "PhaseRss", Counter), \
                    mock.patch.object(runner.storage, "inventory", return_value=inventory) as storage, \
                    mock.patch.object(runner.subprocess, "Popen", side_effect=capture), \
                    contextlib.redirect_stdout(io.StringIO()):
                os.environ.pop("CI", None)
                if storage_failure:
                    storage.side_effect = RuntimeError("injected inventory failure")
                    with self.assertRaisesRegex(RuntimeError, "inventory failure"):
                        runner.main()
                    self.assertEqual(children, [])
                    return None
                code = runner.main()
            report = json.loads((output / "summary.json").read_text())
            logged = (output / "events.jsonl").read_text().splitlines()
            identity = json.loads((output / "identity.json").read_text())
            self.assertEqual(len(children), 1)
            child = children[0]
            self.assertEqual(child.args[1:], ["1", "1"] + ([finish] if finish else []))
            self.assertIsNotNone(child.returncode)
            self.assertTrue(child.stdin.closed)
            self.assertTrue(child.stdout.closed)
            with self.assertRaises(ChildProcessError):
                os.waitpid(child.pid, os.WNOHANG)
            self.assertTrue(all(counter.closed for counter in Counter.instances))
            return code, report, logged, identity

    def test_all_nine_kernel_phases_and_terminal_handshake(self):
        code, report, logged, identity = self.run_fixture()
        self.assertEqual(code, 0)
        self.assertEqual(report["outcome"], "passed")
        self.assertEqual([item["phase"] for item in report["phase_kernel_rss"]], list(runner.PHASES))
        self.assertEqual(len(logged), 19)
        self.assertIsNone(report["children_lifetime_peak_rss_bytes"])
        self.assertTrue(identity["phase_kernel_rss"])

    def test_sampled_mode_retains_same_complete_protocol(self):
        code, report, _, identity = self.run_fixture(kernel=False)
        self.assertEqual(code, 0)
        self.assertEqual(report["phase_kernel_rss"], [])
        self.assertFalse(identity["phase_kernel_rss"])

    def test_explicit_completion_records_time_and_requires_verified_result(self):
        for finish in ("commit", "rollback"):
            with self.subTest(finish=finish):
                code, report, logged, identity = self.run_fixture(finish=finish)
                self.assertEqual(code, 0)
                self.assertEqual(identity["finish"], finish)
                self.assertEqual(report["phase_kernel_rss"][-1]["phase"], finish)
                self.assertGreater(json.loads(logged[-2])["data"]["elapsed_ns"], 0)
                self.assertIs(json.loads(logged[-1])["data"]["result_validated"], True)

    def test_explicit_completion_rejects_old_or_invalid_result_events(self):
        for finish in ("commit", "rollback"):
            for fault in ("old_phase", "missing_time", "zero_time", "boolean_time",
                          "missing_verification", "false_verification", "wrong_finish"):
                events = [{"phase": phase, "data": {"event": kind} if kind else {}}
                          for phase, kind in runner.EXPECTED_EVENTS]
                events[-3]["phase"] = events[-2]["phase"] = finish
                events[-2]["data"]["elapsed_ns"] = 100
                events[-1]["data"].update(finish=finish, result_validated=True)
                if fault == "old_phase": events[-3]["phase"] = "warm"
                if fault == "missing_time": events[-2]["data"].pop("elapsed_ns")
                if fault == "zero_time": events[-2]["data"]["elapsed_ns"] = 0
                if fault == "boolean_time": events[-2]["data"]["elapsed_ns"] = True
                if fault == "missing_verification": events[-1]["data"].pop("result_validated")
                if fault == "false_verification": events[-1]["data"]["result_validated"] = False
                if fault == "wrong_finish": events[-1]["data"]["finish"] = "other"
                with self.subTest(finish=finish, fault=fault):
                    code, report, _, _ = self.run_fixture(events, finish=finish)
                    self.assertEqual(code, 1)
                    self.assertEqual(report["outcome"], "failed")

    def test_missing_complete_is_not_success_in_either_mode(self):
        events = [{"phase": phase, "data": {"event": kind}}
                  for phase, kind in runner.EXPECTED_EVENTS[:-1]]
        for kernel in (False, True):
            with self.subTest(kernel=kernel):
                code, report, logged, _ = self.run_fixture(events, kernel=kernel)
                self.assertEqual(code, 1)
                self.assertIn("incomplete", report["failure"])
                self.assertEqual(len(logged), 18)

    def test_out_of_order_duplicate_and_malformed_events_are_retained(self):
        cases = [
            ['{"phase":"index_build","data":{"event":"start"}}'],
            ['{"phase":"seed","data":{"event":"start"}}'] * 2,
            ['not json'], ['[]'], ['null'], ['{"phase":"seed","data":[]}'],
        ]
        for lines in cases:
            with self.subTest(lines=lines):
                code, report, logged, _ = self.run_fixture(raw=lines)
                self.assertEqual(code, 1)
                self.assertEqual(report["outcome"], "failed")
                self.assertEqual(logged, lines)

    def test_duplicate_terminal_event_fails(self):
        events = [{"phase": phase, "data": {"event": kind} if kind else {}}
                  for phase, kind in runner.EXPECTED_EVENTS]
        events.append(events[-1])
        code, report, logged, _ = self.run_fixture(events)
        self.assertEqual(code, 1)
        self.assertIn("phase sequence", report["failure"])
        self.assertEqual(len(logged), 20)

    def test_observer_failure_keeps_trigger_and_reaps_waiting_child(self):
        for fail, count in (("begin", 1), ("finish", 2)):
            with self.subTest(fail=fail):
                code, report, logged, _ = self.run_fixture(fail=fail)
                self.assertEqual(code, 1)
                self.assertIn("injected", report["failure"])
                self.assertEqual(len(logged), count)
                if fail == "finish":
                    self.assertEqual(report["incomplete_kernel_phase"], {
                        "phase": "seed", "after_reset": {"high_water_bytes": 2048},
                        "after": {"high_water_bytes": 1024}})

    def test_kernel_limit_fails_even_when_sampled_rss_is_small(self):
        code, report, logged, _ = self.run_fixture(peak=8_000_000_000)
        self.assertEqual(code, 1)
        self.assertIn("kernel RSS limit", report["failure"])
        self.assertEqual(len(logged), 2)

    def test_no_events_then_exit_is_failure(self):
        code, report, logged, _ = self.run_fixture(events=[])
        self.assertEqual(code, 1)
        self.assertIn("incomplete", report["failure"])
        self.assertEqual(logged, [])

    def test_complete_does_not_override_nonzero_exit(self):
        code, report, _, _ = self.run_fixture(status=7)
        self.assertEqual(code, 1)
        self.assertIn("exited 7", report["failure"])

    def test_timeout_reaps_child_waiting_after_complete(self):
        code, report, logged, _ = self.run_fixture(tail="time.sleep(10)", timeout=1)
        self.assertEqual(code, 1)
        self.assertIn("timeout", report["failure"])
        self.assertEqual(len(logged), 19)

    def test_inventory_failure_prevents_process_start(self):
        self.run_fixture(storage_failure=True)


if __name__ == "__main__":
    unittest.main()
