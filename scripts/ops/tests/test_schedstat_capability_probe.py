"""Fake-proc and public-boundary tests for the read-only #682 probe."""

import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
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
            samples = {"self": {"classification": "advancing"},
                       "sibling": {"classification": "advancing"}}
            self.assertEqual(probe.capability_code(probe.kernel_setting(proc), samples),
                             "SCHEDSTAT_DISABLED")
            setting.write_text("1\n")
            self.assertEqual(probe.capability_code(probe.kernel_setting(proc), samples),
                             "SCHEDSTAT_ADVANCING")
            samples["sibling"]["classification"] = "zero"
            self.assertEqual(probe.capability_code(probe.kernel_setting(proc), samples),
                             "SCHEDSTAT_ZERO")
            samples["sibling"]["classification"] = "missing"
            self.assertEqual(probe.capability_code(probe.kernel_setting(proc), samples),
                             "SCHEDSTAT_MISSING")

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
                     "samples": {"self": {"classification": "advancing"},
                                 "sibling": {"classification": "advancing"}},
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
                self.assertEqual(json.loads(public)["code"], "SCHEDSTAT_ADVANCING")
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
