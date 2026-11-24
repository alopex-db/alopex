"""Focused contract checks for the #681 runner's storage monitoring modes."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

PERFORMANCE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PERFORMANCE))
import issue681_manual as runner


FAKE_WORKLOAD = """#!/usr/bin/env python3
import json
import sys

finish = sys.argv[3] if len(sys.argv) > 3 else 'rollback'
phases = ('seed', 'index_build', 'cold_transaction_1', 'cold_transaction_2',
          'warm', 'writer_begin', 'first_insert', 'second_insert', finish)
for phase in phases:
    for kind in ('start', 'end'):
        data = {'event': kind}
        if phase == finish and kind == 'end':
            data['elapsed_ns'] = 1
        print(json.dumps({'phase': phase, 'data': data}), flush=True)
        if sys.stdin.readline() != 'continue\\n':
            sys.exit(2)
print(json.dumps({'phase': 'complete', 'data': {
    'finish': finish, 'result_validated': True}}), flush=True)
if sys.stdin.readline() != 'continue\\n':
    sys.exit(2)
"""


class StorageMonitorModeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.binary = self.directory / "fake_workload"
        self.binary.write_text(FAKE_WORKLOAD)
        self.binary.chmod(0o755)

    def run_runner(self, *extra, gate=None, inventory=None):
        output = self.directory / f"output_{len(list(self.directory.glob('output_*')))}"
        argv = ["issue681_manual.py", str(self.binary), "--output", str(output),
                "--rows", "100000", "--timeout", "1800", *extra]
        environment = {runner.EXTERNAL_STORAGE_SUPERVISION_ENV: gate} if gate is not None else {}
        real_check_output = subprocess.check_output
        def check_output(command, **kwargs):
            if command[0] == "ps":
                return "1000\n"
            return real_check_output(command, **kwargs)
        with patch.object(sys, "argv", argv), patch.dict(os.environ, environment, clear=False), \
                patch.object(runner.storage, "inventory", side_effect=inventory) as scans, \
                patch.object(runner.subprocess, "check_output", side_effect=check_output), \
                patch.object(runner, "INVENTORY_INTERVAL_SECONDS", 0), \
                redirect_stdout(io.StringIO()):
            if gate is None:
                os.environ.pop(runner.EXTERNAL_STORAGE_SUPERVISION_ENV, None)
            result = runner.main()
        return result, output, scans.call_count

    def test_supervised_handshake_and_rss_loop_use_only_boundary_inventories(self):
        def inventory(_paths):
            inventory.calls += 1
            if inventory.calls > 2:
                raise AssertionError("periodic full inventory entered active loop")
            return {"allocated_bytes": 0}
        inventory.calls = 0
        result, output, scans = self.run_runner(
            "--external-storage-supervision", gate="1", inventory=inventory)
        self.assertEqual(result, 0)
        self.assertEqual(scans, 2)
        self.assertEqual(len((output / "events.jsonl").read_text().splitlines()), 19)
        self.assertGreater(len((output / "rss.jsonl").read_text().splitlines()), 0)
        self.assertFalse((output / "storage.jsonl").exists())
        identity = json.loads((output / "identity.json").read_text())
        summary = json.loads((output / "summary.json").read_text())
        self.assertEqual(identity["storage_monitor_mode"], "external_supervised_100k")
        self.assertEqual(summary["storage_monitor_mode"], "external_supervised_100k")
        self.assertEqual(summary["outcome"], "passed")

    def test_standalone_retains_periodic_inventory(self):
        result, output, scans = self.run_runner(inventory=lambda _paths: {"allocated_bytes": 0})
        self.assertEqual(result, 0)
        self.assertGreater(scans, 2)
        self.assertTrue((output / "storage.jsonl").read_text().splitlines())
        self.assertEqual(json.loads((output / "identity.json").read_text())["storage_monitor_mode"],
                         "standalone")
        self.assertEqual(json.loads((output / "summary.json").read_text())["storage_monitor_mode"],
                         "standalone")

    def test_external_mode_requires_both_gates_and_exactly_100k_rows(self):
        for argv, gate in ((["--external-storage-supervision"], None),
                           ([], "1"),
                           (["--external-storage-supervision", "--rows", "99999"], "1"),
                           (["--external-storage-supervision"], "yes")):
            with self.subTest(argv=argv, gate=gate):
                with self.assertRaises(SystemExit) as error:
                    self.run_runner(*argv, gate=gate, inventory=lambda _paths: {"allocated_bytes": 0})
                self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
