"""Exercise phase routing and failure propagation without building native code."""
import ast
from contextlib import redirect_stdout, redirect_stderr
import io
import os
from pathlib import Path
import re
import runpy
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("windows-wal-contract.py")
WORKFLOW = SCRIPT.parents[2] / ".github/workflows/ci.yml"


class WindowsPhaseTests(unittest.TestCase):
    def execute(self, phase, *, exit_code=0, output=None, hosted=True):
        calls = []
        # Fake native results exercise the runner's selection and validation;
        # these are deliberately not evidence of Windows native execution.
        if output is None:
            output = "\n".join(
                f"test result: ok. {n} passed; 0 failed; 0 ignored"
                for n in range(1, 40)
            )
        with tempfile.TemporaryDirectory(prefix="alopex-phase-") as directory:
            root = Path(directory).resolve()
            script = root / "scripts/ci" / SCRIPT.name
            script.parent.mkdir(parents=True)
            shutil.copyfile(SCRIPT, script)

            def run(command, *, cwd, stdout, stderr):
                self.assertEqual(Path(cwd), root)
                self.assertEqual(os.environ["CARGO_TARGET_DIR"], str(root / "target"))
                self.assertEqual(command[1], str(root / "scripts/compatibility/storage.py"))
                self.assertIn("20000000000", command)
                self.assertEqual(stderr, subprocess.STDOUT)
                calls.append((Path(stdout.name).stem, command[command.index("--") + 1:]))
                stdout.write(output)
                return SimpleNamespace(returncode=exit_code)

            error = None
            with patch.object(sys, "argv", [str(script), "--phase", phase]), \
                 patch.object(sys, "platform", "win32"), \
                 patch.dict(os.environ, {"RUNNER_ENVIRONMENT": "github-hosted" if hosted else "self-hosted"}), \
                 patch.object(subprocess, "check_output", return_value="source-identity\n"), \
                 patch.object(subprocess, "run", side_effect=run), \
                 io.TextIOWrapper(io.BytesIO(), encoding="utf-8") as console, \
                 redirect_stdout(console), redirect_stderr(io.StringIO()):
                try:
                    runpy.run_path(str(script), run_name="__main__")
                except SystemExit as exc:
                    error = exc.code
            if calls:
                self.assertTrue((root / "artifacts/windows-wal/source.json").is_file())
                for name, _ in calls:
                    self.assertEqual((root / f"artifacts/windows-wal/{name}.log").read_text(encoding="utf-8"), output)
        return calls, error

    def test_phases_cover_every_command_once_with_shared_storage_guard(self):
        assignment = next(node for node in ast.parse(SCRIPT.read_text(encoding="utf-8")).body
                          if isinstance(node, ast.Assign)
                          and any(isinstance(t, ast.Name) and t.id == "commands" for t in node.targets))
        original = ast.literal_eval(assignment.value)
        routed = []
        for phase in ("build", "wal-contracts", "hnsw-contracts"):
            calls, error = self.execute(phase)
            self.assertIsNone(error)
            self.assertTrue(calls)
            if phase == "build":
                self.assertEqual([name for name, _ in calls], ["release-build"])
            elif phase == "hnsw-contracts":
                self.assertTrue(all(name.startswith("hnsw-") for name, _ in calls))
            else:
                self.assertTrue(all(not name.startswith("hnsw-") and name != "release-build"
                                    for name, _ in calls))
            routed.extend(calls)
        self.assertEqual(sorted(routed), sorted(original))
        self.assertEqual(len({name for name, _ in routed}), len(original))

    def test_native_failure_stops_each_contract_phase(self):
        for phase in ("wal-contracts", "hnsw-contracts"):
            with self.subTest(phase=phase):
                calls, error = self.execute(phase, exit_code=17)
                self.assertEqual(error, 17)
                self.assertEqual(len(calls), 1)

    def test_missing_test_results_fail_each_contract_phase(self):
        for phase in ("wal-contracts", "hnsw-contracts"):
            with self.subTest(phase=phase):
                calls, error = self.execute(phase, output="test result: ok. 0 passed; 0 failed; 0 ignored")
                self.assertIn("expected native tests did not run", error)
                self.assertEqual(len(calls), 1)

    def test_unknown_phase_and_uninventoried_host_launch_no_commands(self):
        self.assertEqual(self.execute("unknown"), ([], 2))
        calls, error = self.execute("wal-contracts", hosted=False)
        self.assertEqual(calls, [])
        self.assertIn("isolated GitHub-hosted Windows runner", error)

    def test_workflow_runs_both_contract_phases_serially_before_cleanup(self):
        build = WORKFLOW.read_text(encoding="utf-8").split("\n  build:", 1)[1].split("\n  v08-release-gate:", 1)[0]
        phases = re.findall(r"windows-wal-contract.py --phase ([a-z-]+)", build)
        self.assertEqual(phases, ["build", "wal-contracts", "hnsw-contracts"])
        for phase in phases:
            block = build[:build.index(f"windows-wal-contract.py --phase {phase}")].rsplit("- name:", 1)[1]
            self.assertIn("timeout-minutes: 45", block)
            self.assertIn("if: runner.os == 'Windows'", block)
            self.assertNotIn("continue-on-error", block)
        self.assertLess(build.index("--phase hnsw-contracts"), build.index("Clean Windows validation outputs"))
        self.assertIn("if: always() && runner.os == 'Windows'", build.split("Clean Windows validation outputs", 1)[1])


if __name__ == "__main__":
    unittest.main()
