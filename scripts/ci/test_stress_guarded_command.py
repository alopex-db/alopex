"""Offline checks for fixed-output Stage-2 growth supervision."""

import os
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import stress_guarded_command as guard


class GuardedCommandTests(unittest.TestCase):
    def test_command_uses_docker_aware_host_roots_and_bounded_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.dict(os.environ, {"RUNNER_TEMP": str(root)}):
                args, output = guard.guarded_command(
                    private=root, case_id="asan", phase="test",
                    command=["rustup", "toolchain", "install", "nightly"])
            self.assertEqual(args[:3], [sys.executable,
                                         str(guard.WORKSPACE / "scripts/release/release_storage.py"),
                                         "--docker"])
            self.assertIn(str(guard.WORKSPACE), args)
            self.assertIn(str(root), args)
            self.assertEqual(args[args.index("--additional-bytes") + 1],
                             "20000000000")
            self.assertEqual(args[-4:], ["rustup", "toolchain", "install", "nightly"])
            self.assertEqual(output.name, "asan-test.output.private.log")

    def test_parser_rejects_unreviewed_case_or_phase(self):
        self.assertEqual(guard.parse(["--case", "stress", "--phase", "toolchain",
                                      "--", "rustup"]),
                         ("stress", "toolchain", ["rustup"]))
        for argv in (["--case", "../../host", "--phase", "test", "--", "echo"],
                     ["--case", "stress", "--phase", "raw", "--", "echo"],
                     ["--case", "stress", "--phase", "test", "--"]):
            with self.subTest(argv=argv), self.assertRaises(ValueError):
                guard.parse(argv)

    def test_private_directory_requires_owner_only_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.dict(os.environ, {"RUNNER_TEMP": str(root)}):
                private = guard.private_directory()
                self.assertEqual(private.stat().st_mode & 0o777, 0o700)
                private.chmod(0o755)
                with self.assertRaises(ValueError):
                    guard.private_directory()

    def test_cli_failure_prints_fixed_code_only(self):
        result = subprocess.run([
            sys.executable, str(Path(guard.__file__)), "--case", "stress",
            "--phase", "test", "--", "secret-private-machine-path"],
            capture_output=True, text=True, env={"GITHUB_SHA": "invalid"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr.strip(), "STRESS_GUARDED_COMMAND_REJECTED")
        self.assertNotIn("secret-private-machine-path", result.stdout + result.stderr)

    def test_public_execution_receipt_contains_only_fixed_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(guard, "WORKSPACE", root):
                guard.write_public_execution(case_id="sql_parser", source="a" * 40,
                                             passed=False, duration_ms=600_000,
                                             fuzz_executions=1000)
                path = root / "target/stress-public-artifacts/sql_parser/execution.json"
                self.assertEqual(json.loads(path.read_bytes()), {
                    "schema": "alopex.stress-execution/v1", "case_id": "sql_parser",
                    "source_commit": "a" * 40, "outcome": "failed",
                    "duration_ms": 600_000, "fuzz_executions": 1000,
                })
                with self.assertRaises(FileExistsError):
                    guard.write_public_execution(case_id="sql_parser", source="a" * 40,
                                                 passed=True, duration_ms=1,
                                                 fuzz_executions=1)
                with self.assertRaises(ValueError):
                    guard.write_public_execution(case_id="../../local-path", source="a" * 40,
                                                 passed=True, duration_ms=1)

    def test_fuzz_count_extracts_only_bounded_number(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "private.log"
            output.write_bytes(b"private /local/path\n#1000 DONE cov: 10\n")
            self.assertEqual(guard.fuzz_execution_count(output), 1000)
            output.write_bytes(b"private /local/path\n")
            self.assertIsNone(guard.fuzz_execution_count(output))


if __name__ == "__main__":
    unittest.main()
