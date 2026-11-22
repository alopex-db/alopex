"""Offline checks for fixed-output Stage-2 growth supervision."""

import os
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


if __name__ == "__main__":
    unittest.main()
