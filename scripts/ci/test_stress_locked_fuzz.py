"""Synthetic lock and command checks; never compile or run fuzz targets."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import stress_locked_fuzz as fuzz


class LockedFuzzTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.workspace = self.root / fuzz.CASES["sql_parser"]
        self.workspace.mkdir(parents=True)
        self.lock = self.workspace / "Cargo.lock"
        self.lock.write_bytes(b"exact lock\n")

    def test_preflight_requires_tracked_exact_head_lock(self):
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        subprocess.run(["git", "-C", str(self.root), "add", "--", str(self.lock)], check=True)
        subprocess.run(["git", "-C", str(self.root), "-c", "user.name=Fixture",
                        "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture"],
                       check=True)
        relative = fuzz.CASES["sql_parser"] / "Cargo.lock"
        self.assertEqual(fuzz.tracked_lock(self.root, relative), b"exact lock\n")
        self.lock.write_bytes(b"drift\n")
        with self.assertRaises(ValueError):
            fuzz.tracked_lock(self.root, relative)

    def test_offline_locked_metadata_and_fixed_fuzz_command(self):
        calls = []
        stages = []

        def fake_fetch(workspace):
            self.assertEqual(workspace, self.workspace)
            stages.append("fetch")

        def fake_metadata(workspace, env):
            self.assertEqual(workspace, self.workspace)
            self.assertEqual(env["CARGO_NET_OFFLINE"], "true")
            stages.append("metadata")

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            stages.append("fuzz")
            self.assertEqual(kwargs["env"]["CARGO_NET_OFFLINE"], "true")
            self.assertEqual(kwargs["cwd"], self.workspace)

        with patch.dict(os.environ, {"GITHUB_WORKSPACE": str(self.root)}), \
             patch.object(fuzz, "tracked_lock", return_value=b"exact lock\n") as tracked, \
             patch.object(fuzz, "locked_fetch", side_effect=fake_fetch), \
             patch.object(fuzz, "offline_metadata", side_effect=fake_metadata), \
             patch.object(fuzz.subprocess, "run", side_effect=fake_run):
            fuzz.run("sql_parser", 600, root=self.root)
        self.assertEqual(stages, ["fetch", "metadata", "fuzz"])
        self.assertEqual(tracked.call_count, 3)
        self.assertEqual(calls[0][0], [
            "cargo", "fuzz", "run", "sql_parser", "--", "-max_total_time=600",
            f"-artifact_prefix={self.root}/target/stress-artifacts/fuzz/sql_parser/",
        ])

    def test_postflight_detects_lock_drift_even_when_fuzz_fails(self):
        def mutate_then_fail(_command, **_kwargs):
            self.lock.write_bytes(b"changed by cargo\n")
            raise subprocess.CalledProcessError(9, ["cargo", "fuzz"])

        with patch.dict(os.environ, {"GITHUB_WORKSPACE": str(self.root)}), \
             patch.object(fuzz, "tracked_lock", return_value=b"exact lock\n"), \
             patch.object(fuzz, "locked_fetch"), \
             patch.object(fuzz, "offline_metadata"), \
             patch.object(fuzz.subprocess, "run", side_effect=mutate_then_fail):
            with self.assertRaisesRegex(ValueError, "LOCK"):
                fuzz.run("sql_parser", 600, root=self.root)

    def test_locked_fetch_is_targeted_and_rejects_wrong_runner(self):
        with patch.dict(os.environ, {"RUNNER_OS": "Linux", "RUNNER_ARCH": "X64",
                                  "RUNNER_ENVIRONMENT": "github-hosted"}), \
             patch.object(fuzz.sys, "platform", "linux"), \
             patch.object(fuzz.platform, "machine", return_value="x86_64"), \
             patch.object(fuzz.subprocess, "run") as command:
            fuzz.locked_fetch(self.workspace)
        self.assertEqual(command.call_args.args[0], [
            "cargo", "fetch", "--locked", "--target", "x86_64-unknown-linux-gnu",
            "--manifest-path", str(self.workspace / "Cargo.toml"),
        ])
        self.assertEqual(command.call_args.kwargs["cwd"], self.workspace)
        with patch.dict(os.environ, {"RUNNER_OS": "Linux", "RUNNER_ARCH": "ARM64",
                                  "RUNNER_ENVIRONMENT": "github-hosted"}), \
             patch.object(fuzz.subprocess, "run") as command:
            with self.assertRaises(ValueError):
                fuzz.locked_fetch(self.workspace)
            command.assert_not_called()

    def test_lock_drift_after_fetch_stops_before_metadata_or_fuzz(self):
        def mutate(_workspace):
            self.lock.write_bytes(b"drift from fetch\n")
            raise subprocess.CalledProcessError(9, ["cargo", "fetch"])

        with patch.dict(os.environ, {"GITHUB_WORKSPACE": str(self.root)}), \
             patch.object(fuzz, "tracked_lock", side_effect=lambda root, relative:
                          fuzz.read_lock(root / relative)), \
             patch.object(fuzz, "locked_fetch", side_effect=mutate), \
             patch.object(fuzz, "offline_metadata") as metadata, \
             patch.object(fuzz.subprocess, "run") as command:
            with self.assertRaises(ValueError):
                fuzz.run("sql_parser", 600, root=self.root)
            metadata.assert_not_called()
            command.assert_not_called()

    def test_rejects_unreviewed_target_duration_and_source(self):
        with self.assertRaises(ValueError):
            fuzz.run("other", 600, root=self.root)
        with self.assertRaises(ValueError):
            fuzz.run("sql_parser", 0, root=self.root)
        with self.assertRaises(ValueError):
            fuzz.run("sql_parser", fuzz.MAX_SECONDS + 1, root=self.root)
        with self.assertRaises(ValueError):
            fuzz.run("sql_parser", 600, root=self.root)

    def test_cli_error_is_fixed(self):
        for arguments in (("--case", "sql_parser", "--seconds", "600"),
                          ("--case", "/private/path", "--seconds", "600")):
            with self.subTest(arguments=arguments):
                result = subprocess.run([sys.executable, str(Path(fuzz.__file__)),
                                         *arguments],
                                        env={"GITHUB_WORKSPACE": "/private/path"},
                                        capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stderr.strip(), "STRESS_LOCKED_FUZZ_REJECTED")
                self.assertNotIn("/private/path", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
