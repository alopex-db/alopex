"""Selected-job parser build control and private output boundaries."""

import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_parser_build as builder


class HostedParserBuildTests(unittest.TestCase):
    def test_process_capture_is_private_and_guarded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            calls = []
            log = root / "build.private.log"
            builder._run([sys.executable, "-c", "print('private-build')"],
                         root, {"PATH": "/usr/bin:/bin"}, log,
                         lambda: calls.append(1))
            self.assertEqual(log.read_bytes(), b"private-build\n")
            self.assertEqual(log.stat().st_mode & 0o777, 0o600)
            self.assertGreater(len(calls), 2)

    def test_log_overflow_and_active_guard_failure_reap_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with mock.patch.object(builder, "LOG_LIMIT", 16):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    builder._run([sys.executable, "-c", "print('x' * 17)"],
                                 root, {"PATH": "/usr/bin:/bin"},
                                 root / "overflow.private.log", lambda: None)
            self.assertLessEqual((root / "overflow.private.log").stat().st_size,
                                 16)
            processes = []
            original = builder.subprocess.Popen
            checks = []

            def start(*args, **kwargs):
                process = original(*args, **kwargs)
                processes.append(process)
                return process

            def guard():
                checks.append(1)
                if len(checks) == 2:
                    raise RuntimeError("private host capacity failure")

            with mock.patch.object(builder.subprocess, "Popen", side_effect=start):
                with self.assertRaisesRegex(RuntimeError,
                                            "private host capacity failure"):
                    builder._run([sys.executable, "-c",
                                  "import time; time.sleep(30)"], root,
                                 {"PATH": "/usr/bin:/bin"},
                                 root / "guard.private.log", guard)
            self.assertEqual(len(processes), 1)
            self.assertIsNotNone(processes[0].poll())

    def test_nonzero_exit_and_timeout_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                builder._run([sys.executable, "-c", "raise SystemExit(3)"],
                             root, {"PATH": "/usr/bin:/bin"},
                             root / "nonzero.private.log", lambda: None)
            with mock.patch.object(builder, "TIME_LIMIT", 0):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    builder._run([sys.executable, "-c",
                                  "import time; time.sleep(30)"], root,
                                 {"PATH": "/usr/bin:/bin"},
                                 root / "timeout.private.log", lambda: None)

    def fixture(self, root):
        paths = {}
        for name in ("source", "seed", "build", "stage", "nim-bin", "git-home"):
            path = root / name
            path.mkdir(mode=0o700)
            paths[name] = path
        for name in ("nim", "nimble"):
            tool = paths["nim-bin"] / name
            tool.write_bytes(b"tool")
            tool.chmod(0o700)
            paths[name] = tool
        paths["git_env"] = builder.acquire._git_env(paths["git-home"])
        paths["guard"] = object.__new__(builder.capacity.HostStorageGuard)
        paths["guard"].manifest = type("Manifest", (), {"scratch": builder.SCRATCH})()
        return paths

    def test_changed_source_tool_seed_and_scratch_stop_before_build(self):
        for failure in ("source", "tool", "seed", "scratch"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp).resolve()
                paths = self.fixture(root)
                scratch = root / "scratch"
                if failure == "scratch":
                    scratch.mkdir()
                tools_result = {"nim": paths["nim"],
                                "nimble": paths["nimble"]}
                if failure == "tool":
                    tools_result["nim"] = root / "wrong-nim"
                with mock.patch.object(builder.sys, "platform", "linux"), \
                     mock.patch.object(builder.os, "uname",
                                       return_value=mock.Mock(machine="x86_64")), \
                     mock.patch.object(builder, "SCRATCH", scratch), \
                     mock.patch.object(builder, "_trusted_executable"), \
                     mock.patch.object(builder.capacity.HostStorageGuard,
                                       "__call__", return_value=None), \
                     mock.patch.object(builder.tools, "verify_tools",
                                       return_value=tools_result), \
                     mock.patch.object(builder, "_verify_seed",
                                       side_effect=(ValueError("changed seed")
                                                    if failure == "seed" else None)), \
                     mock.patch.object(builder.producer, "verify_source",
                                       side_effect=(ValueError("changed source")
                                                    if failure == "source" else None)), \
                     mock.patch.object(builder, "_run") as run:
                    paths["guard"].manifest.scratch = scratch
                    with self.assertRaisesRegex(ValueError,
                                                "^PUBLIC_PARSER_INPUT$"):
                        builder.build_parser(
                            paths["source"], paths["nim"], paths["nimble"],
                            paths["seed"], paths["build"], paths["stage"],
                            paths["guard"], builder.acquire.GIT,
                            paths["git_env"])
                    run.assert_not_called()
                log = paths["build"] / "failure.private.log"
                self.assertLess(log.stat().st_size, 128)
                self.assertNotIn(b"changed source", log.read_bytes())
                self.assertNotIn(b"changed seed", log.read_bytes())
                self.assertFalse((paths["build"] / "lib").exists())

    def test_preexisting_build_parent_is_not_modified(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.fixture(Path(tmp).resolve())
            marker = paths["build"] / "failure.private.log"
            marker.write_bytes(b"preserve")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                builder.build_parser(
                    paths["source"], paths["nim"], paths["nimble"],
                    paths["seed"], paths["build"], paths["stage"],
                    paths["guard"], builder.acquire.GIT, paths["git_env"])
            self.assertEqual(marker.read_bytes(), b"preserve")

    def test_exact_build_handoff_and_bad_stage_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            paths = self.fixture(root)
            scratch = root / "scratch"
            calls = []
            digest = "a" * 64

            def run(argv, source, environment, log, guard):
                calls.append((argv, source, environment, log))
                self.assertEqual(argv[2:6], ["--backend", "host", "--target",
                                             builder.TARGET])
                self.assertEqual(environment["ALOPEX_NIM_BIN"],
                                 str(paths["nim"]))
                self.assertEqual(environment["ALOPEX_NIMBLE_SEED_DIR"],
                                 str(paths["seed"]))
                self.assertEqual(environment["PYTHON"], str(builder.PYTHON))
                self.assertNotIn("HTTP_PROXY", environment)

            def staged(archive_dir, stage_parent, guard):
                parser = stage_parent / builder.stage.ROOT_NAME
                parser.mkdir(mode=0o700)
                return {"parser_record": archive_dir / "record.json",
                        "parser_root": parser,
                        "parser_library": parser / "lib.so",
                        "parser_record_sha256": digest,
                        "parser_sha256": digest}

            with mock.patch.object(builder.sys, "platform", "linux"), \
                 mock.patch.object(builder.os, "uname",
                                   return_value=mock.Mock(machine="x86_64")), \
                 mock.patch.object(builder, "SCRATCH", scratch), \
                 mock.patch.object(builder, "_trusted_executable"), \
                 mock.patch.object(builder.tools, "verify_tools",
                                   return_value={"nim": paths["nim"],
                                                 "nimble": paths["nimble"]}), \
                 mock.patch.object(builder, "_verify_seed"), \
                 mock.patch.object(builder.capacity.HostStorageGuard,
                                   "__call__", return_value=None), \
                 mock.patch.object(builder.producer, "verify_source"), \
                 mock.patch.object(builder.producer, "verify_parser",
                                   return_value=digest), \
                 mock.patch.object(builder, "_run", side_effect=run), \
                 mock.patch.object(builder.stage, "stage_parser",
                                   side_effect=staged):
                paths["guard"].manifest.scratch = scratch
                result = builder.build_parser(
                    paths["source"], paths["nim"], paths["nimble"],
                    paths["seed"], paths["build"], paths["stage"],
                    paths["guard"], builder.acquire.GIT, paths["git_env"])
                self.assertEqual(result["parser_record_sha256"], digest)
                self.assertEqual(len(calls), 1)
            self.assertTrue((paths["stage"] / builder.stage.ROOT_NAME).is_dir())

            (paths["stage"] / builder.stage.ROOT_NAME).rmdir()
            (paths["build"] / "lib").rmdir()
            (paths["build"] / "archive").rmdir()
            calls.clear()
            with mock.patch.object(builder.sys, "platform", "linux"), \
                 mock.patch.object(builder.os, "uname",
                                   return_value=mock.Mock(machine="x86_64")), \
                 mock.patch.object(builder, "SCRATCH", scratch), \
                 mock.patch.object(builder, "_trusted_executable"), \
                 mock.patch.object(builder.tools, "verify_tools",
                                   return_value={"nim": paths["nim"],
                                                 "nimble": paths["nimble"]}), \
                 mock.patch.object(builder, "_verify_seed"), \
                 mock.patch.object(builder.capacity.HostStorageGuard,
                                   "__call__", return_value=None), \
                 mock.patch.object(builder.producer, "verify_source"), \
                 mock.patch.object(builder, "_run", side_effect=run), \
                 mock.patch.object(builder.stage, "stage_parser",
                                   side_effect=ValueError("bad record")):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    builder.build_parser(
                        paths["source"], paths["nim"], paths["nimble"],
                        paths["seed"], paths["build"], paths["stage"],
                        paths["guard"], builder.acquire.GIT, paths["git_env"])
            self.assertEqual(len(calls), 1)
            self.assertFalse((paths["build"] / "lib").exists())
            self.assertFalse((paths["build"] / "archive").exists())

    def test_late_consumer_failure_removes_staged_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            paths = self.fixture(root)

            def stage_result(archive_dir, stage_parent, guard):
                parser = stage_parent / builder.stage.ROOT_NAME
                parser.mkdir(mode=0o700)
                return {"parser_record": archive_dir / "record.json",
                        "parser_root": parser,
                        "parser_library": parser / "lib.so",
                        "parser_record_sha256": "a" * 64,
                        "parser_sha256": "b" * 64}

            with mock.patch.object(builder.sys, "platform", "linux"), \
                 mock.patch.object(builder.os, "uname",
                                   return_value=mock.Mock(machine="x86_64")), \
                 mock.patch.object(builder, "SCRATCH", root / "scratch"), \
                 mock.patch.object(builder, "_trusted_executable"), \
                 mock.patch.object(builder.capacity.HostStorageGuard,
                                   "__call__", return_value=None), \
                 mock.patch.object(builder.tools, "verify_tools",
                                   return_value={"nim": paths["nim"],
                                                 "nimble": paths["nimble"]}), \
                 mock.patch.object(builder, "_verify_seed"), \
                 mock.patch.object(builder.producer, "verify_source"), \
                 mock.patch.object(builder.producer, "verify_parser",
                                   side_effect=ValueError("bad consumer")), \
                 mock.patch.object(builder, "_run"), \
                 mock.patch.object(builder.stage, "stage_parser",
                                   side_effect=stage_result):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    builder.build_parser(
                        paths["source"], paths["nim"], paths["nimble"],
                        paths["seed"], paths["build"], paths["stage"],
                        paths["guard"], builder.acquire.GIT, paths["git_env"])
            self.assertFalse((paths["stage"] / builder.stage.ROOT_NAME).exists())
            self.assertFalse((paths["build"] / "lib").exists())
            self.assertFalse((paths["build"] / "archive").exists())


if __name__ == "__main__":
    unittest.main()
