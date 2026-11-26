"""Rust preflight never installs a toolchain or leaks runner details."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_rust_preflight as rust


class RustPreflightTest(unittest.TestCase):
    def test_qualification_workflow_only_checks_exact_source_and_tools(self):
        workflow = (Path(__file__).resolve().parents[3] / ".github/workflows/"
                    "issue681-hosted-qualify.yml").read_text()
        self.assertIn("permissions: {}", workflow)
        self.assertIn("types: [opened]", workflow)
        self.assertNotIn("synchronize", workflow)
        self.assertNotIn("reopened", workflow)
        self.assertIn("head.ref == 'prep/v0817-681-hosted-build'", workflow)
        self.assertLess(workflow.index("Check exact PR event"),
                        workflow.index("Fetch exact public PR head"))
        self.assertLess(workflow.index("Fetch exact public PR head"),
                        workflow.index("Qualify selected Linux job"))
        self.assertLess(workflow.index("Qualify selected Linux job"),
                        workflow.index("Check exact installed Rust without network"))
        self.assertIn("python3 -B scripts/performance/"
                      "issue681_hosted_rust_preflight.py", workflow)
        for forbidden in ("upload-artifact", "rustup toolchain install",
                          "cargo build", "actions/checkout", "uses:"):
            self.assertNotIn(forbidden, workflow)

    def setup_tools(self, root, *, compiler=None, cargo=None):
        home = root / "home"
        home.mkdir(mode=0o700)
        binary = root / "bin"
        binary.mkdir(mode=0o700)
        for name in ("rustc", "cargo"):
            path = binary / name
            path.write_bytes(b"tool")
            path.chmod(0o700)
        compiler = compiler if compiler is not None else (
            b"rustc 1.90.0\nbinary: rustc\nrelease: 1.90.0\n"
            b"host: x86_64-unknown-linux-gnu\n")
        cargo = cargo if cargo is not None else (
            b"cargo 1.90.0 (840b83a10 2025-07-30)\n")
        calls = []

        def which(name, *, path):
            calls.append(("which", name, path))
            return str(binary / name)

        def command(argv, *, env, stdin, capture_output, timeout):
            calls.append(("run", argv[0], tuple(argv[1:]), env))
            self.assertEqual(env["RUSTUP_AUTO_INSTALL"], "0")
            self.assertEqual(env["RUSTUP_TOOLCHAIN"], "1.90.0")
            self.assertEqual(set(env), {"HOME", "PATH", "LANG",
                                        "RUSTUP_AUTO_INSTALL",
                                        "RUSTUP_TOOLCHAIN"})
            self.assertEqual(stdin, subprocess.DEVNULL)
            self.assertTrue(capture_output)
            self.assertEqual(timeout, 10)
            output = compiler if argv[0].endswith("rustc") else cargo
            return subprocess.CompletedProcess(argv, 0, output, b"")

        patchers = [mock.patch.object(rust.Path, "home", return_value=home),
                    mock.patch.object(rust.shutil, "which", side_effect=which),
                    mock.patch.object(rust.subprocess, "run", side_effect=command)]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)
        return calls

    def test_exact_existing_tools_pass_without_install(self):
        with tempfile.TemporaryDirectory() as tmp:
            calls = self.setup_tools(Path(tmp).resolve())
            self.assertEqual(rust.run(), ({"case": "HNSW_TOOLCHAIN",
                                           "outcome": "passed"}, 0))
            self.assertEqual([call[0] for call in calls],
                             ["which", "which", "run", "run"])
            self.assertEqual([call[2] for call in calls if call[0] == "run"],
                             [("-Vv",), ("-V",)])

    def test_wrong_compiler_or_cargo_is_fixed_failure(self):
        for changed in ("compiler", "host", "cargo"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as tmp:
                options = {
                    "compiler": {"compiler": b"release: 1.99.0\n"
                                 b"host: x86_64-unknown-linux-gnu\n"},
                    "host": {"compiler": b"release: 1.90.0\n"
                             b"host: aarch64-unknown-linux-gnu\n"},
                    "cargo": {"cargo":
                              b"cargo 1.99.0 (840b83a10 2026-01-01)\n"},
                }[changed]
                self.setup_tools(Path(tmp).resolve(), **options)
                self.assertEqual(rust.run(), ({"case": "HNSW_TOOLCHAIN",
                                               "outcome": "failed",
                                               "error_code": "TOOLCHAIN"}, 1))

    def test_missing_tool_stops_before_any_version_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            calls = self.setup_tools(Path(tmp).resolve())
            with mock.patch.object(rust.shutil, "which", return_value=None):
                self.assertEqual(rust.run(), ({"case": "HNSW_TOOLCHAIN",
                                               "outcome": "failed",
                                               "error_code": "TOOLCHAIN"}, 1))
            self.assertFalse(any(call[0] == "run" for call in calls))

    def test_path_bearing_failure_is_not_returned(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.setup_tools(Path(tmp).resolve())
            with mock.patch.object(rust.subprocess, "run",
                                   side_effect=OSError("private runner path")):
                outcome, status = rust.run()
            self.assertEqual(status, 1)
            self.assertEqual(outcome, {"case": "HNSW_TOOLCHAIN",
                                       "outcome": "failed",
                                       "error_code": "TOOLCHAIN"})

    def test_check_admission_accepts_healthy_rustup_and_current_stable_without_1_90(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            home = root / "home"
            home.mkdir(mode=0o700)
            binary = root / "bin"
            binary.mkdir(mode=0o700)
            for name in ("rustup", "rustc", "cargo"):
                path = binary / name
                path.write_bytes(b"tool")
                path.chmod(0o700)

            def which(name, *, path):
                return str(binary / name)

            def command(argv, *, env, stdin, capture_output, timeout):
                self.assertEqual(env["RUSTUP_AUTO_INSTALL"], "0")
                self.assertNotIn("RUSTUP_TOOLCHAIN", env)
                if argv[0].endswith("rustup"):
                    out = b"rustup 1.28.2 (e4f3ad6f8 2025-04-28)\n"
                    err = (b"info: This is the version for the rustup toolchain manager, "
                           b"not the rustc compiler.\n"
                           b"info: the currently active `rustc` version is `rustc 1.92.0`\n")
                elif argv[0].endswith("rustc"):
                    out = (b"rustc 1.92.0\nbinary: rustc\nrelease: 1.92.0\n"
                           b"host: x86_64-unknown-linux-gnu\n")
                    err = b""
                else:
                    out = b"cargo 1.92.0 (123456789 2026-01-15)\n"
                    err = b""
                return subprocess.CompletedProcess(argv, 0, out, err)

            with mock.patch.object(rust.Path, "home", return_value=home), \
                 mock.patch.object(rust.shutil, "which", side_effect=which), \
                 mock.patch.object(rust.subprocess, "run", side_effect=command):
                rust.check_admission()
            # Non-info stderr on rustup -V must still fail.
            with mock.patch.object(rust.Path, "home", return_value=home), \
                 mock.patch.object(rust.shutil, "which", side_effect=which), \
                 mock.patch.object(
                     rust.subprocess, "run",
                     return_value=subprocess.CompletedProcess(
                         ["rustup", "-V"], 0,
                         b"rustup 1.28.2 (e4f3ad6f8 2025-04-28)\n",
                         b"error: unexpected warning\n")):
                with self.assertRaises(ValueError):
                    rust.check_admission()
            with mock.patch.object(rust.Path, "home", return_value=home), \
                 mock.patch.object(rust.shutil, "which",
                                   side_effect=lambda n, path: None if n == "rustup" else str(binary / n)), \
                 mock.patch.object(rust.subprocess, "run", side_effect=command):
                with self.assertRaises(ValueError):
                    rust.check_admission()


if __name__ == "__main__":
    unittest.main()
