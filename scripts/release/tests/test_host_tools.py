"""Regression tests for native Git Bash selection on Windows hosts."""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.release.tests.host_tools import git_bash


class GitBashTests(unittest.TestCase):
    def test_windows_uses_real_git_shell_for_supported_git_layouts(self):
        with tempfile.TemporaryDirectory(prefix="Git installation ") as temp:
            root = Path(temp)
            shell = root / "usr/bin/bash.exe"
            shell.parent.mkdir(parents=True)
            shell.touch()
            for layout in ["cmd/git.exe", "bin/git.exe", "mingw64/bin/git.exe"]:
                with self.subTest(layout=layout):
                    git = root / layout
                    git.parent.mkdir(parents=True, exist_ok=True)
                    git.touch()

                    # PATH bash may be a WSL shim or the Git launcher. Neither
                    # may be executed while resolving the actual shell.
                    def which(command, git=git):
                        return str(git) if command == "git" else "WSL/bash.exe"

                    with (
                        mock.patch("platform.system", return_value="Windows"),
                        mock.patch("shutil.which", side_effect=which),
                        mock.patch("subprocess.check_output") as launch,
                    ):
                        self.assertEqual(Path(git_bash()), shell.resolve())
                        launch.assert_not_called()

    def test_windows_reports_missing_git_without_using_path_bash(self):
        with (
            mock.patch("platform.system", return_value="Windows"),
            mock.patch("shutil.which", return_value=None),
            self.assertRaisesRegex(FileNotFoundError, "Git for Windows"),
        ):
            git_bash()

    def test_windows_rejects_git_installation_without_real_shell(self):
        with tempfile.TemporaryDirectory() as temp:
            git = Path(temp) / "cmd/git.exe"
            git.parent.mkdir()
            git.touch()
            with (
                mock.patch("platform.system", return_value="Windows"),
                mock.patch("shutil.which", return_value=str(git)),
                self.assertRaisesRegex(FileNotFoundError, "usr/bin/bash.exe"),
            ):
                git_bash()

    def test_unix_retains_path_bash(self):
        for host in ["Linux", "Darwin"]:
            with (
                self.subTest(host=host),
                mock.patch("platform.system", return_value=host),
                mock.patch("shutil.which") as which,
            ):
                self.assertEqual(git_bash(), "bash")
                which.assert_not_called()
