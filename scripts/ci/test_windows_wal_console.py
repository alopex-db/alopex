"""Small subprocess regressions for CI pipes using a legacy Windows code page."""
import os
from pathlib import Path
import subprocess
import sys
import unittest


class WindowsConsoleTests(unittest.TestCase):
    def test_unicode_log_does_not_replace_child_result_with_encoding_failure(self):
        for result in (0, 17):
            with self.subTest(result=result):
                child = subprocess.run(
                    [sys.executable, "-c",
                     "from windows_wal_console import print_log_tail; "
                     "print_log_tail('warning: 日本語 🦊'); "
                     f"raise SystemExit({result})"],
                    cwd=Path(__file__).resolve().parent,
                    env={**os.environ, "PYTHONIOENCODING": "cp1252:strict",
                         "PYTHONDONTWRITEBYTECODE": "1"},
                    capture_output=True,
                )
                self.assertEqual(child.returncode, result, child.stderr)
                self.assertEqual(child.stdout.decode("utf-8").strip(), "warning: 日本語 🦊")

    def test_excerpt_stays_bounded_and_preserves_unicode_suffix(self):
        child = subprocess.run(
            [sys.executable, "-c",
             "from windows_wal_console import print_log_tail; "
             "print_log_tail('x' * 6001 + '日本語')"],
            cwd=Path(__file__).resolve().parent,
            env={**os.environ, "PYTHONIOENCODING": "cp1252:strict",
                 "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True,
        )
        self.assertEqual(child.returncode, 0, child.stderr)
        self.assertEqual(child.stdout.decode("utf-8").rstrip("\r\n"), "x" * 5997 + "日本語")


if __name__ == "__main__":
    unittest.main()
