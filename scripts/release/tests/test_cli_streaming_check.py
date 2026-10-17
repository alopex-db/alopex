from __future__ import annotations

import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "check_cli_streaming", ROOT / "scripts/ci/check_cli_streaming.py"
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CliStreamingCheckTests(unittest.TestCase):
    def test_jsonl_accepts_exact_ids(self):
        MODULE.validate_output('{"id":1}\n{"id":2}\n', "jsonl", 2)

    def test_jsonl_rejects_missing_duplicate_reordered_and_status_rows(self):
        for output in [
            "",
            '{"id":1}\n',
            '{"id":1}\n{"id":1}\n',
            '{"id":2}\n{"id":1}\n',
            '{"id":"1"}\n{"id":2}\n',
            '{"id":1}\n{"id":2}\n{"status":"OK"}\n',
            "not json\n",
        ]:
            with self.subTest(output=output), self.assertRaises(ValueError):
                MODULE.validate_output(output, "jsonl", 2)

    def test_table_accepts_exact_ids(self):
        MODULE.validate_output(
            "┌────┐\n│ id │\n╞════╡\n│ 1  │\n├╌╌╌╌┤\n│ 2  │\n└────┘\n", "table", 2
        )

    def test_table_rejects_empty_truncated_duplicate_and_diagnostics(self):
        for output in [
            "",
            "│ 1 │\n",
            "│ 1 │\n│ 1 │\n",
            "│ 2 │\n│ 1 │\n",
            "│ 1 │\n│ 2 │\nError: ignored\n",
        ]:
            with self.subTest(output=output), self.assertRaises(ValueError):
                MODULE.validate_output(output, "table", 2)

    def invoke(self, result, *, overflow=False):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(MODULE.subprocess, "run", return_value=result),
        ):
            return MODULE.run_sql(
                Path("alopex"),
                Path(directory),
                "SELECT 1;",
                "table",
                expect_overflow=overflow,
            )

    def test_nonzero_exit_is_not_hidden_by_valid_looking_stdout(self):
        with self.assertRaises(RuntimeError):
            self.invoke(subprocess.CompletedProcess([], 1, '{"id":1}\n', "parse error"))

    def test_timeout_is_fatal(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(
                MODULE.subprocess,
                "run",
                side_effect=subprocess.TimeoutExpired("alopex", 180),
            ),
            self.assertRaises(subprocess.TimeoutExpired),
        ):
            MODULE.run_sql(Path("alopex"), Path(directory), "SELECT 1;", "jsonl")

    def test_expected_overflow_requires_error_and_empty_stdout(self):
        message = "Error: Buffer limit exceeded (~10MB). Use --output json|csv|tsv"
        self.invoke(subprocess.CompletedProcess([], 1, "", message), overflow=True)
        for result in [
            subprocess.CompletedProcess([], 0, "", message),
            subprocess.CompletedProcess([], 1, "partial row", message),
            subprocess.CompletedProcess([], 1, "", "parse error"),
            subprocess.CompletedProcess([], -9, "", message),
        ]:
            with self.subTest(result=result), self.assertRaises(RuntimeError):
                self.invoke(result, overflow=True)
