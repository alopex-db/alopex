"""Public-only Stage-2 upload admission and host-budget checks."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import stress_public_case as public_case
import stress_upload_boundary as boundary

SOURCE = "a" * 40


def fixture(root: Path):
    workspace = root / "checkout"
    evidence = workspace / "target/stress-public-artifacts/sql_parser"
    evidence.mkdir(parents=True)
    (evidence / "execution.json").write_text(json.dumps({
        "schema": "alopex.stress-execution/v1", "case_id": "sql_parser",
        "source_commit": SOURCE, "outcome": "passed", "duration_ms": 1200,
        "fuzz_executions": 1000,
    }))
    result = public_case.assemble(workspace=workspace, case_id="sql_parser",
                                  source=SOURCE, outcome="passed")
    output = public_case.write_result(workspace=workspace, case_id="sql_parser",
                                      result=result)
    return workspace, evidence, output


class UploadBoundaryTests(unittest.TestCase):
    def test_admits_exact_public_case_only(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, _, output = fixture(Path(directory))
            self.assertEqual(boundary.verify_output(
                workspace=workspace, case_id="sql_parser", source=SOURCE), output)

    def test_rejects_raw_fields_extra_file_and_source_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, evidence, output = fixture(Path(directory))
            original = output.read_bytes()
            altered = json.loads(original)
            altered["host_path"] = "/private/sensitive"
            output.write_text(json.dumps(altered))
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="sql_parser",
                                       source=SOURCE)
            output.write_bytes(original)
            duplicate = original.replace(b'"case_id":"sql_parser"',
                                         b'"case_id":"/private/sensitive","case_id":"sql_parser"')
            output.write_bytes(duplicate)
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="sql_parser",
                                       source=SOURCE)
            output.write_bytes(original)
            (output.parent / "raw.log").write_text("private")
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="sql_parser",
                                       source=SOURCE)
            (output.parent / "raw.log").unlink()
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="sql_parser",
                                       source="b" * 40)
            (evidence / "unexpected.json").symlink_to(evidence / "execution.json")
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="sql_parser",
                                       source=SOURCE)

    def test_reserves_space_before_upload_and_checks_after(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "stress-private"
            private.mkdir(mode=0o700)
            with patch.object(boundary.storage, "inventory",
                              return_value={"allocated_bytes": 38_500_000_000}), \
                 patch.object(boundary.release_storage, "docker_inventory",
                              return_value={"allocated_bytes": 0}), \
                 patch.object(boundary.release_storage, "tool_roots",
                              return_value=[]):
                self.assertEqual(boundary.measure(
                    workspace=root, runner_temp=root, private=private,
                    case_id="perf", stage="before"), 38_500_000_000)
            with patch.object(boundary.storage, "inventory",
                              return_value={"allocated_bytes": 39_500_000_000}), \
                 patch.object(boundary.release_storage, "docker_inventory",
                              return_value={"allocated_bytes": 0}), \
                 patch.object(boundary.release_storage, "tool_roots",
                              return_value=[]):
                with self.assertRaises(ValueError):
                    boundary.measure(workspace=root, runner_temp=root, private=private,
                                     case_id="stress", stage="before")
                self.assertEqual(boundary.measure(
                    workspace=root, runner_temp=root, private=private,
                    case_id="perf", stage="after"), 39_500_000_000)

    def test_cli_error_is_fixed_without_private_path(self):
        result = subprocess.run([
            sys.executable, str(Path(boundary.__file__)),
            "--case", "sensitive-machine-path", "--stage", "before",
        ], capture_output=True, text=True, env={"GITHUB_SHA": "invalid"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr.strip(), "STRESS_UPLOAD_BOUNDARY_REJECTED")
        self.assertNotIn("sensitive-machine-path", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
