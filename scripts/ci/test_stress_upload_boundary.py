"""Synthetic allowlist and capacity admission for Stage-2 uploads."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import stress_public_evidence as public_evidence
import stress_upload_boundary as boundary
from test_stress_artifact_intake import make_cms


SOURCE = "a" * 40


def fixture(root: Path):
    workspace = root / "checkout"
    case_dir = workspace / "target/stress-publish/perf"
    case_dir.mkdir(parents=True, mode=0o700)
    cms = case_dir / "perf.cms"
    cms.write_bytes(make_cms(root))
    status = public_evidence.build_status(
        case_id="perf", source_commit=SOURCE, outcome="passed",
        error_code="NONE", ciphertext=cms)
    status_path = case_dir / "perf.json"
    public_evidence.write_status(status_path, status)
    return workspace, case_dir, status_path, cms


class UploadBoundaryTests(unittest.TestCase):
    def test_admits_only_matching_public_and_private_files(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, _, status, cms = fixture(Path(directory))
            self.assertEqual(boundary.verify_output(
                workspace=workspace, case_id="perf", kind="public",
                source=SOURCE), status)
            self.assertEqual(boundary.verify_output(
                workspace=workspace, case_id="perf", kind="private",
                source=SOURCE), cms)

    def test_rejects_raw_status_fields_and_ciphertext_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace, _, status_path, cms = fixture(Path(directory))
            original = status_path.read_bytes()
            altered = json.loads(original)
            altered["host_path"] = "/private/sensitive"
            status_path.write_text(json.dumps(altered))
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="perf",
                                       kind="public", source=SOURCE)
            status_path.write_bytes(original)
            cms.write_bytes(b"plaintext")
            with self.assertRaises(ValueError):
                boundary.verify_output(workspace=workspace, case_id="perf",
                                       kind="private", source=SOURCE)

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
                    case_id="perf", kind="public", stage="before"),
                    38_500_000_000)
            with patch.object(boundary.storage, "inventory",
                              return_value={"allocated_bytes": 39_500_000_000}), \
                 patch.object(boundary.release_storage, "docker_inventory",
                              return_value={"allocated_bytes": 0}), \
                 patch.object(boundary.release_storage, "tool_roots",
                              return_value=[]):
                with self.assertRaises(ValueError):
                    boundary.measure(workspace=root, runner_temp=root, private=private,
                                     case_id="perf", kind="private", stage="before")
                self.assertEqual(boundary.measure(
                    workspace=root, runner_temp=root, private=private,
                    case_id="perf", kind="public", stage="after"),
                    39_500_000_000)

    def test_cli_error_is_fixed_without_private_path(self):
        result = subprocess.run([
            sys.executable, str(Path(boundary.__file__)),
            "--case", "sensitive-machine-path", "--kind", "public",
            "--stage", "before"], capture_output=True, text=True,
            env={"GITHUB_SHA": "invalid"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr.strip(), "STRESS_UPLOAD_BOUNDARY_REJECTED")
        self.assertNotIn("sensitive-machine-path", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
