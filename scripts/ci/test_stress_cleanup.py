"""Stage-2 owned-output cleanup and final inventory contract."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import stress_cleanup as cleanup

SOURCE = "a" * 40
RUN = "12345"


class CleanupTests(unittest.TestCase):
    def test_deletes_only_marked_target_and_preserves_private_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            workspace.mkdir()
            private = cleanup.private_directory(root)
            cleanup.prepare(workspace=workspace, private=private,
                            source=SOURCE, run_id=RUN)
            (workspace / "target/stress-publish").mkdir()
            (workspace / "target/stress-publish/case.json").write_text("public")
            (private / "case-receipt.private.json").write_text("private")
            with patch.object(cleanup.storage, "inventory",
                              return_value={"allocated_bytes": 1000}), \
                 patch.object(cleanup.release_storage, "docker_inventory",
                              return_value={"allocated_bytes": 200}), \
                 patch.object(cleanup.release_storage, "tool_roots",
                              return_value=[]):
                cleanup.cleanup(workspace=workspace, runner_temp=root,
                                private=private, source=SOURCE, run_id=RUN,
                                cases="stress")
            self.assertFalse((workspace / "target").exists())
            self.assertEqual((private / "case-receipt.private.json").read_text(),
                             "private")
            result = json.loads((private / "stage2-post-cleanup.storage.private.json").read_text())
            self.assertEqual(result["total"], 1200)
            self.assertTrue(result["target_deleted"])
            self.assertEqual(result["source"], SOURCE)

    def test_rejects_replaced_target_and_still_records_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            workspace.mkdir()
            private = cleanup.private_directory(root)
            cleanup.prepare(workspace=workspace, private=private,
                            source=SOURCE, run_id=RUN)
            (workspace / "target").rmdir()
            external = root / "external"
            external.mkdir()
            (workspace / "target").symlink_to(external, target_is_directory=True)
            with patch.object(cleanup.storage, "inventory",
                              return_value={"allocated_bytes": 1000}), \
                 patch.object(cleanup.release_storage, "docker_inventory",
                              return_value={"allocated_bytes": 0}), \
                 patch.object(cleanup.release_storage, "tool_roots",
                              return_value=[]):
                with self.assertRaises(ValueError):
                    cleanup.cleanup(workspace=workspace, runner_temp=root,
                                    private=private, source=SOURCE, run_id=RUN,
                                    cases="stress")
            self.assertTrue(external.exists())
            result = json.loads((private / "stage2-post-cleanup.storage.private.json").read_text())
            self.assertFalse(result["target_deleted"])

    def test_removal_failure_still_runs_final_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            workspace.mkdir()
            private = cleanup.private_directory(root)
            cleanup.prepare(workspace=workspace, private=private,
                            source=SOURCE, run_id=RUN)
            with patch.object(cleanup.shutil, "rmtree", side_effect=OSError("private path")) as remove, \
                 patch.object(cleanup.storage, "inventory",
                              return_value={"allocated_bytes": 1000}) as measure, \
                 patch.object(cleanup.release_storage, "docker_inventory",
                              return_value={"allocated_bytes": 0}), \
                 patch.object(cleanup.release_storage, "tool_roots",
                              return_value=[]):
                remove.avoids_symlink_attacks = True
                with self.assertRaises(ValueError):
                    cleanup.cleanup(workspace=workspace, runner_temp=root,
                                    private=private, source=SOURCE, run_id=RUN,
                                    cases="stress")
            measure.assert_called_once()
            self.assertTrue((workspace / "target").exists())
            self.assertFalse(json.loads((private / "stage2-post-cleanup.storage.private.json").read_text())
                             ["target_deleted"])

    def test_prepare_rejects_preexisting_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            workspace.mkdir()
            (workspace / "target").mkdir()
            private = cleanup.private_directory(root)
            with self.assertRaises(ValueError):
                cleanup.prepare(workspace=workspace, private=private,
                                source=SOURCE, run_id=RUN)

    def test_prepare_marker_failure_removes_new_empty_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            workspace.mkdir()
            private = cleanup.private_directory(root)
            with patch.object(cleanup, "write_private", side_effect=OSError("full")):
                with self.assertRaises(OSError):
                    cleanup.prepare(workspace=workspace, private=private,
                                    source=SOURCE, run_id=RUN)
            self.assertFalse((workspace / "target").exists())

    def test_wrong_source_does_not_delete_owned_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            workspace.mkdir()
            private = cleanup.private_directory(root)
            cleanup.prepare(workspace=workspace, private=private,
                            source=SOURCE, run_id=RUN)
            with patch.object(cleanup.storage, "inventory",
                              return_value={"allocated_bytes": 1000}), \
                 patch.object(cleanup.release_storage, "docker_inventory",
                              return_value={"allocated_bytes": 0}), \
                 patch.object(cleanup.release_storage, "tool_roots",
                              return_value=[]):
                with self.assertRaises(ValueError):
                    cleanup.cleanup(workspace=workspace, runner_temp=root,
                                    private=private, source="b" * 40,
                                    run_id=RUN, cases="stress")
            self.assertTrue((workspace / "target").exists())

    def test_workflow_cleanup_is_last_after_all_uploads(self):
        workflow = (cleanup.WORKSPACE / ".github/workflows/stress-tests.yml").read_text()
        for job in ("stress-tests", "sanitizer-lane", "fuzz-lane", "perf-lane"):
            start = workflow.index(f"  {job}:\n")
            following = [workflow.find(f"  {other}:\n", start + 1)
                         for other in ("stress-tests", "sanitizer-lane", "fuzz-lane", "perf-lane")]
            end = min((position for position in following if position >= 0),
                      default=len(workflow))
            block = workflow[start:end]
            self.assertIn("actions: read", block)
            self.assertIn("Record owned Stage-2 target", block)
            self.assertIn("Remove owned output and inventory host", block)
            self.assertLess(block.rindex("uses: ./.github/actions/stress-upload"),
                            block.rindex("Remove owned output and inventory host"))
            self.assertIn("always() && steps.bootstrap.outcome == 'success'", block)
        fuzz = workflow[workflow.index("  fuzz-lane:\n"):workflow.index("  perf-lane:\n")]
        self.assertEqual(fuzz.count("uses: ./.github/actions/stress-upload"), 2)
        action = (cleanup.WORKSPACE / ".github/actions/stress-upload/action.yml").read_text()
        self.assertEqual(action.count("RUNNER_ENVIRONMENT: ${{ runner.environment }}"), 3)

    def test_cli_failure_exposes_fixed_code_only(self):
        result = subprocess.run(
            [sys.executable, str(Path(cleanup.__file__)), "--stage", "cleanup",
             "--cases", "secret-path"], capture_output=True, text=True,
            env={"GITHUB_SHA": "invalid"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr.strip(), "STRESS_CLEANUP_REJECTED")
        self.assertNotIn("secret-path", result.stdout + result.stderr)

    def test_docker_inventory_runtime_error_exposes_fixed_code_only(self):
        with tempfile.TemporaryDirectory() as directory:
            script = """
import runpy, sys
from pathlib import Path
root = Path(sys.argv[1])
sys.path[:0] = [str(root / 'scripts/ci'), str(root / 'scripts/release'),
                str(root / 'scripts/compatibility')]
import release_storage, storage
storage.inventory = lambda _roots: {'allocated_bytes': 0}
def reject():
    raise RuntimeError('private host root')
release_storage.docker_inventory = reject
sys.argv = [str(root / 'scripts/ci/stress_cleanup.py'), '--stage', 'cleanup',
            '--cases', 'stress']
runpy.run_path(sys.argv[0], run_name='__main__')
"""
            source = subprocess.check_output(
                ["git", "-C", str(cleanup.WORKSPACE), "rev-parse", "HEAD"],
                text=True).strip()
            env = dict(os.environ, GITHUB_SHA=source, GITHUB_RUN_ID=RUN,
                       GITHUB_WORKSPACE=str(cleanup.WORKSPACE),
                       RUNNER_TEMP=directory, RUNNER_ENVIRONMENT="github-hosted")
            result = subprocess.run([sys.executable, "-c", script,
                                     str(cleanup.WORKSPACE)],
                                    capture_output=True, text=True, env=env)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stderr.strip(), "STRESS_CLEANUP_REJECTED")
            self.assertNotIn("private host root", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
