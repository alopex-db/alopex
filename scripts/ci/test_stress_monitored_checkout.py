"""Offline bootstrap contract checks before Stage-2 checkout."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import stress_monitored_checkout as bootstrap


class BootstrapTests(unittest.TestCase):
    def test_exact_aggregate_bootstrap_and_helper_pins(self):
        root = Path(bootstrap.__file__).resolve().parents[2]
        for relative, expected in bootstrap.HELPERS.items():
            self.assertEqual(hashlib.sha256((root / relative).read_bytes()).hexdigest(),
                             expected)
        workflow = (root / ".github/workflows/stress-tests.yml").read_text()
        self.assertIn(hashlib.sha256(Path(bootstrap.__file__).read_bytes()).hexdigest(),
                      workflow)

    def test_checkout_is_owned_by_docker_aware_storage_guard(self):
        helper = Path("/private/release_storage.py")
        workspace = Path("/private/workspace")
        runner_temp = Path("/private/runner-temp")
        home = Path("/private/home")
        command = bootstrap.guarded_checkout_command(
            helper=helper, workspace=workspace, runner_temp=runner_temp,
            home=home, log=runner_temp / "storage.private.jsonl")
        self.assertEqual(command[:3], [sys.executable, str(helper), "--docker"])
        self.assertIn("--additional-bytes", command)
        self.assertEqual(command[command.index("--additional-bytes") + 1],
                         "20000000000")
        for root in (workspace, runner_temp, home / ".cargo", home / ".cache",
                     home / ".local", home / ".rustup", Path("/tmp")):
            self.assertIn(str(root), command)
        self.assertIn("--depth=1", command[-1])
        self.assertIn('"$GITHUB_SHA"', command[-1])
        self.assertIn('checkout -q --detach FETCH_HEAD', command[-1])

    def test_fetched_helper_requires_exact_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def synthetic_curl(argv, **_kwargs):
                Path(argv[argv.index("--output") + 1]).write_bytes(b"wrong")
            with patch.object(bootstrap.subprocess, "run", side_effect=synthetic_curl):
                with self.assertRaises(ValueError):
                    bootstrap.fetch_helper("a" * 40, "scripts/release/release_storage.py",
                                           hashlib.sha256(b"expected").hexdigest(), root)

    def test_invalid_source_prints_fixed_error(self):
        result = subprocess.run([sys.executable, str(Path(bootstrap.__file__))],
                                env={"GITHUB_SHA": "invalid"},
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr.strip(),
                         "STRESS_MONITORED_CHECKOUT_REJECTED")

    def test_failed_checkout_removes_owned_contents_and_inventories(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            workspace.mkdir()
            original = workspace.stat()
            (workspace / ".git").mkdir()
            (workspace / ".git/config").write_text("private")
            bootstrap_dir = root / "stress-bootstrap"
            bootstrap_dir.mkdir()
            helper = bootstrap_dir / "release_storage.py"
            helper.write_text("# trusted helper fixture")
            with patch.object(bootstrap.subprocess, "run",
                              return_value=subprocess.CompletedProcess([], 0)) as run:
                bootstrap.failed_checkout_cleanup(
                    workspace=workspace, runner_temp=root,
                    bootstrap_dir=bootstrap_dir, helper=helper,
                    original=(original.st_dev, original.st_ino, original.st_uid))
            self.assertEqual(list(workspace.iterdir()), [])
            self.assertIn("--docker", run.call_args.args[0])
            result = json.loads((bootstrap_dir / "failed-checkout-cleanup.private.json").read_text())
            self.assertTrue(result["workspace_removed"])
            self.assertTrue(result["inventory_completed"])

    def test_failed_checkout_rejects_replaced_workspace_but_inventories(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "checkout"
            workspace.mkdir()
            original = workspace.stat()
            workspace.rmdir()
            external = root / "external"
            external.mkdir()
            (external / "keep").write_text("private")
            workspace.symlink_to(external, target_is_directory=True)
            bootstrap_dir = root / "stress-bootstrap"
            bootstrap_dir.mkdir()
            helper = bootstrap_dir / "release_storage.py"
            helper.write_text("# trusted helper fixture")
            with patch.object(bootstrap.subprocess, "run",
                              return_value=subprocess.CompletedProcess([], 0)):
                with self.assertRaises(ValueError):
                    bootstrap.failed_checkout_cleanup(
                        workspace=workspace, runner_temp=root,
                        bootstrap_dir=bootstrap_dir, helper=helper,
                        original=(original.st_dev, original.st_ino, original.st_uid))
            self.assertTrue((external / "keep").exists())
            result = json.loads((bootstrap_dir / "failed-checkout-cleanup.private.json").read_text())
            self.assertFalse(result["workspace_removed"])
            self.assertTrue(result["inventory_completed"])


if __name__ == "__main__":
    unittest.main()
