"""Offline bootstrap contract checks before Stage-2 checkout."""

import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import stress_monitored_checkout as bootstrap


class BootstrapTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
