"""Source focused checks for the offline selected socket inventory."""
import importlib.util
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch
import yaml

ROOT = Path(__file__).resolve().parents[3]
MODULE = ROOT / "scripts/ops/s04_socket_inventory.py"
SPEC = importlib.util.spec_from_file_location("s04_socket_inventory", MODULE)
import sys
sys.path.insert(0, str(MODULE.parent))
inventory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(inventory)


class SocketInventoryTests(unittest.TestCase):
    def test_selected_context_requires_unique_absolute_unix_endpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            meta = home / ".docker/contexts/meta/one"
            meta.mkdir(parents=True)
            (home / ".docker/config.json").write_text('{"currentContext":"selected"}')
            (meta / "meta.json").write_text(json.dumps({
                "Name": "selected", "Endpoints": {"docker": {"Host": "unix:///run/docker.sock"}}}))
            self.assertEqual(inventory.selected_endpoint(home, {}), Path("/run/docker.sock"))
            for host in ("tcp://127.0.0.1:2375", "unix://relative.sock", "unix:///run/../docker.sock"):
                (meta / "meta.json").write_text(json.dumps({
                    "Name": "selected", "Endpoints": {"docker": {"Host": host}}}))
                with self.subTest(host=host), self.assertRaises(inventory.PreflightError):
                    inventory.selected_endpoint(home, {})
            with self.assertRaises(inventory.PreflightError):
                inventory.selected_endpoint(home, {"DOCKER_HOST": "unix:///tmp/other.sock"})

    def test_path_metadata_rejects_symlink_and_non_socket(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(os.path.realpath(directory))
            endpoint = root / "service.sock"
            with patch.object(inventory, "acl", return_value="user::rw-\n"):
                with self.assertRaises(OSError):
                    inventory.inspect_path(endpoint)
                endpoint.touch()
                original_lstat = Path.lstat
                def fake_lstat(path):
                    info = original_lstat(path)
                    if path == endpoint:
                        fields = list(info)
                        fields[0] = stat.S_IFSOCK | 0o600
                        return os.stat_result(fields)
                    return info
                with patch.object(Path, "lstat", fake_lstat):
                    facts = inventory.inspect_path(endpoint)
                    self.assertEqual(facts[-1]["path"], str(endpoint))
                alias = root / "alias"
                alias.symlink_to(endpoint)
                with self.assertRaises(inventory.PreflightError):
                    inventory.inspect_path(alias)

    def test_workflow_separates_inventory_from_connection_jobs(self):
        source = (ROOT / ".github/workflows/alopex-performance.yml").read_text()
        workflow = yaml.safe_load(source)
        steps = workflow["jobs"]["s04_socket_inventory"]["steps"]
        precheckout = steps[0]["run"]
        job = source.split("  s04_socket_inventory:\n", 1)[1].split("  inventory:\n", 1)[0]
        self.assertIn("!inputs.s04_inspect_mode", job)
        self.assertIn("expected_controller_sha", job)
        self.assertNotIn("docker context", job)
        self.assertNotIn("docker info", job)
        self.assertIn("assert len(daemons) == 1", job)
        self.assertIn("--data-root=", job)
        self.assertIn("--config-file", job)
        self.assertIn("daemon.resolve(strict=True) == daemon", precheckout)
        self.assertIn("'-sk' if root == daemon else '-skL'", precheckout)
        self.assertIn("s04_socket_inventory_seal.py", job)


if __name__ == "__main__":
    unittest.main()
