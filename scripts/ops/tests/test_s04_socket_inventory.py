"""Source focused checks for the offline selected socket inventory."""
import ast
import importlib.util
import json
import os
import pathlib
from pathlib import Path
import socket
import stat
import subprocess
import shutil
import tempfile
import textwrap
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
MODULE = ROOT / "scripts/ops/s04_socket_inventory.py"
SPEC = importlib.util.spec_from_file_location("s04_socket_inventory", MODULE)
import sys
sys.path.insert(0, str(MODULE.parent))
inventory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(inventory)
SEAL_SPEC = importlib.util.spec_from_file_location(
    "s04_socket_inventory_seal", ROOT / "scripts/ops/s04_socket_inventory_seal.py")
sealer = importlib.util.module_from_spec(SEAL_SPEC)
SEAL_SPEC.loader.exec_module(sealer)


class SocketInventoryTests(unittest.TestCase):
    def test_default_context_uses_fixed_local_socket_without_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(os.path.realpath(directory))
            (home / ".docker").mkdir()
            with patch.object(inventory, "default_endpoint", return_value=Path("/run/docker.sock")) as default:
                for config in (None, {}, {"currentContext": "default"}):
                    config_path = home / ".docker/config.json"
                    if config is None:
                        config_path.unlink(missing_ok=True)
                    else:
                        config_path.write_text(json.dumps(config))
                    self.assertEqual(inventory.selected_endpoint(home, {}), Path("/run/docker.sock"))
                self.assertEqual(default.call_count, 3)

    def test_default_endpoint_accepts_only_fixed_run_alias(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "var").mkdir()
            (root / "run").mkdir()
            (root / "var/run").symlink_to("../run")
            self.assertEqual(inventory.default_endpoint(root), root / "run/docker.sock")
            (root / "var/run").unlink()
            (root / "var/run").mkdir()
            self.assertEqual(inventory.default_endpoint(root), root / "var/run/docker.sock")
            (root / "var/run").rmdir()
            (root / "var/run").symlink_to("/other")
            with self.assertRaisesRegex(inventory.PreflightError, "ENDPOINT_INVALID"):
                inventory.default_endpoint(root)
            (root / "var/run").unlink()
            with self.assertRaisesRegex(inventory.PreflightError, "ENDPOINT_INVALID"):
                inventory.default_endpoint(root)
            (root / "var/run").touch()
            with self.assertRaisesRegex(inventory.PreflightError, "ENDPOINT_INVALID"):
                inventory.default_endpoint(root)

    def test_selected_context_requires_unique_absolute_unix_endpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(os.path.realpath(directory))
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
            (meta / "meta.json").write_text(json.dumps({
                "Name": "selected", "Endpoints": {"docker": {"Host": "unix:///run/docker.sock"}}}))
            (meta.parent / "two").mkdir()
            (meta.parent / "two/meta.json").write_text((meta / "meta.json").read_text())
            with self.assertRaisesRegex(inventory.PreflightError, "CONTEXT_METADATA_CARDINALITY"):
                inventory.selected_endpoint(home, {})
            (meta.parent / "two/meta.json").unlink()
            (meta.parent / "two").rmdir()
            (meta / "meta.json").write_text(json.dumps({"Name": "other"}))
            with self.assertRaisesRegex(inventory.PreflightError, "CONTEXT_METADATA_CARDINALITY"):
                inventory.selected_endpoint(home, {})
            (meta / "meta.json").unlink()
            (meta / "meta.json").symlink_to(home / ".docker/config.json")
            with self.assertRaises(OSError):
                inventory.selected_endpoint(home, {})

    def test_malformed_context_selection_has_distinct_private_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(os.path.realpath(directory))
            (home / ".docker").mkdir()
            config_path = home / ".docker/config.json"
            for config in ([], {"currentContext": None}, {"currentContext": ""},
                           {"currentContext": 42}):
                config_path.write_text(json.dumps(config))
                with self.subTest(config=config), self.assertRaisesRegex(
                        inventory.PreflightError, "CONTEXT_SELECTION_INVALID"):
                    inventory.selected_endpoint(home, {})

    @unittest.skipUnless(hasattr(os, "O_PATH"), "Linux O_PATH is required")
    def test_path_metadata_rejects_symlink_and_non_socket(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(os.path.realpath(directory))
            endpoint = root / "service.sock"
            with patch.object(inventory, "acl", return_value="user::rw-\n") as acl:
                with self.assertRaises(OSError):
                    inventory.inspect_path(endpoint)
                endpoint.touch()
                with self.assertRaises(inventory.PreflightError):
                    inventory.inspect_path(endpoint)
                endpoint.unlink()
                with socket.socket(socket.AF_UNIX) as local:
                    local.bind(str(endpoint))
                    facts = inventory.inspect_path(endpoint)
                    self.assertEqual(facts[-1]["path"], str(endpoint))
                    self.assertIsInstance(acl.call_args.args[0], int)
                alias = root / "alias"
                alias.symlink_to(endpoint)
                with self.assertRaises(inventory.PreflightError):
                    inventory.inspect_path(alias)

    @unittest.skipUnless(hasattr(os, "O_PATH"), "Linux O_PATH is required")
    def test_path_change_after_pinned_acl_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            endpoint = Path(os.path.realpath(directory)) / "service.sock"
            with socket.socket(socket.AF_UNIX) as first, socket.socket(socket.AF_UNIX) as second:
                first.bind(str(endpoint))
                replaced = False
                def replace_after_acl(fd):
                    nonlocal replaced
                    if stat.S_ISSOCK(os.fstat(fd).st_mode) and not replaced:
                        endpoint.unlink()
                        second.bind(str(endpoint))
                        replaced = True
                    return "user::rw-\n"
                with patch.object(inventory, "acl", side_effect=replace_after_acl):
                    with self.assertRaises(inventory.PreflightError):
                        inventory.inspect_path(endpoint)
                self.assertTrue(replaced)

    def test_acl_uses_pinned_descriptor_and_rejects_truncation(self):
        class Result:
            returncode = 0
            stderr = b""
            stdout = b"user::rw-\n"
        with patch.object(inventory.subprocess, "run", return_value=Result()) as run:
            self.assertEqual(inventory.acl(17), "user::rw-\n")
            self.assertEqual(run.call_args.kwargs["pass_fds"], (17,))
            self.assertEqual(run.call_args.args[0][-1], "/proc/self/fd/17")
        Result.stdout = b"x" * (inventory.MAX_ACL + 1)
        with patch.object(inventory.subprocess, "run", return_value=Result()):
            with self.assertRaises(inventory.PreflightError):
                inventory.acl(17)

    def test_bounded_metadata_rejects_truncation_and_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(os.path.realpath(directory))
            metadata = root / "mountinfo"
            metadata.write_bytes(b"12345")
            with self.assertRaises(inventory.PreflightError):
                inventory.bounded_read(metadata, 4)
            alias = root / "alias"
            alias.symlink_to(metadata)
            with self.assertRaises(OSError):
                inventory.bounded_read(alias, 6)

    def test_source_mismatch_stops_before_metadata(self):
        with patch.object(inventory, "selected_endpoint") as selected:
            with self.assertRaises(inventory.PreflightError):
                inventory.collect(Path("/tmp"), {"GITHUB_EVENT_NAME": "workflow_dispatch",
                                                 "ISSUE_NUMBER": "684", "GITHUB_SHA": "a" * 40,
                                                 "EXPECTED_CONTROLLER_SHA": "b" * 40})
            selected.assert_not_called()

    def test_failure_receipt_is_encrypted_and_public_status_is_fixed(self):
        if not shutil.which("openssl"):
            self.skipTest("OpenSSL unavailable")
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            expected = "a" * 40
            env = {"RUNNER_TEMP": str(work), "EXPECTED_CONTROLLER_SHA": expected,
                   "GITHUB_SHA": expected, "GITHUB_EVENT_NAME": "workflow_dispatch",
                   "ISSUE_NUMBER": "684", "COLLECT_OUTCOME": "failure"}
            with patch.dict(os.environ, env), \
                    patch.object(inventory.subprocess, "run", return_value=subprocess.CompletedProcess(
                        ["git"], 0, (expected + "\n").encode())), \
                    patch.object(inventory, "selected_endpoint",
                                 side_effect=inventory.PreflightError("CONTEXT_METADATA_CARDINALITY")), \
                    patch("sys.stdout"):
                self.assertEqual(inventory.main(), 1)
            private = work / "alopex-s04-socket-inventory"
            failure = json.loads((private / "inspector.private.json").read_text())
            self.assertEqual(failure, {"schema": "alopex.s04-socket-inventory-failure/v1",
                                       "read_only": True, "outcome": "failed",
                                       "stage": "CONTEXT", "reason": "CONTEXT_METADATA_CARDINALITY"})
            with patch.dict(os.environ, env), patch("sys.stdout"):
                previous = Path.cwd()
                try:
                    os.chdir(work)
                    self.assertEqual(sealer.main(), 1)
                finally:
                    os.chdir(previous)
            export = work / "native-s04-socket-inventory-export"
            public = json.loads((export / "public.json").read_text())
            self.assertEqual(set(public), {"schema", "case", "outcome", "error_code",
                                           "ciphertext_sha256"})
            self.assertEqual((public["outcome"], public["error_code"]),
                             ("failed", "INSPECT_FAILED"))
            self.assertFalse(private.exists())
            ciphertext = (export / "private-evidence.cms").read_bytes()
            self.assertNotIn(b"CONTEXT_METADATA_CARDINALITY", ciphertext)
            self.assertNotIn("CONTEXT_METADATA_CARDINALITY", json.dumps(public))
            parsed = subprocess.run(["openssl", "cms", "-cmsout", "-print", "-inform", "DER",
                                     "-in", str(export / "private-evidence.cms")],
                                    capture_output=True, check=False)
            self.assertEqual(parsed.returncode, 0)

    def test_sealer_public_fields_are_allowlisted(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            private = work / "alopex-s04-socket-inventory"
            private.mkdir(mode=0o700)
            sealed = {"error_code": None, "ciphertext_sha256": "a" * 64,
                      "selected_endpoint": "/private/socket"}
            with patch.dict(os.environ, {"RUNNER_TEMP": str(work), "COLLECT_OUTCOME": "success"}), \
                    patch.object(sealer, "_seal_inner", return_value=(sealed, 0)), \
                    patch("sys.stdout"):
                previous = Path.cwd()
                try:
                    os.chdir(work)
                    self.assertEqual(sealer.main(), 0)
                finally:
                    os.chdir(previous)
            public = json.loads((work / "native-s04-socket-inventory-export/public.json").read_text())
            self.assertEqual(set(public), {"schema", "case", "outcome", "error_code", "ciphertext_sha256"})
            self.assertNotIn("/private/socket", json.dumps(public))

    def test_workflow_separates_inventory_from_connection_jobs(self):
        source = (ROOT / ".github/workflows/alopex-performance.yml").read_text()
        job = source.split("  s04_socket_inventory:\n", 1)[1].split("  inventory:\n", 1)[0]
        precheckout = job.split(
            "      - name: Gate exact source and host allocation before checkout\n", 1
        )[1].split("\n      - name:", 1)[0]
        self.assertIn("!inputs.s04_inspect_mode", job)
        self.assertIn("expected_controller_sha", job)
        self.assertNotIn("docker context", job)
        self.assertNotIn("docker info", job)
        self.assertIn("assert len(daemons) == 1", job)
        self.assertIn("comm = (entry / 'comm').read_text().strip()", precheckout)
        self.assertNotIn("except (OSError, PermissionError)", precheckout)
        self.assertIn("--data-root=", job)
        self.assertIn("--config-file", job)
        self.assertIn("daemon.resolve(strict=True) == daemon", precheckout)
        self.assertIn("'-sk' if root == daemon else '-skL'", precheckout)
        self.assertIn("temp_snapshot = temp_entries(temp_areas)", precheckout)
        self.assertIn("assert temp_entries(temp_areas) == temp_snapshot", precheckout)
        self.assertIn("('TMPDIR', 'TMP', 'TEMP')", precheckout)
        self.assertNotIn("('tmp', pathlib.Path('/tmp'))", precheckout)
        self.assertIn("s04_socket_inventory_seal.py", job)

    def test_precheckout_temp_snapshot_selects_alopex_entries_and_detects_change(self):
        source = (ROOT / ".github/workflows/alopex-performance.yml").read_text()
        job = source.split("  s04_socket_inventory:\n", 1)[1].split("  inventory:\n", 1)[0]
        script = textwrap.dedent(job.split("          python3 - <<'PY'\n", 1)[1]
                                 .split("\n          PY", 1)[0])
        function = next(node for node in ast.walk(ast.parse(script))
                        if isinstance(node, ast.FunctionDef) and node.name == "temp_entries")
        module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
        scope = {"os": os, "pathlib": pathlib}
        exec(compile(module, "s04_precheckout", "exec"), scope)
        with tempfile.TemporaryDirectory() as directory:
            tmp = Path(directory) / "tmp"
            var_tmp = Path(directory) / "var_tmp"
            tmp.mkdir()
            var_tmp.mkdir()
            (tmp / "unrelated").mkdir()
            selected = tmp / "alopex-owned"
            selected.mkdir()
            areas = ((tmp, "tmp"), (var_tmp, "var_tmp"))
            before = scope["temp_entries"](areas)
            self.assertEqual(len(before), 1)
            self.assertEqual(before[0][0:2], ("tmp", "alopex-owned"))
            selected_file = tmp / "alopex-file"
            selected_file.write_text("sample")
            self.assertNotEqual(scope["temp_entries"](areas), before)
            selected_file.unlink()
            (tmp / "another-unrelated").mkdir()
            self.assertEqual(scope["temp_entries"](areas), before)
            (var_tmp / "Alopex-new").mkdir()
            self.assertNotEqual(scope["temp_entries"](areas), before)
            (var_tmp / "Alopex-new").rmdir()
            selected.rmdir()
            selected.symlink_to(tmp / "missing")
            with self.assertRaises(FileNotFoundError):
                scope["temp_entries"](areas)


if __name__ == "__main__":
    unittest.main()
