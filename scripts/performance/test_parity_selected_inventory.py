"""Focused negative contracts for the selected runner diagnostic."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
import sys
sys.path.insert(0, str(HERE))
import parity_selected_inventory as collector
import parity_selected_inventory_seal as sealer


class SelectedInventoryTests(unittest.TestCase):
    JOB = {"GITHUB_SHA": "a" * 40, "GITHUB_RUN_ID": "42",
           "GITHUB_RUN_ATTEMPT": "1", "RUNNER_NAME": "fixture"}

    def receipt(self, private, **changes):
        payload = {"schema": collector.SCHEMA, "read_only": True,
                   "qualification": "not_assessed", "source_commit": self.JOB["GITHUB_SHA"],
                   "run_id": self.JOB["GITHUB_RUN_ID"],
                   "run_attempt": self.JOB["GITHUB_RUN_ATTEMPT"],
                   "runner_name": self.JOB["RUNNER_NAME"]}
        payload.update(changes)
        receipt = private / "inventory.private.json"
        receipt.write_text(json.dumps(payload))
        receipt.chmod(0o600)
        return receipt

    def test_workflow_is_opt_in_and_does_not_construct(self):
        workflow = (HERE.parents[1] / ".github/workflows/parity-performance.yml").read_text()
        inventory = workflow.split("  selected-inventory:\n", 1)[1].split("  performance:\n", 1)[0]
        self.assertIn("if: ${{ github.event.repository.private == true && inputs.selected_inventory && !inputs.hosted_capability", inventory)
        self.assertIn("inputs.issue_number == '712'", inventory)
        self.assertIn("inputs.expected_inventory_sha == github.sha", inventory)
        self.assertIn("timeout-minutes: 10", inventory)
        self.assertIn("private-evidence.cms", inventory)
        self.assertIn("public.json", inventory)
        self.assertIn("if: always() && steps.seal.outcome == 'success'", inventory)
        for forbidden in ("services:", "docker run", "podman run", "docker pull", "podman pull", "cargo build"):
            self.assertNotIn(forbidden, inventory)
        self.assertIn("if: ${{ github.event.repository.private == true && !inputs.hosted_capability && !inputs.selected_inventory }}", workflow)

    def test_missing_profile_fails_closed(self):
        source = "a" * 40
        env = {"GITHUB_EVENT_NAME": "workflow_dispatch", "PARITY_ISSUE_NUMBER": "712",
               "RUNNER_ENVIRONMENT": "self-hosted", "GITHUB_SHA": source,
               "EXPECTED_INVENTORY_SHA": source, "GITHUB_RUN_ID": "1",
               "GITHUB_RUN_ATTEMPT": "1", "RUNNER_NAME": "fixture"}
        def fake_command(argv):
            return {"exit": 0, "stdout": source if argv[0] == "git" else "{}", "stderr": ""}
        with patch.dict(os.environ, env, clear=True), \
             patch.object(collector.platform, "system", return_value="Linux"), \
             patch.object(collector.platform, "machine", return_value="x86_64"), \
             patch.object(collector, "command", side_effect=fake_command), \
             patch.object(collector, "runtime_observation", return_value=({}, Path("/tmp"))), \
             patch.object(collector, "bounded_text", return_value=""):
            self.assertEqual(collector.collect()["error_code"], "PROFILE")

    def test_missing_lsm_keeps_private_runtime_observation(self):
        source = "a" * 40
        env = {"GITHUB_EVENT_NAME": "workflow_dispatch", "PARITY_ISSUE_NUMBER": "712",
               "RUNNER_ENVIRONMENT": "self-hosted", "GITHUB_SHA": source,
               "EXPECTED_INVENTORY_SHA": source, "GITHUB_RUN_ID": "1",
               "GITHUB_RUN_ATTEMPT": "1", "RUNNER_NAME": "fixture"}
        def fake_command(argv):
            return {"exit": 0, "stdout": source if argv[0] == "git" else "{}", "stderr": ""}
        def fake_text(path):
            if str(path) == "/sys/kernel/security/lsm":
                raise collector.Incomplete("STORAGE_READ")
            return ""
        with patch.dict(os.environ, env, clear=True), \
             patch.object(collector.platform, "system", return_value="Linux"), \
             patch.object(collector.platform, "machine", return_value="x86_64"), \
             patch.object(collector, "command", side_effect=fake_command), \
             patch.object(collector, "runtime_observation", return_value=({}, Path("/tmp"))), \
             patch.object(collector, "bounded_text", side_effect=fake_text):
            facts = collector.collect()
        self.assertEqual(facts["error_code"], "STORAGE_READ")
        self.assertIn("podman_info", facts)

    def test_unreadable_and_deadline_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "link").symlink_to(root)
            with self.assertRaisesRegex(collector.Incomplete, "STORAGE_READ"):
                collector.allocation([root], __import__("time").monotonic() + 10)
            with self.assertRaisesRegex(collector.Incomplete, "TIMEOUT"):
                collector.allocation([root], 0)

    def test_oversize_read_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "oversize"
            source.write_bytes(b"x" * 10)
            with self.assertRaisesRegex(collector.Incomplete, "OVERSIZE"):
                collector.bounded_text(source, 8)

    def test_sealer_never_exports_plaintext_on_invalid_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            private = Path(tmp) / "private"
            private.mkdir(mode=0o700)
            receipt = private / "inventory.private.json"
            receipt.write_bytes(b"x" * (collector.MAX_RECEIPT + 1))
            receipt.chmod(0o600)
            export = Path(tmp) / "export"
            status, result = sealer.seal(private, export, "success")
            self.assertEqual(result, 1)
            self.assertEqual(status["error_code"], "EVIDENCE")
            self.assertFalse((export / "private-evidence.cms").exists())
            self.assertFalse(private.exists())

    def test_sealer_encrypts_and_removes_plaintext(self):
        with tempfile.TemporaryDirectory() as tmp:
            private = Path(tmp) / "private"
            private.mkdir(mode=0o700)
            self.receipt(private)
            export = Path(tmp) / "export"
            with patch.dict(os.environ, self.JOB, clear=True):
                status, result = sealer.seal(private, export, "success")
            self.assertEqual(result, 0)
            self.assertEqual(status["outcome"], "collected_not_qualified")
            self.assertEqual(len(status["ciphertext_sha256"]), 64)
            self.assertTrue((export / "private-evidence.cms").is_file())
            self.assertFalse(private.exists())

    def test_stale_receipt_is_rejected_before_encryption(self):
        for field, old in (("source_commit", "b" * 40), ("run_id", "41"),
                           ("run_attempt", "2"), ("runner_name", "old-runner")):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmp:
                private = Path(tmp) / "private"
                private.mkdir(mode=0o700)
                self.receipt(private, **{field: old})
                export = Path(tmp) / "export"
                with patch.dict(os.environ, self.JOB, clear=True):
                    status, result = sealer.seal(private, export, "success")
                self.assertEqual((status["error_code"], result), ("IDENTITY", 1))
                self.assertFalse((export / "private-evidence.cms").exists())
                self.assertFalse(private.exists())

    def test_stale_export_blocks_seal_and_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            private = Path(tmp) / "private"
            private.mkdir(mode=0o700)
            self.receipt(private)
            export = Path(tmp) / "export"
            export.mkdir(mode=0o700)
            (export / "private-evidence.cms").write_bytes(b"stale")
            (export / "public.json").write_text("stale")
            with patch.dict(os.environ, self.JOB, clear=True):
                status, result = sealer.seal(private, export, "success")
            self.assertEqual(result, 1)
            self.assertFalse(private.exists())
            self.assertEqual((export / "private-evidence.cms").read_bytes(), b"stale")
            self.assertEqual((export / "public.json").read_text(), "stale")

    def test_current_incomplete_receipt_is_encrypted_for_diagnosis(self):
        with tempfile.TemporaryDirectory() as tmp:
            private = Path(tmp) / "private"
            private.mkdir(mode=0o700)
            self.receipt(private, outcome="incomplete", error_code="STORAGE_COVERAGE")
            export = Path(tmp) / "export"
            with patch.dict(os.environ, self.JOB, clear=True):
                status, result = sealer.seal(private, export, "failure")
            self.assertEqual(result, 0)
            self.assertEqual(status["error_code"], "STORAGE_COVERAGE")
            self.assertEqual(status["outcome"], "failed")
            self.assertTrue((export / "private-evidence.cms").is_file())
            self.assertFalse(private.exists())

    def test_plaintext_cleanup_failure_blocks_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            private = Path(tmp) / "private"
            private.mkdir(mode=0o700)
            self.receipt(private)
            export = Path(tmp) / "export"
            with patch.dict(os.environ, self.JOB, clear=True), \
                 patch.object(sealer.shutil, "rmtree", side_effect=OSError("denied")):
                status, result = sealer.seal(private, export, "success")
            self.assertEqual((status["error_code"], result), ("CLEANUP", 1))
            self.assertTrue(private.exists())

    def test_oversize_serialized_receipt_becomes_sealable_minimal_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = dict(self.JOB, RUNNER_TEMP=tmp)
            oversized = {"schema": collector.SCHEMA, "read_only": True,
                         "qualification": "not_assessed", "opaque": "x" * collector.MAX_RECEIPT}
            with patch.dict(os.environ, env, clear=True), \
                 patch.object(collector, "collect", return_value=oversized):
                self.assertEqual(collector.main(), 1)
            private = Path(tmp) / collector.PRIVATE_NAME
            receipt = json.loads((private / "inventory.private.json").read_text())
            self.assertEqual(receipt["error_code"], "OVERSIZE")
            self.assertLess((private / "inventory.private.json").stat().st_size, 1024)
            with patch.dict(os.environ, env, clear=True):
                status, result = sealer.seal(private, Path(tmp) / "export", "failure")
            self.assertEqual((status["error_code"], result), ("OVERSIZE", 0))

    def test_required_classes_and_observed_store_are_checked(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp).resolve()
            for name in collector.ROOT_CLASSES:
                (base / name).mkdir()
            profile = base / "profile.json"
            rows = [{"path": str(base / name), "class": name}
                    for name in sorted(collector.ROOT_CLASSES)]
            payload = {"schema": "alopex.parity-selected-inventory-profile/v1",
                       "source_commit": self.JOB["GITHUB_SHA"],
                       "runner_name": self.JOB["RUNNER_NAME"],
                       "coverage_complete": True, "storage_roots": rows}
            profile.write_text(json.dumps(payload))
            profile.chmod(0o600)
            mounts = f"1 0 0:1 / {base / 'windows'} rw - drvfs C: rw"
            observed = {"workspace": [base / "workspace"],
                        "temporary": [base / "temporary"],
                        "cargo": [base / "caches"], "cache": [base / "caches"],
                        "worktrees": [base / "worktrees"],
                        "installations": [base / "installations"]}
            with patch.dict(os.environ, self.JOB, clear=True), \
                 patch.object(collector, "observed_storage_paths", return_value=observed):
                roots = collector.profile_roots(profile, mounts, base / "podman")
                self.assertEqual(len(roots), len(collector.ROOT_CLASSES))
                with self.assertRaisesRegex(collector.Incomplete, "STORAGE_COVERAGE"):
                    collector.profile_roots(profile, mounts, base / "elsewhere")
                for key in ("workspace", "temporary", "cargo", "cache", "worktrees", "installations"):
                    with self.subTest(missing=key):
                        observed[key] = [base / "uncovered"]
                        with self.assertRaisesRegex(collector.Incomplete, "STORAGE_COVERAGE"):
                            collector.profile_roots(profile, mounts, base / "podman")
                        observed[key] = [base / ("caches" if key in ("cargo", "cache") else key)]
                payload["storage_roots"] = rows[:-1]
                profile.write_text(json.dumps(payload))
                with self.assertRaisesRegex(collector.Incomplete, "STORAGE_COVERAGE"):
                    collector.profile_roots(profile, mounts, base / "podman")

    def test_observed_job_and_git_paths_are_source_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            for name in ("workspace", "temporary", "cargo", "cache", "linked"):
                (root / name).mkdir()
            env = {"GITHUB_WORKSPACE": str(root / "workspace"),
                   "RUNNER_TEMP": str(root / "temporary"),
                   "CARGO_HOME": str(root / "cargo"),
                   "XDG_CACHE_HOME": str(root / "cache")}
            result = {"exit": 0, "stdout": f"worktree {root / 'workspace'}\n\nworktree {root / 'linked'}\n"}
            with patch.dict(os.environ, env, clear=True), \
                 patch.object(collector, "command", return_value=result):
                observed = collector.observed_storage_paths()
                self.assertEqual(observed["worktrees"], [root / "workspace", root / "linked"])
            with patch.dict(os.environ, {**env, "CARGO_HOME": str(root / "missing")}, clear=True), \
                 patch.object(collector, "command", return_value=result):
                with self.assertRaisesRegex(collector.Incomplete, "STORAGE_COVERAGE"):
                    collector.observed_storage_paths()

    def test_runtime_store_and_shim_observation_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            graph = Path(tmp).resolve()
            with patch.object(collector.shutil, "which", return_value=None):
                with self.assertRaisesRegex(collector.Incomplete, "RUNTIME"):
                    collector.runtime_observation({"store": {"graphRoot": str(graph)}})
            with self.assertRaisesRegex(collector.Incomplete, "RUNTIME"):
                collector.runtime_observation({"store": {}})

    def test_shim_target_and_service_absence_are_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            target = root / "docker-target"
            target.write_text("#!/bin/sh\n")
            shim = root / "docker"
            shim.symlink_to(target)
            with patch.object(collector.shutil, "which", return_value=str(shim)), \
                 patch.object(collector, "mode_record", side_effect=lambda p: {"path": str(p)}), \
                 patch.object(collector, "service_process_observation",
                              return_value={"status": "same_uid_service_absent_from_proc_snapshot"}):
                result, graph = collector.runtime_observation({"store": {"graphRoot": str(root)}})
            self.assertEqual(result["docker_shim_target"]["path"], str(target))
            self.assertEqual(result["service_process"]["status"],
                             "same_uid_service_absent_from_proc_snapshot")

    def test_network_requires_route_and_listener_facts_but_not_peer(self):
        with patch.object(collector.shutil, "which", return_value="/usr/bin/ip"), \
             patch.object(collector, "command", return_value={"exit": 0, "stdout": "[]"}), \
             patch.object(collector, "bounded_text", return_value="empty"):
            observed = collector.network_observation()
            self.assertEqual(observed["peer_auth"], "not_assessed_no_peer")
            self.assertEqual(observed["tcp"], "empty")
        with patch.object(collector.shutil, "which", return_value=None):
            with self.assertRaisesRegex(collector.Incomplete, "NETWORK"):
                collector.network_observation()

    def test_process_observation_skips_exited_process(self):
        def fake_text(path, _limit):
            if str(path).startswith("/proc/123/"):
                raise collector.Incomplete("STORAGE_READ")
            return "Uid:\t42\t42\t42\t42\n" if path.name == "status" else "read_bytes: 1\n"
        with patch.object(Path, "iterdir", return_value=iter([Path("/proc/123"), Path("/proc/124")])), \
             patch.object(collector.os, "getpid", return_value=999), \
             patch.object(collector.os, "getuid", return_value=42), \
             patch.object(collector, "bounded_text", side_effect=fake_text):
            observed = collector.sibling_observation()
        self.assertEqual(observed["pid"], 124)


if __name__ == "__main__":
    unittest.main()
