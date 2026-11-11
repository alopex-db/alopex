"""Focused admission and orchestration checks; never starts Docker."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess
from types import SimpleNamespace

SCRIPT = Path(__file__).resolve().parents[1] / "hosted_test_env_qualification.py"
SPEC = importlib.util.spec_from_file_location("hosted_test_env_qualification", SCRIPT)
qualification = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(qualification)


class Inputs(unittest.TestCase):
    def test_asset_run_head_mismatch_prevents_archive_download(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory)
            run = {"status": "completed", "conclusion": "success",
                   "head_sha": "0" * 40, "run_attempt": 1,
                   "event": "workflow_dispatch"}
            with patch.object(qualification, "gh_api", return_value=run) as api, \
                 self.assertRaises(qualification.Failure) as caught:
                qualification.acquire_assets(private, private / "assets")
            self.assertEqual(caught.exception.code, "IDENTITY")
            self.assertEqual(api.call_count, 1)
            self.assertFalse((private / "assets").exists())

    def test_artifact_zip_hash_mismatch_prevents_extraction(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory)
            run = {"status": "completed", "conclusion": "success",
                   "head_sha": qualification.ASSET_RUN_HEAD, "run_attempt": 1,
                   "event": "workflow_dispatch"}
            jobs = {"total_count": 1, "jobs": [{"conclusion": "success",
                                                "steps": [{"conclusion": "success"}]}]}
            listing = {"total_count": 1, "artifacts": [{
                "id": qualification.ASSET_ARTIFACT_ID, "name": "native-product-assets",
                "digest": "sha256:" + qualification.ASSET_ZIP_DIGEST,
                "workflow_run": {"head_sha": qualification.ASSET_RUN_HEAD},
                "expired": False, "size_in_bytes": 100}]}
            with patch.object(qualification, "gh_api",
                              side_effect=[run, jobs, listing, b"wrong zip"]) as api, \
                 self.assertRaises(qualification.Failure) as caught:
                qualification.acquire_assets(private, private / "assets")
            self.assertEqual(caught.exception.code, "IDENTITY")
            self.assertEqual(api.call_count, 4)
            self.assertFalse((private / "assets").exists())

    def test_admission_retains_docker_info_and_serializes_private_facts(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            docker_root = base / "docker"
            docker_root.mkdir()
            workspace = base / "workspace" / "checkout"
            workspace.mkdir(parents=True)
            runner_temp = base / "runner-temp"
            runner_temp.mkdir()
            docker_info = {"ServerVersion": qualification.EXPECTED_DOCKER_VERSION,
                           "OSType": "linux", "Architecture": "x86_64",
                           "CgroupVersion": "2", "SecurityOptions":
                           ["name=apparmor", "name=seccomp,profile=builtin"],
                           "NCPU": 2, "DockerRootDir": str(docker_root)}
            def observed(path):
                name = str(path)
                if "config-" in name:
                    value = "CONFIG_TASK_IO_ACCOUNTING=y"
                elif name.endswith("/lsm"):
                    value = "apparmor"
                elif name.endswith("/enabled"):
                    value = "Y"
                else:
                    value = "private"
                return {"status": "read", "value": value}
            def command(args):
                if args[1:] == ["context", "show"]:
                    return "default"
                if args[1:3] == ["context", "inspect"]:
                    return json.dumps([{"Endpoints": {"docker": {"Host": "unix:///socket"}}}])
                return json.dumps(docker_info)
            original_lstat = Path.lstat
            original_read_text = Path.read_text
            def metadata(path, *args, **kwargs):
                info = original_lstat(path, *args, **kwargs)
                if path == docker_root:
                    return SimpleNamespace(st_mode=info.st_mode & ~0o022,
                                           st_uid=0, st_gid=0, st_dev=info.st_dev,
                                           st_ino=info.st_ino)
                return info
            def memory(path, *args, **kwargs):
                if str(path) == "/sys/fs/cgroup/memory.max":
                    return "max"
                return original_read_text(path, *args, **kwargs)
            with patch.dict("os.environ", {"RUNNER_ENVIRONMENT": "github-hosted",
                                         "GITHUB_WORKSPACE": str(workspace),
                                         "RUNNER_TEMP": str(runner_temp)}), \
                 patch.object(qualification.sys, "platform", "linux"), \
                 patch.object(qualification.platform, "machine", return_value="x86_64"), \
                 patch.object(qualification, "read_text", side_effect=observed), \
                 patch.object(qualification, "sibling_observation", return_value={"status": "observed"}), \
                 patch.object(qualification, "checked_command", side_effect=command), \
                 patch.object(qualification.os, "sched_getaffinity", return_value={0, 1}, create=True), \
                 patch.object(qualification.os, "statvfs",
                              return_value=SimpleNamespace(f_bavail=50_000_000_000, f_frsize=1)), \
                 patch.object(Path, "home", return_value=base), \
                 patch.object(Path, "lstat", metadata), \
                 patch.object(Path, "read_text", memory), \
                 patch.object(qualification, "Budget") as budget:
                budget.return_value.used.return_value = 100
                facts = qualification.admission_facts()
            self.assertEqual(facts["docker_info"]["DockerRootDir"], str(docker_root))
            json.dumps(facts)

    def test_checkout_head_must_match_expected_controller_sha(self):
        with patch.dict("os.environ", {"EXPECTED_CONTROLLER_SHA": "a" * 40,
                                    "GITHUB_SHA": "b" * 40}), \
             patch.object(qualification.shutil, "which", return_value="/usr/bin/docker"), \
             patch.object(qualification, "trusted_executable"), \
             patch.object(qualification, "source_identity",
                          return_value={"controller_commit": "b" * 40,
                                        "controller_digest": qualification.CONTROLLER_DIGEST}), \
             self.assertRaises(qualification.Failure) as caught:
            qualification.verified_controller()
        self.assertEqual(caught.exception.code, "IDENTITY")

    def test_path_shadowed_docker_is_rejected_before_source_use(self):
        with patch.object(qualification.shutil, "which", return_value="/tmp/docker"), \
             patch.object(qualification, "source_identity") as identity, \
             self.assertRaises(qualification.Failure) as caught:
            qualification.verified_controller()
        self.assertEqual(caught.exception.code, "PREREQUISITE")
        identity.assert_not_called()

    def test_exact_a415_bundle_and_policy_candidates_pass_static_identity(self):
        bundle = Path(__file__).resolve().parents[4] / (
            ".tmp/v0817-a415-native-full-assets/native-product-run-43110932395")
        if not bundle.is_dir():
            self.skipTest("task-private audited bundle is unavailable")
        manifest = qualification.verified_inputs(bundle)
        self.assertEqual(manifest, bundle / "assets/assets.json")

    def test_manifest_digest_mismatch_stops_before_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "assets").mkdir()
            (root / "assets/assets.json").write_text("{}")
            with patch.object(qualification, "verify_assets") as verify, \
                 self.assertRaises(qualification.Failure) as caught:
                qualification.verified_inputs(root)
            self.assertEqual(caught.exception.code, "IDENTITY")
            verify.assert_not_called()

    def test_controller_digest_mismatch_stops_before_asset_use(self):
        with patch.object(qualification, "source_identity",
                          return_value={"controller_digest": "0" * 64}), \
             patch.object(qualification, "verify_assets") as verify, \
             self.assertRaises(qualification.Failure) as caught:
            qualification.verified_inputs(Path("unused"))
        self.assertEqual(caught.exception.code, "IDENTITY")
        verify.assert_not_called()


class Sequence(unittest.TestCase):
    def test_checkout_growth_must_stay_within_one_gigabyte(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            root = base / "workspace"
            root.mkdir()
            workspace = root / "checkout"
            workspace.mkdir()
            docker = base / "docker"
            docker.mkdir()
            private = base / "private"
            private.mkdir()
            receipt = private / "checkout-preflight.private.json"
            receipt.write_text(json.dumps({"workspace_root": str(root),
                                           "allocated_bytes": 1024,
                                           "aggregate_allocated_bytes": 2048,
                                           "roots": [str(root), str(docker)],
                                           "docker_root": str(docker),
                                           "expected_controller_sha": "a" * 40}))
            receipt.chmod(0o600)
            def allocation(kib):
                def measured(args, **kwargs):
                    path = args[-1]
                    size = kib if path == str(root) else 1
                    return subprocess.CompletedProcess(args, 0,
                                                       f"{size}\t{path}\n", "")
                return measured
            with patch.dict("os.environ", {"GITHUB_WORKSPACE": str(workspace),
                                        "EXPECTED_CONTROLLER_SHA": "a" * 40}), \
                 patch.object(qualification, "trusted_executable"), \
                 patch.object(qualification.subprocess, "run",
                              side_effect=allocation(100)):
                qualification.checkout_growth(private)
            with patch.dict("os.environ", {"GITHUB_WORKSPACE": str(workspace),
                                        "EXPECTED_CONTROLLER_SHA": "a" * 40}), \
                 patch.object(qualification, "trusted_executable"), \
                 patch.object(qualification.subprocess, "run",
                              side_effect=allocation(1_000_000)), \
                 self.assertRaises(qualification.Failure) as caught:
                qualification.checkout_growth(private)
            self.assertEqual(caught.exception.code, "CAPACITY")
            def docker_growth(args, **kwargs):
                path = args[-1]
                size = 100 if path == str(root) else 1_000_000
                return subprocess.CompletedProcess(args, 0,
                                                   f"{size}\t{path}\n", "")
            with patch.dict("os.environ", {"GITHUB_WORKSPACE": str(workspace),
                                        "EXPECTED_CONTROLLER_SHA": "a" * 40}), \
                 patch.object(qualification, "trusted_executable"), \
                 patch.object(qualification.subprocess, "run", side_effect=docker_growth), \
                 self.assertRaises(qualification.Failure) as caught:
                qualification.checkout_growth(private)
            self.assertEqual(caught.exception.code, "CAPACITY")

    def test_workflow_admits_before_candidate_download_and_execution(self):
        workflow = Path(__file__).resolve().parents[3] / (
            ".github/workflows/alopex-performance.yml")
        source = workflow.read_text()
        self.assertIn("inventory:\n    if: ${{ !inputs.qualification_mode }}", source)
        self.assertIn("qualification:\n    if: inputs.qualification_mode", source)
        source = source.split("  qualification:\n", 1)[1]
        positions = [source.index(token) for token in (
            "Bound checkout before source acquisition", "uses: actions/checkout@v4",
            "Admit this runner before candidate acquisition",
            "Retrieve exact candidate assets", "Qualify on this runner")]
        self.assertEqual(positions, sorted(positions))

    def test_qualification_uses_a_default_branch_registered_workflow_path(self):
        repo = Path(__file__).resolve().parents[3]
        result = subprocess.run(["git", "show",
                                 "origin/main:.github/workflows/alopex-performance.yml"],
                                cwd=repo, capture_output=True, text=True)
        if result.returncode:
            self.skipTest("default-branch ref is unavailable in this checkout")
        self.assertIn("workflow_dispatch:", result.stdout)
        self.assertTrue((repo / ".github/workflows/alopex-performance.yml").is_file())

    def test_admission_precedes_profile_and_every_envctl_operation(self):
        events = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "assets").mkdir()
            manifest = root / "assets/assets.json"
            manifest.write_text("{}")
            profile = root / "profile.json"
            profile.write_text(json.dumps({"private_output": str(root), "run_id": "test"}))
            def inputs(*args):
                events.append("inputs")
                return manifest
            def facts():
                events.append("admission")
                raise qualification.Failure("MEASUREMENT")
            with patch.dict("os.environ", {"RUNNER_TEMP": str(root),
                                         "ALOPEX_804_ASSETS": str(root)}), \
                 patch.object(qualification, "verified_controller", side_effect=lambda: events.append("controller")), \
                 patch.object(qualification, "checkout_growth", side_effect=lambda path: events.append("checkout")), \
                 patch.object(qualification, "admission_facts", side_effect=facts), \
                 patch.object(qualification, "create_profile") as make_profile, \
                 patch.object(qualification, "execute") as execute, \
                 patch.object(qualification, "encrypt_evidence") as encrypt, \
                 patch("builtins.print"):
                self.assertEqual(qualification.main(["--admission-only"]), 1)
            self.assertEqual(events, ["controller", "checkout", "admission"])
            make_profile.assert_not_called()
            execute.assert_not_called()
            encrypt.assert_called_once()

    def test_fixed_sequence_includes_all_acceptance_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / "profile.json"
            profile.write_text(json.dumps({"private_output": str(root), "run_id": "test"}))
            run = root / "test"
            run.mkdir()
            accepted = {"status": "passed", "cases": [
                {"id": case, "status": "passed"} for case in qualification.CASES]}
            (run / "public.json").write_text(json.dumps(accepted))
            calls = []
            with patch.object(qualification, "envctl", side_effect=lambda path, *args: calls.append(args)), \
                 patch.object(qualification, "try_collect_clean", return_value=[]) as cleanup:
                self.assertEqual(qualification.execute(profile), accepted)
            self.assertEqual(calls, [
                ("doctor", "--phase", "preflight"), ("fetch",), ("build",),
                ("doctor", "--phase", "runtime"), ("collect", "--probe"),
                ("clean", "--probe"), ("accept",), ("collect",),
                ("export-public",), ("clean",)])
            self.assertEqual(cleanup.call_count, 2)

    def test_incomplete_cases_prevent_final_clean(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / "profile.json"
            profile.write_text(json.dumps({"private_output": str(root), "run_id": "test"}))
            run = root / "test"
            run.mkdir()
            (run / "public.json").write_text(json.dumps({"status": "passed", "cases": []}))
            calls = []
            with patch.object(qualification, "envctl", side_effect=lambda path, *args: calls.append(args)), \
                 patch.object(qualification, "try_collect_clean", return_value=[]):
                with self.assertRaises(qualification.Failure) as caught:
                    qualification.execute(profile)
            self.assertEqual(caught.exception.code, "INCOMPLETE")
            self.assertNotIn(("clean",), calls)

    def test_cleanup_error_is_recorded_and_prevents_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / "profile.json"
            profile.write_text(json.dumps({"private_output": str(root), "run_id": "test"}))
            run = root / "test"
            run.mkdir()
            accepted = {"status": "passed", "cases": [
                {"id": case, "status": "passed"} for case in qualification.CASES]}
            (run / "public.json").write_text(json.dumps(accepted))
            error = {"phase": "probe", "error_code": "CLEANUP", "detail": "private"}
            with patch.object(qualification, "envctl"), \
                 patch.object(qualification, "try_collect_clean", side_effect=[[error], []]):
                with self.assertRaises(qualification.Failure) as caught:
                    qualification.execute(profile)
            self.assertEqual(caught.exception.code, "CLEANUP")
            self.assertEqual(json.loads((root / "cleanup-errors.private.json").read_text()), [error])

    def test_primary_failure_survives_cleanup_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / "profile.json"
            profile.write_text(json.dumps({"private_output": str(root), "run_id": "test"}))
            error = {"phase": "accept", "error_code": "CLEANUP", "detail": "private"}
            def fail_accept(path, *args):
                if args == ("accept",):
                    raise qualification.Failure("CASE", "original")
            with patch.object(qualification, "envctl", side_effect=fail_accept), \
                 patch.object(qualification, "try_collect_clean", side_effect=[[], [error]]):
                with self.assertRaises(qualification.Failure) as caught:
                    qualification.execute(profile)
            self.assertEqual(caught.exception.code, "CASE")
            self.assertEqual(json.loads((root / "cleanup-errors.private.json").read_text()), [error])

    def test_plaintext_archive_is_removed_after_successful_encryption(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            private.mkdir()
            (private / "detail.json").write_text('{"private": true}')
            exported = root / "export"
            def encrypted(args, **kwargs):
                exported.mkdir(exist_ok=True)
                (exported / "private-evidence.cms").write_bytes(b"ciphertext")
                return subprocess.CompletedProcess(args, 0, b"", b"")
            with patch.object(qualification, "EXPORT", exported), \
                 patch.object(qualification.subprocess, "run", side_effect=encrypted):
                qualification.encrypt_evidence(private, {"outcome": "failed"})
            self.assertFalse((private / "evidence.tar.gz").exists())
            self.assertEqual(json.loads((exported / "public.json").read_text()),
                             {"outcome": "failed"})


if __name__ == "__main__":
    unittest.main()
