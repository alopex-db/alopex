"""Bounded release orchestration fixtures; no Docker, network or product build."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.release import bounded_maturin as maturin
from scripts.release import cleanup_rc_build as cleanup
from scripts.release import release_storage as monitor
from scripts.release import verify_python_rc as smoke

ROOT = Path(__file__).resolve().parents[3]


class DockerInventoryTests(unittest.TestCase):
    def setUp(self):
        self.host = patch.dict(os.environ, RUNNER_ENVIRONMENT="github-hosted")
        self.platform = patch.object(monitor.sys, "platform", "linux")
        self.host.start()
        self.platform.start()
        self.addCleanup(self.host.stop)
        self.addCleanup(self.platform.stop)

    def test_counts_both_daemon_stores_in_actual_allocated_bytes(self):
        with patch.object(monitor.subprocess, "check_output", side_effect=[
            '"/var/lib/docker"', '11\t/var/lib/docker\n17\t/var/lib/containerd\n',
        ]) as command, patch.object(monitor.subprocess, "run", return_value=SimpleNamespace(returncode=0)):
            result = monitor.docker_inventory()
        self.assertEqual(result["allocated_bytes"], 28)
        self.assertIn("--one-file-system", command.call_args.args[0])
        self.assertIn("--block-size=1", command.call_args.args[0])

    def test_absent_containerd_is_distinct_from_permission_failure(self):
        for status, stderr, succeeds in ((1, "", True), (1, "password required", False), (2, "", False)):
            with self.subTest(status=status, stderr=stderr), \
                 patch.object(monitor.subprocess, "run", return_value=SimpleNamespace(returncode=status, stderr=stderr)), \
                 patch.object(monitor.subprocess, "check_output", side_effect=['"/var/lib/docker"', '11\t/var/lib/docker\n']):
                if succeeds:
                    self.assertEqual(monitor.docker_inventory()["allocated_bytes"], 11)
                else:
                    with self.assertRaises(RuntimeError):
                        monitor.docker_inventory()

    def test_partial_malformed_or_failed_measurements_are_never_zero(self):
        outputs = ('', '11\t/var/lib/docker\n', '-1\t/var/lib/docker\n17\t/var/lib/containerd\n',
                   subprocess.CalledProcessError(1, ['du']))
        for measured in outputs:
            with self.subTest(measured=str(measured)), \
                 patch.object(monitor.subprocess, "run", return_value=SimpleNamespace(returncode=0)), \
                 patch.object(monitor.subprocess, "check_output", side_effect=['"/var/lib/docker"', measured]):
                with self.assertRaises((RuntimeError, subprocess.CalledProcessError)):
                    monitor.docker_inventory()

    def test_unknown_root_and_nonhosted_machine_are_refused(self):
        with patch.object(monitor.subprocess, "check_output", return_value='"/unreviewed"'):
            with self.assertRaisesRegex(RuntimeError, "unreviewed"):
                monitor.docker_inventory()
        with patch.dict(os.environ, RUNNER_ENVIRONMENT="self-hosted"):
            with self.assertRaisesRegex(RuntimeError, "isolated"):
                monitor.docker_inventory()

    def test_docker_bytes_participate_in_existing_inclusive_40gb_stop(self):
        def run_shared_guard():
            monitor.storage.check()
        original = monitor.storage.inventory
        with patch.object(monitor.storage, "inventory", return_value={"allocated_bytes": 39_999_999_999}) as inventory, \
             patch.object(monitor, "docker_inventory", return_value={"allocated_bytes": 1}), \
             patch.object(monitor.storage, "main", side_effect=run_shared_guard):
            with self.assertRaisesRegex(RuntimeError, "reached stop"):
                monitor.main(["--docker", "--log", "unused"])
            self.assertIs(monitor.storage.inventory, inventory)
            self.assertIn(Path(os.environ.get("RUSTUP_HOME", str(Path.home() / ".rustup"))), inventory.call_args.args[0])
        self.assertIs(monitor.storage.inventory, original)


class PythonReleaseTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="alopex-python-rc-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.workspace = self.root / "workspace"
        self.runner = self.root / "runner"
        self.workspace.mkdir()
        self.runner.mkdir()
        self.environment = patch.dict(os.environ, {
            "GITHUB_WORKSPACE": str(self.workspace), "RUNNER_TEMP": str(self.runner),
            "CARGO_TARGET_DIR": str(self.workspace / "target"),
            "RUNNER_ENVIRONMENT": "github-hosted", "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1",
            "INPUT_TOKEN": "test-secret-not-for-evidence",
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def put(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fixture")
        return path

    def test_action_profiles_keep_existing_targets_versions_and_repair(self):
        for kind, command in (("linux", "build"), ("macos", "build"), ("windows", "build"), ("sdist", "sdist")):
            env = maturin.action_environment(kind, "123-1")
            self.assertEqual(env["INPUT_COMMAND"], command)
            self.assertEqual(env["INPUT_MATURIN-VERSION"], "1.14.1")
            self.assertIn("--out dist", env["INPUT_ARGS"])
            self.assertEqual(env["INPUT_SCCACHE"], "false")
        linux = maturin.action_environment("linux", "123-1")
        self.assertEqual(linux["INPUT_MANYLINUX"], "auto")
        self.assertEqual(linux["INPUT_TARGET"], "x86_64")
        self.assertEqual(linux["INPUT_CONTAINER"], maturin.IMAGE)
        self.assertIn("--init", linux["INPUT_DOCKER-OPTIONS"])
        self.assertIn("--auditwheel repair", maturin.action_environment("macos", "123-1")["INPUT_ARGS"])

    def test_pinned_node_action_is_inside_guard_and_preserves_native_exit(self):
        with patch.object(maturin.sys, "platform", "win32"), \
             patch.object(maturin, "output", side_effect=[maturin.ACTION_SHA, "v24.1.0", "a" * 40]), \
             patch.object(maturin.subprocess, "run", side_effect=[SimpleNamespace(returncode=0), SimpleNamespace(returncode=17)]) as run:
            self.assertEqual(maturin.main(["--kind", "windows"]), 17)
        bounded = run.call_args.args[0]
        self.assertTrue(bounded[1].endswith("release_storage.py"))
        self.assertEqual(bounded[bounded.index("--") + 1], "node")
        self.assertEqual(Path(bounded[-1]), self.workspace / ".maturin-action" / "dist" / "index.js")
        receipt = (self.workspace / "artifacts/python-release/maturin.json").read_text()
        self.assertEqual(json.loads(receipt)["build_exit"], 17)
        self.assertNotIn("test-secret", receipt)

    def test_wrong_action_or_runtime_cannot_start_build(self):
        for output in (["wrong"], [maturin.ACTION_SHA, "v22.0.0"]):
            with self.subTest(output=output), patch.object(maturin.sys, "platform", "win32"), \
                 patch.object(maturin, "output", side_effect=output), \
                 patch.object(maturin.subprocess, "run", return_value=SimpleNamespace(returncode=0)) as run:
                with self.assertRaises(RuntimeError):
                    maturin.main(["--kind", "windows"])
                self.assertLessEqual(run.call_count, 1)  # only the read-only git diff may run

    def test_interrupted_manylinux_action_still_reclaims_owned_container(self):
        with patch.object(maturin.sys, "platform", "linux"), \
             patch.object(maturin, "output", side_effect=[maturin.ACTION_SHA, "v24.1.0", "a" * 40, "", "", ""]), \
             patch.object(maturin.subprocess, "run", side_effect=[SimpleNamespace(returncode=0), KeyboardInterrupt]), \
             patch.object(maturin, "cleanup_docker") as reclaim:
            with self.assertRaises(KeyboardInterrupt):
                maturin.main(["--kind", "linux"])
        reclaim.assert_called_once_with(self.workspace, "123-1", False)
        self.assertEqual(json.loads((self.workspace / "artifacts/python-release/maturin.json").read_text())["cleanup"], "success")

    def test_foreign_container_is_not_removed(self):
        with patch.object(maturin, "output", side_effect=["id", "other-run"]), \
             patch.object(maturin.subprocess, "run") as run:
            with self.assertRaisesRegex(RuntimeError, "another run"):
                maturin.cleanup_docker(self.workspace, "123-1", False)
        run.assert_not_called()

    def test_only_new_image_and_owned_container_are_removed(self):
        for existed in (False, True):
            with self.subTest(existed=existed), \
                 patch.object(maturin, "output", side_effect=["id", "123-1", "image"]), \
                 patch.object(maturin.subprocess, "run", return_value=SimpleNamespace(returncode=0)) as run:
                maturin.cleanup_docker(self.workspace, "123-1", existed)
                self.assertEqual(run.call_args_list[0].args[0], ["docker", "rm", "--force", "alopex-python-123-1"])
                self.assertEqual(run.call_count, 1 if existed else 2)
                if not existed:
                    self.assertEqual(run.call_args.args[0], ["docker", "image", "rm", maturin.IMAGE])

    def test_smoke_preserves_public_scenarios_and_windows_external_cwd(self):
        self.put(self.workspace / "dist/fixture.whl")
        for kind in ("linux", "macos", "windows"):
            with self.subTest(kind=kind), patch.object(smoke.subprocess, "run") as run:
                smoke.main(["--kind", kind])
                self.assertEqual(run.call_count, 5 if kind == "macos" else 4)
                calls = run.call_args_list
                self.assertIn("--only-binary=:all:", calls[1].args[0])
                self.assertTrue(calls[3].args[0][-1].endswith("verify_python_vector_api.py"))
                self.assertEqual(calls[3].kwargs["cwd"], self.runner if kind == "windows" else self.workspace)
                self.assertTrue(all(c.kwargs["check"] for c in calls))
                if kind == "macos":
                    self.assertEqual(calls[4].args[0][-1], "crates/alopex-py/tests/test_sql.py")

    def test_failed_wheel_install_stops_before_api_smoke(self):
        self.put(self.workspace / "dist/fixture.whl")
        with patch.object(smoke.subprocess, "run", side_effect=[None, None, subprocess.CalledProcessError(9, ['pip'])]) as run:
            with self.assertRaises(subprocess.CalledProcessError):
                smoke.main(["--kind", "windows"])
        self.assertEqual(run.call_count, 3)

    def test_missing_or_ambiguous_wheel_does_not_create_venv(self):
        with patch.object(smoke.subprocess, "run") as run:
            with self.assertRaisesRegex(RuntimeError, "one primary wheel"):
                smoke.main(["--kind", "linux"])
            self.put(self.workspace / "dist/one.whl")
            self.put(self.workspace / "dist/two.whl")
            with self.assertRaisesRegex(RuntimeError, "one primary wheel"):
                smoke.main(["--kind", "linux"])
            run.assert_not_called()

    def test_python_cleanup_preserves_qualified_distributions_and_receipts(self):
        target = self.workspace / "target"
        for name in ("target", ".core-parser-assets", ".release-tools", ".maturin-action"):
            self.put(self.workspace / name / "generated")
        for name in ("alopex-wheel-api-smoke", "alopex-wheel-clean-venv"):
            self.put(self.runner / name / "generated")
        retained = [self.put(self.workspace / name) for name in (
            "dist/qualified.whl", "dist/qualified.tar.gz", "artifacts/python-release/maturin.json", "Cargo.toml",
        )]
        cleanup.cleanup(self.workspace, self.runner, target, "python")
        self.assertFalse(target.exists())
        self.assertFalse((self.runner / "alopex-wheel-api-smoke").exists())
        self.assertTrue(all(path.read_bytes() == b"fixture" for path in retained))

    def test_workflows_guard_all_primary_profiles_and_preserve_artifacts(self):
        text = (ROOT / ".github/workflows/alopex-py-release.yml").read_text(encoding="utf-8")
        for kind, end in (("linux", "macos"), ("macos", "windows"), ("windows", "sdist"), ("sdist", "candidate-release")):
            block = text.split(f"\n  {kind}:\n", 1)[1].split(f"\n  {end}:\n", 1)[0]
            self.assertIn(maturin.ACTION_SHA, block)
            self.assertIn(f"bounded_maturin.py --kind {kind}", block)
            self.assertIn("cleanup_rc_build.py --kind python", block)
            self.assertIn("after.jsonl", block)
            self.assertIn("if: always()", block)
            self.assertIn("path: dist/*", block)
            self.assertNotIn("uses: PyO3/maturin-action", block)
        linux = text.split("\n  linux:\n", 1)[1].split("\n  macos:\n", 1)[0]
        self.assertEqual(linux.count("release_storage.py --docker"), 4)
        self.assertIn("steps.manylinux_cleanup.outcome == 'success'", linux)
        self.assertIn("CARGO_TARGET_DIR: ${{ github.workspace }}/target", text)


if __name__ == "__main__":
    unittest.main()
