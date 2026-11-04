"""Small RC cleanup fixtures and workflow guard coverage; no product build."""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/release/cleanup_rc_build.py"
spec = importlib.util.spec_from_file_location("cleanup_rc", SCRIPT)
cleanup_rc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup_rc)


class RcStorageTests(unittest.TestCase):
    def setUp(self):
        fixture = tempfile.TemporaryDirectory(prefix="alopex-rc-storage-")
        self.addCleanup(fixture.cleanup)
        self.root = Path(fixture.name).resolve()
        self.workspace = self.root / "workspace"
        self.temporary = self.root / "runner"
        self.workspace.mkdir()
        self.temporary.mkdir()
        self.target = self.workspace / "target"

    def put(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fixture")
        return path

    def test_core_cleanup_preserves_source_assets_and_receipts(self):
        self.put(self.target / "release/binary")
        self.put(self.workspace / "artifacts/parser/native-smoke/library")
        retained = [self.put(self.workspace / name) for name in (
            "Cargo.toml", "artifacts/cli", "artifacts/parser/qualified.tar.gz",
            "artifacts/rc-storage/cli.jsonl",
        )]
        cleanup_rc.cleanup(self.workspace, self.temporary, self.target, "core")
        self.assertFalse(self.target.exists())
        self.assertFalse((self.workspace / "artifacts/parser/native-smoke").exists())
        self.assertTrue(all(p.read_bytes() == b"fixture" for p in retained))

    def test_crate_cleanup_keeps_qualified_bytes_even_after_failed_upload(self):
        for name in ("alopex-release-source", "alopex-release-parser-assets",
                     "alopex-crate-assets/.candidate-index"):
            self.put(self.temporary / name / "regenerable")
        self.put(self.target / "package/copied.crate")
        retained = [self.put(self.temporary / name) for name in (
            "alopex-crate-assets/qualified.crate",
            "alopex-crate-assets/alopex-core-publish-metadata.json", "unrelated/keep",
        )]
        cleanup_rc.cleanup(self.workspace, self.temporary, self.target, "crates")
        self.assertFalse(self.target.exists())
        self.assertFalse((self.temporary / "alopex-release-source").exists())
        self.assertFalse((self.temporary / "alopex-release-parser-assets").exists())
        self.assertFalse((self.temporary / "alopex-crate-assets/.candidate-index").exists())
        self.assertTrue(all(p.read_bytes() == b"fixture" for p in retained))
        cleanup_rc.cleanup(self.workspace, self.temporary, self.target, "crates")

    def test_bad_target_or_kind_changes_nothing(self):
        keep = self.put(self.target / "keep")
        for target, kind in ((self.workspace, "core"), (self.target, "unknown")):
            with self.assertRaises(ValueError):
                cleanup_rc.cleanup(self.workspace, self.temporary, target, kind)
            self.assertTrue(keep.exists())

    def test_target_symlink_is_unlinked_without_deleting_its_destination(self):
        keep = self.put(self.temporary / "foreign/keep")
        self.target.symlink_to(keep.parent, target_is_directory=True)
        cleanup_rc.cleanup(self.workspace, self.temporary, self.target, "core")
        self.assertFalse(self.target.is_symlink())
        self.assertEqual(keep.read_bytes(), b"fixture")

    def test_symlink_parent_refused_before_any_cleanup(self):
        keep = self.put(self.target / "keep")
        external = self.put(self.temporary / "foreign/parser/native-smoke/keep")
        (self.workspace / "artifacts").symlink_to(self.temporary / "foreign", target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink cleanup parent"):
            cleanup_rc.cleanup(self.workspace, self.temporary, self.target, "core")
        self.assertTrue(keep.exists())
        self.assertTrue(external.exists())

    def test_non_hosted_cli_refuses_before_reading_any_cleanup_paths(self):
        result = subprocess.run([sys.executable, str(SCRIPT), "--kind", "core"],
                                env=dict(os.environ, RUNNER_ENVIRONMENT="self-hosted"),
                                capture_output=True, text=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("isolated GitHub-hosted runner", result.stderr)

    @unittest.skipUnless(shutil.which("bash"), "strict staging uses bash")
    def test_strict_staging_guard_preserves_commands_target_and_failures(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
        block = workflow.split("- name: Build and smoke staged parser through strict default path", 1)[1]
        script = textwrap.dedent(block.split("        run: |\n", 1)[1].split("      - name:", 1)[0])
        stage = self.temporary / "alopex-release-source"
        stage.mkdir()
        tools = self.root / "bin"
        tools.mkdir()
        (tools / "python").symlink_to(sys.executable)
        cargo = tools / "cargo"
        cargo.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\nfrom pathlib import Path\n"
            "assert Path.cwd() == Path(os.environ['RELEASE_STAGE'])\n"
            "assert os.environ['CARGO_TARGET_DIR'].endswith('/workspace/target')\n"
            "assert 'ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD' not in os.environ\n"
            "assert 'NIM_SQL_PARSER_LIB_DIR' not in os.environ\n"
            "with open(os.environ['COMMAND_LOG'], 'a') as stream:\n"
            "    stream.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            "sys.exit(17 if os.environ['FAIL_BUILD'] == '1' else 0)\n",
            encoding="utf-8",
        )
        cargo.chmod(0o755)
        guard = self.workspace / "scripts/compatibility/storage.py"
        guard.parent.mkdir(parents=True)
        # The watchdog owns inventory/stop semantics in its existing tests.
        # This shim verifies that the actual workflow enters that wrapper and
        # propagates the bounded shell's native status without another build.
        guard.write_text(
            "import subprocess, sys\nfrom pathlib import Path\n"
            "assert sys.argv[sys.argv.index('--additional-bytes') + 1] == '20000000000'\n"
            "Path('guard-entered').write_bytes(b'yes')\n"
            "sys.exit(subprocess.run(sys.argv[sys.argv.index('--') + 1:]).returncode)\n",
            encoding="utf-8",
        )
        for fail in (False, True):
            log = self.root / f"commands-{fail}.jsonl"
            result = subprocess.run(
                ["bash", "-euo", "pipefail"], input=script, text=True,
                capture_output=True, cwd=self.workspace, timeout=10,
                env=dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"],
                         RELEASE_STAGE=str(stage), CARGO_TARGET_DIR=str(self.target),
                         COMMAND_LOG=str(log), FAIL_BUILD=str(int(fail)),
                         ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD="1", NIM_SQL_PARSER_LIB_DIR="forbidden"),
            )
            self.assertEqual(result.returncode, 17 if fail else 0, result.stderr)
            self.assertTrue((self.workspace / "guard-entered").is_file())
            commands = log.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(commands), 1 if fail else 3)
            if not fail:
                self.assertIn("exposes_the_nim_wire_contract_version", commands[1])
                self.assertIn("distinct_on_crosses_the_nim_messagepack_boundary", commands[2])

    def test_core_and_crate_builds_have_guards_shared_target_and_always_cleanup(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
        self.assertIn("CARGO_TARGET_DIR: ${{ github.workspace }}/target", workflow)
        self.assertIn("CARGO_BUILD_JOBS: '2'", workflow)
        self.assertIn("CARGO_INCREMENTAL: '0'", workflow)
        for job, end, minimum in (("build-release", "create-release", 3),
                                  ("package-crates", "qualify-python", 4)):
            block = workflow.split(f"\n  {job}:\n", 1)[1].split(f"\n  {end}:\n", 1)[0]
            self.assertIn("timeout-minutes: 90", block)
            self.assertNotIn("Swatinem/rust-cache", block)
            self.assertGreaterEqual(block.count("scripts/compatibility/storage.py"), minimum)
            self.assertIn("--additional-bytes 20000000000 --", block)
            cleanup = block.split("- name: Clean regenerable", 1)[1]
            self.assertIn("if: always()", cleanup)
            self.assertIn("after.jsonl", cleanup)
            self.assertIn("actions/upload-artifact@v4", cleanup)
        self.assertNotIn("alopex-release-strict-target", workflow)
        self.assertNotIn("alopex-release-package-target", workflow)
        self.assertIn('--target-dir "${CARGO_TARGET_DIR}"', workflow)


if __name__ == "__main__":
    unittest.main()
