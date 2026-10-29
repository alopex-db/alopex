"""Exercise the workflow's cleanup against tiny owned/foreign file fixtures."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]


@unittest.skipUnless(shutil.which("bash"), "workflow cleanup requires bash")
class PublicVerificationCleanupTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="alopex-public-cleanup-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.runner = self.root / "runner"
        self.runner.mkdir()
        workflow = (ROOT / ".github/workflows/public-release-verification.yml").read_text()
        block = workflow.split("      - name: Cleanup public wheel workspace\n", 1)[1]
        block = block.split("        run: |\n", 1)[1].split("\n      - name:", 1)[0]
        self.script = "\n".join(line[10:] if line.startswith("          ") else line
                                for line in block.splitlines())
        # Native inventory/threshold behavior has its own storage tests. Here
        # observe that the final measurement runs after the owned files vanish.
        storage = self.root / "scripts/compatibility/storage.py"
        storage.parent.mkdir(parents=True)
        storage.write_text(
            "import os\nfrom pathlib import Path\n"
            "root=Path(os.environ['RUNNER_TEMP'])\n"
            "assert not (root/'pypi-install-v0.8.16').exists()\n"
            "(root/'measured').write_text('after cleanup')\n"
        )
        self.env = dict(os.environ, RUNNER_TEMP=str(self.runner), VERSION="0.8.16",
                        PYTHONDONTWRITEBYTECODE="1")

    def write(self, name, value=b"fixture"):
        path = self.runner / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
        return path

    def run_cleanup(self, version="0.8.16"):
        return subprocess.run(["bash", "-euo", "pipefail"], input=self.script,
                              text=True, capture_output=True, cwd=self.root,
                              env=dict(self.env, VERSION=version), timeout=10)

    def test_removes_only_owned_outputs_and_keeps_failure_logs(self):
        self.write("pypi-readiness-v0.8.16/wheel.whl")
        self.write("pypi-readiness-v0.8.16/availability.log", b"download failed\n")
        self.write("pypi-install-v0.8.16/alopex/__init__.py")
        self.write("verify-python-vector-api-v0.8.16.py")
        report = self.write("release-verification-v0.8.16/v0.8.16.json", b"original report")
        foreign = self.write("pypi-install-v0.8.15/keep", b"other version")
        unrelated = self.write("unrelated/keep", b"other task")
        result = self.run_cleanup()
        self.assertEqual(result.returncode, 0, result.stderr)
        for name in ("pypi-readiness-v0.8.16", "pypi-install-v0.8.16", "verify-python-vector-api-v0.8.16.py"):
            self.assertFalse((self.runner / name).exists())
        self.assertEqual(report.read_bytes(), b"original report")
        self.assertEqual(foreign.read_bytes(), b"other version")
        self.assertEqual(unrelated.read_bytes(), b"other task")
        self.assertEqual((report.parent / "availability.log").read_bytes(), b"download failed\n")
        self.assertEqual((self.runner / "measured").read_text(), "after cleanup")

    def test_invalid_version_fails_before_touching_outputs(self):
        owned = self.write("pypi-install-v0.8.16/keep")
        result = self.run_cleanup("../../elsewhere")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid version", result.stderr)
        self.assertTrue(owned.exists())
        self.assertFalse((self.runner / "measured").exists())

    def test_missing_outputs_are_allowed(self):
        result = self.run_cleanup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.runner / "measured").exists())


if __name__ == "__main__":
    unittest.main()
