from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
RUNNER = ROOT / "scripts" / "run-performance-python.sh"


class PerformancePythonRunnerTests(unittest.TestCase):
    def test_creates_and_uses_python_311_environment_without_activation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            requirements = temporary / "requirements.txt"
            requirements.write_text("", encoding="utf-8")
            virtualenv = temporary / "venv"
            environment = os.environ.copy()
            environment.update(
                {
                    "ALOPEX_PERFORMANCE_REQUIREMENTS": str(requirements),
                    "ALOPEX_PERFORMANCE_VENV": str(virtualenv),
                    "PYTHON": sys.executable,
                }
            )

            result = subprocess.run(
                [
                    "bash",
                    str(RUNNER),
                    "python",
                    "-c",
                    (
                        "import os, pathlib, sys; "
                        "print(sys.version_info[:2]); "
                        "print(pathlib.Path(sys.prefix).resolve()); "
                        "print(pathlib.Path(os.environ['VIRTUAL_ENV']).resolve())"
                    ),
                ],
                cwd=ROOT,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            lines = result.stdout.strip().splitlines()
            self.assertEqual(lines[-3], "(3, 11)")
            self.assertEqual(lines[-2], str(virtualenv.resolve()))
            self.assertEqual(lines[-1], str(virtualenv.resolve()))

            stamp = virtualenv / ".alopex-performance-requirements.sha256"
            first_digest = stamp.read_text(encoding="utf-8").strip()
            requirements.write_text("# changed\n", encoding="utf-8")
            rerun = subprocess.run(
                ["bash", str(RUNNER), "python", "-c", "print('reused')"],
                cwd=ROOT,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(rerun.returncode, 0, rerun.stderr)
            self.assertEqual(rerun.stdout.strip().splitlines()[-1], "reused")
            second_digest = stamp.read_text(encoding="utf-8").strip()
            self.assertNotEqual(first_digest, second_digest)
            self.assertEqual(
                second_digest,
                hashlib.sha256(requirements.read_bytes()).hexdigest(),
            )

    def test_rejects_python_that_is_not_311(self) -> None:
        result = subprocess.run(
            ["bash", str(RUNNER), "true"],
            cwd=ROOT,
            env={**os.environ, "PYTHON": "/bin/false"},
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("requires Python 3.11", result.stderr)


if __name__ == "__main__":
    unittest.main()
