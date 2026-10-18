"""Historical launcher invocations must never start an unmeasured build."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class RetiredEntrypointTest(unittest.TestCase):
    def test_refuses_parent_child_and_missing_arguments_without_outputs(self):
        script = Path(__file__).with_name("run_issue577_verification.py").resolve()
        for arguments, child in [([], "0"), (["run", "issue577_"], "0"),
                                 (["run", "issue577_"], "1")]:
            with self.subTest(arguments=arguments, child=child), tempfile.TemporaryDirectory() as root:
                result = subprocess.run(
                    [sys.executable, "-B", str(script), *arguments],
                    cwd=root, env={**os.environ, "CODEX_RESOURCE_CHILD": child},
                    capture_output=True, text=True, timeout=5, check=False,
                )
                self.assertEqual(result.returncode, 125)
                self.assertIn("Refusing retired", result.stderr)
                self.assertIn("per-host", result.stderr)
                self.assertEqual(list(Path(root).iterdir()), [])


if __name__ == "__main__":
    unittest.main()
