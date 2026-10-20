import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "report", ROOT / "scripts/release/verify-release/report.py"
)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


class ScenarioTests(unittest.TestCase):
    def test_successful_smoke_cannot_be_promoted_to_scenario_success(self):
        payload = {
            "outcome": "success",
            "started_at": "now",
            "completed_at": "later",
            "version": "0.8.16",
            "suite": "scenarios",
            "failure_stage": None,
            "identity": {
                "run_id": "1",
                "run_attempt": 1,
                "commit": "a" * 40,
                "tag": "alopex-py-v0.8.16",
                "run_url": "https://example.invalid/run/1",
                "responsibility": "test",
            },
            "steps": [{"name": "import", "status": "success"}],
        }
        with self.assertRaisesRegex(SystemExit, "complete suite"):
            REPORT.validate_report_payload(payload)

    def test_native_install_failure_stops_before_later_success(self):
        text = (ROOT / "scripts/release/verify-release/run.sh").read_text()
        setup = text.split("    native_setup() (", 1)[1].split("\n    )", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, body in {
                "python3": 'if [[ "$2" == venv ]]; then mkdir -p "$3/bin"; cp "$0" "$3/bin/python"; fi\nexit 0',
                "cargo": 'echo cargo >> "$INSTALL_CALL_LOG"\nexit 9',
            }.items():
                path = root / name
                path.write_text("#!/bin/bash\n" + body + "\n")
                path.chmod(0o755)
            env = dict(
                os.environ,
                PATH=f"{root}:" + os.environ["PATH"],
                ALOPEX_VERIFY_NATIVE_ROOT=directory,
                SCRIPT_DIR=directory,
                ALOPEX_VERSION="0.8.16",
                INSTALL_CALL_LOG=str(root / "calls"),
            )
            result = subprocess.run(
                ["bash", "-c", "native_setup() (" + setup + "\n)\nnative_setup"],
                env=env,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 9)
            self.assertEqual((root / "calls").read_text().splitlines(), ["cargo"])

    def test_scenario_markdown_describes_executed_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = {
                "schema": REPORT.SCHEMA,
                "outcome": "success",
                "version": "0.8.16",
                "suite": "scenarios",
                "started_at": "now",
                "completed_at": "later",
                "failure_stage": None,
                "environment": {
                    "package_source": "crates.io / PyPI",
                    "rust": "rustc actual",
                    "nim": "not used",
                    "python": "3.11",
                },
                "identity": {},
                "steps": [],
            }
            results = root / "v0.8.16.json"
            results.write_text(json.dumps(payload))
            REPORT.render(argparse.Namespace(results=results, output_dir=root))
            markdown = (root / "v0.8.16.md").read_text()
            self.assertIn("下記の全シナリオを実行した", markdown)
            self.assertNotIn("最小importが成功している", markdown)


if __name__ == "__main__":
    unittest.main()
