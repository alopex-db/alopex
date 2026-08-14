from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class V085ReleaseContractTests(unittest.TestCase):
    def test_target_version_is_consistent(self) -> None:
        workspace = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
        run = (ROOT / "scripts/release/verify-release/run.sh").read_text(encoding="utf-8")
        release = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
        python_release = (ROOT / ".github/workflows/alopex-py-release.yml").read_text(
            encoding="utf-8"
        )
        version = re.search(r'^version = "([0-9.]+)"$', workspace, re.MULTILINE)
        self.assertIsNotNone(version)
        self.assertEqual(version.group(1), "0.8.5")
        self.assertIn('ALOPEX_VERSION="0.8.5"', run)
        self.assertIn("parser-assets-v0.8.5.json", release)
        self.assertIn("parser-assets-v0.8.5.json", python_release)

    def test_public_tool_dependencies_are_generated_from_exact_version(self) -> None:
        tools = (ROOT / "crates/alopex-tools/Cargo.toml").read_text(encoding="utf-8")
        run = (ROOT / "scripts/release/verify-release/run.sh").read_text(encoding="utf-8")
        self.assertIn('path = "../alopex-embedded"', tools)
        self.assertIn('alopex-embedded = { version = "=${ALOPEX_VERSION}" }', run)
        self.assertIn('alopex-sql = { version = "=${ALOPEX_VERSION}" }', run)
        self.assertNotIn('alopex-embedded = "=0.7.4"', tools)

    def test_release_dag_requires_python_demos_and_docs(self) -> None:
        rust = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
        python = (ROOT / ".github/workflows/alopex-py-release.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("dispatch-python-release:", rust)
        self.assertIn('gh run watch "${run_id}" --exit-status', rust)
        self.assertIn("verify-public-release:", python)
        self.assertIn("publish_report: true", python)
        self.assertIn("verify_python_vector_api.py", python)

    def test_v08_demos_are_mandatory(self) -> None:
        run = (ROOT / "scripts/release/verify-release/run.sh").read_text(encoding="utf-8")
        self.assertIn("scripts/demo/v08/demo_sql_v08.py", run)
        self.assertIn("scripts/demo/v074/demo_api_surfaces.py", run)
        self.assertIn("scripts/demo/v074/demo_vector_api.py", run)


if __name__ == "__main__":
    unittest.main()
