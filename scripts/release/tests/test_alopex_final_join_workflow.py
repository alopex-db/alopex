#!/usr/bin/env python3
"""Static contract tests for the required Alopex/Python release join."""

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/alopex-py-release.yml"


class FinalJoinWorkflowTests(unittest.TestCase):
    def test_final_join_is_required_after_public_surfaces(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  final-release-join:", maxsplit=1)[1]
        self.assertIn("needs: [publish-pypi, github-release]", block)
        self.assertIn("contents: read", block)
        self.assertIn("actions: read", block)
        self.assertIn("gh run list --workflow release.yml --commit", block)
        self.assertIn("expected exactly one successful core run", block)
        self.assertIn("bash scripts/release/verify-release/run.sh --verify-join", block)
        self.assertIn("parser-assets-v0.8.4.json", block)
        self.assertIn("parser-vendor-manifest-v0.8.4.json", block)
        self.assertIn("core and Python tags do not share a peeled SHA", block)

    def test_join_does_not_use_unbound_latest_run_or_rebuild(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  final-release-join:", maxsplit=1)[1]
        self.assertNotIn("--branch main --limit 1", block)
        self.assertNotIn("cargo publish", block)
        self.assertNotIn("maturin build", block)


if __name__ == "__main__":
    unittest.main()
