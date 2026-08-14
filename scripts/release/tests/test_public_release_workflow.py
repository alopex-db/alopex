from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/public-release-verification.yml"


class PublicReleaseWorkflowContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def test_weekly_and_explicit_entrypoints_exist(self) -> None:
        self.assertIn("workflow_call:", self.text)
        self.assertIn("workflow_dispatch:", self.text)
        self.assertIn('cron: "23 3 * * 1"', self.text)
        self.assertIn("publish_report:", self.text)

    def test_failed_runs_are_artifacts_and_not_public_reports(self) -> None:
        self.assertIn("if: always() && steps.release.outcome == 'success'", self.text)
        self.assertIn("inputs.publish_report == true", self.text)
        self.assertIn("needs.verify.result == 'failure'", self.text)
        self.assertIn("Create or update failure issue", self.text)

    def test_publication_requires_success_without_skip_and_exact_docs_bytes(self) -> None:
        self.assertIn("✅ 全ステップ成功", self.text)
        self.assertIn("must not contain executed SKIP", self.text)
        self.assertIn("cmp -s", self.text)
        self.assertNotIn("git push --force", self.text)

    def test_verification_and_publication_have_separate_permissions(self) -> None:
        verify = self.text.split("  verify:\n", 1)[1].split("  publish:\n", 1)[0]
        publish = self.text.split("  publish:\n", 1)[1].split(
            "  notify-scheduled-failure:\n", 1
        )[0]
        self.assertIn("contents: read", verify)
        self.assertNotIn("contents: write", verify)
        self.assertIn("needs: verify", publish)
        self.assertIn("needs.verify.result == 'success'", publish)
        self.assertIn("contents: write", publish)
        self.assertIn("actions/download-artifact@v4", publish)


if __name__ == "__main__":
    unittest.main()
