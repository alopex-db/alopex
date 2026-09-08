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
        self.assertNotIn("publish_report:", self.text)

    def test_every_run_is_an_immutable_public_report(self) -> None:
        self.assertIn("- name: Finalize verification report", self.text)
        self.assertIn("if: always()", self.text)
        self.assertIn("name: release-verification-${{ github.run_id }}", self.text)
        self.assertIn("${{ runner.temp }}/release-verification-v*/v*.json", self.text)
        self.assertIn("${{ runner.temp }}/release-verification-v*/v*.md", self.text)
        self.assertIn(
            'report_path="reports/release-verification/v${version}/run-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"',
            self.text,
        )
        self.assertIn('"${REPORT_PATH}.json"', self.text)
        self.assertIn('"${REPORT_PATH}.md"', self.text)
        self.assertIn("needs.verify.result == 'failure'", self.text)
        self.assertIn("Create or update failure issue", self.text)
        reporter = (ROOT / "scripts/release/verify-release/report.py").read_text(
            encoding="utf-8"
        )
        for field in ("commit", "tag", "run_url", "responsibility"):
            self.assertIn(f'"{field}"', reporter)

    def test_report_context_exists_before_failure_prone_steps(self) -> None:
        self.assertIn("- name: Initialize verification report context", self.text)
        self.assertIn("id: report_context", self.text)
        self.assertIn("report.py init", self.text)
        self.assertIn("id: availability", self.text)
        self.assertIn('VERSION: ${{ steps.release.outputs.version || steps.report_context.outputs.version || \'unknown\' }}', self.text)

    def test_publication_preserves_failure_evidence_and_exact_docs_bytes(self) -> None:
        publish = self.text.split("  publish:\n", 1)[1].split(
            "  notify-scheduled-failure:\n", 1
        )[0]
        self.assertIn("if: always()", publish)
        self.assertIn("report.py validate-report", publish)
        self.assertNotIn("validate-public", publish)
        self.assertNotIn("✅ 全ステップ成功", publish)
        self.assertIn("repository: alopex-db/docs", publish)
        self.assertIn("DOCS_REPO_TOKEN", publish)
        self.assertIn("working-directory: docs-repo", publish)
        self.assertIn("cmp -s", self.text)
        self.assertIn("?cachebust=${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}", self.text)
        self.assertIn(
            "?cachebust=${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-${attempt}",
            self.text,
        )
        self.assertNotIn("git push --force", self.text)

    def test_verification_and_publication_have_separate_permissions(self) -> None:
        verify = self.text.split("  verify:\n", 1)[1].split("  publish:\n", 1)[0]
        publish = self.text.split("  publish:\n", 1)[1].split(
            "  notify-scheduled-failure:\n", 1
        )[0]
        self.assertIn("contents: read", verify)
        self.assertNotIn("contents: write", verify)
        self.assertIn("needs: verify", publish)
        self.assertIn("if: always()", publish)
        self.assertIn("contents: read", publish)
        self.assertIn("actions/download-artifact@v4", publish)
        self.assertIn("name: release-verification-${{ github.run_id }}", publish)
        self.assertIn("Select downloaded verification report", publish)
        self.assertIn('echo "REPORT_ROOT=${report_root}"', publish)

    def test_report_publication_failure_creates_public_follow_up(self) -> None:
        fallback = self.text.split(
            "  publish-publication-failure-report:\n", 1
        )[1].split("  notify-publication-failure:\n", 1)[0]
        self.assertIn("needs: [verify, publish]", fallback)
        self.assertIn("repository: alopex-db/docs", fallback)
        self.assertIn("DOCS_REPO_TOKEN", fallback)
        self.assertIn("Mark report publication failure", fallback)
        self.assertIn("immutable report publication", fallback)
        self.assertIn("Wait for immutable failure report on docs main", fallback)
        self.assertIn("notify-publication-failure:", self.text)
        notification = self.text.split("  notify-publication-failure:\n", 1)[1].split(
            "  notify-scheduled-failure:\n", 1
        )[0]
        self.assertIn(
            "needs: [verify, publish, publish-publication-failure-report]", notification
        )
        self.assertIn(
            "needs.publish-publication-failure-report.result == 'failure'", notification
        )
        self.assertIn("issues: write", notification)
        self.assertIn("immutable report publication failed", notification)

    def test_public_verification_checks_only_artifact_availability(self) -> None:
        verify = self.text.split("  verify:\n", 1)[1].split("  publish:\n", 1)[0]
        readiness = verify.split(
            "      - name: Verify exact public package availability\n", 1
        )[1]
        self.assertIn('pip download', readiness)
        self.assertIn('pip install', readiness)
        self.assertIn('"alopex==${VERSION}"', readiness)
        self.assertIn("--only-binary=:all:", readiness)
        self.assertIn("seq 1 30", readiness)
        self.assertIn("sleep 20", readiness)
        self.assertNotIn("Run public-package demos", verify)
        self.assertNotIn('verify-release/run.sh "${VERSION}"', verify)


if __name__ == "__main__":
    unittest.main()
