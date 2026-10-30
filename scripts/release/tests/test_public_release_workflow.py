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
        self.assertIn("artifact_name: ${{ steps.report_context.outputs.artifact_name }}", self.text)
        self.assertIn('artifact_name=release-verification-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}', self.text)
        self.assertEqual(self.text.count("name: ${{ needs.verify.outputs.artifact_name }}"), 2)
        self.assertEqual(self.text.count("          name: ${{ steps.report_context.outputs.artifact_name }}"), 1)
        self.assertIn("${{ runner.temp }}/release-verification-v*/v*.json", self.text)
        self.assertIn("${{ runner.temp }}/release-verification-v*/v*.md", self.text)
        self.assertIn('report_path="reports/release-verification/v${VERSION}/${report_id}"', self.text)
        self.assertIn('"${report_path}.json"', self.text)
        self.assertIn('"${report_path}.md"', self.text)
        self.assertIn("needs.verify.result == 'failure'", self.text)
        self.assertIn("Create or update failure issue", self.text)
        reporter = (ROOT / "scripts/release/verify-release/report.py").read_text(
            encoding="utf-8"
        )
        for field in ("commit", "tag", "run_url", "responsibility"):
            self.assertIn(f'"{field}"', reporter)
        for field in (
            "run_id",
            "run_attempt",
            "started_at",
            "completed_at",
            "failure_stage",
        ):
            self.assertIn(f'"{field}"', reporter)
        for outcome in ("success", "failure", "incomplete"):
            self.assertIn(f'"{outcome}"', reporter)

    def test_resolved_python_tag_and_peeled_commit_own_report_identity(self) -> None:
        release = self.text.split("      - name: Resolve exact public version\n", 1)[
            1
        ].split("      - name: Verify exact public package availability\n", 1)[0]
        self.assertIn('python_tag="alopex-py-v${version}"', release)
        self.assertIn(
            '"refs/tags/${python_tag}:refs/tags/${python_tag}"', release
        )
        self.assertIn('git rev-parse "${python_tag}^{commit}"', release)
        self.assertIn('--commit "${python_commit}" --tag "${python_tag}"', release)
        self.assertIn('if [[ "${GITHUB_REF_TYPE}" == tag ]]; then', release)
        self.assertIn('[[ "${GITHUB_REF_NAME}" == "${python_tag}" ]]', release)
        self.assertIn('[[ "${GITHUB_SHA}" == "${python_commit}" ]]', release)
        self.assertIn('provisional_report_root="${REPORT_ROOT}"', release)
        self.assertIn(
            'rm -f "${provisional_report_root}/vunknown.json" '
            '"${provisional_report_root}/vunknown.md"',
            release,
        )
        self.assertLess(
            release.index('echo "REPORT_COMMIT=${python_commit}"'),
            release.index('if [[ "${GITHUB_REF_TYPE}" == tag ]]; then'),
        )
        self.assertLess(
            release.index('rm -f "${provisional_report_root}/vunknown.json"'),
            release.index('if [[ "${GITHUB_REF_TYPE}" == tag ]]; then'),
        )
        self.assertNotIn(
            '--commit "${GITHUB_SHA}" --tag "${GITHUB_REF_NAME}"', self.text
        )

    def test_report_context_exists_before_failure_prone_steps(self) -> None:
        self.assertIn("- name: Initialize verification report context", self.text)
        self.assertIn("id: report_context", self.text)
        self.assertIn("report.py init", self.text)
        self.assertIn("id: availability", self.text)
        self.assertIn(
            "VERSION: ${{ steps.release.outputs.version || "
            "steps.report_context.outputs.version || 'unknown' }}",
            self.text,
        )

    def test_publication_preserves_failure_evidence_and_exact_docs_bytes(self) -> None:
        publish = self.text.split("  publish:\n", 1)[1].split(
            "  publish-vector-benchmark:\n", 1
        )[0]
        self.assertIn("if: ${{ always() && needs.verify.result != 'cancelled' }}", publish)
        self.assertIn("report.py validate-report", publish)
        self.assertNotIn("validate-public", publish)
        self.assertNotIn("✅ 全ステップ成功", publish)
        self.assertIn("contents: write", publish)
        self.assertIn('if: ${{ always() && needs.verify.result != \'cancelled\' }}', publish)
        self.assertIn('report_id="run-${REPORT_RUN_ID}-${REPORT_RUN_ATTEMPT}"', publish)
        self.assertIn('branch="report/verify-release-v${VERSION}-${REPORT_RUN_ID}-${REPORT_RUN_ATTEMPT}"', publish)
        self.assertNotIn("DOCS_REPO_TOKEN", publish)
        self.assertNotIn("dispatches", publish)
        self.assertIn("Wait for identical bytes on docs main", publish)
        self.assertIn("if: success() && steps.publish.outputs.branch", publish)
        self.assertIn('for extension in json md; do', publish)
        self.assertIn('"${RUNNER_TEMP}/existing-report.${extension}"', publish)
        self.assertEqual(publish.count('report_pair.py compare'), 3)
        self.assertIn('git add "${report_path}.md" "${report_path}.json"', publish)
        self.assertIn('"${public_url}.json?cachebust=', publish)
        self.assertIn('"${public_url}.md?cachebust=', publish)
        self.assertNotIn("existing=", publish)
        self.assertNotIn("repository: alopex-db/docs", publish)
        self.assertIn("cmp -s", self.text)
        self.assertIn("?cachebust=${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}", self.text)
        self.assertIn(
            "?cachebust=${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-${attempt}",
            self.text,
        )
        self.assertNotIn("git push --force", self.text)
        self.assertEqual(
            publish.count("refusing to overwrite immutable report"), 2
        )

    def test_verification_and_publication_have_separate_permissions(self) -> None:
        verify = self.text.split("  verify:\n", 1)[1].split("  publish:\n", 1)[0]
        publish = self.text.split("  publish:\n", 1)[1].split(
            "  publish-vector-benchmark:\n", 1
        )[0]
        self.assertIn("contents: read", verify)
        self.assertNotIn("contents: write", verify)
        self.assertIn("needs: verify", publish)
        self.assertIn("if: ${{ always() && needs.verify.result != 'cancelled' }}", publish)
        self.assertIn("contents: write", publish)
        self.assertIn("actions/download-artifact@v4", publish)
        self.assertIn(
            "name: ${{ needs.verify.outputs.artifact_name }}",
            publish,
        )
        self.assertIn("Select downloaded verification report", publish)
        self.assertIn('report_pair.py select', publish)
        self.assertIn('REPORT_ARTIFACT: ${{ needs.verify.outputs.artifact_name }}', publish)
        self.assertNotIn('find "${REPORT_ROOT}"', publish)

    def test_report_publication_failure_creates_public_follow_up(self) -> None:
        fallback = self.text.split(
            "  publish-publication-failure-report:\n", 1
        )[1].split("  notify-publication-failure:\n", 1)[0]
        self.assertIn("needs: [verify, publish]", fallback)
        self.assertIn("contents: write", fallback)
        self.assertNotIn("repository: alopex-db/docs", fallback)
        self.assertNotIn("DOCS_REPO_TOKEN", fallback)
        self.assertNotIn("report.py record", fallback)
        self.assertNotIn("report.py finalize", fallback)
        self.assertIn("name: ${{ needs.verify.outputs.artifact_name }}", fallback)
        self.assertIn("continue-on-error: true", fallback)
        for command in ("prepare", "retain", "confirm"):
            self.assertIn("publication_failure.py " + command, fallback)
        self.assertIn("name: publication-failure-${{ github.run_id }}-${{ github.run_attempt }}", fallback)
        self.assertIn("needs.publish.outputs.failure_stage", fallback)
        self.assertIn("if-no-files-found: error", fallback)
        self.assertLess(fallback.index("actions/upload-artifact@v4"), fallback.index("publication_failure.py retain"))
        upload = fallback.split("- name: Preserve failure bundle", 1)[1].split("- name: Retain immutable", 1)[0]
        self.assertIn("if: always()", upload)
        cleanup = fallback.split("- name: Cleanup confirmed failure-report branch", 1)[1]
        self.assertIn("if: success() && steps.publish_failure.outputs.branch != ''", cleanup)
        self.assertNotIn("always()", cleanup)
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

    def test_public_vector_scenario_uses_the_resolved_release_version(self) -> None:
        self.assertIn("- name: Re-run public wheel scenario", self.text)
        self.assertIn('--name "v${VERSION} public vector scenario"', self.text)
        self.assertIn("v${VERSION}のget_vectors", self.text)
        self.assertNotIn("v0.8.14 public vector scenario", self.text)
        self.assertNotIn("v0.8.14のget_vectors", self.text)
        scenario = (ROOT / "scripts/release/verify_python_vector_api.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("v0.8.14", scenario)
        self.assertIn("version('alopex')", scenario)

    def test_stable_delivery_only_imports_exact_sha_benchmark_evidence(self) -> None:
        benchmark = self.text.split("  publish-vector-benchmark:\n", 1)[1].split(
            "  publish-publication-failure-report:\n", 1
        )[0]
        self.assertIn("needs: [verify, publish]", benchmark)
        self.assertIn("actions: read", benchmark)
        self.assertIn("PARITY_RUN_ID: ${{ inputs.parity_run_id || '' }}", benchmark)
        self.assertNotIn("actions/workflows/parity-performance.yml/runs", benchmark)
        self.assertIn("actions/runs/${PARITY_RUN_ID}", benchmark)
        self.assertIn('run.get("head_sha") != os.environ["SOURCE_COMMIT"]', benchmark)
        self.assertIn(
            'run.get("path") != ".github/workflows/parity-performance.yml"',
            benchmark,
        )
        self.assertIn("gh run download", benchmark)
        self.assertIn("release_hnsw_evidence.py", benchmark)
        self.assertIn("--public-pair", benchmark)
        self.assertIn("already_published=true", benchmark)
        self.assertIn('report="reports/vector-benchmarks/v${VERSION}"', benchmark)
        self.assertIn("repository: alopex-db/docs", benchmark)
        self.assertIn('branch="report/vector-benchmark-v${VERSION}"', benchmark)
        self.assertIn(
            "[Vector benchmark reports](reports/vector-benchmarks/README.md)",
            benchmark,
        )
        self.assertIn("publish-vector-benchmarks.yml", benchmark)
        self.assertIn("candidate-benchmark.json", benchmark)
        self.assertIn('cmp -s "${REPORT_ROOT}/hnsw-diagnostic.json"', benchmark)
        self.assertIn("Wait for identical benchmark bytes on docs main", benchmark)
        self.assertIn("Cleanup benchmark publication branch", benchmark)
        self.assertNotIn("hnsw_v0811_contract.py", benchmark)
        self.assertNotIn("maturin", benchmark)
        self.assertNotIn("pip install", benchmark)


if __name__ == "__main__":
    unittest.main()
