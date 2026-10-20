from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


class PublicReleaseWorkflowContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = (
            ROOT / ".github/workflows/public-release-verification.yml"
        ).read_text()
        cls.verify = cls.text.split("  verify:\n", 1)[1].split("  publish:\n", 1)[0]
        cls.publish = cls.text.split("  publish:\n", 1)[1].split(
            "  publish-vector-benchmark:\n", 1
        )[0]

    def test_complete_scenarios_execute_after_exact_version_resolution(self):
        self.assertIn("workflow_call:", self.text)
        self.assertIn("workflow_dispatch:", self.text)
        self.assertIn('cron: "23 3 * * 1"', self.text)
        self.assertIn('python_tag="alopex-py-v${version}"', self.verify)
        self.assertIn('git rev-parse "${python_tag}^{commit}"', self.verify)
        self.assertIn('"${VERSION}" --native', self.verify)
        self.assertIn("scripts/compatibility/storage.py", self.verify)
        self.assertIn("--additional-bytes 18000000000", self.verify)
        self.assertIn("CARGO_TARGET_DIR: ${{ github.workspace }}/target", self.verify)
        self.assertIn("Remove regenerable verification outputs", self.verify)
        self.assertIn(
            "VECTOR_SCENARIO_OUTCOME: ${{ steps.vector_scenario.outcome }}", self.verify
        )

    def test_report_identity_and_artifact_follow_producing_attempt(self):
        self.assertIn(
            "report_artifact: ${{ steps.report_context.outputs.artifact }}", self.verify
        )
        self.assertIn("name: ${{ needs.verify.outputs.report_artifact }}", self.publish)
        self.assertIn("REPORT_RUN_ATTEMPT=", self.publish)
        self.assertIn(
            'report_id="run-${REPORT_RUN_ID}-${REPORT_RUN_ATTEMPT}"', self.publish
        )
        self.assertIn(
            'branch="report/verify-release-v${VERSION}-${REPORT_RUN_ID}-${REPORT_RUN_ATTEMPT}"',
            self.publish,
        )

    def test_docs_pulls_immutable_pair_without_cross_repo_secret(self):
        self.assertIn(
            "if: ${{ always() && needs.verify.result != 'cancelled' && inputs.baseline_run_id == '' }}",
            self.publish,
        )
        self.assertIn("report.py validate-report", self.publish)
        self.assertIn("for ext in md json; do", self.publish)
        self.assertIn('git add "${report_path}.md" "${report_path}.json"', self.publish)
        self.assertIn("refusing to overwrite immutable report", self.publish)
        self.assertIn("cmp -s", self.publish)
        self.assertNotIn("DOCS_REPO_TOKEN", self.publish)
        self.assertNotIn("gh api", self.publish)
        self.assertNotIn("git push --force", self.publish)

    def test_publication_failure_cannot_change_package_result(self):
        fallback = self.text.split("  publish-publication-failure-report:\n", 1)[
            1
        ].split("  notify-publication-failure:\n", 1)[0]
        self.assertIn("report-publication-recovery-", fallback)
        self.assertNotIn("DOCS_REPO_TOKEN", fallback)
        self.assertNotIn("report.py record", fallback)
        self.assertIn("needs.publish.result == 'failure'", self.text)

    def test_serialized_registry_builds_share_target_and_clear_parser_overrides(self):
        script = (ROOT / "scripts/release/verify-release/run.sh").read_text()
        self.assertIn("CARGO_BUILD_JOBS=2", script)
        self.assertIn('VERIFY_TOOLS_TARGET="${CARGO_TARGET_DIR}"', script)
        self.assertIn(
            'cargo install alopex-cli --version "=${ALOPEX_VERSION}" --locked', script
        )
        self.assertIn(
            'cargo install alopex-server --version "=${ALOPEX_VERSION}" --locked',
            script,
        )
        self.assertIn("unset ALOPEX_SQL_PARSER_LIB_DIR", script)
        self.assertIn("--suite scenarios", script)
        self.assertIn('result["outcome"] != "success"', script)

    def test_resolved_python_tag_and_peeled_commit_own_report_identity(self) -> None:
        release = self.text.split("      - name: Resolve exact public version\n", 1)[
            1
        ].split("      - name: Verify exact public package availability\n", 1)[0]
        self.assertIn('python_tag="alopex-py-v${version}"', release)
        self.assertIn('"refs/tags/${python_tag}:refs/tags/${python_tag}"', release)
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
