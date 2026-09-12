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
        self.assertIn('actions/runs/${CORE_RUN_ID}', block)
        self.assertIn('actions/runs/${CORE_RUN_ID}/jobs?per_page=100', block)
        self.assertIn("Publish to crates.io", block)
        self.assertIn("CORE_RUN_HEAD_SHA", block)
        self.assertIn("CORE_RUN_ID: ${{ inputs.core_run_id }}", block)
        self.assertIn("bash scripts/release/verify-release/run.sh --verify-join", block)
        self.assertIn('parser-assets-v${VERSION}.json', block)
        self.assertIn('parser-vendor-manifest-v${VERSION}.json', block)
        self.assertIn('f"parser-assets-v{version}.json"', block)
        self.assertIn('f"parser-vendor-manifest-v{version}.json"', block)
        self.assertIn('git merge-base --is-ancestor "${core_tag_sha}" "${python_tag_sha}"', block)
        self.assertIn('git merge-base --is-ancestor "${python_tag_sha}" origin/main', block)

    def test_join_does_not_use_unbound_latest_run_or_rebuild(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  final-release-join:", maxsplit=1)[1]
        self.assertNotIn("--branch main --limit 1", block)
        self.assertNotIn("cargo publish", block)
        self.assertNotIn("maturin build", block)

    def test_dispatch_runs_from_the_python_tag(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("workflow_dispatch:", text)
        self.assertIn("core_run_id:", text)
        self.assertNotIn("source_ref:", text)
        self.assertNotIn("target_sha:", text)
        self.assertNotIn("release_tag:", text)
        self.assertNotIn("repair_forward:", text)
        self.assertIn("ref: ${{ github.ref }}", text)
        self.assertIn('PYTHON_HEAD_SHA="$(git rev-parse HEAD)"', text)
        self.assertIn("PYTHON_TAG_NAME: ${{ github.ref_name }}", text)
        self.assertIn("PYTHON_HEAD_SHA=%s", text)

    def test_python_head_sha_is_exported_from_the_tag(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split(
            "      - name: Resolve exact core and Python workflow identities",
            maxsplit=1,
        )[1].split(
            "      - name: Download the immutable parser envelope",
            maxsplit=1,
        )[0]
        self.assertIn(
            "printf 'PYTHON_HEAD_SHA=%s\\n' \"${PYTHON_HEAD_SHA}\" >> \"${GITHUB_ENV}\"",
            block,
        )

    def test_release_has_no_repair_prepare_job(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("prepare-repair-release", text)
        self.assertNotIn("repair_forward", text)
        self.assertNotIn("prepare-python-repair", text)
        for job in ("linux", "macos", "windows", "sdist"):
            job_block = text.split(f"  {job}:", maxsplit=1)[1]
            self.assertNotIn("needs:", job_block.split("    steps:", maxsplit=1)[0])

    def test_join_records_independent_core_and_python_identities(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  final-release-join:", maxsplit=1)[1]
        self.assertIn('"core_tag": {"name": f"v{version}", "peeled_sha": core_sha}', block)
        self.assertIn('"python_tag": {"name": f"alopex-py-v{version}", "peeled_sha": python_sha}', block)
        self.assertIn('"python_descends_from_core": True', block)

    def test_python_release_requires_explicit_core_dispatch(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        trigger = text.split("  workflow_dispatch:", maxsplit=1)[0]
        self.assertNotIn("push:", trigger)
        self.assertIn("core release dispatches", trigger)

    def test_public_verifier_is_detached_from_python_publish(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("verify-public-release:", text)

    def test_sidecars_are_written_with_platform_stable_bytes(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("CONTRACT_VERSION').write_text", text)
        self.assertNotIn("SHA256SUMS').write_text", text)
        self.assertGreaterEqual(text.count("CONTRACT_VERSION').write_bytes(b'0.25.0\\n')"), 3)

    def test_every_wheel_target_retargets_parser_pins_from_release_manifest(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(text.count("ref: ${{ github.sha }}"), 3)
        self.assertEqual(text.count("path: .release-tools"), 3)
        self.assertEqual(
            text.count(
                "python .release-tools/scripts/release/retarget_python_parser_source.py"
            ),
            3,
        )
        self.assertEqual(
            text.count(
                "--vendor-dir crates/alopex-sql/nim-sql-parser/vendor"
            ),
            3,
        )
        self.assertNotIn(
            "--vendor-manifest crates/alopex-sql/nim-sql-parser/vendor/",
            text,
        )
        self.assertEqual(text.count("name: Resolve source release version"), 3)
        self.assertEqual(text.count('echo "CORE_TAG=v${version}"'), 3)
        self.assertGreaterEqual(
            text.count('parser-vendor-manifest-v${ALOPEX_VERSION}.json'), 3
        )

    def test_delivery_builds_pin_maturin_and_repair_macos_dylibs(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(text.count('maturin-version: "1.14.1"'), 4)
        macos = text.split("  macos:", maxsplit=1)[1].split(
            "  windows:", maxsplit=1
        )[0]
        self.assertIn("--auditwheel repair", macos)

    def test_sdist_stages_source_without_native_vendor_directories(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  sdist:", maxsplit=1)[1].split("  publish-testpypi:", maxsplit=1)[0]
        self.assertIn("Remove native parser vendor files from sdist staging", block)
        self.assertIn("find crates/alopex-sql/nim-sql-parser/vendor", block)
        self.assertIn("-type d -exec rm -rf {} +", block)

    def test_github_release_uses_the_python_tag(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  github-release:", maxsplit=1)[1].split("  final-release-join:", maxsplit=1)[0]
        self.assertIn("tag_name: ${{ github.ref_name }}", block)


if __name__ == "__main__":
    unittest.main()
