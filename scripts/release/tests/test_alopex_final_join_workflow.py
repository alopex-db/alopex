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
        self.assertIn("gh run list --workflow release.yml", block)
        self.assertIn("headBranch,status,conclusion", block)
        self.assertIn("repair/v0.8.5-release", block)
        self.assertIn("CORE_RUN_HEAD_SHA", block)
        self.assertIn("expected exactly one successful core run", block)
        self.assertIn("if not repaired:", block)
        self.assertIn(
            'max(repaired, key=lambda run: int(run["databaseId"]))', block
        )
        self.assertNotIn("if len(repaired) != 1:", block)
        self.assertIn("bash scripts/release/verify-release/run.sh --verify-join", block)
        self.assertIn("parser-assets-v0.8.5.json", block)
        self.assertIn("parser-vendor-manifest-v0.8.5.json", block)
        self.assertIn("core and Python tags do not share a peeled SHA", block)

    def test_join_does_not_use_unbound_latest_run_or_rebuild(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  final-release-join:", maxsplit=1)[1]
        self.assertNotIn("--branch main --limit 1", block)
        self.assertNotIn("cargo publish", block)
        self.assertNotIn("maturin build", block)

    def test_repair_forward_run_binds_source_and_target_explicitly(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("workflow_dispatch:", text)
        self.assertIn("source_ref:", text)
        self.assertIn("target_sha:", text)
        self.assertIn("release_tag:", text)
        self.assertIn("repair_forward:", text)
        self.assertIn("type: boolean", text)
        self.assertIn(
            "ref: ${{ inputs.source_ref || (startsWith(github.ref_name, 'alopex-py-v0.8.5-repair') && 'alopex-py-v0.8.5') || github.ref }}",
            text,
        )
        self.assertIn("PYTHON_HEAD_SHA: ${{ inputs.target_sha || github.sha }}", text)
        self.assertIn("PYTHON_TAG_NAME: ${{ inputs.release_tag || (startsWith(github.ref_name, 'alopex-py-v0.8.5-repair') && 'alopex-py-v0.8.5') || github.ref_name }}", text)
        self.assertIn("alopex-py-v0.8.5-repair", text)
        self.assertIn("PYTHON_HEAD_SHA=%s", text)

    def test_repair_dispatch_creates_or_verifies_tag_before_packaging(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  prepare-repair-release:", maxsplit=1)[1].split(
            "  linux:", maxsplit=1
        )[0]
        self.assertIn("contents: write", block)
        self.assertIn("SOURCE_SHA: ${{ inputs.source_ref }}", block)
        self.assertIn("TARGET_SHA: ${{ inputs.target_sha }}", block)
        self.assertIn("RELEASE_TAG: ${{ inputs.release_tag }}", block)
        self.assertIn("run: bash scripts/release/prepare-python-repair.sh", block)
        for job in ("linux", "macos", "windows", "sdist"):
            job_block = text.split(f"  {job}:", maxsplit=1)[1]
            self.assertIn("needs: [prepare-repair-release]", job_block.split("    steps:", maxsplit=1)[0])

    def test_repair_dispatch_selects_core_repair_evidence_explicitly(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  final-release-join:", maxsplit=1)[1]
        self.assertIn("REPAIR_FORWARD: ${{ inputs.repair_forward }}", block)
        self.assertIn('repair_forward == "true"', block)

    def test_v085_immutable_tag_does_not_start_historical_workflow(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        trigger = text.split("  workflow_dispatch:", maxsplit=1)[0]
        self.assertIn('- "alopex-py-v*"', trigger)
        self.assertIn('- "!alopex-py-v0.8.5"', trigger)

    def test_public_verifier_uses_immutable_tag_for_repair_run(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  verify-public-release:", maxsplit=1)[1]
        self.assertIn(
            "version: ${{ inputs.release_tag || (startsWith(github.ref_name, 'alopex-py-v0.8.5-repair') && 'alopex-py-v0.8.5') || github.ref_name }}",
            block,
        )

    def test_sidecars_are_written_with_platform_stable_bytes(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("CONTRACT_VERSION').write_text", text)
        self.assertNotIn("SHA256SUMS').write_text", text)
        self.assertGreaterEqual(text.count("CONTRACT_VERSION').write_bytes(b'0.4.0\\n')"), 3)

    def test_every_wheel_target_retargets_parser_pins_from_release_manifest(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(text.count("ref: ${{ github.sha }}"), 4)
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

    def test_manual_release_uses_the_existing_immutable_tag(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("  github-release:", maxsplit=1)[1].split("  final-release-join:", maxsplit=1)[0]
        self.assertIn("tag_name: ${{ inputs.release_tag || (startsWith(github.ref_name, 'alopex-py-v0.8.5-repair') && 'alopex-py-v0.8.5') || github.ref_name }}", block)


if __name__ == "__main__":
    unittest.main()
