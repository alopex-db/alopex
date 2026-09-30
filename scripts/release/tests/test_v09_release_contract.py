import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class V09ReleaseContractTests(unittest.TestCase):
    def test_release_workflow_uses_rc_manifest_and_same_sha_promotion(self) -> None:
        workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
        for required in (
            "v[0-9]+\\.[0-9]+\\.[0-9]+-rc",
            "candidate_manifest.py",
            "promote-release",
            "RELEASE_TARGET_SHA",
            "prepare-python-release.sh",
        ):
            self.assertIn(required, workflow)
        self.assertNotIn("type_capability_gate.py", workflow)
        self.assertNotIn("v07_gate.sh", workflow)

    def test_ci_invokes_the_v09_target_gate(self) -> None:
        workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        self.assertIn("v09-release-gate:", workflow)
        self.assertIn("--v09-candidate-gate", workflow)
        self.assertNotIn("v08-release-gate", workflow)

    def test_release_identity_helpers_are_present(self) -> None:
        for path in (
            "scripts/release/candidate_manifest.py",
            "scripts/release/package_candidate_crates.py",
            "scripts/release/parser_asset_manifest.py",
            "scripts/release/prepare-python-release.sh",
            "scripts/release/publish_crate_archive.py",
            "scripts/release/safe-tag.sh",
        ):
            self.assertTrue((ROOT / path).is_file(), path)


if __name__ == "__main__":
    unittest.main()
