from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class BuildResponsibilityContractTests(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (ROOT / relative).read_text(encoding="utf-8")

    def test_pr_ci_assigns_each_platform_suite_to_one_owner(self) -> None:
        workflow = self.read(".github/workflows/ci.yml")
        compatibility_test = workflow.split("  test:\n", 1)[1].split(
            "\n  coverage:", 1
        )[0]
        build = workflow.split("  build:\n", 1)[1].split(
            "\n  v08-release-gate:", 1
        )[0]
        current = workflow.split("  v08-release-gate:\n", 1)[1].split(
            "\n  ci-success:", 1
        )[0]

        self.assertIn("- os: macos-latest\n            rust: stable", compatibility_test)
        # A beta toolchain is not a release input, so it has no PR owner.
        self.assertNotIn("rust: beta", compatibility_test)
        self.assertNotIn("nextest", compatibility_test)
        self.assertNotIn("os: [ubuntu-latest, macos-latest, windows-latest]", compatibility_test)
        self.assertNotIn("name: Run doc tests", compatibility_test)
        self.assertIn("scripts/ci/run_with_metrics.py", compatibility_test)
        self.assertIn("Upload compatibility build metrics", compatibility_test)
        self.assertIn("os: [ubuntu-latest, macos-latest, windows-latest]", build)
        self.assertIn("- os: ubuntu-latest\n            rust_suite: full", current)
        self.assertIn("rust_suite: full", current)
        self.assertNotIn("windows-latest", current)
        self.assertNotIn("windows-smoke", current)
        self.assertIn(
            "ALOPEX_CURRENT_RUST_SUITE: ${{ matrix.rust_suite }}", current
        )
        self.assertNotIn("Run v0.7 baseline gate", current)

        extension = current.split(
            "- name: Build Python extension for v0.8 gate", 1
        )[1].split("- name: Run v0.8 release gate", 1)[0]
        self.assertNotIn("if: runner.os == 'Windows'", extension)
        self.assertIn("scripts/ci/run_with_metrics.py", extension)

    def test_current_surface_runs_the_workspace_suite_once_with_metrics(self) -> None:
        verifier = self.read("crates/alopex-tools/v08/verify-v08-surfaces.sh")

        self.assertEqual(verifier.count("cargo test "), 1)
        self.assertIn("--workspace", verifier)
        self.assertIn("--timings", verifier)
        self.assertIn("scripts/ci/run_with_metrics.py", verifier)
        self.assertIn('case "${ALOPEX_CURRENT_RUST_SUITE:-full}"', verifier)
        full_suite = verifier.split("    full)", 1)[1].split(";;", 1)[0]
        self.assertIn("--workspace --features lane_ci", full_suite)
        self.assertNotIn("windows-smoke", verifier)
        for duplicate_selector in (
            "--test distributed_read_http",
            "--test streaming_contract",
            "cargo test --doc",
        ):
            self.assertNotIn(duplicate_selector, verifier)

    def test_prior_version_gates_are_retired(self) -> None:
        # The v0.6/v0.7 gates only re-ran behavior that the current workspace
        # suites already assert on every pull request. They must not return as
        # scheduled jobs, release steps, or scripts.
        compatibility = self.read(".github/workflows/compatibility.yml")
        release = self.read(".github/workflows/release.yml")

        for retired in ("historical-parser:", "historical-contract:", "v06_gate", "v07_gate"):
            self.assertNotIn(retired, compatibility)
            self.assertNotIn(retired, release)
        self.assertFalse((ROOT / "scripts/release/v06_gate.sh").exists())
        self.assertFalse((ROOT / "scripts/release/v07_gate.sh").exists())

    def test_windows_full_suite_is_the_only_scheduled_compatibility_owner(self) -> None:
        compatibility = self.read(".github/workflows/compatibility.yml")

        self.assertIn("workflow_dispatch:", compatibility)
        self.assertIn("schedule:", compatibility)
        self.assertIn("current-windows-full:", compatibility)
        self.assertNotIn("actions/cache@v3", compatibility)
        self.assertIn("version: v0.15.0", compatibility)
        self.assertNotIn("version: latest", compatibility)
        regular_event_guard = (
            "if: github.event_name == 'push' || github.event_name == 'pull_request'"
        )
        native = compatibility.split("  native:\n", 1)[1].split("\n  wasm:", 1)[0]
        wasm = compatibility.split("  wasm:\n", 1)[1]
        self.assertIn(regular_event_guard, native)
        self.assertIn(regular_event_guard, wasm)
        windows_full = compatibility.split("  current-windows-full:\n", 1)[1].split(
            "\n  native:", 1
        )[0]
        self.assertIn("runs-on: windows-latest", windows_full)
        self.assertIn("cargo test --workspace --features lane_ci", windows_full)
        self.assertIn("scripts/ci/run_with_metrics.py", windows_full)
        self.assertIn(
            'echo "VIRTUAL_ENV=$venv_root" >> "$GITHUB_ENV"', windows_full
        )
        self.assertIn('"$venv_python" -m pip install "numpy<2"', windows_full)
        self.assertIn('echo "PYTHONPATH=$python_site" >> "$GITHUB_ENV"', windows_full)

    def test_test_profile_reduces_debug_artifact_generation(self) -> None:
        workspace = self.read("Cargo.toml")
        profile = workspace.split("[profile.test]", 1)[1].split("\n[", 1)[0]
        self.assertIn('debug = "line-tables-only"', profile)

    def test_ci_uploads_owner_metrics_and_final_gate_is_status_only(self) -> None:
        workflow = self.read(".github/workflows/ci.yml")
        current = workflow.split("  v08-release-gate:\n", 1)[1].split(
            "\n  ci-success:", 1
        )[0]
        final = workflow.split("  ci-success:\n", 1)[1]

        self.assertIn("Upload current implementation build metrics", current)
        self.assertIn("actions/upload-artifact@v4", current)
        self.assertNotIn("cargo test", final)
        self.assertNotIn("cargo build", final)


if __name__ == "__main__":
    unittest.main()
