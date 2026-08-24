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
        current = workflow.split("  v08-release-gate:\n", 1)[1].split(
            "\n  ci-success:", 1
        )[0]

        self.assertIn("- os: macos-latest\n            rust: stable", compatibility_test)
        self.assertIn("- os: ubuntu-latest\n            rust: beta", compatibility_test)
        self.assertNotIn("os: [ubuntu-latest, macos-latest, windows-latest]", compatibility_test)
        self.assertNotIn("name: Run doc tests", compatibility_test)
        self.assertIn("scripts/ci/run_with_metrics.py", compatibility_test)
        self.assertIn("Upload compatibility build metrics", compatibility_test)
        self.assertIn("os: [ubuntu-latest, windows-latest]", current)
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
        for duplicate_selector in (
            "--test distributed_read_http",
            "--test streaming_contract",
            "-p alopex-server --tests",
            "-p alopex-dataframe --tests",
            "cargo test --doc",
        ):
            self.assertNotIn(duplicate_selector, verifier)

    def test_historical_gates_are_independent_scheduled_owners(self) -> None:
        compatibility = self.read(".github/workflows/compatibility.yml")
        v07 = self.read("scripts/release/v07_gate.sh")

        self.assertIn("workflow_dispatch:", compatibility)
        self.assertIn("schedule:", compatibility)
        self.assertIn("historical-parser:", compatibility)
        self.assertIn("historical-contract:", compatibility)
        self.assertIn("gate: [v06, v07]", compatibility)
        self.assertIn("actions/upload-artifact@v4", compatibility)
        self.assertIn("actions/download-artifact@v4", compatibility)
        self.assertNotIn("actions/cache@v3", compatibility)
        self.assertIn(
            "CARGO_TARGET_DIR: ${{ github.workspace }}/target/historical-${{ matrix.gate }}",
            compatibility,
        )
        regular_event_guard = (
            "if: github.event_name == 'push' || github.event_name == 'pull_request'"
        )
        native = compatibility.split("  native:\n", 1)[1].split("\n  wasm:", 1)[0]
        wasm = compatibility.split("  wasm:\n", 1)[1]
        self.assertIn(regular_event_guard, native)
        self.assertIn(regular_event_guard, wasm)
        self.assertNotIn("V07_GATE_RUN_V06", v07)
        self.assertNotIn("scripts/release/v06_gate.sh", v07)
        self.assertNotIn("cargo clean --profile dev", v07)
        self.assertNotIn("cargo fmt", v07)
        self.assertNotIn("cargo clippy", v07)
        main = v07.split("main() {", 1)[1]
        self.assertLess(
            main.index('if [[ "${1:-}" == "--workflow-contract-only" ]]'),
            main.index("configure_python_environment"),
        )

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
