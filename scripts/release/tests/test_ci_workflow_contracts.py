#!/usr/bin/env python3
"""Behavior contracts for parser-building pull-request workflows."""

from __future__ import annotations

from pathlib import Path
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_DIR = REPOSITORY_ROOT / ".github/workflows"
EXACT_SETUP = "uses: ./.github/actions/setup-nim-parser-toolchain"
FLOATING_SETUP = "uses: jiro4989/setup-nim-action@"


class CiWorkflowContractTests(unittest.TestCase):
    def workflow(self, name: str) -> str:
        return (WORKFLOW_DIR / name).read_text(encoding="utf-8")

    def test_every_non_linux_host_build_uses_the_exact_local_toolchain(self) -> None:
        for workflow_name in ("alopex-py.yml", "alopex-cli.yml"):
            with self.subTest(workflow=workflow_name):
                workflow = self.workflow(workflow_name)
                self.assertNotIn(FLOATING_SETUP, workflow)
                self.assertEqual(workflow.count(EXACT_SETUP), 1)

                setup_offset = workflow.index(
                    "name: Setup exact Nim parser toolchain (macOS/Windows)"
                )
                build_offset = workflow.index(
                    "name: Build Nim SQL parser library (macOS/Windows)"
                )
                self.assertLess(setup_offset, build_offset)

                setup_block = workflow[setup_offset:build_offset]
                self.assertIn("if: runner.os != 'Linux'", setup_block)
                self.assertIn(EXACT_SETUP, setup_block)

    def test_parser_toolchain_owners_trigger_every_consumer_workflow(self) -> None:
        required_paths = (
            ".github/actions/setup-nim-parser-toolchain/**",
            "scripts/build-nim-parser.sh",
            "crates/alopex-sql/**",
        )
        for workflow_name in ("alopex-py.yml", "alopex-cli.yml"):
            with self.subTest(workflow=workflow_name):
                workflow = self.workflow(workflow_name)
                for required_path in required_paths:
                    self.assertGreaterEqual(
                        workflow.count(required_path),
                        2,
                        f"{required_path} must trigger push and pull_request",
                    )

    def test_signal_harness_captures_status_and_rejects_unexpected_exit(self) -> None:
        workflow = self.workflow("alopex-cli.yml")
        signal_step = workflow.split(
            "- name: Test Ctrl-C signal handling", maxsplit=1
        )[1].split("# S3 compatibility test with MinIO", maxsplit=1)[0]

        self.assertIn("trap cleanup_signal_test EXIT", signal_step)
        self.assertIn('rm -f -- "$SQL_FILE"', signal_step)
        self.assertIn(
            "set +e\n          wait \"$PID\" 2>/dev/null\n"
            "          EXIT_CODE=$?\n          set -e",
            signal_step,
        )
        self.assertNotIn('[[ "$EXIT_CODE" -eq 1 ]]', signal_step)
        self.assertIn("seq 1 100", signal_step)
        self.assertNotIn("seq 1 1000", signal_step)
        self.assertNotIn("seq 1 5000", signal_step)
        self.assertNotIn("seq 1 10000", signal_step)
        self.assertNotIn("seq 1 20000", signal_step)
        self.assertNotIn("seq 1 50000", signal_step)
        self.assertIn('echo "FAIL: Unexpected exit code: $EXIT_CODE" >&2', signal_step)
        self.assertIn("exit 1", signal_step)

    def test_exact_toolchain_pins_the_immutable_nimble_registry_snapshot(self) -> None:
        action = (WORKFLOW_DIR.parent / "actions/setup-nim-parser-toolchain/action.yml").read_text(
            encoding="utf-8"
        )
        revision = "b79eaaa3fc65fc473bc9e803445f8f7aef7112a2"
        self.assertIn(f"package_list_revision='{revision}'", action)
        self.assertIn(
            "raw.githubusercontent.com/nim-lang/packages/${package_list_revision}/packages.json",
            action,
        )
        self.assertIn("cp \"${pinned_metadata}\" \"${nimble_dir}/packages_temp.json\"", action)

    def test_v08_gate_exposes_just_built_parser_dll_on_windows(self) -> None:
        workflow = self.workflow("ci.yml")
        stage_offset = workflow.index("name: Stage just-built parser for v0.8 surfaces")
        gate_offset = workflow.index("name: Run v0.8 release gate", stage_offset)
        windows_path_step = workflow[stage_offset:gate_offset]
        self.assertIn("name: Add Nim library to PATH (Windows)", windows_path_step)
        self.assertIn("if: runner.os == 'Windows'", windows_path_step)
        self.assertIn(
            'echo "${GITHUB_WORKSPACE}/${NIM_SQL_PARSER_DIR}" >> "$GITHUB_PATH"',
            windows_path_step,
        )

    def test_v08_gate_uses_the_just_built_parser_instead_of_vendor(self) -> None:
        workflow = self.workflow("ci.yml")
        gate = workflow.split("  v08-release-gate:", maxsplit=1)[1].split(
            "\n  ci-success:", maxsplit=1
        )[0]

        self.assertIn('ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD: "1"', gate)
        self.assertIn(
            "NIM_SQL_PARSER_LIB_DIR: ${{ github.workspace }}/crates/alopex-sql/nim-sql-parser",
            gate,
        )
        self.assertIn("name: Stage just-built parser for v0.8 surfaces", gate)
        self.assertIn('(cd "${local_dir}" && sha256sum -c SHA256SUMS)', gate)
        self.assertNotIn('reviewed_dir="${NIM_SQL_PARSER_DIR}/vendor/', gate)

    def test_security_audit_is_fail_closed_with_a_guarded_exception(self) -> None:
        workflow = self.workflow("ci.yml")
        audit_job = workflow.split("  security-audit:", maxsplit=1)[1].split(
            "\n  build:", maxsplit=1
        )[0]

        self.assertNotIn("continue-on-error", audit_job)
        self.assertNotIn("RUSTSEC-2026-0194", audit_job)
        self.assertNotIn("RUSTSEC-2026-0195", audit_job)
        tree_guard = audit_job.index(
            "cargo tree --locked --workspace --all-features --target all"
        )
        audit_step = audit_job.index("name: Run RustSec audit")
        self.assertLess(tree_guard, audit_step)
        self.assertIn("RUSTSEC-2026-0235 is reachable", audit_job)
        self.assertIn(
            "cargo install cargo-audit --version 0.22.2 --locked", audit_job
        )
        self.assertIn(
            "cargo audit --file Cargo.lock --ignore RUSTSEC-2026-0235", audit_job
        )
        self.assertNotIn("rustsec/audit-check@", audit_job)

    def test_clippy_uses_the_release_msrv_toolchain(self) -> None:
        workflow = self.workflow("ci.yml")
        clippy_job = workflow.split("  clippy:", maxsplit=1)[1].split(
            "\n  test:", maxsplit=1
        )[0]

        self.assertIn("uses: dtolnay/rust-toolchain@1.90.0", clippy_job)
        self.assertNotIn("uses: dtolnay/rust-toolchain@stable", clippy_job)
        self.assertIn(
            "cargo clippy --all-targets --all-features -- -D warnings",
            clippy_job,
        )

    def test_release_verdict_reuses_approved_ci_and_checks_delivery_only(self) -> None:
        workflow = self.workflow("release.yml")
        approval = workflow.split("  ci-gate:", maxsplit=1)[1].split(
            "\n  build-release:", maxsplit=1
        )[0]

        self.assertIn("Approved source evidence", approval)
        self.assertIn("gh run list --workflow ci.yml", approval)
        self.assertIn("git merge-base --is-ancestor", approval)
        for forbidden in (
            "cargo test",
            "cargo clippy",
            "maturin",
            "test-nim-parser.sh",
            "v07_gate.sh",
            "verify-v08-surfaces.sh",
        ):
            self.assertNotIn(forbidden, approval)

    def test_release_process_changes_have_a_short_dedicated_lane(self) -> None:
        process = self.workflow("release-process.yml")
        parity = self.workflow("parity-harness.yml")
        ci = self.workflow("ci.yml")
        compatibility = self.workflow("compatibility.yml")
        stress = self.workflow("stress-tests.yml")

        for path in (
            ".github/workflows/**",
            "scripts/release/**",
            "formal/release-report/**",
        ):
            self.assertIn(path, process)
            self.assertIn(path, compatibility)
        for path in (
            ".github/workflows/*",
            "scripts/release/*",
            "formal/release-report/*",
        ):
            self.assertIn(path, ci)
        self.assertIn("name: Change scope", ci)
        self.assertIn("needs: [scope]", ci)
        self.assertIn("needs.scope.outputs.production == 'true'", ci)
        self.assertIn("Release-process-only change", ci)
        self.assertIn("paths-ignore:", compatibility)
        self.assertIn("python -m unittest discover -s scripts/release/tests", process)
        self.assertNotIn("cargo test", process)
        self.assertNotIn("pull_request:", stress)
        self.assertNotIn("release-process-contract:", ci)
        self.assertIn("scripts/parity/*", ci)
        self.assertGreaterEqual(compatibility.count("scripts/parity/**"), 2)
        self.assertGreaterEqual(parity.count("scripts/parity/**"), 2)
        self.assertIn("scripts.parity.test_compat_fixture_contract", parity)
        self.assertNotIn("cargo test", parity)

    def test_workflows_do_not_clone_an_unpinned_chirps_checkout(self) -> None:
        for workflow_path in WORKFLOW_DIR.glob("*.yml"):
            with self.subTest(workflow=workflow_path.name):
                workflow = workflow_path.read_text(encoding="utf-8")
                self.assertNotIn("checkout-chirps.sh", workflow)
                self.assertNotIn("Checkout Chirps dependency", workflow)

        self.assertFalse(
            (WORKFLOW_DIR.parent / "scripts/checkout-chirps.sh").exists()
        )


if __name__ == "__main__":
    unittest.main()
